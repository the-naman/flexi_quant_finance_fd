"""Prepare steps: turn bronze rows that are not flat text into flat text columns, so the normal
silver cleaning can run on them. A table names its step in config/silver.yml (`prepare:`).

Every step takes (spark, bronze rows, config entry) and returns text columns named like the
`columns` sources of that entry, plus the bronze metadata columns. A step may add `_reject`
(a reason): that row goes to quarantine as not_prepared.
"""
import io
import json
import re

from pyspark.sql import functions as F

KEEP = ["_source_file", "_business_date", "_ingest_ts", "id_fingerprint"]
IST = "Asia/Kolkata"


def _keep(df):
    return [F.col(c) for c in KEEP if c in df.columns]


def _field(df, parent, child):
    """A nested JSON field, whether bronze holds the parent as a struct or as JSON text."""
    if dict(df.dtypes)[parent].startswith("struct"):
        return F.col(f"{parent}.{child}").cast("string")
    return F.get_json_object(F.col(parent), f"$.{child}")


def _whole(c):
    """Text holding a whole number -> bigint, anything else -> NULL."""
    t = F.trim(c.cast("string"))
    return F.when(t.rlike(r"^-?\d{1,18}$"), t.cast("bigint"))


def _rupees(c):
    """Paise (sent as a number or as text) -> rupees as text with two decimals."""
    return (_whole(c) / 100).cast("decimal(18,2)").cast("string")


def _ist(c, fmt):
    """Epoch seconds (UTC) -> India time as text."""
    return F.date_format(F.from_utc_timestamp(F.timestamp_seconds(_whole(c)), IST), fmt)


def _no_offset(c):
    """2026-10-01T11:20:05+05:30 -> 2026-10-01T11:20:05 (the source already sends India time)."""
    return F.regexp_replace(c.cast("string"), r"(Z|[+-]\d{2}:\d{2})$", "")


def gateway_payments(spark, df, spec):
    """Payment gateway JSON: nested notes, amounts in paise, times as epoch seconds."""
    return df.select(
        F.regexp_replace(F.col("id").cast("string"), r"^pay_", "").alias("repay_id"),
        _field(df, "notes", "loan_id").alias("loan_id"),
        _field(df, "notes", "emi_no").alias("emi_no"),
        _ist(F.col("created_at"), "yyyy-MM-dd").alias("paid_date"),
        _rupees(F.col("amount")).alias("amount"),
        F.col("method").cast("string").alias("mode"),
        _rupees(F.col("late_fee")).alias("late_fee"),
        _ist(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss").alias("last_modified"),
        *_keep(df))


def bureau_checks(spark, df, spec):
    """Credit bureau JSON: score, obligations and decision are nested objects."""
    return df.select(
        F.col("request_id").cast("string").alias("check_id"),
        F.col("application_ref").cast("string").alias("application_id"),
        _field(df, "score", "value").alias("bureau_score"),
        _field(df, "score", "band").alias("score_band"),
        _field(df, "obligations", "dti_pct").alias("dti_pct"),
        _field(df, "decision", "result").alias("decision"),
        _no_offset(F.col("pulled_at")).alias("check_ts"),
        _no_offset(F.col("updated_at")).alias("last_modified"),
        *_keep(df))


def drift_headers(spark, df, spec):
    """A file read without a header (columns _c0, _c1, ...) whose heading names change from file
    to file. The heading row of each file is found, every heading is mapped to its agreed name
    through `headers` in the config entry, and each data row gets its values by name.
    A file without a heading row, or with a heading nobody knows, is rejected with the reason."""
    cells = sorted([c for c in df.columns if re.fullmatch(r"_c\d+", c)], key=lambda c: int(c[2:]))
    alias = {a.strip().lower(): name for name, names in spec["headers"].items() for a in [name] + list(names)}
    lookup = F.create_map(*[F.lit(x) for pair in alias.items() for x in pair])
    known = lambda x: F.try_element_at(lookup, F.lower(F.trim(x)))
    values = F.array(*[F.col(c) for c in cells])
    is_header = F.coalesce(F.lower(F.trim(F.col(cells[0]))).isin(list(alias)), F.lit(False))
    headings = (df.filter(is_header).select(
        F.col("_source_file").alias("_hf"),
        F.transform(values, known).alias("_names"),
        F.filter(values, lambda x: x.isNotNull() & known(x).isNull()).alias("_unknown")).dropDuplicates(["_hf"]))
    rows = (df.filter(~is_header).withColumn("_values", values)
              .join(F.broadcast(headings), F.col("_source_file") == F.col("_hf"), "left"))
    at = lambda name: F.array_position("_names", name).cast("int")
    out = [F.when(at(name) > 0, F.element_at("_values", at(name))).alias(name) for name in spec["headers"]]
    reject = (F.when(F.col("_hf").isNull(), F.lit("file has no heading row"))
               .when(F.size("_unknown") > 0, F.concat(F.lit("unknown heading: "), F.array_join("_unknown", ", "))))
    return rows.select(*out, reject.alias("_reject"), *_keep(df))


BANK_LINE = re.compile(r"^\s*(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\S+)\s+(\S+)\s+(-?[\d,]+\.\d{2})\s+([A-Za-z]+)\s*$")
BANK_HEAD = re.compile(r"Statement no:\s*(\S+)\s+Statement date:\s*(\d{4}-\d{2}-\d{2})")
BANK_FOOT = re.compile(r"Lines:\s*(\d+)\s+Total settled:\s*(-?[\d,]+\.\d{2})")


def read_bank_pdf(content):
    """Text PDF of a bank settlement statement -> (rows, reject reason or None).
    rows: (line_no, value_date, bank_ref, loan_id, amount, status, statement_no, statement_date).
    The statement's own footer (number of lines, total settled) is used as a control."""
    from pypdf import PdfReader
    try:
        text = "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(content)).pages)
    except Exception as e:                          # not a readable PDF
        return [], f"pdf could not be read: {type(e).__name__}"
    head, foot = BANK_HEAD.search(text), BANK_FOOT.search(text)
    lines = [m.groups() for m in map(BANK_LINE.match, text.splitlines()) if m]
    if not head or not foot:
        return [], "statement number, date or footer not found"
    rows = [(n, d, ref, loan, amt, status, head.group(1), head.group(2)) for n, d, ref, loan, amt, status in lines]
    settled = sum(float(r[4].replace(",", "")) for r in rows if r[5].upper() == "SETTLED")
    reason = None
    if len(rows) != int(foot.group(1)):
        reason = f"statement says {foot.group(1)} lines, {len(rows)} were read"
    elif abs(settled - float(foot.group(2).replace(",", ""))) > 0.005:
        reason = f"statement total {foot.group(2)} differs from the lines read ({settled:.2f})"
    return rows, reason


def bank_statement(spark, df, spec):
    """Bank settlement PDFs (one bronze row per file, bytes in `content`) -> one row per statement line.
    The files are few and small, so they are read on the driver with pypdf.
    A file that cannot be read, or whose lines do not add up to its footer, is rejected with the reason."""
    keep = [c for c in KEEP if c in df.columns]
    out = []
    for f in df.select("content", *keep).collect():
        meta = tuple(f[c] for c in keep)
        rows, reason = read_bank_pdf(bytes(f["content"]))
        for r in rows or [(None,) * 8]:             # an unreadable file still gives one row, for the quarantine
            out.append(tuple(None if v is None else str(v) for v in r) + (reason,) + meta)
    names = ["line_no", "value_date", "bank_ref", "loan_id", "amount", "status", "statement_no", "statement_date", "_reject"]
    schema = ", ".join(f"{n} string" for n in names)
    for c in keep:
        schema += f", {c} {dict(df.dtypes)[c]}"
    return spark.createDataFrame(out, schema)


SLIP_NO = re.compile(r"Slip no:\s*(\S+)")
SLIP_PERIOD = re.compile(r"Period:\s*(\d{4}-\d{2}-\d{2})\s*to\s*(\d{4}-\d{2}-\d{2})")
SLIP_LINE = re.compile(r"^\s*(\S+)\s+(\d+)\s+(-?[\d,]+\.\d{2})\s*$")


def read_slip(parsed, min_confidence):
    """OCR result of one cash deposit slip (JSON text from ai_parse_document) ->
    (rows, reject reason or None). rows: (slip_no, branch_id, deposit_from, deposit_to, receipts, amount, confidence).
    Three controls: the OCR must not report an error, its lowest confidence on the parts that
    carry data must reach min_confidence, and the branch lines must add up to the slip's own TOTAL line."""
    try:
        doc = json.loads(parsed)
    except (TypeError, ValueError):
        return [], "no OCR result"
    if doc.get("error_status"):
        return [], f"OCR error: {doc['error_status']}"
    elements = (doc.get("document") or {}).get("elements") or []
    text = "\n".join(str(e.get("content") or "") for e in elements if e.get("type") != "table")
    lines = []
    for e in elements:                              # the OCR returns a table as HTML
        if e.get("type") == "table":
            for tr in re.findall(r"<tr>(.*?)</tr>", str(e.get("content") or ""), re.S):
                lines.append([c.strip() for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)])
    if not lines:                                   # or as plain text lines
        lines = [list(m.groups()) for m in map(SLIP_LINE.match, text.splitlines()) if m]
    lines = [c for c in lines if len(c) == 3 and re.fullmatch(r"\d+", c[1]) and re.fullmatch(r"-?[\d,]+\.\d{2}", c[2])]
    slip, period = SLIP_NO.search(text), SLIP_PERIOD.search(text)
    # confidence of the parts that carry data (the table, the slip number and period); a logo line or
    # a signature line read with less confidence must not block the slip
    carries = lambda e: (e.get("type") == "table" or SLIP_NO.search(str(e.get("content") or ""))
                         or SLIP_PERIOD.search(str(e.get("content") or ""))
                         or any(SLIP_LINE.match(x) for x in str(e.get("content") or "").splitlines()))
    scores = [e["confidence"] for e in elements if e.get("confidence") is not None and carries(e)]
    confidence = min(scores) if scores else 0.0
    branches = [c for c in lines if c[0].upper() != "TOTAL"]
    total = [c for c in lines if c[0].upper() == "TOTAL"]
    rows = [(slip.group(1) if slip else None, b, period.group(1) if period else None,
             period.group(2) if period else None, n, amt, f"{confidence:.4f}") for b, n, amt in branches]
    money = lambda t: round(float(t.replace(",", "")), 2)
    if not slip or not period or not branches or not total:
        return rows, "slip number, period, branch lines or TOTAL line not found"
    if confidence < min_confidence:
        return rows, f"OCR confidence {confidence:.2f} is below {min_confidence}"
    if (sum(int(c[1]) for c in branches) != int(total[0][1])
            or abs(sum(money(c[2]) for c in branches) - money(total[0][2])) > 0.005):
        return rows, "branch lines do not add up to the TOTAL line"
    return rows, None


def deposit_slip(spark, df, spec):
    """Cash deposit slip images (one bronze row per file) -> one row per branch on the slip.
    The image is read by the Databricks AI function ai_parse_document (OCR); the result is parsed
    on the driver. A slip that fails a control (see read_slip) is rejected with the reason."""
    keep = [c for c in KEEP if c in df.columns]
    parsed = df.select(F.expr("CAST(ai_parse_document(content) AS STRING)").alias("_parsed"), *keep).collect()
    out = []
    for f in parsed:
        meta = tuple(f[c] for c in keep)
        rows, reason = read_slip(f["_parsed"], float(spec.get("min_confidence", 0.9)))
        for r in rows or [(None,) * 7]:             # an unreadable slip still gives one row, for the quarantine
            out.append(tuple(r) + (reason,) + meta)
    names = ["slip_no", "branch_id", "deposit_from", "deposit_to", "receipts", "amount", "ocr_confidence", "_reject"]
    schema = ", ".join(f"{n} string" for n in names)
    for c in keep:
        schema += f", {c} {dict(df.dtypes)[c]}"
    return spark.createDataFrame(out, schema)

    