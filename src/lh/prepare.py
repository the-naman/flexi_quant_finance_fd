"""Prepare steps: turn bronze rows that are not flat text into flat text columns, so the normal
silver cleaning can run on them. A table names its step in config/silver.yml (`prepare:`).

Every step takes (spark, bronze rows, config entry) and returns text columns named like the
`columns` sources of that entry, plus the bronze metadata columns. A step may add `_reject`
(a reason): that row goes to quarantine as not_prepared.
"""
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