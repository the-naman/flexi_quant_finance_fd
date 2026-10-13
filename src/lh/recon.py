"""Reconciliation: match two independent records of the same money and list the differences.

  bank_vs_books : loan payouts in the books (silver.disbursements) against the bank's statement
                  lines (silver.bank_settlements), matched by bank reference
  cash_vs_slips : cash repayments in the books, per deposit slip period and branch, against the
                  branch lines of the deposit slips (silver.cash_deposit_slips)

Every difference is one row in <catalog>.silver.reconciliation. The table is rebuilt in full on
each run, so it always shows the open differences as of now. Counts also go to ops.dq_results.
"""
from pyspark.sql import functions as F


def bank_vs_books(spark, cat):
    return spark.sql(f"""
        SELECT 'bank_vs_books' AS recon,
               coalesce(d.bank_ref, b.bank_ref) AS ref,
               CASE WHEN b.bank_ref IS NULL THEN 'missing_in_bank'
                    WHEN d.bank_ref IS NULL THEN 'missing_in_books'
                    WHEN b.status = 'returned' THEN 'returned_by_bank'
                    WHEN d.amount <> b.amount THEN 'amount_mismatch' END AS issue,
               d.amount AS books_amount, b.amount AS other_amount,
               concat_ws(' ', 'loan', coalesce(d.loan_id, b.loan_id), 'statement', b.statement_no) AS detail
        FROM (SELECT * FROM {cat}.silver.disbursements WHERE NOT is_deleted) d
        FULL OUTER JOIN {cat}.silver.bank_settlements b ON d.bank_ref = b.bank_ref""")


def cash_vs_slips(spark, cat):
    return spark.sql(f"""
        WITH periods AS (SELECT DISTINCT slip_no, deposit_from, deposit_to FROM {cat}.silver.cash_deposit_slips),
        cash AS (SELECT l.branch_id, r.paid_date, r.amount + coalesce(r.late_fee, 0) AS amount
                 FROM {cat}.silver.repayments r JOIN {cat}.silver.loans l ON r.loan_id = l.loan_id
                 WHERE r.mode = 'cash' AND NOT r.is_deleted),
        books AS (SELECT p.slip_no, c.branch_id, count(*) AS receipts, sum(c.amount) AS amount
                  FROM cash c LEFT JOIN periods p ON c.paid_date BETWEEN p.deposit_from AND p.deposit_to
                  GROUP BY p.slip_no, c.branch_id)
        SELECT 'cash_vs_slips' AS recon,
               concat_ws('|', coalesce(k.slip_no, s.slip_no, '(no slip)'), coalesce(k.branch_id, s.branch_id)) AS ref,
               CASE WHEN s.slip_no IS NULL THEN 'cash_without_slip'
                    WHEN k.branch_id IS NULL THEN 'slip_without_cash'
                    WHEN s.amount < k.amount THEN 'cash_short'
                    WHEN s.amount > k.amount THEN 'cash_over'
                    WHEN s.receipts <> k.receipts THEN 'receipt_count_differs' END AS issue,
               k.amount AS books_amount, s.amount AS other_amount,
               concat('receipts books ', coalesce(k.receipts, 0), ', slip ', coalesce(s.receipts, 0)) AS detail
        FROM books k
        FULL OUTER JOIN {cat}.silver.cash_deposit_slips s
          ON k.slip_no = s.slip_no AND k.branch_id = s.branch_id""")


def run(spark, cfg):
    """Rebuild silver.reconciliation and log the counts. Returns {recon: (pairs checked, differences)}."""
    cat = cfg["catalog"]
    pairs = bank_vs_books(spark, cat).unionByName(cash_vs_slips(spark, cat))
    pairs = pairs.withColumn("difference", F.coalesce(F.col("other_amount"), F.lit(0)) - F.coalesce(F.col("books_amount"), F.lit(0)))
    counts = {r["recon"]: (r["n"], r["bad"]) for r in
              pairs.groupBy("recon").agg(F.count("*").alias("n"), F.count("issue").alias("bad")).collect()}
    (pairs.filter("issue IS NOT NULL").withColumn("_silver_ts", F.current_timestamp())
          .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{cat}.silver.reconciliation"))
    spark.createDataFrame([("reconciliation", k, n, bad, "pass" if bad == 0 else "warn") for k, (n, bad) in counts.items()],
                          "table_name string, rule string, checked long, failed long, status string") \
         .select(F.current_timestamp().alias("run_ts"), "*") \
         .write.mode("append").saveAsTable(f"{cat}.ops.dq_results")
    return counts