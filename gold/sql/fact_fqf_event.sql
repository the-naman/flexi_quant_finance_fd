-- fact_fqf_event: one row per money event on an FQF loan.
--   lending     disbursement  money paid out: to the customer, or to a BHG hospital (hospital-bill loan)
--   due         emi_due       each instalment that has fallen due up to today, split into principal and interest
--   collection  repayment     an instalment paid, split like the instalment it pays; dpd = days paid late
--   fee         late_fee
-- A repayment whose loan is not in Silver gets key -1 for loan, customer, product and branch.
-- Customer and product are the versions that were valid when the event happened.
WITH
emi AS (
  SELECT loan_id, emi_no, due_date, principal_due, interest_due, emi_amount
  FROM silver.emi_schedule WHERE NOT is_deleted
),
repayments AS (          -- each repayment with the instalment it pays and how many days late it was
  SELECT r.*, e.due_date, e.principal_due, e.interest_due, greatest(datediff(r.paid_date, e.due_date), 0) AS days_late
  FROM (SELECT * FROM silver.repayments WHERE NOT is_deleted) r
  LEFT JOIN emi e ON r.loan_id = e.loan_id AND r.emi_no = e.emi_no
),

events AS (
  SELECT 'disbursement' AS event_type, 'lending' AS event_group, disb_id AS event_id,
         CAST(disb_date AS TIMESTAMP) AS event_ts, loan_id,
         CAST(amount AS DECIMAL(18,2)) AS amount, CAST(amount AS DECIMAL(18,2)) AS principal_part,
         CAST(NULL AS DECIMAL(18,2)) AS interest_part, CAST(NULL AS INT) AS dpd_at_event,
         CAST(NULL AS STRING) AS pay_mode, CAST(NULL AS INT) AS emi_no, CAST(bhg_invoice_id AS STRING) AS bhg_invoice_id
  FROM silver.disbursements WHERE NOT is_deleted

  UNION ALL  -- instalments due up to today; id = loan + instalment number (LN0001-007)
  SELECT 'emi_due', 'due', concat_ws('-', loan_id, lpad(CAST(emi_no AS STRING), 3, '0')),
         CAST(due_date AS TIMESTAMP), loan_id,
         CAST(emi_amount AS DECIMAL(18,2)), CAST(principal_due AS DECIMAL(18,2)), CAST(interest_due AS DECIMAL(18,2)),
         NULL, NULL, CAST(emi_no AS INT), NULL
  FROM emi WHERE due_date <= current_date()

  UNION ALL
  SELECT 'repayment', 'collection', repay_id, CAST(paid_date AS TIMESTAMP), loan_id,
         CAST(amount AS DECIMAL(18,2)), CAST(principal_due AS DECIMAL(18,2)), CAST(interest_due AS DECIMAL(18,2)),
         CAST(days_late AS INT), CAST(mode AS STRING), CAST(emi_no AS INT), NULL
  FROM repayments

  UNION ALL
  SELECT 'late_fee', 'fee', repay_id, CAST(paid_date AS TIMESTAMP), loan_id,
         CAST(late_fee AS DECIMAL(18,2)), NULL, NULL,
         CAST(days_late AS INT), CAST(mode AS STRING), CAST(emi_no AS INT), NULL
  FROM repayments WHERE late_fee > 0
),

customer_versions AS (   -- same versions and keys as dim_customer
  SELECT customer_id, valid_from, valid_to, xxhash64(CAST(customer_id AS STRING), CAST(valid_from AS STRING)) AS customer_key
  FROM (SELECT customer_id,
               CASE WHEN row_number() OVER (PARTITION BY customer_id ORDER BY valid_from) = 1
                    THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END AS valid_from,
               coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59') AS valid_to
        FROM silver.customers)
),
product_versions AS (    -- same versions and keys as dim_product
  SELECT product_id, valid_from, valid_to, xxhash64(CAST(product_id AS STRING), CAST(valid_from AS STRING)) AS product_key
  FROM (SELECT product_id,
               CASE WHEN row_number() OVER (PARTITION BY product_id ORDER BY valid_from) = 1
                    THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END AS valid_from,
               coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59') AS valid_to
        FROM silver.loan_products)
),

keyed AS (
  SELECT e.*, l.loan_id AS found_loan, l.branch_id, b.branch_id AS found_branch,
         CAST(CASE WHEN l.customer_id IS NULL THEN NULL ELSE coalesce(c.customer_key, -1) END AS BIGINT) AS customer_key,
         CAST(CASE WHEN l.product_id  IS NULL THEN NULL ELSE coalesce(p.product_key,  -1) END AS BIGINT) AS product_key
  FROM events e
  LEFT JOIN (SELECT loan_id, customer_id, product_id, branch_id FROM silver.loans WHERE NOT is_deleted) l
         ON e.loan_id = l.loan_id
  LEFT JOIN customer_versions c
         ON l.customer_id = c.customer_id AND e.event_ts >= c.valid_from AND e.event_ts < c.valid_to
  LEFT JOIN product_versions p
         ON l.product_id = p.product_id AND e.event_ts >= p.valid_from AND e.event_ts < p.valid_to
  LEFT JOIN (SELECT branch_id FROM silver.branches) b ON l.branch_id = b.branch_id
)

SELECT
  xxhash64(CAST(event_type AS STRING), CAST(event_id AS STRING))                       AS event_key,
  event_id,
  event_type,
  event_group,
  CAST(date_format(event_ts, 'yyyyMMdd') AS INT)                                       AS date_key,
  CAST(CASE WHEN found_loan IS NOT NULL THEN xxhash64(CAST(loan_id AS STRING)) ELSE -1 END AS BIGINT)     AS loan_key,
  CAST(CASE WHEN found_loan IS NOT NULL THEN customer_key ELSE -1 END AS BIGINT)                          AS customer_key,
  CAST(CASE WHEN found_loan IS NOT NULL THEN product_key  ELSE -1 END AS BIGINT)                          AS product_key,
  CAST(CASE WHEN found_branch IS NOT NULL THEN xxhash64(CAST(branch_id AS STRING)) ELSE -1 END AS BIGINT) AS branch_key,
  loan_id,
  emi_no,
  amount,
  principal_part,
  interest_part,
  dpd_at_event,
  pay_mode,
  bhg_invoice_id,
  bhg_invoice_id IS NOT NULL                                                           AS is_hospital_bill
FROM keyed
