-- dim_loan: one row per loan with its terms, the latest credit check, the credit policy that was in force
-- on the application date, whether it paid a BHG hospital bill, and its latest collections bucket.
-- Key -1 = unknown loan (a repayment whose loan is missing).
WITH
applications AS (
  SELECT application_id, applied_date, bhg_invoice_id FROM silver.loan_applications WHERE NOT is_deleted
),
latest_check AS (        -- the most recent bureau check of each application
  SELECT application_id, bureau_score, score_band, dti_pct
  FROM (SELECT *, row_number() OVER (PARTITION BY application_id ORDER BY check_ts DESC) AS rn
        FROM silver.credit_checks WHERE NOT is_deleted)
  WHERE rn = 1
),
policies AS (
  SELECT policy_id, product_id, effective_from, max_dti AS policy_max_dti,
         min_bureau_score AS policy_min_bureau_score, max_exposure AS policy_max_exposure
  FROM silver.credit_policy WHERE NOT is_deleted
),
latest_bucket AS (       -- the most recent collections bucket of each loan
  SELECT loan_id, bucket AS dpd_bucket
  FROM (SELECT *, row_number() OVER (PARTITION BY loan_id ORDER BY last_modified DESC) AS rn
        FROM silver.collections WHERE NOT is_deleted)
  WHERE rn = 1
),
loans AS (               -- each loan with the newest policy of its product that started on or before the application
  SELECT l.*, a.applied_date, a.bhg_invoice_id, c.bureau_score, c.score_band, c.dti_pct,
         p.policy_id, p.policy_max_dti, p.policy_min_bureau_score, p.policy_max_exposure,
         row_number() OVER (PARTITION BY l.loan_id ORDER BY p.effective_from DESC NULLS LAST) AS rn
  FROM (SELECT * FROM silver.loans WHERE NOT is_deleted) l
  LEFT JOIN applications a ON l.application_id = a.application_id
  LEFT JOIN latest_check c ON l.application_id = c.application_id
  LEFT JOIN policies p     ON l.product_id = p.product_id AND p.effective_from <= a.applied_date
)
SELECT
  xxhash64(CAST(l.loan_id AS STRING)) AS loan_key,
  l.loan_id,
  l.application_id,
  l.principal,
  l.rate_pct,
  l.tenure_m,
  l.start_date,
  l.status,
  l.channel,                          -- source column from 15 Oct 2026 (branch, app, web, partner_hospital), NULL before
  l.bureau_score,
  l.score_band,
  l.dti_pct,
  l.policy_id,
  l.policy_max_dti,
  l.policy_min_bureau_score,
  l.policy_max_exposure,
  l.bhg_invoice_id IS NOT NULL        AS is_hospital_bill,
  coalesce(b.dpd_bucket, 'current')   AS dpd_bucket
FROM loans l
LEFT JOIN latest_bucket b ON l.loan_id = b.loan_id
WHERE l.rn = 1

UNION ALL
--     key  loan_id    application principal rate tenure start status     channel score band  dti   policy max_dti min_score max_exp hospital_bill dpd_bucket
SELECT -1,  'unknown', NULL,       NULL,     NULL, NULL, NULL, 'unknown', NULL,   NULL, NULL, NULL, NULL,  NULL,   NULL,     NULL,   NULL,         'unknown'
