-- dim_customer: every version of every FQF customer id WITHOUT name, phone, Aadhaar number, date of birth
-- or exact income (only an income band).
-- master_customer_id: the same person sometimes has two customer ids; silver.customer_id_map names the one
--                     that survives, so counts of people use master_customer_id.
-- contact_fingerprint() is a function the Gold runner registers (keyed hash of phone + date of birth).
WITH customers AS (
  SELECT
    customer_id, gender, city, branch_id, monthly_income, employment, id_fingerprint, phone, dob, is_current,
    CASE WHEN row_number() OVER (PARTITION BY customer_id ORDER BY valid_from) = 1
         THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END               AS valid_from,
    coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59')                         AS valid_to
  FROM silver.customers
)
SELECT
  xxhash64(CAST(c.customer_id AS STRING), CAST(c.valid_from AS STRING)) AS customer_key,
  c.customer_id,
  coalesce(m.survivor_id, c.customer_id)                               AS master_customer_id,
  c.gender,
  c.city,
  c.branch_id,
  CASE WHEN c.monthly_income IS NULL   THEN 'unknown'
       WHEN c.monthly_income < 25000   THEN '<25k'
       WHEN c.monthly_income < 50000   THEN '25k-50k'
       WHEN c.monthly_income < 100000  THEN '50k-1L'
       ELSE '1L+' END                                                   AS income_band,
  coalesce(c.employment, 'unknown')                                     AS employment,
  c.id_fingerprint,
  contact_fingerprint(c.phone, c.dob)                                   AS contact_fingerprint,
  c.valid_from,
  c.valid_to,
  c.is_current
FROM customers c
LEFT JOIN silver.customer_id_map m ON c.customer_id = m.customer_id

UNION ALL
--     key  customer_id master_id  gender     city       branch income_band employment id_fp contact_fp valid_from                      valid_to                        is_current
SELECT -1,  'unknown',  'unknown', 'unknown', 'unknown', NULL,  'unknown',  'unknown', NULL, NULL,      TIMESTAMP'1900-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59', TRUE
