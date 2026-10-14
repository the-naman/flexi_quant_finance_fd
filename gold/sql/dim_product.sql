-- dim_product: every version of every loan product (type, interest rate, limits). Rates change about every
-- 90 days, so a loan can be matched to the terms that applied when it was taken. Key -1 = unknown product.
WITH products AS (
  SELECT
    product_id, product_name, loan_type, rate_pct, min_amt, max_amt, max_tenure_m, is_current,
    CASE WHEN row_number() OVER (PARTITION BY product_id ORDER BY valid_from) = 1
         THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END               AS valid_from,
    coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59')                         AS valid_to
  FROM silver.loan_products
)
SELECT
  xxhash64(CAST(product_id AS STRING), CAST(valid_from AS STRING)) AS product_key,
  product_id,
  product_name,
  loan_type,
  rate_pct,
  min_amt,
  max_amt,
  max_tenure_m,
  valid_from,
  valid_to,
  is_current
FROM products

UNION ALL
--     key  product_id product_name loan_type  rate  min   max   tenure valid_from                      valid_to                        is_current
SELECT -1,  'unknown', 'unknown',   'unknown', NULL, NULL, NULL, NULL,  TIMESTAMP'1900-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59', TRUE
