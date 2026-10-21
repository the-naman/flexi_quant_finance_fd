-- dim_branch: one row per FQF branch, plus the "unknown" branch (key -1).
SELECT
  xxhash64(CAST(branch_id AS STRING)) AS branch_key,
  branch_id,
  branch_name,
  city,
  state,
  opened_date,
  NOT is_deleted                      AS is_active
FROM silver.branches

UNION ALL
--     key  branch_id  branch_name city  state opened_date is_active
SELECT -1,  'unknown', 'unknown',  NULL, NULL, NULL,       NULL
