# flexi_quant_finance_fd
Lakehouse pipelines of Flexi Quant Finance (FQF), the lending company acquired by Bio Health Group.

## Layout
| Folder | What it holds |
|---|---|
| `simulator/` | synthetic source data generator (a GitHub Action writes the files to the S3 landing folder) |
| `bronze/`, `silver/` | load raw files with Auto Loader, then clean, standardise and keep history (`src/lh/bronze.py`, `silver.py`) |
| `gold/` | Gold runners. The Gold logic itself is SQL, one SELECT per table: `gold/sql/<table>.sql` |
| `security/` | column masks and row filters, driven by `config/privacy.yml` |
| `bi/` | BI views and AI/BI dashboards (published per environment) |
| `jobs/` | the daily job as code (`config/jobs.yml`) |
| `checks/` | automated checks (t00 = smoke check of every daily run, t28 = deployed jobs match config, t29 = each person's own access matches the access matrix) |
| `ci/`, `.github/workflows/` | promotion checks, approval notices, deploy after a merge |
| `config/` | every setting, per environment. No secrets: they live in GitHub secrets and Databricks secret scopes |
| `setup_once/` | one-time environment setup and access grants (admin) |

## How code moves
- Work happens on `fqf_dev`: each person runs their own dev job (`lh_fqf_dev_daily_<role>`).
- `fqf_dev` -> `fqf_test` and `fqf_test` -> `fqf_prod` only by pull request. Checks run first; the Manager
  approves test, the Business-user Manager approves prod. A rejection sends the change back to the lower branch.
- After a merge, CI pulls the environment's Git folder, redeploys the daily job and checks it (`checks/t28_jobs`).
- Only code moves between environments, never data. No personal data, address or secret is ever committed.

## Emergency hotfix (10j)
For a prod problem that cannot wait for the normal path (rules in `config/ci.yml`, checked by `ci/route.py`):
1. Branch `fqf_hotfix_<name>` from `fqf_prod`, make the fix, push.
2. Pull request into `fqf_test`: same checks, URGENT approval by Management (environment `fqf_test_hotfix`).
   Merge with "Create a merge commit".
3. Pull request into `fqf_prod`: accepted only once the fix is in test and the branch holds only prod + the fix;
   the Business-user Manager approves.
4. After the prod deploy, `backmerge.yml` (`ci/backmerge.py`) opens a pull request `fqf_prod -> fqf_dev`
   so dev keeps the fix. Merge it with "Create a merge commit", then delete the hotfix branch.
