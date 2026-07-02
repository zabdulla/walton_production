# Walton Production Analysis

Aggregates shift-level Excel reports from the Walton recycling facility into
daily production data, validates it, and publishes five interactive
dashboards. Fully automated: data arrives by email, the pipeline fetches,
aggregates, gates on data quality, builds, and publishes — with a cloud
fallback if the primary machine is off.

**Live dashboards:** https://zabdulla.github.io/walton_production/
(weekly trends) and [/daily.html](https://zabdulla.github.io/walton_production/daily.html)
(daily calendar). Light/dark theme, mobile-friendly.

## How data flows

```
Gmail (Carl's weekly emails)
  │  src/fetch_emails.py          Gmail API; renames from subject line
  ▼
processing_reports/*.xlsx          (gitignored, raw shift workbooks)
  │  src/aggregate_daily_data.py  incremental parse → merge → dedup →
  ▼                                atomic write + rolling snapshots
data/aggregated_daily_data.xlsx    (tracked; canonical snake_case schema)
  │  src/validate_data.py         7 checks; gating_decision() can BLOCK
  ▼                                publication (restores from snapshot)
5 dashboard builders               templates in *_template.py + dashboard_common.py
  ▼
docs/*.html (published)  +  reports/*.html (local-only, gitignored)
  │  git push → .github/workflows/deploy-pages.yml
  ▼
GitHub Pages
```

Payroll runs beside this: `src/parse_payroll_pdf.py` reads bi-weekly pay-period
PDFs (Walton-only, department-filtered) into `data/aggregated_payroll.xlsx`
(gitignored — PII) and powers the payroll + profit dashboards via the local
`data/employee_roster.json` (gitignored; schema in `employee_roster.example.json`).

## The three-layer Monday automation

| Layer | When | What |
|---|---|---|
| launchd (this Mac) | Mon 12:00 local | `src/weekly_update.py` end-to-end; macOS notification |
| Cloud fallback | Mon 18:00 UTC | `.github/workflows/weekly-cloud.yml` — runs the same pipeline headless in Actions if the local run didn't publish (Gmail creds from repo secrets; payroll skipped, roster is local-only) |
| Heartbeat | Tue 14:00 UTC | `.github/workflows/heartbeat.yml` opens an issue if Monday's data never landed |

CI (`.github/workflows/build-dashboard.yml`) is **verify-only**: tests + build +
smoke on every push. It never commits back (that pattern caused chronic rebase
conflicts; the weekly job is the single publisher).

## Operations runbook

```bash
python3 src/last_run_status.py            # did the last run work? what's next?
python3 src/weekly_update.py              # manual full run (fetch→publish)
python3 src/weekly_update.py --no-fetch --no-push   # rebuild only
python3 src/aggregate_daily_data.py --full          # full re-parse of all workbooks
gh workflow run deploy-pages.yml          # re-deploy Pages manually
gh workflow run weekly-cloud.yml -f force=true      # force a cloud run
python3 -m pytest tests/ -q               # 200+ tests
```

**When the validation gate blocks publication** (run exits 3, notification says
BLOCKED), it's almost always one of:

| Reason | Fix |
|---|---|
| Unmapped product (≥5 rows) | Add to `PRODUCT_CATEGORY_MAP` (or `PRODUCT_TYPO_MAP`) in `src/config.py` |
| Unrostered payroll employee | Add them to `data/employee_roster.json` (see example file for schema/roles) |
| Duplicate rows | Dedup key drift — compare `DEDUP_SUBSET` usage in aggregate vs validate |

Then re-run `python3 src/weekly_update.py`. The gate restored the previous
data automatically; nothing is corrupted.

**Adding a machine** requires touching four dicts in `src/config.py`:
`MACHINE_DATA_RANGES`, `MACHINE_WEEKLY_CAPACITY`,
`MACHINE_WEEKLY_OUTPUT_TARGETS` (optional), `MACHINE_PRESETS`.

## Repository map

```
src/
  config.py                     Single source of truth: machines, products,
                                targets, rates, dedup key, chart palette
  fetch_emails.py               Gmail API fetcher (60s timeouts, retries)
  aggregate_daily_data.py       Parse + incremental merge + dedup + atomic write
  validate_data.py              Health checks + publish gating
  parse_payroll_pdf.py          Pay-period PDF parser + roster + comparison
  weekly_update.py              Orchestrator (in-process, 6 steps, notifications)
  last_run_status.py            Run history / next-scheduled CLI
  atomic.py                     Atomic writes, snapshots, growth sanity check
  dashboard_common.py           Shared CSS/JS: theme tokens, dark mode, mobile
  build_interactive_dashboard.py / interactive_template.py    docs/index.html
  build_daily_dashboard.py     / daily_template.py            docs/daily.html
  build_operator_dashboard.py   reports/operator.html (local)
  build_profit_dashboard.py     reports/profit.html (local)
  build_payroll_dashboard.py    reports/payroll.html (local, anonymized by default)
tests/                          pytest suite (unit + golden-file + HTML smoke)
setup/                          One-time guides: GMAIL_API_SETUP, WEEKLY_AUTOMATION
scripts/                        launchd install/uninstall for the Monday job
data/snapshots/                 Rolling backups (last 7 per file, gitignored)
```

Column meanings for the aggregated data: [setup/DATA_DICTIONARY.md](setup/DATA_DICTIONARY.md).

## One-time setup on a new machine

1. `pip install -r requirements.lock`
2. Gmail API: follow [setup/GMAIL_API_SETUP.md](setup/GMAIL_API_SETUP.md)
3. Payroll (optional): copy `data/employee_roster.example.json` →
   `data/employee_roster.json` and fill in real names/roles
4. Schedule: `scripts/install_schedule.sh` (Monday noon launchd job)

Details in [setup/WEEKLY_AUTOMATION.md](setup/WEEKLY_AUTOMATION.md).

## Data sensitivity

Production volumes/costs are in the tracked data and published dashboards.
The employee roster and payroll aggregates are **never** committed
(gitignored); the payroll dashboard embeds pseudonyms unless built with
`--with-names`. See CODEBASE_REVIEW.md §0 for the outstanding
repo-visibility decision.
