# Processing Analysis

Aggregates daily production data from shift-level Excel reports and generates interactive dashboards for the Walton processing facility. Dashboards are auto-deployed to GitHub Pages via GitHub Actions.

## Directory Structure

```
src/                    Active Python scripts
  aggregate_daily_data.py       Parse raw Excel reports into aggregated data
  build_daily_dashboard.py      Generate daily production dashboard
  build_interactive_dashboard.py Generate weekly trends dashboard
data/                   Aggregated data files (tracked in git)
docs/                   Generated HTML dashboards (GitHub Pages)
processing_reports/     Raw shift Excel reports (gitignored)
archive/                Legacy scripts (kept for reference)
```

## Local Setup

```bash
pip install -r requirements.txt
```

## Usage

**Aggregate raw data** (run when new processing reports are added):
```bash
python src/aggregate_daily_data.py
```

**Build dashboards**:
```bash
python src/build_interactive_dashboard.py
python src/build_daily_dashboard.py
```

Dashboards are written to `docs/index.html` and `docs/daily.html`.

## Automation

The GitHub Actions workflow (`.github/workflows/build-dashboard.yml`) rebuilds dashboards automatically when scripts or data files are pushed to `main`.
