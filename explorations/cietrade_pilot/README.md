# cieTrade converting-export pilot

Interim daily production from cieTrade's **Converting Inquiry** export, while the
weekly workbooks are no longer hand-built. Nothing here touches the live dashboard.

Every export you download is a snapshot of the jobs it lists at the moment it was
downloaded. Open-jobs exports (Status = Work) are the valuable ones: the same job shows
up in each with a larger Output Qty, and the difference between two exports is what that
line produced in between. Post jobs whenever convenient (weekly is fine); export daily.

    python3 explorations/cietrade_pilot/ingest.py   # archives new ~/Downloads exports by their download time
    python3 explorations/cietrade_pilot/build.py    # derives daily figures, validates, writes out/*.html

- `ingest.py` copies exports to `data/cietrade_exports/converting_<time>.csv` (gitignored).
  The file's creation time is the export time; override with `--at "2026-09-14 11:34"`.
- `model.py` owns the rules: coverage (which shift-days a job owns), even spread for
  jobs without snapshots, hours-weighted windows for jobs with snapshots, validation
  against the Aug 3–21 workbooks (`data/aggregated_daily_data.xlsx`).
- `build.py` injects the result into `template.html`; publish `out/walton_daily_pilot.html`
  as the artifact (the standalone file opens directly in a browser).

Best export time: **7 AM, before 1st shift starts** — then every window is exactly one
production day per line and every day reads as exact. Any other time still works; a
shift that straddles two exports is split by hours.
