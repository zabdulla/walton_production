# cieTrade API — converting jobs into the dashboard

Since 2026-08-24 production tonnage comes from cieTrade instead of the hand-built weekly
workbooks. The chain is:

    cieTrade API ─10 min─▶ data/cietrade/ ─▶ cietrade_model ─▶ cietrade_daily ─▶ aggregated_daily_data.xlsx ─▶ dashboards
    End of Shift app ────▶ data/labor_entries.xlsx ──────────────────┘ (hours, crew, material, comments)

## The endpoint

`GET https://api.cietrade.net/ListConvertingJobs` — the Converting Job Inquiry screen as JSON.
Two things authenticate every call: the header `Authorization: Bearer <API key>` (identifies the
database) and the query parameter `UserID=<cieTrade login email>` (identifies who is calling; the
call is logged under that user). Send `Accept: application/json`. Filters: `JobNo`, `DateType`
(JOB | POST), `DateFrom`, `DateTo`, `Warehouse`, `WarehouseStatus`, `Status` (WORK | POSTED | ALL),
`Machine`, `Dept`, `Operator`, `FinishedProduct`. Errors arrive as HTTP 200 with
`[{"ERROR": "..."}]`. Reference: cieTrade's *ListConvertingJobs endpoint reference* v1.0
(2026-09-16) and <https://cietrade.helpscoutdocs.com/article/1689-list-converting-jobs>.

Credentials live outside the repo in `~/.config/walton/cietrade.json` (mode 600):

```json
{"user_id": "<login email>", "api_key": "<token from Settings > Integration > Api Token>"}
```

`CIETRADE_USER_ID` / `CIETRADE_API_KEY` in the environment override the file (CI). The key is
never placed in a URL or a log line.

## The poller — `src/cietrade_poll.py`

Every 10 minutes (launchd `com.walton.cietrade_poll`):

1. fetch every unposted job (`Status=WORK`) — the open-jobs snapshot;
2. fetch jobs posted in the last 14 days (`DateType=POST`);
3. append one line to `data/cietrade/polls.jsonl` (also on failure);
4. when the open-jobs list differs from the previous poll, append it to
   `data/cietrade/snapshots/<date>.csv` stamped with the poll time;
5. upsert postings into `data/cietrade/posted.csv` (First Seen / Last Seen, edits flagged);
6. rebuild the pilot page and copy it to `config.LIVE_PAGE_COPY` (a OneDrive folder, so it opens
   on a phone).

`--backfill-posted` fetches every posting since 2026-01-01 (run once; done 2026-09-17).
`--dry-run` calls the API and writes nothing. All three data files are small text and are
committed by the daily update — the snapshots are the one thing that cannot be re-fetched.

## The model — `src/cietrade_model.py`

Observations = manual Converting Inquiry exports (`data/cietrade_exports/`, the history) +
the poller's files. Each open-jobs observation is a snapshot; the pounds a job gains between
two snapshots are credited to the shifts that ran in between (`config.SHIFT_HOURS`, split by
hours when a shift straddles a poll). Jobs without snapshots spread their posted output
evenly over the shift-days they own. Validated against the Aug 3–21 workbooks: 98.8% of
shift-days to the pound. The shift under way at the last poll is *partial*; a shift that
began before the last successful poll and has no job is idle.

## Into the dashboard — `src/cietrade_daily.py`

From `config.CIETRADE_FROM_DATE` on, every run regenerates one aggregate row per
(date, shift, machine): pounds from the model (per cieTrade line, mapped by
`config.CIETRADE_LINE_TO_MACHINE`), hours / crew / operators / material / comments from
`data/labor_entries.xlsx` when the End of Shift app has an entry. Guillotine keeps the
workbook convention (rolls in as `Actual_Input`, no `Actual_Output`). Rows carry
`Source = cietrade`; weeks that ever get a workbook again win over cieTrade rows.

`src/weekly_update.py --daily` (launchd `com.walton.daily_update`, 07:30) runs: cieTrade rows →
validate → build dashboards → commit + push, which republishes
<https://zabdulla.github.io/walton_production/>. The Monday weekly run does the same plus the
Gmail fetch and payroll.

## Weekly routine for the office

- **Post jobs once a week** (any day). The API sees the posting within 10 minutes; nothing else
  changes. Daily figures were already exact from the polls.
- **Open next week's jobs** as before (one per machine and shift). The 1st-shift job is opened
  after the shift ends, which the model understands.
- **Tag bales during the shift they come from.** Anything entered late lands in the window in
  which it was entered. Weekly totals are unaffected.
- Deploy the End of Shift app (`setup/LABOR_CAPTURE.md`) so hours and operators flow in; until
  then rows carry output only and the validation report warns about output without hours.

## Operations

    scripts/install_schedule.sh cietrade_poll     # every 10 min
    scripts/install_schedule.sh daily_update      # 07:30 daily
    launchctl kickstart -k gui/$(id -u)/com.walton.cietrade_poll
    tail -f logs/cietrade_poll_stdout.log
    python3 src/cietrade_daily.py --dry-run       # what the next daily run would write
    python3 explorations/cietrade_pilot/build.py  # rebuild the pilot page by hand
