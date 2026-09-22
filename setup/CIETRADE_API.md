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

**The poll log is the official data set; everything on the dashboard is derived from it after
every poll.** The poller runs every 10 minutes, around the clock, in GitHub Actions
(`.github/workflows/cietrade-poll.yml`). It used to run on one Mac via launchd
(`com.walton.cietrade_poll`), which meant it stopped whenever that laptop slept, and the
dashboards were rebuilt only once a day at 07:30 by another launchd job. Each cloud run now:

1. fetches every unposted job (`Status=WORK`) — the open-jobs snapshot;
2. fetches jobs posted in the last 14 days (`DateType=POST`);
3. appends one line to `data/cietrade/polls.jsonl` (also on failure);
4. when the open-jobs list differs from the previous poll, appends it to
   `data/cietrade/snapshots/<date>.csv` stamped with the poll time;
5. upserts postings into `data/cietrade/posted.csv` (First Seen / Last Seen, edits flagged);
6. republishes the live feed to the gist (see *Live feed* below);
7. commits `data/cietrade/` to `main` — the runner is discarded after every run, and the
   snapshots are the one thing that cannot be re-fetched;
8. rebuilds the dashboards from the fresh log (`src/weekly_update.py --daily --no-commit`:
   cieTrade rows → validate → build) and deploys `docs/` to GitHub Pages straight from the
   runner, so week at a glance, the daily table and the Live card all move together;
9. once a day, when the committed aggregate is more than 20 hours old, also commits the
   derived record (`data/aggregated_daily_data.xlsx`, `docs/`), so the repo keeps a daily
   snapshot without a 600 KB workbook landing 144 times a day.

The workflow needs three repository secrets (Settings > Secrets and variables > Actions):
`CIETRADE_USER_ID` and `CIETRADE_API_KEY` (the same values as `cietrade.json`), and
`GIST_TOKEN`, a classic personal access token with only the `gist` scope (the built-in
`GITHUB_TOKEN` cannot edit gists). Without `GIST_TOKEN` the dashboards still deploy but the
Live card's gist is not refreshed, and the run prints a warning. A failed poll, or a build
blocked by validation, turns the run red, so GitHub's failed-workflow email is the alarm.
`TZ=America/New_York` in the workflow keeps snapshot timestamps in plant time.

Cadence: GitHub's cron has proven hours late for this repository, so each run dispatches the
next one after an 8-minute wait (the `next` job); the cron only re-seeds the chain if it
breaks. If the Actions tab shows no run in the last 20 minutes, seed it by hand:
`gh workflow run cietrade-poll.yml`.

**One writer.** With the cloud poller enabled, uninstall the Mac's poller and daily job
(`scripts/uninstall_schedule.sh cietrade_poll` and `... daily_update`); a second poller
appending to the same files would make every push conflict. The Monday weekly run stays on
the Mac (Gmail workbooks, payroll) and pulls `main` (fast-forward only) before it builds.
Running the poller by hand on the Mac is still fine for a `--dry-run`. `--rebuild` (pilot
page + OneDrive copy) is a Mac-only convenience; the public dashboard has replaced it.

End of Shift hours: each run also pulls the web app's log sheet (`src/labor_sheet.py`) into
`data/labor_entries.xlsx` before building, when the secrets `SHEETS_TOKEN_JSON` (the
read-only Sheets token from a one-time `python3 src/labor_sheet.py --dry-run` on the Mac,
`~/.config/walton/sheets_token.json`) and `WALTON_LABOR_SHEET_ID` (the sheet's ID) exist.
Without them, or if Sheets is down, the run warns and rows carry output only, as before.
The landing file itself stays out of the repo (operator names). See `setup/LABOR_CAPTURE.md`.

`--backfill-posted` fetches every posting since 2026-01-01 (run once; done 2026-09-17).
`--dry-run` calls the API and writes nothing.

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

`src/weekly_update.py --daily --no-commit` runs after every cloud poll: cieTrade rows →
validate → build dashboards, and the workflow deploys the result to
<https://zabdulla.github.io/walton_production/>. The Monday weekly run on the Mac does the
same plus the Gmail fetch and payroll, then commits and pushes.

## Weekly routine for the office

- **Post jobs once a week** (any day). The API sees the posting within 10 minutes; nothing else
  changes. Daily figures were already exact from the polls.
- **Open next week's jobs** as before (one per machine and shift). The 1st-shift job is opened
  after the shift ends, which the model understands.
- **Tag bales during the shift they come from.** Anything entered late lands in the window in
  which it was entered. Weekly totals are unaffected.
- Deploy the End of Shift app (`setup/LABOR_CAPTURE.md`) so hours and operators flow in; until
  then rows carry output only and the validation report warns about output without hours.

## Live feed — `src/cietrade_live.py`

After every poll the poller rewrites `data/cietrade/live.json` (gitignored) and republishes
it to the gist named by `config.LIVE_GIST_ID` through `gh api` (in Actions `GH_TOKEN` is the
`GIST_TOKEN` secret; on a Mac `gh` is already signed in). The dashboard's **Live** card embeds the copy present at build time and then fetches
the gist on load and every five minutes, so the public page is current without a redeploy.

A change is a job's gain between two polls; the window starts at the previous poll (or at
the job's creation for its first sighting), so every entry is placed within one polling
interval. The card shows today's pounds per machine (6 AM to 6 AM), a cumulative curve
with shift bands and yesterday's curve for comparison, and the last 20 changes; a machine
that produced this shift but has been quiet for `LIVE_QUIET_MINUTES` is flagged. Corrections
(a quantity going down) are shown as removed pounds.

To recreate the gist: `gh gist create --filename live.json data/cietrade/live.json`, then put
the id in `config.LIVE_GIST_ID`.

## Inbound dashboard — `src/cietrade_inbound.py`

In cieTrade the purchase order is the promise and the **PR worksheet is the receipt**, with the
receiver's gross / tare / net per grade line. `TradingInquiry` (Status=ALL, DateType=SHIP)
returns those lines with the PO number and date, supplier, receiving warehouse, product, units,
price and posting status; `ListOrders` (Source=PO, Status=OPEN) gives the orders still expected.

    python3 src/cietrade_inbound.py                 # -> reports/inbound.html (local only, gitignored)
    python3 src/cietrade_inbound.py --days 60 --out ~/Desktop/inbound.html

The page filters by warehouse, department, window and open-PO age, and shows weekly lbs by
supplier, loads per day, grade mix, a supplier table (loads, avg load, tare %, PO-to-receipt
lead time, unposted), the inbound due board (open POs with nothing received) and the receipts
log with data flags (no tare, no gross, net > gross, EA/KG units, no PO, received before the
PO). Bulk "Processing Input" adjustment lines booked as PR lines are excluded. The page holds
supplier names and volumes, so it is not copied into `docs/` (the public site) by default.
The full endpoint reference is `explorations/cietrade_ops/API_REFERENCE.md`.

## Daily digest — `src/daily_digest.py`

One email about yesterday, sent by the cloud workflow once a day after 06:00 plant time:
pounds by machine and shift against the 4-week average for that weekday, the week so far
against target pace, what the End of Shift forms reported (hours per machine, pounds per
machine hour, downtime, comments, shift notes, missing shifts), the weekly trend as an inline
chart, and a link to the dashboard.

    python3 src/daily_digest.py                        # reports/digest/<date>.html + .png, no send
    python3 src/daily_digest.py --send --to me@x.com   # send now through the Gmail API

Setup, once, on the Mac (the Gmail OAuth client already exists for the weekly fetch):

    python3 src/daily_digest.py --authorize            # browser consent for gmail.send only -> ~/.config/walton/gmail_send_token.json
    gh secret set GMAIL_SEND_TOKEN_JSON < ~/.config/walton/gmail_send_token.json
    gh secret set DIGEST_TO --body "you@plusmaterials.com,them@plusmaterials.com"

`data/digest_state.json` (committed) records the last send so the ten-minute chain sends
exactly once a day; the repository variable `DIGEST_SEND_HOUR` moves the hour.

## Operations

    gh workflow run cietrade-poll.yml             # poll + rebuild + deploy now; runs every 10 min on its own
    gh run list --workflow cietrade-poll.yml -L 5 # recent runs and whether they failed
    tail -1 data/cietrade/polls.jsonl             # last poll on main (after git pull)
    scripts/uninstall_schedule.sh cietrade_poll   # the Mac poller and daily job must stay off
    scripts/uninstall_schedule.sh daily_update    #   while the cloud workflow runs
    python3 src/weekly_update.py --daily --no-commit   # what the cloud builds, locally, without committing
    python3 src/cietrade_daily.py --dry-run       # what the next daily run would write
    python3 explorations/cietrade_pilot/build.py  # rebuild the pilot page by hand
