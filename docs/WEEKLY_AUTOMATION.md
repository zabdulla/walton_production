# Weekly Update Automation

The full Monday-morning routine — fetch new emails, aggregate, build dashboards, commit, push — is wrapped into a single command:

```bash
python3 src/weekly_update.py
```

Schedule it to run automatically every Monday at noon with `scripts/install_schedule.sh`.

---

## What it does

```
Step 1/6: Fetch new emails (src/fetch_emails.py --all)
Step 2/6: Aggregate daily data (src/aggregate_daily_data.py)
Step 3/6: Parse payroll PDFs (src/parse_payroll_pdf.py --pdf-dir ...)
Step 4/6: Validate data quality (src/validate_data.py)
Step 5/6: Build all 5 dashboards
Step 6/6: Commit + push to GitHub (with rebase retry on GH Actions conflict)
```

Each step prints structured progress. A summary at the end shows totals.

---

## Manual usage

```bash
# Full run
python3 src/weekly_update.py

# Build only — skip the Gmail fetch (e.g., you're offline)
python3 src/weekly_update.py --no-fetch

# Build + commit but don't push
python3 src/weekly_update.py --no-push

# Dry run — show what fetch would download, then exit
python3 src/weekly_update.py --dry-run
```

---

## One-time scheduling install (macOS launchd)

Prereq: complete the [Gmail API setup](GMAIL_API_SETUP.md) first. The schedule is useless without working credentials.

```bash
scripts/install_schedule.sh
```

This:
1. Generates `~/Library/LaunchAgents/com.walton.weekly_update.plist` with your absolute project path.
2. Loads it into launchd.
3. Schedules every **Monday at 12:00 PM local time**.

The schedule survives reboots automatically. If your Mac is asleep at noon Monday, launchd will run the job as soon as it next wakes up.

### Verify it's installed

```bash
launchctl list | grep walton
# expected output: -    0    com.walton.weekly_update
```

### Test it now (without waiting for Monday)

```bash
launchctl kickstart -k gui/$(id -u)/com.walton.weekly_update
```

The job runs immediately. Watch the log live:
```bash
tail -f logs/weekly_stdout.log
```

### Check status

```bash
launchctl print gui/$(id -u)/com.walton.weekly_update | grep -E 'state|last_exit_status|run_interval'
```

### Logs

| File | Contents |
|------|----------|
| `logs/weekly_stdout.log` | Every Monday's full run output, appended |
| `logs/weekly_stderr.log` | Errors from the job's stderr |
| `logs/weekly_update.log` | Structured timestamped log written by Python (one line per command) |

The repo includes `logs/.gitkeep` so the directory exists; the log files themselves are gitignored.

### Uninstall

```bash
scripts/uninstall_schedule.sh
```

Removes the plist and unloads the job. Logs are preserved.

---

## Changing the schedule

Edit `scripts/com.walton.weekly_update.plist`, then re-run `scripts/install_schedule.sh`.

The `<key>StartCalendarInterval</key>` block controls the time:
```xml
<key>Weekday</key>
<integer>1</integer>   <!-- 0/7=Sun, 1=Mon, …, 6=Sat -->
<key>Hour</key>
<integer>9</integer>
<key>Minute</key>
<integer>0</integer>
```

For multiple times per week, replace the `<dict>` with a `<array>` of dicts. See `man launchd.plist` for full syntax.

---

## Troubleshooting

**Job runs but nothing happens / token expired**
Re-authenticate Gmail: `python3 src/fetch_emails.py --auth`

**"command not found: python3" in stderr log**
launchd has a minimal PATH. The plist already adds `/opt/homebrew/bin` and `/usr/local/bin`. If your `python3` is elsewhere, add it to the `PATH` in the `EnvironmentVariables` dict and re-run `install_schedule.sh`.

**Push fails with rejection**
The orchestrator already retries once with `git pull --rebase`. If both attempts fail (rare), check the `weekly_stderr.log` and resolve manually with `git pull --rebase origin main`, then re-run.

**Job doesn't run at the scheduled time**
- Mac was off — launchd runs missed jobs at next wake (usually).
- User wasn't logged in — `LaunchAgents` only run for the active user. To run regardless, move to `LaunchDaemons` (requires root install).

**Want to run more than once a week**
Either edit `StartCalendarInterval` to an array of dicts, or use `StartInterval` (seconds, e.g. `<integer>3600</integer>` for hourly).

---

## Notifications (webhook)

In addition to the macOS desktop notification, the orchestrator can POST to a
Slack-compatible webhook so the whole team hears about validation blocks and
pipeline failures even when nobody is at the Mac:

```bash
# Slack: create an Incoming Webhook and export its URL before the run.
# For launchd, add it to the plist's EnvironmentVariables dict.
export WALTON_WEBHOOK_URL="https://hooks.slack.com/services/T000/B000/XXXX"
```

`SLACK_WEBHOOK_URL` is accepted as an alias. Discord works via the `/slack`
suffix on its webhook URLs. If the variable is unset, the webhook step is
skipped silently; webhook failures are logged but never break the pipeline.

---

## Monitoring

Two safety nets watch the pipeline itself:

1. **Heartbeat workflow** (`.github/workflows/heartbeat.yml`) — every Tuesday,
   GitHub Actions checks when `data/aggregated_daily_data.xlsx` last changed.
   If it's more than 8 days old, it opens a GitHub issue. This catches the
   "Mac was off on Monday" failure, which is otherwise invisible.
2. **Stale-data banners** — both published dashboards display a red banner
   when their embedded data is older than expected (daily >9 days, weekly
   >13 days), so viewers can't unknowingly read stale numbers.

---

## Cloud migration path (moving off the Mac)

The single-Mac dependency is the pipeline's biggest operational risk. Moving
the schedule to GitHub Actions cron is possible but has real prerequisites —
don't flip the switch without solving these:

1. **Raw reports archive — SOLVED via incremental aggregation.** Run
   `python3 src/aggregate_daily_data.py --incremental`: weeks parsed from
   `processing_reports/` replace the matching (Week_Start, Shift) slices of
   the committed `data/aggregated_daily_data.xlsx` and everything else is
   preserved, so a cloud runner only needs the files it just fetched (the
   default full rebuild on the Mac is unchanged). A corrected re-sent file
   supersedes its old rows; re-runs are idempotent.
2. **Secrets.** `gmail_credentials.json`, `gmail_token.json`, and
   `employee_roster.json` would become GitHub Actions secrets, written to
   the runner at job start. The OAuth token refreshes itself; persist the
   refreshed token back to the secret or it will eventually expire.
3. **Repo must be private first** (it currently is not — see
   CODEBASE_REVIEW.md §0) since secrets-adjacent automation and wage-related
   data should never live in a public repo.

Until then, the launchd job remains primary and the heartbeat workflow is the
detector for missed runs.
