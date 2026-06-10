# Walton Production — Codebase Review & Improvement Plan

**Date:** 2026-06-09 · **Scope:** full repo (src/, tests/, scripts/, .github/, data/, docs/) · **Test baseline:** 141 passed, 3 skipped

> **Execution status (2026-06-09, this branch):** Phases 0–3 and most of Phase 4
> are IMPLEMENTED here (test suite now: 176 passed, 4 skipped).
> Fixed: bugs D1–D12; roster untracked from git; ci.yml (PR/branch test gate),
> heartbeat.yml, requirements.lock, hardened push logic, content-asserting smoke
> tests, stale-data banners, missed-report alarm, weekly output anomaly
> detection, webhook (Slack) notifications, downtime impact report, mobile
> table scrolling. Cloud migration path documented in docs/WEEKLY_AUTOMATION.md.
> **Still manual (repo admin):** make the repo private + Pages strategy (§6.3),
> purge roster from git history (§0 step 3), enable branch protection on main
> requiring the "CI / test" check.
> **Remaining code work (do as separate PRs):** target RAG KPIs, custom
> date-range picker, incremental aggregation, Actions-cron migration.
> The dashboard_common.py refactor is DONE: shared CSS base + JS helpers
> extracted, all five builders migrated, each verified byte-identical against
> pre-refactor output (payroll via synthetic render snapshot; index modulo its
> pre-existing random table ids). Further consolidation (controls/table CSS,
> range-button styles) can continue incrementally in dashboard_common.py.

> ⚠️ **Read section 0 first.** This repository is currently **public on GitHub**, and this
> review discusses its weaknesses. Consider making the repo private before merging this
> document (or anything else) to `main`.

---

## 0. The One Critical Finding (act on this before anything else)

**The GitHub repository is PUBLIC** (`visibility: public`, GitHub Pages enabled), and
`data/employee_roster.json` is committed to it. That file contains:

- Full names of ~20+ employees, plus a list of operator first names (`unmatched_production_operators`)
- Roles, shift assignments, and **hourly pay rates** for some employees
- The default labor rate (`$25/hr`) is also in `src/config.py`, and the published
  dashboards (`docs/index.html`, `docs/daily.html`) expose production volumes, costs
  per pound, and operator names to anyone on the internet.

**Why it matters:** wage disclosure is an HR/legal liability; production cost data is
competitive intelligence for anyone who knows the facility; operator names attached to
performance data is an employee-relations problem.

**Fix (in order, ~1 hour):**
1. **Make the repo private** (Settings → Danger Zone → Change visibility). GitHub Pages
   on a free plan won't serve from a private repo — see the deployment plan (§6) for
   options (GitHub Pro/Team keeps Pages working, or move dashboards elsewhere).
2. Move `employee_roster.json` out of git: add to `.gitignore`, load from a local path
   or a GitHub Actions secret, and commit a `employee_roster.example.json` with fake data.
3. **Purge history**: `git filter-repo --path data/employee_roster.json --invert-paths`
   (the file remains in every old commit otherwise). Since the repo is public, assume
   the data has been crawled; treat the purge as damage limitation, not erasure.
4. Audit what else the public dashboards reveal and decide deliberately what should be
   public (see §1, User perspective).

---

## 1. Review as a **User** (the people who open the dashboards)

What exists today: a weekly trends dashboard (`docs/index.html`, ~1.9 MB), a daily
dashboard (`docs/daily.html`, ~760 KB), and three local-only reports (profit, payroll,
operator) in `reports/`.

**What works well:** self-contained single-file dashboards that need no server; sensible
KPIs (output, output/hr, cost/lb); shift comparison; anonymize toggle on payroll.

**Pain points:**

| # | Issue | Where |
|---|-------|-------|
| U1 | No navigation between dashboards — daily links to index, but nothing links back or to the local reports | all builders |
| U2 | 1.9 MB initial load; slow on mobile, no loading indicator | `build_interactive_dashboard.py` |
| U3 | Mobile layout breaks under ~480px: fixed-width 280px selects overflow, tables don't scroll horizontally | `build_interactive_dashboard.py:814` |
| U4 | Payroll "anonymize" is cosmetic — full names remain in the embedded JSON (`PERIODS`); View Source defeats it | `build_payroll_dashboard.py:280` |
| U5 | A supervisor note containing `<`, `>` or `&` renders broken (and is an XSS vector, see D1) | `build_daily_dashboard.py:938` |
| U6 | Profit dashboard "unaccounted labor" explanation is too technical ("gap of 45h divided by 360h reported") | `build_profit_dashboard.py:261` |
| U7 | Only preset ranges (last N weeks); no custom date-range picker | interactive dashboard |
| U8 | Inconsistent date formats between views (`3/5` vs `03/05`) | `build_daily_dashboard.py:1059` |

---

## 2. Review as a **Senior Full-Stack Developer**

### 2.1 Bugs (with fixes)

**D1 — XSS / broken rendering via supervisor notes (HIGH).**
`build_daily_dashboard.py:938` interpolates `${n.note}` (and machine names) into
`innerHTML` with no escaping; verified no `escapeHtml` exists anywhere in `src/`. Notes
are free text typed by supervisors and the page is on the public internet.
*Fix:* add one JS `escapeHtml()` helper to the shared template and wrap every
interpolated string field across all five builders. ~30 min.

**D2 — Payroll names embedded despite anonymize default (HIGH).**
`build_payroll_dashboard.py` embeds the full named dataset and hides names with CSS.
*Fix:* embed anonymized data by default; gate the named JSON behind an explicit build
flag (`--with-names`) so the default artifact is safe to share. ~1 hr.

**D3 — `Cost_per_Pound` is 0 when output is 0 (HIGH, data correctness).**
`aggregate_daily_data.py:212` and `build_interactive_dashboard.py:645` use
`0` / `.fillna(0)` for zero-output rows, while `build_interactive_dashboard.py:131`
correctly uses `pd.NA`. A $0.00/lb cost on a downtime day reads as "free production"
and drags weekly averages down.
*Fix:* standardize on `pd.NA` and exclude NA from averages; render "—" in tables.

**D4 — Dedup key can silently drop legitimate rows (HIGH, data correctness).**
`aggregate_daily_data.py:315` dedups on `[Date, Shift, Machine_Name, Output_Product,
Actual_Output]` — `Operator` and hours are excluded. Two operators producing the same
round number (e.g. 2,000 lbs) on the same machine/shift/day collapse to one row, losing
output and man-hours.
*Fix:* add `Operator`, `Machine_Hours`, `Man_Hours` to the subset; log what was dropped.

**D5 — Fetch errors don't fail the fetch step (HIGH, pipeline integrity).**
`weekly_update.py:159-182` catches `HttpError` and prints it but leaves `ok=True`, so
aggregation runs on incomplete data and the run reports success.
*Fix:* set `result["ok"] = False` in the handler, mirroring `step_build_dashboards`.

**D6 — Snapshot restore covers daily data but not payroll (MEDIUM).**
`weekly_update.py:504-527` restores `aggregated_daily_data.xlsx` when validation blocks,
but `aggregated_payroll.xlsx` is left at the new state — the two files desync.
*Fix:* snapshot and restore both, or restore nothing and mark the run quarantined.

**D7 — Silent date "auto-correction" rewrites data (MEDIUM).**
`aggregate_daily_data.py:162-181` swaps month/day or infers the date from the sheet name
when the cell date falls outside the filename week. Clever, but it mutates source-of-truth
data with only a log line; a mis-swap (e.g. 04-05 vs 05-04, both plausible) is invisible.
*Fix:* keep the correction but record it in a `Date_Corrected` flag column and surface
corrected-row counts in `validate_data.py` warnings.

**D8 — PDF handles leak on parse failure (MEDIUM).**
`parse_payroll_pdf.py:128-135`: `doc.close()` is skipped on exception. Repeated failed
runs can exhaust file handles. *Fix:* `with pymupdf.open(...) as doc:` or `try/finally`.

**D9 — Unvalidated calendar dates from email subjects (MEDIUM).**
`fetch_emails.py:246-250` regex-parses `M/D/YY` and formats without checking it's a real
date (2/30/26 passes). Downstream `_parse_date_range` then raises and the file is skipped
with only a warning. *Fix:* `datetime.strptime` validation at parse time; reject loudly.

**D10 — Timezone shift in calendar rendering (LOW-MEDIUM).**
`build_daily_dashboard.py:821` uses `toISOString()` (UTC) for day-cell keys; viewers west
of UTC can see entries on the wrong day. *Fix:* build `YYYY-MM-DD` from local getters.

**D11 — Failed payroll PDFs retry forever (LOW).**
`parse_payroll_pdf.py:288-312` logs failures but leaves the file in place to fail again
every week. *Fix:* move to `data/payroll_pdfs/failed/` and list them in run summary.

**D12 — Log timestamp format coupling (LOW).**
`last_run_status.py` regex-parses timestamps that `weekly_update.py:99-102` produces via
default `%(asctime)s`. *Fix:* pin `datefmt="%Y-%m-%d %H:%M:%S"` explicitly.

### 2.2 Code quality

- **~700 lines of CSS/HTML/JS template duplicated across five builders.** A style or
  escaping fix must be made five times. Extract `src/dashboard_common.py` with
  `base_html(title, body, scripts)`, shared CSS, `escapeHtml`, and number formatters.
  This is the highest-leverage refactor in the repo and makes D1's fix a one-place change.
- **Silent `except: pass/continue`** in `build_profit_dashboard.py:590` (forecast) and
  `build_payroll_dashboard.py:69-74` (period skips) — log at WARNING minimum.
- **Rename-on-load column mapping** (`build_interactive_dashboard.py:70-85`) converts
  snake_case to Title Case at read time; normalize once at aggregation instead.
- Unused import `_fmt_num` in `build_operator_dashboard.py:28`.
- `requirements.txt` uses ranges only — CI and the Mac can run different pandas minors.
  Add a `requirements.lock` via `pip-compile` for reproducible installs.

### 2.3 Tests & CI (what "never breaks" currently rests on)

The good news: 141 tests pass; `atomic.py`, `validate_data.py`, `parse_payroll_pdf.py`
and aggregation helpers are genuinely well covered; CI runs the full suite **before**
building, and a failing step does stop the workflow (no `continue-on-error`).

The gaps:

1. **CI only triggers on push to `main`** — there is no PR validation, so the first time
   broken code meets the tests is *after* it lands on main.
2. **`git pull --rebase origin main || true`** in the workflow swallows rebase failures
   and then pushes anyway — conflict resolution by coin flip when the Mac's launchd job
   and Actions both push (`.github/workflows/build-dashboard.yml`, last step).
3. **Five modules have no tests at all:** all of `build_daily_dashboard.py`,
   `build_payroll_dashboard.py`, `build_profit_dashboard.py`, `build_operator_dashboard.py`
   beyond smoke level, plus `rename_reports.py` and `last_run_status.py`.
4. **Smoke tests check size and tag-balance only** — a dashboard with wrong numbers but
   valid HTML passes. Add content assertions (expected machine names, a known week's
   total embedded in the JSON payload).
5. **No branch protection** on `main`.

---

## 3. Review as the **GM running the recycling facility**

My questions are: *can I trust the numbers, will the report be there Monday, and does it
tell me what to do about it?*

**Trust the numbers?** Mostly, with caveats I'd want fixed: zero-output days showing
$0.00/lb cost (D3) understates my real cost per pound; the dedup bug (D4) can quietly
erase a shift's production; silent date correction (D7) means a misfiled day might be
charted on the wrong date with no flag I can see. Validation gating (`validate_data.py`)
blocking on duplicates/unmapped products is genuinely good — but anomaly *warnings*
(50,000 lbs in one shift) don't block and nobody is forced to look at them.

**Will it be there Monday?** The whole pipeline runs as launchd on one person's Mac
(`scripts/com.walton.weekly_update.plist`). If that Mac is asleep at Monday noon, off, or
its owner leaves the company, reporting stops with no alert. There is no "the pipeline
didn't run" alarm — only alerts when it runs and fails.

**Does it tell me what to do?** The data to answer GM questions is already collected but
not surfaced:

| Feature | Why I want it | Implementation sketch |
|---|---|---|
| **Downtime cost report** | Notes are already categorized (`downtime`/`material`/`quality` in `config.py:195`) but never priced | Join downtime-note days against the machine's rolling-average output; report "lost lbs × margin preset" per machine per week. ~1 day, new section in daily dashboard. |
| **Missed-report alarm** | Carl forgets a week → silent data gap | In `validate_data.py`, fail-warn when any of the 3 shifts is missing for the latest week; email/Slack it (see §5). |
| **Output vs target trend** | Targets exist (`MACHINE_WEEKLY_OUTPUT_TARGETS`) but I want misses flagged, not just charted | Add a "weeks below target (last 8)" KPI per machine with red/amber/green. |
| **Labor reconciliation alert** | Payroll-vs-production hour gap is computed for the profit dashboard already | Threshold it: if unaccounted hours > 15%, banner on dashboard + notification. |
| **Anomaly detection** | Catch sensor/typo outliers and machine failures early | Rolling mean ± 2σ per machine in `validate_data.py`; flag in dashboard, include in run summary. |
| **Stale-data banner** | I should never unknowingly read a 3-week-old dashboard | Embed build date (already present) and add JS: if `today - latest_data > 9 days`, show a red banner. ~30 min. |

---

## 4. Review as the **CEO**

1. **We are publishing wages and unit economics to the open internet** (§0). Cost per
   pound, volumes by product line, machine utilization — a buyer or competitor can
   reconstruct our margins from the public Pages site. Fix this week.
2. **Key-person + single-machine risk.** Revenue-relevant reporting depends on one Mac,
   one Gmail account (`carl@plusmaterials.com` hardcoded in `fetch_emails.py:77`), and
   one person who understands the pipeline. Mitigation is cheap: move the schedule to
   GitHub Actions cron (§6 Phase 3) and document runbooks (partially done in
   `docs/WEEKLY_AUTOMATION.md` — good start).
3. **Decision risk from data quality.** If we're setting prices or staffing from
   cost-per-pound, bugs D3/D4 bias that number. The validation framework is the right
   instinct — fund the two days it takes to close the gaps.
4. **The codebase is an asset, not a liability.** Honest assessment: for an internal
   tool this is *above-average* engineering (atomic writes with snapshots, validation
   gating, real tests, CI). The investment to make it durable is small — roughly 2–3
   developer-weeks across the phases below — versus the cost of a wrong pricing decision
   or a wage-data complaint.

---

## 5. Detailed Improvement Plan (phased, each phase independently shippable)

### Phase 0 — Stop the bleeding (Day 1, ~2 hrs)
- [ ] Make repo private; decide Pages strategy (§6).
- [ ] Remove `employee_roster.json` from tracking; add example file; purge history.
- [ ] Verify no other PII in `git log` (`git log --all --diff-filter=A --name-only | sort -u`).

### Phase 1 — Correctness fixes (Week 1, ~3 days)
- [ ] D1 XSS escaping (do together with the shared-template refactor below).
- [ ] D3 cost-per-pound NA handling + D4 dedup key — **add regression tests first**, then fix.
- [ ] D5 fetch step failure propagation; D6 payroll snapshot restore; D8 PDF handle leak.
- [ ] D2 payroll anonymization at the data layer.
- Acceptance: full suite green; golden-file test asserting a known week's cost/lb.

### Phase 2 — Never-breaks infrastructure (Week 2, ~3 days)
- [ ] New `ci.yml`: run pytest on **every PR and every push to any branch**.
- [ ] Branch protection on `main`: require the CI check + 1 review; no force-push.
- [ ] Replace `git pull --rebase || true` with retry loop that **fails** on conflict.
- [ ] `requirements.lock` (pip-compile); CI installs from lock.
- [ ] Strengthen smoke tests: assert machine names present, row counts > 0, build date
      is recent, embedded JSON parses.
- [ ] Unit tests for the five untested modules (`prepare_daily_summary`,
      `explode_operators` edge cases, payroll period math) — target the *math*, not the HTML.
- [ ] Pipeline heartbeat: scheduled Actions workflow that opens an issue if the last
      data commit is older than 8 days.

### Phase 3 — De-risk operations (Week 3, ~4 days)
- [ ] Extract `src/dashboard_common.py` (shared CSS/JS/template + escaping + formatters);
      migrate builders one at a time, smoke tests after each.
- [ ] D7 date-correction flag column + validation surfacing; D9 date validation;
      D11 failed-PDF quarantine; D12 log format pin.
- [ ] Notifications: Slack webhook or plain email on validation block / missed run
      (webhook URL from env var, never committed).
- [ ] Migrate the weekly schedule from launchd to GitHub Actions cron with Gmail token
      stored as an encrypted repo secret; keep launchd as documented fallback.

### Phase 4 — Features (Week 4+, prioritized by GM value)
1. Stale-data banner (30 min) → 2. Missed-report alarm (½ day) → 3. Downtime cost
   report (1 day) → 4. Anomaly detection (1 day) → 5. Target RAG KPIs (½ day) →
   6. Mobile CSS + nav header (½ day) → 7. Custom date-range picker (½ day) →
   8. Incremental aggregation with file-hash manifest (1 day, perf only).

---

## 6. Deployment Plan

**Principle:** `main` is production (Pages serves from it), so nothing reaches `main`
without passing CI, and every release is reversible.

### 6.1 Environments
| Stage | What | How |
|---|---|---|
| Dev | feature branches | CI (pytest + dashboard build + smoke) on every push |
| Staging | `staging` branch | Same workflow builds dashboards into an artifact; reviewer downloads/preview before merge. (Alternative: deploy Pages preview from a `docs-staging/` path.) |
| Prod | `main` | Existing build-dashboard workflow, hardened per Phase 2 |

### 6.2 Release procedure (per phase above)
1. Branch from `main`; implement; CI green.
2. **Data-affecting changes** (Phase 1): regenerate `aggregated_daily_data.xlsx` on the
   branch, diff row counts and weekly totals against `main`'s copy
   (`validate_data.py` already provides the comparison primitives), attach the diff to the PR.
3. Merge → Actions rebuilds dashboards → verify the live page (spot-check one known
   week's numbers, check browser console for JS errors).
4. Tag each phase (`v1.1-phase1` …) so rollback is `git revert -m1 <merge>` or redeploy
   the previous tag; `data/snapshots/` + the snapshot machinery in `atomic.py` cover
   data rollback.

### 6.3 Pages strategy after going private
- **Option A (recommended):** GitHub Pro ($4/mo) or move repo to a Team org — private
  repo Pages keeps working; combine with Pages access control if on Enterprise.
- **Option B:** publish dashboards to Cloudflare Pages/Netlify behind basic auth from a
  CI step; `docs/` stays in the private repo.
- **Option C (minimum):** keep Pages public but strip operator names and cost data from
  the published dashboards; keep full versions local in `reports/`.

### 6.4 Rollout order & risk
Phase 0 has zero code risk (config/visibility only). Phase 2 lands **before** the bug
fixes in Phase 1 touch shared math if you prefer maximum safety — at minimum, write
Phase 1's regression tests before each fix (red → green). The template refactor (Phase 3)
is the riskiest change; migrate one dashboard per PR with before/after HTML diffs.

### 6.5 "Never breaks" guarantees (summary)
1. PR-level CI — broken code can't reach `main` (today it can).
2. Branch protection — no untested direct pushes, no force-push history rewrites.
3. Golden-file + content smoke tests — wrong *numbers* fail CI, not just broken HTML.
4. Locked dependencies — CI and local runs are reproducible.
5. Snapshot/rollback — `atomic.py` already guards data; tags guard code; `git revert` guards dashboards.
6. Heartbeat + notifications — silence is detected, not assumed to be success.
