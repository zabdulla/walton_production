# End-of-Shift labor capture

Supervisors record machine hours, crew hours, operators and comments per machine on a
printed End of Shift sheet. This is the only source for labor data — cieTrade has the
weights, but not the people or the hours. Two routes bring it into the pipeline; both
land in `data/labor_entries.xlsx` (gitignored) in one schema, so downstream code never
cares which route a row came from:

| Column | Meaning |
|---|---|
| `Date`, `Shift` | Production day and `1st`/`2nd`/`3rd` |
| `Machine_Name` | Canonical name (`config.SHIFT_FORM_MACHINE_MAP` maps the sheet's row labels) |
| `Machine_Hours`, `Man_Hours` | As written; `Man_Hours` is the **crew total** |
| `Operator` | Comma-separated first names; bracketed material is split out into `Material` |
| `Downtime_Minutes`, `Downtime_Reason` | Structured downtime from the web app (the paper sheet buries it in the operator cell: "1 hour down") |
| `Submitted_By` | Who filed the report |
| `Comment` | The row's comment; review reasons are appended as `REVIEW: …` |
| `Source` | `form` or `image:<file>` — a re-submission replaces the earlier row from the same source |
| `Confidence`, `Needs_Review` | 1.0 / false for the form; per-row from the extractor for photos |

A second sheet, `shift_notes`, holds writing outside the grid ("Steven A. was
unloading / dumping trash") — labor that belongs to the shift, not a machine.

`src/cietrade_daily.py` reads this file: a (date, shift, machine) row with an entry gets its
hours, crew, operators, material and comments from here, next to the cieTrade pounds. The
cloud poll workflow (`.github/workflows/cietrade-poll.yml`) pulls the web app's sheet before
every rebuild, so a report filed on a phone is on the dashboard within ten minutes. It needs
two repository secrets, made once on the Mac after the app is deployed (step 5 below):

```bash
python3 src/labor_sheet.py --dry-run                                   # browser consent once -> sheets_token.json
gh secret set SHEETS_TOKEN_JSON < ~/.config/walton/sheets_token.json
gh secret set WALTON_LABOR_SHEET_ID --body "<spreadsheet id>"
```

## Route 1 — the End of Shift web app (target state)

`scripts/end_of_shift_app/` is a phone-first page that mirrors the paper sheet: date
(defaults to today, or last night for a 3rd-shift report filed before 6 AM), shift
(pre-picked from the clock), the submitter's name (remembered on the device), then one
card per machine in the paper's order. A machine card stays collapsed as "Didn't run"
until its switch is turned on — only the machines that ran need any typing. Each open
card takes machine hours, total man hours (auto-suggested as hours × crew, editable),
operators (one-tap chips from the `Operators` tab plus free text), material (chips),
**downtime minutes + reason**, and comments. Shift notes go in one box at the bottom.
Review → Submit, with a warning if that date + shift was already sent. Drafts survive a
closed browser; a failed send keeps the entries on the phone.

Deploy once (about five minutes):

1. <https://script.google.com> → New project → paste `Code.gs` over the default file and
   add a file named `Index.html` with the page.
2. Deploy → New deployment → Web app → Execute as **Me**, Who has access **Anyone**.
   Copy the URL; that link (or a QR code of it) is what supervisors open.
3. Open it once yourself — that creates the spreadsheet **Walton End of Shift (log)** in
   your Drive with tabs `Entries`, `Submissions`, `Operators`, `Materials`. Type the
   crew's first names into `Operators`; they become the chips.
4. Save the spreadsheet ID for the reader:
   ```json
   // ~/.config/walton/labor_sheet.json
   {"spreadsheet_id": "<id from the sheet URL>", "range": "Entries"}
   ```
5. Pull: `python3 src/labor_sheet.py --dry-run`, then without the flag to land rows.
   Shift notes are read from the `Submissions` tab automatically.
6. Every submission emails a few-line summary (machines, hours, operators, downtime, shift
   notes) to the account the app runs as, or to the script property `NOTIFY_EMAIL`. After
   pasting a new `Code.gs`, run `sendTestNotification` once in the editor (it asks for the
   mail permission and sends a sample), then Deploy → Manage deployments → New version.
7. Share the short link <https://zabdulla.github.io/walton_production/shift> (or print
   `docs/shift/qr.png`); `docs/shift/index.html` forwards to the web app's URL, so a
   redeploy only means editing that one file.

Why one page instead of the earlier one-submission-per-machine Form: the supervisors
already think in the paper sheet's shape, and the machines that didn't run cost nothing
here. The Form (`scripts/create_labor_form.gs`) remains as a fallback; the reader
accepts either.

## Route 1b — the Google Form (fallback)

One submission per machine per shift, phone-first. `scripts/create_labor_form.gs` builds
the form and its response spreadsheet:

1. Open <https://script.google.com>, New project, paste the file over `Code.gs`, Run
   `createForm`, approve the prompt.
2. View → Logs shows the **form link** (share with supervisors; consider one pre-filled
   link per shift from the form's ⋮ → *Get pre-filled link*) and the **spreadsheet ID**.
3. Save the ID for the reader:
   ```json
   // ~/.config/walton/labor_sheet.json
   {"spreadsheet_id": "<id from the log>", "range": "Form Responses 1"}
   ```
4. Pull responses (first run opens a browser once for read-only Sheets access; the token
   is kept apart from the Gmail one):
   ```bash
   python3 src/labor_sheet.py --dry-run     # look first
   python3 src/labor_sheet.py               # land the rows
   ```

Why a Form and not a custom page: zero hosting, works on any phone, responses already
live in a Sheet the pipeline can read with the Google plumbing it has. Why one submission
per machine: only 3–4 machines run per shift, so it is faster than a 40-field grid, and
each answer stays short enough to thumb in. If supervisors find the repeat-submit
annoying, an Apps Script web app with the paper layout is the upgrade path — same
sheet, same reader.

## Route 2 — photographed sheets (bridge, and history)

Until every supervisor is on the form, photos still arrive. `src/shift_report_ocr.py`
reads them with Claude's vision into the same landing file.

- **Email convention:** photos are picked up from messages whose subject contains
  **End of Shift** (`shift_report_ocr.DEFAULT_QUERY`); forward or send them with that
  subject.
- **Credentials:** `pip install anthropic`, then either `export ANTHROPIC_API_KEY=…` or
  `ant auth login`. The extractor uses `claude-opus-5`.
- ```bash
  python3 src/shift_report_ocr.py fetch                       # Gmail -> data/shift_reports/
  python3 src/shift_report_ocr.py extract --image photo.jpg --dry-run
  python3 src/shift_report_ocr.py run                         # fetch + extract new photos
  ```
- Each photo gets a `.json` sidecar with the raw reading; re-running skips photos that
  already have one. Rows below 0.7 confidence, unknown row labels, or an ambiguous
  date are landed **flagged** (`Needs_Review`, with the reason in `Comment`) — the point
  is that a person checks three rows, not the whole sheet.

What the two sample photos taught the prompt: material is written in brackets after the
names; blank rows mean the machine did not run; margin notes matter; and handwritten
dates are the weak spot (the 3rd-shift sample reads as 9/3 or 8/3). Accuracy on real
volume is unmeasured until photos flow — the fixtures in `tests/fixtures/` are
hand transcriptions of those two sheets and pin the normalisation, not the model.

## Tests

`tests/test_labor_capture.py` covers row-label mapping for both supervisors' templates,
bracket handling, the form reader, review flagging, and the landing file's replace-on-
resubmit behaviour — none of it needs network or credentials.
