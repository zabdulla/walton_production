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
cloud poll workflow (`.github/workflows/cietrade-poll.yml`) pulls the web app's sheet (or, with the repository
variable `EOS_SOURCE=microsoft`, the Excel log in SharePoint) before every rebuild, so a report filed on a phone is on the dashboard within ten minutes. It needs
two repository secrets, made once on the Mac after the app is deployed (step 5 below):

```bash
python3 src/labor_sheet.py --dry-run                                   # browser consent once -> sheets_token.json
gh secret set SHEETS_TOKEN_JSON < ~/.config/walton/sheets_token.json
gh secret set WALTON_LABOR_SHEET_ID --body "<spreadsheet id>"
```

## Route 1 on Microsoft 365 — the same page, a Power Automate flow, an Excel log

The target state after the move off Google. `docs/eos/index.html` is the same phone
page as the Apps Script one (date and shift from the clock, one card per machine, chips,
drafts kept on the phone), served from GitHub Pages. Each report is posted to a Power
Automate flow that appends the rows to the Excel log in SharePoint and emails the
summary. The reader (`src/labor_sheet.py` with `EOS_SOURCE=microsoft`) downloads that
workbook before every rebuild, so a report is on the dashboard within ten minutes, as
before. Nothing in the pipeline after `data/labor_entries.xlsx` changes.

Set up once (about twenty minutes, in your Microsoft 365 account):

1. **The log workbook.** In the SharePoint site for operations, create
   `Walton End of Shift (log).xlsx` with two sheets, each holding one formatted table
   (Insert → Table, "My table has headers"): sheet `Entries`, table `Entries`, headings
   `Timestamp, Submission ID, Date, Shift, Machine, Machine hours operated, Total man
   hours, Operator(s), Material run, Downtime minutes, Downtime reason, Comments,
   Submitted by`; sheet `Submissions`, table `Submissions`, headings `Timestamp,
   Submission ID, Date, Shift, Submitted by, Shift notes, Machines reported, Replaces
   earlier`. Copy the file's sharing link for `EOS_WORKBOOK_URL`
   (setup/MICROSOFT_365_SETUP.md).
2. **The flow** (make.powerautomate.com → Create → Instant cloud flow → *When an HTTP
   request is received*):
   - Trigger: method **POST**, *Who can trigger the flow*: **Anyone** (the URL is the key,
     treat it like one; the Apps Script app worked the same way). No request schema needed.
   - **Compose** `req`: expression `json(string(triggerBody()))` (the page sends the JSON
     as plain text so phones can post without a CORS preflight).
   - **Apply to each** over `outputs('req')?['entries']`, containing **Add a row into a
     table** (Excel Online (Business)): the log workbook, table `Entries`, each column
     mapped to `items('Apply_to_each')?['<column heading>']`.
   - **Add a row into a table**: table `Submissions`, columns from
     `outputs('req')?['submission']?['<column heading>']`.
   - **Send an email (V2)** (Office 365 Outlook): To the people who should hear about
     every report (you, Thomas, Tim), Subject `outputs('req')?['notice']?['subject']`,
     Body `outputs('req')?['notice']?['body']` (switch the body editor to plain text /
     code view so the line breaks survive).
   - **Response**: status 200, **Headers** `Access-Control-Allow-Origin: *` and
     `Content-Type: application/json`, Body `{"id": "@{outputs('req')?['id']}"}`.
     The header is what lets the page read the confirmation; without it the supervisor
     sees "no answer from the flow" even though the rows were saved.
   - Save; copy the trigger's **HTTP POST URL**.
3. **Point the page at the flow**: put that URL in `FLOW_URL` at the top of
   `docs/eos/index.html` and commit. Open the page: the preview banner is gone, send a
   test report, check the two tables and the email.
4. **Switch the reader**: repository secrets `GRAPH_*` and `EOS_WORKBOOK_URL`, then
   `gh variable set EOS_SOURCE --body microsoft`. The old Sheet is no longer read.
5. **Move the floor's link**: make `docs/shift/index.html` forward to `../eos/` instead
   of the Apps Script URL. The short link and the printed QR code keep working.

The chip lists (operators, materials, downtime reasons) live at the top of the page
now, next to `FLOW_URL`, since there is no sheet to read them from. The flow runs
whether or not anyone's computer is on.

## Route 1 — the End of Shift web app on Google (until the move completes)

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
   notes) to the account the app runs as, or to the script property `NOTIFY_EMAIL`
   (one address or a comma-separated list, which replaces the default, so include your own
   address). Set it in the web app's own project: <https://script.google.com> → the project
   holding this `Code.gs` → Project Settings → Script Properties, next to `EOS_SPREADSHEET_ID`.
   The script reached from the spreadsheet's Extensions → Apps Script menu is a different,
   empty project. Changing the property needs no redeploy; `sendTestNotification` confirms
   the recipients in its log line. After
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
