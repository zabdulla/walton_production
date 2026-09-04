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
| `Comment` | The row's comment; review reasons are appended as `REVIEW: …` |
| `Source` | `form` or `image:<file>` — a re-submission replaces the earlier row from the same source |
| `Confidence`, `Needs_Review` | 1.0 / false for the form; per-row from the extractor for photos |

A second sheet, `shift_notes`, holds writing outside the grid ("Steven A. was
unloading / dumping trash") — labor that belongs to the shift, not a machine.

**Nothing in the weekly run reads this file yet.** Wiring it in is the next step once
either route has a few weeks of real submissions.

## Route 1 — the Google Form (target state)

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
