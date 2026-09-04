"""Read End-of-Shift submissions from the Google Form's response sheet.

The form (created by ``scripts/create_labor_form.gs``) takes one submission per
machine per shift and writes to a linked spreadsheet. This module pulls that
sheet through the Sheets API and lands the rows in ``data/labor_entries.xlsx``
via ``labor_entries.record``.

Setup (once):
  1. Deploy the form; note the linked spreadsheet's ID (the long token in its URL).
  2. Save it: ``~/.config/walton/labor_sheet.json`` -> {"spreadsheet_id": "...", "range": "Form Responses 1"}
     (or export WALTON_LABOR_SHEET_ID).
  3. First run opens a browser once to grant read-only Sheets access; the token is
     stored separately from the Gmail token so neither login clobbers the other.

Usage:
    python3 src/labor_sheet.py                       # pull and land
    python3 src/labor_sheet.py --dry-run             # show what would land
    python3 src/labor_sheet.py --from-csv responses.csv   # a Forms CSV export, no API
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from config import WALTON_CONFIG_DIR
from labor_entries import (
    LABOR_ENTRIES_PATH, empty_entries, empty_notes, make_entry, normalize_machine,
    normalize_shift, parse_operators, record, to_number,
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CREDENTIALS_PATH = WALTON_CONFIG_DIR / "gmail_credentials.json"   # same OAuth client as Gmail
TOKEN_PATH = WALTON_CONFIG_DIR / "sheets_token.json"
SETTINGS_PATH = WALTON_CONFIG_DIR / "labor_sheet.json"
DEFAULT_RANGE = "Form Responses 1"

# Form question titles (prefix-matched on their slug) -> field
FIELD_PREFIXES = [
    ("timestamp", "timestamp"), ("date", "date"), ("shift", "shift"), ("machine", "machine"),
    ("machinehours", "machine_hours"), ("totalmanhours", "man_hours"), ("manhours", "man_hours"),
    ("operator", "operators"), ("material", "material"), ("comment", "comment"),
    ("shiftnote", "shift_note"), ("anythingelse", "shift_note"),
]


def _slug(text: object) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())


def map_columns(columns: list[str]) -> dict[str, str]:
    """Form header -> field name. Longer prefixes win ('machinehours' before 'machine')."""
    out: dict[str, str] = {}
    for col in columns:
        key = _slug(col)
        best = None
        for prefix, field in FIELD_PREFIXES:
            if key.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
                best = (prefix, field)
        if best and best[1] not in out.values():
            out[col] = best[1]
    return out


def responses_to_entries(df: pd.DataFrame, source: str = "form") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalise raw form rows into the landing schema."""
    cols = map_columns(list(df.columns))
    rows, notes = [], []
    for _, r in df.iterrows():
        get = lambda f: next((r[c] for c, fld in cols.items() if fld == f), None)
        date = pd.to_datetime(get("date"), errors="coerce")
        if pd.isna(date):
            date = pd.to_datetime(get("timestamp"), errors="coerce")
        date_s = date.strftime("%Y-%m-%d") if not pd.isna(date) else None
        shift = normalize_shift(get("shift"))
        captured = str(get("timestamp") or "")
        machine = normalize_machine(get("machine"))
        if machine is not None or to_number(get("machine_hours")) is not None:
            rows.append(make_entry(
                Date=date_s, Shift=shift, Machine_Name=machine,
                Machine_Hours=to_number(get("machine_hours")), Man_Hours=to_number(get("man_hours")),
                Operator=parse_operators(get("operators")), Material=(str(get("material")).strip() or None) if get("material") is not None and str(get("material")).strip() != "nan" else None,
                Comment=(str(get("comment")).strip() or None) if get("comment") is not None and str(get("comment")).strip() != "nan" else None,
                Source=source, Confidence=1.0,
                Needs_Review=(machine is None or date_s is None or shift is None),
                Captured_At=captured,
            ))
        note = get("shift_note")
        if note is not None and str(note).strip() and str(note).strip() != "nan":
            notes.append({"Date": date_s, "Shift": shift, "Note": str(note).strip(), "Source": source, "Captured_At": captured})
    return (pd.DataFrame(rows) if rows else empty_entries(), pd.DataFrame(notes) if notes else empty_notes())


def get_sheets_service() -> Any:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_PATH.exists():
                raise FileNotFoundError(f"OAuth client file not found at {CREDENTIALS_PATH}")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json())
        TOKEN_PATH.chmod(0o600)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_responses(service: Any, spreadsheet_id: str, sheet_range: str = DEFAULT_RANGE) -> pd.DataFrame:
    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=sheet_range, valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute().get("values", [])
    if not values:
        return pd.DataFrame()
    header, body = values[0], values[1:]
    width = len(header)
    body = [row + [None] * (width - len(row)) for row in body]
    return pd.DataFrame(body, columns=header)


def load_settings() -> tuple[str | None, str]:
    sid = os.environ.get("WALTON_LABOR_SHEET_ID")
    rng = DEFAULT_RANGE
    if SETTINGS_PATH.exists():
        cfg = json.loads(SETTINGS_PATH.read_text())
        sid = sid or cfg.get("spreadsheet_id")
        rng = cfg.get("range", rng)
    return sid, rng


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from-csv", type=Path, help="Read a Google Forms CSV export instead of the API")
    ap.add_argument("--sheet-id", help="Override the spreadsheet ID")
    ap.add_argument("--range", dest="sheet_range", help="Sheet/range to read")
    ap.add_argument("--output", type=Path, default=LABOR_ENTRIES_PATH)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.from_csv:
        raw = pd.read_csv(args.from_csv)
    else:
        sid, rng = load_settings()
        sid = args.sheet_id or sid
        rng = args.sheet_range or rng
        if not sid:
            print("No spreadsheet configured. Set WALTON_LABOR_SHEET_ID or write "
                  f"{SETTINGS_PATH} with {{\"spreadsheet_id\": ...}}", file=sys.stderr)
            return 2
        raw = read_responses(get_sheets_service(), sid, rng)
    entries, notes = responses_to_entries(raw)
    result = record(entries, notes, path=args.output, dry_run=args.dry_run)
    print(f"{'[dry-run] ' if args.dry_run else ''}{result['new_entries']} submission(s) -> "
          f"{result['total_entries']} entries on file ({result['needs_review']} need review), "
          f"{result['total_notes']} shift notes")
    if args.dry_run and len(entries):
        print(entries.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
