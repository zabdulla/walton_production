"""Shared landing zone for supervisor labor data.

Two capture routes feed it — the Google Form reader (``labor_sheet.py``) and the
End-of-Shift photo extractor (``shift_report_ocr.py``) — and both normalise to
the pipeline's canonical vocabulary here, so the eventual join with cieTrade
output needs no per-source special-casing.

Nothing in the weekly run reads this file yet.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd

from config import DATA_DIR, SHIFT_FORM_MACHINE_MAP

LABOR_ENTRIES_PATH = DATA_DIR / "labor_entries.xlsx"

ENTRY_COLUMNS = [
    "Date", "Shift", "Machine_Name", "Machine_Hours", "Man_Hours", "Operator",
    "Material", "Comment", "Source", "Confidence", "Needs_Review", "Captured_At",
]
NOTE_COLUMNS = ["Date", "Shift", "Note", "Source", "Captured_At"]
SHIFTS = ("1st", "2nd", "3rd")

# One row per (day, shift, machine) from a given source. A re-submitted form or
# a re-extracted photo replaces its earlier version rather than duplicating it.
DEDUP_KEY = ["Date", "Shift", "Machine_Name", "Source"]


def _slug(text: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())


def normalize_machine(label: object) -> str | None:
    """Map a form/sheet row label to the canonical machine name, or None."""
    key = _slug(label)
    if not key:
        return None
    if key in SHIFT_FORM_MACHINE_MAP:
        return SHIFT_FORM_MACHINE_MAP[key]
    # "New densifier (Green Max)" -> slug starts with a known key
    candidates = [k for k in SHIFT_FORM_MACHINE_MAP if key.startswith(k) and len(k) >= 6]
    if candidates:
        return SHIFT_FORM_MACHINE_MAP[max(candidates, key=len)]
    return None


def normalize_shift(value: object) -> str | None:
    """'Second Shift', '3rd shift', '2', 'first' -> '2nd' / '3rd' / '2nd' / '1st'."""
    key = _slug(value)
    if not key:
        return None
    if "third" in key or "3" in key:
        return "3rd"
    if "second" in key or "2" in key:
        return "2nd"
    if "first" in key or "1" in key:
        return "1st"
    return None


_BRACKETS = re.compile(r"[\[\(]([^\]\)]*)[\]\)]")


def split_operator_material(text: object) -> tuple[list[str], str | None]:
    """'Montez, Tevin [BOPP]' -> (['Montez', 'Tevin'], 'BOPP').

    Supervisors write the material being run in brackets or parentheses after
    the names. Anything bracketed is material; the rest is people.
    """
    raw = str(text or "")
    materials = [m.strip() for m in _BRACKETS.findall(raw) if m.strip()]
    names_part = _BRACKETS.sub(" ", raw)
    return parse_operators(names_part), (", ".join(materials) or None)


def parse_operators(text: object) -> list[str]:
    """Split a crew cell on commas, slashes, ampersands and 'and'."""
    parts = re.split(r"[,/&]|\band\b", str(text or ""))
    return [p.strip(" .\t\n") for p in parts if p and p.strip(" .\t\n")]


def to_number(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().replace(",", "")
    if not s:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None


def empty_entries() -> pd.DataFrame:
    return pd.DataFrame(columns=ENTRY_COLUMNS)


def empty_notes() -> pd.DataFrame:
    return pd.DataFrame(columns=NOTE_COLUMNS)


def make_entry(**fields: object) -> dict:
    row = {c: None for c in ENTRY_COLUMNS}
    row.update(fields)
    ops = row.get("Operator")
    if isinstance(ops, (list, tuple)):
        row["Operator"] = ", ".join(str(o) for o in ops)
    row["Needs_Review"] = bool(row.get("Needs_Review") or False)
    return row


def merge_entries(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    frames = [f for f in (existing, new) if f is not None and len(f)]
    if not frames:
        return empty_entries()
    df = pd.concat(frames, ignore_index=True)
    df = df.reindex(columns=ENTRY_COLUMNS)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=DEDUP_KEY, keep="last")
    return df.sort_values(["Date", "Shift", "Machine_Name"], kind="stable").reset_index(drop=True)


def merge_notes(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    frames = [f for f in (existing, new) if f is not None and len(f)]
    if not frames:
        return empty_notes()
    df = pd.concat(frames, ignore_index=True).reindex(columns=NOTE_COLUMNS)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df.drop_duplicates(subset=["Date", "Shift", "Note", "Source"], keep="last").reset_index(drop=True)


def load_entries(path: Path = LABOR_ENTRIES_PATH) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not Path(path).exists():
        return empty_entries(), empty_notes()
    sheets = pd.read_excel(path, sheet_name=None)
    entries = sheets.get("entries", empty_entries()).reindex(columns=ENTRY_COLUMNS)
    notes = sheets.get("shift_notes", empty_notes()).reindex(columns=NOTE_COLUMNS)
    return entries, notes


def save_entries(entries: pd.DataFrame, notes: pd.DataFrame, path: Path = LABOR_ENTRIES_PATH) -> Path:
    """Write both sheets atomically (temp file + rename)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.xlsx")
    with pd.ExcelWriter(tmp) as xw:
        entries.reindex(columns=ENTRY_COLUMNS).to_excel(xw, sheet_name="entries", index=False)
        notes.reindex(columns=NOTE_COLUMNS).to_excel(xw, sheet_name="shift_notes", index=False)
    os.replace(tmp, path)
    return path


def record(entries: pd.DataFrame, notes: pd.DataFrame | None = None,
           path: Path = LABOR_ENTRIES_PATH, dry_run: bool = False) -> dict:
    """Merge new rows into the landing file. Returns counts for the caller's summary."""
    existing_e, existing_n = load_entries(path)
    merged_e = merge_entries(existing_e, entries)
    merged_n = merge_notes(existing_n, notes if notes is not None else empty_notes())
    if not dry_run:
        save_entries(merged_e, merged_n, path)
    return {
        "new_entries": int(len(entries)) if entries is not None else 0,
        "total_entries": int(len(merged_e)),
        "needs_review": int(merged_e["Needs_Review"].fillna(False).astype(bool).sum()) if len(merged_e) else 0,
        "total_notes": int(len(merged_n)),
        "path": str(path),
    }
