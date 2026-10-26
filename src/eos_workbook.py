"""Read the End of Shift log from an Excel workbook in SharePoint or OneDrive.

The floor's form appends one row per machine to an `Entries` table and one row
per report to `Submissions`, with the same headings the Google Sheet had, so the
rows feed `labor_sheet.responses_to_entries` and `submissions_to_notes` unchanged.
The workbook is downloaded whole (a few hundred KB at most) and read with pandas;
no Excel session, no table API, nothing to lock.

Where the workbook is, one of:
  EOS_WORKBOOK_URL        the file's SharePoint sharing link (Copy link in SharePoint); read
                          through Graph's /shares endpoint
  EOS_WORKBOOK_DRIVE_ID + EOS_WORKBOOK_PATH   a drive id and the path inside it
  ~/.config/walton/eos_workbook.json          {"url": "..."} or {"drive_id": "...", "path": "..."}
"""
from __future__ import annotations

import base64
import io
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import WALTON_CONFIG_DIR  # noqa: E402

SETTINGS_PATH = WALTON_CONFIG_DIR / "eos_workbook.json"
ENTRIES_SHEET = "Entries"
SUBMISSIONS_SHEET = "Submissions"


def load_settings() -> dict[str, str]:
    cfg: dict[str, str] = {}
    if SETTINGS_PATH.exists():
        cfg = {k: str(v) for k, v in json.loads(SETTINGS_PATH.read_text()).items() if v}
    for env, key in (("EOS_WORKBOOK_URL", "url"), ("EOS_WORKBOOK_DRIVE_ID", "drive_id"), ("EOS_WORKBOOK_PATH", "path")):
        if os.environ.get(env):
            cfg[key] = os.environ[env]
    return cfg


def configured(cfg: dict[str, str] | None = None) -> bool:
    cfg = cfg if cfg is not None else load_settings()
    return bool(cfg.get("url") or (cfg.get("drive_id") and cfg.get("path")))


def share_token(url: str) -> str:
    """Graph's encoding of a sharing URL: 'u!' + base64url without padding."""
    return "u!" + base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def download_workbook(client: Any, cfg: dict[str, str] | None = None) -> bytes:
    cfg = cfg if cfg is not None else load_settings()
    if cfg.get("url"):
        return client.get_bytes(f"/shares/{share_token(cfg['url'])}/driveItem/content")
    if cfg.get("drive_id") and cfg.get("path"):
        path = cfg["path"].strip("/")
        return client.get_bytes(f"/drives/{cfg['drive_id']}/root:/{path}:/content")
    raise FileNotFoundError("End of Shift workbook not configured: set EOS_WORKBOOK_URL (or drive id + path, or "
                            f"{SETTINGS_PATH})")


def read_tables(data: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(entries, submissions) with string column names; a missing Submissions sheet is empty."""
    book = pd.read_excel(io.BytesIO(data), sheet_name=None)
    entries = book.get(ENTRIES_SHEET)
    if entries is None:
        raise ValueError(f"workbook has no '{ENTRIES_SHEET}' sheet (sheets: {', '.join(book)})")
    subs = book.get(SUBMISSIONS_SHEET, pd.DataFrame())
    entries.columns = [str(c) for c in entries.columns]
    subs.columns = [str(c) for c in subs.columns]
    return entries.dropna(how="all"), subs.dropna(how="all")


def fetch(client: Any, cfg: dict[str, str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    return read_tables(download_workbook(client, cfg))
