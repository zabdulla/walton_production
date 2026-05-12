"""
Fetch production data attachments from Gmail via the Gmail API.

Replaces the brittle Chrome-MCP click-through workflow with a stable,
scriptable, runs-without-a-browser approach.

ONE-TIME SETUP (see README at bottom of this file or docs/GMAIL_API_SETUP.md):
    1. Create a Google Cloud project, enable Gmail API
    2. Create OAuth 2.0 client credentials (Desktop app type)
    3. Download credentials JSON to ~/.config/walton/gmail_credentials.json
    4. Run: python3 src/fetch_emails.py --auth
       (opens browser for one-time consent, saves token.json)

USAGE:
    python3 src/fetch_emails.py --auth                # initial OAuth (one-time)
    python3 src/fetch_emails.py --processing-weights  # fetch latest weekly Excel files
    python3 src/fetch_emails.py --payroll             # fetch latest pay period PDFs
    python3 src/fetch_emails.py --all                 # both
    python3 src/fetch_emails.py --list                # dry-run: show what would be fetched
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

# Lazy imports — fail with friendly message if google-api-python-client missing
try:
    import httplib2
    import socket
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_httplib2 import AuthorizedHttp
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError as e:
    print(f"ERROR: Missing Google API libraries.\n"
          f"  Install with: pip install -r requirements.txt\n"
          f"  Underlying error: {e}", file=sys.stderr)
    sys.exit(1)

# Per-API-call HTTP timeout in seconds. Without this, a hung connection can
# wedge the whole job for hours. 60s is generous for any single call.
HTTP_TIMEOUT_SEC = 60

# Retry config for transient failures (5xx, timeouts)
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 5
_RETRY_STATUS = {500, 502, 503, 504}

from config import (
    PROJECT_ROOT,
    REPORTS_DIR,           # processing_reports/
    PAYROLL_PDF_DIR,       # data/payroll_pdfs/
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Auth & config
# ---------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
CONFIG_DIR = Path.home() / ".config" / "walton"
CREDENTIALS_PATH = CONFIG_DIR / "gmail_credentials.json"
TOKEN_PATH = CONFIG_DIR / "gmail_token.json"

# Email queries
PROCESSING_WEIGHTS_QUERY = (
    'from:carl@plusmaterials.com '
    'subject:"processing weights for the week of" '
    'has:attachment'
)
PAYROLL_QUERY = (
    'from:carl@plusmaterials.com '
    'filename:PayPeriod '
    'has:attachment'
)


def get_service() -> Any:
    """Authenticate and return a Gmail API service object.

    First call requires OAuth consent in browser. Subsequent calls use the
    saved token.json (auto-refreshed when it expires).
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    creds: Credentials | None = None

    if TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception as e:
            logger.warning(f"Could not load existing token: {e}")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing expired token...")
            creds.refresh(Request())
        else:
            if not CREDENTIALS_PATH.exists():
                raise FileNotFoundError(
                    f"OAuth credentials not found at {CREDENTIALS_PATH}\n"
                    f"See setup instructions at the top of {Path(__file__).name}"
                )
            logger.info("Running OAuth flow (browser will open)...")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_PATH), SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Persist token
        TOKEN_PATH.write_text(creds.to_json())
        TOKEN_PATH.chmod(0o600)
        logger.info(f"Token saved to {TOKEN_PATH}")

    # Wrap in an authorized http with an explicit timeout so a single
    # hung call can't stall the orchestrator (see logs from 2026-05-11
    # where the job spent 36 min in a single fetch).
    http = httplib2.Http(timeout=HTTP_TIMEOUT_SEC)
    authed_http = AuthorizedHttp(creds, http=http)
    return build("gmail", "v1", http=authed_http, cache_discovery=False)


def _with_retry(api_call_fn, *, label: str = "Gmail API call"):
    """Execute an API call with retry on transient errors.

    ``api_call_fn`` is a zero-arg callable that returns a request object
    (e.g. ``lambda: service.users().messages().list(...).execute()``).
    Retries on HTTP 5xx and network timeouts with exponential backoff.
    """
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return api_call_fn()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in _RETRY_STATUS and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SEC * attempt
                logger.warning(
                    f"{label}: HTTP {status} on attempt {attempt}, retrying in {wait}s"
                )
                time.sleep(wait)
                last_exc = e
                continue
            raise
        except (socket.timeout, TimeoutError, OSError) as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SEC * attempt
                logger.warning(
                    f"{label}: timeout/network error on attempt {attempt} ({e!r}), retrying in {wait}s"
                )
                time.sleep(wait)
                last_exc = e
                continue
            raise
    if last_exc:
        raise last_exc


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------

def list_messages(service, query: str, max_results: int = 50) -> list[dict]:
    """Return a list of message metadata dicts matching ``query``."""
    response = _with_retry(
        lambda: service.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute(),
        label=f"messages.list({query[:40]!r})",
    )
    return response.get("messages", [])


def get_message(service, msg_id: str) -> dict:
    """Get a full message including payload + headers + attachment metadata."""
    return _with_retry(
        lambda: service.users().messages().get(
            userId="me", id=msg_id, format="full"
        ).execute(),
        label=f"messages.get({msg_id[:12]})",
    )


def header_value(message: dict, name: str) -> str:
    """Extract a header value (Subject, From, Date, etc.) from a message."""
    headers = message.get("payload", {}).get("headers", [])
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def iter_attachments(payload: dict) -> Iterator[dict]:
    """Yield all attachment parts (recursively walks payload tree)."""
    if not payload:
        return
    for part in payload.get("parts", []) or []:
        if part.get("filename") and part.get("body", {}).get("attachmentId"):
            yield part
        # Recurse for multipart messages
        yield from iter_attachments(part)


def download_attachment_bytes(service, msg_id: str, attachment_id: str) -> bytes:
    """Download attachment bytes (decodes base64url)."""
    att = _with_retry(
        lambda: service.users().messages().attachments().get(
            userId="me", messageId=msg_id, id=attachment_id
        ).execute(),
        label=f"attachments.get({attachment_id[:12]})",
    )
    data = att.get("data", "")
    return base64.urlsafe_b64decode(data)


# ---------------------------------------------------------------------------
# Processing weights — weekly Excel files
# ---------------------------------------------------------------------------

_DATE_RANGE_RE = re.compile(
    r"(\d{1,2})/(\d{1,2})/(\d{2,4})\s*[-–]\s*(\d{1,2})/(\d{1,2})/(\d{2,4})"
)


def parse_week_dates(subject: str) -> tuple[str, str] | None:
    """Extract MM-DD-YY start/end from 'week of M/D/YY-M/D/YY' subjects.

    Returns ('MM-DD-YY', 'MM-DD-YY') or None if not found.
    """
    m = _DATE_RANGE_RE.search(subject)
    if not m:
        return None
    parts = m.groups()
    sm, sd, sy = parts[0], parts[1], parts[2]
    em, ed, ey = parts[3], parts[4], parts[5]
    sy = sy if len(sy) == 2 else sy[-2:]
    ey = ey if len(ey) == 2 else ey[-2:]
    return (
        f"{int(sm):02d}-{int(sd):02d}-{sy}",
        f"{int(em):02d}-{int(ed):02d}-{ey}",
    )


def shift_from_filename(filename: str) -> str | None:
    """Return '1st', '2nd', or '3rd' if the filename matches a shift pattern."""
    fl = filename.lower()
    for shift in ("1st", "2nd", "3rd"):
        if shift in fl and "shift" in fl and "processing" in fl:
            return shift
    return None


def fetch_processing_weights(service, days_back: int = 14, dry_run: bool = False) -> list[Path]:
    """Find weekly processing-weights emails, download all 3 shift files.

    Each file is renamed using the date range parsed from the email subject,
    e.g. ``1st shift processing weights 04-13-26 to 04-17-26.xlsx`` and saved
    under ``processing_reports/``. Skips any file that already exists.
    """
    query = f"{PROCESSING_WEIGHTS_QUERY} newer_than:{days_back}d"
    msgs = list_messages(service, query)
    if not msgs:
        logger.info("No matching processing-weights emails.")
        return []

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    for stub in msgs:
        msg = get_message(service, stub["id"])
        subject = header_value(msg, "Subject")
        date_range = parse_week_dates(subject)
        if not date_range:
            logger.warning(f"Could not parse date range from subject: {subject!r}")
            continue
        start, end = date_range

        for part in iter_attachments(msg["payload"]):
            filename = part["filename"]
            if not filename.lower().endswith(".xlsx"):
                continue
            shift = shift_from_filename(filename)
            if shift is None:
                logger.info(f"Skipping non-shift attachment: {filename}")
                continue
            target_name = f"{shift} shift processing weights {start} to {end}.xlsx"
            target = REPORTS_DIR / target_name

            if target.exists():
                logger.info(f"Already have: {target_name}")
                continue
            if dry_run:
                print(f"  [dry-run] would download → {target_name}")
                continue

            data = download_attachment_bytes(service, stub["id"], part["body"]["attachmentId"])
            target.write_bytes(data)
            logger.info(f"Downloaded {target_name} ({len(data):,} bytes)")
            saved.append(target)

    return saved


# ---------------------------------------------------------------------------
# Payroll PDFs
# ---------------------------------------------------------------------------

# Filenames to skip when searching pay-period attachments
_PAYROLL_SKIP_TOKENS = ("snellville", "prn", "southeast")


def fetch_payroll_pdfs(service, days_back: int = 90, dry_run: bool = False) -> list[Path]:
    """Find pay-period emails, download Walton (and unprefixed) PDFs only.

    Skips Snellville and PRN Southeast attachments. Filenames preserved as-is
    (e.g., ``Walton PayPeriod_Report(2026-04-20).pdf``). Saved under
    ``data/payroll_pdfs/``. Skips files that already exist.
    """
    query = f"{PAYROLL_QUERY} newer_than:{days_back}d"
    msgs = list_messages(service, query, max_results=50)
    if not msgs:
        logger.info("No matching payroll emails.")
        return []

    PAYROLL_PDF_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    for stub in msgs:
        msg = get_message(service, stub["id"])
        subject = header_value(msg, "Subject")

        for part in iter_attachments(msg["payload"]):
            filename = part["filename"]
            if not filename.lower().endswith(".pdf"):
                continue
            fl = filename.lower()
            if any(skip in fl for skip in _PAYROLL_SKIP_TOKENS):
                logger.debug(f"Skipping non-Walton: {filename}")
                continue
            target = PAYROLL_PDF_DIR / filename

            if target.exists():
                logger.info(f"Already have: {filename}")
                continue
            if dry_run:
                print(f"  [dry-run] would download → {filename} ({subject!r})")
                continue

            data = download_attachment_bytes(service, stub["id"], part["body"]["attachmentId"])
            target.write_bytes(data)
            logger.info(f"Downloaded {filename} ({len(data):,} bytes)")
            saved.append(target)

    return saved


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(
        description="Fetch production data attachments from Gmail via the Gmail API.",
        epilog="One-time setup: place credentials.json at "
               f"{CREDENTIALS_PATH} and run with --auth",
    )
    parser.add_argument("--auth", action="store_true",
                        help="Run OAuth flow (browser opens) and save token. One-time.")
    parser.add_argument("--processing-weights", action="store_true",
                        help="Download new weekly processing-weights xlsx files.")
    parser.add_argument("--payroll", action="store_true",
                        help="Download new Walton pay-period PDFs.")
    parser.add_argument("--all", action="store_true",
                        help="Run both --processing-weights and --payroll.")
    parser.add_argument("--list", action="store_true",
                        help="Dry-run: show what would be downloaded without saving.")
    parser.add_argument("--days-back", type=int, default=None,
                        help="Override default lookback window (days).")
    args = parser.parse_args()

    if not any([args.auth, args.processing_weights, args.payroll, args.all]):
        parser.print_help()
        return 1

    service = get_service()

    if args.auth:
        # Reaching here means auth succeeded
        profile = service.users().getProfile(userId="me").execute()
        print(f"Authenticated as: {profile['emailAddress']}")
        print(f"Token saved to: {TOKEN_PATH}")
        if not (args.processing_weights or args.payroll or args.all):
            return 0

    if args.processing_weights or args.all:
        days = args.days_back if args.days_back is not None else 14
        print(f"\n=== Processing weights (last {days} days) ===")
        try:
            saved = fetch_processing_weights(service, days_back=days, dry_run=args.list)
            print(f"  {len(saved)} new file(s) downloaded" if not args.list else "  (dry-run)")
        except HttpError as e:
            print(f"ERROR fetching processing weights: {e}", file=sys.stderr)

    if args.payroll or args.all:
        days = args.days_back if args.days_back is not None else 90
        print(f"\n=== Payroll PDFs (last {days} days) ===")
        try:
            saved = fetch_payroll_pdfs(service, days_back=days, dry_run=args.list)
            print(f"  {len(saved)} new file(s) downloaded" if not args.list else "  (dry-run)")
        except HttpError as e:
            print(f"ERROR fetching payroll: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
