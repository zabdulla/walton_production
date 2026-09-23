"""Poll cieTrade for converting-job activity and keep a durable production log.

Every run (launchd, every 10 minutes):
  1. fetch every unposted job (Status=WORK) — the open-jobs snapshot;
  2. fetch jobs posted in the last CIETRADE_POSTED_LOOKBACK_DAYS (DateType=POST);
  3. append a line to data/cietrade/polls.jsonl (always, even on failure);
  4. if the open-jobs snapshot differs from the previous poll (or it is the first
     poll of the day), append its rows to data/cietrade/snapshots/<date>.csv
     with a Snapshot timestamp — consecutive snapshots of one job bracket the
     output produced in between;
  5. upsert postings into data/cietrade/posted.csv (First Seen / Last Seen).

All three files are small text and are committed by the daily update, so the
log survives this machine. Nothing is written back to cieTrade.

    python3 src/cietrade_poll.py                  # one poll
    python3 src/cietrade_poll.py --rebuild        # poll, then rebuild the pilot page (+ live copy)
    python3 src/cietrade_poll.py --backfill-posted   # one-off: every posted job since 2026-01-01
    python3 src/cietrade_poll.py --dry-run        # call the API, write nothing
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import (
    CIETRADE_DATA_DIR, CIETRADE_POSTED_LOOKBACK_DAYS, CIETRADE_SITES, LIVE_PAGE_COPY, PROJECT_ROOT,
)
import cietrade_api as api
import cietrade_live

POLLS = CIETRADE_DATA_DIR / "polls.jsonl"
POSTED = CIETRADE_DATA_DIR / "posted.csv"
SNAPSHOTS = CIETRADE_DATA_DIR / "snapshots"
BACKFILL_FROM = "1/1/2026"
HASH_COLS = ["Job No", "Machine", "Status", "Output Qty", "Output Units", "Input Qty"]
LOG = logging.getLogger("cietrade_poll")


def snapshot_hash(open_rows: pd.DataFrame) -> str:
    if open_rows.empty:
        return hashlib.sha1(b"empty").hexdigest()[:16]
    key = open_rows[HASH_COLS].astype(str).sort_values("Job No").to_csv(index=False, header=False)
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def last_poll(path: Path | None = None) -> dict | None:
    path = path or POLLS
    if not path.exists():
        return None
    with path.open("rb") as fh:
        tail = deque(fh, maxlen=1)
    if not tail:
        return None
    try:
        return json.loads(tail[0].decode())
    except json.JSONDecodeError:
        return None


def append_snapshot(open_rows: pd.DataFrame, ts: datetime, root: Path | None = None) -> Path:
    root = root or SNAPSHOTS
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{ts:%Y-%m-%d}.csv"
    rows = open_rows.copy()
    rows.insert(0, "Snapshot", ts.strftime("%Y-%m-%dT%H:%M:%S"))
    rows.to_csv(path, mode="a", header=not path.exists(), index=False)
    return path


def upsert_posted(posted_rows: pd.DataFrame, ts: datetime, path: Path | None = None) -> dict:
    """Merge newly seen postings into the posted master. Returns counts."""
    path = path or POSTED
    stamp = ts.strftime("%Y-%m-%dT%H:%M:%S")
    existing = pd.read_csv(path) if path.exists() else pd.DataFrame(columns=list(posted_rows.columns) + ["First Seen", "Last Seen"])
    new = posted_rows.copy()
    new["First Seen"] = stamp
    new["Last Seen"] = stamp
    if existing.empty:
        merged, n_new, n_changed = new, len(new), 0
    else:
        existing = existing.set_index("Job No")
        new = new.set_index("Job No")
        seen = new.index.intersection(existing.index)
        n_new = len(new.index.difference(existing.index))
        n_changed = 0
        for j in seen:
            new.at[j, "First Seen"] = existing.at[j, "First Seen"]
            if abs(float(existing.at[j, "Output Qty"]) - float(new.at[j, "Output Qty"])) > 0.5 or str(existing.at[j, "Post Date"]) != str(new.at[j, "Post Date"]):
                n_changed += 1
                LOG.warning("posted job %s changed after posting: %s -> %s lbs", j, existing.at[j, "Output Qty"], new.at[j, "Output Qty"])
        merged = pd.concat([existing.drop(index=seen), new]).reset_index()
    merged = merged.sort_values("Job No").reset_index(drop=True)
    tmp = path.with_suffix(".tmp")
    merged.to_csv(tmp, index=False)
    tmp.replace(path)
    return {"posted_new": int(n_new), "posted_changed": int(n_changed), "posted_total": int(len(merged))}


def rebuild_pilot() -> bool:
    """Rebuild the pilot page and drop a copy where a phone can reach it. Never fatal."""
    build = PROJECT_ROOT / "explorations" / "cietrade_pilot" / "build.py"
    try:
        r = subprocess.run([sys.executable, str(build)], capture_output=True, text=True, timeout=600)
        if r.returncode != 0:
            LOG.error("pilot build failed: %s", (r.stderr or r.stdout).strip()[-400:])
            return False
        if LIVE_PAGE_COPY:
            src = build.parent / "out" / "walton_daily_pilot_standalone.html"
            LIVE_PAGE_COPY.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, LIVE_PAGE_COPY)
        return True
    except Exception as e:  # noqa: BLE001 — the poll must still be logged
        LOG.error("pilot rebuild error: %s", e)
        return False


def poll(dry_run: bool = False, backfill_posted: bool = False, fetch=None) -> dict:
    """One poll. ``fetch`` (creds, **filters) -> rows is injectable for tests."""
    fetch = fetch or (lambda creds, **f: api.list_converting_jobs(creds, **f))
    ts = datetime.now().replace(microsecond=0)
    prev = last_poll()
    if prev and prev.get("ts") and datetime.strptime(prev["ts"], "%Y-%m-%dT%H:%M:%S") >= ts:
        ts = datetime.strptime(prev["ts"], "%Y-%m-%dT%H:%M:%S") + timedelta(seconds=1)   # snapshots must never share a second
    rec: dict = {"ts": ts.strftime("%Y-%m-%dT%H:%M:%S"), "ok": False, "ms": 0, "changed": False, "error": None}
    t0 = time.time()
    try:
        creds = api.load_credentials()
        open_rows = api.normalize(fetch(creds, Status="WORK"))
        since = BACKFILL_FROM if backfill_posted else (ts - timedelta(days=CIETRADE_POSTED_LOOKBACK_DAYS)).strftime("%m/%d/%Y")
        posted_rows = api.normalize(fetch(creds, Status="POSTED", DateFrom=since, **({} if backfill_posted else {"DateType": "POST"})))
    except Exception as e:  # noqa: BLE001 — record the failure and move on
        rec["error"] = f"{e.__class__.__name__}: {str(e)[:200]}"
        rec["ms"] = int((time.time() - t0) * 1000)
        LOG.error("poll failed: %s", rec["error"])
        if not dry_run:
            _append_poll(rec)
        return rec
    rec["ms"] = int((time.time() - t0) * 1000)
    site = open_rows[open_rows["Warehouse"].isin(CIETRADE_SITES)]
    h = snapshot_hash(open_rows)
    first_today = prev is None or str(prev.get("ts", ""))[:10] != rec["ts"][:10]
    changed = first_today or prev.get("open_hash") != h
    rec.update(ok=True, open_jobs=int(len(open_rows)), monroe_open=int(len(site)),
               open_lbs=float(site["Output Qty"].fillna(0).sum()), open_hash=h, changed=bool(changed),
               posted_recent=int(len(posted_rows)))
    if dry_run:
        rec["dry_run"] = True
        return rec
    if changed:
        append_snapshot(open_rows, ts)
    if len(posted_rows):
        rec.update(upsert_posted(posted_rows, ts))
    _append_poll(rec)
    return rec


def _append_poll(rec: dict, path: Path | None = None) -> None:
    path = path or POLLS
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(rec) + "\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--rebuild", action="store_true", help="rebuild the pilot page after polling")
    ap.add_argument("--backfill-posted", action="store_true", help=f"fetch every posted job since {BACKFILL_FROM}")
    ap.add_argument("--dry-run", action="store_true", help="call the API but write nothing")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    rec = poll(dry_run=args.dry_run, backfill_posted=args.backfill_posted)
    if rec["ok"]:
        LOG.info("open jobs %d (Monroe %d, %s lbs so far) · postings seen %d (new %d, changed %d) · %s · %d ms",
                 rec["open_jobs"], rec["monroe_open"], f"{rec['open_lbs']:,.0f}", rec.get("posted_recent", 0),
                 rec.get("posted_new", 0), rec.get("posted_changed", 0),
                 "snapshot appended" if rec["changed"] else "no change", rec["ms"])
    if not args.dry_run:
        # Published after a failed poll as well: the feed then carries api_down_since and the error,
        # so the dashboard says "cieTrade unavailable since ..." instead of looking frozen.
        try:
            live = cietrade_live.write_live()
            published = cietrade_live.publish()
            LOG.info("live feed: %d changes today, %s lbs · %s%s", live["changes_today"], f"{live['lbs_today']:,}",
                     "published" if published else "not published",
                     "" if rec["ok"] else f" · cieTrade unavailable since {live.get('api_down_since')}")
        except Exception as e:  # noqa: BLE001 — never mask the poll result
            LOG.error("live feed failed: %s", e)
    if args.rebuild and rec["ok"] and not args.dry_run:
        LOG.info("pilot page %s", "rebuilt" if rebuild_pilot() else "NOT rebuilt")
    return 0 if rec["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
