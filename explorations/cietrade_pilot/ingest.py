"""Archive cieTrade Converting Inquiry exports as timestamped snapshots.

Every export is a snapshot of the jobs it lists at the moment it was downloaded.
cieTrade does not stamp the file, but the download creates it, so the file's
creation time (st_birthtime on macOS) is the export time to the second. The
archive names each file by that time and is the log the model reads:

    data/cietrade_exports/converting_2026-09-14T11-34-19.csv

Usage
    python3 explorations/cietrade_pilot/ingest.py                 # sweep ~/Downloads for new exports
    python3 explorations/cietrade_pilot/ingest.py path/to/x.csv   # one file
    python3 explorations/cietrade_pilot/ingest.py x.csv --at "2026-09-14 11:34"   # override the time
Identical files (same bytes) are archived once. Nothing here is committed: the
archive directory is gitignored.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
EXPORT_DIR = REPO / "data" / "cietrade_exports"
DOWNLOADS = Path.home() / "Downloads"
PATTERN = "ConvertingInquiryExport*.csv"
NAME_FMT = "converting_%Y-%m-%dT%H-%M-%S.csv"


def export_time(path: Path) -> datetime:
    st = path.stat()
    ts = getattr(st, "st_birthtime", None) or st.st_mtime
    return datetime.fromtimestamp(ts)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def describe(path: Path) -> dict:
    df = pd.read_csv(path)
    status = df["Status"].astype(str).str.strip().value_counts().to_dict() if "Status" in df else {}
    kind = "in-process" if set(status) == {"Work"} else "posted" if set(status) == {"Posted"} else "mixed"
    return {"rows": len(df), "jobs": df["Job No"].nunique() if "Job No" in df else 0, "kind": kind,
            "output_lbs": float(df["Output Qty"].sum()) if "Output Qty" in df else 0.0}


def archive(path: Path, at: datetime | None = None) -> tuple[Path | None, str]:
    """Copy one export into the archive. Returns (archived path or None, note)."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    h = digest(path)
    for existing in EXPORT_DIR.glob("converting_*.csv"):
        if digest(existing) == h:
            return None, f"already archived as {existing.name}"
    ts = at or export_time(path)
    target = EXPORT_DIR / ts.strftime(NAME_FMT)
    n = 1
    while target.exists():
        n += 1
        target = EXPORT_DIR / (ts.strftime(NAME_FMT)[:-4] + f"_{n}.csv")
    shutil.copy2(path, target)
    return target, "archived"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("files", nargs="*", help="export files (default: sweep ~/Downloads)")
    ap.add_argument("--at", help="export time override, e.g. '2026-09-14 11:34' (single file only)")
    ap.add_argument("--since", default="2026-01-01", help="ignore Downloads files created before this date")
    args = ap.parse_args(argv)
    files = [Path(f).expanduser() for f in args.files] or sorted(
        (Path(p) for p in glob.glob(str(DOWNLOADS / PATTERN))), key=export_time)
    if args.at and len(files) != 1:
        ap.error("--at applies to exactly one file")
    since = pd.Timestamp(args.since)
    rows = []
    for f in files:
        if not f.exists():
            print(f"missing: {f}", file=sys.stderr)
            continue
        if not args.files and export_time(f) < since:
            continue
        at = pd.Timestamp(args.at).to_pydatetime() if args.at else None
        target, note = archive(f, at)
        d = describe(target or f)
        rows.append({"source": f.name, "export_time": (at or export_time(f)).strftime("%Y-%m-%d %H:%M:%S"),
                     "archived": target.name if target else "-", "note": note, **d})
    if rows:
        print(pd.DataFrame(rows).to_string(index=False))
    else:
        print("no exports found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
