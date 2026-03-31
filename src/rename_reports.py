"""Rename downloaded processing weight attachments to match the expected format.

Email attachments come named like:
    1st shift processing weights.xlsx

This script renames them to include the date range:
    1st shift processing weights 03-23-26 to 03-27-26.xlsx

Usage:
    python src/rename_reports.py --start 3/23/26 --end 3/27/26 [--downloads ~/Downloads]

Files are renamed in-place in the downloads directory, then moved to processing_reports/.
"""

from __future__ import annotations

import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORTS_DIR = _PROJECT_ROOT / "processing_reports"

SHIFTS = ["1st", "2nd", "3rd"]


def normalize_date(date_str: str) -> str:
    """Parse a date like '3/23/26' or '03-23-26' and return 'MM-DD-YY'."""
    date_str = date_str.strip()
    for fmt in ("%m/%d/%y", "%m-%d-%y", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%m-%d-%y")
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str!r}")


def parse_subject_dates(subject: str) -> tuple[str, str]:
    """Extract start and end dates from an email subject line.

    Handles formats like:
        'processing weights for the week of 3/23/26 - 3/27/26'
        'processing weights for the week of 3/16/26-3/20/26'
    """
    match = re.search(
        r"(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        subject,
    )
    if not match:
        raise ValueError(f"Cannot extract date range from subject: {subject!r}")
    return normalize_date(match.group(1)), normalize_date(match.group(2))


def find_and_rename(
    downloads_dir: Path,
    reports_dir: Path,
    start_date: str,
    end_date: str,
) -> list[Path]:
    """Find shift attachments in downloads_dir, rename and move to reports_dir.

    Returns list of destination paths for files that were moved.
    """
    moved = []
    for shift in SHIFTS:
        # Match files like "1st shift processing weights.xlsx" or
        # "1st shift processing weights (1).xlsx" (duplicate downloads)
        pattern = f"{shift} shift processing weights*.xlsx"
        candidates = sorted(downloads_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)

        if not candidates:
            print(f"  WARNING: No file found for {shift} shift in {downloads_dir}")
            continue

        src = candidates[0]  # most recently modified
        target_name = f"{shift} shift processing weights {start_date} to {end_date}.xlsx"
        dest = reports_dir / target_name

        if dest.exists():
            print(f"  SKIP (exists): {target_name}")
            continue

        shutil.move(str(src), str(dest))
        print(f"  MOVED: {src.name} -> {target_name}")
        moved.append(dest)

    return moved


def main():
    parser = argparse.ArgumentParser(description="Rename and move processing weight reports.")
    parser.add_argument("--start", required=True, help="Week start date (e.g., 3/23/26)")
    parser.add_argument("--end", required=True, help="Week end date (e.g., 3/27/26)")
    parser.add_argument("--downloads", type=Path, default=Path.home() / "Downloads",
                        help="Directory where attachments were downloaded")
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR,
                        help="Destination processing_reports directory")
    args = parser.parse_args()

    start = normalize_date(args.start)
    end = normalize_date(args.end)

    print(f"Renaming files for week {start} to {end}")
    print(f"  Source: {args.downloads}")
    print(f"  Destination: {args.reports_dir}")

    moved = find_and_rename(args.downloads, args.reports_dir, start, end)

    if moved:
        print(f"\nMoved {len(moved)} file(s). Ready for aggregation.")
    else:
        print("\nNo files moved.")


if __name__ == "__main__":
    main()
