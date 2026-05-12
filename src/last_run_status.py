"""
Quick status check for the most recent weekly_update.py run.

Usage:
    python3 src/last_run_status.py             # show latest run
    python3 src/last_run_status.py --last 3    # show last 3 runs
    python3 src/last_run_status.py --tail      # tail full log
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_FILE = PROJECT_ROOT / "logs" / "weekly_update.log"
STDOUT_LOG = PROJECT_ROOT / "logs" / "weekly_stdout.log"

USE_COLOR = sys.stdout.isatty()


def _c(code: str, t: str) -> str:
    return f"\033[{code}m{t}\033[0m" if USE_COLOR else t


def green(t): return _c("32", t)
def red(t): return _c("31", t)
def yellow(t): return _c("33", t)
def bold(t): return _c("1", t)
def dim(t): return _c("2", t)
def cyan(t): return _c("36", t)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[INFO\]")


def parse_runs() -> list[dict]:
    """Parse logs/weekly_update.log into a list of run dicts (chronological)."""
    if not LOG_FILE.exists():
        return []

    lines = LOG_FILE.read_text(encoding="utf-8").splitlines()

    runs: list[dict] = []
    current: dict | None = None
    for line in lines:
        m = _TS_RE.match(line)
        if not m:
            if current is not None:
                current["lines"].append(line)
            continue
        ts = m.group(1)
        rest = line[m.end():].strip()
        if rest.startswith("=== START"):
            current = {"start": ts, "end": None, "runtime": None, "lines": []}
            runs.append(current)
        elif rest.startswith("=== END") and current is not None:
            current["end"] = ts
            rt_m = re.search(r"runtime=([\d.]+)s", rest)
            if rt_m:
                current["runtime"] = float(rt_m.group(1))
        elif current is not None:
            current["lines"].append(rest)
    return runs


def parse_stdout_for_run(run_start_ts: str) -> dict:
    """Best-effort: scan logs/weekly_stdout.log for the chunk that matches the run.

    The stdout log has a "Walton Weekly Update — YYYY-MM-DD HH:MM:SS" banner
    and a "Summary" footer for each run. We find the chunk by the banner.
    """
    out: dict = {"steps": [], "summary": {}, "issues": []}
    if not STDOUT_LOG.exists():
        return out
    text = STDOUT_LOG.read_text(encoding="utf-8")
    # The banner line uses dashes (—) in the script but plain text in logs.
    needle = f"Walton Weekly Update"
    # Look for the run with the matching timestamp (HH:MM:SS-prefix match)
    # ts format: 2026-05-11 12:09:55
    chunks = text.split("=" * 70)
    target_chunk = None
    for i, chunk in enumerate(chunks):
        if needle in chunk and run_start_ts[:16] in chunk:
            # Take this chunk and the following chunks until the next Walton banner
            tail = chunks[i:]
            joined = ("=" * 70).join(tail)
            # Trim at next banner (if any)
            nxt = joined.find(needle, len(needle) + 30)
            target_chunk = joined[:nxt] if nxt > 0 else joined
            break

    if not target_chunk:
        return out

    # Extract step lines (lines starting with ✓ or ⚠ or ✗)
    for line in target_chunk.splitlines():
        stripped = line.strip()
        if stripped.startswith(("✓", "⚠", "✗")):
            out["steps"].append(stripped)
            if stripped.startswith("⚠"):
                out["issues"].append(stripped[2:].strip())

    # Extract summary key/value lines
    summary_keys = [
        "New processing-weights files", "New payroll PDFs",
        "Total daily records", "Pay periods aggregated",
        "Validation issues", "Dashboards built", "Failed dashboards",
        "Git", "Runtime",
    ]
    for line in target_chunk.splitlines():
        for k in summary_keys:
            if k in line and ":" in line:
                val = line.split(":", 1)[1].strip()
                # Strip ANSI codes
                val = re.sub(r"\033\[[\d;]*m", "", val)
                out["summary"][k] = val
                break
    return out


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def human_age(when: datetime) -> str:
    delta = datetime.now() - when
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    if days >= 1:
        return f"{days} day{'s' if days != 1 else ''} ago"
    if hours >= 1:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if minutes >= 1:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    return "just now"


def format_runtime(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    if seconds < 60:
        return f"{seconds:.1f}s"
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s"


def print_run(run: dict, idx: int, total: int) -> None:
    start_dt = datetime.strptime(run["start"], "%Y-%m-%d %H:%M:%S")
    age = human_age(start_dt)
    runtime = format_runtime(run.get("runtime"))
    completed = run["end"] is not None

    # Health emoji
    out = parse_stdout_for_run(run["start"])
    failed = "Failed dashboards" in out["summary"]
    push_ok = "pushed" in out["summary"].get("Git", "")
    health = green("✓") if completed and push_ok and not failed else \
             yellow("⚠") if completed else red("✗")

    label = f"Run {total - idx} of {total}" if idx > 0 else "LATEST RUN"
    print(f"\n{bold(label)}  {health}")
    print(f"  Started:   {run['start']}  ({dim(age)})")
    print(f"  Runtime:   {runtime}" + (red("  ⚠ unusually long") if run.get("runtime") and run["runtime"] > 300 else ""))

    if not out["summary"]:
        print(f"  {dim('(no parseable summary — log may have been rotated)')}")
        return

    s = out["summary"]
    fetched_proc = s.get("New processing-weights files", "?")
    fetched_pay = s.get("New payroll PDFs", "?")
    records = s.get("Total daily records", "?")
    periods = s.get("Pay periods aggregated", "?")
    dashboards = s.get("Dashboards built", "?")
    git = s.get("Git", "?")
    issues = s.get("Validation issues", "?")

    print(f"  Fetched:   {fetched_proc} processing-weights, {fetched_pay} payroll PDFs")
    print(f"  Data:      {records} daily records · {periods} pay periods")
    print(f"  Built:     {dashboards} dashboards")
    print(f"  Git:       {git}")
    if issues != "?" and int(issues) > 0:
        print(f"  Issues:    {issues} validation warning(s)")

    # Show failed dashboards if any
    if "Failed dashboards" in s:
        print(f"  {red('Failed:')}    {s['Failed dashboards']}")

    # Show stderr-like errors
    err_lines = [l for l in out["steps"] if l.startswith("✗")]
    if err_lines:
        print(f"  {red('Errors:')}")
        for e in err_lines[:5]:
            print(f"    {e}")


def show_next_scheduled() -> None:
    """If a launchd job is registered, show its next fire time."""
    try:
        result = subprocess.run(
            ["launchctl", "print", f"gui/{subprocess.run(['id', '-u'], capture_output=True, text=True).stdout.strip()}/com.walton.weekly_update"],
            capture_output=True, text=True, timeout=5,
        )
        out = result.stdout
        # Extract weekday/hour from the print output
        m_wd = re.search(r'"Weekday" => (\d+)', out)
        m_h = re.search(r'"Hour" => (\d+)', out)
        m_m = re.search(r'"Minute" => (\d+)', out)
        if m_wd and m_h:
            wd_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            wd = int(m_wd.group(1)) % 7
            hr = int(m_h.group(1))
            mn = int(m_m.group(1)) if m_m else 0
            # Compute next run date
            now = datetime.now()
            target_wd = wd if wd > 0 else 7  # plist 0=Sun, 7=Sun
            # Python: Monday=0
            py_target = (target_wd - 1) % 7
            days_ahead = (py_target - now.weekday()) % 7
            next_run = now.replace(hour=hr, minute=mn, second=0, microsecond=0)
            if days_ahead == 0 and next_run <= now:
                days_ahead = 7
            next_run = next_run + timedelta(days=days_ahead)
            print(f"\n{cyan('Next scheduled run:')} {wd_names[wd]} {next_run.strftime('%Y-%m-%d %H:%M')} ({human_age(next_run).replace(' ago', '').replace('just now', 'now').strip()} from now)")
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--last", type=int, default=1,
                        help="Show last N runs (default: 1)")
    parser.add_argument("--tail", action="store_true",
                        help="Also tail logs/weekly_stdout.log")
    args = parser.parse_args()

    runs = parse_runs()
    if not runs:
        print(red("No runs found in logs/weekly_update.log"))
        print(dim(f"  Expected at: {LOG_FILE}"))
        return 1

    print("=" * 70)
    print(bold(f"Walton Weekly Update — Status"))
    print("=" * 70)
    print(f"Total runs logged: {len(runs)}")

    show = runs[-args.last:]
    for i, run in enumerate(show):
        idx_from_latest = len(show) - 1 - i
        print_run(run, idx_from_latest, len(runs))

    show_next_scheduled()
    print()

    if args.tail:
        print(bold("\nFull tail of weekly_stdout.log (last 40 lines):"))
        if STDOUT_LOG.exists():
            for line in STDOUT_LOG.read_text(encoding="utf-8").splitlines()[-40:]:
                print(f"  {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
