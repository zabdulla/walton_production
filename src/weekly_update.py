"""
Weekly orchestrator: fetch new data, aggregate, build dashboards, commit, push.

Designed to run unattended on a schedule (launchd, cron). All steps emit
structured progress to stdout and append to ``logs/weekly_update.log``. Any
single step failing is logged but does not abort the rest of the pipeline
(except aggregate failures, which are fatal). The git step retries once on
the typical GH Actions push conflict.

Usage:
    python3 src/weekly_update.py                # full run
    python3 src/weekly_update.py --no-push      # build but skip git push
    python3 src/weekly_update.py --no-fetch     # skip Gmail fetch (rebuild only)
    python3 src/weekly_update.py --dry-run      # show what would happen
"""
from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "weekly_update.log"

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
USE_COLOR = sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def green(t): return _c("32", t)
def red(t): return _c("31", t)
def yellow(t): return _c("33", t)
def bold(t): return _c("1", t)
def dim(t): return _c("2", t)


def log_info(msg: str) -> None:
    print(msg, flush=True)


def log_step(num: int, total: int, title: str) -> None:
    print(f"\n{bold(f'Step {num}/{total}:')} {title}", flush=True)


def log_ok(msg: str) -> None:
    print(f"  {green('✓')} {msg}", flush=True)


def log_warn(msg: str) -> None:
    print(f"  {yellow('⚠')} {msg}", flush=True)


def log_err(msg: str) -> None:
    print(f"  {red('✗')} {msg}", flush=True)


def setup_file_logger() -> None:
    """Tee output to logs/weekly_update.log (rotated by date in the line prefix)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)


def run_cmd(cmd: list[str], capture: bool = True, timeout: int = 600,
            extra_env: dict | None = None) -> tuple[int, str, str]:
    """Run a shell command, return (returncode, stdout, stderr).

    `extra_env` merges into os.environ for this single call (used to set
    GIT_EDITOR=true so `git rebase --continue` doesn't open an editor).
    """
    import os
    logging.info(f"$ {' '.join(cmd)}")
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=capture,
            text=True,
            timeout=timeout,
            env=env,
        )
        if capture:
            logging.info(f"stdout: {result.stdout[:500]}")
            if result.stderr:
                logging.info(f"stderr: {result.stderr[:500]}")
        return result.returncode, result.stdout or "", result.stderr or ""
    except subprocess.TimeoutExpired:
        logging.error(f"Timeout: {' '.join(cmd)}")
        return -1, "", f"Command timed out after {timeout}s"


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step_fetch_emails(dry_run: bool = False) -> dict[str, Any]:
    """Run fetch_emails.py --all and parse the result counts."""
    args = ["python3", str(SRC_DIR / "fetch_emails.py"), "--all"]
    if dry_run:
        args.append("--list")
    rc, out, err = run_cmd(args, timeout=300)

    result = {"ok": rc == 0, "processing": 0, "payroll": 0, "raw": out + err}
    if rc != 0:
        log_err(f"Fetch failed (exit {rc})")
        if err:
            log_err(err.splitlines()[-1] if err else "no stderr")
        return result

    # Parse "  N new file(s) downloaded"
    for kind, pat in [("processing", r"Processing weights.*?(\d+) new file"),
                      ("payroll", r"Payroll.*?(\d+) new file")]:
        m = re.search(pat, out, re.DOTALL)
        if m:
            result[kind] = int(m.group(1))

    log_ok(f"Processing weights: {result['processing']} new file(s)")
    log_ok(f"Payroll PDFs: {result['payroll']} new file(s)")
    return result


def step_aggregate() -> dict[str, Any]:
    """Run aggregate_daily_data.py and extract record count."""
    rc, out, err = run_cmd(["python3", str(SRC_DIR / "aggregate_daily_data.py")], timeout=300)
    result = {"ok": rc == 0, "records": 0, "duplicates": 0}
    if rc != 0:
        log_err(f"Aggregation FAILED (exit {rc})")
        return result
    combined = out + err
    m = re.search(r"saved.*?\((\d+) records\)", combined)
    if m:
        result["records"] = int(m.group(1))
    m = re.search(r"Dropped (\d+) duplicate", combined)
    if m:
        result["duplicates"] = int(m.group(1))
    log_ok(f"{result['records']:,} records ({result['duplicates']} duplicate{'s' if result['duplicates'] != 1 else ''} dropped)")
    return result


def step_parse_payroll() -> dict[str, Any]:
    """Run parse_payroll_pdf.py --pdf-dir to (re)aggregate payroll data."""
    rc, out, err = run_cmd(
        ["python3", str(SRC_DIR / "parse_payroll_pdf.py"),
         "--pdf-dir", str(PROJECT_ROOT / "data" / "payroll_pdfs")],
        timeout=300,
    )
    result = {"ok": rc == 0, "processed": 0, "skipped": 0, "failed": 0}
    if rc != 0:
        log_err(f"Payroll parse exited with {rc}")
    combined = out + err
    m = re.search(r"Processed:\s*(\d+)", combined)
    if m: result["processed"] = int(m.group(1))
    m = re.search(r"Skipped.*?(\d+)", combined)
    if m: result["skipped"] = int(m.group(1))
    m = re.search(r"Failed:\s*(\d+)", combined)
    if m: result["failed"] = int(m.group(1))
    log_ok(f"{result['processed']} period(s) processed, {result['skipped']} skipped, {result['failed']} failed")
    return result


def step_validate() -> dict[str, Any]:
    """Run validate_data.py — non-fatal warnings."""
    rc, out, err = run_cmd(["python3", str(SRC_DIR / "validate_data.py")], timeout=120)
    # Validation exits 1 on critical issues but we treat as warning
    combined = out + err
    # Only parse the final "Summary" section to avoid double-counting the
    # warnings that appear in both the body and the summary.
    summary_marker = combined.rfind("Summary")
    if summary_marker > 0:
        combined = combined[summary_marker:]
    issues: list[str] = []
    for line in combined.splitlines():
        m = re.match(r"\s*WARN\s+(.*)", line)
        if m: issues.append(m.group(1).strip())
        m = re.match(r"\s*FAIL\s+(.*)", line)
        if m: issues.append("CRITICAL: " + m.group(1).strip())
    if issues:
        for issue in issues:
            log_warn(issue)
    else:
        log_ok("All checks passed")
    return {"ok": True, "issues": issues}


def step_build_dashboards() -> dict[str, Any]:
    """Build all 5 dashboards. Each runs independently — one failure doesn't stop others."""
    builds = [
        ("Interactive", "build_interactive_dashboard.py"),
        ("Daily", "build_daily_dashboard.py"),
        ("Operator", "build_operator_dashboard.py"),
        ("Profit", "build_profit_dashboard.py"),
        ("Payroll", "build_payroll_dashboard.py"),
    ]
    results = {"ok": True, "built": [], "failed": []}
    for label, script in builds:
        rc, out, err = run_cmd(["python3", str(SRC_DIR / script)], timeout=600)
        if rc == 0:
            results["built"].append(label)
            # Pull a notable line from output if present
            extra = ""
            for line in out.splitlines():
                if "uplift" in line.lower():
                    extra = f" ({line.strip()})"
                    break
            log_ok(f"{label}{extra}")
        else:
            results["failed"].append(label)
            results["ok"] = False
            log_err(f"{label} failed (exit {rc})")
            if err:
                log_err(f"   {err.splitlines()[-1] if err.splitlines() else ''}")
    return results


def step_git_commit_push(no_push: bool = False) -> dict[str, Any]:
    """Stage tracked + new files, commit if anything changed, push with rebase retry."""
    result = {"ok": True, "committed": False, "pushed": False, "files": 0, "msg": ""}

    # Stage all relevant tracked files (gitignore excludes the sensitive ones)
    rc, _, _ = run_cmd(["git", "add",
                        "data/aggregated_daily_data.xlsx",
                        "data/aggregated_notes.xlsx",
                        "data/employee_roster.json",
                        "docs/index.html",
                        "docs/daily.html"], capture=True)

    rc, out, _ = run_cmd(["git", "diff", "--cached", "--stat"], capture=True)
    if not out.strip():
        log_info("  " + dim("(no changes to commit)"))
        return result

    # Count files
    file_count = len([l for l in out.splitlines() if "|" in l])
    result["files"] = file_count

    # Build commit message
    today = datetime.now().strftime("%Y-%m-%d")
    msg = f"Weekly auto-update {today}\n\nAutomated via src/weekly_update.py orchestrator."
    rc, _, err = run_cmd(["git", "commit", "-m", msg])
    if rc != 0:
        log_err(f"Commit failed: {err.strip()[:200]}")
        result["ok"] = False
        return result
    result["committed"] = True
    result["msg"] = msg.split("\n")[0]
    log_ok(f"Committed: {result['msg']}")

    if no_push:
        log_info("  " + dim("(--no-push, skipping push)"))
        return result

    # Run all git commands non-interactively (no editor prompts)
    no_editor = {"GIT_EDITOR": "true", "GIT_SEQUENCE_EDITOR": "true"}

    # Try push with one retry on rebase conflict
    for attempt in (1, 2):
        rc, out, err = run_cmd(["git", "push", "origin", "main"], capture=True, timeout=60)
        if rc == 0:
            result["pushed"] = True
            log_ok("Pushed to origin/main")
            return result
        # Common: GH Actions auto-commit pushed first → fast-forward rejected
        if "fast-forward" in err or "rejected" in err:
            log_warn(f"Push rejected (attempt {attempt}); rebasing...")
            run_cmd(["git", "pull", "--rebase", "origin", "main"],
                    capture=True, timeout=60, extra_env=no_editor)
            # If the rebase had a docs/index.html conflict, re-build and continue
            rc2, status, _ = run_cmd(["git", "status", "--porcelain"], capture=True)
            if "UU docs/index.html" in status:
                log_info("  resolving docs/index.html conflict by rebuilding...")
                run_cmd(["git", "checkout", "--theirs", "docs/index.html"], capture=True)
                run_cmd(["python3", str(SRC_DIR / "build_interactive_dashboard.py")], capture=True)
                run_cmd(["git", "add", "docs/index.html"], capture=True)
                run_cmd(["git", "rebase", "--continue"],
                        capture=True, timeout=60, extra_env=no_editor)
            # Confirm the rebase actually finished before retrying push
            rc3, status2, _ = run_cmd(["git", "status", "--porcelain=2", "--branch"], capture=True)
            if "rebase in progress" in status2 or any(l.startswith("u ") for l in status2.splitlines()):
                log_err("Rebase did not finish cleanly — manual intervention required")
                result["ok"] = False
                return result
            continue
        log_err(f"Push failed: {err.strip()[:200]}")
        result["ok"] = False
        return result

    log_err("Push failed after rebase retry")
    result["ok"] = False
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--no-fetch", action="store_true",
                        help="Skip Gmail fetch step (rebuild from existing data)")
    parser.add_argument("--no-push", action="store_true",
                        help="Build and commit but skip git push")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch step runs in --list mode; no aggregate/build/commit")
    args = parser.parse_args()

    setup_file_logger()
    started = time.time()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 70)
    print(bold(f"Walton Weekly Update — {timestamp}"))
    print("=" * 70)
    logging.info(f"=== START {timestamp} ===")

    summary: dict[str, Any] = {}

    # Step 1: Fetch
    log_step(1, 6, "Fetching new emails from Gmail")
    if args.no_fetch:
        log_info("  " + dim("(--no-fetch, skipping)"))
        summary["fetch"] = {"ok": True, "processing": 0, "payroll": 0}
    else:
        summary["fetch"] = step_fetch_emails(dry_run=args.dry_run)

    if args.dry_run:
        print(f"\n{dim('Dry-run complete.')}\n")
        return 0

    # Step 2: Aggregate
    log_step(2, 6, "Aggregating daily production data")
    summary["aggregate"] = step_aggregate()
    if not summary["aggregate"]["ok"]:
        log_err("Aggregation failed — aborting pipeline")
        return 1

    # Step 3: Parse payroll
    log_step(3, 6, "Parsing payroll PDFs")
    summary["payroll"] = step_parse_payroll()

    # Step 4: Validate
    log_step(4, 6, "Validating data quality")
    summary["validate"] = step_validate()

    # Step 5: Build all dashboards
    log_step(5, 6, "Building dashboards")
    summary["build"] = step_build_dashboards()

    # Step 6: Commit + push
    log_step(6, 6, "Committing changes to git")
    summary["git"] = step_git_commit_push(no_push=args.no_push)

    # Final summary
    elapsed = time.time() - started
    print("\n" + "=" * 70)
    print(bold("Summary"))
    print("=" * 70)
    print(f"  New processing-weights files: {summary['fetch'].get('processing', 0)}")
    print(f"  New payroll PDFs:             {summary['fetch'].get('payroll', 0)}")
    print(f"  Total daily records:          {summary['aggregate'].get('records', '?'):,}")
    print(f"  Pay periods aggregated:       {summary['payroll'].get('processed', '?')}")
    print(f"  Validation issues:            {len(summary['validate'].get('issues', []))}")
    print(f"  Dashboards built:             {len(summary['build'].get('built', []))} / 5")
    if summary["build"].get("failed"):
        print(f"  {red('Failed dashboards:')}            {', '.join(summary['build']['failed'])}")
    print(f"  Git: ", end="")
    if summary["git"].get("pushed"):
        print(green(f"committed + pushed ({summary['git']['files']} files)"))
    elif summary["git"].get("committed"):
        print(yellow("committed (not pushed)"))
    else:
        print(dim("no changes"))
    print(f"  Runtime: {elapsed:.1f}s")
    print("=" * 70)

    logging.info(f"=== END runtime={elapsed:.1f}s ===")

    # Exit non-zero if anything important failed
    if not summary["build"].get("ok") or not summary["git"].get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
