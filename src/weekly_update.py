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


def check_dependencies() -> list[str]:
    """Verify that all third-party packages used by the pipeline are importable.

    Returns a list of *missing* import names. The launchd-spawned Python has
    been observed to silently lose packages after a Python upgrade or a
    `pip install` for an unrelated project, so we want to fail fast and
    loud rather than crashing somewhere deep in the pipeline.
    """
    required = [
        ("pandas", "pandas"),
        ("plotly", "plotly"),
        ("openpyxl", "openpyxl"),
        ("pymupdf", "pymupdf"),  # PDF parsing
        ("googleapiclient", "google-api-python-client"),
        ("google.auth", "google-auth"),
        ("google_auth_oauthlib", "google-auth-oauthlib"),
        ("google_auth_httplib2", "google-auth-httplib2"),
        ("httplib2", "httplib2"),
    ]
    missing: list[str] = []
    for module_name, pip_name in required:
        try:
            __import__(module_name)
        except ImportError:
            missing.append(pip_name)
    return missing


def setup_file_logger() -> None:
    """Tee output to logs/weekly_update.log (rotated by date in the line prefix)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)


def send_notification(title: str, message: str, success: bool = True) -> None:
    """Send a macOS desktop notification. No-op on other platforms; never raises."""
    if sys.platform != "darwin":
        return
    try:
        # Escape double quotes inside the strings so the AppleScript stays valid.
        t = title.replace('"', r'\"')
        m = message.replace('"', r'\"')
        sound = "Glass" if success else "Basso"
        script = f'display notification "{m}" with title "{t}" sound name "{sound}"'
        subprocess.run(["osascript", "-e", script], timeout=5, capture_output=True)
    except Exception as e:
        logging.warning(f"Notification failed (non-fatal): {e}")


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
    """Run aggregation. Snapshots the previous aggregated file first so the
    orchestrator can roll back if downstream validation gates the run.
    """
    import shutil
    from datetime import datetime as _dt
    agg_path = PROJECT_ROOT / "data" / "aggregated_daily_data.xlsx"
    snap_dir = PROJECT_ROOT / "data" / "snapshots"
    pre_snapshot: Path | None = None
    if agg_path.exists():
        snap_dir.mkdir(parents=True, exist_ok=True)
        ts = _dt.now().strftime("%Y-%m-%dT%H-%M-%S")
        pre_snapshot = snap_dir / f"{agg_path.stem}_prerun_{ts}{agg_path.suffix}"
        shutil.copy2(agg_path, pre_snapshot)

    rc, out, err = run_cmd(["python3", str(SRC_DIR / "aggregate_daily_data.py")], timeout=300)
    result: dict[str, Any] = {
        "ok": rc == 0, "records": 0, "duplicates": 0,
        "pre_snapshot": pre_snapshot,
    }
    if rc != 0:
        log_err(f"Aggregation FAILED (exit {rc})")
        # If the aggregation script bailed out (e.g., growth sanity check
        # rejected an unexpectedly small new file), preserve the snapshot
        # for inspection but don't restore yet — atomic writes mean the
        # on-disk file is still the previous good one.
        return result
    combined = out + err
    # Strip any thousands-separator commas before int() — supports either
    # "(3,904 records)" or "(3904 records)".
    m = re.search(r"saved.*?\(([\d,]+) records\)", combined)
    if m:
        result["records"] = int(m.group(1).replace(",", ""))
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
    """Run validation in-process and compute the publish-gating decision.

    Returns dict with:
      ok       – always True (validation itself never errors fatally)
      issues   – list of human-readable summary lines (for the report)
      blocked  – True if publication should be blocked
      reasons  – list of strings explaining why blocked (empty if not)
    """
    # Import the validation module directly so we get structured results
    # rather than parsing stdout. Both modules live in src/ and are siblings.
    sys.path.insert(0, str(SRC_DIR))
    try:
        from validate_data import run_validation, gating_decision
    finally:
        if str(SRC_DIR) in sys.path:
            sys.path.remove(str(SRC_DIR))

    try:
        results = run_validation()
    except Exception as e:
        log_err(f"Validation crashed: {e}")
        # Treat crash as block-publish — safer than letting bad data ship.
        return {"ok": False, "issues": [f"validation crashed: {e}"],
                "blocked": True, "reasons": [str(e)]}

    # Build a compact summary similar to before
    issues: list[str] = []
    if results.get("unmapped_products"):
        items = results["unmapped_products"]
        issues.append(f"{len(items)} unmapped product(s)")
    if results.get("duplicates_count", 0):
        issues.append(f"{results['duplicates_count']} duplicate row(s)")
    if results.get("missing_operators"):
        n = sum(results["missing_operators"].values())
        issues.append(f"{n} missing operator(s)")
    if results.get("missing_weeks"):
        issues.append(f"{len(results['missing_weeks'])} missing week(s)")
    payroll = results.get("payroll", {})
    if payroll.get("unmatched_production_ops"):
        issues.append(f"{len(payroll['unmatched_production_ops'])} unmapped production operator(s)")
    if payroll.get("unrostered_employees"):
        issues.append(f"{len(payroll['unrostered_employees'])} unrostered payroll employee(s)")
    anomalies = results.get("anomalous_values") or []
    if anomalies:
        issues.append(f"{len(anomalies)} anomalous value(s)")

    blocked, reasons = gating_decision(results)

    if not issues:
        log_ok("All checks passed")
    else:
        for issue in issues:
            log_warn(issue)

    if blocked:
        log_err("Validation gating: BLOCK publication")
        for r in reasons:
            log_err(f"  reason: {r}")

    return {"ok": True, "issues": issues, "blocked": blocked, "reasons": reasons}


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

    # Map of published HTML dashboard → builder script that produces it.
    # Used to auto-resolve rebase conflicts on generated artifacts: prefer
    # the remote version (--theirs), rebuild locally on top, re-stage.
    DASHBOARD_BUILDERS = {
        "docs/index.html": "build_interactive_dashboard.py",
        "docs/daily.html": "build_daily_dashboard.py",
    }

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
            # Resolve any conflicts on generated dashboard HTML files.
            # Pattern: `UU docs/<name>.html` means both modified. Take remote,
            # then rebuild locally so the on-disk version reflects local data.
            rc2, status, _ = run_cmd(["git", "status", "--porcelain"], capture=True)
            for path, builder in DASHBOARD_BUILDERS.items():
                if f"UU {path}" in status:
                    log_info(f"  resolving {path} conflict by rebuilding...")
                    run_cmd(["git", "checkout", "--theirs", path], capture=True)
                    run_cmd(["python3", str(SRC_DIR / builder)], capture=True)
                    run_cmd(["git", "add", path], capture=True)
            # Continue the rebase only if there are no remaining conflicts.
            rc_status, status_after, _ = run_cmd(["git", "status", "--porcelain"], capture=True)
            unresolved = [l for l in status_after.splitlines() if l.startswith(("UU ", "AA ", "DD "))]
            if unresolved:
                log_err(f"Unresolved rebase conflicts remain: {unresolved[:3]}")
                log_err("Manual intervention required — leaving rebase in progress")
                result["ok"] = False
                return result
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

    # Fail fast if a dependency is missing — the launchd-spawned Python has
    # lost packages mid-week before (May 2026, see logs). Better to crash
    # at startup with a clear message than crash deep in the pipeline.
    missing_deps = check_dependencies()
    if missing_deps:
        msg = (
            f"Missing Python packages: {', '.join(missing_deps)}\n"
            f"  Fix: cd {PROJECT_ROOT} && python3 -m pip install -r requirements.txt"
        )
        log_err(msg)
        send_notification(
            "Walton Weekly Update ⚠",
            f"Pipeline aborted: missing deps ({', '.join(missing_deps[:3])})",
            success=False,
        )
        return 2

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

    # If validation gates publication, restore the pre-aggregation snapshot
    # so the next run starts from a known-good state, then abort before
    # building dashboards or pushing.
    if summary["validate"].get("blocked"):
        log_err("Validation BLOCKED publication. Skipping dashboard build + git push.")
        pre_snap = summary["aggregate"].get("pre_snapshot")
        if pre_snap and Path(pre_snap).exists():
            import shutil
            agg_path = PROJECT_ROOT / "data" / "aggregated_daily_data.xlsx"
            tmp = agg_path.with_suffix(agg_path.suffix + ".tmp")
            shutil.copy2(pre_snap, tmp)
            tmp.replace(agg_path)
            log_warn(f"Restored aggregated data from snapshot: {Path(pre_snap).name}")
        else:
            log_warn("No pre-run snapshot available; aggregated file left as-is.")
        reasons_short = "; ".join(summary["validate"].get("reasons", []))[:120]
        send_notification(
            "Walton Weekly Update ⚠",
            f"BLOCKED by validation: {reasons_short or 'see logs'}",
            success=False,
        )
        elapsed = time.time() - started
        logging.info(f"=== END runtime={elapsed:.1f}s BLOCKED ===")
        return 3

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

    # ---- Mac native notification ----
    fetch = summary["fetch"]
    build = summary["build"]
    git = summary["git"]
    build_ok = build.get("ok", False)
    git_ok = git.get("ok", False)
    success = build_ok and git_ok

    # Build a compact one-line message
    parts = []
    new_files = fetch.get("processing", 0) + fetch.get("payroll", 0)
    if new_files:
        parts.append(f"{new_files} new file(s)")
    parts.append(f"{len(build.get('built', []))}/5 dashboards")
    if git.get("pushed"):
        parts.append("pushed")
    elif git.get("committed"):
        parts.append("committed (no push)")
    else:
        parts.append("no changes")
    parts.append(f"{elapsed:.0f}s")

    minutes = int(elapsed // 60)
    runtime_label = f"{minutes}m {int(elapsed % 60)}s" if minutes else f"{int(elapsed)}s"

    if success:
        title = "Walton Weekly Update ✓"
        message = f"{' · '.join(parts[:-1])} · {runtime_label}"
    else:
        title = "Walton Weekly Update ⚠"
        failed = build.get("failed", [])
        if failed:
            message = f"Dashboard build failed: {', '.join(failed)}"
        elif not git_ok:
            message = "Pipeline ran but git push failed"
        else:
            message = "See logs/weekly_update.log"

    send_notification(title, message, success=success)

    # Exit non-zero if anything important failed
    if not build_ok or not git_ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
