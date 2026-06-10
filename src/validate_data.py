"""Data validation for the processing analysis pipeline.

Pre-build health check that runs automated quality checks on the aggregated
daily data and prints a clear report.  Importable (call ``run_validation()``)
or runnable standalone (``python validate_data.py``).

Exit code 1 when critical issues are found (unmapped products, duplicates).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from config import (
    ALL_MACHINES,
    DEFAULT_AGGREGATED_DATA,
    DEFAULT_PAYROLL_DATA,
    EMPLOYEE_ROSTER_PATH,
    MACHINE_WEEKLY_CAPACITY,
    PRODUCT_CATEGORY_MAP,
    PRODUCT_TYPO_MAP,
)

# ---------------------------------------------------------------------------
# ANSI helpers — colour only when stdout is an interactive terminal
# ---------------------------------------------------------------------------
_USE_COLOR = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    """Wrap *text* with an ANSI escape if colour is enabled."""
    if not _USE_COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def _bold(text: str) -> str:
    return _c("1", text)


def _green(text: str) -> str:
    return _c("32", text)


def _yellow(text: str) -> str:
    return _c("33", text)


def _red(text: str) -> str:
    return _c("31", text)


def _dim(text: str) -> str:
    return _c("2", text)


# ---------------------------------------------------------------------------
# Validation checks
# ---------------------------------------------------------------------------

def _check_unmapped_products(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Return list of ``{product, count}`` for products with no category."""
    products = df["Output_Product"].copy()
    products = products.replace(PRODUCT_TYPO_MAP)
    unmapped = products[~products.isin(PRODUCT_CATEGORY_MAP) & products.notna() & (products != "")]
    if unmapped.empty:
        return []
    counts = unmapped.value_counts()
    return [{"product": name, "count": int(cnt)} for name, cnt in counts.items()]


def _check_missing_weeks(df: pd.DataFrame) -> list[str]:
    """Return ISO-formatted Monday dates for weeks with zero records."""
    dates = pd.to_datetime(df["Date"])
    min_date = dates.min()
    max_date = dates.max()
    if pd.isna(min_date) or pd.isna(max_date):
        return []

    # Build the full range of Mondays spanning the data
    all_mondays = pd.date_range(
        start=min_date - pd.Timedelta(days=min_date.weekday()),
        end=max_date,
        freq="W-MON",
    )
    present_mondays = set(
        (dates - pd.to_timedelta(dates.dt.weekday, unit="D")).dt.normalize().unique()
    )
    missing = sorted(
        m.strftime("%Y-%m-%d")
        for m in all_mondays
        if m not in present_mondays
    )
    return missing


def _check_latest_week_shifts(
    df: pd.DataFrame,
    expected_shifts: tuple[str, ...] = ("1st", "2nd", "3rd"),
) -> dict[str, Any]:
    """Missed-report alarm: expected shifts with zero rows in the latest week.

    Each shift sends its own weekly report file; a missing shift here almost
    always means one report was never emailed (or failed to download), and the
    week would otherwise publish silently incomplete.
    """
    dates = pd.to_datetime(df["Date"])
    if dates.isna().all():
        return {"week_start": None, "missing_shifts": []}
    week_start = (dates - pd.to_timedelta(dates.dt.weekday, unit="D")).dt.normalize()
    latest = week_start.max()
    present = set(df.loc[week_start == latest, "Shift"].astype(str).unique())
    missing = [s for s in expected_shifts if s not in present]
    return {
        "week_start": str(pd.Timestamp(latest).date()),
        "missing_shifts": missing,
    }


def _check_weekly_output_anomalies(
    df: pd.DataFrame,
    window: int = 4,
    sigma: float = 2.0,
    recent_weeks: int = 8,
) -> list[dict[str, Any]]:
    """Flag machine-weeks whose total output deviates > *sigma* standard
    deviations from the rolling mean of the preceding *window* weeks.

    Only weeks within the last *recent_weeks* of data are reported — old
    anomalies would otherwise spam every run. Catches both data-entry errors
    (extra zero) and real production collapses worth a look.
    """
    dates = pd.to_datetime(df["Date"])
    d = df.copy()
    d["_week"] = (dates - pd.to_timedelta(dates.dt.weekday, unit="D")).dt.normalize()
    weekly = d.groupby(["Machine_Name", "_week"])["Actual_Output"].sum().reset_index()
    cutoff = weekly["_week"].max() - pd.Timedelta(weeks=recent_weeks)

    anomalies: list[dict[str, Any]] = []
    for machine, grp in weekly.groupby("Machine_Name"):
        grp = grp.sort_values("_week").reset_index(drop=True)
        if len(grp) < window + 1:
            continue
        # Stats from the *preceding* weeks only, so the anomaly itself
        # doesn't inflate the baseline it's compared against.
        prior_mean = grp["Actual_Output"].shift(1).rolling(window).mean()
        prior_std = grp["Actual_Output"].shift(1).rolling(window).std()
        for i in range(len(grp)):
            if grp["_week"].iloc[i] < cutoff:
                continue
            m, s = prior_mean.iloc[i], prior_std.iloc[i]
            if pd.isna(m) or pd.isna(s) or s == 0:
                continue
            val = float(grp["Actual_Output"].iloc[i])
            if abs(val - m) > sigma * s:
                anomalies.append({
                    "machine": str(machine),
                    "week_start": str(pd.Timestamp(grp["_week"].iloc[i]).date()),
                    "output": round(val, 1),
                    "expected": round(float(m), 1),
                    "deviation_sigma": round(abs(val - m) / float(s), 1),
                })
    return anomalies


def _check_missing_operators(df: pd.DataFrame) -> dict[str, int]:
    """Return ``{machine_name: count}`` for rows with blank Operator."""
    mask = df["Operator"].isna() | (df["Operator"].astype(str).str.strip() == "")
    missing = df.loc[mask]
    if missing.empty:
        return {}
    counts = missing.groupby("Machine_Name").size()
    return {str(machine): int(cnt) for machine, cnt in counts.items()}


def _check_duplicates(df: pd.DataFrame) -> dict[str, Any]:
    """Detect exact duplicates on key columns.  Returns count + examples."""
    dup_cols = ["Date", "Shift", "Machine_Name", "Output_Product", "Actual_Output"]
    duped = df[df.duplicated(subset=dup_cols, keep=False)]
    count = len(duped) - len(duped.drop_duplicates(subset=dup_cols))  # extra copies
    examples: list[dict[str, Any]] = []
    if not duped.empty:
        sample = duped.head(6)  # show a few example rows
        for _, row in sample.iterrows():
            examples.append({
                "Date": str(row["Date"].date()) if hasattr(row["Date"], "date") else str(row["Date"]),
                "Shift": row["Shift"],
                "Machine_Name": row["Machine_Name"],
                "Output_Product": row["Output_Product"],
                "Actual_Output": row["Actual_Output"],
            })
    return {"count": count, "examples": examples}


def _check_anomalous_values(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Flag rows with values that likely indicate data-entry errors."""
    flags: list[dict[str, Any]] = []
    rules: list[tuple[str, str, float]] = [
        ("Actual_Output", "> 50,000", 50_000),
        ("Machine_Hours", "> 24", 24),
        ("Man_Hours", "> 24", 24),
        ("Output_per_Hour", "> 5,000", 5_000),
    ]
    for col, label, threshold in rules:
        if col not in df.columns:
            continue
        bad = df[pd.to_numeric(df[col], errors="coerce") > threshold]
        for _, row in bad.iterrows():
            flags.append({
                "rule": f"{col} {label}",
                "value": row[col],
                "Date": str(row["Date"].date()) if hasattr(row["Date"], "date") else str(row["Date"]),
                "Machine_Name": row.get("Machine_Name", ""),
                "Shift": row.get("Shift", ""),
            })
    return flags


def _check_payroll_roster(
    payroll_path: Path = DEFAULT_PAYROLL_DATA,
    roster_path: Path = EMPLOYEE_ROSTER_PATH,
) -> dict[str, Any]:
    """Check that every payroll employee is in the roster, and flag unmatched production operators.

    Returns dict with keys:
        status: 'missing_data' | 'ok' | 'issues'
        unrostered_employees: list[str] — employees in payroll but not roster
        unclassified_aliases: list[str] — payroll employees with no role set
        unmatched_production_ops: list[str] — production operators no payroll employee claims
        latest_period: dict with capture rate summary for the most recent period
    """
    result: dict[str, Any] = {
        "status": "missing_data",
        "unrostered_employees": [],
        "unclassified_aliases": [],
        "unmatched_production_ops": [],
        "latest_period": None,
    }

    if not payroll_path.exists() or not roster_path.exists():
        return result

    df_pay = pd.read_excel(payroll_path)
    with open(roster_path) as f:
        roster = json.load(f)
    employees_map = roster.get("employees", {})

    # Unrostered employees: in payroll but not in roster
    payroll_names = set(df_pay["employee_name"].unique())
    rostered_names = set(employees_map.keys())
    result["unrostered_employees"] = sorted(payroll_names - rostered_names)

    # Unclassified: rostered but role is unknown/invalid, or role requires aliases that aren't set
    valid_roles = {"machine_operator", "shipping_receiving", "maintenance", "hybrid_sr", "supervisor", "unknown"}
    for name, info in employees_map.items():
        role = info.get("role", "unknown")
        aliases = info.get("production_aliases", [])
        if role == "unknown" or role not in valid_roles:
            result["unclassified_aliases"].append(name)
        elif role in ("machine_operator", "hybrid_sr", "supervisor") and not aliases:
            result["unclassified_aliases"].append(f"{name} ({role}, no aliases)")

    # Unmatched production operators: in production data but not in any roster alias
    all_aliases = set()
    for info in employees_map.values():
        for alias in info.get("production_aliases", []):
            all_aliases.add(alias)
    meta_unmatched = roster.get("_meta", {}).get("unmatched_production_operators", [])
    # Filter meta list to exclude any that have since been added to aliases
    result["unmatched_production_ops"] = [
        op for op in meta_unmatched if op not in all_aliases
    ]

    # Latest period capture rate
    if len(df_pay):
        # Pick the most recent period_start by date sort
        try:
            df_pay["_sort"] = pd.to_datetime(df_pay["period_start"], format="%m/%d/%Y")
            latest_start = df_pay.sort_values("_sort").iloc[-1]["period_start"]
            latest = df_pay[df_pay["period_start"] == latest_start]
            clock = float(latest["total"].sum())
            pto = float(latest["pto_hours"].sum()) if "pto_hours" in latest.columns else 0.0
            worked = float(latest["worked_hours"].sum()) if "worked_hours" in latest.columns else clock - pto
            result["latest_period"] = {
                "period_start": latest_start,
                "period_end": latest.iloc[0]["period_end"],
                "employees": len(latest),
                "clock_hours": round(clock, 1),
                "worked_hours": round(worked, 1),
                "pto_hours": round(pto, 1),
            }
        except Exception:
            pass

    has_issues = bool(
        result["unrostered_employees"]
        or result["unclassified_aliases"]
    )
    result["status"] = "issues" if has_issues else "ok"
    return result


def _check_completeness(df: pd.DataFrame, n_weeks: int = 4) -> list[dict[str, Any]]:
    """Per-week summary for the most recent *n_weeks* weeks."""
    dates = pd.to_datetime(df["Date"])
    df = df.copy()
    df["_week_start"] = dates - pd.to_timedelta(dates.dt.weekday, unit="D")
    latest_weeks = sorted(df["_week_start"].unique())[-n_weeks:]
    summaries: list[dict[str, Any]] = []
    for week in latest_weeks:
        subset = df[df["_week_start"] == week]
        avg_quality = subset["Data_Quality_Score"].mean() if "Data_Quality_Score" in subset.columns else None
        summaries.append({
            "week_start": str(pd.Timestamp(week).date()),
            "total_records": len(subset),
            "machines_active": int(subset["Machine_Name"].nunique()),
            "avg_quality_score": round(float(avg_quality), 2) if avg_quality is not None and not pd.isna(avg_quality) else None,
        })
    return summaries


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_validation(path: Path = DEFAULT_AGGREGATED_DATA) -> dict[str, Any]:
    """Run all validation checks and return a results dict.

    Parameters
    ----------
    path : Path
        Path to the aggregated daily data Excel file.

    Returns
    -------
    dict
        Keys: ``unmapped_products``, ``missing_weeks``, ``missing_operators``,
        ``duplicates_count``, ``duplicate_examples``, ``anomalous_values``,
        ``completeness``, ``total_rows``.
    """
    df = pd.read_excel(path)
    df["Date"] = pd.to_datetime(df["Date"])

    unmapped = _check_unmapped_products(df)
    missing_weeks = _check_missing_weeks(df)
    latest_week_shifts = _check_latest_week_shifts(df)
    output_anomalies = _check_weekly_output_anomalies(df)
    missing_ops = _check_missing_operators(df)
    dup_info = _check_duplicates(df)
    anomalies = _check_anomalous_values(df)
    completeness = _check_completeness(df)
    payroll = _check_payroll_roster()

    return {
        "total_rows": len(df),
        "unmapped_products": unmapped,
        "missing_weeks": missing_weeks,
        "latest_week_shifts": latest_week_shifts,
        "output_anomalies": output_anomalies,
        "missing_operators": missing_ops,
        "duplicates_count": dup_info["count"],
        "duplicate_examples": dup_info["examples"],
        "anomalous_values": anomalies,
        "completeness": completeness,
        "payroll": payroll,
    }


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

def print_report(results: dict[str, Any]) -> None:
    """Print a formatted validation report to stdout."""

    def _header(title: str) -> None:
        print()
        print(_bold(f"{'=' * 60}"))
        print(_bold(f"  {title}"))
        print(_bold(f"{'=' * 60}"))

    def _ok(msg: str) -> None:
        print(f"  {_green('PASS')}  {msg}")

    def _warn(msg: str) -> None:
        print(f"  {_yellow('WARN')}  {msg}")

    def _fail(msg: str) -> None:
        print(f"  {_red('FAIL')}  {msg}")

    # Title
    print()
    print(_bold("Processing Analysis - Data Validation Report"))
    print(_dim(f"Total rows: {results['total_rows']:,}"))

    # 1. Unmapped products
    _header("1. Unmapped Products")
    items = results["unmapped_products"]
    if not items:
        _ok("All products map to a known category.")
    else:
        _fail(f"{len(items)} unmapped product(s) found:")
        for entry in items:
            print(f"        {entry['product']!r:40s}  ({entry['count']:,} rows)")

    # 2. Missing weeks
    _header("2. Missing Weeks")
    missing_w = results["missing_weeks"]
    if not missing_w:
        _ok("No gaps in weekly coverage.")
    else:
        _warn(f"{len(missing_w)} week(s) with zero records:")
        for w in missing_w:
            print(f"        Week of {w}")

    # 2b. Latest-week shift coverage (missed-report alarm)
    lws = results.get("latest_week_shifts") or {}
    if lws.get("week_start"):
        _header("2b. Latest Week Shift Coverage")
        if not lws["missing_shifts"]:
            _ok(f"All shifts reported for week of {lws['week_start']}.")
        else:
            _warn(
                f"Week of {lws['week_start']} has NO data for shift(s): "
                f"{', '.join(lws['missing_shifts'])} — a report may not have been sent."
            )

    # 2c. Weekly output anomalies (rolling 2-sigma per machine)
    out_anom = results.get("output_anomalies") or []
    _header("2c. Weekly Output Anomalies")
    if not out_anom:
        _ok("No machine-weeks deviate >2σ from their rolling average.")
    else:
        _warn(f"{len(out_anom)} machine-week(s) deviate >2σ from rolling average:")
        for a in out_anom:
            print(
                f"        {a['machine']:<30s} week of {a['week_start']}: "
                f"{a['output']:>12,.0f} lbs (expected ~{a['expected']:,.0f}, "
                f"{a['deviation_sigma']}σ)"
            )

    # 3. Missing operators
    _header("3. Missing Operators")
    missing_ops = results["missing_operators"]
    if not missing_ops:
        _ok("All rows have an operator.")
    else:
        total = sum(missing_ops.values())
        _warn(f"{total:,} row(s) missing an operator:")
        for machine, cnt in sorted(missing_ops.items(), key=lambda x: -x[1]):
            print(f"        {machine:40s}  {cnt:,} rows")

    # 4. Duplicate rows
    _header("4. Duplicate Rows")
    dup_count = results["duplicates_count"]
    if dup_count == 0:
        _ok("No duplicate rows detected.")
    else:
        _fail(f"{dup_count:,} duplicate row(s) detected.")
        examples = results["duplicate_examples"]
        if examples:
            print(f"        {'Date':<12s} {'Shift':<10s} {'Machine':<30s} {'Product':<25s} {'Output':>10s}")
            print(f"        {'-'*12} {'-'*10} {'-'*30} {'-'*25} {'-'*10}")
            for ex in examples:
                print(
                    f"        {ex['Date']:<12s} {str(ex['Shift']):<10s} "
                    f"{str(ex['Machine_Name']):<30s} {str(ex['Output_Product']):<25s} "
                    f"{str(ex['Actual_Output']):>10s}"
                )

    # 5. Anomalous values
    _header("5. Anomalous Values")
    anomalies = results["anomalous_values"]
    if not anomalies:
        _ok("No anomalous values found.")
    else:
        _warn(f"{len(anomalies)} anomalous value(s) flagged:")
        for a in anomalies:
            print(
                f"        {a['Date']:<12s} {a['Machine_Name']:<30s} "
                f"{a['rule']:<28s} value={a['value']}"
            )

    # 6. Payroll roster coverage
    _header("6. Payroll Roster Coverage")
    payroll = results.get("payroll", {})
    status = payroll.get("status", "missing_data")
    if status == "missing_data":
        _dim("  (No payroll data found \u2014 skip. Run parse_payroll_pdf.py to enable.)")
    else:
        unrostered = payroll.get("unrostered_employees", [])
        unclassified = payroll.get("unclassified_aliases", [])
        unmatched = payroll.get("unmatched_production_ops", [])
        latest = payroll.get("latest_period")

        if not unrostered and not unclassified:
            _ok("All payroll employees are in the roster.")
        else:
            if unrostered:
                _fail(f"{len(unrostered)} payroll employee(s) not in roster:")
                for name in unrostered:
                    print(f"        {name}")
            if unclassified:
                _warn(f"{len(unclassified)} employee(s) with unknown role:")
                for name in unclassified:
                    print(f"        {name}")

        if unmatched:
            _warn(f"{len(unmatched)} production operator(s) not mapped to any payroll employee:")
            for name in unmatched[:15]:
                print(f"        {name}")
            if len(unmatched) > 15:
                print(f"        ... and {len(unmatched) - 15} more")

        if latest:
            print()
            print(f"        {_bold('Latest pay period:')} {latest['period_start']} - {latest['period_end']}")
            print(f"        {latest['employees']} employees, "
                  f"{latest['clock_hours']} clock hrs ({latest['worked_hours']} worked, "
                  f"{latest['pto_hours']} PTO)")

    # 7. Data completeness (last 4 weeks)
    _header("7. Data Completeness (Last 4 Weeks)")
    completeness = results["completeness"]
    if not completeness:
        _warn("No recent data found.")
    else:
        print(f"        {'Week of':<14s} {'Records':>8s} {'Machines':>10s} {'Avg Quality':>13s}")
        print(f"        {'-'*14} {'-'*8} {'-'*10} {'-'*13}")
        for wk in completeness:
            quality_str = f"{wk['avg_quality_score']:.2f}" if wk["avg_quality_score"] is not None else "N/A"
            print(
                f"        {wk['week_start']:<14s} {wk['total_records']:>8,} "
                f"{wk['machines_active']:>10} {quality_str:>13}"
            )

    # Summary line
    _header("Summary")
    issues = []
    if results["unmapped_products"]:
        issues.append(f"{len(results['unmapped_products'])} unmapped product(s)")
    if results["duplicates_count"]:
        issues.append(f"{results['duplicates_count']:,} duplicate(s)")
    if results["anomalous_values"]:
        issues.append(f"{len(results['anomalous_values'])} anomalous value(s)")
    if results["missing_operators"]:
        total_missing = sum(results["missing_operators"].values())
        issues.append(f"{total_missing:,} missing operator(s)")
    if results["missing_weeks"]:
        issues.append(f"{len(results['missing_weeks'])} missing week(s)")
    lws = results.get("latest_week_shifts") or {}
    if lws.get("missing_shifts"):
        issues.append(
            f"latest week missing shift(s): {', '.join(lws['missing_shifts'])}"
        )
    if results.get("output_anomalies"):
        issues.append(f"{len(results['output_anomalies'])} weekly output anomaly(ies)")
    payroll = results.get("payroll", {})
    if payroll.get("unrostered_employees"):
        issues.append(f"{len(payroll['unrostered_employees'])} unrostered payroll employee(s)")
    if payroll.get("unmatched_production_ops"):
        issues.append(f"{len(payroll['unmatched_production_ops'])} unmapped production operator(s)")

    if not issues:
        _ok("All checks passed. Data looks healthy.")
    else:
        for issue in issues:
            _warn(issue)
    print()


# ---------------------------------------------------------------------------
# Gating decision — which validation issues should block publishing dashboards?
# ---------------------------------------------------------------------------

def gating_decision(results: dict[str, Any]) -> tuple[bool, list[str]]:
    """Decide whether validation results should block dashboard publication.

    Returns ``(should_block, reasons)``. The orchestrator uses this to decide
    whether to restore the previous aggregated file and skip the dashboard +
    push steps.

    Blocking rules (any one triggers a block):
      • Unmapped products with >= 5 rows total — small counts are tolerable
        because the typo map catches them next time someone reviews; a flood
        of unmapped rows means a new product type appeared and charts will
        be silently wrong until added.
      • Duplicate rows detected after aggregation — aggregation already
        dedups, so duplicates here mean the dedup key is missing a column.
      • Excessive growth-sanity failure — handled at write time by atomic.py.
      • Any payroll employee unrostered (only if payroll data is present).

    Non-blocking (warn-only):
      • Missing weeks (Carl sometimes ships late; missing 1 week is normal).
      • Missing operators (data-quality issue but not pipeline-blocking).
      • Anomalous values (often legitimate; per-machine thresholds would help).
      • New unmapped product with <5 rows (typo, single-occurrence).
    """
    reasons: list[str] = []

    unmapped = results.get("unmapped_products") or []
    unmapped_total_rows = sum(item.get("count", 0) for item in unmapped)
    if unmapped_total_rows >= 5:
        reasons.append(
            f"{len(unmapped)} unmapped product(s) with {unmapped_total_rows} total rows "
            f"(threshold: 5). Add to PRODUCT_CATEGORY_MAP or PRODUCT_TYPO_MAP."
        )

    if results.get("duplicates_count", 0) > 0:
        reasons.append(
            f"{results['duplicates_count']} duplicate row(s) after aggregation. "
            f"Aggregation dedup key may be missing a column."
        )

    payroll = results.get("payroll", {})
    if payroll.get("status") != "missing_data":
        unrostered = payroll.get("unrostered_employees") or []
        if unrostered:
            reasons.append(
                f"{len(unrostered)} payroll employee(s) not in roster: "
                f"{', '.join(unrostered[:3])}. Edit data/employee_roster.json."
            )

    return (len(reasons) > 0, reasons)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = run_validation()
    print_report(results)
    blocked, reasons = gating_decision(results)
    if blocked:
        print()
        print(_red(_bold("GATING: publication would be BLOCKED")))
        for r in reasons:
            print(f"  - {r}")
        sys.exit(1)
