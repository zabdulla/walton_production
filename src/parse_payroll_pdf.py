"""
Parse bi-weekly pay period PDF reports and integrate with production data.

Extracts employee clock hours from time-clock PDFs, maintains an aggregated
payroll Excel file, generates/updates an employee roster, and compares
payroll hours against production-reported man-hours.

Usage:
    python src/parse_payroll_pdf.py --pdf path/to/report.pdf         # parse + aggregate
    python src/parse_payroll_pdf.py --init-roster                     # auto-generate roster
    python src/parse_payroll_pdf.py --compare 2026-03-23 2026-04-05  # run comparison
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import pymupdf
except ImportError:
    pymupdf = None

from config import (
    DEFAULT_AGGREGATED_DATA,
    DEFAULT_PAYROLL_DATA,
    EMPLOYEE_ROSTER_PATH,
    LABOR_RATE,
    OT_MULTIPLIER_1,
    OT_MULTIPLIER_2,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column header aliases — handles varying PDF layouts
# ---------------------------------------------------------------------------
_HEADER_ALIASES: dict[str, list[str]] = {
    "employee_name": ["EMPLOYEE NAME", "EMPLOYEE_NAME", "NAME"],
    "first_name": ["FIRST NAME", "FIRST_NAME", "FIRST"],
    "last_name": ["LAST NAME", "LAST_NAME", "LAST"],
    "department": ["DEPARTMENT NAME", "DEPARTMENT", "DEPT"],
    "reg": ["REG", "REGULAR", "REG HRS"],
    "ot1": ["OT1", "OT 1", "OVERTIME1", "OVERTIME 1", "OT"],
    "ot2": ["OT2", "OT 2", "OVERTIME2", "OVERTIME 2", "DBL OT"],
    "vac": ["VAC", "VACATION"],
    "hol": ["HOL", "HOLIDAY"],
    "sick": ["SIC", "SICK"],
    "other": ["OTH", "OTHER"],
    "total": ["TOTAL", "TOTAL HRS", "TOTAL HOURS"],
}


def _safe_float(val: Any) -> float:
    """Convert a cell value to float, returning 0.0 on failure."""
    if val is None or val == "":
        return 0.0
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _match_header(cell_text: str) -> str | None:
    """Match a header cell to a known column key, or None."""
    text = cell_text.strip().upper()
    for key, aliases in _HEADER_ALIASES.items():
        if text in aliases:
            return key
    return None


def _extract_date_range(rows: list[list[str]]) -> tuple[str, str] | None:
    """Scan the first few rows for a date range like MM/DD/YYYY - MM/DD/YYYY."""
    pattern = re.compile(
        r"(\d{1,2}/\d{1,2}/\d{4})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{4})"
    )
    for row in rows[:5]:
        for cell in row:
            if cell:
                m = pattern.search(str(cell))
                if m:
                    return m.group(1), m.group(2)
    return None


# ---------------------------------------------------------------------------
# PDF Parsing
# ---------------------------------------------------------------------------

def parse_payroll_pdf(pdf_path: Path) -> dict:
    """
    Parse a pay period PDF into structured data.

    Returns dict with keys:
        period_start, period_end : str (MM/DD/YYYY)
        employees : list[dict] with per-employee hour breakdown
        totals : dict with sum columns
    """
    if pymupdf is None:
        raise ImportError("pymupdf is required: pip install pymupdf")

    doc = pymupdf.open(str(pdf_path))
    page = doc[0]
    tables = page.find_tables()

    if not tables.tables:
        raise ValueError(f"No tables found in {pdf_path}")

    raw = tables.tables[0].extract()
    doc.close()

    # --- Extract date range from header rows ---
    date_range = _extract_date_range(raw)
    if date_range is None:
        raise ValueError(f"Could not find date range in {pdf_path}")
    period_start, period_end = date_range

    # --- Find the header row (contains "EMPLOYEE NAME" and "TOTAL") ---
    header_idx = None
    col_map: dict[str, int] = {}
    for i, row in enumerate(raw):
        upper_cells = [str(c).strip().upper() if c else "" for c in row]
        # Check if this row has at least "EMPLOYEE NAME" and "TOTAL"
        has_name = any("EMPLOYEE NAME" in c or "NAME" in c for c in upper_cells)
        has_total = any(c == "TOTAL" for c in upper_cells)
        if has_name and has_total:
            header_idx = i
            for j, cell in enumerate(row):
                if cell:
                    key = _match_header(str(cell))
                    if key:
                        col_map[key] = j
            break

    if header_idx is None:
        raise ValueError(f"Could not find header row in {pdf_path}")

    logger.info(f"Header at row {header_idx}, columns: {col_map}")

    # --- Parse employee rows ---
    employees = []
    totals_row = None

    for row in raw[header_idx + 1:]:
        # Skip blank rows
        name = str(row[col_map.get("employee_name", 0)] or "").strip()
        if not name:
            continue

        # Stop at TOTAL row
        if name.upper() == "TOTAL":
            totals_row = row
            break

        emp = {
            "employee_name": name,
            "first_name": str(row[col_map["first_name"]] or "").strip() if "first_name" in col_map else "",
            "last_name": str(row[col_map["last_name"]] or "").strip() if "last_name" in col_map else "",
            "department": str(row[col_map["department"]] or "").strip() if "department" in col_map else "",
            "reg": _safe_float(row[col_map["reg"]]) if "reg" in col_map else 0.0,
            "ot1": _safe_float(row[col_map["ot1"]]) if "ot1" in col_map else 0.0,
            "ot2": _safe_float(row[col_map["ot2"]]) if "ot2" in col_map else 0.0,
            "vac": _safe_float(row[col_map["vac"]]) if "vac" in col_map else 0.0,
            "hol": _safe_float(row[col_map["hol"]]) if "hol" in col_map else 0.0,
            "sick": _safe_float(row[col_map["sick"]]) if "sick" in col_map else 0.0,
            "other": _safe_float(row[col_map["other"]]) if "other" in col_map else 0.0,
            "total": _safe_float(row[col_map["total"]]) if "total" in col_map else 0.0,
        }
        emp["worked_hours"] = emp["reg"] + emp["ot1"] + emp["ot2"]
        emp["pto_hours"] = emp["vac"] + emp["hol"] + emp["sick"] + emp["other"]
        emp["period_start"] = period_start
        emp["period_end"] = period_end
        employees.append(emp)

    # Parse totals row
    totals = {}
    if totals_row is not None:
        for key in ["reg", "ot1", "ot2", "vac", "hol", "sick", "other", "total"]:
            if key in col_map:
                totals[key] = _safe_float(totals_row[col_map[key]])

    logger.info(f"Parsed {len(employees)} employees for period {period_start} - {period_end}")

    return {
        "period_start": period_start,
        "period_end": period_end,
        "employees": employees,
        "totals": totals,
    }


def aggregate_payroll(parsed: dict, output_path: Path = DEFAULT_PAYROLL_DATA) -> pd.DataFrame:
    """Append parsed payroll data to the aggregated payroll Excel file."""
    df_new = pd.DataFrame(parsed["employees"])

    if output_path.exists():
        df_existing = pd.read_excel(output_path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Dedup on (employee_name, period_start)
    before = len(df_combined)
    df_combined.drop_duplicates(
        subset=["employee_name", "period_start"], keep="last", inplace=True
    )
    dupes = before - len(df_combined)
    if dupes:
        logger.info(f"Dropped {dupes} duplicate payroll row(s)")

    df_combined.sort_values(["period_start", "employee_name"], inplace=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_excel(output_path, index=False)
    logger.info(f"Payroll data saved to {output_path} ({len(df_combined)} rows)")
    return df_combined


# ---------------------------------------------------------------------------
# Roster generation
# ---------------------------------------------------------------------------

def generate_roster(
    payroll_path: Path = DEFAULT_PAYROLL_DATA,
    production_path: Path = DEFAULT_AGGREGATED_DATA,
    output_path: Path = EMPLOYEE_ROSTER_PATH,
) -> dict:
    """
    Auto-generate employee roster by cross-referencing payroll names
    with production operator names.

    Returns the roster dict and writes to JSON.
    """
    # Load payroll employees
    df_payroll = pd.read_excel(payroll_path)
    payroll_employees = df_payroll.groupby("employee_name").first().reset_index()

    # Load production data and build operator → machines map
    df_prod = pd.read_excel(production_path)
    ops = df_prod[df_prod["Operator"].notna()].copy()
    ops["Operator"] = ops["Operator"].str.strip()
    exploded = ops.assign(Operator=ops["Operator"].str.split(",")).explode("Operator")
    exploded["Operator"] = exploded["Operator"].str.strip()

    op_machines: dict[str, list[str]] = {}
    op_hours: dict[str, float] = {}
    for op_name in exploded["Operator"].unique():
        if not op_name:
            continue
        subset = exploded[exploded["Operator"] == op_name]
        op_machines[op_name] = sorted(subset["Machine_Name"].unique().tolist())
        op_hours[op_name] = float(subset["Man_Hours"].sum())

    # Match payroll employees to production operators
    roster = {
        "_meta": {
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "notes": "Auto-generated — review and correct before use",
        },
        "employees": {},
    }

    all_prod_names = set(op_machines.keys())
    matched_prod_names: set[str] = set()

    for _, emp in payroll_employees.iterrows():
        full_name = emp["employee_name"]
        first = emp.get("first_name", "").strip()
        last = emp.get("last_name", "").strip()

        # Find production aliases matching this employee's first name
        aliases = []
        machines_found: list[str] = []
        best_machine = None
        best_hours = 0.0

        # Exact first name match
        candidates = [n for n in all_prod_names if n.lower() == first.lower()]
        # Partial / nickname matches
        if not candidates:
            candidates = [
                n for n in all_prod_names
                if first.lower().startswith(n.lower()) or n.lower().startswith(first.lower())
            ]

        for cand in candidates:
            aliases.append(cand)
            matched_prod_names.add(cand)
            machines_found.extend(op_machines.get(cand, []))
            hrs = op_hours.get(cand, 0.0)
            if hrs > best_hours:
                best_hours = hrs
                # Primary machine = one with most hours for this operator
                cand_subset = exploded[exploded["Operator"] == cand]
                if not cand_subset.empty:
                    by_machine = cand_subset.groupby("Machine_Name")["Man_Hours"].sum()
                    best_machine = by_machine.idxmax()

        machines_found = sorted(set(machines_found))
        role = "machine_operator" if aliases and best_hours > 10 else "shipping_receiving"

        roster["employees"][full_name] = {
            "role": role,
            "production_aliases": sorted(set(aliases)),
            "primary_machine": best_machine,
            "secondary_machines": [m for m in machines_found if m != best_machine],
        }

    # Report unmatched production operators
    unmatched = all_prod_names - matched_prod_names
    if unmatched:
        roster["_meta"]["unmatched_production_operators"] = sorted(unmatched)

    # Write roster
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(roster, f, indent=2)
    logger.info(f"Roster saved to {output_path} ({len(roster['employees'])} employees)")

    return roster


# ---------------------------------------------------------------------------
# Payroll vs Production comparison
# ---------------------------------------------------------------------------

def load_roster(path: Path = EMPLOYEE_ROSTER_PATH) -> dict:
    """Load the employee roster JSON."""
    with open(path) as f:
        return json.load(f)


def compare_payroll_to_production(
    period_start: str,
    period_end: str,
    payroll_path: Path = DEFAULT_PAYROLL_DATA,
    production_path: Path = DEFAULT_AGGREGATED_DATA,
    roster_path: Path = EMPLOYEE_ROSTER_PATH,
) -> pd.DataFrame:
    """
    Compare payroll clock hours to production-reported man-hours for a pay period.

    Returns a DataFrame with per-employee breakdown:
        employee_name, role, clock_total, pto_hours, worked_hours,
        production_hours, sr_hours, gap_hours, labor_cost_clock,
        labor_cost_production
    """
    roster = load_roster(roster_path)
    employees_map = roster.get("employees", {})

    # Load payroll data for this period
    df_payroll = pd.read_excel(payroll_path)
    mask = df_payroll["period_start"] == period_start
    df_period = df_payroll[mask].copy()
    if df_period.empty:
        raise ValueError(f"No payroll data for period starting {period_start}")

    # Load production data for the date range
    start_dt = pd.to_datetime(period_start)
    end_dt = pd.to_datetime(period_end)
    df_prod = pd.read_excel(production_path)
    df_prod["Date"] = pd.to_datetime(df_prod["Date"])
    df_prod_period = df_prod[(df_prod["Date"] >= start_dt) & (df_prod["Date"] <= end_dt)].copy()

    # Build production hours by operator alias
    prod_ops = df_prod_period[df_prod_period["Operator"].notna()].copy()
    prod_ops["Operator"] = prod_ops["Operator"].str.strip()
    prod_exploded = prod_ops.assign(
        Operator=prod_ops["Operator"].str.split(",")
    ).explode("Operator")
    prod_exploded["Operator"] = prod_exploded["Operator"].str.strip()

    alias_hours: dict[str, float] = {}
    for name in prod_exploded["Operator"].unique():
        if name:
            alias_hours[name] = float(
                prod_exploded[prod_exploded["Operator"] == name]["Man_Hours"].sum()
            )

    # Build comparison rows
    rows = []
    for _, emp_row in df_period.iterrows():
        name = emp_row["employee_name"]
        info = employees_map.get(name, {})
        role = info.get("role", "unknown")
        aliases = info.get("production_aliases", [])

        clock_total = emp_row["total"]
        pto_hours = emp_row["pto_hours"]
        worked_hours = emp_row["worked_hours"]
        reg = emp_row["reg"]
        ot1 = emp_row["ot1"]
        ot2 = emp_row["ot2"]

        # Sum production hours from all aliases
        production_hours = sum(alias_hours.get(a, 0.0) for a in aliases)

        # SR hours = worked hours if role is shipping_receiving
        sr_hours = worked_hours if role == "shipping_receiving" else 0.0

        # Gap = worked - production - SR
        gap_hours = max(0.0, worked_hours - production_hours - sr_hours)

        # Labor cost using actual clock hours (including OT multipliers)
        labor_cost_clock = (reg * LABOR_RATE) + (ot1 * LABOR_RATE * OT_MULTIPLIER_1) + (ot2 * LABOR_RATE * OT_MULTIPLIER_2) + (pto_hours * LABOR_RATE)

        # Labor cost as reported in production data
        labor_cost_production = production_hours * LABOR_RATE

        rows.append({
            "employee_name": name,
            "role": role,
            "clock_total": clock_total,
            "reg": reg,
            "ot1": ot1,
            "ot2": ot2,
            "pto_hours": pto_hours,
            "worked_hours": worked_hours,
            "production_hours": production_hours,
            "sr_hours": sr_hours,
            "gap_hours": gap_hours,
            "labor_cost_clock": round(labor_cost_clock, 2),
            "labor_cost_production": round(labor_cost_production, 2),
        })

    df_result = pd.DataFrame(rows)
    return df_result


def print_comparison(df: pd.DataFrame) -> None:
    """Print a formatted comparison summary."""
    is_tty = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
    green = "\033[32m" if is_tty else ""
    red = "\033[31m" if is_tty else ""
    yellow = "\033[33m" if is_tty else ""
    bold = "\033[1m" if is_tty else ""
    reset = "\033[0m" if is_tty else ""

    total_clock = df["clock_total"].sum()
    total_pto = df["pto_hours"].sum()
    total_worked = df["worked_hours"].sum()
    total_production = df["production_hours"].sum()
    total_sr = df["sr_hours"].sum()
    total_gap = df["gap_hours"].sum()
    capture_pct = (total_production / total_worked * 100) if total_worked else 0

    print(f"\n{bold}Payroll vs Production Comparison{reset}")
    print("=" * 70)
    print(f"  Total Clock Hours:      {total_clock:>10.1f}")
    print(f"  PTO Hours:              {total_pto:>10.1f}")
    print(f"  Worked Hours:           {total_worked:>10.1f}")
    print(f"  {green}Production Hours:       {total_production:>10.1f}{reset}")
    print(f"  S&R Hours:              {total_sr:>10.1f}")
    print(f"  {red}Unaccounted Gap:        {total_gap:>10.1f}{reset}")
    print(f"  Capture Rate:           {capture_pct:>9.1f}%")
    print()
    print(f"  Clock Labor Cost:       ${df['labor_cost_clock'].sum():>10,.2f}")
    print(f"  Production Labor Cost:  ${df['labor_cost_production'].sum():>10,.2f}")
    print(f"  Hidden Overhead:        ${df['labor_cost_clock'].sum() - df['labor_cost_production'].sum():>10,.2f}")
    print()

    # Per-employee table
    print(f"{'Employee':<28} {'Role':<18} {'Clock':>7} {'Worked':>7} {'Prod':>7} {'Gap':>7}")
    print("-" * 92)
    for _, row in df.sort_values("gap_hours", ascending=False).iterrows():
        role_display = row["role"].replace("_", "/").title()
        gap_color = red if row["gap_hours"] > 10 else (yellow if row["gap_hours"] > 0 else "")
        gap_reset = reset if gap_color else ""
        print(
            f"  {row['employee_name']:<26} {role_display:<18} "
            f"{row['clock_total']:>7.1f} {row['worked_hours']:>7.1f} "
            f"{row['production_hours']:>7.1f} {gap_color}{row['gap_hours']:>7.1f}{gap_reset}"
        )
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Parse payroll PDFs and compare to production data.")
    parser.add_argument("--pdf", type=Path, help="Path to a pay period PDF to parse and aggregate")
    parser.add_argument("--init-roster", action="store_true", help="Auto-generate employee roster from data")
    parser.add_argument("--compare", nargs=2, metavar=("START", "END"),
                        help="Compare payroll to production for a date range (MM/DD/YYYY MM/DD/YYYY)")
    parser.add_argument("--payroll-data", type=Path, default=DEFAULT_PAYROLL_DATA,
                        help="Path to aggregated payroll Excel")
    args = parser.parse_args()

    if args.pdf:
        print(f"Parsing {args.pdf}...")
        parsed = parse_payroll_pdf(args.pdf)
        print(f"  Period: {parsed['period_start']} - {parsed['period_end']}")
        print(f"  Employees: {len(parsed['employees'])}")
        print(f"  Total hours: {parsed['totals'].get('total', 0):.2f}")
        df = aggregate_payroll(parsed, args.payroll_data)
        print(f"  Aggregated to {args.payroll_data} ({len(df)} total rows)")

    if args.init_roster:
        if not args.payroll_data.exists() and not (args.pdf and args.payroll_data == DEFAULT_PAYROLL_DATA):
            print("ERROR: No payroll data found. Parse a PDF first with --pdf.")
            sys.exit(1)
        print("\nGenerating employee roster...")
        roster = generate_roster(payroll_path=args.payroll_data)
        emp = roster["employees"]
        operators = sum(1 for e in emp.values() if e["role"] == "machine_operator")
        sr = sum(1 for e in emp.values() if e["role"] == "shipping_receiving")
        print(f"  {operators} machine operators, {sr} shipping/receiving")
        unmatched = roster.get("_meta", {}).get("unmatched_production_operators", [])
        if unmatched:
            print(f"  {len(unmatched)} production operators not matched to any payroll employee:")
            for name in unmatched:
                print(f"    - {name}")
        print(f"\nRoster saved to {EMPLOYEE_ROSTER_PATH}")
        print("IMPORTANT: Review and correct before using for comparisons.")

    if args.compare:
        start, end = args.compare
        print(f"\nComparing payroll to production for {start} - {end}...")
        df = compare_payroll_to_production(start, end, payroll_path=args.payroll_data)
        print_comparison(df)


if __name__ == "__main__":
    main()
