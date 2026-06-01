"""Tests for src/parse_payroll_pdf.py — header matching, date/department parsing."""
from __future__ import annotations

import pandas as pd
import pytest

from parse_payroll_pdf import (
    _extract_date_range,
    _extract_department,
    _match_header,
    _safe_float,
    _sum_production_hours_for_employee,
)


# ---------------------------------------------------------------------------
# _safe_float
# ---------------------------------------------------------------------------

def test_safe_float_basic() -> None:
    assert _safe_float("42.5") == 42.5
    assert _safe_float(10) == 10.0
    assert _safe_float("100") == 100.0


def test_safe_float_strips_commas() -> None:
    assert _safe_float("1,234.56") == 1234.56


def test_safe_float_handles_blank() -> None:
    assert _safe_float(None) == 0.0
    assert _safe_float("") == 0.0


def test_safe_float_handles_garbage() -> None:
    assert _safe_float("not a number") == 0.0
    assert _safe_float("abc123") == 0.0


def test_safe_float_strips_whitespace() -> None:
    assert _safe_float(" 42 ") == 42.0


# ---------------------------------------------------------------------------
# _match_header — maps PDF column headers to canonical keys
# ---------------------------------------------------------------------------

def test_match_header_exact_match() -> None:
    assert _match_header("EMPLOYEE NAME") == "employee_name"
    assert _match_header("FIRST NAME") == "first_name"
    assert _match_header("TOTAL") == "total"


def test_match_header_case_insensitive() -> None:
    assert _match_header("employee name") == "employee_name"
    assert _match_header("Total") == "total"


def test_match_header_handles_whitespace() -> None:
    assert _match_header("  REG  ") == "reg"


def test_match_header_returns_none_for_unknown() -> None:
    assert _match_header("RANDOM JUNK") is None
    assert _match_header("") is None


def test_match_header_overtime_aliases() -> None:
    # OT1/OT2 have multiple aliases
    assert _match_header("OT1") == "ot1"
    assert _match_header("OT 1") == "ot1"


# ---------------------------------------------------------------------------
# _extract_date_range — pulls MM/DD/YYYY range from PDF header rows
# ---------------------------------------------------------------------------

def test_extract_date_range_standard_format() -> None:
    rows = [["Pay Period Report"], ["03/23/2026 - 04/05/2026"], ["other text"]]
    assert _extract_date_range(rows) == ("03/23/2026", "04/05/2026")


def test_extract_date_range_with_en_dash() -> None:
    rows = [["03/23/2026 – 04/05/2026"]]
    assert _extract_date_range(rows) == ("03/23/2026", "04/05/2026")


def test_extract_date_range_no_match() -> None:
    rows = [["just text"], ["more text"]]
    assert _extract_date_range(rows) is None


def test_extract_date_range_only_first_5_rows() -> None:
    rows = [["a"], ["b"], ["c"], ["d"], ["e"], ["03/23/2026 - 04/05/2026"]]
    # 6th row (index 5) shouldn't be searched
    assert _extract_date_range(rows) is None


def test_extract_date_range_ignores_blank_cells() -> None:
    rows = [[None, "", "03/23/2026 - 04/05/2026"]]
    assert _extract_date_range(rows) == ("03/23/2026", "04/05/2026")


# ---------------------------------------------------------------------------
# _extract_department — pulls Walton/Snellville/PRN from header
# ---------------------------------------------------------------------------

def test_extract_department_basic() -> None:
    rows = [["Departments : Walton Logistics"]]
    assert _extract_department(rows) == "Walton Logistics"


def test_extract_department_case_insensitive() -> None:
    rows = [["departments: Snellville"]]
    assert _extract_department(rows) == "Snellville"


def test_extract_department_no_match() -> None:
    rows = [["just text"]]
    assert _extract_department(rows) is None


def test_extract_department_only_first_6_rows() -> None:
    rows = [["a"]] * 7 + [["Departments : Walton"]]
    assert _extract_department(rows) is None


# ---------------------------------------------------------------------------
# _sum_production_hours_for_employee — alias + shift filter
# ---------------------------------------------------------------------------

def _prod_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"Operator": "Steven", "Shift": "1st", "Man_Hours": 8.0},
        {"Operator": "Steven", "Shift": "2nd", "Man_Hours": 8.0},
        {"Operator": "Steve",  "Shift": "1st", "Man_Hours": 4.0},
        {"Operator": "Other",  "Shift": "1st", "Man_Hours": 5.0},
    ])


def test_sum_production_hours_no_aliases_returns_zero() -> None:
    assert _sum_production_hours_for_employee(_prod_df(), aliases=[]) == 0.0


def test_sum_production_hours_matches_aliases() -> None:
    # All Steven/Steve hours regardless of shift
    h = _sum_production_hours_for_employee(_prod_df(), aliases=["Steven", "Steve"])
    assert h == 8.0 + 8.0 + 4.0


def test_sum_production_hours_with_shift_filter() -> None:
    # Steven Broach: 1st shift only
    h = _sum_production_hours_for_employee(_prod_df(),
                                            aliases=["Steven", "Steve"],
                                            shift_filter="1st")
    assert h == 8.0 + 4.0


def test_sum_production_hours_shift_filter_excludes_other_shifts() -> None:
    # Steven Byrd: 2nd shift only
    h = _sum_production_hours_for_employee(_prod_df(),
                                            aliases=["Steven"],
                                            shift_filter="2nd")
    assert h == 8.0
