"""The sheet tab label decides which day a row belongs to.

Regression context: the date-correction heuristic used to try a month/day
swap first and accept it whenever the result landed anywhere inside the
week. For a Tuesday sheet dated 2026-06-07 the swap produced 2026-07-06 —
a Monday inside the right week — so Tuesday's production was stamped onto
Monday. Monday read 62% high, Tuesday vanished from the calendar, and one
row was additionally lost when dedup collapsed it against a real Monday
row. Weekly totals were unaffected, so nothing noticed for weeks.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from aggregate_daily_data import _date_for_sheet_label


def _d(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d")


# ---------------------------------------------------------------------------
# The bug that shipped
# ---------------------------------------------------------------------------

def test_tuesday_sheet_never_lands_on_monday() -> None:
    """The exact 2026-07-06 case: a swap-derived Monday must not win."""
    got = _date_for_sheet_label("Tue", _d("2026-07-06"), _d("2026-07-10"),
                                date_cell=_d("2026-06-07"))
    assert got == _d("2026-07-07")
    assert got.strftime("%a") == "Tue"


@pytest.mark.parametrize("label,expected", [
    ("Mon", "2026-07-06"),
    ("Tue", "2026-07-07"),
    ("Wed", "2026-07-08"),
    ("Thu", "2026-07-09"),
    ("Fri", "2026-07-10"),
])
def test_every_label_maps_to_its_own_weekday(label: str, expected: str) -> None:
    got = _date_for_sheet_label(label, _d("2026-07-06"), _d("2026-07-10"))
    assert got == _d(expected)
    assert got.strftime("%a") == label


# ---------------------------------------------------------------------------
# In-range disagreement: the label still wins
# ---------------------------------------------------------------------------

def test_copy_pasted_date_cell_is_overridden() -> None:
    """2025-02-15: the Sat sheet carried Friday's date by copy-paste.

    The date was inside the week, so the old out-of-range-only correction
    never looked at it.
    """
    got = _date_for_sheet_label("Sat", _d("2025-02-10"), _d("2025-02-15"),
                                date_cell=_d("2025-02-14"))
    assert got == _d("2025-02-15")
    assert got.strftime("%a") == "Sat"


# ---------------------------------------------------------------------------
# Degenerate and oversized filename ranges
# ---------------------------------------------------------------------------

def test_degenerate_range_anchors_to_week_monday() -> None:
    """Filenames like "03-09-26 to 03-09-26" give a zero-length range."""
    for label, expected in [("Mon", "2026-03-09"), ("Wed", "2026-03-11"),
                            ("Fri", "2026-03-13")]:
        got = _date_for_sheet_label(label, _d("2026-03-09"), _d("2026-03-09"))
        assert got == _d(expected), label
        assert got.strftime("%a") == label


def test_oversized_range_picks_candidate_nearest_the_typed_date() -> None:
    """A range spanning >1 week offers two candidates for a weekday."""
    got = _date_for_sheet_label("Wed", _d("2025-09-22"), _d("2025-10-03"),
                                date_cell=_d("2025-10-01"))
    assert got == _d("2025-10-01")
    got_early = _date_for_sheet_label("Wed", _d("2025-09-22"), _d("2025-10-03"),
                                      date_cell=_d("2025-09-24"))
    assert got_early == _d("2025-09-24")


def test_unknown_label_returns_none() -> None:
    assert _date_for_sheet_label("Weekly Report", _d("2026-07-06"),
                                 _d("2026-07-10")) is None


# ---------------------------------------------------------------------------
# The invariant, enforced downstream
# ---------------------------------------------------------------------------

def test_weekday_mismatch_check_catches_a_regression() -> None:
    from validate_data import _check_weekday_mismatches
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-06", "2026-07-06"]),
        "Day_of_Week": ["Mon", "Tue"],          # second row is the bug
        "Actual_Output": [1000.0, 500.0],
    })
    out = _check_weekday_mismatches(df)
    assert len(out) == 1
    assert out[0]["sheet_label"] == "Tue"
    assert out[0]["actual_weekday"] == "Mon"
    assert out[0]["output"] == 500.0


def test_weekday_mismatch_check_silent_when_consistent() -> None:
    from validate_data import _check_weekday_mismatches
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-06", "2026-07-07"]),
        "Day_of_Week": ["Mon", "Tue"],
        "Actual_Output": [1000.0, 500.0],
    })
    assert _check_weekday_mismatches(df) == []


def test_recent_weekday_mismatch_blocks_publication() -> None:
    """Fresh corruption must stop the publish; ancient history must not."""
    from validate_data import gating_decision
    today = pd.Timestamp.today().normalize()
    recent = {"date": str((today - pd.Timedelta(days=3)).date()),
              "sheet_label": "Tue", "actual_weekday": "Mon", "rows": 4, "output": 900.0}
    old = {"date": "2025-02-14", "sheet_label": "Sat",
           "actual_weekday": "Fri", "rows": 2, "output": 100.0}

    blocked, reasons = gating_decision({"weekday_mismatches": [recent]})
    assert blocked and any("wrong weekday" in r for r in reasons)

    blocked_old, _ = gating_decision({"weekday_mismatches": [old]})
    assert not blocked_old
