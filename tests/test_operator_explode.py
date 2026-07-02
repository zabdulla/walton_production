"""Tests for build_operator_dashboard.explode_operators.

Regression tests for the bug where a trailing comma in an operator string
(e.g. "Alice,Bob,") inflated Operator_Count, causing output/hours to be
divided by the wrong denominator.
"""
from __future__ import annotations

import pandas as pd
import pytest

from build_operator_dashboard import explode_operators


def _row(operator: str, output: float = 100.0, man_hours: float = 8.0) -> dict:
    return {
        "Operator": operator,
        "Actual_Output": output,
        "Man_Hours": man_hours,
    }


def test_explode_single_operator() -> None:
    df = pd.DataFrame([_row("Alice")])
    out = explode_operators(df)
    assert len(out) == 1
    assert out.iloc[0]["Individual_Operator"] == "Alice"
    assert out.iloc[0]["Actual_Output"] == 100.0


def test_explode_two_operators_splits_evenly() -> None:
    df = pd.DataFrame([_row("Alice, Bob", output=100.0, man_hours=8.0)])
    out = explode_operators(df)
    assert len(out) == 2
    # Each gets half the output/hours
    assert out["Actual_Output"].tolist() == [50.0, 50.0]
    assert out["Man_Hours"].tolist() == [4.0, 4.0]


def test_explode_trailing_comma_does_not_inflate_count() -> None:
    """Bug fix: 'Alice,Bob,' should still divide by 2, not 3."""
    df = pd.DataFrame([_row("Alice,Bob,", output=100.0, man_hours=8.0)])
    out = explode_operators(df)
    # Only Alice and Bob, not an empty third operator
    assert len(out) == 2
    assert sorted(out["Individual_Operator"].tolist()) == ["Alice", "Bob"]
    # Each gets half (not a third)
    assert out["Actual_Output"].tolist() == [50.0, 50.0]


def test_explode_leading_and_trailing_commas() -> None:
    df = pd.DataFrame([_row(",Alice, Bob, ,", output=120.0)])
    out = explode_operators(df)
    assert len(out) == 2
    assert sorted(out["Individual_Operator"].tolist()) == ["Alice", "Bob"]
    # Each gets half
    assert out["Actual_Output"].tolist() == [60.0, 60.0]


def test_explode_drops_empty_operator_strings() -> None:
    df = pd.DataFrame([_row(""), _row("Bob")])
    out = explode_operators(df)
    # Empty string filtered out at the initial filter; only Bob remains
    assert len(out) == 1
    assert out.iloc[0]["Individual_Operator"] == "Bob"


def test_explode_drops_nan_operator() -> None:
    df = pd.DataFrame([_row("Alice"), _row(pd.NA)])
    out = explode_operators(df)
    assert len(out) == 1
    assert out.iloc[0]["Individual_Operator"] == "Alice"
