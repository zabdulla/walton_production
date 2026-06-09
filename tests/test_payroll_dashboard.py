"""Tests for src/build_payroll_dashboard.py — data-layer anonymization."""
from __future__ import annotations

from build_payroll_dashboard import anonymize_period_data


def _period(names: list[str]) -> dict:
    return {
        "period_start": "01/01/2026",
        "period_end": "01/14/2026",
        "employees": [{"employee_name": n, "clock_total": 80.0} for n in names],
        "machines": [{"machine": "EXTRUDER", "workers": list(names)}],
    }


def test_anonymize_replaces_names_in_employees_and_workers() -> None:
    data = [_period(["Alice Smith"])]
    out = anonymize_period_data(data)
    assert out[0]["employees"][0]["employee_name"] == "Employee 01"
    assert out[0]["machines"][0]["workers"] == ["Employee 01"]


def test_anonymize_does_not_mutate_input() -> None:
    data = [_period(["Alice Smith"])]
    anonymize_period_data(data)
    assert data[0]["employees"][0]["employee_name"] == "Alice Smith"


def test_anonymize_is_stable_across_periods() -> None:
    data = [_period(["Alice Smith", "Bob Jones"]), _period(["Bob Jones"])]
    out = anonymize_period_data(data)
    label_p1 = next(e["employee_name"] for e in out[0]["employees"]
                    if data[0]["employees"][1]["employee_name"] == "Bob Jones")
    # Bob sorts after Alice -> Employee 02 in both periods
    assert out[1]["employees"][0]["employee_name"] == "Employee 02"
    assert "Employee 02" in {e["employee_name"] for e in out[0]["employees"]}


def test_no_real_name_survives() -> None:
    names = ["Alice Smith", "Bob Jones", "Carol White"]
    out = anonymize_period_data([_period(names)])
    blob = str(out)
    for n in names:
        assert n not in blob
