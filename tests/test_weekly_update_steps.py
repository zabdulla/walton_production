"""Tests for the in-process orchestrator steps (weekly_update.py).

Focus: failure isolation and the roster-skip path in step_build_dashboards —
the behaviors the subprocess->in-process refactor must preserve.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import weekly_update as wu


def _fake_builders(monkeypatch, behaviors: dict[str, object]) -> None:
    """Replace _dashboard_builders with stubs.

    behaviors: label -> None (succeed) | Exception instance (raise).
    """
    def builders():
        out = []
        for label, beh in behaviors.items():
            if isinstance(beh, Exception):
                def fn(_e=beh):
                    raise _e
            else:
                def fn():
                    print(f"built ok")
            out.append((label, fn))
        return out
    monkeypatch.setattr(wu, "_dashboard_builders", builders)


def test_build_step_isolates_a_failing_builder(monkeypatch, tmp_path) -> None:
    """One raising builder must not stop the others, and must be reported."""
    # Roster present so Payroll isn't skipped
    monkeypatch.setattr(wu, "PROJECT_ROOT", tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "employee_roster.json").write_text("{}")

    _fake_builders(monkeypatch, {
        "Interactive": None,
        "Daily": RuntimeError("boom"),
        "Payroll": None,
    })
    results = wu.step_build_dashboards()
    assert results["built"] == ["Interactive", "Payroll"]
    assert results["failed"] == ["Daily"]
    assert results["ok"] is False


def test_build_step_skips_payroll_without_roster(monkeypatch, tmp_path) -> None:
    """Missing PII roster => Payroll skipped with warning, run still ok."""
    monkeypatch.setattr(wu, "PROJECT_ROOT", tmp_path)
    (tmp_path / "data").mkdir()  # no roster file inside

    _fake_builders(monkeypatch, {
        "Interactive": None,
        "Payroll": None,   # would succeed, but must not even be attempted
    })
    results = wu.step_build_dashboards()
    assert results["skipped"] == ["Payroll"]
    assert results["built"] == ["Interactive"]
    assert results["ok"] is True


def test_build_step_surfaces_notable_output(monkeypatch, tmp_path, capsys) -> None:
    """The 'uplift' line from a builder's stdout is surfaced in the log."""
    monkeypatch.setattr(wu, "PROJECT_ROOT", tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "employee_roster.json").write_text("{}")

    def builders():
        def profit():
            print("  Payroll uplift available: +14.2% labor overhead")
        return [("Profit", profit)]
    monkeypatch.setattr(wu, "_dashboard_builders", builders)

    results = wu.step_build_dashboards()
    assert results["built"] == ["Profit"]
    captured = capsys.readouterr().out
    assert "uplift" in captured.lower()


def test_fetch_step_reports_missing_credentials(monkeypatch) -> None:
    """A FileNotFoundError from get_service (no OAuth file) is a clean
    step failure with ok=False, not a crash."""
    import fetch_emails as fe

    def raise_fnf():
        raise FileNotFoundError("OAuth credentials not found")
    monkeypatch.setattr(fe, "get_service", raise_fnf)

    result = wu.step_fetch_emails()
    assert result["ok"] is False
    assert result["processing"] == 0


def test_aggregate_step_returns_structured_counts(monkeypatch, tmp_path) -> None:
    """step_aggregate consumes run_aggregation()'s dict directly."""
    import aggregate_daily_data as agg

    monkeypatch.setattr(wu, "PROJECT_ROOT", tmp_path)
    (tmp_path / "data").mkdir()

    monkeypatch.setattr(agg, "run_aggregation",
                        lambda: {"records": 1234, "duplicates": 2,
                                 "notes": 9, "parsed_files": 3, "changed": True})
    result = wu.step_aggregate()
    assert result["ok"] is True
    assert result["records"] == 1234
    assert result["duplicates"] == 2


def test_aggregate_step_contains_growth_error(monkeypatch, tmp_path) -> None:
    """GrowthSanityError becomes ok=False without propagating."""
    import aggregate_daily_data as agg
    from atomic import GrowthSanityError

    monkeypatch.setattr(wu, "PROJECT_ROOT", tmp_path)
    (tmp_path / "data").mkdir()

    def boom():
        raise GrowthSanityError("new row count too small")
    monkeypatch.setattr(agg, "run_aggregation", boom)

    result = wu.step_aggregate()
    assert result["ok"] is False
