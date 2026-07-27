"""Run-over-run movement in the validation counts.

Context: for months the weekly block printed five warnings whose numbers
never moved, because four of them physically could not change. A real
regression (missing operators climbing on 3rd shift) was invisible inside
that block. Reporting the delta makes movement the headline.
"""
from __future__ import annotations

import json

import pytest

from validate_data import (
    _scalar_counts,
    compute_deltas,
    save_validation_state,
)


def _results(**overrides):
    base = {
        "total_rows": 4837,
        "unmapped_products": [],
        "missing_operators": {"EXTRUDER": 300, "GRINDER": 56},
        "duplicates_count": 0,
        "anomalous_values": [{"rule": "x"}],
        "output_anomalies": [{"machine": "BALER"}],
        "payroll": {"unmatched_production_ops": ["a"] * 64,
                    "unmatched_active": ["a"] * 20},
        "weekday_mismatches": [],
    }
    base.update(overrides)
    return base


def test_scalar_counts_flattens_the_run(tmp_path) -> None:
    counts = _scalar_counts(_results())
    assert counts["missing_operator_rows"] == 356
    assert counts["unmatched_operators"] == 64
    assert counts["unmatched_operators_active"] == 20
    assert counts["weekday_mismatch_rows"] == 0


def test_first_run_has_no_baseline(tmp_path) -> None:
    """No state file yet — report nothing rather than inventing a delta."""
    assert compute_deltas(_results(), tmp_path / "missing.json") == {}


def test_delta_reports_only_what_moved(tmp_path) -> None:
    state = tmp_path / "state.json"
    save_validation_state(_results(), state)

    worse = _results(missing_operators={"EXTRUDER": 312, "GRINDER": 56})
    deltas = compute_deltas(worse, state)

    assert deltas == {"missing_operator_rows": 12}, (
        "counts that did not change must not appear at all"
    )


def test_delta_is_signed(tmp_path) -> None:
    state = tmp_path / "state.json"
    save_validation_state(_results(), state)
    better = _results(anomalous_values=[], output_anomalies=[])
    deltas = compute_deltas(better, state)
    assert deltas["anomalous_values"] == -1
    assert deltas["output_anomalies"] == -1


def test_identical_run_reports_no_movement(tmp_path) -> None:
    state = tmp_path / "state.json"
    save_validation_state(_results(), state)
    assert compute_deltas(_results(), state) == {}


def test_corrupt_state_file_is_not_fatal(tmp_path) -> None:
    """A truncated or hand-edited state file must not break validation."""
    state = tmp_path / "state.json"
    state.write_text("{not json")
    assert compute_deltas(_results(), state) == {}


def test_new_count_key_is_ignored_until_it_has_a_baseline(tmp_path) -> None:
    """Adding a check shouldn't report its first value as a delta."""
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"counts": {"rows": 4837}}))
    deltas = compute_deltas(_results(), state)
    assert "missing_operator_rows" not in deltas


def test_saved_state_round_trips(tmp_path) -> None:
    state = tmp_path / "state.json"
    save_validation_state(_results(), state)
    payload = json.loads(state.read_text())
    assert payload["counts"] == _scalar_counts(_results())
    assert "generated" in payload
