"""
Golden-file integration test for the aggregate pipeline.

Builds a synthetic processing-weights xlsx that mirrors the real Excel layout
(Mon sheet with the standard machine row ranges from config.py), runs
``extract_daily_data_from_file`` against it, and asserts the resulting
DataFrame matches a known expected shape and content.

This is the smallest reliable safety net against accidental regressions in
the core ingestion pipeline. Pure functions are covered by other tests; this
one exercises Excel layout assumptions specifically.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from aggregate_daily_data import extract_daily_data_from_file
from config import (
    COL_ACTUAL_INPUT,
    COL_ACTUAL_OUTPUT,
    COL_COMMENT,
    COL_DATE,
    COL_INPUT_ITEM,
    COL_MACHINE_HOURS,
    COL_MAN_HOURS,
    COL_OPERATOR,
    COL_OUTPUT_PRODUCT,
    MACHINE_DATA_RANGES,
)


def _build_synthetic_sheet(date_value, machine_rows: dict[str, list[dict]]) -> pd.DataFrame:
    """Build a single daily sheet (e.g. 'Mon') as a DataFrame.

    ``machine_rows`` maps machine name → list of row dicts, where each dict
    has keys matching the COL_* constants (e.g. ``input_item``,
    ``actual_output``). The function places each row in the correct slice
    of the sheet per MACHINE_DATA_RANGES.

    The sheet is sized to be at least as tall as the largest machine end_row
    so the aggregation function processes all of it.
    """
    max_row = max(end for _, end in MACHINE_DATA_RANGES.values())
    n_rows = max_row + 1
    n_cols = COL_DATE + 1
    grid: list[list] = [[None] * n_cols for _ in range(n_rows)]

    # Sheet date at row 0, col 9
    grid[0][COL_DATE] = date_value

    # Sentinel in the very last row to keep openpyxl from trimming trailing
    # empty rows. Cell is outside every machine's range so it won't be
    # interpreted as data.
    grid[n_rows - 1][0] = "_END_"

    for machine_name, rows in machine_rows.items():
        if machine_name not in MACHINE_DATA_RANGES:
            raise KeyError(f"Unknown machine {machine_name!r}")
        start, end = MACHINE_DATA_RANGES[machine_name]
        for offset, row_data in enumerate(rows):
            r = start + offset
            if r >= end:
                raise ValueError(
                    f"Too many rows for {machine_name}: "
                    f"offset {offset} would land outside [{start},{end})"
                )
            grid[r][COL_MACHINE_HOURS] = row_data.get("machine_hours")
            grid[r][COL_MAN_HOURS] = row_data.get("man_hours")
            grid[r][COL_INPUT_ITEM] = row_data.get("input_item")
            grid[r][COL_ACTUAL_INPUT] = row_data.get("actual_input")
            grid[r][COL_OUTPUT_PRODUCT] = row_data.get("output_product")
            grid[r][COL_ACTUAL_OUTPUT] = row_data.get("actual_output")
            grid[r][COL_OPERATOR] = row_data.get("operator")
            grid[r][COL_COMMENT] = row_data.get("comment")

    return pd.DataFrame(grid)


def _write_synthetic_xlsx(path: Path, mon_sheet: pd.DataFrame) -> None:
    """Write a workbook with just a 'Mon' sheet (other days skipped)."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        mon_sheet.to_excel(writer, sheet_name="Mon", header=False, index=False)


# ---------------------------------------------------------------------------
# Golden-file: end-to-end extract from a synthetic xlsx
# ---------------------------------------------------------------------------

def test_aggregate_extracts_machine_rows_from_synthetic_xlsx(tmp_path: Path) -> None:
    """A 1st-shift xlsx with two machines should produce 2 daily rows."""
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "AUTO TIE BALER": [
                {"machine_hours": 8.0, "man_hours": 16.0,
                 "input_item": "LDPE film",
                 "actual_input": 5000.0,
                 "output_product": "LD Bales",
                 "actual_output": 4500.0,
                 "operator": "Alice"},
            ],
            "EXTRUDER": [
                {"machine_hours": 12.0, "man_hours": 24.0,
                 "input_item": "Mixed regrinds",
                 "actual_input": 8000.0,
                 "output_product": "PP resin",
                 "actual_output": 7800.0,
                 "operator": "Bob"},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, df_notes = extract_daily_data_from_file(tmp_path / fname)

    # Two rows produced (one per machine)
    assert len(df_daily) == 2, df_daily

    # Expected columns present
    expected_cols = {"Date", "Day_of_Week", "Week_Start", "Week_End", "Shift",
                     "Machine_Name", "Input_Item", "Actual_Input",
                     "Output_Product", "Actual_Output",
                     "Machine_Hours", "Man_Hours", "Operator",
                     "Output_per_Hour", "Labor_Cost", "Total_Expense"}
    assert expected_cols.issubset(set(df_daily.columns))

    # Verify content row-by-row
    by_machine = {row["Machine_Name"]: row for _, row in df_daily.iterrows()}
    assert "AUTO TIE BALER" in by_machine
    assert "EXTRUDER" in by_machine

    auto_tie = by_machine["AUTO TIE BALER"]
    assert auto_tie["Actual_Output"] == 4500.0
    assert auto_tie["Operator"] == "Alice"
    assert auto_tie["Shift"] == "1st"
    assert auto_tie["Date"] == "2026-01-05"
    assert auto_tie["Output_per_Hour"] == pytest.approx(4500.0 / 8.0)

    extr = by_machine["EXTRUDER"]
    assert extr["Output_Product"] == "PP resin"
    assert extr["Man_Hours"] == 24.0

    # Labor_Cost = man_hours * $25/hr (LABOR_RATE)
    assert auto_tie["Labor_Cost"] == pytest.approx(16.0 * 25.0)


def test_aggregate_skips_empty_rows(tmp_path: Path) -> None:
    """Rows with zero output/hours/no operator should not appear in the result."""
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "GUILLOTINE": [
                # Real row
                {"machine_hours": 8.0, "man_hours": 8.0,
                 "input_item": "LDPE slabs", "actual_input": 1000.0,
                 "output_product": "LDPE slabs", "actual_output": 950.0,
                 "operator": "Sam"},
                # Empty row (should be skipped)
                {"machine_hours": 0, "man_hours": 0,
                 "input_item": "", "actual_input": 0,
                 "output_product": "", "actual_output": 0,
                 "operator": ""},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, _ = extract_daily_data_from_file(tmp_path / fname)
    assert len(df_daily) == 1
    assert df_daily.iloc[0]["Operator"] == "Sam"


def test_aggregate_extracts_shift_and_date_range_from_filename(tmp_path: Path) -> None:
    """Filename drives shift + week boundaries; sheet date drives the actual day."""
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-04-13"),  # a Monday
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 8.0, "man_hours": 8.0,
                 "input_item": "x", "actual_input": 100,
                 "output_product": "PP resin", "actual_output": 100, "operator": "Z"},
            ],
        },
    )
    fname = "2nd shift processing weights 04-13-26 to 04-17-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, _ = extract_daily_data_from_file(tmp_path / fname)
    row = df_daily.iloc[0]
    assert row["Shift"] == "2nd"
    assert row["Week_Start"] == "2026-04-13"
    assert row["Week_End"] == "2026-04-17"
    assert row["Date"] == "2026-04-13"


def test_aggregate_includes_notes_when_comment_present(tmp_path: Path) -> None:
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 8.0, "man_hours": 8.0,
                 "input_item": "x", "actual_input": 100,
                 "output_product": "PP resin", "actual_output": 100,
                 "operator": "Z", "comment": "Machine ran great, no issues."},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, df_notes = extract_daily_data_from_file(tmp_path / fname)
    assert len(df_notes) == 1
    assert "great" in df_notes.iloc[0]["Note"]
    # Operational note (no downtime/quality/material keywords)
    assert df_notes.iloc[0]["Category"] == "operational"


def test_aggregate_categorizes_downtime_note(tmp_path: Path) -> None:
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 4.0, "man_hours": 8.0,
                 "input_item": "x", "actual_input": 100,
                 "output_product": "PP resin", "actual_output": 50,
                 "operator": "Z",
                 "comment": "Belt broken, machine down 4 hrs awaiting repair."},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    _, df_notes = extract_daily_data_from_file(tmp_path / fname)
    assert df_notes.iloc[0]["Category"] == "downtime"


def test_zero_output_row_has_nan_cost_not_zero(tmp_path: Path) -> None:
    """A shift with labor but no output must NOT report $0.00/lb cost.

    Cost_per_Pound and Output_per_Hour are undefined (NaN) when the
    denominator is zero; 0 would read as 'free production' and bias averages.
    """
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 0, "man_hours": 8.0,
                 "input_item": "PP regrind", "actual_input": 500.0,
                 "output_product": "PP resin", "actual_output": 0,
                 "operator": "Z", "comment": "machine down all shift"},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, _ = extract_daily_data_from_file(tmp_path / fname)
    assert len(df_daily) == 1
    row = df_daily.iloc[0]
    assert row["Labor_Cost"] == pytest.approx(8.0 * 25.0)
    assert pd.isna(row["Cost_per_Pound"]), "zero output must yield NaN cost/lb, not 0"
    assert pd.isna(row["Output_per_Hour"]), "zero machine hours must yield NaN output/hr, not 0"


def test_date_outside_week_is_corrected_and_flagged(tmp_path: Path) -> None:
    """A sheet date far outside the filename week gets auto-corrected, and the
    row carries Date_Corrected=True so the correction is reviewable."""
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2025-06-05"),  # months before the filename week
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 8.0, "man_hours": 8.0,
                 "input_item": "x", "actual_input": 100,
                 "output_product": "PP resin", "actual_output": 100, "operator": "Z"},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, _ = extract_daily_data_from_file(tmp_path / fname)
    assert len(df_daily) == 1
    row = df_daily.iloc[0]
    assert row["Date"] == "2026-01-05"  # inferred from 'Mon' sheet within week
    assert bool(row["Date_Corrected"]) is True


def test_in_week_date_is_not_flagged(tmp_path: Path) -> None:
    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 8.0, "man_hours": 8.0,
                 "input_item": "x", "actual_input": 100,
                 "output_product": "PP resin", "actual_output": 100, "operator": "Z"},
            ],
        },
    )
    fname = "1st shift processing weights 01-05-26 to 01-09-26.xlsx"
    _write_synthetic_xlsx(tmp_path / fname, sheet)

    df_daily, _ = extract_daily_data_from_file(tmp_path / fname)
    assert bool(df_daily.iloc[0]["Date_Corrected"]) is False


def test_incremental_folder_run_updates_one_week_keeps_others(tmp_path: Path) -> None:
    """End-to-end: full build from two weeks, then an incremental run on a
    folder containing only a corrected week-2 file must update week 2 and
    leave week 1 untouched."""
    from aggregate_daily_data import aggregate_daily_folder

    def _report(folder: Path, week: str, output: float) -> None:
        sheet = _build_synthetic_sheet(
            date_value=pd.Timestamp(week),
            machine_rows={
                "EXTRUDER": [
                    {"machine_hours": 8.0, "man_hours": 8.0,
                     "input_item": "x", "actual_input": 100,
                     "output_product": "PP resin", "actual_output": output,
                     "operator": "Z", "comment": "ran fine"},
                ],
            },
        )
        end = (pd.Timestamp(week) + pd.Timedelta(days=4)).strftime("%m-%d-%y")
        start = pd.Timestamp(week).strftime("%m-%d-%y")
        _write_synthetic_xlsx(folder / f"1st shift processing weights {start} to {end}.xlsx", sheet)

    out_xlsx = tmp_path / "agg.xlsx"
    notes_xlsx = tmp_path / "notes.xlsx"

    # Full build: weeks of Jan 5 and Jan 12
    full_dir = tmp_path / "full"
    full_dir.mkdir()
    _report(full_dir, "2026-01-05", 1000.0)
    _report(full_dir, "2026-01-12", 2000.0)
    aggregate_daily_folder(full_dir, output_path=out_xlsx, notes_path=notes_xlsx)
    assert sorted(pd.read_excel(out_xlsx)["Actual_Output"]) == [1000.0, 2000.0]

    # Incremental: folder contains ONLY a corrected week-2 file
    incr_dir = tmp_path / "incr"
    incr_dir.mkdir()
    _report(incr_dir, "2026-01-12", 2500.0)
    aggregate_daily_folder(incr_dir, incremental=True,
                           output_path=out_xlsx, notes_path=notes_xlsx)

    df = pd.read_excel(out_xlsx)
    assert len(df) == 2
    assert sorted(df["Actual_Output"]) == [1000.0, 2500.0]
    # Notes merged the same way: one note per week survives
    notes = pd.read_excel(notes_xlsx)
    assert len(notes) == 2


def test_non_incremental_full_rebuild_unchanged_behavior(tmp_path: Path) -> None:
    """Without --incremental, output reflects only the folder contents."""
    from aggregate_daily_data import aggregate_daily_folder

    sheet = _build_synthetic_sheet(
        date_value=pd.Timestamp("2026-01-05"),
        machine_rows={
            "EXTRUDER": [
                {"machine_hours": 8.0, "man_hours": 8.0,
                 "input_item": "x", "actual_input": 100,
                 "output_product": "PP resin", "actual_output": 100, "operator": "Z"},
            ],
        },
    )
    folder = tmp_path / "reports"
    folder.mkdir()
    _write_synthetic_xlsx(folder / "1st shift processing weights 01-05-26 to 01-09-26.xlsx", sheet)
    out_xlsx = tmp_path / "agg.xlsx"
    aggregate_daily_folder(folder, output_path=out_xlsx, notes_path=tmp_path / "n.xlsx")
    assert len(pd.read_excel(out_xlsx)) == 1
