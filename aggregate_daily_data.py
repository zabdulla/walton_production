"""
Daily data aggregation script for processing reports.

Extracts daily-level production data including:
- Per-day machine/man hours and output
- Operator names
- Supervisor comments/notes
- Data quality flags

Outputs:
- processing_reports/aggregated_daily_data.xlsx
- processing_reports/aggregated_notes.xlsx
"""

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Column indices for daily sheets
COL_MACHINE_HOURS = 1
COL_MAN_HOURS = 2
COL_INPUT_ITEM = 3
COL_ACTUAL_INPUT = 4
COL_OUTPUT_PRODUCT = 5
COL_ACTUAL_OUTPUT = 6
COL_OPERATOR = 7
COL_COMMENT = 8
COL_DATE = 9

# Machine configuration: maps machine name to (start_row, end_row) in Excel sheet
MACHINE_DATA_RANGES: dict[str, tuple[int, int]] = {
    "AUTO TIE BALER": (4, 13),
    "BALER 1": (16, 25),
    "BALER 2": (28, 37),
    "GUILLOTINE": (40, 44),
    "SHREDDER": (47, 50),
    "AVANGUARD DENSIFIER (OLD)": (53, 55),
    "GREEN MAX DENSIFIER (NEW)": (58, 60),
    "EXTRUDER": (63, 66),
    "GRINDER": (69, 74),
}

# Daily sheet names in order
DAILY_SHEETS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

# Note categorization keywords
NOTE_CATEGORIES = {
    "downtime": ["down", "stopped", "broken", "repair", "fix", "belt", "chiller", "filter"],
    "material": ["no material", "waiting for material", "material shortage", "ran out"],
    "quality": ["no weights", "missing", "not entered", "incomplete"],
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float, returning default if conversion fails or value is NA."""
    numeric = pd.to_numeric(value, errors="coerce")
    return default if pd.isna(numeric) else float(numeric)


def _safe_str(value: Any) -> str:
    """Convert value to string, returning empty string if NA."""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _parse_shift(file_name: str) -> str:
    """Extract shift label from filename."""
    lowered = file_name.lower()
    if "1st shift" in lowered:
        return "1st"
    elif "2nd shift" in lowered:
        return "2nd"
    elif "3rd shift" in lowered:
        return "3rd"
    return "unspecified"


def _parse_date_range(file_name: str) -> tuple[str, str]:
    """Extract start/end dates from filename."""
    date_match = re.search(r"(\d{1,2}-\d{1,2}-\d{2,4}) to (\d{1,2}-\d{1,2}-\d{2,4})", file_name)
    if not date_match:
        raise ValueError(f"File name {file_name} does not contain a valid date range.")

    def parse_date(date_str: str) -> str:
        for fmt in ("%m-%d-%y", "%m-%d-%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        raise ValueError(f"Could not parse date value: {date_str}")

    return parse_date(date_match.group(1)), parse_date(date_match.group(2))


def _categorize_note(note: str) -> str:
    """Categorize a supervisor note based on keywords."""
    if not note:
        return ""
    note_lower = note.lower()
    for category, keywords in NOTE_CATEGORIES.items():
        if any(kw in note_lower for kw in keywords):
            return category
    return "operational"


def _extract_date_from_sheet(data: pd.DataFrame) -> str | None:
    """Extract date from row 0, col 9 of a daily sheet."""
    try:
        date_val = data.iloc[0, COL_DATE]
        if pd.isna(date_val):
            return None
        if isinstance(date_val, (datetime, pd.Timestamp)):
            return date_val.strftime("%Y-%m-%d")
        # Try parsing string
        for fmt in ("%Y-%m-%d", "%m-%d-%y", "%m-%d-%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(str(date_val), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None
    except (IndexError, KeyError):
        return None


def extract_daily_data_from_file(
    file_path: str | Path,
    hourly_rate: float = 24,
    overhead_multiplier: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract daily data from a single processing report Excel file.

    Returns:
        Tuple of (daily_data_df, notes_df)
    """
    file_name = os.path.basename(file_path)
    shift = _parse_shift(file_name)
    week_start, week_end = _parse_date_range(file_name)

    workbook = pd.ExcelFile(file_path)

    daily_records: list[dict] = []
    notes_records: list[dict] = []

    for sheet_name in DAILY_SHEETS:
        if sheet_name not in workbook.sheet_names:
            continue

        data = workbook.parse(sheet_name, header=None)

        # Get date from sheet
        sheet_date = _extract_date_from_sheet(data)
        if not sheet_date:
            logger.warning(f"{file_name}/{sheet_name}: Could not extract date, skipping")
            continue

        # Extract machine data
        for machine, (start_row, end_row) in MACHINE_DATA_RANGES.items():
            if data.shape[0] < end_row:
                continue

            for row_idx in range(start_row, end_row):
                row = data.iloc[row_idx]

                machine_hours = _safe_float(row[COL_MACHINE_HOURS])
                man_hours = _safe_float(row[COL_MAN_HOURS])
                input_item = _safe_str(row[COL_INPUT_ITEM])
                actual_input = _safe_float(row[COL_ACTUAL_INPUT])
                output_product = _safe_str(row[COL_OUTPUT_PRODUCT])
                actual_output = _safe_float(row[COL_ACTUAL_OUTPUT])
                operator = _safe_str(row[COL_OPERATOR])
                comment = _safe_str(row[COL_COMMENT])

                # Skip rows with no meaningful data
                if not input_item or "TOTALS" in input_item.upper():
                    continue

                # Skip completely empty rows
                if machine_hours == 0 and man_hours == 0 and actual_output == 0 and not operator:
                    continue

                # Calculate derived metrics
                output_per_hour = actual_output / machine_hours if machine_hours > 0 else 0
                labor_cost = man_hours * hourly_rate
                total_expense = labor_cost * overhead_multiplier
                cost_per_pound = total_expense / actual_output if actual_output > 0 else 0

                # Data quality flags
                has_machine_hours = machine_hours > 0
                has_man_hours = man_hours > 0
                has_output = actual_output > 0
                has_comment = bool(comment)

                # Calculate quality score (0-100)
                quality_score = sum([
                    has_machine_hours * 25,
                    has_man_hours * 25,
                    has_output * 40,
                    10 if (has_machine_hours == has_output) else 0,  # Consistency bonus
                ])

                daily_records.append({
                    "Date": sheet_date,
                    "Day_of_Week": sheet_name,
                    "Week_Start": week_start,
                    "Week_End": week_end,
                    "Shift": shift,
                    "Machine_Name": machine,
                    "Input_Item": input_item,
                    "Actual_Input": actual_input,
                    "Output_Product": output_product,
                    "Actual_Output": actual_output,
                    "Machine_Hours": machine_hours,
                    "Man_Hours": man_hours,
                    "Operator": operator,
                    "Comment": comment,
                    "Output_per_Hour": output_per_hour,
                    "Labor_Cost": labor_cost,
                    "Total_Expense": total_expense,
                    "Cost_per_Pound": cost_per_pound,
                    "Has_Machine_Hours": has_machine_hours,
                    "Has_Man_Hours": has_man_hours,
                    "Has_Output": has_output,
                    "Has_Comment": has_comment,
                    "Data_Quality_Score": quality_score,
                })

                # Extract notes separately
                if comment:
                    notes_records.append({
                        "Date": sheet_date,
                        "Shift": shift,
                        "Machine_Name": machine,
                        "Input_Item": input_item,
                        "Operator": operator,
                        "Note": comment,
                        "Category": _categorize_note(comment),
                    })

    return pd.DataFrame(daily_records), pd.DataFrame(notes_records)


def aggregate_daily_folder(
    folder_path: str | Path,
    hourly_rate: float = 24,
    overhead_multiplier: float = 1.0,
) -> None:
    """Aggregate all processing report files in folder to daily data files."""
    folder = Path(folder_path)
    file_paths = sorted(p for p in folder.glob("*processing weights*.xlsx") if not p.name.startswith("~"))

    if not file_paths:
        logger.info("No processing report files found in %s", folder_path)
        return

    daily_dataframes: list[pd.DataFrame] = []
    notes_dataframes: list[pd.DataFrame] = []

    for file_path in file_paths:
        logger.info("Processing: %s", file_path)
        try:
            daily_df, notes_df = extract_daily_data_from_file(
                file_path,
                hourly_rate=hourly_rate,
                overhead_multiplier=overhead_multiplier,
            )
            if not daily_df.empty:
                daily_dataframes.append(daily_df)
            if not notes_df.empty:
                notes_dataframes.append(notes_df)
        except ValueError as e:
            logger.warning("Skipping %s: %s", file_path, e)
        except pd.errors.EmptyDataError as e:
            logger.warning("Empty or corrupt file %s: %s", file_path, e)
        except Exception as e:
            logger.error("Unexpected error processing %s: %s", file_path, e, exc_info=True)

    # Save daily data
    if daily_dataframes:
        aggregated_daily = pd.concat(daily_dataframes, ignore_index=True)
        aggregated_daily = aggregated_daily.sort_values(["Date", "Shift", "Machine_Name"])
        output_path = folder / "aggregated_daily_data.xlsx"
        aggregated_daily.to_excel(output_path, index=False)
        logger.info("Daily data saved to %s (%d records)", output_path, len(aggregated_daily))
    else:
        logger.warning("No daily data aggregated.")

    # Save notes
    if notes_dataframes:
        aggregated_notes = pd.concat(notes_dataframes, ignore_index=True)
        aggregated_notes = aggregated_notes.sort_values(["Date", "Shift", "Machine_Name"])
        notes_path = folder / "aggregated_notes.xlsx"
        aggregated_notes.to_excel(notes_path, index=False)
        logger.info("Notes saved to %s (%d records)", notes_path, len(aggregated_notes))
    else:
        logger.info("No supervisor notes found.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    folder_name = "processing_reports"
    aggregate_daily_folder(folder_name, hourly_rate=24, overhead_multiplier=1.0)
