import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

HEADER_ROW = 2
DATA_COLUMNS = [1, 2, 5, 6]  # columns B, C, F, G (0-indexed for iloc)

# Machine configuration: maps machine name to (start_row, end_row) in Excel sheet
MACHINE_DATA_RANGES: dict[str, tuple[int, int]] = {
    "AUTO TIE BALER": (4, 13),
    "BALER 1": (16, 25),
    "BALER 2": (28, 37),
    "GUILLOTINE": (40, 44),
    "SHREDDER": (47, 50),
    "AVANGURAD DENSIFER (OLD)": (53, 55),
    "GREEN MAX DENSIFIER (NEW)": (58, 60),
    "EXTRUDER": (63, 66),
    "GRINDER": (69, 74),
}


def _parse_date(date_str: str) -> str:
    """Parse a date string in m-d-y or m-d-Y and normalize to YYYY-MM-DD."""
    for fmt in ("%m-%d-%y", "%m-%d-%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Could not parse date value: {date_str}")


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float, returning default if conversion fails or value is NA."""
    numeric = pd.to_numeric(value, errors="coerce")
    return default if pd.isna(numeric) else float(numeric)


def _parse_dates_and_shift(file_name: str) -> tuple[str, str, str]:
    """Extract start/end dates and shift label from a filename."""
    shift = "unspecified"
    lowered = file_name.lower()
    if "1st shift" in lowered:
        shift = "1st"
    elif "2nd shift" in lowered:
        shift = "2nd"
    elif "3rd shift" in lowered:
        shift = "3rd"

    date_match = re.search(r"(\d{1,2}-\d{1,2}-\d{2,4}) to (\d{1,2}-\d{1,2}-\d{2,4})", file_name)
    if not date_match:
        raise ValueError(f"File name {file_name} does not contain a valid date range.")

    start_date = _parse_date(date_match.group(1))
    end_date = _parse_date(date_match.group(2))
    return start_date, end_date, shift


def extract_data_from_file(
    file_path: str | Path,
    hourly_rate: float = 24,
    overhead_multiplier: float = 1.0,
    include_shift: bool = False,
) -> pd.DataFrame:
    """Extract machine rows from a single processing report Excel file."""
    file_name = os.path.basename(file_path)
    start_date, end_date, shift = _parse_dates_and_shift(file_name)

    # Validate the workbook and ensure the expected sheet exists before parsing it.
    workbook = pd.ExcelFile(file_path)
    sheet_name = "Weekly Report"
    if sheet_name not in workbook.sheet_names:
        raise ValueError(f"{file_name}: missing expected sheet '{sheet_name}'. Found: {workbook.sheet_names}")
    data = workbook.parse(sheet_name, header=None)

    # Verify the sheet has enough rows/cols for the expected layout.
    min_rows_needed = max(end for _, end in MACHINE_DATA_RANGES.values())
    min_cols_needed = max(DATA_COLUMNS) + 1
    if data.shape[0] < min_rows_needed or data.shape[1] < min_cols_needed:
        raise ValueError(
            f"{file_name}: sheet is smaller than expected ({data.shape}); needs at least {min_rows_needed} rows and {min_cols_needed} columns."
        )

    # Extract the correct headers from row 3 (columns B, C, F, and G)
    headers = data.iloc[HEADER_ROW, DATA_COLUMNS].tolist()
    headers = [str(header).strip() for header in headers]
    final_headers = ["Start Date", "End Date", "Machine Name"] + headers + [
        "Output per Hour",
        "Labor Cost",
        "Total Expense",
        "Production Cost per Pound",
    ]
    if include_shift:
        final_headers.append("Shift")

    # Compile the data
    compiled_data = []
    for machine, (start_row, end_row) in MACHINE_DATA_RANGES.items():
        rows = data.iloc[start_row:end_row, DATA_COLUMNS].values
        for raw_machine_hours, raw_man_hours, output_product, raw_actual_output in rows:
            machine_hours = _safe_float(raw_machine_hours)
            man_hours = _safe_float(raw_man_hours)
            actual_output = _safe_float(raw_actual_output)

            # Calculate analysis columns
            output_per_hour = actual_output / machine_hours if machine_hours > 0 else 0
            labor_cost = man_hours * hourly_rate
            total_expense = labor_cost * overhead_multiplier
            production_cost_per_pound = total_expense / actual_output if actual_output > 0 else 0

            row_values = [
                start_date,
                end_date,
                machine,
                machine_hours,
                man_hours,
                output_product,
                actual_output,
                output_per_hour,
                labor_cost,
                total_expense,
                production_cost_per_pound,
            ]
            if include_shift:
                row_values.append(shift)

            compiled_data.append(row_values)

    # Create a DataFrame from the compiled data
    return pd.DataFrame(compiled_data, columns=final_headers)


def aggregate_folder(
    folder_path: str | Path,
    hourly_rate: float = 24,
    overhead_multiplier: float = 1.0,
    include_shift: bool = False,
) -> None:
    """Aggregate all processing report files in folder to a single Excel file."""
    folder = Path(folder_path)
    file_paths = sorted(p for p in folder.glob("*processing weights*.xlsx") if not p.name.startswith("~"))
    if not file_paths:
        logger.info("No processing report files found in %s", folder_path)
        return

    dataframes: list[pd.DataFrame] = []
    for file_path in file_paths:
        logger.info("Processing: %s", file_path)
        try:
            file_data = extract_data_from_file(
                file_path,
                hourly_rate=hourly_rate,
                overhead_multiplier=overhead_multiplier,
                include_shift=include_shift,
            )
            dataframes.append(file_data)
        except ValueError as e:
            logger.warning("Skipping %s: %s", file_path, e)
        except pd.errors.EmptyDataError as e:
            logger.warning("Empty or corrupt file %s: %s", file_path, e)
        except Exception as e:
            logger.error("Unexpected error processing %s: %s", file_path, e, exc_info=True)

    if not dataframes:
        logger.warning("No data aggregated; check the input files.")
        return

    aggregated_data = pd.concat(dataframes, ignore_index=True)
    output_path = folder / "aggregated_master_data.xlsx"
    aggregated_data.to_excel(output_path, index=False)
    logger.info("Aggregated data saved to %s", output_path)

# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    # Folder containing the processing reports
    folder_name = "processing_reports"
    aggregate_folder(folder_name, hourly_rate=24, overhead_multiplier=1.0)
