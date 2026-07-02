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

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import (
    COL_MACHINE_HOURS,
    COL_MAN_HOURS,
    COL_INPUT_ITEM,
    COL_ACTUAL_INPUT,
    COL_OUTPUT_PRODUCT,
    COL_ACTUAL_OUTPUT,
    COL_OPERATOR,
    COL_COMMENT,
    COL_DATE,
    LABOR_RATE,
    PRODUCT_TYPO_MAP,
    MACHINE_DATA_RANGES,
    DAILY_SHEETS,
    NOTE_CATEGORIES,
)

logger = logging.getLogger(__name__)


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
    hourly_rate: float = LABOR_RATE,
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

        # Validate date falls within a reasonable range of the week from the filename.
        # Catches typos like 12-30 instead of 01-30 in the Excel cell.
        date_corrected = False
        if week_start and week_end:
            parsed_date = datetime.strptime(sheet_date, "%Y-%m-%d")
            ws = datetime.strptime(week_start, "%Y-%m-%d")
            we = datetime.strptime(week_end, "%Y-%m-%d")
            tolerance = pd.Timedelta(days=7)
            if parsed_date < ws - tolerance or parsed_date > we + tolerance:
                logger.warning(
                    "%s/%s: Date %s outside expected week %s–%s, correcting to week range",
                    file_name, sheet_name, sheet_date, week_start, week_end,
                )
                corrected = False
                # Try swapping month/day to see if it fits
                try:
                    swapped = datetime(parsed_date.year, parsed_date.day, parsed_date.month)
                    if ws - tolerance <= swapped <= we + tolerance:
                        sheet_date = swapped.strftime("%Y-%m-%d")
                        logger.info("  Corrected to %s (month/day swap)", sheet_date)
                        corrected = True
                        date_corrected = True
                except ValueError:
                    pass
                # Fallback: infer date from sheet name (day of week) within the week range
                if not corrected:
                    day_map = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5}
                    if sheet_name in day_map:
                        inferred = ws + pd.Timedelta(days=day_map[sheet_name])
                        sheet_date = inferred.strftime("%Y-%m-%d")
                        logger.info("  Corrected to %s (inferred from sheet name '%s')", sheet_date, sheet_name)
                        date_corrected = True
                    else:
                        logger.warning("  Could not auto-correct, skipping sheet")
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
                if machine_hours == 0 and man_hours == 0 and actual_output == 0 and actual_input == 0 and not operator:
                    continue

                # Calculate derived metrics. Ratios are undefined (NaN) when the
                # denominator is zero — a 0 here would read as "free production"
                # and silently bias averages downstream.
                output_per_hour = actual_output / machine_hours if machine_hours > 0 else float("nan")
                labor_cost = man_hours * hourly_rate
                total_expense = labor_cost * overhead_multiplier
                cost_per_pound = total_expense / actual_output if actual_output > 0 else float("nan")

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
                    # True when the sheet's date cell was auto-corrected (month/day
                    # swap or inferred from sheet name) — review these rows.
                    "Date_Corrected": date_corrected,
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


# DEDUP_SUBSET lives in config.py so validate_data can use the same key.
from config import DEDUP_SUBSET  # noqa: E402  (kept near use site for clarity)


def dedup_daily(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop duplicate daily rows; return (deduped_df, n_dropped)."""
    before = len(df)
    df = df.drop_duplicates(subset=DEDUP_SUBSET, keep="first")
    return df, before - len(df)


def merge_incremental(
    existing: pd.DataFrame,
    new: pd.DataFrame,
    week_col: str = "Week_Start",
    shift_col: str = "Shift",
) -> pd.DataFrame:
    """Merge newly parsed rows into an existing aggregate.

    Each raw report file covers one (week, shift), so every (week, shift)
    pair present in *new* REPLACES that slice of *existing* wholesale —
    a corrected re-sent file supersedes the old rows, and re-running on the
    same input is idempotent. Slices absent from *new* are kept untouched,
    which is what lets a runner without the full raw-file archive update
    the committed aggregate.
    """
    if existing is None or existing.empty:
        return new.copy()

    new_keys = set(zip(new[week_col].astype(str), new[shift_col].astype(str)))
    keep_mask = [
        (w, s) not in new_keys
        for w, s in zip(existing[week_col].astype(str), existing[shift_col].astype(str))
    ]
    kept = existing.loc[keep_mask]
    combined = pd.concat([kept, new], ignore_index=True)
    # Rows from older aggregates may predate later-added columns (e.g.
    # Date_Corrected); flag-like columns default to False, not NaN.
    if "Date_Corrected" in combined.columns:
        combined["Date_Corrected"] = combined["Date_Corrected"].fillna(False)
    return combined


def aggregate_daily_folder(
    folder_path: str | Path,
    hourly_rate: float = LABOR_RATE,
    overhead_multiplier: float = 1.0,
    incremental: bool = False,
    output_path: Path | None = None,
    notes_path: Path | None = None,
) -> dict:
    """Aggregate all processing report files in folder to daily data files.

    With ``incremental=True``, weeks parsed from the folder replace the
    matching (Week_Start, Shift) slices of the existing aggregated file and
    everything else is preserved — the folder does NOT need to contain the
    full historical archive. Without it, the output is rebuilt purely from
    the folder contents (original behavior; requires the full archive).

    Returns a summary dict for callers (the orchestrator uses it directly
    instead of scraping log output):
        records      total rows in the written daily file
        duplicates   duplicate rows dropped this run
        notes        total rows in the written notes file
        parsed_files number of workbooks parsed this run
        changed      False when nothing needed to be written
    """
    summary: dict = {"records": 0, "duplicates": 0, "notes": 0,
                     "parsed_files": 0, "changed": False}
    folder = Path(folder_path)
    data_dir = Path(__file__).resolve().parent.parent / "data"
    output_path = output_path or data_dir / "aggregated_daily_data.xlsx"
    notes_path = notes_path or data_dir / "aggregated_notes.xlsx"

    file_paths = sorted(p for p in folder.glob("*processing weights*.xlsx") if not p.name.startswith("~"))

    if incremental and output_path.exists():
        # Only parse workbooks touched since the last aggregation (with a
        # 3-day cushion so a re-sent correction for an older week is still
        # picked up). This is the actual speed win — the corpus is ~170
        # workbooks and growing, and merge_incremental() only needs the
        # weeks that changed. It also means a fresh checkout with an empty
        # processing_reports/ folder (e.g. CI) can aggregate: newly fetched
        # files are parsed, everything else is preserved from the existing
        # aggregated file.
        cutoff = output_path.stat().st_mtime - 3 * 24 * 3600
        skipped = [p for p in file_paths if p.stat().st_mtime <= cutoff]
        file_paths = [p for p in file_paths if p.stat().st_mtime > cutoff]
        logger.info("Incremental: parsing %d recently-modified file(s), "
                    "skipping %d unchanged", len(file_paths), len(skipped))
        if not file_paths:
            n_existing = len(pd.read_excel(output_path))
            logger.info("No new or modified reports — aggregated data unchanged. "
                        "Daily data saved to %s (%d records)", output_path, n_existing)
            summary["records"] = n_existing
            if notes_path.exists():
                summary["notes"] = len(pd.read_excel(notes_path))
            return summary

    if not file_paths:
        logger.info("No processing report files found in %s", folder_path)
        return summary

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

        if incremental and output_path.exists():
            existing = pd.read_excel(output_path)
            n_before = len(existing)
            aggregated_daily = merge_incremental(existing, aggregated_daily)
            logger.info("Incremental merge: %d existing + %d new rows -> %d",
                        n_before, sum(len(d) for d in daily_dataframes),
                        len(aggregated_daily))

        aggregated_daily = aggregated_daily.sort_values(["Date", "Shift", "Machine_Name"])

        # Apply product typo corrections (re-applied to the whole frame so
        # newly added typo mappings also fix historical rows).
        aggregated_daily["Output_Product"] = aggregated_daily["Output_Product"].replace(PRODUCT_TYPO_MAP)

        # Drop exact duplicate rows
        aggregated_daily, n_dropped = dedup_daily(aggregated_daily)
        if n_dropped:
            logger.warning("Dropped %d duplicate rows during aggregation", n_dropped)
        summary["duplicates"] = n_dropped

        snapshot_dir = output_path.parent / "snapshots"
        try:
            from atomic import write_with_snapshot, GrowthSanityError
            result = write_with_snapshot(
                output_path,
                lambda tmp: aggregated_daily.to_excel(tmp, index=False),
                snapshot_dir,
                new_row_count=len(aggregated_daily),
            )
            logger.info("Daily data saved to %s (%d records) [%s]",
                        output_path, len(aggregated_daily), result["growth_msg"])
            summary["records"] = len(aggregated_daily)
            summary["changed"] = True
        except GrowthSanityError as e:
            logger.error("REFUSING to overwrite %s: %s", output_path, e)
            logger.error("If this drop is legitimate, delete the existing file or run with manual override.")
            raise
    else:
        logger.warning("No daily data aggregated.")

    # Save notes
    if notes_dataframes:
        aggregated_notes = pd.concat(notes_dataframes, ignore_index=True)

        if incremental and notes_path.exists():
            existing_notes = pd.read_excel(notes_path)
            # Notes rows carry Date (not Week_Start); replacement is keyed on
            # the same (week, shift) slices as the daily data.
            def _with_week(df: pd.DataFrame) -> pd.DataFrame:
                df = df.copy()
                dates = pd.to_datetime(df["Date"])
                df["_Week_Start"] = (
                    (dates - pd.to_timedelta(dates.dt.weekday, unit="D"))
                    .dt.strftime("%Y-%m-%d")
                )
                return df

            merged = merge_incremental(
                _with_week(existing_notes), _with_week(aggregated_notes),
                week_col="_Week_Start",
            )
            aggregated_notes = merged.drop(columns=["_Week_Start"])

        aggregated_notes = aggregated_notes.sort_values(["Date", "Shift", "Machine_Name"])
        snapshot_dir = notes_path.parent / "snapshots"
        from atomic import write_with_snapshot
        result = write_with_snapshot(
            notes_path,
            lambda tmp: aggregated_notes.to_excel(tmp, index=False),
            snapshot_dir,
            new_row_count=len(aggregated_notes),
            # Notes can shrink legitimately if older files are removed.
            min_ratio=0.5,
        )
        logger.info("Notes saved to %s (%d records) [%s]",
                    notes_path, len(aggregated_notes), result["growth_msg"])
        summary["notes"] = len(aggregated_notes)
    else:
        logger.info("No supervisor notes found.")

    summary["parsed_files"] = len(file_paths)
    return summary


def run_aggregation(reports_dir: Path | None = None, full: bool = False) -> dict:
    """Aggregate with the standard mode selection (shared by CLI + orchestrator).

    Incremental when an aggregate already exists, full rebuild otherwise or
    when ``full=True``. Returns the summary dict from aggregate_daily_folder.
    """
    project_root = Path(__file__).resolve().parent.parent
    reports_dir = reports_dir or project_root / "processing_reports"
    agg_exists = (project_root / "data" / "aggregated_daily_data.xlsx").exists()
    use_incremental = agg_exists and not full
    logger.info("Mode: %s", "incremental" if use_incremental else "full rebuild")
    return aggregate_daily_folder(
        reports_dir,
        hourly_rate=LABOR_RATE,
        overhead_multiplier=1.0,
        incremental=use_incremental,
    )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Aggregate processing reports into daily data files.")
    parser.add_argument(
        "--reports-dir", type=Path,
        default=Path(__file__).resolve().parent.parent / "processing_reports",
        help="Folder containing raw processing-weights xlsx files.",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Force a full rebuild from every workbook in the folder "
             "(requires the complete archive). Default is incremental: only "
             "recently-modified workbooks are parsed and merged into the "
             "existing aggregate.",
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help=argparse.SUPPRESS,  # legacy alias; incremental is now the default
    )
    args = parser.parse_args()
    if args.full and args.incremental:
        parser.error("--full and --incremental are mutually exclusive")

    run_aggregation(reports_dir=args.reports_dir, full=args.full)
