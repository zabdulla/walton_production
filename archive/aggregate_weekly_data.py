import os
import pandas as pd
import re

def extract_week_date(file_name):
    """
    Extract the start date of the week from the file name.

    :param file_name: The name of the file containing the date range.
    :return: Parsed start date of the week in YYYY-MM-DD format.
    """
    match = re.search(r'(\d{1,2}-\d{1,2}-\d{2})', file_name)
    if match:
        try:
            # Convert the extracted date to a proper datetime object
            week_date = pd.to_datetime(match.group(0), format="%m-%d-%y")
            return week_date
        except ValueError:
            return None
    return None

def process_weekly_report(file_path, week_date):
    """
    Extract relevant data from the Weekly Report tab of the given Excel file.

    :param file_path: Path to the Excel file.
    :param week_date: Date of the week's production extracted from the file name.
    :return: DataFrame containing extracted data.
    """
    try:
        # Load the Excel file and parse the 'Weekly Report' sheet
        weekly_report = pd.read_excel(file_path, sheet_name="Weekly Report", header=2)

        # Strip column names to remove leading and trailing spaces
        weekly_report.columns = weekly_report.columns.str.strip()

        # Initialize a list to store the extracted data
        machine_data = []

        for _, row in weekly_report.iterrows():
            # Check if "totals" is in the "Input Item" column
            if isinstance(row["Input Item"], str) and "totals" in row["Input Item"].lower():
                # Extract the machine name, including hyphens and spaces before "- totals"
                machine_name = "-".join(row["Input Item"].split("-")[:-1]).strip()

                # Extract relevant columns for totals with robust numeric parsing
                total_machine_hours = pd.to_numeric(row["Total Machine Hours"], errors="coerce") or 0.0
                total_man_hours = pd.to_numeric(row["Total Man Hours"], errors="coerce") or 0.0
                actual_output_weight = pd.to_numeric(row["Actual Output (Lbs)"], errors="coerce") or 0.0

                # Append the extracted data
                machine_data.append({
                    "Week Date": week_date,
                    "Machine Name": machine_name,
                    "Total Machine Hours": total_machine_hours,
                    "Total Man Hours": total_man_hours,
                    "Total Output Weight (lbs)": actual_output_weight
                })

        # Convert the list of data into a DataFrame
        return pd.DataFrame(machine_data)

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()

def aggregate_data(folder_path, hourly_wage=24, overhead_multiplier=1.0):
    """
    Aggregate data from all weekly reports in the specified folder into a master file, including analysis columns.

    :param folder_path: Path to the folder containing weekly report files.
    :param hourly_wage: Nominal hourly wage for labor cost calculation.
    :param overhead_multiplier: Multiplier for total expense to account for overhead.
    """
    master_file_path = os.path.join(folder_path, "master_file.csv")

    # Initialize an empty DataFrame for the master file
    master_data = pd.DataFrame(columns=[
        "Week Date", "Machine Name", "Total Machine Hours", "Total Man Hours", "Total Output Weight (lbs)",
        "Labor Cost", "Total Expense", "Production Cost per Pound", "Output per Hour"
    ])

    # Process each file in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.xlsx'):
            file_path = os.path.join(folder_path, file_name)

            # Extract week date from the file name (adjusting for date range format)
            week_date = extract_week_date(file_name)

            if week_date is None:
                print(f"Could not parse date from file name: {file_name}")
                continue

            weekly_data = process_weekly_report(file_path, week_date)

            # Append the new data to the master data
            master_data = pd.concat([master_data, weekly_data], ignore_index=True)

    # Add analysis columns
    master_data["Labor Cost"] = (master_data["Total Man Hours"] * hourly_wage).round(3)
    master_data["Total Expense"] = (master_data["Labor Cost"] * overhead_multiplier).round(3)
    master_data["Production Cost per Pound"] = (master_data["Total Expense"] / master_data["Total Output Weight (lbs)"]).round(3)
    master_data["Output per Hour"] = (master_data["Total Output Weight (lbs)"] / master_data["Total Machine Hours"]).round(3)

    # Handle cases where machine hours or output weight is zero to avoid division errors
    master_data["Production Cost per Pound"].replace([float("inf"), -float("inf"), pd.NA], 0, inplace=True)
    master_data["Output per Hour"].replace([float("inf"), -float("inf"), pd.NA], 0, inplace=True)

    # Save the updated master data to the file
    master_data.to_csv(master_file_path, index=False)
    print(f"Master file updated successfully: {master_file_path}")

# Example usage
folder_path = "processing_reports"  # Folder containing weekly Excel files
aggregate_data(folder_path)
