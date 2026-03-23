import pandas as pd
import re
from datetime import datetime
import os

def extract_data_from_file(file_path, hourly_rate=24, overhead_multiplier=1.0):
    # Extract the start and end dates from the file name
    file_name = os.path.basename(file_path)
    date_match = re.search(r'(\d{1,2}-\d{1,2}-\d{2,4}) to (\d{1,2}-\d{1,2}-\d{2,4})', file_name)
    if not date_match:
        raise ValueError(f"File name {file_name} does not contain valid date range.")
    
    # Parse start and end dates
    start_date = datetime.strptime(date_match.group(1), '%m-%d-%y').strftime('%Y-%m-%d')
    end_date = datetime.strptime(date_match.group(2), '%m-%d-%y').strftime('%Y-%m-%d')

    # Read the data from the Excel file
    data = pd.read_excel(file_path, sheet_name='Weekly Report', header=None)

    # Define the machine data ranges with updated machine names
    machine_data_ranges = {
        "AUTO TIE BALER": (4, 13),
        "BALER 1": (16, 25),
        "BALER 2": (28, 37),
        "GUILLOTINE": (40, 44),
        "SHREDDER": (47, 50),
        "AVANGURAD DENSIFER (OLD)": (53, 55),
        "GREEN MAX DENSIFIER (NEW)": (58, 60),
        "EXTRUDER": (63, 66),
        "GRINDER": (69, 74)
    }

    # Extract the correct headers from row 3 (columns B, C, F, and G)
    headers = data.iloc[2, [1, 2, 5, 6]].tolist()
    headers = [str(header).strip() for header in headers]
    final_headers = ["Start Date", "End Date", "Machine Name"] + headers + [
        "Output per Hour", "Labor Cost", "Total Expense", "Production Cost per Pound"
    ]

    # Compile the data
    compiled_data = []
    for machine, (start_row, end_row) in machine_data_ranges.items():
        rows = data.iloc[start_row:end_row, [1, 2, 5, 6]].values
        for row in rows:
            machine_hours, man_hours, _, actual_output = row
            try:
                machine_hours = float(machine_hours) if machine_hours else 0
                man_hours = float(man_hours) if man_hours else 0
                actual_output = float(actual_output) if actual_output else 0
            except ValueError:
                machine_hours = man_hours = actual_output = 0
            
            # Calculate analysis columns
            output_per_hour = actual_output / machine_hours if machine_hours > 0 else 0
            labor_cost = man_hours * hourly_rate
            total_expense = labor_cost * overhead_multiplier
            production_cost_per_pound = total_expense / actual_output if actual_output > 0 else 0

            compiled_data.append([
                start_date, end_date, machine, machine_hours, man_hours, row[2], actual_output,
                output_per_hour, labor_cost, total_expense, production_cost_per_pound
            ])
    
    # Create a DataFrame from the compiled data
    return pd.DataFrame(compiled_data, columns=final_headers)

def aggregate_folder(folder_path, hourly_rate=24, overhead_multiplier=1.0):
    # Get all Excel files in the folder that match the processing report pattern
    file_paths = [
        os.path.join(folder_path, file)
        for file in os.listdir(folder_path)
        if file.startswith("processing weights") and file.endswith('.xlsx') and not file.startswith('~')
    ]

    # Process all files and aggregate the data
    aggregated_data = pd.DataFrame()
    for file_path in file_paths:
        try:
            file_data = extract_data_from_file(file_path, hourly_rate, overhead_multiplier)
            aggregated_data = pd.concat([aggregated_data, file_data], ignore_index=True)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    # Save the aggregated data to an Excel file in the same folder
    output_path = os.path.join(folder_path, "aggregated_master_data.xlsx")
    aggregated_data.to_excel(output_path, index=False)
    print(f"Aggregated data saved to {output_path}")

# Example usage
if __name__ == "__main__":
    # Folder containing the processing reports
    folder_name = "processing_reports"
    aggregate_folder(folder_name, hourly_rate=24, overhead_multiplier=1.0)