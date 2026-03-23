import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from PyQt5.QtWidgets import QApplication, QVBoxLayout, QDialog, QLabel, QPushButton, QComboBox, QListWidget, QAbstractItemView, QDateEdit, QMessageBox
from PyQt5.QtCore import QDate

# Load and preprocess data
master_file_path = "processing_reports/master_file.csv"
data = pd.read_csv(master_file_path)
data["Week Date"] = pd.to_datetime(data["Week Date"])

# Change the name of machines containing "grinder" to "Grinder"
data["Machine Name"] = data["Machine Name"].str.replace(r"(?i).*grinder.*", "Grinder", regex=True)

# Define metrics and machines
metrics = [
    "Total Machine Hours",
    "Total Man Hours",
    "Total Output Weight (lbs)",
    "Labor Cost",
    "Total Expense",
    "Production Cost per Pound",
    "Output per Hour"
]
machines = data["Machine Name"].unique()

class DataVisualizationDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Visualization Options")
        self.setup_ui()

    def setup_ui(self):
        self.layout = QVBoxLayout()

        self.layout.addWidget(QLabel("Select Machines:"))
        self.machine_list = QListWidget()
        self.machine_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self.machine_list.addItems(machines)
        self.layout.addWidget(self.machine_list)

        self.layout.addWidget(QLabel("Select Metric:"))
        self.metric_combo = QComboBox()
        self.metric_combo.addItems(metrics)
        self.layout.addWidget(self.metric_combo)

        self.layout.addWidget(QLabel("Select Plot Type:"))
        self.plot_type_combo = QComboBox()
        self.plot_type_combo.addItems(["Scatter with Trend Line", "Bar Graph"])
        self.layout.addWidget(self.plot_type_combo)

        self.layout.addWidget(QLabel("Select Start Date:"))
        self.start_date_edit = QDateEdit(calendarPopup=True)
        self.start_date_edit.setDate(QDate.fromString(data["Week Date"].min().strftime('%Y-%m-%d'), "yyyy-MM-dd"))
        self.layout.addWidget(self.start_date_edit)

        self.layout.addWidget(QLabel("Select End Date:"))
        self.end_date_edit = QDateEdit(calendarPopup=True)
        self.end_date_edit.setDate(QDate.fromString(data["Week Date"].max().strftime('%Y-%m-%d'), "yyyy-MM-dd"))
        self.layout.addWidget(self.end_date_edit)

        submit_button = QPushButton("OK", clicked=self.on_submit)
        self.layout.addWidget(submit_button)

        auto_graph_button = QPushButton("Auto-Generate Graphs", clicked=self.auto_generate_graphs)
        self.layout.addWidget(auto_graph_button)

        self.setLayout(self.layout)

    def on_submit(self):
        selected_machines = [item.text() for item in self.machine_list.selectedItems()]
        selected_metric = self.metric_combo.currentText()
        plot_type = self.plot_type_combo.currentText()
        start_date = pd.to_datetime(self.start_date_edit.date().toString("yyyy-MM-dd"))
        end_date = pd.to_datetime(self.end_date_edit.date().toString("yyyy-MM-dd"))

        if not selected_machines:
            QMessageBox.critical(self, "Error", "Please select at least one machine.")
            return
        self.create_plot(selected_machines, selected_metric, plot_type, start_date, end_date)

    def create_plot(self, selected_machines, metric, plot_type, start_date, end_date):
        filtered_data = data[(data["Week Date"] >= start_date) & (data["Week Date"] <= end_date)]
        filtered_data = filtered_data[filtered_data["Machine Name"].isin(selected_machines)].sort_values("Week Date")

        plt.figure(figsize=(14, 10))

        if plot_type == "Scatter with Trend Line":
            for machine in selected_machines:
                machine_data = filtered_data[filtered_data["Machine Name"] == machine]
                avg_value = machine_data[metric].mean()
                plt.scatter(machine_data["Week Date"], machine_data[metric], label=f"{machine} Data (Avg: {avg_value:.3f})")

                if len(machine_data) > 1:
                    x = (machine_data["Week Date"] - machine_data["Week Date"].min()).dt.days
                    y = machine_data[metric]
                    trend = np.polyfit(x, y, 1)
                    trend_line = np.poly1d(trend)
                    plt.plot(machine_data["Week Date"], trend_line(x), linestyle="--", label=f"{machine} Trend")
        elif plot_type == "Bar Graph":
            bar_width = 5 / len(selected_machines)
            for idx, machine in enumerate(selected_machines):
                machine_data = filtered_data[filtered_data["Machine Name"] == machine]
                plt.bar(
                    machine_data["Week Date"] + pd.to_timedelta(idx * bar_width, unit="d"),
                    machine_data[metric],
                    width=bar_width,
                    label=f"{machine}"
                )

        plt.title(f"{metric} Analysis ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})", fontsize=16)
        plt.xlabel("Week Date", fontsize=12)
        plt.ylabel(metric, fontsize=12)
        plt.legend(title="Machines", fontsize=10, loc="upper left", bbox_to_anchor=(1.05, 1))
        plt.grid(True, linestyle="--", alpha=0.7)
        plt.ylim(bottom=0)

        filename = f"{metric}_Analysis_{plot_type.replace(' ', '_')}_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.png"
        plt.savefig(filename, bbox_inches="tight")
        plt.close()
        QMessageBox.information(self, "Success", f"Plot saved as {filename}.")

    def auto_generate_graphs(self):
        base_folder = "auto_generated_graphs"
        os.makedirs(base_folder, exist_ok=True)

        for machine in machines:
            machine_folder = os.path.join(base_folder, machine)
            os.makedirs(machine_folder, exist_ok=True)

            for plot_type in ["Scatter with Trend Line", "Bar Graph"]:
                plot_type_folder = os.path.join(machine_folder, plot_type.replace(" ", "_"))
                os.makedirs(plot_type_folder, exist_ok=True)

                for metric in metrics:
                    filtered_data = data[data["Machine Name"] == machine].sort_values("Week Date")
                    start_date = data["Week Date"].min()
                    end_date = data["Week Date"].max()

                    plt.figure(figsize=(14, 10))

                    if plot_type == "Scatter with Trend Line":
                        machine_data = filtered_data
                        avg_value = machine_data[metric].mean()
                        plt.scatter(machine_data["Week Date"], machine_data[metric], label=f"{machine} Data (Avg: {avg_value:.3f})")

                        if len(machine_data) > 1:
                            x = (machine_data["Week Date"] - machine_data["Week Date"].min()).dt.days
                            y = machine_data[metric]
                            trend = np.polyfit(x, y, 1)
                            trend_line = np.poly1d(trend)
                            plt.plot(machine_data["Week Date"], trend_line(x), linestyle="--", label=f"{machine} Trend")
                    elif plot_type == "Bar Graph":
                        plt.bar(
                            machine_data["Week Date"],
                            machine_data[metric],
                            label=f"{machine}"
                        )

                    plt.title(f"{metric} Analysis ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})", fontsize=16)
                    plt.xlabel("Week Date", fontsize=12)
                    plt.ylabel(metric, fontsize=12)
                    plt.legend(title="Machines", fontsize=10, loc="upper left", bbox_to_anchor=(1.05, 1))
                    plt.grid(True, linestyle="--", alpha=0.7)
                    plt.ylim(bottom=0)

                    filename = os.path.join(plot_type_folder, f"{metric}_{machine}.png")
                    plt.savefig(filename, bbox_inches="tight")
                    plt.close()

        all_machines_folder = os.path.join(base_folder, "all_machines")
        os.makedirs(all_machines_folder, exist_ok=True)

        for plot_type in ["Scatter with Trend Line", "Bar Graph"]:
            plot_type_folder = os.path.join(all_machines_folder, plot_type.replace(" ", "_"))
            os.makedirs(plot_type_folder, exist_ok=True)

            for metric in metrics:
                plt.figure(figsize=(14, 10))

                if plot_type == "Scatter with Trend Line":
                    for machine in machines:
                        machine_data = data[data["Machine Name"] == machine].sort_values("Week Date")
                        avg_value = machine_data[metric].mean()
                        plt.scatter(machine_data["Week Date"], machine_data[metric], label=f"{machine} Data (Avg: {avg_value:.3f})")

                        if len(machine_data) > 1:
                            x = (machine_data["Week Date"] - machine_data["Week Date"].min()).dt.days
                            y = machine_data[metric]
                            trend = np.polyfit(x, y, 1)
                            trend_line = np.poly1d(trend)
                            plt.plot(machine_data["Week Date"], trend_line(x), linestyle="--", label=f"{machine} Trend")
                elif plot_type == "Bar Graph":
                    bar_width = 0.8 / len(machines)
                    for idx, machine in enumerate(machines):
                        machine_data = data[data["Machine Name"] == machine].sort_values("Week Date")
                        plt.bar(
                            machine_data["Week Date"] + pd.to_timedelta(idx * bar_width, unit="d"),
                            machine_data[metric],
                            width=bar_width,
                            label=f"{machine}"
                        )

                plt.title(f"{metric} Analysis (All Machines)", fontsize=16)
                plt.xlabel("Week Date", fontsize=12)
                plt.ylabel(metric, fontsize=12)
                plt.legend(title="Machines", fontsize=10, loc="upper left", bbox_to_anchor=(1.05, 1))
                plt.grid(True, linestyle="--", alpha=0.7)
                plt.ylim(bottom=0)

                filename = os.path.join(plot_type_folder, f"{metric}_all_machines.png")
                plt.savefig(filename, bbox_inches="tight")
                plt.close()

        QMessageBox.information(self, "Success", f"Graphs saved to folder: {base_folder}.")
        self.close()

if __name__ == "__main__":
    app = QApplication([])
    dialog = DataVisualizationDialog()
    dialog.exec_()
