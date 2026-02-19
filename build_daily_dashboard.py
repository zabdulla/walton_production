"""
Daily Processing Dashboard Builder - Redesigned.

Generates an interactive HTML dashboard for daily production data with:
- Week/Month period selector with navigation
- Calendar-style grid showing data completeness per day
- Clear handling of missing data (supervisor forgot to input)
- Notes displayed as indicators on days, not separate section
- Focused visualizations for selected time period

Usage:
    python build_daily_dashboard.py
"""

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_DAILY_INPUT = Path("processing_reports/aggregated_daily_data.xlsx")
DEFAULT_NOTES_INPUT = Path("processing_reports/aggregated_notes.xlsx")
DEFAULT_OUTPUT = Path("docs/daily.html")

CHART_PALETTE = [
    "#0B6E4F", "#2CA58D", "#84BCDA", "#33658A", "#F26419",
    "#FFAF87", "#3A3042", "#5BC0BE", "#C5283D", "#1f77b4",
]


def load_data(daily_path: Path, notes_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load daily data and notes from Excel files."""
    daily = pd.read_excel(daily_path)
    daily["Date"] = pd.to_datetime(daily["Date"])
    daily = daily.sort_values(["Date", "Machine_Name"])

    notes = pd.DataFrame()
    if notes_path.exists():
        notes = pd.read_excel(notes_path)
        notes["Date"] = pd.to_datetime(notes["Date"])
        notes = notes.sort_values("Date", ascending=False)

    return daily, notes


def prepare_daily_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create day-level summary across all machines."""
    summary = (
        daily.groupby("Date")
        .agg(
            Total_Output=("Actual_Output", "sum"),
            Total_Machine_Hours=("Machine_Hours", "sum"),
            Total_Man_Hours=("Man_Hours", "sum"),
            Records=("Date", "count"),
            Avg_Quality=("Data_Quality_Score", "mean"),
            Has_Hours=("Has_Machine_Hours", "any"),
            Has_Output=("Has_Output", "any"),
            Machines_Active=("Machine_Name", "nunique"),
        )
        .reset_index()
    )
    summary["Date_Str"] = summary["Date"].dt.strftime("%Y-%m-%d")
    summary["Day_Name"] = summary["Date"].dt.day_name().str[:3]
    summary["Week_Start"] = summary["Date"] - pd.to_timedelta(summary["Date"].dt.dayofweek, unit="d")
    summary["Week_Start_Str"] = summary["Week_Start"].dt.strftime("%Y-%m-%d")
    summary["Month"] = summary["Date"].dt.to_period("M").astype(str)

    # Data status: "complete", "partial", "missing"
    summary["Status"] = "complete"
    summary.loc[(summary["Total_Output"] == 0) | (~summary["Has_Output"]), "Status"] = "partial"
    summary.loc[(summary["Total_Machine_Hours"] == 0) & (summary["Total_Man_Hours"] == 0), "Status"] = "missing"

    return summary


def prepare_machine_daily(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate data by date and machine for charts."""
    agg = (
        daily.groupby(["Date", "Machine_Name"])
        .agg(
            Actual_Output=("Actual_Output", "sum"),
            Machine_Hours=("Machine_Hours", "sum"),
            Man_Hours=("Man_Hours", "sum"),
            Avg_Quality=("Data_Quality_Score", "mean"),
        )
        .reset_index()
    )
    agg["Date_Str"] = agg["Date"].dt.strftime("%Y-%m-%d")
    agg["Week_Start"] = agg["Date"] - pd.to_timedelta(agg["Date"].dt.dayofweek, unit="d")
    agg["Week_Start_Str"] = agg["Week_Start"].dt.strftime("%Y-%m-%d")
    agg["Month"] = agg["Date"].dt.to_period("M").astype(str)
    return agg


def prepare_notes_by_date(notes: pd.DataFrame) -> dict:
    """Create a dict of date -> list of notes."""
    if notes.empty:
        return {}

    notes_dict = {}
    for _, row in notes.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        note_info = {
            "machine": row.get("Machine_Name", ""),
            "category": row.get("Category", "operational"),
            "note": row.get("Note", ""),
            "operator": row.get("Operator", ""),
            "shift": row.get("Shift", ""),
        }
        if date_str not in notes_dict:
            notes_dict[date_str] = []
        notes_dict[date_str].append(note_info)

    return notes_dict


def get_weeks_list(daily_summary: pd.DataFrame) -> list[dict]:
    """Get list of unique weeks with their date ranges."""
    weeks = []
    for week_start_str in sorted(daily_summary["Week_Start_Str"].unique()):
        week_data = daily_summary[daily_summary["Week_Start_Str"] == week_start_str]
        week_start = pd.to_datetime(week_start_str)
        week_end = week_start + pd.Timedelta(days=6)
        weeks.append({
            "start": week_start_str,
            "end": week_end.strftime("%Y-%m-%d"),
            "label": f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}",
            "days": len(week_data),
        })
    return weeks


def get_months_list(daily_summary: pd.DataFrame) -> list[dict]:
    """Get list of unique months."""
    months = []
    for month_str in sorted(daily_summary["Month"].unique()):
        month_data = daily_summary[daily_summary["Month"] == month_str]
        months.append({
            "value": month_str,
            "label": pd.to_datetime(month_str).strftime("%B %Y"),
            "days": len(month_data),
        })
    return months


def build_dashboard_html(
    daily_summary: pd.DataFrame,
    machine_daily: pd.DataFrame,
    notes_by_date: dict,
    weeks_list: list[dict],
    months_list: list[dict],
    machines: list[str],
) -> str:
    """Build the complete dashboard HTML with embedded data."""

    # Convert dataframes to JSON for JavaScript
    summary_json = daily_summary.to_json(orient="records", date_format="iso")
    machine_json = machine_daily.to_json(orient="records", date_format="iso")
    notes_json = json.dumps(notes_by_date)
    weeks_json = json.dumps(weeks_list)
    months_json = json.dumps(months_list)
    machines_json = json.dumps(machines)
    palette_json = json.dumps(CHART_PALETTE)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Daily Processing Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        :root {{
            --primary: #111827;
            --accent: #3b82f6;
            --border: #e5e7eb;
            --bg: #f9fafc;
            --complete: #10b981;
            --partial: #f59e0b;
            --missing: #ef4444;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            padding: 24px;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: var(--bg);
            color: var(--primary);
        }}
        header {{
            margin-bottom: 24px;
        }}
        h1 {{
            margin: 0 0 8px;
            font-size: 1.75rem;
        }}
        .subtitle {{
            color: #6b7280;
            margin: 0 0 12px;
        }}
        .nav-link {{
            display: inline-block;
            padding: 8px 16px;
            background: var(--accent);
            color: white;
            text-decoration: none;
            border-radius: 6px;
            font-size: 14px;
        }}
        .nav-link:hover {{
            background: #2563eb;
        }}

        /* Period selector */
        .period-controls {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 20px;
            flex-wrap: wrap;
            background: white;
            padding: 16px;
            border-radius: 12px;
            border: 1px solid var(--border);
        }}
        .period-type-btns {{
            display: flex;
            gap: 4px;
        }}
        .period-type-btn {{
            padding: 8px 16px;
            border: 1px solid var(--border);
            background: white;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.2s;
        }}
        .period-type-btn:first-child {{ border-radius: 6px 0 0 6px; }}
        .period-type-btn:last-child {{ border-radius: 0 6px 6px 0; }}
        .period-type-btn.active {{
            background: var(--accent);
            color: white;
            border-color: var(--accent);
        }}
        .period-nav {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .nav-btn {{
            width: 36px;
            height: 36px;
            border: 1px solid var(--border);
            background: white;
            border-radius: 6px;
            cursor: pointer;
            font-size: 18px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .nav-btn:hover {{
            background: #f3f4f6;
        }}
        .nav-btn:disabled {{
            opacity: 0.3;
            cursor: not-allowed;
        }}
        .period-label {{
            font-weight: 600;
            min-width: 200px;
            text-align: center;
        }}
        .period-select {{
            padding: 8px 12px;
            border: 1px solid var(--border);
            border-radius: 6px;
            font-size: 14px;
            background: white;
        }}

        /* Status legend */
        .legend {{
            display: flex;
            gap: 16px;
            margin-left: auto;
            font-size: 13px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .legend-dot {{
            width: 12px;
            height: 12px;
            border-radius: 3px;
        }}
        .legend-dot.complete {{ background: var(--complete); }}
        .legend-dot.partial {{ background: var(--partial); }}
        .legend-dot.missing {{ background: var(--missing); }}
        .legend-dot.has-note {{
            background: white;
            border: 2px solid var(--accent);
        }}

        /* Calendar grid */
        .calendar-section {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid var(--border);
        }}
        .calendar-section h2 {{
            margin: 0 0 16px;
            font-size: 1.1rem;
        }}
        .calendar-grid {{
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 8px;
        }}
        .calendar-header {{
            text-align: center;
            font-weight: 600;
            font-size: 12px;
            color: #6b7280;
            padding: 8px 0;
        }}
        .calendar-day {{
            padding: 12px 8px;
            border-radius: 8px;
            text-align: center;
            min-height: 80px;
            cursor: pointer;
            transition: transform 0.1s, box-shadow 0.1s;
            position: relative;
        }}
        .calendar-day:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }}
        .calendar-day.complete {{ background: #d1fae5; border: 1px solid var(--complete); }}
        .calendar-day.partial {{ background: #fef3c7; border: 1px solid var(--partial); }}
        .calendar-day.missing {{ background: #fee2e2; border: 1px solid var(--missing); }}
        .calendar-day.empty {{ background: #f3f4f6; border: 1px solid transparent; cursor: default; }}
        .calendar-day.empty:hover {{ transform: none; box-shadow: none; }}
        .calendar-day.has-note::after {{
            content: "";
            position: absolute;
            top: 6px;
            right: 6px;
            width: 10px;
            height: 10px;
            background: var(--accent);
            border-radius: 50%;
        }}
        .day-date {{
            font-weight: 600;
            font-size: 14px;
            margin-bottom: 4px;
        }}
        .day-output {{
            font-size: 12px;
            color: #374151;
        }}
        .day-output.zero {{
            color: #9ca3af;
            font-style: italic;
        }}

        /* Day detail popup */
        .day-detail {{
            display: none;
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 24px;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            z-index: 1000;
            max-width: 500px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
        }}
        .day-detail.active {{
            display: block;
        }}
        .overlay {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0,0,0,0.5);
            z-index: 999;
        }}
        .overlay.active {{
            display: block;
        }}
        .day-detail-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}
        .day-detail-header h3 {{
            margin: 0;
        }}
        .close-btn {{
            background: none;
            border: none;
            font-size: 24px;
            cursor: pointer;
            color: #6b7280;
        }}
        .detail-stats {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin-bottom: 16px;
        }}
        .detail-stat {{
            background: #f9fafc;
            padding: 12px;
            border-radius: 8px;
        }}
        .detail-stat-label {{
            font-size: 11px;
            color: #6b7280;
            text-transform: uppercase;
        }}
        .detail-stat-value {{
            font-size: 18px;
            font-weight: 600;
        }}
        .detail-notes {{
            background: #eff6ff;
            padding: 12px;
            border-radius: 8px;
            border-left: 4px solid var(--accent);
        }}
        .detail-notes h4 {{
            margin: 0 0 8px;
            font-size: 13px;
            color: var(--accent);
        }}
        .detail-note-item {{
            font-size: 14px;
            margin-bottom: 8px;
            padding-bottom: 8px;
            border-bottom: 1px solid #dbeafe;
        }}
        .detail-note-item:last-child {{
            margin-bottom: 0;
            padding-bottom: 0;
            border-bottom: none;
        }}
        .detail-note-meta {{
            font-size: 11px;
            color: #6b7280;
            margin-bottom: 4px;
        }}
        .missing-warning {{
            background: #fef2f2;
            border: 1px solid #fecaca;
            padding: 12px;
            border-radius: 8px;
            color: #b91c1c;
            font-size: 13px;
            margin-bottom: 16px;
        }}

        /* KPI cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }}
        .kpi-card {{
            background: white;
            border-radius: 12px;
            padding: 16px;
            text-align: center;
            border: 1px solid var(--border);
        }}
        .kpi-label {{
            font-size: 11px;
            color: #6b7280;
            text-transform: uppercase;
            margin-bottom: 4px;
        }}
        .kpi-value {{
            font-size: 1.25rem;
            font-weight: 600;
        }}
        .kpi-value.good {{ color: var(--complete); }}
        .kpi-value.warn {{ color: var(--partial); }}
        .kpi-value.bad {{ color: var(--missing); }}

        /* Chart section */
        .chart-section {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid var(--border);
        }}
        .chart-section h2 {{
            margin: 0 0 16px;
            font-size: 1.1rem;
        }}

        /* Mobile responsive */
        @media (max-width: 768px) {{
            body {{ padding: 12px; }}
            .period-controls {{
                flex-direction: column;
                align-items: stretch;
            }}
            .legend {{
                margin-left: 0;
                flex-wrap: wrap;
            }}
            .calendar-grid {{
                gap: 4px;
            }}
            .calendar-day {{
                padding: 8px 4px;
                min-height: 60px;
            }}
            .day-date {{ font-size: 12px; }}
            .day-output {{ font-size: 10px; }}
            .kpi-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
        }}
        @media (max-width: 480px) {{
            .kpi-grid {{
                grid-template-columns: 1fr;
            }}
            .detail-stats {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <header>
        <h1>Daily Processing Dashboard</h1>
        <p class="subtitle">View production data by week or month. Click any day for details. Blue dots indicate supervisor notes.</p>
        <a href="index.html" class="nav-link">← Weekly Summary</a>
    </header>

    <div class="period-controls">
        <div class="period-type-btns">
            <button class="period-type-btn active" data-type="week">Week</button>
            <button class="period-type-btn" data-type="month">Month</button>
        </div>
        <div class="period-nav">
            <button class="nav-btn" id="prevPeriod">‹</button>
            <span class="period-label" id="periodLabel">Loading...</span>
            <button class="nav-btn" id="nextPeriod">›</button>
        </div>
        <select class="period-select" id="periodSelect"></select>
        <div class="legend">
            <div class="legend-item"><span class="legend-dot complete"></span> Complete</div>
            <div class="legend-item"><span class="legend-dot partial"></span> Partial</div>
            <div class="legend-item"><span class="legend-dot missing"></span> No Data</div>
            <div class="legend-item"><span class="legend-dot has-note"></span> Has Notes</div>
        </div>
    </div>

    <div class="kpi-grid" id="kpiGrid"></div>

    <div class="calendar-section">
        <h2>Daily Status</h2>
        <div class="calendar-grid" id="calendarGrid"></div>
    </div>

    <div class="chart-section">
        <h2>Daily Output by Machine</h2>
        <div id="outputChart"></div>
    </div>

    <div class="chart-section">
        <h2>Machine Hours</h2>
        <div id="hoursChart"></div>
    </div>

    <!-- Day detail popup -->
    <div class="overlay" id="overlay"></div>
    <div class="day-detail" id="dayDetail">
        <div class="day-detail-header">
            <h3 id="detailTitle">Day Details</h3>
            <button class="close-btn" id="closeDetail">×</button>
        </div>
        <div id="detailContent"></div>
    </div>

    <script>
        // Embedded data
        const dailySummary = {summary_json};
        const machineDaily = {machine_json};
        const notesByDate = {notes_json};
        const weeksList = {weeks_json};
        const monthsList = {months_json};
        const machines = {machines_json};
        const palette = {palette_json};

        // State
        let periodType = 'week'; // 'week' or 'month'
        let currentIndex = weeksList.length - 1; // Start with most recent

        // Elements
        const periodLabel = document.getElementById('periodLabel');
        const periodSelect = document.getElementById('periodSelect');
        const prevBtn = document.getElementById('prevPeriod');
        const nextBtn = document.getElementById('nextPeriod');
        const calendarGrid = document.getElementById('calendarGrid');
        const kpiGrid = document.getElementById('kpiGrid');
        const dayDetail = document.getElementById('dayDetail');
        const overlay = document.getElementById('overlay');

        // Initialize
        function init() {{
            // Set up period type buttons
            document.querySelectorAll('.period-type-btn').forEach(btn => {{
                btn.addEventListener('click', () => {{
                    document.querySelectorAll('.period-type-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    periodType = btn.dataset.type;
                    currentIndex = periodType === 'week' ? weeksList.length - 1 : monthsList.length - 1;
                    updatePeriodSelect();
                    render();
                }});
            }});

            // Navigation
            prevBtn.addEventListener('click', () => {{
                if (currentIndex > 0) {{
                    currentIndex--;
                    periodSelect.value = currentIndex;
                    render();
                }}
            }});
            nextBtn.addEventListener('click', () => {{
                const max = periodType === 'week' ? weeksList.length - 1 : monthsList.length - 1;
                if (currentIndex < max) {{
                    currentIndex++;
                    periodSelect.value = currentIndex;
                    render();
                }}
            }});
            periodSelect.addEventListener('change', (e) => {{
                currentIndex = parseInt(e.target.value);
                render();
            }});

            // Close detail
            document.getElementById('closeDetail').addEventListener('click', closeDetail);
            overlay.addEventListener('click', closeDetail);

            updatePeriodSelect();
            render();
        }}

        function updatePeriodSelect() {{
            const list = periodType === 'week' ? weeksList : monthsList;
            periodSelect.innerHTML = list.map((item, idx) => {{
                const label = periodType === 'week' ? item.label : item.label;
                return `<option value="${{idx}}">${{label}}</option>`;
            }}).join('');
            periodSelect.value = currentIndex;
        }}

        function getCurrentPeriodData() {{
            if (periodType === 'week') {{
                const week = weeksList[currentIndex];
                return {{
                    label: week.label,
                    filter: (d) => d.Week_Start_Str === week.start,
                    startDate: new Date(week.start),
                    endDate: new Date(week.end),
                }};
            }} else {{
                const month = monthsList[currentIndex];
                return {{
                    label: month.label,
                    filter: (d) => d.Month === month.value,
                    startDate: new Date(month.value + '-01'),
                    endDate: new Date(new Date(month.value + '-01').getFullYear(), new Date(month.value + '-01').getMonth() + 1, 0),
                }};
            }}
        }}

        function render() {{
            const period = getCurrentPeriodData();
            periodLabel.textContent = period.label;

            // Update nav buttons
            prevBtn.disabled = currentIndex === 0;
            const max = periodType === 'week' ? weeksList.length - 1 : monthsList.length - 1;
            nextBtn.disabled = currentIndex >= max;

            const filteredSummary = dailySummary.filter(period.filter);
            const filteredMachine = machineDaily.filter(period.filter);

            renderKPIs(filteredSummary);
            renderCalendar(period, filteredSummary);
            renderOutputChart(filteredMachine);
            renderHoursChart(filteredMachine);
        }}

        function renderKPIs(data) {{
            if (data.length === 0) {{
                kpiGrid.innerHTML = '<div class="kpi-card"><div class="kpi-value">No data for this period</div></div>';
                return;
            }}

            const totalOutput = data.reduce((sum, d) => sum + (d.Total_Output || 0), 0);
            const totalHours = data.reduce((sum, d) => sum + (d.Total_Machine_Hours || 0), 0);
            const avgQuality = data.reduce((sum, d) => sum + (d.Avg_Quality || 0), 0) / data.length;
            const completeDays = data.filter(d => d.Status === 'complete').length;
            const partialDays = data.filter(d => d.Status === 'partial').length;
            const missingDays = data.filter(d => d.Status === 'missing').length;
            const daysWithNotes = data.filter(d => notesByDate[d.Date_Str]).length;

            const qualityClass = avgQuality >= 80 ? 'good' : avgQuality >= 50 ? 'warn' : 'bad';
            const completeClass = completeDays === data.length ? 'good' : completeDays > data.length / 2 ? 'warn' : 'bad';

            kpiGrid.innerHTML = `
                <div class="kpi-card">
                    <div class="kpi-label">Total Output</div>
                    <div class="kpi-value">${{totalOutput.toLocaleString()}} lbs</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Machine Hours</div>
                    <div class="kpi-value">${{totalHours.toLocaleString(undefined, {{maximumFractionDigits: 1}})}}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Avg Quality</div>
                    <div class="kpi-value ${{qualityClass}}">${{avgQuality.toFixed(0)}}%</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Complete Days</div>
                    <div class="kpi-value ${{completeClass}}">${{completeDays}} / ${{data.length}}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Partial Data</div>
                    <div class="kpi-value ${{partialDays > 0 ? 'warn' : ''}}">${{partialDays}}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">No Data</div>
                    <div class="kpi-value ${{missingDays > 0 ? 'bad' : ''}}">${{missingDays}}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Days w/ Notes</div>
                    <div class="kpi-value">${{daysWithNotes}}</div>
                </div>
            `;
        }}

        function renderCalendar(period, data) {{
            // Create date lookup
            const dateMap = {{}};
            data.forEach(d => {{ dateMap[d.Date_Str] = d; }});

            // Build calendar
            let html = '';
            const dayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
            dayNames.forEach(name => {{
                html += `<div class="calendar-header">${{name}}</div>`;
            }});

            // Get all days in period
            const start = period.startDate;
            const end = period.endDate;

            // Pad to start of week (Monday)
            const startDay = start.getDay();
            const padStart = startDay === 0 ? 6 : startDay - 1; // Monday = 0
            for (let i = 0; i < padStart; i++) {{
                html += '<div class="calendar-day empty"></div>';
            }}

            // Render each day
            const current = new Date(start);
            while (current <= end) {{
                const dateStr = current.toISOString().split('T')[0];
                const dayData = dateMap[dateStr];
                const hasNote = notesByDate[dateStr];

                if (dayData) {{
                    const status = dayData.Status;
                    const outputText = dayData.Total_Output > 0
                        ? `${{(dayData.Total_Output / 1000).toFixed(1)}}k lbs`
                        : '<span class="zero">No output</span>';
                    const noteClass = hasNote ? ' has-note' : '';

                    html += `
                        <div class="calendar-day ${{status}}${{noteClass}}" data-date="${{dateStr}}" onclick="showDayDetail('${{dateStr}}')">
                            <div class="day-date">${{current.getDate()}}</div>
                            <div class="day-output">${{outputText}}</div>
                        </div>
                    `;
                }} else {{
                    // Day in range but no data
                    const dayOfWeek = current.getDay();
                    if (dayOfWeek !== 0) {{ // Not Sunday (typically no work)
                        html += `
                            <div class="calendar-day missing" data-date="${{dateStr}}" onclick="showDayDetail('${{dateStr}}')">
                                <div class="day-date">${{current.getDate()}}</div>
                                <div class="day-output zero">No data</div>
                            </div>
                        `;
                    }} else {{
                        html += `
                            <div class="calendar-day empty">
                                <div class="day-date" style="color:#9ca3af">${{current.getDate()}}</div>
                            </div>
                        `;
                    }}
                }}

                current.setDate(current.getDate() + 1);
            }}

            calendarGrid.innerHTML = html;
        }}

        function showDayDetail(dateStr) {{
            const dayData = dailySummary.find(d => d.Date_Str === dateStr);
            const dayNotes = notesByDate[dateStr] || [];
            const dayMachines = machineDaily.filter(d => d.Date_Str === dateStr);

            const date = new Date(dateStr);
            const dateLabel = date.toLocaleDateString('en-US', {{ weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }});

            document.getElementById('detailTitle').textContent = dateLabel;

            let content = '';

            if (!dayData) {{
                content = `
                    <div class="missing-warning">
                        <strong>⚠️ No data recorded for this day</strong><br>
                        This likely means the supervisor forgot to input the production data.
                    </div>
                `;
            }} else {{
                const statusLabel = dayData.Status === 'complete' ? '✓ Complete'
                    : dayData.Status === 'partial' ? '⚠ Partial Data'
                    : '✗ Missing Data';
                const statusColor = dayData.Status === 'complete' ? 'var(--complete)'
                    : dayData.Status === 'partial' ? 'var(--partial)'
                    : 'var(--missing)';

                content += `
                    <div class="detail-stats">
                        <div class="detail-stat">
                            <div class="detail-stat-label">Status</div>
                            <div class="detail-stat-value" style="color:${{statusColor}}">${{statusLabel}}</div>
                        </div>
                        <div class="detail-stat">
                            <div class="detail-stat-label">Total Output</div>
                            <div class="detail-stat-value">${{(dayData.Total_Output || 0).toLocaleString()}} lbs</div>
                        </div>
                        <div class="detail-stat">
                            <div class="detail-stat-label">Machine Hours</div>
                            <div class="detail-stat-value">${{(dayData.Total_Machine_Hours || 0).toFixed(1)}}</div>
                        </div>
                        <div class="detail-stat">
                            <div class="detail-stat-label">Man Hours</div>
                            <div class="detail-stat-value">${{(dayData.Total_Man_Hours || 0).toFixed(1)}}</div>
                        </div>
                        <div class="detail-stat">
                            <div class="detail-stat-label">Machines Active</div>
                            <div class="detail-stat-value">${{dayData.Machines_Active || 0}}</div>
                        </div>
                        <div class="detail-stat">
                            <div class="detail-stat-label">Quality Score</div>
                            <div class="detail-stat-value">${{(dayData.Avg_Quality || 0).toFixed(0)}}%</div>
                        </div>
                    </div>
                `;

                if (dayData.Status === 'missing' || dayData.Status === 'partial') {{
                    content += `
                        <div class="missing-warning">
                            ${{dayData.Status === 'missing'
                                ? '⚠️ No machine/man hours recorded - supervisor may have forgotten to input data'
                                : '⚠️ Some data missing - output may be incomplete'}}
                        </div>
                    `;
                }}
            }}

            // Show notes if any
            if (dayNotes.length > 0) {{
                content += `
                    <div class="detail-notes">
                        <h4>📝 Supervisor Notes (${{dayNotes.length}})</h4>
                        ${{dayNotes.map(n => `
                            <div class="detail-note-item">
                                <div class="detail-note-meta">${{n.machine}} | ${{n.shift}} shift | ${{n.operator || 'Unknown operator'}} | <em>${{n.category}}</em></div>
                                <div>${{n.note}}</div>
                            </div>
                        `).join('')}}
                    </div>
                `;
            }}

            // Show machine breakdown if available
            if (dayMachines.length > 0) {{
                content += `
                    <h4 style="margin: 16px 0 8px;">Machine Breakdown</h4>
                    <table style="width:100%; font-size:13px; border-collapse:collapse;">
                        <tr style="background:#f3f4f6;">
                            <th style="padding:8px; text-align:left;">Machine</th>
                            <th style="padding:8px; text-align:right;">Output</th>
                            <th style="padding:8px; text-align:right;">Hours</th>
                        </tr>
                        ${{dayMachines.map(m => `
                            <tr style="border-bottom:1px solid #e5e7eb;">
                                <td style="padding:8px;">${{m.Machine_Name}}</td>
                                <td style="padding:8px; text-align:right;">${{(m.Actual_Output || 0).toLocaleString()}}</td>
                                <td style="padding:8px; text-align:right;">${{(m.Machine_Hours || 0).toFixed(1)}}</td>
                            </tr>
                        `).join('')}}
                    </table>
                `;
            }}

            document.getElementById('detailContent').innerHTML = content;
            dayDetail.classList.add('active');
            overlay.classList.add('active');
        }}

        function closeDetail() {{
            dayDetail.classList.remove('active');
            overlay.classList.remove('active');
        }}

        function renderOutputChart(data) {{
            if (data.length === 0) {{
                document.getElementById('outputChart').innerHTML = '<p style="color:#9ca3af;text-align:center;">No data for this period</p>';
                return;
            }}

            // Group by date
            const dates = [...new Set(data.map(d => d.Date_Str))].sort();

            const traces = machines.map((machine, idx) => {{
                const machineData = data.filter(d => d.Machine_Name === machine);
                return {{
                    x: machineData.map(d => d.Date_Str),
                    y: machineData.map(d => d.Actual_Output || 0),
                    name: machine,
                    type: 'bar',
                    marker: {{ color: palette[idx % palette.length] }},
                }};
            }});

            Plotly.newPlot('outputChart', traces, {{
                barmode: 'stack',
                xaxis: {{ title: '', tickangle: -45 }},
                yaxis: {{ title: 'Output (Lbs)' }},
                legend: {{ orientation: 'h', y: -0.3 }},
                margin: {{ t: 20, b: 100, l: 60, r: 20 }},
                height: 350,
            }}, {{ responsive: true }});
        }}

        function renderHoursChart(data) {{
            if (data.length === 0) {{
                document.getElementById('hoursChart').innerHTML = '<p style="color:#9ca3af;text-align:center;">No data for this period</p>';
                return;
            }}

            const traces = machines.map((machine, idx) => {{
                const machineData = data.filter(d => d.Machine_Name === machine);
                return {{
                    x: machineData.map(d => d.Date_Str),
                    y: machineData.map(d => d.Machine_Hours || 0),
                    name: machine,
                    type: 'bar',
                    marker: {{ color: palette[idx % palette.length] }},
                }};
            }});

            Plotly.newPlot('hoursChart', traces, {{
                barmode: 'stack',
                xaxis: {{ title: '', tickangle: -45 }},
                yaxis: {{ title: 'Machine Hours' }},
                legend: {{ orientation: 'h', y: -0.3 }},
                margin: {{ t: 20, b: 100, l: 60, r: 20 }},
                height: 350,
            }}, {{ responsive: true }});
        }}

        // Initialize on load
        init();
    </script>
</body>
</html>
"""


def main(daily_path: Path, notes_path: Path, output_path: Path) -> None:
    """Main entry point."""
    print(f"Loading data from {daily_path}...")
    daily, notes = load_data(daily_path, notes_path)

    print("Preparing summaries...")
    daily_summary = prepare_daily_summary(daily)
    machine_daily = prepare_machine_daily(daily)
    notes_by_date = prepare_notes_by_date(notes)

    weeks_list = get_weeks_list(daily_summary)
    months_list = get_months_list(daily_summary)
    machines = sorted(daily["Machine_Name"].unique().tolist())

    print(f"Building dashboard with {len(weeks_list)} weeks, {len(months_list)} months...")
    html = build_dashboard_html(
        daily_summary,
        machine_daily,
        notes_by_date,
        weeks_list,
        months_list,
        machines,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    print(f"Wrote daily dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build daily processing dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_DAILY_INPUT,
                        help="Path to aggregated_daily_data.xlsx")
    parser.add_argument("--notes", type=Path, default=DEFAULT_NOTES_INPUT,
                        help="Path to aggregated_notes.xlsx")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Path to write HTML dashboard")
    args = parser.parse_args()
    main(args.input, args.notes, args.output)
