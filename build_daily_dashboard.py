"""
Daily Processing Dashboard Builder.

Generates an interactive HTML dashboard for daily production data including:
- Daily output timeline with running averages
- Day-of-week analysis heatmap
- Operator performance charts
- Data quality indicators
- Supervisor notes timeline

Usage:
    python build_daily_dashboard.py
    python build_daily_dashboard.py --input processing_reports/aggregated_daily_data.xlsx \\
        --notes processing_reports/aggregated_notes.xlsx --output docs/daily.html

The output HTML is self-contained and ready to host on GitHub Pages.
"""

import argparse
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.io import to_html

DEFAULT_DAILY_INPUT = Path("processing_reports/aggregated_daily_data.xlsx")
DEFAULT_NOTES_INPUT = Path("processing_reports/aggregated_notes.xlsx")
DEFAULT_OUTPUT = Path("docs/daily.html")
DEFAULT_RUNNING_AVG_WINDOW = 7

CHART_PALETTE = [
    "#0B6E4F", "#2CA58D", "#84BCDA", "#33658A", "#F26419",
    "#FFAF87", "#3A3042", "#5BC0BE", "#C5283D", "#1f77b4",
]

NOTE_CATEGORY_COLORS = {
    "downtime": "#dc2626",
    "material": "#f59e0b",
    "quality": "#6366f1",
    "operational": "#6b7280",
}

DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


def _fmt_num(value: Any, kind: str = "int") -> str:
    """Format a numeric value for display."""
    if pd.isna(value):
        return "—"
    if kind == "currency":
        return f"${value:,.0f}"
    if kind == "currency4":
        return f"${value:,.4f}"
    if kind == "float1":
        return f"{value:,.1f}"
    if kind == "float2":
        return f"{value:,.2f}"
    return f"{value:,.0f}"


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


def aggregate_by_date_machine(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily data by date and machine for cleaner visualization."""
    grouped = (
        daily.groupby(["Date", "Machine_Name", "Shift"])
        .agg(
            Actual_Output=("Actual_Output", "sum"),
            Machine_Hours=("Machine_Hours", "sum"),
            Man_Hours=("Man_Hours", "sum"),
            Labor_Cost=("Labor_Cost", "sum"),
            Total_Expense=("Total_Expense", "sum"),
            Data_Quality_Score=("Data_Quality_Score", "mean"),
            Records=("Date", "count"),
        )
        .reset_index()
    )
    grouped["Output_per_Hour"] = grouped["Actual_Output"] / grouped["Machine_Hours"].replace(0, pd.NA)
    grouped["Output_per_Man_Hour"] = grouped["Actual_Output"] / grouped["Man_Hours"].replace(0, pd.NA)
    grouped["Cost_per_Pound"] = grouped["Total_Expense"] / grouped["Actual_Output"].replace(0, pd.NA)
    grouped["Day_of_Week"] = grouped["Date"].dt.day_name().str[:3]
    grouped["Date_Label"] = grouped["Date"].dt.strftime("%Y-%m-%d")
    return grouped


def add_running_averages(
    df: pd.DataFrame,
    metrics: list[str],
    window: int = DEFAULT_RUNNING_AVG_WINDOW,
) -> pd.DataFrame:
    """Add running-average columns per machine."""
    df = df.sort_values(["Machine_Name", "Date"]).copy()
    for col in metrics:
        ra_col = f"{col}_RA"
        numeric_series = pd.to_numeric(df[col], errors="coerce")
        df[ra_col] = (
            df.assign(_val=numeric_series)
            .groupby("Machine_Name")["_val"]
            .transform(lambda s: s.rolling(window=window, min_periods=1).mean())
        )
    return df


def build_daily_output_fig(agg: pd.DataFrame) -> go.Figure:
    """Build daily output timeline with running averages."""
    machines = sorted(agg["Machine_Name"].unique())

    fig = go.Figure()

    for idx, machine in enumerate(machines):
        subset = agg[agg["Machine_Name"] == machine].sort_values("Date")
        color = CHART_PALETTE[idx % len(CHART_PALETTE)]

        # Raw daily output (scatter)
        fig.add_trace(go.Scatter(
            x=subset["Date"],
            y=subset["Actual_Output"],
            mode="markers",
            name=f"{machine}",
            marker=dict(size=6, color=color, opacity=0.5),
            hovertemplate=(
                f"<b>{machine}</b><br>"
                "Date: %{x|%Y-%m-%d}<br>"
                "Output: %{y:,.0f} lbs<br>"
                "<extra></extra>"
            ),
            legendgroup=machine,
        ))

        # Running average (line)
        fig.add_trace(go.Scatter(
            x=subset["Date"],
            y=subset["Actual_Output_RA"],
            mode="lines",
            name=f"{machine} (7-day avg)",
            line=dict(width=2, color=color),
            hovertemplate=(
                f"<b>{machine}</b> (7-day avg)<br>"
                "Date: %{x|%Y-%m-%d}<br>"
                "Avg Output: %{y:,.0f} lbs<br>"
                "<extra></extra>"
            ),
            legendgroup=machine,
            showlegend=False,
        ))

    fig.update_layout(
        title="Daily Output by Machine (with 7-Day Running Average)",
        xaxis_title="Date",
        yaxis_title="Output (Lbs)",
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=220, b=100, l=70),
        legend=dict(orientation="v", x=1.02, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
        hovermode="closest",
    )
    fig.update_xaxes(rangeslider=dict(visible=True), showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


def build_day_of_week_heatmap(agg: pd.DataFrame) -> go.Figure:
    """Build heatmap showing average output by machine and day of week."""
    pivot = agg.pivot_table(
        index="Machine_Name",
        columns="Day_of_Week",
        values="Actual_Output",
        aggfunc="mean",
        fill_value=0,
    )
    # Reorder columns to match day order
    pivot = pivot.reindex(columns=[d for d in DAY_ORDER if d in pivot.columns])

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale="Greens",
        hovertemplate="Machine: %{y}<br>Day: %{x}<br>Avg Output: %{z:,.0f} lbs<extra></extra>",
        colorbar=dict(title="Avg Output"),
    ))

    fig.update_layout(
        title="Average Output by Day of Week",
        xaxis_title="Day of Week",
        yaxis_title="Machine",
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=80, l=180),
    )
    return fig


def build_operator_performance_fig(daily: pd.DataFrame) -> go.Figure:
    """Build operator performance chart."""
    # Filter to rows with operator data
    op_data = daily[daily["Operator"].notna() & (daily["Operator"] != "")]
    if op_data.empty:
        fig = go.Figure()
        fig.add_annotation(text="No operator data available", showarrow=False, font_size=16)
        return fig

    # Aggregate by operator
    op_summary = (
        op_data.groupby("Operator")
        .agg(
            Total_Output=("Actual_Output", "sum"),
            Total_Hours=("Machine_Hours", "sum"),
            Days_Worked=("Date", "nunique"),
        )
        .reset_index()
        .sort_values("Total_Output", ascending=True)
        .tail(15)  # Top 15 operators
    )

    op_summary["Output_per_Hour"] = op_summary["Total_Output"] / op_summary["Total_Hours"].replace(0, pd.NA)

    fig = go.Figure(data=go.Bar(
        x=op_summary["Total_Output"],
        y=op_summary["Operator"],
        orientation="h",
        marker_color="#3b82f6",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Total Output: %{x:,.0f} lbs<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title="Top Operators by Total Output",
        xaxis_title="Total Output (Lbs)",
        yaxis_title="Operator",
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=80, l=150),
    )
    return fig


def build_data_quality_fig(agg: pd.DataFrame) -> go.Figure:
    """Build data quality score over time."""
    quality_by_date = (
        agg.groupby("Date")["Data_Quality_Score"]
        .mean()
        .reset_index()
        .sort_values("Date")
    )

    fig = go.Figure()

    # Quality score line
    fig.add_trace(go.Scatter(
        x=quality_by_date["Date"],
        y=quality_by_date["Data_Quality_Score"],
        mode="lines+markers",
        name="Data Quality Score",
        line=dict(color="#059669", width=2),
        marker=dict(size=4),
        hovertemplate="Date: %{x|%Y-%m-%d}<br>Quality Score: %{y:.0f}%<extra></extra>",
    ))

    # Add 80% threshold line
    fig.add_hline(y=80, line_dash="dash", line_color="#9ca3af",
                  annotation_text="80% threshold")

    fig.update_layout(
        title="Data Quality Score Over Time",
        xaxis_title="Date",
        yaxis_title="Quality Score (%)",
        yaxis_range=[0, 105],
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=80, l=70),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb")
    return fig


def build_summary_cards(daily: pd.DataFrame, notes: pd.DataFrame) -> str:
    """Build KPI summary cards HTML."""
    total_records = len(daily)
    total_output = daily["Actual_Output"].sum()
    avg_quality = daily["Data_Quality_Score"].mean()
    date_range = f"{daily['Date'].min():%b %d, %Y} – {daily['Date'].max():%b %d, %Y}"
    machines = daily["Machine_Name"].nunique()
    operators = daily[daily["Operator"].notna()]["Operator"].nunique()
    total_notes = len(notes)
    downtime_notes = len(notes[notes["Category"] == "downtime"]) if not notes.empty else 0

    cards = [
        ("Date Range", date_range),
        ("Total Records", _fmt_num(total_records)),
        ("Total Output", f"{_fmt_num(total_output)} lbs"),
        ("Avg Quality Score", f"{avg_quality:.0f}%"),
        ("Machines", str(machines)),
        ("Operators", str(operators)),
        ("Supervisor Notes", str(total_notes)),
        ("Downtime Issues", str(downtime_notes)),
    ]

    cards_html = "".join(
        f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div></div>'
        for label, value in cards
    )
    return f'<div class="kpi-grid">{cards_html}</div>'


def build_notes_timeline_html(notes: pd.DataFrame) -> str:
    """Build searchable notes timeline HTML."""
    if notes.empty:
        return '<p class="muted">No supervisor notes recorded.</p>'

    rows = []
    for _, row in notes.head(100).iterrows():  # Limit to recent 100
        category = row.get("Category", "operational")
        color = NOTE_CATEGORY_COLORS.get(category, "#6b7280")
        date_str = row["Date"].strftime("%Y-%m-%d") if pd.notna(row["Date"]) else "N/A"
        machine = row.get("Machine_Name", "N/A")
        shift = row.get("Shift", "N/A")
        operator = row.get("Operator", "") or "—"
        note_text = row.get("Note", "")

        rows.append(f"""
        <div class="note-item" data-category="{category}" data-machine="{machine}">
            <div class="note-meta">
                <span class="note-date">{date_str}</span>
                <span class="note-badge" style="background:{color}">{category}</span>
                <span class="note-machine">{machine}</span>
                <span class="note-shift">{shift} shift</span>
                <span class="note-operator">{operator}</span>
            </div>
            <div class="note-text">{note_text}</div>
        </div>
        """)

    return f"""
    <div class="notes-controls">
        <input type="text" id="noteSearch" placeholder="Search notes..." class="note-search">
        <select id="noteCategoryFilter" class="note-filter">
            <option value="all">All Categories</option>
            <option value="downtime">Downtime</option>
            <option value="material">Material</option>
            <option value="quality">Quality</option>
            <option value="operational">Operational</option>
        </select>
    </div>
    <div class="notes-timeline" id="notesTimeline">
        {''.join(rows)}
    </div>
    """


def render_dashboard(
    summary_html: str,
    fig_sections: list[tuple[str, str, go.Figure]],
    notes_html: str,
) -> str:
    """Render the complete dashboard HTML."""
    sections_html = ""
    for title, div_id, fig in fig_sections:
        fig_html = to_html(fig, include_plotlyjs=False, full_html=False, div_id=div_id)
        sections_html += f'<section class="card"><h2>{title}</h2>{fig_html}</section>\n'

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
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        padding: 24px;
        font-family: Helvetica, Arial, sans-serif;
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
        margin: 0;
      }}
      .nav-link {{
        display: inline-block;
        margin-top: 12px;
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
      .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 12px;
        margin-bottom: 24px;
      }}
      .kpi-card {{
        background: #fff;
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        border: 1px solid var(--border);
      }}
      .kpi-label {{
        font-size: 12px;
        color: #6b7280;
        margin-bottom: 4px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
      }}
      .kpi-value {{
        font-size: 1.25rem;
        font-weight: 600;
        color: var(--primary);
      }}
      .card {{
        background: #fff;
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        border: 1px solid var(--border);
      }}
      .card h2 {{
        margin: 0 0 16px;
        font-size: 1.1rem;
        color: var(--primary);
      }}
      /* Notes styling */
      .notes-controls {{
        display: flex;
        gap: 12px;
        margin-bottom: 16px;
        flex-wrap: wrap;
      }}
      .note-search {{
        flex: 1;
        min-width: 200px;
        padding: 10px 14px;
        border: 1px solid var(--border);
        border-radius: 8px;
        font-size: 14px;
      }}
      .note-filter {{
        padding: 10px 14px;
        border: 1px solid var(--border);
        border-radius: 8px;
        font-size: 14px;
        background: white;
      }}
      .notes-timeline {{
        max-height: 500px;
        overflow-y: auto;
      }}
      .note-item {{
        padding: 12px;
        border-left: 4px solid var(--border);
        margin-bottom: 12px;
        background: #fafafa;
        border-radius: 0 8px 8px 0;
      }}
      .note-item[data-category="downtime"] {{
        border-left-color: #dc2626;
      }}
      .note-item[data-category="material"] {{
        border-left-color: #f59e0b;
      }}
      .note-item[data-category="quality"] {{
        border-left-color: #6366f1;
      }}
      .note-meta {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 8px;
        font-size: 12px;
      }}
      .note-date {{
        font-weight: 600;
        color: var(--primary);
      }}
      .note-badge {{
        padding: 2px 8px;
        border-radius: 4px;
        color: white;
        font-size: 11px;
        text-transform: uppercase;
      }}
      .note-machine, .note-shift, .note-operator {{
        color: #6b7280;
      }}
      .note-text {{
        font-size: 14px;
        line-height: 1.5;
      }}
      .note-item.hidden {{
        display: none;
      }}
      .muted {{
        color: #9ca3af;
        font-style: italic;
      }}
      /* Export buttons */
      .export-buttons {{
        display: flex;
        gap: 8px;
        margin-bottom: 20px;
      }}
      .export-btn {{
        padding: 8px 14px;
        border-radius: 8px;
        border: 1px solid var(--border);
        background: #fff;
        cursor: pointer;
        font-size: 13px;
        transition: background 0.2s;
      }}
      .export-btn:hover {{
        background: #f3f4f6;
      }}
      /* Mobile responsive */
      @media (max-width: 768px) {{
        body {{ padding: 12px; }}
        .kpi-grid {{
          grid-template-columns: repeat(2, 1fr);
          gap: 8px;
        }}
        .kpi-card {{ padding: 10px 8px; }}
        .kpi-value {{ font-size: 16px; }}
        h1 {{ font-size: 1.5rem; }}
        .card {{ padding: 12px; border-radius: 12px; }}
        .notes-controls {{ flex-direction: column; }}
        .note-search {{ min-width: 100%; }}
      }}
      @media (max-width: 480px) {{
        .kpi-grid {{ grid-template-columns: 1fr; }}
      }}
      @media print {{
        .export-buttons, .notes-controls {{ display: none; }}
        .card {{ break-inside: avoid; page-break-inside: avoid; }}
        body {{ background: white; padding: 0; }}
      }}
    </style>
  </head>
  <body>
    <header>
      <h1>Daily Processing Dashboard</h1>
      <p class="subtitle">Daily production data with running averages, operator performance, and supervisor notes.</p>
      <a href="index.html" class="nav-link">View Weekly Summary</a>
    </header>
    <main>
      <div class="export-buttons">
        <button class="export-btn" onclick="exportChart('fig-daily')">Export Chart PNG</button>
        <button class="export-btn" onclick="window.print()">Export PDF</button>
      </div>
      <section class="card">
        <h2>At a Glance</h2>
        {summary_html}
      </section>
      {sections_html}
      <section class="card">
        <h2>Supervisor Notes & Issues</h2>
        {notes_html}
      </section>
    </main>
    <script>
      function exportChart(divId) {{
        const graphDiv = document.getElementById(divId);
        if (graphDiv) {{
          Plotly.downloadImage(graphDiv, {{
            format: 'png',
            width: 1200,
            height: 800,
            filename: 'daily-dashboard-' + divId,
          }});
        }}
      }}

      // Notes filtering
      const noteSearch = document.getElementById('noteSearch');
      const noteCategoryFilter = document.getElementById('noteCategoryFilter');
      const notesTimeline = document.getElementById('notesTimeline');

      function filterNotes() {{
        const searchTerm = noteSearch.value.toLowerCase();
        const category = noteCategoryFilter.value;
        const notes = notesTimeline.querySelectorAll('.note-item');

        notes.forEach(note => {{
          const text = note.textContent.toLowerCase();
          const noteCategory = note.dataset.category;
          const matchesSearch = text.includes(searchTerm);
          const matchesCategory = category === 'all' || noteCategory === category;

          if (matchesSearch && matchesCategory) {{
            note.classList.remove('hidden');
          }} else {{
            note.classList.add('hidden');
          }}
        }});
      }}

      if (noteSearch) noteSearch.addEventListener('input', filterNotes);
      if (noteCategoryFilter) noteCategoryFilter.addEventListener('change', filterNotes);
    </script>
  </body>
</html>
    """


def main(daily_path: Path, notes_path: Path, output_path: Path) -> None:
    """Main entry point."""
    daily, notes = load_data(daily_path, notes_path)

    # Aggregate by date/machine for cleaner visualization
    agg = aggregate_by_date_machine(daily)
    agg = add_running_averages(
        agg,
        metrics=["Actual_Output", "Output_per_Hour", "Machine_Hours"],
        window=DEFAULT_RUNNING_AVG_WINDOW,
    )

    # Build components
    summary_html = build_summary_cards(daily, notes)
    notes_html = build_notes_timeline_html(notes)

    fig_sections = [
        ("Daily Output Timeline", "fig-daily", build_daily_output_fig(agg)),
        ("Day of Week Analysis", "fig-dow", build_day_of_week_heatmap(agg)),
        ("Operator Performance", "fig-operators", build_operator_performance_fig(daily)),
        ("Data Quality Over Time", "fig-quality", build_data_quality_fig(agg)),
    ]

    # Render and write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_dashboard(summary_html, fig_sections, notes_html))
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
