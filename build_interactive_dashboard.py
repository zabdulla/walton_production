"""
Generate an interactive HTML dashboard (Plotly) from aggregated processing data.

Features:
- Metric toggle (including 4-week running averages)
- Per-machine weekly trends with legend filtering & range slider
- Output-product stacked breakdown (all machines or per machine)
- Presentation polish (spacing, colors, hover, typography)

Usage:
    python build_interactive_dashboard.py \
        --input processing_reports/aggregated_master_data.xlsx \
        --output docs/index.html

The output HTML is self-contained and ready to host on GitHub Pages (e.g., via a
`docs/` folder).
"""

import argparse
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.io import to_html


DEFAULT_INPUT = Path("processing_reports/aggregated_master_data.xlsx")
DEFAULT_OUTPUT = Path("docs/index.html")
RUNNING_AVG_WINDOW = 4
COST_PER_POUND_THRESHOLD = 0.10  # Highlight cells exceeding this threshold

# Consolidated color palette for all charts
CHART_PALETTE = [
    "#0B6E4F",
    "#2CA58D",
    "#84BCDA",
    "#33658A",
    "#F26419",
    "#FFAF87",
    "#3A3042",
    "#5BC0BE",
    "#C5283D",
    "#1f77b4",
]

BASE_METRICS = {
    "Actual_Output": ("Actual Output (Lbs)", "int"),
    "Output_per_Hour": ("Output per Hour", "float1"),
    "Output_per_Man_Hour": ("Output per Man-Hour", "float1"),
    "Production_Cost_per_Pound": ("Production Cost per Pound", "currency4"),
    "Total_Machine_Hours": ("Total Machine Hours", "float1"),
    "Total_Man_Hours": ("Total Man Hours", "float1"),
    "Labor_Cost": ("Labor Cost", "currency"),
    "Total_Expense": ("Total Expense", "currency"),
}


def metric_option_labels() -> list[tuple[str, str, str]]:
    """Return metric options in display order: running avg first, then raw."""
    opts: list[tuple[str, str, str]] = []
    for key, (label, fmt_kind) in BASE_METRICS.items():
        opts.append((f"{key}_RA", f"{label} ({RUNNING_AVG_WINDOW}-week running avg)", fmt_kind))
    for key, (label, fmt_kind) in BASE_METRICS.items():
        opts.append((key, f"{label} (raw)", fmt_kind))
    return opts

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


def _calc_wow_change(current: float, previous: float) -> str:
    """Calculate week-over-week change indicator HTML."""
    if previous == 0 or pd.isna(previous) or pd.isna(current):
        return ""
    pct_change = ((current - previous) / previous) * 100
    if pct_change > 0:
        return f'<span class="trend-up">&#9650; {pct_change:+.1f}%</span>'
    elif pct_change < 0:
        return f'<span class="trend-down">&#9660; {pct_change:.1f}%</span>'
    return '<span class="trend-flat">&#9644; 0%</span>'


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    # Normalize date columns
    for col in ["Start Date", "End Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def aggregate_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per machine per week (start date) and recompute derived metrics."""
    grouped = (
        df.groupby(["Machine Name", "Start Date"])
        .agg(
            Actual_Output=("Actual Output (Lbs)", "sum"),
            Total_Machine_Hours=("Total Machine Hours", "sum"),
            Total_Man_Hours=("Total Man Hours", "sum"),
            Labor_Cost=("Labor Cost", "sum"),
            Total_Expense=("Total Expense", "sum"),
        )
        .reset_index()
        .rename(columns={"Start Date": "Week Start"})
    )

    # Derived metrics (avoid divide-by-zero)
    grouped["Output_per_Hour"] = grouped["Actual_Output"] / grouped["Total_Machine_Hours"].replace(0, pd.NA)
    grouped["Output_per_Man_Hour"] = grouped["Actual_Output"] / grouped["Total_Man_Hours"].replace(0, pd.NA)
    grouped["Production_Cost_per_Pound"] = grouped["Total_Expense"] / grouped["Actual_Output"].replace(0, pd.NA)

    grouped["Week Start"] = pd.to_datetime(grouped["Week Start"])
    grouped["Week Label"] = grouped["Week Start"].dt.strftime("%Y-%m-%d")
    numeric_cols = [
        "Actual_Output",
        "Total_Machine_Hours",
        "Total_Man_Hours",
        "Labor_Cost",
        "Total_Expense",
        "Output_per_Hour",
        "Output_per_Man_Hour",
        "Production_Cost_per_Pound",
    ]
    grouped[numeric_cols] = grouped[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return grouped


def add_running_averages(
    df: pd.DataFrame,
    metrics: list[str],
    window: int = RUNNING_AVG_WINDOW,
) -> pd.DataFrame:
    """Add running-average versions of each metric per machine."""
    df = df.sort_values(["Machine Name", "Week Start"]).copy()
    for col in metrics:
        ra_col = f"{col}_RA"
        numeric_series = pd.to_numeric(df[col], errors="coerce")
        df[ra_col] = (
            df.assign(_val=numeric_series)
            .groupby("Machine Name")["_val"]
            .transform(lambda s: s.rolling(window=window, min_periods=1).mean())
        )
    return df


def build_summary_cards(df_raw: pd.DataFrame, weekly: pd.DataFrame) -> dict[str, str]:
    """Return HTML for KPI cards per machine (and All Machines) with WoW trends."""
    machine_options = ["All Machines"] + sorted(df_raw["Machine Name"].unique())
    result = {}

    for machine in machine_options:
        scope_raw = df_raw if machine == "All Machines" else df_raw[df_raw["Machine Name"] == machine]
        scope_weekly = weekly if machine == "All Machines" else weekly[weekly["Machine Name"] == machine]
        if scope_raw.empty:
            result[machine] = "<p class='muted'>No data for this selection.</p>"
            continue

        start_date = pd.to_datetime(scope_raw["Start Date"]).min()
        end_date = pd.to_datetime(scope_raw["End Date"]).max()
        total_output = scope_raw["Actual Output (Lbs)"].sum()
        total_expense = scope_raw["Total Expense"].sum()
        avg_cost_per_lb = (scope_weekly["Production_Cost_per_Pound"].mean(skipna=True)) or 0
        weeks = scope_weekly["Week Start"].nunique()
        machines = scope_weekly["Machine Name"].nunique()

        # Calculate week-over-week changes for latest two weeks
        sorted_weeks = scope_weekly.sort_values("Week Start")
        latest_weeks = sorted_weeks.groupby("Week Start").agg({
            "Actual_Output": "sum",
            "Total_Expense": "sum",
        }).tail(2)

        output_trend = ""
        expense_trend = ""
        if len(latest_weeks) >= 2:
            prev_output, curr_output = latest_weeks["Actual_Output"].iloc[-2], latest_weeks["Actual_Output"].iloc[-1]
            prev_expense, curr_expense = latest_weeks["Total_Expense"].iloc[-2], latest_weeks["Total_Expense"].iloc[-1]
            output_trend = _calc_wow_change(curr_output, prev_output)
            expense_trend = _calc_wow_change(curr_expense, prev_expense)

        cards = [
            ("Date Range", f"{start_date:%b %d, %Y} – {end_date:%b %d, %Y}", ""),
            ("Total Output (Lbs)", _fmt_num(total_output, "int"), output_trend),
            ("Total Expense", _fmt_num(total_expense, "currency"), expense_trend),
            ("Avg Cost / Lb", _fmt_num(avg_cost_per_lb, "currency4"), ""),
            ("Weeks Covered", f"{weeks}", ""),
            ("Machines", f"{machines}", ""),
        ]

        cards_html = "".join(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">{label}</div>
              <div class="kpi-value">{value} {trend}</div>
            </div>
            """
            for label, value, trend in cards
        )
        result[machine] = f'<div class="kpi-grid summary-block" data-machine="{machine}">{cards_html}</div>'

    return result


def build_interactive_fig(df: pd.DataFrame) -> go.Figure:
    """Build the main metrics line chart with all machines and metrics."""
    metric_options = metric_option_labels()

    machines = sorted(df["Machine Name"].unique())

    traces = []
    for metric_key, label, fmt_kind in metric_options:
        for idx, machine in enumerate(machines):
            subset = df[df["Machine Name"] == machine]
            traces.append(
                go.Scatter(
                    x=subset["Week Start"],
                    y=subset[metric_key],
                    mode="lines+markers",
                    name=f"{machine}",
                    hovertemplate=(
                        "Machine: %{text}<br>"
                        "Week: %{customdata[0]}<br>"
                        f"{label}: %{{y:{',.2f' if fmt_kind.startswith('float') else '$,.2f' if fmt_kind.startswith('currency') else ',.2f'}}}<extra></extra>"
                    ),
                    text=subset["Machine Name"],
                    customdata=subset[["Week Label"]],
                    showlegend=True,
                    visible=False,
                    marker=dict(size=7, line=dict(width=1.5, color="white")),
                    line=dict(width=2, color=CHART_PALETTE[idx % len(CHART_PALETTE)]),
                    meta={"metric": metric_key, "machine": machine, "label": label},
                )
            )

    # Default visibility: running average of the first metric, all machines
    default_metric = metric_options[0][0]
    for trace in traces:
        if trace.meta["metric"] == default_metric:
            trace.visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=f"{metric_options[0][1]} by Machine and Week",
        yaxis_title=metric_options[0][1],
        xaxis_title="Week Start",
        hovermode="x unified",
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=220, b=80, l=70),
        legend=dict(title="Machine", orientation="v", x=1.08, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
    )

    fig.update_xaxes(rangeslider=dict(visible=True), showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


def build_output_product_fig(df: pd.DataFrame) -> go.Figure:
    """Stacked breakdown of output products per week, with machine filter."""
    df = df.copy()
    df["Start Date"] = pd.to_datetime(df["Start Date"])
    products = sorted(df["Output Product"].dropna().unique())
    machine_options = ["All Machines"] + sorted(df["Machine Name"].unique())

    traces = []
    for option_idx, machine in enumerate(machine_options):
        scope = df if machine == "All Machines" else df[df["Machine Name"] == machine]
        grouped = (
            scope.groupby(["Start Date", "Output Product"])["Actual Output (Lbs)"]
            .sum()
            .reset_index()
            .rename(columns={"Start Date": "Week Start"})
        )
        grouped["Week Label"] = grouped["Week Start"].dt.strftime("%Y-%m-%d")

        for product_idx, product in enumerate(products):
            subset = grouped[grouped["Output Product"] == product]
            traces.append(
                go.Bar(
                    x=subset["Week Start"],
                    y=subset["Actual Output (Lbs)"],
                    name=product,
                    hovertemplate=(
                        f"Machine: {machine}<br>"
                        "Week: %{customdata[0]}<br>"
                        "Output Product: %{text}<br>"
                        "Lbs: %{y:,.0f}<extra></extra>"
                    ),
                    text=subset["Output Product"],
                    customdata=subset[["Week Label"]],
                    visible=False,
                    marker_color=CHART_PALETTE[product_idx % len(CHART_PALETTE)],
                    meta={"machine": machine},
                )
            )

    # default visibility: All Machines
    for trace in traces:
        if trace.meta["machine"] == "All Machines":
            trace.visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title="Output Product Breakdown — All Machines",
        barmode="stack",
        xaxis_title="Week Start",
        yaxis_title="Actual Output (Lbs)",
        hovermode="x unified",
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=220, b=80, l=70),
        legend=dict(title="Output Product", orientation="v", x=1.08, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
    )
    fig.update_xaxes(rangeslider=dict(visible=True), showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


def build_utilization_heatmap(weekly: pd.DataFrame) -> go.Figure:
    """Build a heatmap showing machine utilization (hours) by week."""
    pivot = weekly.pivot_table(
        index="Machine Name",
        columns="Week Label",
        values="Total_Machine_Hours",
        aggfunc="sum",
        fill_value=0,
    )

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale="Blues",
        hovertemplate="Machine: %{y}<br>Week: %{x}<br>Hours: %{z:.1f}<extra></extra>",
        colorbar=dict(title="Machine Hours"),
    ))

    fig.update_layout(
        title="Machine Utilization Heatmap",
        xaxis_title="Week",
        yaxis_title="Machine",
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=100, l=180),
        xaxis=dict(tickangle=45),
    )
    return fig


def build_pareto_chart(weekly: pd.DataFrame) -> go.Figure:
    """Build Pareto chart showing cumulative output contribution by machine."""
    machine_totals = (
        weekly.groupby("Machine Name")["Actual_Output"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    machine_totals["Cumulative"] = machine_totals["Actual_Output"].cumsum()
    machine_totals["Cumulative_Pct"] = (
        machine_totals["Cumulative"] / machine_totals["Actual_Output"].sum() * 100
    )

    fig = go.Figure()

    # Bars for output
    fig.add_trace(go.Bar(
        x=machine_totals["Machine Name"],
        y=machine_totals["Actual_Output"],
        name="Output (Lbs)",
        marker_color="#3b82f6",
        hovertemplate="Machine: %{x}<br>Output: %{y:,.0f} lbs<extra></extra>",
    ))

    # Line for cumulative percentage
    fig.add_trace(go.Scatter(
        x=machine_totals["Machine Name"],
        y=machine_totals["Cumulative_Pct"],
        name="Cumulative %",
        mode="lines+markers",
        yaxis="y2",
        line=dict(color="#ef4444", width=2),
        marker=dict(size=8),
        hovertemplate="Cumulative: %{y:.1f}%<extra></extra>",
    ))

    # 80% threshold line
    fig.add_hline(
        y=80, line_dash="dash", line_color="#9ca3af",
        annotation_text="80% threshold", yref="y2",
    )

    fig.update_layout(
        title="Pareto Analysis: Machine Output Contribution",
        xaxis_title="Machine",
        yaxis=dict(title="Output (Lbs)", side="left"),
        yaxis2=dict(title="Cumulative %", side="right", overlaying="y", range=[0, 105]),
        template="plotly_white",
        plot_bgcolor="#f9fafc",
        paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        legend=dict(x=0.7, y=1.1, orientation="h"),
        margin=dict(t=100, b=100),
    )
    return fig


def build_latest_week_table_html(
    weekly: pd.DataFrame,
    cost_threshold: float = COST_PER_POUND_THRESHOLD,
) -> dict[str, str]:
    """Return HTML tables for the latest week per machine (and All) with conditional formatting."""
    latest_week = weekly["Week Start"].max()
    week_label = latest_week.strftime("%Y-%m-%d")

    tables = {}
    machine_options = ["All Machines"] + sorted(weekly["Machine Name"].unique())
    for machine in machine_options:
        scope = weekly[weekly["Week Start"] == latest_week]
        if machine != "All Machines":
            scope = scope[scope["Machine Name"] == machine]
        scope = scope.copy().sort_values("Actual_Output", ascending=False)
        if scope.empty:
            tables[machine] = "<p class='muted'>No data for latest week.</p>"
            continue
        rows = []
        for _, row in scope.iterrows():
            cost_per_lb = row['Production_Cost_per_Pound']
            cost_class = ' class="highlight-warning"' if cost_per_lb > cost_threshold else ""
            rows.append(
                f"""
                <tr>
                  <td>{row['Machine Name']}</td>
                  <td>{week_label}</td>
                  <td>{_fmt_num(row['Actual_Output'], 'int')}</td>
                  <td>{_fmt_num(row['Output_per_Hour'], 'float1')}</td>
                  <td{cost_class}>{_fmt_num(cost_per_lb, 'currency4')}</td>
                  <td>{_fmt_num(row['Labor_Cost'], 'currency')}</td>
                  <td>{_fmt_num(row['Total_Expense'], 'currency')}</td>
                </tr>
                """
            )
        tables[machine] = f"""
        <div class="table-wrap summary-block" data-machine="{machine}">
          <table>
            <thead>
              <tr>
                <th>Machine</th>
                <th>Week Start</th>
                <th>Actual Output (Lbs)</th>
                <th>Output / Hour</th>
                <th>Cost / Lb</th>
                <th>Labor Cost</th>
                <th>Total Expense</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows)}
            </tbody>
          </table>
        </div>
        """
    return tables


def render_dashboard(
    summary_html: str,
    fig_sections: list[tuple[str, str, go.Figure]],
    options_html: str,
    metric_options_html: str,
    tables_html: str,
) -> str:
    """Render Plotly figures and summary HTML into a single, styled page."""
    rendered = [
        (
            title,
            to_html(
                fig,
                include_plotlyjs=False,
                full_html=False,
                default_width="100%",
                default_height="650px",
                div_id=fig_id,
            ),
        )
        for title, fig_id, fig in fig_sections
    ]

    sections_html = "\n".join(
        f"""
      <section class="card">
        <h2 style="margin-top:0">{title}</h2>
        {html}
      </section>
        """
        for title, html in rendered
    )

    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Processing Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.35.3.min.js"></script>
    <style>
      :root {{
        --bg: #f3f4f6;
        --card: #ffffff;
        --text: #111827;
        --muted: #6b7280;
        --border: #e5e7eb;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        padding: 24px;
        font-family: "Helvetica Neue", Arial, sans-serif;
        background: radial-gradient(circle at 20% 20%, #f9fafb 0, #eef2ff 40%, #f3f4f6 90%);
        color: var(--text);
      }}
      h1 {{
        margin: 0 0 8px 0;
        font-weight: 700;
        letter-spacing: -0.01em;
      }}
      p.subtitle {{
        margin: 0 0 24px 0;
        color: var(--muted);
      }}
      .card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 16px;
        box-shadow: 0 10px 50px rgba(15, 23, 42, 0.08);
        padding: 20px;
        margin-bottom: 20px;
      }}
      .kpi-grid {{
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        margin: 12px 0 8px;
      }}
      .kpi-card {{
        background: #f8fafc;
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 12px 12px 10px;
      }}
      .kpi-label {{
        color: var(--muted);
        font-size: 12px;
        letter-spacing: 0.01em;
      }}
      .kpi-value {{
        font-size: 20px;
        font-weight: 700;
        margin-top: 4px;
      }}
      .controls {{
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin-bottom: 12px;
      }}
      .controls label {{
        font-weight: 600;
        color: var(--muted);
        margin-right: 6px;
      }}
      select {{
        padding: 8px 10px;
        border-radius: 8px;
        border: 1px solid var(--border);
        background: #fff;
        min-width: 180px;
      }}
      .muted {{ color: var(--muted); }}
      .table-wrap {{
        overflow-x: auto;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        text-align: left;
        padding: 8px 10px;
        border-bottom: 1px solid var(--border);
      }}
      th {{
        background: #111827;
        color: white;
      }}
      /* Trend indicators */
      .trend-up {{ color: #059669; font-size: 12px; margin-left: 6px; }}
      .trend-down {{ color: #dc2626; font-size: 12px; margin-left: 6px; }}
      .trend-flat {{ color: #6b7280; font-size: 12px; margin-left: 6px; }}
      /* Conditional formatting */
      .highlight-warning {{
        background: #fef3c7;
        color: #92400e;
        font-weight: 600;
      }}
      /* Export buttons */
      .export-buttons {{
        display: flex;
        gap: 8px;
        margin-left: auto;
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
      /* Mobile responsiveness */
      @media (max-width: 768px) {{
        body {{ padding: 12px; }}
        .kpi-grid {{
          grid-template-columns: repeat(2, 1fr);
          gap: 8px;
        }}
        .kpi-card {{ padding: 10px 8px; }}
        .kpi-value {{ font-size: 16px; }}
        .controls {{
          flex-direction: column;
          gap: 8px;
        }}
        .export-buttons {{
          margin-left: 0;
          width: 100%;
        }}
        select {{ width: 100%; min-width: unset; }}
        h1 {{ font-size: 1.5rem; }}
        .card {{ padding: 12px; border-radius: 12px; }}
        .js-plotly-plot {{
          overflow-x: auto;
          -webkit-overflow-scrolling: touch;
        }}
      }}
      @media (max-width: 480px) {{
        .kpi-grid {{ grid-template-columns: 1fr; }}
        table {{ font-size: 12px; }}
        th, td {{ padding: 6px 4px; }}
      }}
      /* Print styles */
      @media print {{
        .controls, .export-buttons {{ display: none; }}
        .card {{ break-inside: avoid; page-break-inside: avoid; }}
        body {{ background: white; padding: 0; }}
      }}
    </style>
  </head>
  <body>
    <header>
      <h1>Processing Performance Dashboard</h1>
      <p class="subtitle">Toggle metrics, running averages, and output product splits. Drag the range slider to zoom.</p>
    </header>
    <main>
      <div class="controls">
        <div>
          <label for="machineSelect">Machine:</label>
          <select id="machineSelect">
            {options_html}
          </select>
        </div>
        <div>
          <label for="metricSelect">Metric:</label>
          <select id="metricSelect">
            {metric_options_html}
          </select>
        </div>
        <div class="export-buttons">
          <button class="export-btn" onclick="exportChart('fig-metrics')">Export Chart PNG</button>
          <button class="export-btn" onclick="window.print()">Export PDF</button>
        </div>
      </div>
      <section class="card">
        <h2 style="margin-top:0">At a Glance</h2>
        {summary_html}
      </section>
      {sections_html}
      <section class="card">
        <h2 style="margin-top:0">Latest Week Snapshot</h2>
        {tables_html}
      </section>
    </main>
    <script>
      const summaryBlocks = document.querySelectorAll('.summary-block');
      const tableBlocks = document.querySelectorAll('.table-wrap.summary-block');
      const machineSelect = document.getElementById('machineSelect');
      const metricSelect = document.getElementById('metricSelect');
      const metricsFig = document.getElementById('fig-metrics');
      const productFig = document.getElementById('fig-products');

      function updateMachineVisibility(selectedMachine) {{
        summaryBlocks.forEach(el => {{
          el.style.display = (selectedMachine === el.dataset.machine) ? 'grid' : 'none';
        }});
        tableBlocks.forEach(el => {{
          el.style.display = (selectedMachine === el.dataset.machine) ? 'block' : 'none';
        }});
      }}

      function updatePlots() {{
        const selectedMachine = machineSelect.value;
        const selectedMetric = metricSelect.value;

        if (metricsFig && metricsFig.data) {{
          const vis = metricsFig.data.map(tr => {{
            const metricMatch = tr.meta && tr.meta.metric === selectedMetric;
            const machineMatch = tr.meta && (selectedMachine === 'All Machines' || tr.meta.machine === selectedMachine);
            return metricMatch && machineMatch;
          }});
          Plotly.restyle(metricsFig, 'visible', vis);
          const label = metricsFig.data.find((tr, idx) => vis[idx])?.meta?.label || selectedMetric;
          Plotly.relayout(metricsFig, {{title: `${{label}} by Machine and Week`, yaxis: {{title: label}}}});
        }}

        if (productFig && productFig.data) {{
          const vis = productFig.data.map(tr => {{
            const machineMatch = tr.meta && (selectedMachine === 'All Machines' ? tr.meta.machine === 'All Machines' : tr.meta.machine === selectedMachine);
            return machineMatch;
          }});
          Plotly.restyle(productFig, 'visible', vis);
          Plotly.relayout(productFig, {{title: `Output Product Breakdown — ${{selectedMachine}}`}});
        }}

        updateMachineVisibility(selectedMachine);
      }}

      function exportChart(divId) {{
        const graphDiv = document.getElementById(divId);
        if (graphDiv) {{
          Plotly.downloadImage(graphDiv, {{
            format: 'png',
            width: 1200,
            height: 800,
            filename: 'processing-dashboard-' + divId,
          }});
        }}
      }}

      machineSelect.addEventListener('change', updatePlots);
      metricSelect.addEventListener('change', updatePlots);
      updatePlots();
    </script>
  </body>
</html>
    """


def main(input_path: Path, output_path: Path) -> None:
    """Main entry point: load data, build charts, and generate dashboard HTML."""
    df = load_data(input_path)
    # Filter out rows with missing/zero hours to avoid unlogged skew.
    df = df[(df["Total Man Hours"] > 0) & (df["Total Machine Hours"] > 0)]

    weekly = aggregate_weekly(df)
    weekly = add_running_averages(
        weekly,
        metrics=[
            "Actual_Output",
            "Output_per_Hour",
            "Output_per_Man_Hour",
            "Production_Cost_per_Pound",
            "Total_Machine_Hours",
            "Total_Man_Hours",
            "Labor_Cost",
            "Total_Expense",
        ],
        window=RUNNING_AVG_WINDOW,
    )

    summary_blocks = build_summary_cards(df, weekly)
    table_blocks = build_latest_week_table_html(weekly)

    machine_options = ["All Machines"] + sorted(df["Machine Name"].unique())
    options_html = "\n".join(f'<option value="{m}">{m}</option>' for m in machine_options)

    metric_options_labels = metric_option_labels()
    metric_options_html = "\n".join(
        f'<option value="{val}" {"selected" if idx == 0 else ""}>{label}</option>'
        for idx, (val, label, _) in enumerate(metric_options_labels)
    )

    fig_sections = [
        ("Weekly Metrics by Machine", "fig-metrics", build_interactive_fig(weekly)),
        ("Machine Utilization Heatmap", "fig-heatmap", build_utilization_heatmap(weekly)),
        ("Pareto Analysis: Output Contribution", "fig-pareto", build_pareto_chart(weekly)),
        ("Output Product Breakdown", "fig-products", build_output_product_fig(df)),
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_dashboard(
            "\n".join(summary_blocks.values()),
            fig_sections,
            options_html,
            metric_options_html,
            "\n".join(table_blocks.values()),
        ),
        encoding="utf-8",
    )

    print(f"Wrote interactive dashboard to {output_path} (open in a browser or host via GitHub Pages).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build interactive processing dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to aggregated_master_data.xlsx")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Path to write HTML dashboard")
    args = parser.parse_args()
    main(args.input, args.output)
