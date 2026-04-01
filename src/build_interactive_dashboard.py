"""
Generate an interactive HTML dashboard (Plotly) from aggregated processing data.

Designed for periodic viewers — defaults to the last 20 weeks with clear trend
summaries, simplified metric selection, and month-over-month context.

Usage:
    python src/build_interactive_dashboard.py \
        --input data/aggregated_daily_data.xlsx \
        --output docs/index.html

The output HTML is self-contained and ready to host on GitHub Pages.
"""

import argparse
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.io import to_html


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = _PROJECT_ROOT / "data" / "aggregated_daily_data.xlsx"
DEFAULT_OUTPUT = _PROJECT_ROOT / "docs" / "index.html"
DEFAULT_WEEKS = 20
RUNNING_AVG_WINDOW = 4
COST_PER_POUND_THRESHOLD = 0.10

CHART_PALETTE = [
    "#0B6E4F", "#2CA58D", "#84BCDA", "#33658A", "#F26419",
    "#FFAF87", "#3A3042", "#5BC0BE", "#C5283D", "#1f77b4",
]

# Key metrics shown by default (running average). Full list available via toggle.
KEY_METRICS = {
    "Actual_Output": ("Actual Output (Lbs)", "int"),
    "Output_per_Hour": ("Output per Hour", "float1"),
    "Production_Cost_per_Pound": ("Production Cost per Pound", "currency4"),
    "Total_Expense": ("Total Expense", "currency"),
}

ALL_METRICS = {
    "Actual_Output": ("Actual Output (Lbs)", "int"),
    "Output_per_Hour": ("Output per Hour", "float1"),
    "Output_per_Man_Hour": ("Output per Man-Hour", "float1"),
    "Production_Cost_per_Pound": ("Production Cost per Pound", "currency4"),
    "Total_Machine_Hours": ("Total Machine Hours", "float1"),
    "Total_Man_Hours": ("Total Man Hours", "float1"),
    "Labor_Cost": ("Labor Cost", "currency"),
    "Total_Expense": ("Total Expense", "currency"),
}


def _fmt_num(value: Any, kind: str = "int") -> str:
    if pd.isna(value):
        return "\u2014"
    if kind == "currency":
        return f"${value:,.0f}"
    if kind == "currency4":
        return f"${value:,.4f}"
    if kind == "float1":
        return f"{value:,.1f}"
    if kind == "float2":
        return f"{value:,.2f}"
    return f"{value:,.0f}"


def _pct_change_html(current: float, previous: float) -> str:
    if previous == 0 or pd.isna(previous) or pd.isna(current):
        return ""
    pct = ((current - previous) / previous) * 100
    if pct > 0:
        return f'<span class="trend-up">&#9650; {pct:+.1f}%</span>'
    if pct < 0:
        return f'<span class="trend-down">&#9660; {pct:.1f}%</span>'
    return '<span class="trend-flat">&#9644; 0%</span>'


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    if "Week_Start" in df.columns and "Start Date" not in df.columns:
        df = df.rename(columns={
            "Week_Start": "Start Date",
            "Week_End": "End Date",
            "Machine_Name": "Machine Name",
            "Actual_Output": "Actual Output (Lbs)",
            "Actual_Input": "Actual Input (Lbs)",
            "Machine_Hours": "Total Machine Hours",
            "Man_Hours": "Total Man Hours",
            "Output_Product": "Output Product",
            "Output_per_Hour": "Output per Hour",
            "Labor_Cost": "Labor Cost",
            "Total_Expense": "Total Expense",
            "Cost_per_Pound": "Production Cost per Pound",
        })
    for col in ["Start Date", "End Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def _apply_guillotine_support(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy where Guillotine rows with output=0 use input as output."""
    df = df.copy()
    mask = (
        df["Machine Name"].str.contains("GUILLOTINE", case=False, na=False)
        & (df["Actual Output (Lbs)"] == 0)
        & (df["Actual Input (Lbs)"] > 0)
    )
    df.loc[mask, "Actual Output (Lbs)"] = df.loc[mask, "Actual Input (Lbs)"]
    return df


def aggregate_weekly(df: pd.DataFrame) -> pd.DataFrame:
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
    grouped["Output_per_Hour"] = grouped["Actual_Output"] / grouped["Total_Machine_Hours"].replace(0, pd.NA)
    grouped["Output_per_Man_Hour"] = grouped["Actual_Output"] / grouped["Total_Man_Hours"].replace(0, pd.NA)
    grouped["Production_Cost_per_Pound"] = grouped["Total_Expense"] / grouped["Actual_Output"].replace(0, pd.NA)
    grouped["Week Start"] = pd.to_datetime(grouped["Week Start"])
    grouped["Week Label"] = grouped["Week Start"].dt.strftime("%Y-%m-%d")
    numeric_cols = [
        "Actual_Output", "Total_Machine_Hours", "Total_Man_Hours",
        "Labor_Cost", "Total_Expense", "Output_per_Hour",
        "Output_per_Man_Hour", "Production_Cost_per_Pound",
    ]
    grouped[numeric_cols] = grouped[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return grouped


def add_running_averages(df: pd.DataFrame, metrics: list, window: int = RUNNING_AVG_WINDOW) -> pd.DataFrame:
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


def _recent_weeks(weekly: pd.DataFrame, n: int) -> pd.DataFrame:
    cutoff_weeks = sorted(weekly["Week Start"].unique())[-n:]
    return weekly[weekly["Week Start"].isin(cutoff_weeks)]


# ---------------------------------------------------------------------------
# Recent Trends summary (replaces old all-time KPI cards)
# ---------------------------------------------------------------------------

def build_recent_trends_html(weekly: pd.DataFrame) -> str:
    """Build a 'Recent Trends' section: this month vs last month + mini sparkline data."""
    weekly = weekly.copy()
    weekly["Month"] = weekly["Week Start"].dt.to_period("M")
    months = sorted(weekly["Month"].unique())
    if len(months) < 2:
        return "<p class='muted'>Not enough data for trend comparison.</p>"

    curr_month = months[-1]
    prev_month = months[-2]
    curr = weekly[weekly["Month"] == curr_month]
    prev = weekly[weekly["Month"] == prev_month]

    def _agg(df):
        return {
            "output": df["Actual_Output"].sum(),
            "expense": df["Total_Expense"].sum(),
            "cost_per_lb": df["Total_Expense"].sum() / max(df["Actual_Output"].sum(), 1),
            "hours": df["Total_Machine_Hours"].sum(),
        }

    c, p = _agg(curr), _agg(prev)

    # Sparkline: last 12 weeks of total output
    last_12 = _recent_weeks(weekly, 12)
    spark_data = (
        last_12.groupby("Week Start")["Actual_Output"].sum()
        .sort_index()
        .tolist()
    )
    spark_max = max(spark_data) if spark_data else 1
    spark_points = []
    bar_width = 100 / max(len(spark_data), 1)
    for i, val in enumerate(spark_data):
        h = max(val / spark_max * 40, 2)
        x = i * bar_width
        spark_points.append(f'<rect x="{x:.1f}%" y="{40 - h:.1f}" width="{bar_width * 0.7:.1f}%" height="{h:.1f}" rx="2" fill="#3b82f6" opacity="0.7"/>')
    sparkline_svg = f'<svg viewBox="0 0 200 40" style="width:100%;height:40px;display:block;">{"".join(spark_points)}</svg>'

    cards = [
        ("Total Output", _fmt_num(c["output"]), _pct_change_html(c["output"], p["output"])),
        ("Total Expense", _fmt_num(c["expense"], "currency"), _pct_change_html(c["expense"], p["expense"])),
        ("Cost / Lb", _fmt_num(c["cost_per_lb"], "currency4"), _pct_change_html(c["cost_per_lb"], p["cost_per_lb"])),
        ("Machine Hours", _fmt_num(c["hours"], "float1"), _pct_change_html(c["hours"], p["hours"])),
    ]

    cards_html = "".join(
        f'<div class="kpi-card"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value} {trend}</div></div>'
        for label, value, trend in cards
    )

    return f"""
    <div style="margin-bottom:8px;color:var(--muted);font-size:13px;">
        {curr_month.strftime('%B %Y')} vs {prev_month.strftime('%B %Y')}
    </div>
    <div class="kpi-grid">{cards_html}</div>
    <div style="margin-top:16px;">
        <div style="font-size:12px;color:var(--muted);margin-bottom:4px;">Weekly Output — Last 12 Weeks</div>
        {sparkline_svg}
    </div>
    """


# ---------------------------------------------------------------------------
# Monthly summary table
# ---------------------------------------------------------------------------

def build_monthly_summary_html(weekly: pd.DataFrame) -> str:
    """Month-over-month summary table for the last 6 months."""
    weekly = weekly.copy()
    weekly["Month"] = weekly["Week Start"].dt.to_period("M")
    months = sorted(weekly["Month"].unique())[-6:]

    rows = []
    prev_output = None
    prev_expense = None
    for month in months:
        m = weekly[weekly["Month"] == month]
        output = m["Actual_Output"].sum()
        expense = m["Total_Expense"].sum()
        hours = m["Total_Machine_Hours"].sum()
        cost_lb = expense / max(output, 1)
        output_trend = _pct_change_html(output, prev_output) if prev_output is not None else ""
        expense_trend = _pct_change_html(expense, prev_expense) if prev_expense is not None else ""
        rows.append(f"""<tr>
            <td>{month.strftime('%b %Y')}</td>
            <td>{_fmt_num(output)} {output_trend}</td>
            <td>{_fmt_num(expense, 'currency')} {expense_trend}</td>
            <td>{_fmt_num(cost_lb, 'currency4')}</td>
            <td>{_fmt_num(hours, 'float1')}</td>
        </tr>""")
        prev_output, prev_expense = output, expense

    return f"""
    <div class="table-wrap">
      <table>
        <thead><tr>
            <th>Month</th><th>Output (Lbs)</th><th>Expense</th><th>Cost / Lb</th><th>Machine Hrs</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>
    """


# ---------------------------------------------------------------------------
# Latest week table with 4-week average comparison
# ---------------------------------------------------------------------------

def build_latest_week_table_html(weekly: pd.DataFrame, cost_threshold: float = COST_PER_POUND_THRESHOLD) -> str:
    latest_week = weekly["Week Start"].max()
    last_4_weeks = sorted(weekly["Week Start"].unique())[-4:]
    avg_4 = weekly[weekly["Week Start"].isin(last_4_weeks)]

    scope = weekly[weekly["Week Start"] == latest_week].copy().sort_values("Actual_Output", ascending=False)
    if scope.empty:
        return "<p class='muted'>No data for latest week.</p>"

    # 4-week averages per machine
    avg_by_machine = avg_4.groupby("Machine Name").agg(
        Avg_Output=("Actual_Output", "mean"),
        Avg_OPH=("Output_per_Hour", "mean"),
        Avg_Cost=("Production_Cost_per_Pound", "mean"),
    )

    rows = []
    for _, row in scope.iterrows():
        machine = row["Machine Name"]
        output = row["Actual_Output"]
        oph = row["Output_per_Hour"]
        cost_lb = row["Production_Cost_per_Pound"]

        avg_row = avg_by_machine.loc[machine] if machine in avg_by_machine.index else None
        if avg_row is not None and not pd.isna(avg_row["Avg_Output"]) and avg_row["Avg_Output"] > 0:
            vs_avg = ((output - avg_row["Avg_Output"]) / avg_row["Avg_Output"]) * 100
            vs_avg_html = f'<span class="{"trend-up" if vs_avg >= 0 else "trend-down"}">{vs_avg:+.0f}%</span>'
        else:
            vs_avg_html = ""

        cost_class = ' class="highlight-warning"' if not pd.isna(cost_lb) and cost_lb > cost_threshold else ""
        rows.append(f"""<tr>
            <td>{machine}</td>
            <td>{_fmt_num(output)}</td>
            <td>{_fmt_num(avg_row['Avg_Output'] if avg_row is not None else None)}</td>
            <td>{vs_avg_html}</td>
            <td>{_fmt_num(oph, 'float1')}</td>
            <td{cost_class}>{_fmt_num(cost_lb, 'currency4')}</td>
        </tr>""")

    week_label = latest_week.strftime("%b %d, %Y")
    return f"""
    <div class="table-wrap">
      <p style="color:var(--muted);font-size:13px;margin:0 0 8px;">Week of {week_label}</p>
      <table>
        <thead><tr>
            <th>Machine</th><th>Output (Lbs)</th><th>4-Wk Avg</th><th>vs Avg</th><th>Output/Hr</th><th>Cost/Lb</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>
    """


# ---------------------------------------------------------------------------
# Plotly charts — all accept a recent-only dataframe
# ---------------------------------------------------------------------------

def build_interactive_fig(df: pd.DataFrame) -> go.Figure:
    """Main metrics line chart. Only running averages of key metrics shown by default."""
    machines = sorted(df["Machine Name"].unique())

    # Build traces: running avg of key metrics (default visible) + all raw (hidden by default)
    traces = []

    # Running averages of key metrics
    for key, (label, fmt_kind) in KEY_METRICS.items():
        ra_key = f"{key}_RA"
        for idx, machine in enumerate(machines):
            subset = df[df["Machine Name"] == machine]
            fmt_str = ',.2f' if fmt_kind.startswith('float') else '$,.2f' if fmt_kind.startswith('currency') else ',.0f'
            traces.append(go.Scatter(
                x=subset["Week Start"], y=subset[ra_key],
                mode="lines+markers", name=machine,
                hovertemplate=f"Machine: %{{text}}<br>Week: %{{customdata[0]}}<br>{label}: %{{y:{fmt_str}}}<extra></extra>",
                text=subset["Machine Name"], customdata=subset[["Week Label"]],
                visible=False,
                marker=dict(size=6, line=dict(width=1, color="white")),
                line=dict(width=2, color=CHART_PALETTE[idx % len(CHART_PALETTE)]),
                meta={"metric": ra_key, "machine": machine, "label": f"{label} ({RUNNING_AVG_WINDOW}-wk avg)", "group": "key_ra"},
            ))

    # Running averages of all metrics (includes key ones again, toggled by "Show all metrics")
    for key, (label, fmt_kind) in ALL_METRICS.items():
        if key in KEY_METRICS:
            continue
        ra_key = f"{key}_RA"
        for idx, machine in enumerate(machines):
            subset = df[df["Machine Name"] == machine]
            fmt_str = ',.2f' if fmt_kind.startswith('float') else '$,.2f' if fmt_kind.startswith('currency') else ',.0f'
            traces.append(go.Scatter(
                x=subset["Week Start"], y=subset[ra_key],
                mode="lines+markers", name=machine,
                hovertemplate=f"Machine: %{{text}}<br>Week: %{{customdata[0]}}<br>{label}: %{{y:{fmt_str}}}<extra></extra>",
                text=subset["Machine Name"], customdata=subset[["Week Label"]],
                visible=False,
                marker=dict(size=6, line=dict(width=1, color="white")),
                line=dict(width=2, color=CHART_PALETTE[idx % len(CHART_PALETTE)]),
                meta={"metric": ra_key, "machine": machine, "label": f"{label} ({RUNNING_AVG_WINDOW}-wk avg)", "group": "extra_ra"},
            ))

    # Raw values of all metrics
    for key, (label, fmt_kind) in ALL_METRICS.items():
        for idx, machine in enumerate(machines):
            subset = df[df["Machine Name"] == machine]
            fmt_str = ',.2f' if fmt_kind.startswith('float') else '$,.2f' if fmt_kind.startswith('currency') else ',.0f'
            traces.append(go.Scatter(
                x=subset["Week Start"], y=subset[key],
                mode="lines+markers", name=machine,
                hovertemplate=f"Machine: %{{text}}<br>Week: %{{customdata[0]}}<br>{label}: %{{y:{fmt_str}}}<extra></extra>",
                text=subset["Machine Name"], customdata=subset[["Week Label"]],
                visible=False,
                marker=dict(size=6, line=dict(width=1, color="white")),
                line=dict(width=2, color=CHART_PALETTE[idx % len(CHART_PALETTE)]),
                meta={"metric": key, "machine": machine, "label": f"{label} (raw)", "group": "raw"},
            ))

    # Default: first key metric RA, all machines
    first_metric = f"{list(KEY_METRICS.keys())[0]}_RA"
    for trace in traces:
        if trace.meta["metric"] == first_metric:
            trace.visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=f"{list(KEY_METRICS.values())[0][0]} ({RUNNING_AVG_WINDOW}-wk avg) by Machine",
        yaxis_title=list(KEY_METRICS.values())[0][0],
        xaxis_title="Week",
        hovermode="x unified",
        template="plotly_white",
        plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=220, b=60, l=70),
        legend=dict(title="Machine", orientation="v", x=1.08, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


def build_output_product_fig(df: pd.DataFrame) -> go.Figure:
    df = df.copy()
    df["Start Date"] = pd.to_datetime(df["Start Date"])
    products = sorted(df["Output Product"].dropna().unique())
    machine_options = ["All Machines"] + sorted(df["Machine Name"].unique())

    traces = []
    for machine in machine_options:
        scope = df if machine == "All Machines" else df[df["Machine Name"] == machine]
        grouped = (
            scope.groupby(["Start Date", "Output Product"])["Actual Output (Lbs)"]
            .sum().reset_index()
            .rename(columns={"Start Date": "Week Start"})
        )
        grouped["Week Label"] = grouped["Week Start"].dt.strftime("%Y-%m-%d")
        for pidx, product in enumerate(products):
            subset = grouped[grouped["Output Product"] == product]
            traces.append(go.Bar(
                x=subset["Week Start"], y=subset["Actual Output (Lbs)"],
                name=product,
                hovertemplate=f"Machine: {machine}<br>Week: %{{customdata[0]}}<br>{product}: %{{y:,.0f}} lbs<extra></extra>",
                text=subset["Output Product"], customdata=subset[["Week Label"]],
                visible=False,
                marker_color=CHART_PALETTE[pidx % len(CHART_PALETTE)],
                meta={"machine": machine},
            ))

    for trace in traces:
        if trace.meta["machine"] == "All Machines":
            trace.visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title="Output Product Breakdown \u2014 All Machines",
        barmode="stack", xaxis_title="Week", yaxis_title="Output (Lbs)",
        hovermode="x unified", template="plotly_white",
        plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=220, b=60, l=70),
        legend=dict(title="Product", orientation="v", x=1.08, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


def build_utilization_heatmap(weekly: pd.DataFrame) -> go.Figure:
    pivot = weekly.pivot_table(
        index="Machine Name", columns="Week Label",
        values="Total_Machine_Hours", aggfunc="sum", fill_value=0,
    )
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
        colorscale="Blues",
        hovertemplate="Machine: %{y}<br>Week: %{x}<br>Hours: %{z:.1f}<extra></extra>",
        colorbar=dict(title="Hours"),
    ))
    fig.update_layout(
        title="Machine Utilization Heatmap",
        xaxis_title="Week", yaxis_title="Machine",
        template="plotly_white", plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=100, l=180),
        xaxis=dict(tickangle=45),
    )
    return fig


def build_pareto_chart(weekly: pd.DataFrame) -> go.Figure:
    machine_totals = (
        weekly.groupby("Machine Name")["Actual_Output"]
        .sum().sort_values(ascending=False).reset_index()
    )
    machine_totals["Cumulative"] = machine_totals["Actual_Output"].cumsum()
    machine_totals["Cumulative_Pct"] = machine_totals["Cumulative"] / machine_totals["Actual_Output"].sum() * 100

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=machine_totals["Machine Name"], y=machine_totals["Actual_Output"],
        name="Output (Lbs)", marker_color="#3b82f6",
        hovertemplate="Machine: %{x}<br>Output: %{y:,.0f} lbs<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=machine_totals["Machine Name"], y=machine_totals["Cumulative_Pct"],
        name="Cumulative %", mode="lines+markers", yaxis="y2",
        line=dict(color="#ef4444", width=2), marker=dict(size=8),
        hovertemplate="Cumulative: %{y:.1f}%<extra></extra>",
    ))
    fig.add_hline(y=80, line_dash="dash", line_color="#9ca3af", annotation_text="80%", yref="y2")
    fig.update_layout(
        title="Pareto Analysis: Machine Output Contribution",
        xaxis_title="Machine",
        yaxis=dict(title="Output (Lbs)", side="left"),
        yaxis2=dict(title="Cumulative %", side="right", overlaying="y", range=[0, 105]),
        template="plotly_white", plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        legend=dict(x=0.7, y=1.1, orientation="h"),
        margin=dict(t=100, b=100),
    )
    return fig


# ---------------------------------------------------------------------------
# Dashboard renderer
# ---------------------------------------------------------------------------

def render_dashboard(
    trends_std: str, trends_sup: str,
    fig_sections_std: list, fig_sections_sup: list,
    machine_options_html: str,
    metric_options_html: str,
    snapshot_std: str, snapshot_sup: str,
    monthly_std: str, monthly_sup: str,
    total_weeks: int = 20,
) -> str:
    def _render_figs(fig_sections):
        rendered = [
            (title, to_html(fig, include_plotlyjs=False, full_html=False,
                            default_width="100%", default_height="600px", div_id=fig_id))
            for title, fig_id, fig in fig_sections
        ]
        return "\n".join(
            f'<section class="card"><h2 style="margin-top:0">{title}</h2>{html}</section>'
            for title, html in rendered
        )
    sections_std = _render_figs(fig_sections_std)
    sections_sup = _render_figs(fig_sections_sup)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Processing Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.3.min.js"></script>
  <style>
    :root {{ --bg:#f3f4f6; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; padding:24px; font-family:"Helvetica Neue",Arial,sans-serif;
            background:radial-gradient(circle at 20% 20%,#f9fafb 0,#eef2ff 40%,#f3f4f6 90%); color:var(--text); }}
    h1 {{ margin:0 0 4px; font-weight:700; }}
    .subtitle {{ margin:0 0 16px; color:var(--muted); font-size:14px; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:16px;
             box-shadow:0 10px 50px rgba(15,23,42,.08); padding:20px; margin-bottom:20px; }}
    .kpi-grid {{ display:grid; gap:12px; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); margin:8px 0; }}
    .kpi-card {{ background:#f8fafc; border:1px solid var(--border); border-radius:12px; padding:12px; }}
    .kpi-label {{ color:var(--muted); font-size:12px; }}
    .kpi-value {{ font-size:20px; font-weight:700; margin-top:4px; }}
    .controls {{ display:flex; gap:12px; flex-wrap:wrap; margin-bottom:16px; align-items:center; }}
    .controls label {{ font-weight:600; color:var(--muted); margin-right:4px; }}
    select {{ padding:8px 10px; border-radius:8px; border:1px solid var(--border); background:#fff; min-width:160px; }}
    .toggle-btn {{ padding:7px 14px; border-radius:8px; border:1px solid var(--border); background:#fff;
                   cursor:pointer; font-size:13px; transition:all .2s; }}
    .toggle-btn.active {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .toggle-btn:hover {{ background:#f3f4f6; }}
    .toggle-btn.active:hover {{ background:#2563eb; }}
    .range-control {{ display:flex; align-items:center; gap:6px; }}
    .range-control label {{ font-weight:600; color:var(--muted); }}
    .range-btns {{ display:flex; gap:0; }}
    .range-btn {{ padding:6px 12px; border:1px solid var(--border); background:#fff; cursor:pointer;
                  font-size:12px; transition:all .2s; }}
    .range-btn:first-child {{ border-radius:8px 0 0 8px; }}
    .range-btn:last-child {{ border-radius:0 8px 8px 0; }}
    .range-btn:not(:first-child) {{ border-left:none; }}
    .range-btn.active {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .range-btn:hover:not(.active) {{ background:#f3f4f6; }}
    .muted {{ color:var(--muted); }}
    .table-wrap {{ overflow-x:auto; }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }}
    th {{ background:#111827; color:#fff; }}
    .trend-up {{ color:#059669; font-size:12px; margin-left:6px; }}
    .trend-down {{ color:#dc2626; font-size:12px; margin-left:6px; }}
    .trend-flat {{ color:#6b7280; font-size:12px; margin-left:6px; }}
    .highlight-warning {{ background:#fef3c7; color:#92400e; font-weight:600; }}
    .export-buttons {{ display:flex; gap:8px; margin-left:auto; }}
    .export-btn {{ padding:8px 14px; border-radius:8px; border:1px solid var(--border); background:#fff;
                   cursor:pointer; font-size:13px; }}
    .export-btn:hover {{ background:#f3f4f6; }}
    .nav-link {{ display:inline-block; padding:8px 16px; background:#3b82f6; color:#fff;
                 text-decoration:none; border-radius:6px; font-size:14px; margin-bottom:16px; }}
    .nav-link:hover {{ background:#2563eb; }}
    @media (max-width:768px) {{
      body {{ padding:12px; }}
      .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
      .kpi-value {{ font-size:16px; }}
      .controls {{ flex-direction:column; gap:8px; }}
      select {{ width:100%; min-width:unset; }}
      h1 {{ font-size:1.5rem; }}
      .card {{ padding:12px; border-radius:12px; }}
    }}
    @media (max-width:480px) {{ .kpi-grid {{ grid-template-columns:1fr; }} table {{ font-size:12px; }} }}
    @media print {{ .controls,.export-buttons,.toggle-btn {{ display:none; }}
      .card {{ break-inside:avoid; }} body {{ background:#fff; padding:0; }} }}
  </style>
</head>
<body>
  <header>
    <h1>Processing Performance Dashboard</h1>
    <p class="subtitle">Use controls below to adjust view. {total_weeks} weeks of data available.</p>
    <a href="daily.html" class="nav-link">View Daily Details</a>
  </header>
  <main>
    <div class="controls">
      <div>
        <label for="machineSelect">Machine:</label>
        <select id="machineSelect">{machine_options_html}</select>
      </div>
      <div>
        <label for="metricSelect">Metric:</label>
        <select id="metricSelect">{metric_options_html}</select>
      </div>
      <button class="toggle-btn" id="showRawBtn" title="Show raw weekly values instead of running averages">Show Raw</button>
      <button class="toggle-btn" id="supportBtn" title="Include Guillotine support work (cutting for other machines) in output totals">+ Guillotine Support</button>
      <div class="range-control">
        <label>Range:</label>
        <div class="range-btns">
          <button class="range-btn" data-weeks="12">12w</button>
          <button class="range-btn active" data-weeks="{DEFAULT_WEEKS}">20w</button>
          <button class="range-btn" data-weeks="52">1y</button>
          <button class="range-btn" data-weeks="{total_weeks}">All</button>
        </div>
      </div>
      <div class="export-buttons">
        <button class="export-btn" onclick="exportChart(includeSupport ? 'fig-metrics-sup' : 'fig-metrics')">Export PNG</button>
        <button class="export-btn" onclick="window.print()">Print</button>
      </div>
    </div>
    <!-- Standard view (profit-producing output only) -->
    <div id="view-standard">
      <section class="card">
        <h2 style="margin-top:0">Recent Trends</h2>
        {trends_std}
      </section>
      {sections_std}
      <section class="card">
        <h2 style="margin-top:0">Latest Week vs 4-Week Average</h2>
        {snapshot_std}
      </section>
      <section class="card">
        <h2 style="margin-top:0">Monthly Summary</h2>
        {monthly_std}
      </section>
    </div>
    <!-- With Guillotine support work -->
    <div id="view-support" style="display:none;">
      <section class="card">
        <h2 style="margin-top:0">Recent Trends <span style="font-size:13px;color:var(--muted);font-weight:400;">(incl. Guillotine support)</span></h2>
        {trends_sup}
      </section>
      {sections_sup}
      <section class="card">
        <h2 style="margin-top:0">Latest Week vs 4-Week Average <span style="font-size:13px;color:var(--muted);font-weight:400;">(incl. Guillotine support)</span></h2>
        {snapshot_sup}
      </section>
      <section class="card">
        <h2 style="margin-top:0">Monthly Summary <span style="font-size:13px;color:var(--muted);font-weight:400;">(incl. Guillotine support)</span></h2>
        {monthly_sup}
      </section>
    </div>
  </main>
  <script>
    const machineSelect = document.getElementById('machineSelect');
    const metricSelect = document.getElementById('metricSelect');
    const showRawBtn = document.getElementById('showRawBtn');
    const supportBtn = document.getElementById('supportBtn');
    const viewStandard = document.getElementById('view-standard');
    const viewSupport = document.getElementById('view-support');
    const rangeBtns = document.querySelectorAll('.range-btn');
    let showRaw = false;
    let includeSupport = false;
    let supportInitialized = false;
    let rangeWeeks = {DEFAULT_WEEKS};

    function getMetricsFig() {{
      return document.getElementById(includeSupport ? 'fig-metrics-sup' : 'fig-metrics');
    }}
    function getProductFig() {{
      return document.getElementById(includeSupport ? 'fig-products-sup' : 'fig-products');
    }}

    // Compute x-axis date range from weeks setting
    function getXRange(fig) {{
      if (!fig || !fig.data) return null;
      // Collect all x dates across visible and hidden traces
      let allDates = [];
      fig.data.forEach(tr => {{
        if (tr.x) tr.x.forEach(d => allDates.push(new Date(d)));
      }});
      if (allDates.length === 0) return null;
      const maxDate = new Date(Math.max(...allDates));
      const totalWeeks = {total_weeks};
      if (rangeWeeks >= totalWeeks) return null; // show all — let Plotly autorange
      const minDate = new Date(maxDate);
      minDate.setDate(minDate.getDate() - rangeWeeks * 7);
      // Pad by a few days for readability
      const padMin = new Date(minDate); padMin.setDate(padMin.getDate() - 3);
      const padMax = new Date(maxDate); padMax.setDate(padMax.getDate() + 3);
      return [padMin.toISOString().slice(0, 10), padMax.toISOString().slice(0, 10)];
    }}

    // Range buttons
    rangeBtns.forEach(btn => {{
      btn.addEventListener('click', () => {{
        rangeBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        rangeWeeks = parseInt(btn.dataset.weeks, 10);
        applyRange();
      }});
    }});

    function applyRange() {{
      // Apply range to all chart figures in both views
      const figIds = [
        'fig-metrics', 'fig-products', 'fig-heatmap',
        'fig-metrics-sup', 'fig-products-sup', 'fig-heatmap-sup'
      ];
      figIds.forEach(id => {{
        const el = document.getElementById(id);
        if (!el || !el.data) return;
        const range = getXRange(el);
        if (range) {{
          Plotly.relayout(el, {{'xaxis.range': range, 'xaxis.autorange': false}});
        }} else {{
          Plotly.relayout(el, {{'xaxis.autorange': true}});
        }}
      }});
    }}

    supportBtn.addEventListener('click', () => {{
      includeSupport = !includeSupport;
      supportBtn.classList.toggle('active', includeSupport);
      supportBtn.textContent = includeSupport ? '\\u2713 Guillotine Support' : '+ Guillotine Support';
      viewStandard.style.display = includeSupport ? 'none' : '';
      viewSupport.style.display = includeSupport ? '' : 'none';
      if (includeSupport && !supportInitialized) {{
        supportInitialized = true;
        ['fig-metrics-sup','fig-heatmap-sup','fig-pareto-sup','fig-products-sup'].forEach(id => {{
          const el = document.getElementById(id);
          if (el && el.data) Plotly.Plots.resize(el);
        }});
      }}
      updatePlots();
      applyRange();
    }});

    showRawBtn.addEventListener('click', () => {{
      showRaw = !showRaw;
      showRawBtn.classList.toggle('active', showRaw);
      showRawBtn.textContent = showRaw ? 'Show Smoothed' : 'Show Raw';
      rebuildMetricDropdown();
      updatePlots();
    }});

    function rebuildMetricDropdown() {{
      const opts = metricSelect.querySelectorAll('option');
      opts.forEach(opt => {{
        const isRaw = opt.dataset.group === 'raw';
        const isKeyRA = opt.dataset.group === 'key_ra';
        const isExtraRA = opt.dataset.group === 'extra_ra';
        if (showRaw) {{
          opt.style.display = isRaw ? '' : 'none';
        }} else {{
          opt.style.display = (isKeyRA || isExtraRA) ? '' : 'none';
        }}
      }});
      const current = metricSelect.options[metricSelect.selectedIndex];
      if (current && current.style.display === 'none') {{
        for (const opt of metricSelect.options) {{
          if (opt.style.display !== 'none') {{ metricSelect.value = opt.value; break; }}
        }}
      }}
    }}

    function updatePlots() {{
      const selectedMachine = machineSelect.value;
      const selectedMetric = metricSelect.value;
      const metricsFig = getMetricsFig();
      const productFig = getProductFig();

      if (metricsFig && metricsFig.data) {{
        const vis = metricsFig.data.map(tr => {{
          if (!tr.meta) return false;
          const metricMatch = tr.meta.metric === selectedMetric;
          const machineMatch = selectedMachine === 'All Machines' || tr.meta.machine === selectedMachine;
          return metricMatch && machineMatch;
        }});
        Plotly.restyle(metricsFig, 'visible', vis);
        const label = metricsFig.data.find((tr, idx) => vis[idx])?.meta?.label || selectedMetric;
        const range = getXRange(metricsFig);
        const layoutUpdate = {{title: label + ' by Machine', yaxis: {{title: label}}}};
        if (range) {{
          layoutUpdate['xaxis.range'] = range;
          layoutUpdate['xaxis.autorange'] = false;
        }} else {{
          layoutUpdate['xaxis.autorange'] = true;
        }}
        Plotly.relayout(metricsFig, layoutUpdate);
      }}

      if (productFig && productFig.data) {{
        const vis = productFig.data.map(tr => {{
          if (!tr.meta) return false;
          return selectedMachine === 'All Machines' ? tr.meta.machine === 'All Machines' : tr.meta.machine === selectedMachine;
        }});
        Plotly.restyle(productFig, 'visible', vis);
        const range = getXRange(productFig);
        const layoutUpdate = {{title: 'Output Product Breakdown \\u2014 ' + selectedMachine}};
        if (range) {{
          layoutUpdate['xaxis.range'] = range;
          layoutUpdate['xaxis.autorange'] = false;
        }} else {{
          layoutUpdate['xaxis.autorange'] = true;
        }}
        Plotly.relayout(productFig, layoutUpdate);
      }}
    }}

    function exportChart(divId) {{
      const el = document.getElementById(divId);
      if (el) Plotly.downloadImage(el, {{format:'png', width:1200, height:800, filename:'dashboard-'+divId}});
    }}

    machineSelect.addEventListener('change', updatePlots);
    metricSelect.addEventListener('change', updatePlots);

    // Initialize
    rebuildMetricDropdown();
    updatePlots();
    // Apply default range after Plotly renders
    setTimeout(applyRange, 500);
  </script>
</body>
</html>"""


def _build_pipeline(df: pd.DataFrame):
    """Run full aggregation + chart pipeline on a dataframe. Returns (weekly_all, df, trends, snapshot, monthly)."""
    weekly_all = aggregate_weekly(df)
    weekly_all = add_running_averages(weekly_all, metrics=list(ALL_METRICS.keys()), window=RUNNING_AVG_WINDOW)

    trends_html = build_recent_trends_html(weekly_all)
    snapshot_html = build_latest_week_table_html(weekly_all)
    monthly_html = build_monthly_summary_html(weekly_all)

    return weekly_all, df, trends_html, snapshot_html, monthly_html


def main(input_path: Path, output_path: Path) -> None:
    df = load_data(input_path)
    df = df[(df["Total Man Hours"] > 0) | (df["Actual Input (Lbs)"] > 0)]

    machine_options = ["All Machines"] + sorted(df["Machine Name"].unique())
    machine_options_html = "\n".join(f'<option value="{m}">{m}</option>' for m in machine_options)

    # Metric dropdown
    metric_opts = []
    for key, (label, _) in KEY_METRICS.items():
        metric_opts.append((f"{key}_RA", f"{label} ({RUNNING_AVG_WINDOW}-wk avg)", "key_ra"))
    for key, (label, _) in ALL_METRICS.items():
        if key not in KEY_METRICS:
            metric_opts.append((f"{key}_RA", f"{label} ({RUNNING_AVG_WINDOW}-wk avg)", "extra_ra"))
    for key, (label, _) in ALL_METRICS.items():
        metric_opts.append((key, f"{label} (raw)", "raw"))
    metric_options_html = "\n".join(
        f'<option value="{val}" data-group="{group}" {"selected" if i == 0 else ""}'
        f' style="{"display:none" if group == "raw" else ""}">{label}</option>'
        for i, (val, label, group) in enumerate(metric_opts)
    )

    # Standard pipeline (profit-producing output only)
    df_std = df[(df["Total Man Hours"] > 0) & (df["Total Machine Hours"] > 0)]
    weekly_std, df_std_full, trends_std, snapshot_std, monthly_std = _build_pipeline(df_std)

    # With Guillotine support work included
    df_sup = _apply_guillotine_support(df)
    df_sup = df_sup[(df_sup["Total Man Hours"] > 0) | (df_sup["Actual Output (Lbs)"] > 0)]
    df_sup = df_sup[(df_sup["Total Man Hours"] > 0) & (df_sup["Total Machine Hours"] > 0)]
    weekly_sup, df_sup_full, trends_sup, snapshot_sup, monthly_sup = _build_pipeline(df_sup)

    # Total weeks available (for range control)
    total_weeks = len(weekly_std["Week Start"].unique())

    # Charts for both modes — pass ALL data, JS controls visible range
    fig_sections_std = [
        ("Weekly Metrics by Machine", "fig-metrics", build_interactive_fig(weekly_std)),
        ("Machine Utilization Heatmap", "fig-heatmap", build_utilization_heatmap(weekly_std)),
        ("Pareto Analysis: Output Contribution", "fig-pareto", build_pareto_chart(weekly_std)),
        ("Output Product Breakdown", "fig-products", build_output_product_fig(df_std_full)),
    ]
    fig_sections_sup = [
        ("Weekly Metrics by Machine", "fig-metrics-sup", build_interactive_fig(weekly_sup)),
        ("Machine Utilization Heatmap", "fig-heatmap-sup", build_utilization_heatmap(weekly_sup)),
        ("Pareto Analysis: Output Contribution", "fig-pareto-sup", build_pareto_chart(weekly_sup)),
        ("Output Product Breakdown", "fig-products-sup", build_output_product_fig(df_sup_full)),
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_dashboard(
            trends_std, trends_sup,
            fig_sections_std, fig_sections_sup,
            machine_options_html, metric_options_html,
            snapshot_std, snapshot_sup,
            monthly_std, monthly_sup,
            total_weeks=total_weeks,
        ),
        encoding="utf-8",
    )
    print(f"Wrote interactive dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build interactive processing dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to aggregated_daily_data.xlsx")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Path to write HTML dashboard")
    args = parser.parse_args()
    main(args.input, args.output)
