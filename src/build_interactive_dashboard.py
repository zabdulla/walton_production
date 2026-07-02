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
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.io import to_html

from config import (
    PROJECT_ROOT, DEFAULT_AGGREGATED_DATA,
    CHART_PALETTE, MACHINE_WEEKLY_CAPACITY, DEFAULT_WEEKLY_CAPACITY,
    UTILIZATION_TARGET_PCT, MACHINE_WEEKLY_OUTPUT_TARGETS,
    PRODUCT_TYPO_MAP, PRODUCT_CATEGORY_MAP,
    KEY_METRICS, ALL_METRICS,
    DEFAULT_WEEKS, RUNNING_AVG_WINDOW, COST_PER_POUND_THRESHOLD,
    LABOR_RATE,
)
from dashboard_common import (
    BASE_CSS, CARD_CSS, PLOTLY_CONFIG, MOBILE_MODEBAR_CSS, MOBILE_PLOTLY_JS,
    SHIFT_METRICS, SHIFT_COLORS,
)
from interactive_template import render_dashboard  # re-export; template extracted 2026-07

logger = logging.getLogger(__name__)

DEFAULT_INPUT = DEFAULT_AGGREGATED_DATA
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "index.html"


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
    # Check NaN/NA before the equality comparison: `previous == 0` evaluates
    # to pd.NA (not False) when previous is pd.NA, which makes `or` raise
    # TypeError: boolean value of NA is ambiguous.
    if pd.isna(previous) or pd.isna(current) or previous == 0:
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


def clean_product_names(df: pd.DataFrame) -> pd.DataFrame:
    """Fix typos and map output products to categories."""
    df = df.copy()
    if "Output Product" in df.columns:
        df["Output Product"] = df["Output Product"].replace(PRODUCT_TYPO_MAP)
        df["Product Category"] = df["Output Product"].map(PRODUCT_CATEGORY_MAP).fillna("Other")
        unmapped = df.loc[df["Product Category"] == "Other", "Output Product"].dropna().unique()
        for p in unmapped:
            logger.warning("Unmapped product: %s", p)
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

def build_monthly_summary_html(weekly: pd.DataFrame, visible_months: int = 6) -> str:
    """Month-over-month summary table showing all months (newest on top).

    First ``visible_months`` rows are shown by default; older rows are hidden
    behind a toggle button (unique per table instance via a random id).
    """
    import uuid
    table_id = f"monthly-{uuid.uuid4().hex[:8]}"

    weekly = weekly.copy()
    weekly["Month"] = weekly["Week Start"].dt.to_period("M")
    months_asc = sorted(weekly["Month"].unique())

    # Build row data in ascending order so trends compare correctly to the
    # chronologically prior month, then reverse for descending display.
    row_data = []
    prev_output = None
    prev_expense = None
    for month in months_asc:
        m = weekly[weekly["Month"] == month]
        output = m["Actual_Output"].sum()
        expense = m["Total_Expense"].sum()
        hours = m["Total_Machine_Hours"].sum()
        cost_lb = expense / max(output, 1)
        output_trend = _pct_change_html(output, prev_output) if prev_output is not None else ""
        expense_trend = _pct_change_html(expense, prev_expense) if prev_expense is not None else ""
        row_data.append({
            "label": month.strftime("%b %Y"),
            "output": output, "output_trend": output_trend,
            "expense": expense, "expense_trend": expense_trend,
            "cost_lb": cost_lb, "hours": hours,
        })
        prev_output, prev_expense = output, expense

    # Reverse so newest is on top
    row_data.reverse()
    total_months = len(row_data)
    older_count = max(0, total_months - visible_months)

    rows_html = []
    for i, d in enumerate(row_data):
        older_attr = ' class="older-month" style="display:none;"' if i >= visible_months else ""
        rows_html.append(f"""<tr{older_attr}>
            <td>{d['label']}</td>
            <td>{_fmt_num(d['output'])} {d['output_trend']}</td>
            <td>{_fmt_num(d['expense'], 'currency')} {d['expense_trend']}</td>
            <td>{_fmt_num(d['cost_lb'], 'currency4')}</td>
            <td>{_fmt_num(d['hours'], 'float1')}</td>
        </tr>""")

    toggle_html = ""
    if older_count > 0:
        toggle_html = (
            f'<button class="toggle-btn monthly-toggle" data-table="{table_id}" '
            f'style="margin-top:10px;">Show all months ({older_count} older)</button>'
        )

    return f"""
    <div class="table-wrap">
      <table id="{table_id}">
        <thead><tr>
            <th>Month</th><th>Output (Lbs)</th><th>Expense</th><th>Cost / Lb</th><th>Machine Hrs</th>
        </tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
    </div>
    {toggle_html}
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
# Target performance RAG (red/amber/green) cards
# ---------------------------------------------------------------------------

RAG_LOOKBACK_WEEKS = 8
# Misses out of the lookback window → status. <=1 green, <=3 amber, else red.
RAG_THRESHOLDS = [(1, "#22c55e", "On target"), (3, "#f59e0b", "Needs attention")]
RAG_RED = ("#ef4444", "Below target")


def target_miss_counts(weekly: pd.DataFrame, lookback: int = RAG_LOOKBACK_WEEKS) -> dict[str, dict]:
    """Per tracked machine: how many of the last `lookback` weeks missed target.

    A week with no rows for the machine counts as a miss — the target is
    weekly, so an idle week is a missed week, not missing data.
    """
    week_starts = sorted(weekly["Week Start"].unique())[-lookback:]
    results: dict[str, dict] = {}
    seen_machines = set(weekly["Machine Name"].unique())
    for machine, target in sorted(MACHINE_WEEKLY_OUTPUT_TARGETS.items()):
        if machine not in seen_machines:
            continue
        scope = weekly[(weekly["Machine Name"] == machine) & weekly["Week Start"].isin(week_starts)]
        by_week = scope.groupby("Week Start")["Actual_Output"].sum()
        weeks = [
            {"week": pd.Timestamp(w), "output": float(by_week.get(w, 0.0)), "target": target,
             "hit": bool(by_week.get(w, 0.0) >= target)}
            for w in week_starts
        ]
        results[machine] = {
            "target": target,
            "weeks": weeks,
            "misses": sum(1 for w in weeks if not w["hit"]),
            "n_weeks": len(weeks),
        }
    return results


def _rag_status(misses: int) -> tuple[str, str]:
    for limit, color, label in RAG_THRESHOLDS:
        if misses <= limit:
            return color, label
    return RAG_RED


def build_target_rag_html(weekly: pd.DataFrame, lookback: int = RAG_LOOKBACK_WEEKS) -> str:
    """KPI cards flagging machines below weekly output target, red/amber/green."""
    counts = target_miss_counts(weekly, lookback)
    if not counts:
        return "<p class='muted'>No tracked machines in the data.</p>"

    cards = []
    for machine, info in counts.items():
        color, status = _rag_status(info["misses"])
        dots = "".join(
            f'<span class="rag-dot" title="{w["week"].strftime("%b %d")}: '
            f'{w["output"]:,.0f} / {w["target"]:,.0f} lbs" '
            f'style="background:{"#22c55e" if w["hit"] else "#ef4444"};"></span>'
            for w in info["weeks"]
        )
        cards.append(f"""
        <div class="kpi-card rag-card" style="border-left:5px solid {color};">
            <div class="kpi-label">{machine}</div>
            <div class="kpi-value" style="color:{color};">{info['misses']} / {info['n_weeks']}
                <span style="font-size:12px;font-weight:400;color:var(--muted);">weeks below target</span>
            </div>
            <div class="rag-dots">{dots}</div>
            <div style="font-size:12px;font-weight:600;color:{color};margin-top:4px;">{status}</div>
        </div>""")

    return f"""
    <p style="color:var(--muted);font-size:13px;margin:0 0 8px;">
        Weeks below weekly output target, last {lookback} weeks (oldest dot first; hover a dot for the numbers).
    </p>
    <div class="kpi-grid">{''.join(cards)}</div>
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


def build_utilization_bullet_fig(weekly: pd.DataFrame) -> go.Figure:
    """Horizontal bullet bars: latest week utilization % per machine with target line."""
    latest_week = weekly["Week Start"].max()
    latest = weekly[weekly["Week Start"] == latest_week]
    week_label = latest["Week Label"].iloc[0] if not latest.empty else ""

    # Only show machines with output targets
    machines = sorted(m for m in latest["Machine Name"].unique() if m in MACHINE_WEEKLY_OUTPUT_TARGETS)
    utils = []
    caps = []
    hours_vals = []
    for m in machines:
        row = latest[latest["Machine Name"] == m]
        hrs = row["Total_Machine_Hours"].sum() if not row.empty else 0
        cap = MACHINE_WEEKLY_CAPACITY.get(m, DEFAULT_WEEKLY_CAPACITY)
        pct = (hrs / cap * 100) if cap > 0 else 0
        utils.append(round(pct, 1))
        caps.append(cap)
        hours_vals.append(round(hrs, 1))

    # Color bars by utilization level
    colors = []
    for u in utils:
        if u >= 90:
            colors.append("#22c55e")  # green — strong
        elif u >= 70:
            colors.append("#3b82f6")  # blue — on track
        elif u >= 50:
            colors.append("#f59e0b")  # amber — needs attention
        else:
            colors.append("#ef4444")  # red — underutilized

    target_pct = UTILIZATION_TARGET_PCT

    fig = go.Figure()

    # Actual utilization bars
    fig.add_trace(go.Bar(
        y=machines, x=utils, orientation="h",
        marker_color=colors,
        text=[f"{u:.0f}%" for u in utils],
        textposition="auto",
        hovertemplate=[
            f"{m}<br>Utilization: {u:.0f}%<br>Hours: {h:.1f} / {c}h capacity<extra></extra>"
            for m, u, h, c in zip(machines, utils, hours_vals, caps)
        ],
    ))

    # Target line
    fig.add_vline(
        x=target_pct, line_dash="dash", line_color="#ef4444", line_width=2,
        annotation_text=f"Target {target_pct}%", annotation_position="top",
        annotation_font=dict(color="#ef4444", size=12),
    )

    fig.update_layout(
        title=f"Machine Utilization — Week of {week_label}",
        xaxis_title="Utilization %", yaxis_title="",
        xaxis=dict(range=[0, max(max(utils) + 10, target_pct + 10, 110)], dtick=20),
        template="plotly_white", plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=60, l=200),
        showlegend=False,
        height=max(350, len(machines) * 45 + 120),
    )
    fig.update_yaxes(autorange="reversed")
    return fig


def build_targets_vs_actuals_fig(weekly: pd.DataFrame) -> go.Figure:
    """Weekly output vs target per machine, with machine selector and WoW delta."""
    # Only include machines with targets
    tracked_machines = sorted(m for m in weekly["Machine Name"].unique() if m in MACHINE_WEEKLY_OUTPUT_TARGETS)
    weekly_tracked = weekly[weekly["Machine Name"].isin(tracked_machines)]
    machine_options = ["All Machines"] + tracked_machines

    traces = []
    for machine in machine_options:
        if machine == "All Machines":
            scope = weekly_tracked.groupby("Week Start").agg(
                Actual_Output=("Actual_Output", "sum"),
                Week_Label=("Week Label", "first"),
            ).reset_index()
            target = sum(MACHINE_WEEKLY_OUTPUT_TARGETS.values())
        else:
            scope = weekly_tracked[weekly_tracked["Machine Name"] == machine].copy()
            scope = scope.rename(columns={"Week Label": "Week_Label"})
            target = MACHINE_WEEKLY_OUTPUT_TARGETS.get(machine, 0)

        scope = scope.sort_values("Week Start")

        # Actual output bars
        bar_colors = ["#22c55e" if v >= target else "#ef4444" for v in scope["Actual_Output"]]
        traces.append(go.Bar(
            x=scope["Week Start"], y=scope["Actual_Output"],
            name="Actual",
            marker_color=bar_colors,
            hovertemplate=[
                f"Machine: {machine}<br>Week: {wl}<br>Actual: {act:,.0f} lbs<br>Target: {target:,.0f} lbs<br>{'✓ Hit' if act >= target else f'Gap: {target - act:,.0f} lbs'}<extra></extra>"
                for wl, act in zip(scope["Week_Label"], scope["Actual_Output"])
            ],
            visible=False,
            meta={"machine": machine, "chart_type": "targets"},
        ))

        # Target line
        traces.append(go.Scatter(
            x=scope["Week Start"], y=[target] * len(scope),
            name="Target",
            mode="lines",
            line=dict(color="#6b7280", width=2, dash="dash"),
            hovertemplate=f"Target: {target:,.0f} lbs<extra></extra>",
            visible=False,
            showlegend=True,
            meta={"machine": machine, "chart_type": "targets"},
        ))

        # Forecast projection (linear trend from last 8 weeks, projected 4 weeks ahead)
        if len(scope) >= 4:  # Need at least 4 data points
            recent = scope.tail(8).copy()
            x_numeric = (recent["Week Start"] - recent["Week Start"].iloc[0]).dt.days.values.astype(float)
            y_vals = recent["Actual_Output"].values.astype(float)

            try:
                coeffs = np.polyfit(x_numeric, y_vals, 1)
                poly = np.poly1d(coeffs)

                # Build forecast dates: from first point of regression window to 4 weeks ahead
                last_date = recent["Week Start"].iloc[-1]
                forecast_dates = pd.date_range(recent["Week Start"].iloc[0], periods=len(recent) + 4, freq="7D")
                x_forecast = (forecast_dates - recent["Week Start"].iloc[0]).days.astype(float)
                y_forecast = poly(x_forecast)

                traces.append(go.Scatter(
                    x=forecast_dates, y=y_forecast,
                    name="Trend/Forecast",
                    mode="lines",
                    line=dict(color="#a855f7", width=2, dash="dot"),
                    hovertemplate="Forecast: %{y:,.0f} lbs<extra></extra>",
                    visible=False,
                    showlegend=True,
                    meta={"machine": machine, "chart_type": "targets"},
                ))
            except Exception:
                pass  # Skip forecast if regression fails

    # Default: All Machines visible
    for tr in traces:
        if tr.meta["machine"] == "All Machines":
            tr.visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title="Output vs Target — All Machines",
        xaxis_title="Week", yaxis_title="Output (Lbs)",
        hovermode="x unified", template="plotly_white",
        plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=60, l=70),
        legend=dict(orientation="h", x=0.5, xanchor="center", y=1.08),
        barmode="overlay",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


# ---------------------------------------------------------------------------
# Shift comparison
# ---------------------------------------------------------------------------



def aggregate_weekly_by_shift(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily data by machine, week, and shift."""
    df = df.copy()
    shift_col = "Shift" if "Shift" in df.columns else None
    if shift_col is None:
        return pd.DataFrame()
    df = df[df["Shift"].isin(["1st", "2nd", "3rd"])]
    grouped = (
        df.groupby(["Machine Name", "Start Date", "Shift"])
        .agg(
            Actual_Output=("Actual Output (Lbs)", "sum"),
            Total_Machine_Hours=("Total Machine Hours", "sum"),
            Total_Man_Hours=("Total Man Hours", "sum"),
            Total_Expense=("Total Expense", "sum"),
        )
        .reset_index()
        .rename(columns={"Start Date": "Week Start"})
    )
    # Leave NaN where the denominator is zero — Plotly omits the bar, which is
    # truthful; a 0 bar would read as "free production" / "zero rate".
    grouped["Output_per_Hour"] = grouped["Actual_Output"] / grouped["Total_Machine_Hours"].replace(0, float("nan"))
    grouped["Cost_per_Pound"] = grouped["Total_Expense"] / grouped["Actual_Output"].replace(0, float("nan"))
    grouped["Week Start"] = pd.to_datetime(grouped["Week Start"])
    grouped["Week Label"] = grouped["Week Start"].dt.strftime("%Y-%m-%d")
    return grouped


def build_shift_comparison_fig(df_shift: pd.DataFrame) -> go.Figure:
    """Build a grouped bar chart comparing shifts by metric, filterable by machine."""
    if df_shift.empty:
        return go.Figure()

    shifts = sorted(df_shift["Shift"].unique())
    machine_options = ["All Machines"] + sorted(df_shift["Machine Name"].unique())

    traces = []
    for metric_label, (metric_col, fmt, unit) in SHIFT_METRICS.items():
        for machine in machine_options:
            scope = df_shift if machine == "All Machines" else df_shift[df_shift["Machine Name"] == machine]
            if machine == "All Machines":
                # Aggregate across machines per week+shift
                agg = scope.groupby(["Week Start", "Week Label", "Shift"]).agg(
                    Actual_Output=("Actual_Output", "sum"),
                    Total_Machine_Hours=("Total_Machine_Hours", "sum"),
                    Total_Expense=("Total_Expense", "sum"),
                ).reset_index()
                agg["Output_per_Hour"] = agg["Actual_Output"] / agg["Total_Machine_Hours"].replace(0, float("nan"))
                agg["Cost_per_Pound"] = agg["Total_Expense"] / agg["Actual_Output"].replace(0, float("nan"))
                scope = agg

            for shift in shifts:
                subset = scope[scope["Shift"] == shift].sort_values("Week Start")
                traces.append(go.Bar(
                    x=subset["Week Start"],
                    y=subset[metric_col],
                    name=f"{shift} Shift",
                    hovertemplate=f"{shift} Shift<br>Week: %{{customdata[0]}}<br>{metric_label}: %{{y:{fmt}}} {unit}<extra></extra>",
                    customdata=subset[["Week Label"]],
                    visible=False,
                    marker_color=SHIFT_COLORS.get(shift, "#999"),
                    meta={"machine": machine, "shift_metric": metric_label, "shift": shift},
                ))

    # Default: Output/Hr, All Machines
    for tr in traces:
        if tr.meta["machine"] == "All Machines" and tr.meta["shift_metric"] == "Output/Hr":
            tr.visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title="Shift Comparison \u2014 Output/Hr \u2014 All Machines",
        barmode="group", xaxis_title="Week", yaxis_title="Output/Hr",
        hovermode="x unified", template="plotly_white",
        plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=180, b=60, l=70),
        legend=dict(title="Shift", orientation="v", x=1.08, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig




def _build_pipeline(df: pd.DataFrame):
    """Run full aggregation + chart pipeline on a dataframe.

    Returns (weekly_all, df, trends, rag, snapshot, monthly)."""
    weekly_all = aggregate_weekly(df)
    weekly_all = add_running_averages(weekly_all, metrics=list(ALL_METRICS.keys()), window=RUNNING_AVG_WINDOW)

    trends_html = build_recent_trends_html(weekly_all)
    rag_html = build_target_rag_html(weekly_all)
    snapshot_html = build_latest_week_table_html(weekly_all)
    monthly_html = build_monthly_summary_html(weekly_all)

    return weekly_all, df, trends_html, rag_html, snapshot_html, monthly_html


def main(input_path: Path, output_path: Path) -> None:
    df = load_data(input_path)
    df = clean_product_names(df)
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
    weekly_std, df_std_full, trends_std, rag_std, snapshot_std, monthly_std = _build_pipeline(df_std)

    # With Guillotine support work included
    df_sup = _apply_guillotine_support(df)
    df_sup = df_sup[(df_sup["Total Man Hours"] > 0) | (df_sup["Actual Output (Lbs)"] > 0)]
    df_sup = df_sup[(df_sup["Total Man Hours"] > 0) & (df_sup["Total Machine Hours"] > 0)]
    weekly_sup, df_sup_full, trends_sup, rag_sup, snapshot_sup, monthly_sup = _build_pipeline(df_sup)

    # Shift comparison charts
    shift_std = aggregate_weekly_by_shift(df_std)
    shift_fig_std = build_shift_comparison_fig(shift_std)
    shift_sup = aggregate_weekly_by_shift(df_sup)
    shift_fig_sup = build_shift_comparison_fig(shift_sup)

    # Total weeks available (for range control)
    total_weeks = len(weekly_std["Week Start"].unique())

    # Charts for both modes — pass ALL data, JS controls visible range
    fig_sections_std = [
        ("Weekly Metrics by Machine", "fig-metrics", build_interactive_fig(weekly_std)),
        ("Machine Utilization", "fig-util", build_utilization_bullet_fig(weekly_std)),
        ("Output vs Target", "fig-targets", build_targets_vs_actuals_fig(weekly_std)),
    ]
    fig_sections_sup = [
        ("Weekly Metrics by Machine", "fig-metrics-sup", build_interactive_fig(weekly_sup)),
        ("Machine Utilization", "fig-util-sup", build_utilization_bullet_fig(weekly_sup)),
        ("Output vs Target", "fig-targets-sup", build_targets_vs_actuals_fig(weekly_sup)),
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    from atomic import write_atomic_text
    write_atomic_text(
        output_path,
        render_dashboard(
            trends_std, trends_sup,
            rag_std, rag_sup,
            fig_sections_std, fig_sections_sup,
            machine_options_html, metric_options_html,
            snapshot_std, snapshot_sup,
            monthly_std, monthly_sup,
            shift_fig_std=shift_fig_std, shift_fig_sup=shift_fig_sup,
            total_weeks=total_weeks,
            latest_data_date=weekly_std["Week Start"].max().strftime("%Y-%m-%d"),
        ),
    )
    print(f"Wrote interactive dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build interactive processing dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to aggregated_daily_data.xlsx")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Path to write HTML dashboard")
    args = parser.parse_args()
    main(args.input, args.output)
