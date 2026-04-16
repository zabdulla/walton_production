"""
Generate an operator productivity dashboard (local only, not published).

Usage:
    python src/build_operator_dashboard.py \
        --input data/aggregated_daily_data.xlsx \
        --output reports/operator.html
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.io import to_html

# Add src to path so we can import from sibling module
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    PROJECT_ROOT, DEFAULT_AGGREGATED_DATA,
    CHART_PALETTE, DEFAULT_WEEKS, RUNNING_AVG_WINDOW,
)
from build_interactive_dashboard import load_data, clean_product_names, _fmt_num

DEFAULT_INPUT = DEFAULT_AGGREGATED_DATA
DEFAULT_OUTPUT = PROJECT_ROOT / "reports" / "operator.html"

TOP_N = 20  # Show top N operators by total hours


def explode_operators(df: pd.DataFrame) -> pd.DataFrame:
    """Split comma-separated operator names and divide output/hours evenly."""
    df = df.copy()
    df = df[df["Operator"].notna() & (df["Operator"].str.strip() != "")]
    df["Operator_List"] = df["Operator"].str.split(r",\s*")
    df["Operator_Count"] = df["Operator_List"].apply(len)
    df = df.explode("Operator_List")
    df["Individual_Operator"] = df["Operator_List"].str.strip().str.title()
    # Split output and hours evenly among operators
    for col in ["Actual Output (Lbs)", "Total Man Hours", "Total Machine Hours",
                "Labor Cost", "Total Expense"]:
        if col in df.columns:
            df[col] = df[col] / df["Operator_Count"]
    return df


def get_top_operators(df: pd.DataFrame, n: int = TOP_N) -> list[str]:
    """Get top N operators by total man hours worked."""
    totals = df.groupby("Individual_Operator")["Total Man Hours"].sum()
    return totals.nlargest(n).index.tolist()


def build_oph_bar_chart(df: pd.DataFrame, top_ops: list[str]) -> go.Figure:
    """Horizontal bar chart: average output per man-hour by operator."""
    scope = df[df["Individual_Operator"].isin(top_ops)]
    agg = scope.groupby("Individual_Operator").agg(
        Total_Output=("Actual Output (Lbs)", "sum"),
        Total_Hours=("Total Man Hours", "sum"),
    )
    agg["OPH"] = (agg["Total_Output"] / agg["Total_Hours"].replace(0, np.nan)).fillna(0)
    agg = agg.sort_values("OPH", ascending=True)

    fig = go.Figure(go.Bar(
        x=agg["OPH"], y=agg.index,
        orientation="h",
        marker_color="#3b82f6",
        hovertemplate="Operator: %{y}<br>Output/Man-Hr: %{x:,.1f} lbs<br>Total Hours: %{customdata:,.0f}<extra></extra>",
        customdata=agg["Total_Hours"],
    ))
    fig.update_layout(
        title=f"Average Output per Man-Hour — Top {len(top_ops)} Operators",
        xaxis_title="Output per Man-Hour (lbs)",
        yaxis_title="",
        template="plotly_white", plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=60, l=160),
        height=max(400, len(top_ops) * 28 + 100),
    )
    return fig


def build_operator_machine_heatmap(df: pd.DataFrame, top_ops: list[str]) -> go.Figure:
    """Heatmap: operator × machine total output."""
    scope = df[df["Individual_Operator"].isin(top_ops)]
    pivot = scope.pivot_table(
        index="Individual_Operator", columns="Machine Name",
        values="Actual Output (Lbs)", aggfunc="sum", fill_value=0,
    )
    # Sort operators by total output
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
        colorscale="Blues",
        hovertemplate="Operator: %{y}<br>Machine: %{x}<br>Output: %{z:,.0f} lbs<extra></extra>",
        colorbar=dict(title="Output (lbs)"),
    ))
    fig.update_layout(
        title="Operator-Machine Output Matrix",
        xaxis_title="Machine", yaxis_title="",
        template="plotly_white", plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=40, b=120, l=160),
        xaxis=dict(tickangle=45),
        height=max(400, len(top_ops) * 28 + 100),
    )
    return fig


def build_operator_trends_fig(df: pd.DataFrame, top_ops: list[str]) -> go.Figure:
    """Weekly output/man-hour trends by operator (4-wk running avg)."""
    scope = df[df["Individual_Operator"].isin(top_ops)]
    weekly = (
        scope.groupby(["Individual_Operator", "Start Date"])
        .agg(Output=("Actual Output (Lbs)", "sum"), Hours=("Total Man Hours", "sum"))
        .reset_index()
    )
    weekly["Week Start"] = pd.to_datetime(weekly["Start Date"])
    weekly["Week Label"] = weekly["Week Start"].dt.strftime("%Y-%m-%d")
    weekly["OPH"] = (weekly["Output"] / weekly["Hours"].replace(0, np.nan)).fillna(0)
    weekly = weekly.sort_values(["Individual_Operator", "Week Start"])
    weekly["OPH_RA"] = (
        weekly.groupby("Individual_Operator")["OPH"]
        .transform(lambda s: s.rolling(window=RUNNING_AVG_WINDOW, min_periods=1).mean())
    )

    traces = []
    for idx, op in enumerate(sorted(top_ops)):
        subset = weekly[weekly["Individual_Operator"] == op]
        traces.append(go.Scatter(
            x=subset["Week Start"], y=subset["OPH_RA"],
            mode="lines+markers", name=op,
            hovertemplate=f"Operator: {op}<br>Week: %{{customdata[0]}}<br>Output/Man-Hr: %{{y:,.1f}}<extra></extra>",
            customdata=subset[["Week Label"]],
            visible=False,
            marker=dict(size=6, line=dict(width=1, color="white")),
            line=dict(width=2, color=CHART_PALETTE[idx % len(CHART_PALETTE)]),
            meta={"operator": op},
        ))

    # Default: first operator visible
    if traces:
        traces[0].visible = True

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=f"Output/Man-Hour Trend ({RUNNING_AVG_WINDOW}-wk avg)",
        yaxis_title="Output per Man-Hour (lbs)",
        xaxis_title="Week",
        hovermode="x unified",
        template="plotly_white", plot_bgcolor="#f9fafc", paper_bgcolor="#fdfdff",
        font=dict(family="Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        margin=dict(t=80, r=220, b=60, l=70),
        legend=dict(title="Operator", orientation="v", x=1.08, y=0.5, bgcolor="#ffffff", bordercolor="#e5e7eb"),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", zerolinecolor="#cbd5e1")
    return fig


def render_operator_dashboard(
    oph_fig: go.Figure,
    heatmap_fig: go.Figure,
    trends_fig: go.Figure,
    operator_options_html: str,
    total_weeks: int,
) -> str:
    oph_html = to_html(oph_fig, include_plotlyjs=False, full_html=False,
                       default_width="100%", default_height="600px", div_id="fig-oph")
    heatmap_html = to_html(heatmap_fig, include_plotlyjs=False, full_html=False,
                           default_width="100%", default_height="600px", div_id="fig-op-heatmap")
    trends_html = to_html(trends_fig, include_plotlyjs=False, full_html=False,
                          default_width="100%", default_height="600px", div_id="fig-op-trends")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Operator Productivity (Internal)</title>
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
    .controls {{ display:flex; gap:12px; flex-wrap:wrap; margin-bottom:16px; align-items:center; }}
    .controls label {{ font-weight:600; color:var(--muted); margin-right:4px; }}
    select {{ padding:8px 10px; border-radius:8px; border:1px solid var(--border); background:#fff; min-width:180px; }}
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
    .internal-badge {{ display:inline-block; padding:4px 10px; background:#fef3c7; color:#92400e;
                       border-radius:6px; font-size:12px; font-weight:600; margin-left:12px; }}
  </style>
</head>
<body>
  <header>
    <h1>Operator Productivity Dashboard <span class="internal-badge">INTERNAL ONLY</span></h1>
    <p class="subtitle">Top {TOP_N} operators by total hours worked. {total_weeks} weeks of data available.</p>
  </header>
  <main>
    <div class="controls">
      <div>
        <label for="operatorSelect">Operator:</label>
        <select id="operatorSelect">{operator_options_html}</select>
      </div>
      <div class="range-control">
        <label>Range:</label>
        <div class="range-btns">
          <button class="range-btn" data-weeks="12">12w</button>
          <button class="range-btn active" data-weeks="{DEFAULT_WEEKS}">20w</button>
          <button class="range-btn" data-weeks="52">1y</button>
          <button class="range-btn" data-weeks="{total_weeks}">All</button>
        </div>
      </div>
    </div>
    <section class="card"><h2 style="margin-top:0">Average Output per Man-Hour</h2>{oph_html}</section>
    <section class="card"><h2 style="margin-top:0">Operator-Machine Output Matrix</h2>{heatmap_html}</section>
    <section class="card"><h2 style="margin-top:0">Weekly Trends by Operator</h2>{trends_html}</section>
  </main>
  <script>
    const operatorSelect = document.getElementById('operatorSelect');
    const trendsFig = document.getElementById('fig-op-trends');
    const rangeBtns = document.querySelectorAll('.range-btn');
    let rangeWeeks = {DEFAULT_WEEKS};

    function getXRange(fig) {{
      if (!fig || !fig.data) return null;
      let allDates = [];
      fig.data.forEach(tr => {{ if (tr.x) tr.x.forEach(d => allDates.push(new Date(d))); }});
      if (allDates.length === 0) return null;
      const maxDate = new Date(Math.max(...allDates));
      const totalWeeks = {total_weeks};
      if (rangeWeeks >= totalWeeks) return null;
      const minDate = new Date(maxDate);
      minDate.setDate(minDate.getDate() - rangeWeeks * 7);
      const padMin = new Date(minDate); padMin.setDate(padMin.getDate() - 3);
      const padMax = new Date(maxDate); padMax.setDate(padMax.getDate() + 3);
      return [padMin.toISOString().slice(0, 10), padMax.toISOString().slice(0, 10)];
    }}

    rangeBtns.forEach(btn => {{
      btn.addEventListener('click', () => {{
        rangeBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        rangeWeeks = parseInt(btn.dataset.weeks, 10);
        applyRange();
      }});
    }});

    function applyRange() {{
      const range = getXRange(trendsFig);
      if (trendsFig && trendsFig.data) {{
        if (range) {{
          Plotly.relayout(trendsFig, {{'xaxis.range': range, 'xaxis.autorange': false}});
        }} else {{
          Plotly.relayout(trendsFig, {{'xaxis.autorange': true}});
        }}
      }}
    }}

    operatorSelect.addEventListener('change', () => {{
      const selected = operatorSelect.value;
      if (!trendsFig || !trendsFig.data) return;
      const vis = trendsFig.data.map(tr => {{
        if (!tr.meta) return false;
        return selected === 'All' || tr.meta.operator === selected;
      }});
      Plotly.restyle(trendsFig, 'visible', vis);
      applyRange();
    }});

    setTimeout(applyRange, 500);
  </script>
</body>
</html>"""


def main(input_path: Path, output_path: Path) -> None:
    df = load_data(input_path)
    df = clean_product_names(df)
    df = df[(df["Total Man Hours"] > 0)]
    df = explode_operators(df)

    top_ops = get_top_operators(df, TOP_N)
    total_weeks = len(df["Start Date"].unique())

    oph_fig = build_oph_bar_chart(df, top_ops)
    heatmap_fig = build_operator_machine_heatmap(df, top_ops)
    trends_fig = build_operator_trends_fig(df, top_ops)

    operator_options = ["All"] + sorted(top_ops)
    operator_options_html = "\n".join(
        f'<option value="{op}">{op}</option>' for op in operator_options
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_operator_dashboard(oph_fig, heatmap_fig, trends_fig,
                                  operator_options_html, total_weeks),
        encoding="utf-8",
    )
    print(f"Wrote operator dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build operator productivity dashboard (local only).")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    main(args.input, args.output)
