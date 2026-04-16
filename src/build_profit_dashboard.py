"""
Generate a local-only interactive profit margin dashboard.

Sliders for sale price, buy price, and overhead let users explore
profit scenarios per machine using actual production data.

Usage:
    python src/build_profit_dashboard.py

Output: reports/profit.html  (gitignored, local only)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from config import PROJECT_ROOT, DEFAULT_AGGREGATED_DATA, LABOR_RATE, MACHINE_PRESETS, DEFAULT_PRESET

DEFAULT_INPUT = DEFAULT_AGGREGATED_DATA
DEFAULT_OUTPUT = PROJECT_ROOT / "reports" / "profit.html"


def load_and_aggregate(path: Path) -> list[dict]:
    """Load daily data and aggregate weekly by machine."""
    df = pd.read_excel(path)
    weekly = (
        df.groupby(["Machine_Name", "Week_Start"])
        .agg(
            output_lbs=("Actual_Output", "sum"),
            input_lbs=("Actual_Input", "sum"),
            man_hours=("Man_Hours", "sum"),
            machine_hours=("Machine_Hours", "sum"),
        )
        .reset_index()
    )
    weekly["Week_Start"] = pd.to_datetime(weekly["Week_Start"])
    weekly = weekly.sort_values(["Machine_Name", "Week_Start"])
    weekly["week_label"] = weekly["Week_Start"].dt.strftime("%Y-%m-%d")
    weekly["labor_cost"] = weekly["man_hours"] * LABOR_RATE

    records = weekly[
        ["Machine_Name", "week_label", "output_lbs", "input_lbs",
         "man_hours", "machine_hours", "labor_cost"]
    ].to_dict(orient="records")
    return records


def render_html(data_json: str, machines: list[str], presets_json: str, total_weeks: int) -> str:
    machine_options = "\n".join(
        f'<option value="{m}">{m}</option>'
        for m in ["All Machines"] + machines
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Profit Margin Simulator</title>
  <script src="https://cdn.plot.ly/plotly-2.35.3.min.js"></script>
  <style>
    :root {{ --bg:#f3f4f6; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; padding:24px; font-family:"Helvetica Neue",Arial,sans-serif;
            background:radial-gradient(circle at 20% 20%,#f9fafb 0,#eef2ff 40%,#f3f4f6 90%); color:var(--text); }}
    h1 {{ margin:0 0 4px; font-weight:700; }}
    .subtitle {{ margin:0 0 16px; color:var(--muted); font-size:14px; }}
    .badge {{ display:inline-block; background:#fef3c7; color:#92400e; font-size:11px; font-weight:600;
              padding:2px 8px; border-radius:6px; margin-left:8px; vertical-align:middle; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:16px;
             box-shadow:0 10px 50px rgba(15,23,42,.08); padding:20px; margin-bottom:20px; }}
    .controls {{ display:flex; gap:20px; flex-wrap:wrap; align-items:flex-start; }}
    .control-group {{ flex:1; min-width:220px; }}
    .control-group label {{ display:block; font-weight:600; color:var(--muted); font-size:13px; margin-bottom:6px; }}
    select {{ padding:8px 10px; border-radius:8px; border:1px solid var(--border); background:#fff;
              min-width:200px; font-size:14px; }}
    .slider-row {{ display:flex; align-items:center; gap:12px; margin-bottom:4px; }}
    .slider-row input[type=range] {{
      flex:1; -webkit-appearance:none; appearance:none;
      height:8px; border-radius:4px; background:#e2e8f0; outline:none;
      cursor:pointer; transition:background .2s;
    }}
    .slider-row input[type=range]:hover {{ background:#cbd5e1; }}
    .slider-row input[type=range]::-webkit-slider-thumb {{
      -webkit-appearance:none; appearance:none;
      width:22px; height:22px; border-radius:50%;
      background:linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
      border:3px solid #fff; box-shadow:0 2px 6px rgba(37,99,235,.35);
      cursor:grab; transition:transform .15s, box-shadow .15s;
    }}
    .slider-row input[type=range]::-webkit-slider-thumb:hover {{
      transform:scale(1.15); box-shadow:0 3px 10px rgba(37,99,235,.45);
    }}
    .slider-row input[type=range]::-webkit-slider-thumb:active {{
      cursor:grabbing; transform:scale(1.05);
      background:linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
    }}
    .slider-row input[type=range]::-moz-range-thumb {{
      width:22px; height:22px; border-radius:50%;
      background:linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
      border:3px solid #fff; box-shadow:0 2px 6px rgba(37,99,235,.35);
      cursor:grab; transition:transform .15s, box-shadow .15s;
    }}
    .slider-row input[type=range]::-moz-range-thumb:hover {{
      transform:scale(1.15); box-shadow:0 3px 10px rgba(37,99,235,.45);
    }}
    .slider-row input[type=range]::-moz-range-track {{
      height:8px; border-radius:4px; background:#e2e8f0;
    }}
    .slider-val {{
      font-weight:700; min-width:64px; text-align:center; font-size:15px;
      border:1px solid var(--border); border-radius:8px;
      padding:4px 8px; transition:background .25s, color .25s, border-color .25s;
    }}
    .slider-val.val-ok {{ background:#f1f5f9; color:#1e40af; border-color:var(--border); }}
    .slider-val.val-warn {{ background:#fef2f2; color:#dc2626; border-color:#fca5a5; }}
    .slider-bounds {{ display:flex; justify-content:space-between; font-size:11px; color:#94a3b8; margin-top:-2px; padding:0 2px; }}
    .range-control {{ display:flex; align-items:center; gap:6px; margin-left:auto; }}
    .range-control label {{ font-weight:600; color:var(--muted); font-size:13px; }}
    .range-btns {{ display:flex; gap:0; }}
    .range-btn {{ padding:6px 12px; border:1px solid var(--border); background:#fff; cursor:pointer;
                  font-size:12px; transition:all .2s; }}
    .range-btn:first-child {{ border-radius:8px 0 0 8px; }}
    .range-btn:last-child {{ border-radius:0 8px 8px 0; }}
    .range-btn.active {{ background:#3b82f6; color:#fff; border-color:#3b82f6; }}
    .range-btn:hover:not(.active) {{ background:#f3f4f6; }}
    .kpi-grid {{ display:grid; gap:12px; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); margin:16px 0; }}
    .kpi-card {{ background:#f8fafc; border:1px solid var(--border); border-radius:12px; padding:12px; text-align:center; }}
    .kpi-label {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }}
    .kpi-value {{ font-size:22px; font-weight:700; margin-top:4px; }}
    .kpi-value.positive {{ color:#16a34a; }}
    .kpi-value.negative {{ color:#dc2626; }}
    .charts-row {{ display:flex; gap:20px; flex-wrap:wrap; }}
    .charts-row > div {{ flex:1; min-width:300px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:12px; }}
    th {{ background:#f1f5f9; padding:8px 10px; text-align:right; font-weight:600; color:var(--muted);
         border-bottom:2px solid var(--border); font-size:12px; text-transform:uppercase; letter-spacing:0.3px; }}
    th:first-child {{ text-align:left; }}
    td {{ padding:7px 10px; text-align:right; border-bottom:1px solid #f1f5f9; }}
    td:first-child {{ text-align:left; font-weight:500; }}
    tr:hover td {{ background:#f8fafc; }}
    .profit-pos {{ color:#16a34a; font-weight:600; }}
    .profit-neg {{ color:#dc2626; font-weight:600; }}
  </style>
</head>
<body>
  <h1>Profit Margin Simulator <span class="badge">INTERNAL ONLY</span></h1>
  <p class="subtitle">Adjust pricing assumptions to explore profit scenarios per machine. Labor rate: ${LABOR_RATE}/hr.</p>

  <div class="card">
    <div class="controls">
      <div class="control-group">
        <label>Machine</label>
        <select id="machineSelect">{machine_options}</select>
      </div>
      <div class="control-group">
        <label>Sale Price ($/lb)</label>
        <div class="slider-row">
          <input type="range" id="saleSlider" min="-0.10" max="0.50" step="0.01" value="0.20"/>
          <span class="slider-val val-ok" id="saleVal">$0.20</span>
        </div>
        <div class="slider-bounds"><span>-$0.10</span><span>$0.50</span></div>
      </div>
      <div class="control-group">
        <label>Feedstock Buy Price ($/lb)</label>
        <div class="slider-row">
          <input type="range" id="buySlider" min="-0.10" max="0.50" step="0.01" value="0.05"/>
          <span class="slider-val val-ok" id="buyVal">$0.05</span>
        </div>
        <div class="slider-bounds"><span>-$0.10</span><span>$0.50</span></div>
      </div>
      <div class="control-group">
        <label>Overhead ($/lb)</label>
        <div class="slider-row">
          <input type="range" id="overheadSlider" min="0.01" max="0.10" step="0.01" value="0.03"/>
          <span class="slider-val val-ok" id="overheadVal">$0.03</span>
        </div>
        <div class="slider-bounds"><span>$0.01</span><span>$0.10</span></div>
      </div>
      <div class="range-control">
        <label>Range:</label>
        <div class="range-btns">
          <button class="range-btn" data-weeks="12">12w</button>
          <button class="range-btn active" data-weeks="20">20w</button>
          <button class="range-btn" data-weeks="52">1y</button>
          <button class="range-btn" data-weeks="{total_weeks}">All</button>
        </div>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="kpi-grid" id="kpis"></div>
  </div>

  <div class="card">
    <div class="charts-row">
      <div id="profitChart" style="height:500px;"></div>
      <div id="costDonut" style="height:500px;max-width:350px;"></div>
    </div>
  </div>

  <!-- All Machines comparison (hidden when single machine selected) -->
  <div class="card" id="comparisonCard" style="display:none;">
    <div id="comparisonChart" style="width:100%;height:450px;"></div>
  </div>

  <div class="card" id="tableCard">
    <h2 style="margin-top:0;font-size:16px;">Weekly Breakdown</h2>
    <div style="max-height:500px;overflow-y:auto;">
      <table>
        <thead>
          <tr>
            <th>Week</th>
            <th>Output (lbs)</th>
            <th>Input (lbs)</th>
            <th>Man Hrs</th>
            <th>Revenue</th>
            <th>Feedstock</th>
            <th>Labor</th>
            <th>Overhead</th>
            <th>Profit</th>
            <th>Margin %</th>
          </tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
  </div>

  <script>
    const DATA = {data_json};
    const PRESETS = {presets_json};
    const TOTAL_WEEKS = {total_weeks};
    const machineSelect = document.getElementById('machineSelect');
    const saleSlider = document.getElementById('saleSlider');
    const buySlider = document.getElementById('buySlider');
    const overheadSlider = document.getElementById('overheadSlider');
    const saleVal = document.getElementById('saleVal');
    const buyVal = document.getElementById('buyVal');
    const overheadVal = document.getElementById('overheadVal');
    const rangeBtns = document.querySelectorAll('.range-btn');
    let rangeWeeks = 20;

    function fmt(n) {{ return n.toLocaleString('en-US', {{minimumFractionDigits:0, maximumFractionDigits:0}}); }}
    function fmtD(n) {{
      const abs = Math.abs(n);
      const s = abs.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});
      return (n < 0 ? '-$' : '$') + s;
    }}
    function fmtPct(n) {{ return n.toFixed(1) + '%'; }}
    function fmtSlider(v) {{ return (v < 0 ? '-$' + Math.abs(v).toFixed(2) : '$' + v.toFixed(2)); }}

    // Feature 4: Per-machine preset defaults
    machineSelect.addEventListener('change', () => {{
      const m = machineSelect.value;
      if (m !== 'All Machines' && PRESETS[m]) {{
        const [s, b, o] = PRESETS[m];
        saleSlider.value = s; buySlider.value = b; overheadSlider.value = o;
      }}
      update();
    }});

    // Feature 6: Time range buttons
    rangeBtns.forEach(btn => {{
      btn.addEventListener('click', () => {{
        rangeBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        rangeWeeks = parseInt(btn.dataset.weeks, 10);
        update();
      }});
    }});

    function calcRow(r, salePrice, buyPrice, overhead) {{
      const revenue = r.output_lbs * salePrice;
      const feedstock = r.input_lbs * buyPrice;
      const labor = r.labor_cost;
      const oh = r.output_lbs * overhead;
      const profit = revenue - feedstock - labor - oh;
      const margin = revenue !== 0 ? (profit / revenue) * 100 : 0;
      return {{ revenue, feedstock, labor, oh, profit, margin }};
    }}

    function update() {{
      const machine = machineSelect.value;
      const salePrice = parseFloat(saleSlider.value);
      const buyPrice = parseFloat(buySlider.value);
      const overhead = parseFloat(overheadSlider.value);

      saleVal.textContent = fmtSlider(salePrice);
      buyVal.textContent = fmtSlider(buyPrice);
      overheadVal.textContent = fmtSlider(overhead);

      const isAll = machine === 'All Machines';

      // Get data rows (aggregate across machines for "All")
      let rows;
      if (isAll) {{
        const byWeek = {{}};
        DATA.forEach(r => {{
          if (!byWeek[r.week_label]) byWeek[r.week_label] = {{week_label:r.week_label, output_lbs:0, input_lbs:0, man_hours:0, machine_hours:0, labor_cost:0}};
          const w = byWeek[r.week_label];
          w.output_lbs += r.output_lbs; w.input_lbs += r.input_lbs;
          w.man_hours += r.man_hours; w.machine_hours += r.machine_hours;
          w.labor_cost += r.labor_cost;
        }});
        rows = Object.values(byWeek).sort((a,b) => a.week_label.localeCompare(b.week_label));
      }} else {{
        rows = DATA.filter(d => d.Machine_Name === machine);
      }}

      // Feature 6: Apply time range filter
      if (rangeWeeks < TOTAL_WEEKS && rows.length > rangeWeeks) {{
        rows = rows.slice(-rangeWeeks);
      }}

      const weeks = [], profits = [], margins = [], colors = [];
      let totalRevenue = 0, totalFeedstock = 0, totalLabor = 0, totalOverhead = 0, totalProfit = 0;

      let tableHtml = '';
      rows.forEach(r => {{
        const c = calcRow(r, salePrice, buyPrice, overhead);
        weeks.push(r.week_label);
        profits.push(c.profit);
        margins.push(c.margin);
        colors.push(c.profit >= 0 ? '#22c55e' : '#ef4444');

        totalRevenue += c.revenue; totalFeedstock += c.feedstock;
        totalLabor += c.labor; totalOverhead += c.oh; totalProfit += c.profit;

        const cls = c.profit >= 0 ? 'profit-pos' : 'profit-neg';
        tableHtml += `<tr>
          <td>${{r.week_label}}</td>
          <td>${{fmt(r.output_lbs)}}</td>
          <td>${{fmt(r.input_lbs)}}</td>
          <td>${{r.man_hours.toFixed(1)}}</td>
          <td>${{fmtD(c.revenue)}}</td>
          <td>${{fmtD(c.feedstock)}}</td>
          <td>${{fmtD(c.labor)}}</td>
          <td>${{fmtD(c.oh)}}</td>
          <td class="${{cls}}">${{fmtD(c.profit)}}</td>
          <td class="${{cls}}">${{fmtPct(c.margin)}}</td>
        </tr>`;
      }});

      document.getElementById('tableBody').innerHTML = tableHtml;

      // KPIs
      const totalMargin = totalRevenue !== 0 ? (totalProfit / totalRevenue) * 100 : 0;
      const avgWeeklyProfit = rows.length > 0 ? totalProfit / rows.length : 0;
      const marginCls = totalMargin >= 0 ? 'positive' : 'negative';
      const profitCls = totalProfit >= 0 ? 'positive' : 'negative';

      document.getElementById('kpis').innerHTML = `
        <div class="kpi-card"><div class="kpi-label">Total Revenue</div><div class="kpi-value">${{fmtD(totalRevenue)}}</div></div>
        <div class="kpi-card"><div class="kpi-label">Total Cost</div><div class="kpi-value">${{fmtD(totalFeedstock + totalLabor + totalOverhead)}}</div></div>
        <div class="kpi-card"><div class="kpi-label">Total Profit</div><div class="kpi-value ${{profitCls}}">${{fmtD(totalProfit)}}</div></div>
        <div class="kpi-card"><div class="kpi-label">Avg Margin</div><div class="kpi-value ${{marginCls}}">${{fmtPct(totalMargin)}}</div></div>
        <div class="kpi-card"><div class="kpi-label">Avg Weekly Profit</div><div class="kpi-value ${{profitCls}}">${{fmtD(avgWeeklyProfit)}}</div></div>
        <div class="kpi-card"><div class="kpi-label">Weeks</div><div class="kpi-value">${{rows.length}}</div></div>
      `;

      // Feature 3: Sensitivity highlight — color slider badges red when overall margin is negative
      const allValBadges = [saleVal, buyVal, overheadVal];
      allValBadges.forEach(el => {{
        el.classList.toggle('val-warn', totalMargin < 0);
        el.classList.toggle('val-ok', totalMargin >= 0);
      }});

      // Feature 1: Breakeven line + profit chart
      const trace1 = {{
        x: weeks, y: profits, type: 'bar', name: 'Profit ($)',
        marker: {{ color: colors }},
        hovertemplate: 'Week: %{{x}}<br>Profit: %{{y:$,.0f}}<extra></extra>',
      }};
      const trace2 = {{
        x: weeks, y: margins, type: 'scatter', mode: 'lines+markers',
        name: 'Margin %', yaxis: 'y2',
        line: {{ color: '#6366f1', width: 2 }}, marker: {{ size: 5 }},
        hovertemplate: 'Margin: %{{y:.1f}}%<extra></extra>',
      }};
      const label = isAll ? 'All Machines' : machine;
      const layout = {{
        title: label + ' — Weekly Profit & Margin',
        xaxis: {{ title: 'Week', tickangle: 45 }},
        yaxis: {{ title: 'Profit ($)', side: 'left', zeroline: true, zerolinecolor: '#ef4444', zerolinewidth: 2 }},
        yaxis2: {{ title: 'Margin %', side: 'right', overlaying: 'y', zeroline: true, zerolinecolor: '#e5e7eb' }},
        shapes: [{{
          type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 0, y1: 0,
          line: {{ color: '#ef4444', width: 2, dash: 'dash' }},
        }}],
        annotations: [{{
          xref: 'paper', x: 1.0, yref: 'y', y: 0, text: 'Breakeven',
          showarrow: false, font: {{ color: '#ef4444', size: 11 }},
          xanchor: 'right', yanchor: 'bottom', yshift: 2,
        }}],
        template: 'plotly_white',
        plot_bgcolor: '#f9fafc', paper_bgcolor: '#fdfdff',
        font: {{ family: 'Helvetica, Arial, sans-serif', size: 13, color: '#1f2937' }},
        legend: {{ orientation: 'h', x: 0.5, xanchor: 'center', y: 1.12 }},
        margin: {{ t: 80, r: 60, b: 100, l: 80 }},
        hovermode: 'x unified',
      }};
      Plotly.react('profitChart', [trace1, trace2], layout, {{responsive: true}});

      // Feature 2: Cost breakdown donut
      const absFeedstock = Math.abs(totalFeedstock);
      const absLabor = Math.abs(totalLabor);
      const absOverhead = Math.abs(totalOverhead);
      const donutTrace = {{
        values: [absFeedstock, absLabor, absOverhead],
        labels: ['Feedstock', 'Labor', 'Overhead'],
        type: 'pie', hole: 0.55,
        marker: {{ colors: ['#f59e0b', '#3b82f6', '#8b5cf6'] }},
        textinfo: 'label+percent',
        textposition: 'outside',
        hovertemplate: '%{{label}}: %{{value:$,.0f}} (%{{percent}})<extra></extra>',
      }};
      const donutLayout = {{
        title: 'Cost Breakdown',
        showlegend: false,
        template: 'plotly_white', paper_bgcolor: '#fdfdff',
        font: {{ family: 'Helvetica, Arial, sans-serif', size: 12, color: '#1f2937' }},
        margin: {{ t: 60, r: 20, b: 20, l: 20 }},
        annotations: [{{
          text: fmtD(absFeedstock + absLabor + absOverhead),
          showarrow: false, font: {{ size: 16, color: '#374151', family: 'Helvetica, Arial, sans-serif' }},
          x: 0.5, y: 0.5, xref: 'paper', yref: 'paper',
        }}],
      }};
      Plotly.react('costDonut', [donutTrace], donutLayout, {{responsive: true}});

      // Feature 5: All Machines comparison bar chart
      const compCard = document.getElementById('comparisonCard');
      const tableCard = document.getElementById('tableCard');
      if (isAll) {{
        compCard.style.display = '';
        tableCard.style.display = 'none';

        // Build per-machine profit totals
        const machineNames = [...new Set(DATA.map(d => d.Machine_Name))].sort();
        const machineProfits = [];
        const machineColors = [];
        machineNames.forEach(mn => {{
          let mRows = DATA.filter(d => d.Machine_Name === mn);
          if (rangeWeeks < TOTAL_WEEKS) {{
            mRows = mRows.slice(-rangeWeeks);
          }}
          let mp = 0;
          mRows.forEach(r => {{ mp += calcRow(r, salePrice, buyPrice, overhead).profit; }});
          machineProfits.push(mp);
          machineColors.push(mp >= 0 ? '#22c55e' : '#ef4444');
        }});

        const compTrace = {{
          x: machineNames, y: machineProfits, type: 'bar',
          marker: {{ color: machineColors }},
          text: machineProfits.map(p => fmtD(p)),
          textposition: 'outside',
          hovertemplate: '%{{x}}<br>Profit: %{{y:$,.0f}}<extra></extra>',
        }};
        const compLayout = {{
          title: 'Total Profit by Machine — All Machines',
          xaxis: {{ title: '' }},
          yaxis: {{ title: 'Total Profit ($)', zeroline: true, zerolinecolor: '#ef4444', zerolinewidth: 2 }},
          shapes: [{{
            type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 0, y1: 0,
            line: {{ color: '#ef4444', width: 1.5, dash: 'dash' }},
          }}],
          template: 'plotly_white', plot_bgcolor: '#f9fafc', paper_bgcolor: '#fdfdff',
          font: {{ family: 'Helvetica, Arial, sans-serif', size: 13, color: '#1f2937' }},
          margin: {{ t: 60, r: 20, b: 80, l: 80 }},
          showlegend: false,
        }};
        Plotly.react('comparisonChart', [compTrace], compLayout, {{responsive: true}});
      }} else {{
        compCard.style.display = 'none';
        tableCard.style.display = '';
      }}
    }}

    saleSlider.addEventListener('input', update);
    buySlider.addEventListener('input', update);
    overheadSlider.addEventListener('input', update);

    update();
  </script>
</body>
</html>"""


def main(input_path: Path, output_path: Path) -> None:
    records = load_and_aggregate(input_path)
    machines = sorted(set(r["Machine_Name"] for r in records))
    data_json = json.dumps(records, default=str)
    presets_json = json.dumps(MACHINE_PRESETS)
    total_weeks = len(set(r["week_label"] for r in records))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_html(data_json, machines, presets_json, total_weeks),
        encoding="utf-8",
    )
    print(f"Wrote profit dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build profit margin simulator dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    main(args.input, args.output)
