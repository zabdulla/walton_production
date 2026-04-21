"""
Generate a local-only interactive payroll analysis dashboard.

Compares payroll clock hours against production-reported hours for each
pay period, highlighting unaccounted gaps and hidden labor overhead.

Usage:
    python src/build_payroll_dashboard.py

Output: reports/payroll.html  (gitignored, local only)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from config import (
    PROJECT_ROOT,
    DEFAULT_PAYROLL_DATA,
    EMPLOYEE_ROSTER_PATH,
    DEFAULT_AGGREGATED_DATA,
    LABOR_RATE,
    OT_MULTIPLIER_1,
    OT_MULTIPLIER_2,
)
from parse_payroll_pdf import compare_payroll_to_production, load_roster

DEFAULT_OUTPUT = PROJECT_ROOT / "reports" / "payroll.html"


def load_all_periods(payroll_path: Path) -> list[dict]:
    """Load aggregated payroll and return sorted unique (start, end) period pairs."""
    df = pd.read_excel(payroll_path)
    periods = (
        df.groupby(["period_start", "period_end"])
        .size()
        .reset_index(name="count")
    )
    periods["sort_key"] = pd.to_datetime(periods["period_start"])
    periods = periods.sort_values("sort_key")
    return [
        {"start": row["period_start"], "end": row["period_end"]}
        for _, row in periods.iterrows()
    ]


def build_period_data(
    periods: list[dict],
    payroll_path: Path,
    production_path: Path,
    roster_path: Path,
) -> list[dict]:
    """Run compare_payroll_to_production for each period, return JSON-ready data."""
    roster = load_roster(roster_path)
    employees_map = roster.get("employees", {})
    all_periods = []

    for p in periods:
        try:
            df = compare_payroll_to_production(
                p["start"], p["end"],
                payroll_path=payroll_path,
                production_path=production_path,
                roster_path=roster_path,
            )
        except (ValueError, FileNotFoundError) as exc:
            print(f"  Skipping {p['start']} - {p['end']}: {exc}")
            continue

        employees = df.to_dict(orient="records")

        # Build per-machine attribution from roster
        machine_data: dict[str, dict] = {}
        for emp in employees:
            info = employees_map.get(emp["employee_name"], {})
            aliases = info.get("production_aliases", [])
            primary = info.get("primary_machine")
            secondary = info.get("secondary_machines", [])
            all_machines = ([primary] if primary else []) + secondary

            for machine in all_machines:
                if machine not in machine_data:
                    machine_data[machine] = {
                        "machine": machine,
                        "production_hours": 0.0,
                        "workers": [],
                        "clock_hours_allocated": 0.0,
                    }

            # Attribute production hours to primary machine
            if primary and primary in machine_data:
                machine_data[primary]["production_hours"] += emp["production_hours"]

            # Add worker to their primary machine
            if primary and primary in machine_data:
                machine_data[primary]["workers"].append(emp["employee_name"])
                machine_data[primary]["clock_hours_allocated"] += emp["clock_total"]

        # Deduplicate worker lists
        for md in machine_data.values():
            md["workers"] = sorted(set(md["workers"]))

        machine_list = sorted(machine_data.values(), key=lambda x: x["machine"])

        period_record = {
            "period_start": p["start"],
            "period_end": p["end"],
            "employees": employees,
            "machines": machine_list,
        }
        all_periods.append(period_record)

    return all_periods


def format_period_label(start: str, end: str) -> str:
    """Format 'MM/DD/YYYY' pair as 'MM/DD - MM/DD/YYYY'."""
    try:
        s = pd.to_datetime(start)
        e = pd.to_datetime(end)
        return f"{s.strftime('%m/%d')} - {e.strftime('%m/%d/%Y')}"
    except Exception:
        return f"{start} - {end}"


def render_html(periods_json: str, period_labels_json: str, roster_json: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Payroll Analysis Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.3.min.js"></script>
  <style>
    :root {{ --bg:#f3f4f6; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; padding:24px; font-family:"Helvetica Neue",Arial,sans-serif;
            background:radial-gradient(circle at 20% 20%,#f9fafb 0,#eef2ff 40%,#f3f4f6 90%); color:var(--text); }}
    h1 {{ margin:0 0 4px; font-weight:700; }}
    .subtitle {{ margin:0 0 16px; color:var(--muted); font-size:14px; }}
    .badge {{ display:inline-block; background:#fef2f2; color:#dc2626; font-size:11px; font-weight:600;
              padding:2px 8px; border-radius:6px; margin-left:8px; vertical-align:middle; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:16px;
             box-shadow:0 10px 50px rgba(15,23,42,.08); padding:20px; margin-bottom:20px; }}
    .controls {{ display:flex; gap:20px; flex-wrap:wrap; align-items:flex-start; }}
    .control-group {{ flex:1; min-width:220px; }}
    .control-group label {{ display:block; font-weight:600; color:var(--muted); font-size:13px; margin-bottom:6px; }}
    select {{ padding:8px 10px; border-radius:8px; border:1px solid var(--border); background:#fff;
              min-width:280px; font-size:14px; }}
    .kpi-grid {{ display:grid; gap:12px; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); margin:16px 0; }}
    .kpi-card {{ background:#f8fafc; border:1px solid var(--border); border-radius:12px; padding:12px; text-align:center; }}
    .kpi-card.green {{ background:#f0fdf4; border-color:#bbf7d0; }}
    .kpi-card.blue {{ background:#eff6ff; border-color:#bfdbfe; }}
    .kpi-card.red {{ background:#fef2f2; border-color:#fecaca; }}
    .kpi-card.yellow {{ background:#fefce8; border-color:#fef08a; }}
    .kpi-label {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }}
    .kpi-value {{ font-size:22px; font-weight:700; margin-top:4px; }}
    .kpi-value.positive {{ color:#16a34a; }}
    .kpi-value.negative {{ color:#dc2626; }}
    .kpi-value.warning {{ color:#ca8a04; }}
    .charts-row {{ display:flex; gap:20px; flex-wrap:wrap; }}
    .charts-row > div {{ flex:1; min-width:300px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:12px; }}
    th {{ background:#f1f5f9; padding:8px 10px; text-align:right; font-weight:600; color:var(--muted);
         border-bottom:2px solid var(--border); font-size:12px; text-transform:uppercase; letter-spacing:0.3px; }}
    th:first-child {{ text-align:left; }}
    td {{ padding:7px 10px; text-align:right; border-bottom:1px solid #f1f5f9; }}
    td:first-child {{ text-align:left; font-weight:500; }}
    tr:hover td {{ background:#f8fafc; }}
    .section-title {{ font-size:16px; font-weight:700; margin:0 0 8px; }}
    .trend-section {{ display:none; }}
    .trend-section.visible {{ display:block; }}
    .trend-row {{ display:flex; gap:20px; flex-wrap:wrap; }}
    .trend-row > div {{ flex:1; min-width:400px; }}
  </style>
</head>
<body>
  <h1>Payroll Analysis <span class="badge">INTERNAL ONLY &mdash; Payroll Analysis</span></h1>
  <p class="subtitle">
    Clock hours vs production-reported hours per pay period.
    Labor rate: ${LABOR_RATE}/hr | OT1: {OT_MULTIPLIER_1}x | OT2: {OT_MULTIPLIER_2}x
  </p>

  <!-- Controls -->
  <div class="card">
    <div class="controls">
      <div class="control-group">
        <label>Pay Period</label>
        <select id="periodSelect"></select>
      </div>
    </div>
  </div>

  <!-- KPI Cards -->
  <div class="card">
    <div class="kpi-grid" id="kpis"></div>
  </div>

  <!-- Waterfall Chart -->
  <div class="card">
    <h2 class="section-title">Hour Flow Waterfall</h2>
    <div id="waterfallChart" style="height:450px;"></div>
  </div>

  <!-- Per-Employee Stacked Bars -->
  <div class="card">
    <h2 class="section-title">Per-Employee Hour Breakdown</h2>
    <div id="employeeChart" style="width:100%;"></div>
  </div>

  <!-- Machine Attribution Table -->
  <div class="card">
    <h2 class="section-title">Machine Attribution</h2>
    <div style="max-height:500px;overflow-y:auto;">
      <table>
        <thead>
          <tr>
            <th>Machine</th>
            <th>Production Hours</th>
            <th>Workers Assigned</th>
            <th>Clock Hours Allocated</th>
          </tr>
        </thead>
        <tbody id="machineTableBody"></tbody>
      </table>
    </div>
  </div>

  <!-- Trend Section (only shown when 3+ periods) -->
  <div class="card trend-section" id="trendSection">
    <h2 class="section-title">Trends Across Pay Periods</h2>
    <div class="trend-row">
      <div id="trendCaptureChart" style="height:400px;"></div>
      <div id="trendGapChart" style="height:400px;"></div>
    </div>
  </div>

  <script>
    const PERIODS = {periods_json};
    const LABELS = {period_labels_json};
    const ROSTER = {roster_json};
    const periodSelect = document.getElementById('periodSelect');

    // Populate period dropdown
    LABELS.forEach((lbl, i) => {{
      const opt = document.createElement('option');
      opt.value = i;
      opt.textContent = lbl;
      periodSelect.appendChild(opt);
    }});
    // Default to most recent period
    if (LABELS.length > 0) {{
      periodSelect.value = LABELS.length - 1;
    }}

    function fmt(n) {{ return n.toLocaleString('en-US', {{minimumFractionDigits:1, maximumFractionDigits:1}}); }}
    function fmtInt(n) {{ return n.toLocaleString('en-US', {{minimumFractionDigits:0, maximumFractionDigits:0}}); }}
    function fmtD(n) {{
      const abs = Math.abs(n);
      const s = abs.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});
      return (n < 0 ? '-$' : '$') + s;
    }}
    function fmtPct(n) {{ return n.toFixed(1) + '%'; }}

    function computeSummary(employees) {{
      let clockTotal = 0, ptoTotal = 0, workedTotal = 0;
      let prodTotal = 0, srTotal = 0, gapTotal = 0;
      let costClock = 0, costProd = 0;
      employees.forEach(e => {{
        clockTotal += e.clock_total;
        ptoTotal += e.pto_hours;
        workedTotal += e.worked_hours;
        prodTotal += e.production_hours;
        srTotal += e.sr_hours;
        gapTotal += e.gap_hours;
        costClock += e.labor_cost_clock;
        costProd += e.labor_cost_production;
      }});
      const available = clockTotal - ptoTotal;
      const captureRate = workedTotal > 0 ? (prodTotal / workedTotal * 100) : 0;
      const hiddenOverhead = costClock - costProd;
      return {{ clockTotal, ptoTotal, workedTotal, available, prodTotal, srTotal, gapTotal,
                captureRate, costClock, costProd, hiddenOverhead }};
    }}

    function updateKPIs(s) {{
      let captureClass = 'negative';
      let captureCardClass = 'red';
      if (s.captureRate >= 70) {{ captureClass = 'positive'; captureCardClass = 'green'; }}
      else if (s.captureRate >= 50) {{ captureClass = 'warning'; captureCardClass = 'yellow'; }}

      document.getElementById('kpis').innerHTML = `
        <div class="kpi-card">
          <div class="kpi-label">Total Clock Hours</div>
          <div class="kpi-value">${{fmt(s.clockTotal)}}</div>
        </div>
        <div class="kpi-card green">
          <div class="kpi-label">Productive Hours</div>
          <div class="kpi-value positive">${{fmt(s.prodTotal)}}</div>
        </div>
        <div class="kpi-card blue">
          <div class="kpi-label">S&amp;R Hours</div>
          <div class="kpi-value">${{fmt(s.srTotal)}}</div>
        </div>
        <div class="kpi-card red">
          <div class="kpi-label">Unaccounted Hours</div>
          <div class="kpi-value negative">${{fmt(s.gapTotal)}}</div>
        </div>
        <div class="kpi-card ${{captureCardClass}}">
          <div class="kpi-label">Capture Rate</div>
          <div class="kpi-value ${{captureClass}}">${{fmtPct(s.captureRate)}}</div>
        </div>
        <div class="kpi-card red">
          <div class="kpi-label">Hidden Overhead</div>
          <div class="kpi-value negative">${{fmtD(s.hiddenOverhead)}}</div>
        </div>
      `;
    }}

    function updateWaterfall(s) {{
      const trace = {{
        type: 'waterfall',
        orientation: 'v',
        x: ['Total Clock', '-PTO', 'Available', '-S&R', '-Productive', 'Unaccounted Gap'],
        y: [s.clockTotal, -s.ptoTotal, 0, -s.srTotal, -s.prodTotal, 0],
        measure: ['absolute', 'relative', 'total', 'relative', 'relative', 'total'],
        connector: {{ line: {{ color: '#e5e7eb' }} }},
        increasing: {{ marker: {{ color: '#22c55e' }} }},
        decreasing: {{ marker: {{ color: '#ef4444' }} }},
        totals: {{ marker: {{ color: '#3b82f6' }} }},
        textposition: 'outside',
        text: [
          fmt(s.clockTotal),
          '-' + fmt(s.ptoTotal),
          fmt(s.available),
          '-' + fmt(s.srTotal),
          '-' + fmt(s.prodTotal),
          fmt(s.gapTotal)
        ],
        hovertemplate: '%{{x}}: %{{y:.1f}} hrs<extra></extra>',
      }};
      const layout = {{
        title: 'Hour Flow: Clock to Unaccounted',
        yaxis: {{ title: 'Hours' }},
        template: 'plotly_white',
        plot_bgcolor: '#f9fafc', paper_bgcolor: '#fdfdff',
        font: {{ family: 'Helvetica, Arial, sans-serif', size: 13, color: '#1f2937' }},
        margin: {{ t: 60, r: 40, b: 60, l: 60 }},
        showlegend: false,
      }};
      Plotly.react('waterfallChart', [trace], layout, {{responsive: true}});
    }}

    function updateEmployeeChart(employees) {{
      // Sort by gap descending
      const sorted = [...employees].sort((a, b) => b.gap_hours - a.gap_hours);
      const names = sorted.map(e => e.employee_name);

      const traceProd = {{
        type: 'bar', orientation: 'h', name: 'Production',
        y: names, x: sorted.map(e => e.production_hours),
        marker: {{ color: '#22c55e' }},
        hovertemplate: '%{{y}}<br>Production: %{{x:.1f}} hrs<extra></extra>',
      }};
      const traceSR = {{
        type: 'bar', orientation: 'h', name: 'S&R',
        y: names, x: sorted.map(e => e.sr_hours),
        marker: {{ color: '#3b82f6' }},
        hovertemplate: '%{{y}}<br>S&R: %{{x:.1f}} hrs<extra></extra>',
      }};
      const tracePTO = {{
        type: 'bar', orientation: 'h', name: 'PTO',
        y: names, x: sorted.map(e => e.pto_hours),
        marker: {{ color: '#9ca3af' }},
        hovertemplate: '%{{y}}<br>PTO: %{{x:.1f}} hrs<extra></extra>',
      }};
      const traceGap = {{
        type: 'bar', orientation: 'h', name: 'Unaccounted',
        y: names, x: sorted.map(e => e.gap_hours),
        marker: {{ color: '#ef4444' }},
        hovertemplate: '%{{y}}<br>Unaccounted: %{{x:.1f}} hrs<extra></extra>',
      }};

      const chartHeight = Math.max(400, names.length * 32 + 120);
      const layout = {{
        title: 'Per-Employee Hour Breakdown (sorted by gap)',
        barmode: 'stack',
        xaxis: {{ title: 'Hours' }},
        yaxis: {{ autorange: 'reversed', dtick: 1 }},
        template: 'plotly_white',
        plot_bgcolor: '#f9fafc', paper_bgcolor: '#fdfdff',
        font: {{ family: 'Helvetica, Arial, sans-serif', size: 12, color: '#1f2937' }},
        legend: {{ orientation: 'h', x: 0.5, xanchor: 'center', y: 1.05 }},
        margin: {{ t: 60, r: 40, b: 60, l: 180 }},
        height: chartHeight,
      }};

      document.getElementById('employeeChart').style.height = chartHeight + 'px';
      Plotly.react('employeeChart', [traceProd, traceSR, tracePTO, traceGap], layout, {{responsive: true}});
    }}

    function updateMachineTable(machines) {{
      let html = '';
      const sorted = [...machines].sort((a, b) => b.production_hours - a.production_hours);
      sorted.forEach(m => {{
        html += `<tr>
          <td>${{m.machine}}</td>
          <td>${{fmt(m.production_hours)}}</td>
          <td style="text-align:left">${{m.workers.join(', ') || '&mdash;'}}</td>
          <td>${{fmt(m.clock_hours_allocated)}}</td>
        </tr>`;
      }});
      document.getElementById('machineTableBody').innerHTML = html;
    }}

    function updateTrends() {{
      const section = document.getElementById('trendSection');
      if (PERIODS.length < 3) {{
        section.classList.remove('visible');
        return;
      }}
      section.classList.add('visible');

      const labels = [];
      const captureRates = [];
      const gapHours = [];

      PERIODS.forEach((p, i) => {{
        const s = computeSummary(p.employees);
        labels.push(LABELS[i]);
        captureRates.push(s.captureRate);
        gapHours.push(s.gapTotal);
      }});

      // Capture rate line chart
      const captureTrace = {{
        x: labels, y: captureRates,
        type: 'scatter', mode: 'lines+markers',
        name: 'Capture Rate %',
        line: {{ color: '#16a34a', width: 3 }},
        marker: {{ size: 8, color: captureRates.map(r => r >= 70 ? '#16a34a' : (r >= 50 ? '#ca8a04' : '#dc2626')) }},
        hovertemplate: '%{{x}}<br>Capture: %{{y:.1f}}%<extra></extra>',
      }};
      const captureLayout = {{
        title: 'Capture Rate % Over Time',
        xaxis: {{ tickangle: 45 }},
        yaxis: {{ title: 'Capture Rate %', range: [0, 100] }},
        shapes: [
          {{ type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 70, y1: 70,
             line: {{ color: '#16a34a', width: 1.5, dash: 'dash' }} }},
          {{ type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 50, y1: 50,
             line: {{ color: '#ca8a04', width: 1.5, dash: 'dash' }} }},
        ],
        annotations: [
          {{ xref: 'paper', x: 1, yref: 'y', y: 70, text: '70% target',
             showarrow: false, font: {{ color: '#16a34a', size: 10 }}, xanchor: 'right', yshift: 8 }},
          {{ xref: 'paper', x: 1, yref: 'y', y: 50, text: '50% floor',
             showarrow: false, font: {{ color: '#ca8a04', size: 10 }}, xanchor: 'right', yshift: 8 }},
        ],
        template: 'plotly_white',
        plot_bgcolor: '#f9fafc', paper_bgcolor: '#fdfdff',
        font: {{ family: 'Helvetica, Arial, sans-serif', size: 12, color: '#1f2937' }},
        margin: {{ t: 60, r: 40, b: 100, l: 60 }},
        showlegend: false,
      }};
      Plotly.react('trendCaptureChart', [captureTrace], captureLayout, {{responsive: true}});

      // Gap hours bar chart
      const gapTrace = {{
        x: labels, y: gapHours, type: 'bar',
        name: 'Unaccounted Hours',
        marker: {{ color: '#ef4444' }},
        text: gapHours.map(h => fmt(h)),
        textposition: 'outside',
        hovertemplate: '%{{x}}<br>Gap: %{{y:.1f}} hrs<extra></extra>',
      }};
      const gapLayout = {{
        title: 'Total Unaccounted Hours per Period',
        xaxis: {{ tickangle: 45 }},
        yaxis: {{ title: 'Hours' }},
        template: 'plotly_white',
        plot_bgcolor: '#f9fafc', paper_bgcolor: '#fdfdff',
        font: {{ family: 'Helvetica, Arial, sans-serif', size: 12, color: '#1f2937' }},
        margin: {{ t: 60, r: 40, b: 100, l: 60 }},
        showlegend: false,
      }};
      Plotly.react('trendGapChart', [gapTrace], gapLayout, {{responsive: true}});
    }}

    function update() {{
      const idx = parseInt(periodSelect.value, 10);
      const period = PERIODS[idx];
      if (!period) return;

      const s = computeSummary(period.employees);

      updateKPIs(s);
      updateWaterfall(s);
      updateEmployeeChart(period.employees);
      updateMachineTable(period.machines);
      updateTrends();
    }}

    periodSelect.addEventListener('change', update);
    update();
  </script>
</body>
</html>"""


def main(output_path: Path) -> None:
    print("Loading payroll periods...")
    periods = load_all_periods(DEFAULT_PAYROLL_DATA)
    print(f"  Found {len(periods)} pay periods")

    if not periods:
        print("ERROR: No payroll data found. Parse PDFs first with parse_payroll_pdf.py --pdf")
        return

    print("Running payroll vs production comparison for each period...")
    period_data = build_period_data(
        periods,
        payroll_path=DEFAULT_PAYROLL_DATA,
        production_path=DEFAULT_AGGREGATED_DATA,
        roster_path=EMPLOYEE_ROSTER_PATH,
    )
    print(f"  Processed {len(period_data)} periods successfully")

    # Build period labels for the dropdown
    period_labels = [
        format_period_label(p["period_start"], p["period_end"])
        for p in period_data
    ]

    # Load roster for embedding
    roster = load_roster(EMPLOYEE_ROSTER_PATH)

    # Render
    periods_json = json.dumps(period_data, default=str)
    labels_json = json.dumps(period_labels)
    roster_json = json.dumps(roster.get("employees", {}), default=str)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_html(periods_json, labels_json, roster_json),
        encoding="utf-8",
    )
    print(f"Wrote payroll dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build payroll analysis dashboard.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    main(args.output)
