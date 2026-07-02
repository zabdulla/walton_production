"""
Page template for the interactive weekly dashboard (docs/index.html).

Extracted from build_interactive_dashboard.py so the page shell (CSS/HTML/JS)
and the data/figure pipeline can evolve independently. The builder computes
all figures and HTML fragments; render_dashboard() assembles the page.
"""
from __future__ import annotations

import plotly.graph_objects as go
from plotly.io import to_html

from config import ALL_METRICS, DEFAULT_WEEKS, RUNNING_AVG_WINDOW
from dashboard_common import (
    BASE_CSS, CARD_CSS, PLOTLY_CONFIG, MOBILE_MODEBAR_CSS, MOBILE_PLOTLY_JS,
    SHIFT_METRICS,
)


def render_dashboard(
    trends_std: str, trends_sup: str,
    rag_std: str, rag_sup: str,
    fig_sections_std: list, fig_sections_sup: list,
    machine_options_html: str,
    metric_options_html: str,
    snapshot_std: str, snapshot_sup: str,
    monthly_std: str, monthly_sup: str,
    shift_fig_std: go.Figure = None, shift_fig_sup: go.Figure = None,
    total_weeks: int = 20,
    latest_data_date: str = "",
) -> str:
    def _render_figs(fig_sections):
        rendered = [
            (title, to_html(fig, include_plotlyjs=False, full_html=False,
                            default_width="100%", default_height="600px", div_id=fig_id,
                            config=PLOTLY_CONFIG))
            for title, fig_id, fig in fig_sections
        ]
        return "\n".join(
            f'<section class="card"><h2 style="margin-top:0">{title}</h2>{html}</section>'
            for title, html in rendered
        )
    sections_std = _render_figs(fig_sections_std)
    sections_sup = _render_figs(fig_sections_sup)

    def _render_shift(fig, div_id):
        if fig is None or not fig.data:
            return ""
        html = to_html(fig, include_plotlyjs=False, full_html=False,
                        default_width="100%", default_height="500px", div_id=div_id,
                        config=PLOTLY_CONFIG)
        metric_btns = ''.join(
            f'<button class="range-btn shift-metric-btn{" active" if m == "Output/Hr" else ""}" data-metric="{m}">{m}</button>'
            for m in SHIFT_METRICS
        )
        return f'''<section class="card">
            <h2 style="margin-top:0;display:inline-block;">Shift Comparison</h2>
            <div class="range-btns" style="display:inline-flex;margin-left:16px;vertical-align:middle;">{metric_btns}</div>
            {html}
        </section>'''
    shift_section_std = _render_shift(shift_fig_std, "fig-shift")
    shift_section_sup = _render_shift(shift_fig_sup, "fig-shift-sup")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Processing Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.3.min.js"></script>
  <style>
{BASE_CSS}
{CARD_CSS}
    .kpi-grid {{ display:grid; gap:12px; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); margin:8px 0; }}
    .kpi-card {{ background:#f8fafc; border:1px solid var(--border); border-radius:12px; padding:12px; }}
    .kpi-label {{ color:var(--muted); font-size:12px; }}
    .kpi-value {{ font-size:20px; font-weight:700; margin-top:4px; }}
    .controls {{ display:flex; gap:12px; flex-wrap:wrap; margin-bottom:16px; align-items:center; }}
    .controls label {{ font-weight:600; color:var(--muted); margin-right:4px; }}
    select {{ padding:8px 10px; border-radius:8px; border:1px solid var(--border); background:#fff; min-width:160px; }}
    .toggle-btn {{ padding:7px 14px; border-radius:8px; border:1px solid var(--border); background:#fff;
                   cursor:pointer; font-size:13px; transition:all .2s; }}
    .toggle-btn.active {{ background:var(--brand); color:#fff; border-color:var(--brand); }}
    .toggle-btn:hover {{ background:#f3f4f6; }}
    .toggle-btn.active:hover {{ background:var(--brand-strong); }}
    .range-control {{ display:flex; align-items:center; gap:6px; }}
    .range-control label {{ font-weight:600; color:var(--muted); }}
    .range-btns {{ display:flex; gap:0; }}
    .range-btn {{ padding:6px 12px; border:1px solid var(--border); background:#fff; cursor:pointer;
                  font-size:12px; transition:all .2s; }}
    .range-btn:first-child {{ border-radius:8px 0 0 8px; }}
    .range-btn:last-child {{ border-radius:0 8px 8px 0; }}
    .range-btn:not(:first-child) {{ border-left:none; }}
    .range-btn.active {{ background:var(--brand); color:#fff; border-color:var(--brand); }}
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
    .nav-link {{ display:inline-block; padding:8px 16px; background:var(--brand); color:#fff;
                 text-decoration:none; border-radius:6px; font-size:14px; margin-bottom:16px; }}
    .nav-link:hover {{ background:var(--brand-strong); }}
    .rag-dots {{ margin-top:8px; line-height:1; }}
    .rag-dot {{ display:inline-block; width:11px; height:11px; border-radius:50%; margin-right:4px; }}
    .date-input {{ padding:6px 8px; border-radius:8px; border:1px solid var(--border); background:#fff;
                   font-size:12px; min-width:unset; width:auto; }}
    #clearCustomBtn {{ display:none; }}
    #clearCustomBtn.visible {{ display:inline-block; }}
    @media (max-width:768px) {{
      body {{ padding:12px; }}
      .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
      .kpi-value {{ font-size:16px; }}
      .controls {{ flex-direction:column; gap:8px; }}
      select {{ width:100%; min-width:unset; }}
      h1 {{ font-size:1.5rem; }}
      .card {{ padding:12px; border-radius:12px; overflow-x:auto; }}
    }}
    @media (max-width:480px) {{ .kpi-grid {{ grid-template-columns:1fr; }} table {{ font-size:12px; }} }}
{MOBILE_MODEBAR_CSS}
    @media print {{ .controls,.export-buttons,.toggle-btn {{ display:none; }}
      .card {{ break-inside:avoid; }} body {{ background:#fff; padding:0; }} }}
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">Walton Logistics &mdash; Production</p>
    <h1>Processing Performance Dashboard</h1>
    <p class="subtitle">Use controls below to adjust view. {total_weeks} weeks of data available.</p>
    <a href="daily.html" class="nav-link">View Daily Details</a>
  </header>
  <div id="staleBanner" style="display:none;background:#fef2f2;border:1px solid #dc2626;color:#991b1b;padding:10px 16px;border-radius:8px;margin:12px 0;font-weight:600;"></div>
  <script>
    // Warn when the newest data week is old — the weekly update may have
    // silently stopped running. Date injected at build time.
    (function checkStale() {{
      const latest = "{latest_data_date}";
      if (!latest) return;
      const ageDays = Math.floor((Date.now() - new Date(latest + 'T00:00:00').getTime()) / 86400000);
      if (ageDays > 13) {{  // weekly data: latest week start can be ~7 days old normally
        const b = document.getElementById('staleBanner');
        b.textContent = '⚠ Data may be stale: latest week starts ' + latest +
          ' (' + ageDays + ' days ago). The weekly update may not have run.';
        b.style.display = '';
      }}
    }})();
  </script>
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
      <div class="range-control">
        <label for="rangeFrom">Custom:</label>
        <input type="date" id="rangeFrom" class="date-input" title="Start date"/>
        <span class="muted">to</span>
        <input type="date" id="rangeTo" class="date-input" title="End date"/>
        <button class="toggle-btn" id="clearCustomBtn" title="Clear custom date range">&#10005;</button>
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
      <section class="card">
        <h2 style="margin-top:0">Target Performance</h2>
        {rag_std}
      </section>
      {sections_std}
      {shift_section_std}
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
      <section class="card">
        <h2 style="margin-top:0">Target Performance <span style="font-size:13px;color:var(--muted);font-weight:400;">(incl. Guillotine support)</span></h2>
        {rag_sup}
      </section>
      {sections_sup}
      {shift_section_sup}
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
    const rangeBtns = document.querySelectorAll('.range-btn:not(.shift-metric-btn)');
    const shiftMetricBtns = document.querySelectorAll('.shift-metric-btn');
    let showRaw = false;
    let includeSupport = false;
    let supportInitialized = false;
    let rangeWeeks = {DEFAULT_WEEKS};
    let shiftMetric = 'Output/Hr';
    let customRange = null;  // [fromISO, toISO] — overrides preset weeks when set
    const rangeFrom = document.getElementById('rangeFrom');
    const rangeTo = document.getElementById('rangeTo');
    const clearCustomBtn = document.getElementById('clearCustomBtn');

    function getMetricsFig() {{
      return document.getElementById(includeSupport ? 'fig-metrics-sup' : 'fig-metrics');
    }}
    function getTargetsFig() {{
      return document.getElementById(includeSupport ? 'fig-targets-sup' : 'fig-targets');
    }}
    function getShiftFig() {{
      return document.getElementById(includeSupport ? 'fig-shift-sup' : 'fig-shift');
    }}

    // Compute x-axis date range: custom from/to dates win over preset weeks
    function getXRange(fig) {{
      if (customRange) return customRange;
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

    // Range buttons — a preset click clears any custom range
    rangeBtns.forEach(btn => {{
      btn.addEventListener('click', () => {{
        clearCustom(false);
        rangeBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        rangeWeeks = parseInt(btn.dataset.weeks, 10);
        applyRange();
      }});
    }});

    // Custom date-range picker — both dates set (and ordered) activates it
    function onCustomChange() {{
      if (rangeFrom.value && rangeTo.value && rangeFrom.value <= rangeTo.value) {{
        customRange = [rangeFrom.value, rangeTo.value];
        rangeBtns.forEach(b => b.classList.remove('active'));
        clearCustomBtn.classList.add('visible');
        applyRange();
      }}
    }}
    function clearCustom(reapply) {{
      customRange = null;
      rangeFrom.value = '';
      rangeTo.value = '';
      clearCustomBtn.classList.remove('visible');
      if (reapply) applyRange();
    }}
    rangeFrom.addEventListener('change', onCustomChange);
    rangeTo.addEventListener('change', onCustomChange);
    clearCustomBtn.addEventListener('click', () => {{
      clearCustom(true);
      // Fall back to the default preset
      rangeBtns.forEach(b => b.classList.toggle('active', parseInt(b.dataset.weeks, 10) === rangeWeeks));
    }});

    function applyRange() {{
      // Apply range to all chart figures in both views
      const figIds = [
        'fig-metrics', 'fig-targets', 'fig-shift',
        'fig-metrics-sup', 'fig-targets-sup', 'fig-shift-sup'
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
        ['fig-metrics-sup','fig-util-sup','fig-targets-sup','fig-shift-sup'].forEach(id => {{
          const el = document.getElementById(id);
          if (el && el.data) Plotly.Plots.resize(el);
        }});
      }}
      updatePlots();
      applyRange();
      optimizePlotlyForMobile();
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
      const targetsFig = getTargetsFig();

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
        // Dotted keys: replacing the whole yaxis object would wipe settings
        // applied elsewhere (e.g. mobile automargin).
        const layoutUpdate = {{title: label + ' by Machine', 'yaxis.title': label}};
        if (range) {{
          layoutUpdate['xaxis.range'] = range;
          layoutUpdate['xaxis.autorange'] = false;
        }} else {{
          layoutUpdate['xaxis.autorange'] = true;
        }}
        Plotly.relayout(metricsFig, layoutUpdate);
      }}

      // Targets vs Actuals chart
      if (targetsFig && targetsFig.data) {{
        const vis = targetsFig.data.map(tr => {{
          if (!tr.meta) return false;
          return selectedMachine === 'All Machines' ? tr.meta.machine === 'All Machines' : tr.meta.machine === selectedMachine;
        }});
        Plotly.restyle(targetsFig, 'visible', vis);
        const range = getXRange(targetsFig);
        const layoutUpdate = {{title: 'Output vs Target \\u2014 ' + selectedMachine}};
        if (range) {{
          layoutUpdate['xaxis.range'] = range;
          layoutUpdate['xaxis.autorange'] = false;
        }} else {{
          layoutUpdate['xaxis.autorange'] = true;
        }}
        Plotly.relayout(targetsFig, layoutUpdate);
      }}

      // Shift comparison chart
      const shiftFig = getShiftFig();
      if (shiftFig && shiftFig.data) {{
        const vis = shiftFig.data.map(tr => {{
          if (!tr.meta) return false;
          const machineMatch = selectedMachine === 'All Machines' ? tr.meta.machine === 'All Machines' : tr.meta.machine === selectedMachine;
          const metricMatch = tr.meta.shift_metric === shiftMetric;
          return machineMatch && metricMatch;
        }});
        Plotly.restyle(shiftFig, 'visible', vis);
        const range = getXRange(shiftFig);
        const layoutUpdate = {{title: 'Shift Comparison \\u2014 ' + shiftMetric + ' \\u2014 ' + selectedMachine, 'yaxis.title': shiftMetric}};
        if (range) {{
          layoutUpdate['xaxis.range'] = range;
          layoutUpdate['xaxis.autorange'] = false;
        }} else {{
          layoutUpdate['xaxis.autorange'] = true;
        }}
        Plotly.relayout(shiftFig, layoutUpdate);
      }}
    }}

    // Shift metric buttons
    shiftMetricBtns.forEach(btn => {{
      btn.addEventListener('click', () => {{
        shiftMetricBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        shiftMetric = btn.dataset.metric;
        updatePlots();
      }});
    }});

    // Monthly summary: Show all / Show fewer toggle
    document.querySelectorAll('.monthly-toggle').forEach(btn => {{
      btn.addEventListener('click', () => {{
        const tableId = btn.dataset.table;
        const table = document.getElementById(tableId);
        if (!table) return;
        const olderRows = table.querySelectorAll('tr.older-month');
        const expanded = btn.classList.toggle('active');
        olderRows.forEach(row => {{
          row.style.display = expanded ? '' : 'none';
        }});
        btn.textContent = expanded
          ? `Show fewer (${{olderRows.length}} older)`
          : `Show all months (${{olderRows.length}} older)`;
      }});
    }});

    function exportChart(divId) {{
      const el = document.getElementById(divId);
      if (el) Plotly.downloadImage(el, {{format:'png', width:1200, height:800, filename:'dashboard-'+divId}});
    }}

{MOBILE_PLOTLY_JS}

    machineSelect.addEventListener('change', updatePlots);
    metricSelect.addEventListener('change', updatePlots);

    // Initialize
    rebuildMetricDropdown();
    updatePlots();
    // Apply default range + mobile layout after Plotly renders
    setTimeout(() => {{ applyRange(); optimizePlotlyForMobile(); }}, 500);
  </script>
</body>
</html>"""
