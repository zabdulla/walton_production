"""Shared HTML/CSS/JS building blocks for the dashboard builders.

The five ``build_*_dashboard.py`` scripts each render a self-contained HTML
page. Anything visual or behavioral that should stay consistent across them
lives here so a fix lands once instead of five times.

Conventions:
- Constants hold FINAL output text (single braces). Builders interpolate them
  into their f-string templates, so the rendered HTML is identical to what an
  inline (double-braced) copy would produce.
- Keep indentation inside constants exactly as it should appear in the output;
  builders inject them at column 0 of the template line.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

# Base look shared by the interactive/profit/payroll/operator dashboards
# (the daily dashboard has its own separate design).
# Palette: "evergreen industrial" — the UI brand color IS the lead chart
# color (CHART_PALETTE[0] = #0B6E4F) so page chrome and data read as one
# system. Semantic colors (green=hit / red=miss / amber=warn) are defined
# per-chart and deliberately NOT part of these tokens.
BASE_CSS = """\
    :root {
      --brand:#0b6e4f; --brand-strong:#095c42; --brand-soft:#e7f3ee;
      --bg:#f6f7f9; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb;
      --shadow-card:0 1px 2px rgba(16,24,40,.05), 0 8px 24px rgba(16,24,40,.06);
      --shadow-lift:0 2px 4px rgba(16,24,40,.06), 0 12px 32px rgba(16,24,40,.10);
    }
    * { box-sizing:border-box; }
    body { margin:0; padding:24px;
            font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,Helvetica,Arial,sans-serif;
            background:linear-gradient(180deg,#edf4f0 0,var(--bg) 360px) fixed; color:var(--text); }
    .eyebrow { font-size:11px; font-weight:700; letter-spacing:.14em; text-transform:uppercase;
               color:var(--brand); margin:0 0 6px; }
    h1 { margin:0 0 4px; font-weight:700; letter-spacing:-0.02em; }
    .subtitle { margin:0 0 16px; color:var(--muted); font-size:14px; }
    .kpi-card { transition:transform .18s ease, box-shadow .18s ease; }
    .kpi-card:hover { transform:translateY(-2px); box-shadow:var(--shadow-lift); }
    .kpi-value { font-variant-numeric:tabular-nums; }
    button { transition:background .15s ease, color .15s ease, border-color .15s ease,
             transform .1s ease, box-shadow .15s ease; }
    button:active { transform:scale(.97); }
    @keyframes rise { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:none; } }
    .kpi-grid > .kpi-card { animation:rise .4s cubic-bezier(.2,.7,.3,1) backwards; }
    .kpi-grid > .kpi-card:nth-child(1){animation-delay:.02s} .kpi-grid > .kpi-card:nth-child(2){animation-delay:.07s}
    .kpi-grid > .kpi-card:nth-child(3){animation-delay:.12s} .kpi-grid > .kpi-card:nth-child(4){animation-delay:.17s}
    .kpi-grid > .kpi-card:nth-child(5){animation-delay:.22s} .kpi-grid > .kpi-card:nth-child(6){animation-delay:.27s}
    .kpi-grid > .kpi-card:nth-child(n+7){animation-delay:.32s}
    @media (prefers-reduced-motion: reduce) {
      * { animation:none !important; transition:none !important; }
    }"""

CARD_CSS = """\
    .card { background:var(--card); border:1px solid var(--border); border-radius:16px;
             box-shadow:var(--shadow-card); padding:20px; margin-bottom:20px;
             animation:rise .45s cubic-bezier(.2,.7,.3,1) backwards; animation-delay:.05s; }"""

# ---------------------------------------------------------------------------
# Plotly mobile support
# ---------------------------------------------------------------------------

# Pass as ``config=`` to every plotly ``to_html`` call. Without responsive,
# charts render once at load width and never reflow — rotating a phone leaves
# a clipped or letterboxed chart.
PLOTLY_CONFIG = {"responsive": True, "displaylogo": False}

# The modebar is hover-oriented and eats vertical space on phones.
MOBILE_MODEBAR_CSS = """\
    @media (max-width:768px) { .modebar { display:none !important; } }"""

# Phone-size fixes Plotly can't do via CSS (layout lives inside the figure):
# move legends below the plot (the desktop right-side legend plus its reserved
# margin eats most of a narrow screen), shrink margins/fonts, cap tall charts,
# thin out x ticks, and disable dragmode so a touch-drag scrolls the page
# instead of panning the chart — tapping still shows tooltips.
MOBILE_PLOTLY_JS = """\
    function optimizePlotlyForMobile() {
      if (!window.matchMedia('(max-width: 768px)').matches) return;
      document.querySelectorAll('.js-plotly-plot').forEach(el => {
        if (!el.data || !el._fullLayout) return;
        const update = {
          'legend.orientation': 'h', 'legend.x': 0, 'legend.xanchor': 'left',
          'legend.y': -0.22, 'legend.yanchor': 'top', 'legend.font.size': 10,
          'margin.l': 45, 'margin.r': 12,
          'font.size': 11,
          'xaxis.nticks': 5,
          'yaxis.automargin': true,
          'dragmode': false
        };
        if (el._fullLayout.height >= 550) update['height'] = 420;
        Plotly.relayout(el, update);
      });
    }
    (function () {
      let resizeTimer = null;
      window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(optimizePlotlyForMobile, 250);
      });
    })();"""

# ---------------------------------------------------------------------------
# JS helpers
# ---------------------------------------------------------------------------

# Escape free-text fields (supervisor notes, operator/employee names) before
# inserting into innerHTML — they come straight from Excel/PDF cells.
# Indented for injection inside an 8-space <script> body (daily dashboard);
# the 4-space dashboards inject ESCAPE_HTML_JS_4SP below.
ESCAPE_HTML_JS = """\
        function escapeHtml(s) {
            return String(s ?? '').replace(/[&<>"']/g, c => ({
                '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
            }[c]));
        }"""

ESCAPE_HTML_JS_4SP = """\
    function escapeHtml(s) {
      return String(s ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[c]));
    }"""

# Format a Date as YYYY-MM-DD in LOCAL time. toISOString() is UTC and shifts
# entries to the wrong calendar day for viewers west of UTC.
LOCAL_DATE_JS = """\
        function localDateStr(d) {
            return d.getFullYear() + '-' +
                String(d.getMonth() + 1).padStart(2, '0') + '-' +
                String(d.getDate()).padStart(2, '0');
        }"""


# ---------------------------------------------------------------------------
# Shift-comparison chart config (shared by the figure builder in
# build_interactive_dashboard and the page JS in interactive_template)
# ---------------------------------------------------------------------------
SHIFT_METRICS = {
    "Output": ("Actual_Output", ",.0f", "lbs"),
    "Output/Hr": ("Output_per_Hour", ",.1f", "lbs/hr"),
    "Cost/Lb": ("Cost_per_Pound", "$.4f", ""),
}
SHIFT_COLORS = {"1st": "#3b82f6", "2nd": "#f59e0b", "3rd": "#8b5cf6"}
