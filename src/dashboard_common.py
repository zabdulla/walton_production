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
      --shadow-card:0 1px 2px rgba(16,24,32,.06), 0 6px 20px rgba(16,24,32,.05);
      --shadow-lift:0 2px 4px rgba(16,24,40,.06), 0 12px 32px rgba(16,24,40,.10);
      --grid:#eceff2; --good:#0ca30c; --warn:#fab219; --await:#b3bcc3; --await-ink:#6b7680; --idle:#d3d8dd;
      --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --s4:#eda100; --s5:#e87ba4; --s6:#7d8b9c; --s7:#8e6fd8; --s8:#2fb5c9; --s9:#b8863b; --s10:#5a6b7b;
    }
    * { box-sizing:border-box; }
    body { margin:0; padding:28px 24px 48px;
            font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,Helvetica,Arial,sans-serif;
            background:var(--bg); color:var(--text); }
    header, main, footer { max-width:1180px; margin-left:auto; margin-right:auto; }
    .eyebrow { font-size:11px; font-weight:700; letter-spacing:.14em; text-transform:uppercase;
               color:var(--brand); margin:0 0 6px; }
    h1 { margin:0 0 4px; font-size:28px; font-weight:700; letter-spacing:-0.02em; text-wrap:balance; }
    .subtitle { margin:0 0 14px; color:var(--muted); font-size:13px; }
    .subtitle b { color:var(--text); font-weight:600; }
    .kpi-value { font-variant-numeric:tabular-nums; }
    button { transition:background .15s ease, color .15s ease, border-color .15s ease,
             transform .1s ease, box-shadow .15s ease; }
    button:active { transform:scale(.97); }
    @media (prefers-reduced-motion: reduce) {
      * { animation:none !important; transition:none !important; }
    }"""

CARD_CSS = """\
    .card { background:var(--card); border:1px solid var(--border); border-radius:12px;
             box-shadow:var(--shadow-card); padding:18px 20px; margin-bottom:20px; }
    .card h2 { margin:0 0 4px; font-size:20px; font-weight:600; letter-spacing:-.01em; }
    .card .lede { margin:0 0 12px; color:var(--muted); font-size:13px; }
    .card-head { display:flex; flex-wrap:wrap; align-items:flex-end; justify-content:space-between; gap:8px 16px; margin-bottom:8px; }
    .card-head h2, .card-head .lede { margin-bottom:0; }"""

# ---------------------------------------------------------------------------
# Dark mode
# ---------------------------------------------------------------------------

# Dark token overrides + generic control fixes. Scoped to html[data-theme=
# "dark"], set by THEME_INIT_JS before first paint (no flash). Brand shifts
# one step brighter for contrast on dark surfaces. Semantic chart colors
# (green/red/amber) are used on both themes unchanged.
DARK_CSS = """\
    html[data-theme="dark"] {
      --brand:#34a882; --brand-strong:#2c8f6f; --brand-soft:#12261f;
      --bg:#0f1417; --card:#161d23; --text:#e5e9ec; --muted:#9aa4ad; --border:#2a333a;
      --shadow-card:0 1px 2px rgba(0,0,0,.4), 0 8px 24px rgba(0,0,0,.35);
      --shadow-lift:0 2px 4px rgba(0,0,0,.45), 0 12px 32px rgba(0,0,0,.5);
    }
    html[data-theme="dark"] {
      --grid:#222b32; --await:#4f5a63; --await-ink:#9aa5ad; --idle:#343f47;
      --s1:#3987e5; --s2:#d95926; --s3:#199e70; --s4:#c98500; --s5:#d55181; --s6:#8b99a8; --s7:#9d84e0; --s8:#39b9cb; --s9:#c9964c; --s10:#8797a6;
    }
    html[data-theme="dark"] body { background:var(--bg); color:var(--text); }
    html[data-theme="dark"] select, html[data-theme="dark"] input,
    html[data-theme="dark"] .toggle-btn, html[data-theme="dark"] .range-btn,
    html[data-theme="dark"] .export-btn, html[data-theme="dark"] .nav-btn,
    html[data-theme="dark"] .period-select, html[data-theme="dark"] .period-type-btn {
      background:var(--card); color:var(--text); border-color:var(--border);
    }
    html[data-theme="dark"] .toggle-btn:hover:not(.active),
    html[data-theme="dark"] .range-btn:hover:not(.active),
    html[data-theme="dark"] .export-btn:hover,
    html[data-theme="dark"] .nav-btn:hover { background:#1e2830; }
    html[data-theme="dark"] .toggle-btn.active, html[data-theme="dark"] .range-btn.active,
    html[data-theme="dark"] .period-type-btn.active {
      background:var(--brand); color:#08110d; border-color:var(--brand);
    }
    html[data-theme="dark"] .kpi-card { background:#1b242b; border-color:var(--border); }
    html[data-theme="dark"] tr:hover td { background:#1b242b; }
    html[data-theme="dark"] input[type="date"] { color-scheme:dark; }"""

# Runs in <head> BEFORE stylesheets apply: resolves the saved (or OS) theme
# so the first paint is already correct, and defines the color helpers the
# per-page chart scripts read at every render.
THEME_INIT_JS = """\
  <script>
    (function () {
      try {
        var t = localStorage.getItem('walton-theme');
        if (!t) t = (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', t);
      } catch (e) { /* default = light */ }
    })();
    function waltonPlotColors() {
      var dark = document.documentElement.getAttribute('data-theme') === 'dark';
      return dark
        ? { font: '#cbd5d1', grid: '#2a333a', zero: '#3b464e' }
        : { font: '#1f2937', grid: '#e5e7eb', zero: '#cbd5e1' };
    }
    function waltonThemePlots() {
      if (!window.Plotly) return;
      var c = waltonPlotColors();
      document.querySelectorAll('.js-plotly-plot').forEach(function (el) {
        try {
          Plotly.relayout(el, {
            'font.color': c.font,
            'xaxis.gridcolor': c.grid, 'yaxis.gridcolor': c.grid,
            'xaxis.zerolinecolor': c.zero, 'yaxis.zerolinecolor': c.zero,
            'yaxis2.gridcolor': c.grid, 'legend.bgcolor': 'rgba(0,0,0,0)'
          });
        } catch (e) { /* figure without those axes */ }
      });
    }
  </script>"""

# Floating theme toggle. Include once per page (before </body> is fine —
# the button is position:fixed).
THEME_TOGGLE_HTML = """\
  <button id="themeToggle" class="theme-toggle" title="Toggle dark mode" aria-label="Toggle dark mode">&#127769;</button>
  <script>
    (function () {
      var btn = document.getElementById('themeToggle');
      function icon() {
        btn.textContent = document.documentElement.getAttribute('data-theme') === 'dark' ? '\\u2600\\ufe0f' : '\\ud83c\\udf19';
      }
      btn.addEventListener('click', function () {
        var next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        try { localStorage.setItem('walton-theme', next); } catch (e) {}
        icon();
        waltonThemePlots();
      });
      icon();
      // Server-rendered figures are baked light; fix them once they exist.
      if (document.documentElement.getAttribute('data-theme') === 'dark') {
        setTimeout(waltonThemePlots, 600);
      }
    })();
  </script>"""

THEME_TOGGLE_CSS = """\
    .theme-toggle { position:fixed; top:14px; right:14px; z-index:60;
      width:38px; height:38px; border-radius:999px; border:1px solid var(--border);
      background:var(--card); box-shadow:var(--shadow-card); cursor:pointer;
      font-size:15px; line-height:1; display:flex; align-items:center; justify-content:center; }
    .theme-toggle:hover { box-shadow:var(--shadow-lift); }"""

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
