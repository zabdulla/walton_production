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
BASE_CSS = """\
    :root { --bg:#f3f4f6; --card:#fff; --text:#111827; --muted:#6b7280; --border:#e5e7eb; }
    * { box-sizing:border-box; }
    body { margin:0; padding:24px; font-family:"Helvetica Neue",Arial,sans-serif;
            background:radial-gradient(circle at 20% 20%,#f9fafb 0,#eef2ff 40%,#f3f4f6 90%); color:var(--text); }
    h1 { margin:0 0 4px; font-weight:700; }
    .subtitle { margin:0 0 16px; color:var(--muted); font-size:14px; }"""

CARD_CSS = """\
    .card { background:var(--card); border:1px solid var(--border); border-radius:16px;
             box-shadow:0 10px 50px rgba(15,23,42,.08); padding:20px; margin-bottom:20px; }"""

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
