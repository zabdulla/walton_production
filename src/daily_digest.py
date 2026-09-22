"""Daily production digest: one email about yesterday.

Sections: yesterday's pounds by machine and shift against the 4-week average for
that weekday; the week so far; what the End of Shift forms reported (machine and
man hours per machine, pounds per machine hour, downtime, comments, shift notes,
which shifts are missing); the weekly trend (8 weeks of plant output with the
4-week average) as an inline image; and a link to the production page.

    python3 src/daily_digest.py                       # write reports/digest/<date>.html and .png for yesterday
    python3 src/daily_digest.py --date 2026-09-21     # a specific production day
    python3 src/daily_digest.py --send --to you@x.com # build and send through the Gmail API
    python3 src/daily_digest.py --send-if-due         # cloud: send once a day after DIGEST_SEND_HOUR local (recipients from DIGEST_TO)
    python3 src/daily_digest.py --authorize           # one-time consent for the gmail.send scope (browser)

Inputs: data/aggregated_daily_data.xlsx (pounds, hours per shift-day-machine) and
data/cietrade_status.json (End of Shift reports). The chart is the dashboard's own
"Weekly Metrics by Machine" figure (Actual Output, 4-wk average, last 20 weeks),
exported to PNG with kaleido for the email and embedded live on the hosted page
(docs/digest/<date>.html). If kaleido cannot export, a Pillow chart of plant
output stands in so the email still goes.
"""
from __future__ import annotations

import argparse
import base64
import html as _h
import json
import os
import sys
from datetime import date, datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_DIR, MACHINE_WEEKLY_OUTPUT_TARGETS, PROJECT_ROOT, WALTON_CONFIG_DIR  # noqa: E402

AGG_PATH = DATA_DIR / "aggregated_daily_data.xlsx"
STATUS_PATH = DATA_DIR / "cietrade_status.json"
OUT_DIR = PROJECT_ROOT / "reports" / "digest"
STATE_PATH = DATA_DIR / "digest_state.json"
DASHBOARD_URL = "https://zabdulla.github.io/walton_production/"
DOCS_DIGEST = PROJECT_ROOT / "docs" / "digest"
SEND_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
SEND_TOKEN_PATH = WALTON_CONFIG_DIR / "gmail_send_token.json"
CREDENTIALS_PATH = WALTON_CONFIG_DIR / "gmail_credentials.json"
SHIFTS = ("1st", "2nd", "3rd")
BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#1a1d21", "#66707c", "#e3e6ea"


# ---------------------------------------------------------------- data

def load_inputs(agg_path: Path = AGG_PATH, status_path: Path = STATUS_PATH) -> tuple[pd.DataFrame, dict]:
    df = pd.read_excel(agg_path)
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    return df, status


def _lbs(df: pd.DataFrame) -> pd.Series:
    """Output pounds, counting Guillotine's rolls in when it books no output (the dashboard's support basis)."""
    out = pd.to_numeric(df["Actual_Output"], errors="coerce").fillna(0.0)
    inp = pd.to_numeric(df.get("Actual_Input"), errors="coerce").fillna(0.0) if "Actual_Input" in df.columns else 0.0
    guillotine = df["Machine_Name"].astype(str).str.contains("GUILLOTINE", case=False, na=False)
    return out.where(~(guillotine & (out == 0)), inp)


def pick_day(df: pd.DataFrame, for_date: date | None = None) -> date:
    """Yesterday, or the latest production day on or before it that has rows."""
    target = pd.Timestamp(for_date or (date.today() - timedelta(days=1)))
    have = df.loc[df["Date"] <= target, "Date"]
    return (have.max() if len(have) else target).date()


def build_digest(df: pd.DataFrame, status: dict, for_date: date) -> dict:
    d = df.copy()
    d["lbs"] = _lbs(d)
    d["mh"] = pd.to_numeric(d["Machine_Hours"], errors="coerce").fillna(0.0).clip(upper=24)
    d["man"] = pd.to_numeric(d["Man_Hours"], errors="coerce").fillna(0.0)
    day = pd.Timestamp(for_date)
    today_rows = d[d["Date"] == day]
    # 4-week average for the same weekday, per machine, over the previous four occurrences with rows
    prev = d[(d["Date"] < day) & (d["Date"].dt.weekday == day.weekday()) & (d["Date"] >= day - pd.Timedelta(weeks=5))]
    prev_days = sorted(prev["Date"].unique())[-4:]
    prev = prev[prev["Date"].isin(prev_days)]
    avg4 = prev.groupby("Machine_Name")["lbs"].sum().div(max(len(prev_days), 1)) if len(prev_days) else pd.Series(dtype=float)
    machines = list(today_rows.groupby("Machine_Name")["lbs"].sum().sort_values(ascending=False).index)
    by_shift = today_rows.pivot_table(index="Machine_Name", columns="Shift", values="lbs", aggfunc="sum").reindex(columns=list(SHIFTS)).fillna(0.0)
    hours = today_rows.groupby("Machine_Name")[["mh", "man"]].sum()
    rows = []
    for m in machines:
        tot = float(by_shift.loc[m].sum()) if m in by_shift.index else 0.0
        mh = float(hours.loc[m, "mh"]) if m in hours.index else 0.0
        rows.append({"machine": m, "shifts": [round(float(by_shift.loc[m, s])) for s in SHIFTS] if m in by_shift.index else [0, 0, 0],
                     "total": round(tot), "avg4": round(float(avg4.get(m, 0.0))), "machine_h": round(mh, 2),
                     "man_h": round(float(hours.loc[m, "man"]) if m in hours.index else 0.0, 2),
                     "lbs_per_mh": round(tot / mh) if mh > 0 else None})
    day_total = round(float(today_rows["lbs"].sum()))
    day_avg4 = round(float(avg4.sum())) if len(avg4) else 0
    # week so far
    monday = day - pd.Timedelta(days=day.weekday())
    wk = d[(d["Date"] >= monday) & (d["Date"] <= day)]
    wdays = [monday + pd.Timedelta(days=i) for i in range((day - monday).days + 1)]
    wk_pivot = wk.pivot_table(index="Machine_Name", columns="Date", values="lbs", aggfunc="sum").reindex(columns=wdays).fillna(0.0)
    week_rows = []
    workdays_elapsed = sum(1 for x in wdays if x.weekday() < 5)
    for m in wk_pivot.sum(axis=1).sort_values(ascending=False).index:
        wtd = float(wk_pivot.loc[m].sum())
        target = MACHINE_WEEKLY_OUTPUT_TARGETS.get(m)
        pace = round(target * workdays_elapsed / 5) if target else None
        week_rows.append({"machine": m, "days": [round(float(v)) for v in wk_pivot.loc[m]], "wtd": round(wtd), "pace": pace})
    week_total = round(float(wk["lbs"].sum()))
    # weekly trend: 8 weeks of plant output (Monday weeks) ending this week, 4-week average of completed weeks
    d = d[d["Date"] <= day].copy()
    d["wk"] = d["Date"] - pd.to_timedelta(d["Date"].dt.weekday, unit="D")
    weekly = d.groupby("wk")["lbs"].sum().sort_index()
    weekly = weekly[weekly.index <= monday].tail(8)
    completed = weekly[weekly.index < monday]
    avg_line = completed.rolling(4, min_periods=1).mean()
    trend = [{"week": w.strftime("%Y-%m-%d"), "lbs": round(float(v)), "partial": bool(w == monday),
              "avg4": (round(float(avg_line.get(w))) if w in avg_line.index else None)} for w, v in weekly.items()]
    mweekly = d[d["wk"] < monday].groupby(["Machine_Name", "wk"])["lbs"].sum().unstack("wk").fillna(0.0)
    last4 = sorted(mweekly.columns)[-4:] if len(mweekly.columns) else []
    machine_trend = []
    for m in machines:
        if m in mweekly.index and last4:
            machine_trend.append({"machine": m, "last_week": round(float(mweekly.loc[m, last4[-1]])), "avg4": round(float(mweekly.loc[m, last4].mean())),
                                  "wtd": next((r["wtd"] for r in week_rows if r["machine"] == m), 0), "target": MACHINE_WEEKLY_OUTPUT_TARGETS.get(m)})
    # End of Shift
    eos = (status.get("end_of_shift") or {})
    reports = [r for r in eos.get("reports", []) if r.get("date") == for_date.isoformat()]
    filed = {r["shift"]: r for r in reports}
    # one block per shift: every machine that produced or was reported, with the form's row beside the pounds
    shift_blocks = []
    for sh in SHIFTS:
        rep = filed.get(sh)
        form = {m["machine"]: m for m in rep["machines"]} if rep else {}
        srows = today_rows[today_rows["Shift"].astype(str) == sh]
        lbs_by = srows.groupby("Machine_Name")["lbs"].sum().to_dict()
        names = sorted({m for m, v in lbs_by.items() if v > 0} | set(form), key=lambda m: -(lbs_by.get(m, 0) + (1 if m in form else 0)))
        items = []
        for m in names:
            f = form.get(m, {})
            items.append({"machine": m, "lbs": round(float(lbs_by.get(m, 0.0))), "machine_h": f.get("machine_hours"), "man_h": f.get("man_hours"),
                          "operators": f.get("operators", ""), "material": f.get("material", ""), "downtime_min": f.get("downtime_min", 0),
                          "reason": f.get("reason", ""), "comment": f.get("comment", "")})
        shift_blocks.append({"shift": sh, "by": rep["by"] if rep else None, "filed_at": (rep or {}).get("filed_at", ""),
                             "lbs": round(float(srows["lbs"].sum())), "machine_h": round(sum((f.get("machine_hours") or 0) for f in form.values()), 1),
                             "man_h": round(sum((f.get("man_hours") or 0) for f in form.values()), 1),
                             "downtime_min": int(sum((f.get("downtime_min") or 0) for f in form.values())),
                             "notes": (rep or {}).get("notes", []), "rows": items})
    downtime = [{"shift": r["shift"], "machine": m["machine"], "min": m["downtime_min"], "reason": m.get("reason", ""), "comment": m.get("comment", ""), "operators": m.get("operators", "")}
                for r in reports for m in r["machines"] if m.get("downtime_min") or m.get("comment")]
    notes = [{"shift": r["shift"], "note": n} for r in reports for n in r.get("notes", [])]
    eos_hours = sum(m["machine_hours"] for r in reports for m in r["machines"])
    eos_man = sum(m["man_hours"] for r in reports for m in r["machines"])
    return {"date": for_date.isoformat(), "day_label": day.strftime("%A, %B %-d"), "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "day_total": day_total, "day_avg4": day_avg4, "rows": rows, "week": {"monday": monday.strftime("%Y-%m-%d"), "days": [x.strftime("%a %-d") for x in wdays],
            "rows": week_rows, "total": week_total, "workdays_elapsed": workdays_elapsed},
            "trend": trend, "machine_trend": machine_trend, "shifts": shift_blocks,
            "eos": {"filed": {s: (filed[s]["by"] if s in filed else None) for s in SHIFTS}, "machine_h": round(eos_hours, 1), "man_h": round(eos_man, 1),
                    "downtime": downtime, "notes": notes, "downtime_total": sum(x["min"] for x in downtime)},
            "status": {"last_poll": status.get("last_poll"), "awaiting": [a for a in status.get("awaiting", []) if a and a[0] == for_date.isoformat()]},
            "url": DASHBOARD_URL}


# ---------------------------------------------------------------- chart (the dashboard's weekly figure)

def weekly_figure(df: pd.DataFrame, weeks: int | None = None):
    """The dashboard's "Weekly Metrics by Machine" chart, exactly as built there: aggregate_weekly ->
    add_running_averages -> build_interactive_fig, trimmed to the traces the dashboard shows by default
    (first key metric's 4-wk average per machine) and to its default range."""
    import plotly.graph_objects as go
    from build_interactive_dashboard import add_running_averages, aggregate_weekly, build_interactive_fig, clean_product_names
    from config import ALL_METRICS, DEFAULT_WEEKS, RUNNING_AVG_WINDOW
    d = clean_product_names(df.copy())
    for c in ("Man_Hours", "Machine_Hours", "Actual_Input", "Actual_Output"):
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)
    d = d[(d["Man_Hours"] > 0) | (d["Machine_Hours"] > 0) | (d["Actual_Input"] > 0) | (d["Actual_Output"] > 0)]
    weekly = add_running_averages(aggregate_weekly(d), metrics=list(ALL_METRICS.keys()), window=RUNNING_AVG_WINDOW)
    full = build_interactive_fig(weekly)
    fig = go.Figure(data=[t for t in full.data if t.visible is True], layout=full.layout)
    starts = sorted(weekly["Week_Start"].unique())
    if starts:
        cutoff = starts[-(weeks or DEFAULT_WEEKS):][0]
        fig.update_xaxes(range=[pd.Timestamp(cutoff) - pd.Timedelta(days=3), pd.Timestamp(starts[-1]) + pd.Timedelta(days=3)])
    fig.update_layout(margin=dict(t=60, r=20, b=40, l=70), legend=dict(orientation="h", x=0, y=-0.28, xanchor="left", yanchor="top", title=None),
                      plot_bgcolor="#ffffff", paper_bgcolor="#ffffff")
    return fig


def write_figure_png(fig, path: Path, width: int = 1100, height: int = 520) -> Path:
    """PNG through kaleido (0.2.x). Raises if the exporter is missing; callers fall back to draw_trend_png."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(path), format="png", width=width, height=height, scale=2)
    return path


def figure_html(fig, div_id: str = "weeklyFig") -> str:
    from dashboard_common import PLOTLY_CONFIG
    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id=div_id, config=PLOTLY_CONFIG)


def chart_png(df: pd.DataFrame, dg: dict, path: Path) -> tuple[Path, object | None]:
    """The dashboard figure as a PNG, or the Pillow fallback. Returns (png path, figure or None)."""
    try:
        fig = weekly_figure(df)
        return write_figure_png(fig, path), fig
    except Exception as exc:  # kaleido missing or its chromium cannot start on this host
        print(f"kaleido export failed ({exc.__class__.__name__}: {str(exc)[:120]}); using the Pillow fallback chart")
        return draw_trend_png(dg["trend"], path), None


# ---------------------------------------------------------------- chart (Pillow)

def draw_trend_png(trend: list[dict], path: Path, width: int = 1000, height: int = 380) -> Path:
    from PIL import Image, ImageDraw, ImageFont
    def font(size: int, bold: bool = False):
        for f in (f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf", "/System/Library/Fonts/Supplemental/Arial.ttf"):
            try:
                return ImageFont.truetype(f, size)
            except OSError:
                continue
        return ImageFont.load_default()
    img = Image.new("RGB", (width, height), "white")
    dr = ImageDraw.Draw(img)
    f12, f13b, f11 = font(13), font(14, True), font(12)
    left, right, top, bottom = 70, 20, 44, 50
    dr.text((left, 10), "Weekly plant output, lbs", fill=INK, font=f13b)
    dr.text((left + 230, 12), "bars: week total   line: 4-week average of completed weeks", fill=MUTED, font=f11)
    if not trend:
        return path
    mx = max(max(t["lbs"] for t in trend), max((t["avg4"] or 0) for t in trend), 1)
    step = 10 ** (len(str(int(mx))) - 1); step = step if mx / step >= 3 else step / 2
    ymax = (int(mx / step) + 1) * step
    ph = height - top - bottom
    def y(v): return top + ph - ph * v / ymax
    n = len(trend); slot = (width - left - right) / n; bw = slot * 0.62
    for g in range(0, int(ymax) + 1, int(step)):
        yy = y(g); dr.line([(left, yy), (width - right, yy)], fill=GRID, width=1)
        dr.text((left - 8 - dr.textlength(f"{g/1000:.0f}k", font=f11), yy - 7), f"{g/1000:.0f}k", fill=MUTED, font=f11)
    pts, labels = [], []
    for i, t in enumerate(trend):
        x0 = left + i * slot + (slot - bw) / 2
        col = "#9ec5f4" if t["partial"] else BLUE
        dr.rounded_rectangle([x0, y(t["lbs"]), x0 + bw, top + ph], radius=4, fill=col)
        wk = datetime.strptime(t["week"], "%Y-%m-%d").strftime("%b %-d") + (" (to date)" if t["partial"] else "")
        dr.text((x0 + bw / 2 - dr.textlength(wk, font=f11) / 2, top + ph + 8), wk, fill=MUTED, font=f11)
        ay = y(t["avg4"]) if t["avg4"] is not None else None
        if ay is not None:
            pts.append((x0 + bw / 2, ay))
        ly = y(t["lbs"]) - 16
        if ay is not None and abs(ly + 6 - ay) < 14:      # label would sit on the average dot: lift it
            ly = min(ly, ay) - 20
        labels.append((x0 + bw / 2, ly, f"{t['lbs']/1000:.0f}k"))
    if len(pts) > 1:
        dr.line(pts, fill=ORANGE, width=3)
    for p in pts:
        dr.ellipse([p[0] - 4, p[1] - 4, p[0] + 4, p[1] + 4], fill="white", outline=ORANGE, width=2)
    for cx, ly, lbl in labels:
        dr.text((cx - dr.textlength(lbl, font=f11) / 2, ly), lbl, fill=INK, font=f11)
    dr.line([(left, top + ph), (width - right, top + ph)], fill="#c3c2b7", width=1)
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, "PNG", optimize=True)
    return path


# ---------------------------------------------------------------- email HTML

def _n(v) -> str:
    return "–" if v is None else f"{int(round(v)):,}"


def _delta(cur: float, base: float) -> str:
    if not base:
        return '<span style="color:#66707c">no baseline</span>'
    pct = 100 * (cur - base) / base
    col = "#0ca30c" if pct >= 0 else "#d03b3b"
    return f'<span style="color:{col};font-weight:600">{pct:+.0f}%</span> <span style="color:#66707c">vs 4-wk avg {_n(base)}</span>'


def _shift_table_rows(dg: dict, e, cell: str, num: str, sub: str, bar: str, shifts: list[str] | None = None, ncols: int = 6, mcell: str | None = None) -> str:
    """Rows of the one consolidated table: a bar per shift, one row per machine (lbs, hours, crew, material),
    and a full-width line under a machine when the form carried downtime or a comment. Notes close each shift."""
    def n(v):
        return "" if v in (None, "", 0, 0.0) else f"{int(round(float(v))):,}"
    def hrs(v):
        return "" if v in (None, "") else f"{float(v):g}"
    out = []
    mcell = mcell or cell
    for b in dg["shifts"]:
        if shifts and b["shift"] not in shifts:
            continue
        filed = f'filed by {e(b["by"])}' if b["by"] else '<span style="color:#c0392b">no End of Shift report</span>'
        hours = f' &nbsp;·&nbsp; {b["machine_h"]:g} machine h &nbsp;·&nbsp; {b["man_h"]:g} man h' if b["by"] else ""
        down = f' &nbsp;·&nbsp; <span style="color:#c0392b">{b["downtime_min"]} min down</span>' if b["downtime_min"] else ""
        out.append(f'<tr><td colspan="{ncols}" {bar}>{e(b["shift"])} shift &nbsp;·&nbsp; {n(b["lbs"]) or "0"} lbs &nbsp;·&nbsp; {filed}{hours}{down}</td></tr>')
        for r in b["rows"]:
            out.append(f'<tr><td {mcell}><b>{e(r["machine"])}</b></td><td {num}>{n(r["lbs"])}</td><td {num}>{hrs(r["machine_h"])}</td><td {num}>{hrs(r["man_h"])}</td>'
                       f'<td {cell}>{e(r["operators"])}</td><td {cell}>{e(r["material"])}</td></tr>')
            bits = []
            if r["downtime_min"]:
                bits.append(f'<span style="color:#c0392b"><b>{r["downtime_min"]} min down</b>' + (f' · {e(r["reason"])}' if r["reason"] else "") + "</span>")
            if r["comment"]:
                bits.append(e(r["comment"]))
            if bits:
                out.append(f'<tr><td colspan="{ncols}" {sub}>{" &nbsp;·&nbsp; ".join(bits)}</td></tr>')
        for note in b["notes"]:
            out.append(f'<tr><td colspan="{ncols}" {sub}><span style="color:#6b7280">Shift note:</span> {e(note)}</td></tr>')
    return "".join(out)


def render_email(dg: dict, image_src: str = "cid:trend.png", page_url: str | None = None) -> str:
    """Email-safe HTML: nested tables, every style inline, fixed column widths so nothing wraps oddly."""
    e = _h.escape
    F = "font-family:Arial,Helvetica,sans-serif;"
    base = f"{F}font-size:13px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top;"
    cell = f'style="{base}"'
    num = f'style="{base}text-align:right;white-space:nowrap"'
    mcell = f'style="{base}white-space:nowrap"'
    sub = f'style="{base}font-size:12px;color:#4b5563;padding-top:0;padding-left:18px"'
    head = f'style="{F}font-size:11px;color:#6b7280;padding:6px 8px;border-bottom:2px solid #d9dde2;text-align:left;text-transform:uppercase;white-space:nowrap"'
    headn = head.replace("text-align:left", "text-align:right")
    h2 = f'style="{F}font-size:16px;font-weight:bold;color:#1a1d21;margin:0;padding:24px 0 8px"'
    small = f'style="{F}font-size:12px;color:#6b7280"'
    bar = f'style="{F}font-size:13px;font-weight:bold;color:#1a1d21;background:#f1f3f5;padding:8px"'
    def n(v):
        return "" if v in (None, "", 0, 0.0) else f"{int(round(float(v))):,}"
    delta = ""
    if dg["day_avg4"]:
        pct = 100 * (dg["day_total"] - dg["day_avg4"]) / dg["day_avg4"]
        delta = f' &nbsp;<span style="color:{"#0a7d2c" if pct >= 0 else "#c0392b"};font-weight:bold">{pct:+.0f}%</span> <span style="color:#6b7280">vs the 4-week average for a {e(dg["day_label"].split(",")[0])} ({n(dg["day_avg4"])} lbs)</span>'
    body_rows = _shift_table_rows(dg, e, cell, num, sub, bar, mcell=mcell)
    wk = dg["week"]
    wrows = "".join(f'<tr><td {cell}>{e(r["machine"])}</td>' + "".join(f'<td {num}>{n(v)}</td>' for v in r["days"]) + f'<td {num}><b>{n(r["wtd"])}</b></td></tr>' for r in wk["rows"])
    wrows += f'<tr><td {cell}><b>Plant</b></td>' + "".join(f'<td {num}></td>' for _ in wk["days"]) + f'<td {num}><b>{n(wk["total"])}</b></td></tr>'
    page_link = f' &nbsp;·&nbsp; <a href="{page_url}" style="color:#1f6feb">web version with shift tabs and the live chart</a>' if page_url else ""
    cols = '<col width="178"><col width="62"><col width="58"><col width="54"><col width="150"><col width="130">'
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Walton production · {e(dg["day_label"])}</title></head>
<body style="margin:0;padding:0;background:#f3f4f6">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6"><tr><td align="center" style="padding:16px 8px">
<table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:680px;width:100%;background:#ffffff;border:1px solid #e6e8eb;border-radius:8px">
<tr><td style="padding:22px 24px 6px">
  <div {small}>Walton production · daily digest</div>
  <div style="{F}font-size:24px;font-weight:bold;color:#1a1d21;padding:2px 0 4px">{e(dg["day_label"])}</div>
  <div style="{F}font-size:15px;color:#1a1d21"><b>{n(dg["day_total"]) or "0"} lbs</b> produced{delta}</div>
  <div {small} style="padding-top:6px"><a href="{dg["url"]}" style="color:#1f6feb">Production dashboard</a>{page_link}</div>
</td></tr>
<tr><td style="padding:0 24px">
  <div {h2}>By shift and machine</div>
  <table width="632" cellspacing="0" cellpadding="0" style="border-collapse:collapse;table-layout:fixed;width:632px">{cols}<tr><th {head}>machine</th><th {headn}>lbs</th><th {headn}>mach h</th><th {headn}>man h</th><th {head}>operators</th><th {head}>material</th></tr>{body_rows}</table>
  <div {small} style="padding-top:6px">Pounds from cieTrade converting jobs; hours, operators, material, downtime and comments from the End of Shift forms.</div>
</td></tr>
<tr><td style="padding:0 24px">
  <div {h2}>Week at a glance <span style="font-weight:normal;color:#6b7280;font-size:13px">week of {e(datetime.strptime(wk["monday"], "%Y-%m-%d").strftime("%b %-d"))}</span></div>
  <table width="632" cellspacing="0" cellpadding="0" style="border-collapse:collapse;width:632px"><tr><th {head}>machine</th>{"".join(f"<th {headn}>{e(x)}</th>" for x in wk["days"])}<th {headn}>week to date</th></tr>{wrows}</table>
</td></tr>
<tr><td style="padding:0 24px 8px">
  <div {h2}>Weekly metrics by machine <span style="font-weight:normal;color:#6b7280;font-size:13px">actual output, 4-week average, last 20 weeks</span></div>
  <img src="{image_src}" alt="Weekly output by machine, 4-week average" width="632" style="display:block;width:100%;max-width:632px;height:auto;border:1px solid #e6e8eb;border-radius:6px">
  <div {small} style="padding-top:6px">The same chart as the dashboard's Weekly Metrics by Machine, default view.</div>
</td></tr>
<tr><td style="padding:14px 24px 20px;border-top:1px solid #e6e8eb">
  <div {small}>Built {e(dg["generated"])} from cieTrade polls through {e(str(dg["status"]["last_poll"] or "")[:16].replace("T", " "))}. <a href="{dg["url"]}" style="color:#1f6feb">Production dashboard</a> for the live view and every shift's full form.</div>
</td></tr>
</table></td></tr></table></body></html>"""


# ---------------------------------------------------------------- hosted page (docs/digest/<date>.html)

def render_page(dg: dict, chart_html: str, days: list[str], png_name: str | None = None) -> str:
    """The digest with the site's own look: shift tabs, a day picker, the live Plotly figure."""
    from dashboard_common import BASE_CSS, CARD_CSS
    e = _h.escape
    def n(v):
        return "" if v in (None, "", 0, 0.0) else f"{int(round(float(v))):,}"
    cell, num, bar = 'class="c"', 'class="num"', 'class="bar" style="background:var(--brand-soft);font-weight:600"'
    sub = 'class="c sub"'
    tabs = "".join(f'<button class="seg-btn{" active" if i == 0 else ""}" data-tab="{t}">{t}</button>' for i, t in enumerate(["All shifts", "1st", "2nd", "3rd"]))
    thead = '<thead><tr><th>machine</th><th class="num">lbs</th><th class="num">machine h</th><th class="num">man h</th><th>operators</th><th>material</th></tr></thead>'
    panels = []
    for t in ["All shifts", "1st", "2nd", "3rd"]:
        rows = _shift_table_rows(dg, e, cell, num, sub, bar, shifts=None if t == "All shifts" else [t])
        panels.append(f'<div class="panel" data-panel="{t}"{"" if t == "All shifts" else " hidden"}><div class="table-wrap"><table class="sheet">{thead}<tbody>{rows}</tbody></table></div></div>')
    wk = dg["week"]
    wrows = "".join(f'<tr><td>{e(r["machine"])}</td>' + "".join(f'<td class="num">{n(v)}</td>' for v in r["days"]) + f'<td class="num total">{n(r["wtd"])}</td></tr>' for r in wk["rows"])
    wrows += f'<tr class="total"><td>Plant</td>' + "".join('<td class="num"></td>' for _ in wk["days"]) + f'<td class="num total">{n(wk["total"])}</td></tr>'
    opts = "".join(f'<option value="{d}"{" selected" if d == dg["date"] else ""}>{datetime.strptime(d, "%Y-%m-%d").strftime("%a %b %-d")}</option>' for d in sorted(days, reverse=True))
    delta = ""
    if dg["day_avg4"]:
        pct = 100 * (dg["day_total"] - dg["day_avg4"]) / dg["day_avg4"]
        delta = f' · <span style="color:{"var(--good)" if pct >= 0 else "#c0392b"};font-weight:600">{pct:+.0f}%</span> vs the 4-week average for a {e(dg["day_label"].split(",")[0])} ({n(dg["day_avg4"])} lbs)'
    filed = sum(1 for v in dg["eos"]["filed"].values() if v)
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Walton production · {e(dg["day_label"])}</title>
<style>
{BASE_CSS}
{CARD_CSS}
    .topbar {{ display:flex; flex-wrap:wrap; gap:10px 18px; align-items:center; margin:0 0 18px; font-size:13px; }}
    .topbar a {{ color:var(--brand); font-weight:600; text-decoration:none; }} .topbar a:hover {{ text-decoration:underline; }}
    .sel {{ font:inherit; font-size:13px; font-weight:600; color:var(--text); background:var(--card); border:1px solid var(--border); border-radius:8px; padding:7px 10px; }}
    .seg {{ display:inline-flex; gap:4px; align-items:center; flex-wrap:wrap; }}
    .seg-btn {{ font:inherit; font-size:12px; font-weight:600; color:var(--text); background:transparent; border:1px solid var(--border); border-radius:8px; padding:6px 10px; cursor:pointer; }}
    .seg-btn:hover {{ background:var(--brand-soft); }} .seg-btn.active {{ background:var(--brand); color:var(--card); border-color:var(--brand); }}
    .table-wrap {{ overflow-x:auto; }}
    table.sheet, table.wk {{ width:100%; border-collapse:collapse; font-size:13px; }}
    table.sheet th, table.wk th {{ font-size:11px; color:var(--muted); font-weight:600; text-align:left; padding:6px 8px; text-transform:uppercase; letter-spacing:.03em; border-bottom:2px solid var(--border); }}
    table.sheet td, table.wk td {{ padding:7px 8px; border-bottom:1px solid var(--border); vertical-align:top; }}
    table.sheet td.bar {{ padding:8px; }} table.sheet td.sub {{ padding-top:0; padding-left:18px; font-size:12px; color:var(--muted); }}
    td.num, th.num {{ text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }}
    table.wk tr.total td {{ font-weight:600; border-top:2px solid var(--border); }} table.wk td.total {{ font-weight:600; border-left:1px solid var(--border); }}
    .kpi {{ font-size:15px; margin:0 0 4px; }} .kpi b {{ font-size:22px; }}
    .foot {{ color:var(--muted); font-size:12px; margin-top:8px; }}
</style></head>
<body>
<header>
  <p class="eyebrow">Walton production · daily digest</p>
  <h1>{e(dg["day_label"])}</h1>
  <p class="kpi"><b>{n(dg["day_total"]) or "0"} lbs</b> produced{delta}</p>
  <p class="subtitle">End of Shift reports filed for {filed} of 3 shifts · built {e(dg["generated"])} from cieTrade polls through {e(str(dg["status"]["last_poll"] or "")[:16].replace("T", " "))}</p>
  <div class="topbar">
    <a href="../">← Production dashboard</a>
    <label>Day <select class="sel" id="dayPick" aria-label="Pick a digest day">{opts}</select></label>
    {'<a href="' + png_name + '">Chart as PNG</a>' if png_name else ""}
  </div>
</header>
<main>
  <section class="card" id="shiftCard">
    <div class="card-head"><div><h2>By shift and machine</h2><p class="lede">Pounds from cieTrade converting jobs; hours, operators, material, downtime and comments from the End of Shift forms.</p></div>
      <div class="seg" role="tablist" aria-label="Shift">{tabs}</div></div>
    {"".join(panels)}
  </section>
  <section class="card" id="weekCard">
    <div class="card-head"><div><h2>Week at a glance</h2><p class="lede">Week of {e(datetime.strptime(wk["monday"], "%Y-%m-%d").strftime("%B %-d"))}, pounds per machine per day through {e(dg["day_label"].split(",")[0])}.</p></div></div>
    <div class="table-wrap"><table class="wk"><thead><tr><th>machine</th>{"".join(f'<th class="num">{e(x)}</th>' for x in wk["days"])}<th class="num">week to date</th></tr></thead><tbody>{wrows}</tbody></table></div>
  </section>
  <section class="card" id="chartCard">
    <div class="card-head"><div><h2>Weekly metrics by machine</h2><p class="lede">The dashboard's weekly chart: actual output per machine, 4-week average, last 20 weeks. Hover for values; click a machine in the legend to hide it.</p></div></div>
    {chart_html}
  </section>
</main>
<footer><p class="foot">One page per production day. The email version of this digest carries the same tables and a PNG of the chart.</p></footer>
<script>
(function(){{
  var btns=document.querySelectorAll('#shiftCard .seg-btn'), panels=document.querySelectorAll('#shiftCard .panel');
  btns.forEach(function(b){{b.addEventListener('click',function(){{btns.forEach(function(x){{x.classList.toggle('active',x===b);}});panels.forEach(function(p){{p.hidden=p.dataset.panel!==b.dataset.tab;}});}});}});
  var s=document.getElementById('dayPick'); if(s) s.addEventListener('change',function(){{location.href=s.value+'.html';}});
}})();
</script>
</body></html>"""


# ---------------------------------------------------------------- send (Gmail API, send scope only)

def gmail_send_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    raw = os.environ.get("GMAIL_SEND_TOKEN_JSON")
    creds = Credentials.from_authorized_user_info(json.loads(raw), SEND_SCOPES) if raw else (
        Credentials.from_authorized_user_file(str(SEND_TOKEN_PATH), SEND_SCOPES) if SEND_TOKEN_PATH.exists() else None)
    if creds is None:
        raise FileNotFoundError(f"no send token: run `python3 src/daily_digest.py --authorize` once (writes {SEND_TOKEN_PATH})")
    if not creds.valid and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def authorize() -> Path:
    from google_auth_oauthlib.flow import InstalledAppFlow
    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SEND_SCOPES)
    creds = flow.run_local_server(port=0)
    SEND_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    SEND_TOKEN_PATH.write_text(creds.to_json()); SEND_TOKEN_PATH.chmod(0o600)
    return SEND_TOKEN_PATH


def build_message(to: list[str], subject: str, html: str, png: Path, sender: str = "me") -> dict:
    msg = MIMEMultipart("related")
    msg["To"], msg["Subject"] = ", ".join(to), subject
    alt = MIMEMultipart("alternative"); msg.attach(alt)
    alt.attach(MIMEText("Walton daily production digest. Open in an HTML mail client, or see " + DASHBOARD_URL, "plain"))
    alt.attach(MIMEText(html, "html"))
    img = MIMEImage(png.read_bytes(), _subtype="png"); img.add_header("Content-ID", "<trend.png>"); img.add_header("Content-Disposition", "inline", filename="trend.png")
    msg.attach(img)
    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode()}


def send(to: list[str], subject: str, html: str, png: Path) -> str:
    svc = gmail_send_service()
    r = svc.users().messages().send(userId="me", body=build_message(to, subject, html, png)).execute()
    return r.get("id", "")


def due_now(state_path: Path = STATE_PATH, now: datetime | None = None, send_hour: int = 6) -> tuple[bool, str]:
    """Once per calendar day, at or after send_hour local time."""
    now = now or datetime.now()
    today = now.strftime("%Y-%m-%d")
    if now.hour < send_hour:
        return False, f"before {send_hour:02d}:00"
    st = json.loads(state_path.read_text()) if state_path.exists() else {}
    if st.get("last_sent") == today:
        return False, "already sent today"
    return True, "due"


def mark_sent(state_path: Path = STATE_PATH, now: datetime | None = None) -> None:
    now = now or datetime.now()
    state_path.write_text(json.dumps({"last_sent": now.strftime("%Y-%m-%d"), "at": now.strftime("%Y-%m-%dT%H:%M:%S")}, indent=1) + "\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--date", help="production day (default: yesterday, or the latest day with rows before it)")
    ap.add_argument("--out", type=Path, default=OUT_DIR)
    ap.add_argument("--send", action="store_true", help="send through the Gmail API")
    ap.add_argument("--to", help="comma-separated recipients (default: env DIGEST_TO)")
    ap.add_argument("--send-if-due", action="store_true", help="send once per day after DIGEST_SEND_HOUR (default 6) local time")
    ap.add_argument("--authorize", action="store_true", help="one-time browser consent for gmail.send")
    ap.add_argument("--publish", action="store_true", help="also write docs/digest/<date>.html, .png and latest.html for the site")
    args = ap.parse_args(argv)
    if args.authorize:
        print(f"token saved to {authorize()}"); return 0
    if args.send_if_due:
        ok, why = due_now(send_hour=int(os.environ.get("DIGEST_SEND_HOUR", "6")))
        if not ok:
            print(f"digest not sent: {why}"); return 0
    df, status = load_inputs()
    day = pick_day(df, datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None)
    dg = build_digest(df, status, day)
    args.out.mkdir(parents=True, exist_ok=True)
    png, fig = chart_png(df, dg, args.out / f"{day}.png")
    html_path = args.out / f"{day}.html"
    html_path.write_text(render_email(dg, image_src=f"{day}.png"))
    (args.out / f"{day}.json").write_text(json.dumps(dg, indent=1, default=str))
    print(f"{dg['day_label']}: {dg['day_total']:,} lbs, {len(dg['rows'])} machines, EOS {sum(1 for v in dg['eos']['filed'].values() if v)}/3 shifts -> {html_path}")
    page_url = f"{DASHBOARD_URL}digest/{day}.html"
    if args.publish:
        DOCS_DIGEST.mkdir(parents=True, exist_ok=True)
        (DOCS_DIGEST / f"{day}.png").write_bytes(png.read_bytes())
        days = sorted({p.stem for p in DOCS_DIGEST.glob("20??-??-??.html")} | {str(day)})
        chart = figure_html(fig) if fig is not None else f'<img src="{day}.png" alt="Weekly plant output" style="width:100%;height:auto">'
        page = render_page(dg, chart, days, png_name=f"{day}.png")
        (DOCS_DIGEST / f"{day}.html").write_text(page)
        (DOCS_DIGEST / "latest.html").write_text(page)
        print(f"published docs/digest/{day}.html (+ latest.html)")
    if args.send or args.send_if_due:
        to = [x.strip() for x in (args.to or os.environ.get("DIGEST_TO", "")).split(",") if x.strip()]
        if not to:
            print("no recipients (use --to or DIGEST_TO)"); return 2
        mid = send(to, f"Walton production · {dg['day_label']} · {dg['day_total']:,} lbs", render_email(dg, page_url=page_url), png)
        print(f"sent {mid} to {', '.join(to)}")
        if args.send_if_due:
            mark_sent()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
