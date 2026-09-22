"""Daily production email: yesterday, in three parts.

1. Week at a glance, one card per shift (1st, 2nd, 3rd): pounds per machine per
   day for the current week through yesterday, week-to-date, plant total.
2. Yesterday's three End of Shift reports, each laid out like the dashboard's
   submitted forms (machine, machine h, man h, operators, material, downtime,
   reason, comments, shift notes). A shift nobody filed says so.
3. The dashboard's "Weekly Metrics by Machine" chart (Actual Output, 4-wk
   average, last 20 weeks) as an inline image.

Everything else lives on the dashboard, and the email says so with a link.

    python3 src/daily_digest.py                       # preview: reports/digest/<date>.html and .png for yesterday (gitignored)
    python3 src/daily_digest.py --date 2026-09-21     # a specific production day
    python3 src/daily_digest.py --chart docs/charts/weekly_metrics.png   # just the chart PNG (the workflow does this every poll)
    python3 src/daily_digest.py --send --to you@x.com # build and send through the Gmail API
    python3 src/daily_digest.py --send-if-due         # cloud: send once a day after DIGEST_SEND_HOUR local (recipients from DIGEST_TO)
    python3 src/daily_digest.py --authorize           # one-time consent for the gmail.send scope (browser)

Inputs: data/aggregated_daily_data.xlsx (pounds per shift-day-machine) and
data/cietrade_status.json (End of Shift reports). The chart is exported with
kaleido; if the exporter cannot start, a Pillow chart of plant output stands in.
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
from config import DATA_DIR, PROJECT_ROOT, WALTON_CONFIG_DIR  # noqa: E402

AGG_PATH = DATA_DIR / "aggregated_daily_data.xlsx"
STATUS_PATH = DATA_DIR / "cietrade_status.json"
OUT_DIR = PROJECT_ROOT / "reports" / "digest"
STATE_PATH = DATA_DIR / "digest_state.json"
DASHBOARD_URL = "https://zabdulla.github.io/walton_production/"
CHART_URL = DASHBOARD_URL + "charts/weekly_metrics.png"
SEND_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
SEND_TOKEN_PATH = WALTON_CONFIG_DIR / "gmail_send_token.json"
CREDENTIALS_PATH = WALTON_CONFIG_DIR / "gmail_credentials.json"
SHIFTS = ("1st", "2nd", "3rd")


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
    d["Shift"] = d["Shift"].astype(str)
    day = pd.Timestamp(for_date)
    monday = day - pd.Timedelta(days=day.weekday())
    wdays = [monday + pd.Timedelta(days=i) for i in range((day - monday).days + 1)]
    wk = d[(d["Date"] >= monday) & (d["Date"] <= day)]
    # week at a glance, one block per shift
    week_by_shift = []
    for sh in SHIFTS:
        s = wk[wk["Shift"] == sh]
        piv = s.pivot_table(index="Machine_Name", columns="Date", values="lbs", aggfunc="sum").reindex(columns=wdays).fillna(0.0)
        piv = piv[piv.sum(axis=1) > 0]
        rows = [{"machine": m, "days": [round(float(v)) for v in piv.loc[m]], "wtd": round(float(piv.loc[m].sum()))}
                for m in piv.sum(axis=1).sort_values(ascending=False).index]
        totals = [round(float(v)) for v in piv.sum(axis=0)] if len(piv) else [0 for _ in wdays]
        week_by_shift.append({"shift": sh, "rows": rows, "totals": totals, "wtd": sum(totals), "day": totals[-1] if totals else 0})
    plant_days = [round(float(wk[wk["Date"] == x]["lbs"].sum())) for x in wdays]
    # yesterday's End of Shift reports, as filed
    eos = status.get("end_of_shift") or {}
    filed = {r["shift"]: r for r in eos.get("reports", []) if r.get("date") == for_date.isoformat()}
    reports = []
    for sh in SHIFTS:
        r = filed.get(sh)
        if not r:
            reports.append({"shift": sh, "filed": False}); continue
        machines = [{"machine": m.get("machine", ""), "machine_hours": m.get("machine_hours"), "man_hours": m.get("man_hours"),
                     "operators": m.get("operators", "") or "", "material": m.get("material", "") or "",
                     "downtime_min": m.get("downtime_min") or 0, "reason": m.get("reason", "") or "", "comment": m.get("comment", "") or ""}
                    for m in r.get("machines", [])]
        reports.append({"shift": sh, "filed": True, "by": r.get("by", ""), "filed_at": r.get("filed_at", ""), "machines": machines,
                        "notes": list(r.get("notes", [])),
                        "machine_h": round(sum((m["machine_hours"] or 0) for m in machines), 2),
                        "man_h": round(sum((m["man_hours"] or 0) for m in machines), 2),
                        "downtime_min": int(sum(m["downtime_min"] for m in machines))})
    # plant output by week, for the fallback chart only
    hist = d[d["Date"] <= day].copy()
    hist["wk"] = hist["Date"] - pd.to_timedelta(hist["Date"].dt.weekday, unit="D")
    weekly = hist.groupby("wk")["lbs"].sum().sort_index().tail(8)
    completed = weekly[weekly.index < monday]
    avg_line = completed.rolling(4, min_periods=1).mean()
    trend = [{"week": w.strftime("%Y-%m-%d"), "lbs": round(float(v)), "partial": bool(w == monday),
              "avg4": (round(float(avg_line.get(w))) if w in avg_line.index else None)} for w, v in weekly.items()]
    return {"date": for_date.isoformat(), "day_label": day.strftime("%A, %B %-d"), "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "day_total": plant_days[-1] if plant_days else 0,
            "week": {"monday": monday.strftime("%Y-%m-%d"), "days": [x.strftime("%a %-d") for x in wdays], "plant_days": plant_days,
                     "plant_wtd": sum(plant_days), "by_shift": week_by_shift},
            "reports": reports, "trend": trend,
            "status": {"last_poll": status.get("last_poll")}, "url": DASHBOARD_URL}


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


def chart_png(df: pd.DataFrame, trend: list[dict], path: Path) -> Path:
    """The dashboard figure as a PNG, or the Pillow fallback."""
    try:
        return write_figure_png(weekly_figure(df), path)
    except Exception as exc:  # kaleido missing or its chromium cannot start on this host
        print(f"kaleido export failed ({exc.__class__.__name__}: {str(exc)[:120]}); using the Pillow fallback chart")
        return draw_trend_png(trend, path)


# ---------------------------------------------------------------- fallback chart (Pillow)

def draw_trend_png(trend: list[dict], path: Path, width: int = 1000, height: int = 380) -> Path:
    from PIL import Image, ImageDraw, ImageFont

    def font(size: int, bold: bool = False):
        for name in (["DejaVuSans-Bold.ttf", "Arial Bold.ttf"] if bold else ["DejaVuSans.ttf", "Arial.ttf"]):
            try:
                return ImageFont.truetype(name, size)
            except OSError:
                continue
        return ImageFont.load_default()

    img = Image.new("RGB", (width, height), "white")
    dr = ImageDraw.Draw(img)
    left, right, top, bottom = 90, 30, 40, 70
    pw, ph = width - left - right, height - top - bottom
    vals = [t["lbs"] for t in trend] + [t["avg4"] or 0 for t in trend]
    ymax = max(vals + [1]) * 1.15

    def y(v): return top + ph - ph * v / ymax
    for i in range(5):
        v = ymax * i / 4
        dr.line([(left, y(v)), (width - right, y(v))], fill="#e5e7eb", width=1)
        dr.text((left - 10, y(v)), f"{v / 1000:,.0f}k", fill="#6b7280", font=font(13), anchor="rm")
    n = max(len(trend), 1)
    slot = pw / n
    bw = slot * 0.6
    pts = []
    for i, t in enumerate(trend):
        x0 = left + slot * i + (slot - bw) / 2
        col = "#9ec5b5" if t["partial"] else "#0b6e4f"
        dr.rectangle([x0, y(t["lbs"]), x0 + bw, y(0)], fill=col)
        dr.text((x0 + bw / 2, y(t["lbs"]) - 6), f"{t['lbs'] / 1000:,.0f}k", fill="#111827", font=font(12), anchor="mb")
        dr.text((x0 + bw / 2, top + ph + 8), datetime.strptime(t["week"], "%Y-%m-%d").strftime("%b %-d"), fill="#6b7280", font=font(12), anchor="mt")
        if t["avg4"] is not None:
            pts.append((x0 + bw / 2, y(t["avg4"])))
    if len(pts) > 1:
        dr.line(pts, fill="#eb6834", width=3)
        for p in pts:
            dr.ellipse([p[0] - 4, p[1] - 4, p[0] + 4, p[1] + 4], fill="#eb6834")
    dr.text((left, 12), "Weekly plant output (lbs)", fill="#111827", font=font(15, True))
    dr.text((width - right, 12), "line: 4-week average of completed weeks", fill="#6b7280", font=font(12), anchor="ra")
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, optimize=True)
    return path


# ---------------------------------------------------------------- email (inline styles only: mail clients strip stylesheets)

F = "font-family:Arial,Helvetica,sans-serif;"
_TH = f'style="{F}font-size:11px;color:#6b7280;padding:6px 8px;border-bottom:2px solid #d9dde2;text-align:left;text-transform:uppercase;white-space:nowrap"'
_THN = _TH.replace("text-align:left", "text-align:right")
_TD = f'style="{F}font-size:13px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top"'
_TDN = f'style="{F}font-size:14px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top;text-align:right;white-space:nowrap"'
_TDM = f'style="{F}font-size:13px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top;white-space:nowrap;font-weight:bold"'
_SMALL = f'style="{F}font-size:12px;color:#6b7280"'


def _n(v) -> str:
    return "" if v in (None, "", 0, 0.0) else f"{int(round(float(v))):,}"


def _hrs(v) -> str:
    return "" if v in (None, "", 0, 0.0) else f"{float(v):g}"


def _card(title: str, meta: str, body: str, head_bg: str = "#e7f3ee") -> str:
    """A bordered card with a header band, the way the dashboard frames a section."""
    return (f'<table width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #d9dde2;border-radius:10px;margin:0 0 14px;border-collapse:separate">'
            f'<tr><td style="{F}background:{head_bg};padding:10px 14px;border-radius:10px 10px 0 0;border-bottom:1px solid #d9dde2">'
            f'<span style="font-size:15px;font-weight:bold;color:#1a1d21">{title}</span>'
            + (f' <span style="font-size:12px;color:#4b5563">&nbsp;·&nbsp; {meta}</span>' if meta else "")
            + f'</td></tr><tr><td style="padding:4px 8px 8px">{body}</td></tr></table>')


def _week_card(block: dict, wk: dict, e) -> str:
    ncols = len(wk["days"])
    if not block["rows"]:
        body = f'<div {_SMALL} style="padding:8px 6px">No production booked to this shift this week.</div>'
        return _card(f'{e(block["shift"])} shift', "", body)
    rows = "".join(f'<tr><td {_TDM}>{e(r["machine"])}</td>' + "".join(f'<td {_TDN}>{_n(v)}</td>' for v in r["days"])
                   + f'<td {_TDN}><b>{_n(r["wtd"])}</b></td></tr>' for r in block["rows"])
    rows += (f'<tr><td {_TDM.replace("border-bottom:1px solid #e6e8eb", "border-top:2px solid #d9dde2;border-bottom:0")}>Shift total</td>'
             + "".join(f'<td {_TDN.replace("border-bottom:1px solid #e6e8eb", "border-top:2px solid #d9dde2;border-bottom:0")}><b>{_n(v)}</b></td>' for v in block["totals"])
             + f'<td {_TDN.replace("border-bottom:1px solid #e6e8eb", "border-top:2px solid #d9dde2;border-bottom:0")}><b>{_n(block["wtd"])}</b></td></tr>')
    head = f'<tr><th {_TH}>machine</th>' + "".join(f'<th {_THN}>{e(x)}</th>' for x in wk["days"]) + f'<th {_THN}>week to date</th></tr>'
    table = f'<table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse">{head}{rows}</table>'
    meta = f'{_n(block["day"]) or "0"} lbs {e(wk["days"][-1])} &nbsp;·&nbsp; {_n(block["wtd"]) or "0"} lbs week to date'
    return _card(f'{e(block["shift"])} shift', meta, table)


def _report_card(r: dict, e) -> str:
    if not r["filed"]:
        return _card(f'{e(r["shift"])} shift', "", f'<div style="{F}font-size:13px;color:#c0392b;padding:8px 6px">No End of Shift report was filed for this shift.</div>', head_bg="#fdf2f2")
    cols = '<col width="116"><col width="50"><col width="46"><col width="94"><col width="86"><col width="40"><col width="74"><col width="126">'
    head = (f'<tr><th {_TH}>machine</th><th {_THN}>mach h</th><th {_THN}>man h</th><th {_TH}>operators</th><th {_TH}>material</th>'
            f'<th {_THN}>down</th><th {_TH}>reason</th><th {_TH}>comments</th></tr>')
    tdw = _TD.replace('vertical-align:top"', 'vertical-align:top;font-size:12px"')
    tdm = _TD.replace('vertical-align:top"', 'vertical-align:top;font-weight:bold;font-size:12px"')   # may wrap: long machine names
    rows = "".join(f'<tr><td {tdm}>{e(m["machine"])}</td><td {_TDN}>{_hrs(m["machine_hours"])}</td><td {_TDN}>{_hrs(m["man_hours"])}</td>'
                   f'<td {tdw}>{e(m["operators"])}</td><td {tdw}>{e(m["material"])}</td>'
                   f'<td {_TDN}>{(str(m["downtime_min"]) if m["downtime_min"] else "")}</td><td {tdw}>{e(m["reason"])}</td><td {tdw}>{e(m["comment"])}</td></tr>'
                   for m in r["machines"])
    table = f'<table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;table-layout:fixed">{cols}{head}{rows}</table>'
    notes = "".join(f'<li style="margin:2px 0">{e(n)}</li>' for n in r["notes"])
    if notes:
        table += f'<div style="{F}font-size:13px;color:#1a1d21;padding:8px 6px 2px"><b>Shift notes</b><ul style="margin:4px 0 0 18px;padding:0">{notes}</ul></div>'
    down = f' &nbsp;·&nbsp; <span style="color:#c0392b"><b>{r["downtime_min"]} min down</b></span>' if r["downtime_min"] else ""
    when = f' at {e(str(r["filed_at"]))}' if r.get("filed_at") else ""
    meta = f'filed by {e(r["by"])}{when} &nbsp;·&nbsp; {len(r["machines"])} machines &nbsp;·&nbsp; {r["machine_h"]:g} machine h &nbsp;·&nbsp; {r["man_h"]:g} man h{down}'
    return _card(f'{e(r["shift"])} shift', meta, table)


def render_email(dg: dict, image_src: str = "cid:trend.png") -> str:
    e = _h.escape
    wk = dg["week"]
    h2 = f'style="{F}font-size:17px;font-weight:bold;color:#1a1d21;margin:0;padding:22px 0 10px"'
    link = (f'<div style="{F}font-size:13px;color:#1a1d21;background:#e7f3ee;border:1px solid #cfe5db;border-radius:8px;padding:10px 14px">'
            f'All the details live on the production dashboard: <a href="{dg["url"]}" style="color:#0b6e4f;font-weight:bold">{dg["url"].replace("https://", "")}</a></div>')
    week_cards = "".join(_week_card(b, wk, e) for b in wk["by_shift"])
    plant = (f'<div style="{F}font-size:13px;color:#1a1d21;padding:2px 6px 6px"><b>Plant</b>: '
             + " &nbsp;·&nbsp; ".join(f'{e(dd)} {_n(v) or "0"}' for dd, v in zip(wk["days"], wk["plant_days"]))
             + f' &nbsp;·&nbsp; <b>week to date {_n(wk["plant_wtd"]) or "0"} lbs</b></div>')
    report_cards = "".join(_report_card(r, e) for r in dg["reports"])
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Walton production · {e(dg["day_label"])}</title></head>
<body style="margin:0;padding:0;background:#f3f4f6">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6"><tr><td align="center" style="padding:16px 8px">
<table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:680px;width:100%;background:#ffffff;border:1px solid #e6e8eb;border-radius:8px">
<tr><td style="padding:22px 24px 6px">
  <div {_SMALL}>Walton production · daily email</div>
  <div style="{F}font-size:24px;font-weight:bold;color:#1a1d21;padding:2px 0 4px">{e(dg["day_label"])}</div>
  <div style="{F}font-size:15px;color:#1a1d21;padding-bottom:12px"><b>{_n(dg["day_total"]) or "0"} lbs</b> produced across three shifts</div>
  {link}
</td></tr>
<tr><td style="padding:0 24px">
  <div {h2}>Week at a glance <span style="font-weight:normal;color:#6b7280;font-size:13px">week of {e(datetime.strptime(wk["monday"], "%Y-%m-%d").strftime("%b %-d"))}, pounds by machine and day</span></div>
  {week_cards}{plant}
</td></tr>
<tr><td style="padding:0 24px">
  <div {h2}>End of Shift reports <span style="font-weight:normal;color:#6b7280;font-size:13px">{e(dg["day_label"])}, as filed by the supervisors</span></div>
  {report_cards}
</td></tr>
<tr><td style="padding:0 24px 8px">
  <div {h2}>Weekly metrics by machine <span style="font-weight:normal;color:#6b7280;font-size:13px">actual output, 4-week average</span></div>
  <img src="{image_src}" alt="Actual Output (Lbs) (4-wk avg) by Machine" width="632" style="display:block;width:100%;max-width:632px;height:auto;border:1px solid #e6e8eb;border-radius:6px">
</td></tr>
<tr><td style="padding:14px 24px 20px;border-top:1px solid #e6e8eb">
  {link}
  <div {_SMALL} style="padding-top:8px">Built {e(dg["generated"])} from cieTrade polls through {e(str(dg["status"]["last_poll"] or "")[:16].replace("T", " "))}. Pounds from cieTrade converting jobs; hours, crew, material, downtime and comments from the End of Shift forms.</div>
</td></tr>
</table></td></tr></table></body></html>"""


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
    alt.attach(MIMEText("Walton daily production email. Open in an HTML mail client, or see " + DASHBOARD_URL, "plain"))
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
    ap.add_argument("--out", type=Path, default=OUT_DIR, help="preview folder for <date>.html and <date>.png (gitignored)")
    ap.add_argument("--chart", type=Path, help="write only the weekly chart PNG here (e.g. docs/charts/weekly_metrics.png) and exit")
    ap.add_argument("--send", action="store_true", help="send through the Gmail API")
    ap.add_argument("--to", help="comma-separated recipients (default: env DIGEST_TO)")
    ap.add_argument("--send-if-due", action="store_true", help="send once per day after DIGEST_SEND_HOUR (default 6) local time")
    ap.add_argument("--authorize", action="store_true", help="one-time browser consent for gmail.send")
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
    if args.chart:
        chart_png(df, dg["trend"], args.chart)
        print(f"chart -> {args.chart}"); return 0
    args.out.mkdir(parents=True, exist_ok=True)
    png = chart_png(df, dg["trend"], args.out / f"{day}.png")
    html_path = args.out / f"{day}.html"
    html_path.write_text(render_email(dg, image_src=f"{day}.png"))
    filed = sum(1 for r in dg["reports"] if r["filed"])
    print(f"{dg['day_label']}: {dg['day_total']:,} lbs, EOS {filed}/3 shifts -> {html_path}")
    if args.send or args.send_if_due:
        to = [x.strip() for x in (args.to or os.environ.get("DIGEST_TO", "")).split(",") if x.strip()]
        if not to:
            print("no recipients (use --to or DIGEST_TO)"); return 2
        mid = send(to, f"Walton production · {dg['day_label']} · {dg['day_total']:,} lbs", render_email(dg), png)
        print(f"sent {mid} to {', '.join(to)}")
        if args.send_if_due:
            mark_sent()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
