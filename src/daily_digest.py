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
data/cietrade_status.json (End of Shift reports). The chart is drawn with Pillow,
no browser or plotting server needed.
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
    downtime = [{"shift": r["shift"], "machine": m["machine"], "min": m["downtime_min"], "reason": m.get("reason", ""), "comment": m.get("comment", ""), "operators": m.get("operators", "")}
                for r in reports for m in r["machines"] if m.get("downtime_min") or m.get("comment")]
    notes = [{"shift": r["shift"], "note": n} for r in reports for n in r.get("notes", [])]
    eos_hours = sum(m["machine_hours"] for r in reports for m in r["machines"])
    eos_man = sum(m["man_hours"] for r in reports for m in r["machines"])
    return {"date": for_date.isoformat(), "day_label": day.strftime("%A, %B %-d"), "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "day_total": day_total, "day_avg4": day_avg4, "rows": rows, "week": {"monday": monday.strftime("%Y-%m-%d"), "days": [x.strftime("%a %-d") for x in wdays],
            "rows": week_rows, "total": week_total, "workdays_elapsed": workdays_elapsed},
            "trend": trend, "machine_trend": machine_trend,
            "eos": {"filed": {s: (filed[s]["by"] if s in filed else None) for s in SHIFTS}, "machine_h": round(eos_hours, 1), "man_h": round(eos_man, 1),
                    "downtime": downtime, "notes": notes, "downtime_total": sum(x["min"] for x in downtime)},
            "status": {"last_poll": status.get("last_poll"), "awaiting": [a for a in status.get("awaiting", []) if a and a[0] == for_date.isoformat()]},
            "url": DASHBOARD_URL}


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


def render_email(dg: dict, image_src: str = "cid:trend.png") -> str:
    e = _h.escape
    td = 'style="padding:6px 8px;border-bottom:1px solid #e3e6ea;font-size:13px"'
    tdn = 'style="padding:6px 8px;border-bottom:1px solid #e3e6ea;font-size:13px;text-align:right;font-variant-numeric:tabular-nums"'
    th = 'style="padding:6px 8px;border-bottom:2px solid #e3e6ea;font-size:11px;color:#66707c;text-transform:uppercase;letter-spacing:.04em;text-align:left"'
    thn = th.replace("text-align:left", "text-align:right")
    h2 = 'style="font-size:16px;margin:26px 0 8px;color:#1a1d21"'
    tile = 'style="display:inline-block;min-width:130px;padding:10px 14px;margin:0 8px 8px 0;border:1px solid #e3e6ea;border-radius:10px;vertical-align:top"'
    eos = dg["eos"]
    filed_txt = " · ".join(f"{s}: <b>{e(by)}</b>" if by else f'{s}: <span style="color:#b42318">missing</span>' for s, by in eos["filed"].items())
    tiles = "".join(f'<div {tile}><div style="font-size:22px;font-weight:700">{v}</div><div style="font-size:12px;color:#66707c">{l}</div></div>'
                    for v, l in [(f'{_n(dg["day_total"])} lbs', "produced"), (_delta(dg["day_total"], dg["day_avg4"]), "against the same weekday"),
                                 (f'{eos["machine_h"]:g} h', "machine hours reported"), (f'{eos["man_h"]:g} h', "man hours reported"),
                                 (f'{eos["downtime_total"]} min', "downtime reported")])
    mrows = "".join(f'<tr><td {td}><b>{e(r["machine"])}</b></td>' + "".join(f'<td {tdn}>{_n(v) if v else "–"}</td>' for v in r["shifts"])
                    + f'<td {tdn}><b>{_n(r["total"])}</b></td><td {tdn}>{_n(r["avg4"]) if r["avg4"] else "–"}</td>'
                    + f'<td {tdn}>{r["machine_h"]:g}</td><td {tdn}>{r["man_h"]:g}</td><td {tdn}>{_n(r["lbs_per_mh"]) if r["lbs_per_mh"] else "–"}</td></tr>' for r in dg["rows"])
    wk = dg["week"]
    wrows = "".join(f'<tr><td {td}><b>{e(r["machine"])}</b></td>' + "".join(f'<td {tdn}>{_n(v) if v else "–"}</td>' for v in r["days"])
                    + f'<td {tdn}><b>{_n(r["wtd"])}</b></td><td {tdn}>{(_n(r["pace"]) + (" ✓" if r["wtd"] >= r["pace"] else "")) if r["pace"] else "–"}</td></tr>' for r in wk["rows"])
    dt = "".join(f'<li style="margin:4px 0"><span style="color:#66707c">{e(x["shift"])} · {e(x["machine"])}</span> '
                 + (f'<b>{x["min"]} min down</b>' + (f' ({e(x["reason"])})' if x["reason"] else "") if x["min"] else "")
                 + (f' {e(x["comment"])}' if x["comment"] else "") + (f' <span style="color:#66707c">— {e(x["operators"])}</span>' if x["operators"] else "") + "</li>" for x in eos["downtime"])
    notes = "".join(f'<li style="margin:4px 0"><span style="color:#66707c">{e(n["shift"])}</span> {e(n["note"])}</li>' for n in eos["notes"])
    trows = "".join(f'<tr><td {td}><b>{e(r["machine"])}</b></td><td {tdn}>{_n(r["last_week"])}</td><td {tdn}>{_n(r["avg4"])}</td><td {tdn}>{_n(r["wtd"])}</td><td {tdn}>{_n(r["target"]) if r["target"] else "–"}</td></tr>' for r in dg["machine_trend"])
    awaiting = len(dg["status"]["awaiting"])
    return f"""<!DOCTYPE html><html><body style="margin:0;background:#f6f7f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#1a1d21">
<div style="max-width:720px;margin:0 auto;background:#fff;padding:24px 28px">
<div style="font-size:12px;color:#66707c">Walton production · daily digest</div>
<h1 style="font-size:22px;margin:4px 0 2px">{e(dg["day_label"])}</h1>
<div style="font-size:13px;color:#66707c;margin-bottom:14px">Full detail on the <a href="{dg["url"]}" style="color:#1f6feb">production dashboard</a> · End of Shift: {filed_txt}</div>
<div>{tiles}</div>
<h2 {h2}>Yesterday by machine</h2>
<table cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse"><tr><th {th}>machine</th><th {thn}>1st</th><th {thn}>2nd</th><th {thn}>3rd</th><th {thn}>day lbs</th><th {thn}>4-wk avg</th><th {thn}>machine h</th><th {thn}>man h</th><th {thn}>lbs / mach h</th></tr>{mrows}
<tr><td {td}><b>Plant</b></td><td {tdn}></td><td {tdn}></td><td {tdn}></td><td {tdn}><b>{_n(dg["day_total"])}</b></td><td {tdn}>{_n(dg["day_avg4"])}</td><td {tdn}>{eos["machine_h"]:g}</td><td {tdn}>{eos["man_h"]:g}</td><td {tdn}></td></tr></table>
<div style="font-size:12px;color:#66707c;margin-top:6px">Pounds from cieTrade converting jobs; hours from the End of Shift forms.{" " + str(awaiting) + " shift-machine cells were still awaiting a poll or posting when this was built." if awaiting else ""}</div>
<h2 {h2}>Week at a glance <span style="font-weight:400;color:#66707c;font-size:13px">week of {e(datetime.strptime(wk["monday"], "%Y-%m-%d").strftime("%b %-d"))}</span></h2>
<table cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse"><tr><th {th}>machine</th>{"".join(f"<th {thn}>{e(x)}</th>" for x in wk["days"])}<th {thn}>week to date</th><th {thn}>target pace</th></tr>{wrows}
<tr><td {td}><b>Plant</b></td>{"".join(f"<td {tdn}></td>" for _ in wk["days"])}<td {tdn}><b>{_n(wk["total"])}</b></td><td {tdn}></td></tr></table>
<div style="font-size:12px;color:#66707c;margin-top:6px">Target pace = weekly target × {wk["workdays_elapsed"]}/5 workdays elapsed.</div>
<h2 {h2}>End of Shift reports</h2>
<div style="font-size:13px"><b>Downtime and comments</b><ul style="margin:6px 0 10px 18px;padding:0">{dt or '<li style="color:#66707c">none reported</li>'}</ul>
<b>Shift notes</b><ul style="margin:6px 0 0 18px;padding:0">{notes or '<li style="color:#66707c">none</li>'}</ul></div>
<h2 {h2}>Weekly trend</h2>
<img src="{image_src}" alt="Weekly plant output with 4-week average" width="700" style="width:100%;max-width:700px;height:auto;border:1px solid #e3e6ea;border-radius:8px">
<table cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse;margin-top:10px"><tr><th {th}>machine</th><th {thn}>last week</th><th {thn}>4-wk avg</th><th {thn}>this week to date</th><th {thn}>weekly target</th></tr>{trows}</table>
<div style="font-size:12px;color:#66707c;margin-top:18px;border-top:1px solid #e3e6ea;padding-top:10px">Built {e(dg["generated"])} from cieTrade polls through {e(str(dg["status"]["last_poll"] or "")[:16].replace("T", " "))}. <a href="{dg["url"]}" style="color:#1f6feb">Open the production dashboard</a> for the live view, every shift's form and the week table.</div>
</div></body></html>"""


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
    png = draw_trend_png(dg["trend"], args.out / f"{day}.png")
    html_path = args.out / f"{day}.html"
    html_path.write_text(render_email(dg, image_src=f"{day}.png"))
    (args.out / f"{day}.json").write_text(json.dumps(dg, indent=1, default=str))
    print(f"{dg['day_label']}: {dg['day_total']:,} lbs, {len(dg['rows'])} machines, EOS {sum(1 for v in dg['eos']['filed'].values() if v)}/3 shifts -> {html_path}")
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
