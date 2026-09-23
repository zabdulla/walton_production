"""Live production feed from the cieTrade poll log.

The poller appends the full open-jobs list every time it changes. Two consecutive
observations of one job bracket what that job gained; the previous *poll* (changed or
not) narrows the window to one polling interval. From that this module builds
data/cietrade/live.json:

  machines   today's pounds per machine and shift, last change, quiet-time flag
  series     cumulative pounds per machine through the production day (6 AM to 6 AM)
  yesterday  the previous production day's plant curve, for comparison
  changes    the most recent changes, newest first (unchanged polls never appear)

The file is republished to a gist after every poll so the static dashboard can fetch it.
"""
from __future__ import annotations

import glob
import json
import logging
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import (
    CIETRADE_DATA_DIR, CIETRADE_LINE_TO_MACHINE, CIETRADE_SITES, LIVE_FEED_LENGTH, LIVE_GIST_ID,
    LIVE_JSON_PATH, LIVE_QUIET_MINUTES, SHIFT_HOURS,
)

LOG = logging.getLogger("cietrade_live")
SHIFT_RE = re.compile(r"\((\d)(?:ST|ND|RD) SHIFT\)")
SHIFT_NAME = {"1": "1st", "2": "2nd", "3": "3rd"}
DAY_START = timedelta(hours=SHIFT_HOURS["1st"][0])
WORKDAYS = {0, 1, 2, 3, 4}


def load_snapshots(api_dir: Path = CIETRADE_DATA_DIR, sites: set = CIETRADE_SITES) -> pd.DataFrame:
    files = sorted(glob.glob(str(api_dir / "snapshots" / "*.csv")))
    if not files:
        return pd.DataFrame(columns=["ts", "job", "machine", "shift", "line", "qty", "units", "created"])
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df = df[df["Warehouse"].astype(str).str.strip().isin(sites)].copy()
    df["ts"] = pd.to_datetime(df["Snapshot"])
    df["job"] = df["Job No"].astype(int)
    df["line"] = df["Machine"].astype(str).str.replace(r"\s*\(\d(?:ST|ND|RD) SHIFT\)", "", regex=True).str.strip()
    df["machine"] = df["line"].map(CIETRADE_LINE_TO_MACHINE).fillna(df["line"])
    df["shift"] = df["Machine"].astype(str).str.extract(SHIFT_RE)[0].map(SHIFT_NAME)
    df["qty"] = pd.to_numeric(df["Output Qty"], errors="coerce").fillna(0.0)
    df["units"] = pd.to_numeric(df["Output Units"], errors="coerce").fillna(0).astype(int)
    start = pd.to_datetime(df["Start-Time"].astype(str).str.strip(), format="%I:%M%p", errors="coerce")
    df["created"] = pd.to_datetime(df["Job Date"]) + pd.to_timedelta(start.dt.hour.fillna(0) * 60 + start.dt.minute.fillna(0), unit="m")
    return df[["ts", "job", "machine", "shift", "line", "qty", "units", "created"]].sort_values(["job", "ts"]).reset_index(drop=True)


def load_polls(api_dir: Path = CIETRADE_DATA_DIR) -> pd.DataFrame:
    path = api_dir / "polls.jsonl"
    if not path.exists():
        return pd.DataFrame(columns=["ts", "ok", "changed", "error"])
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"])
    return df.sort_values("ts").reset_index(drop=True)


def outage(polls: pd.DataFrame) -> dict:
    """The current cieTrade outage, if the newest poll failed: since when, how many failures, the error."""
    if not len(polls):
        return {"poll_ok": False, "last_ok_poll": None, "api_down_since": None, "failed_polls": 0, "last_error": ""}
    polls = polls.sort_values("ts")
    ok = bool(polls.iloc[-1]["ok"])
    okp = polls[polls["ok"].astype(bool)]
    last_ok = okp["ts"].max() if len(okp) else None
    failed = polls[polls["ts"] > last_ok] if last_ok is not None else polls
    err = polls.iloc[-1].get("error") if "error" in polls.columns else None
    return {"poll_ok": ok, "last_ok_poll": last_ok.isoformat() if last_ok is not None else None,
            "api_down_since": failed["ts"].min().isoformat() if (not ok and len(failed)) else None,
            "failed_polls": int(len(failed)) if not ok else 0,
            "last_error": str(err) if (not ok and err) else ""}


def changes(snaps: pd.DataFrame, polls: pd.DataFrame) -> pd.DataFrame:
    """One row per (job, snapshot) whose quantity or unit count moved.

    ``t1`` is the poll that saw the new value; ``t0`` is the last successful poll before
    it, so the window is one polling interval even when the job's previous snapshot is
    older. A job's first observation counts only if the job was created after the
    previous poll (then its whole quantity is new); otherwise it existed before polling
    began and its history is unknown.
    """
    cols = ["t0", "t1", "job", "machine", "shift", "lbs", "units", "correction"]
    if snaps.empty:
        return pd.DataFrame(columns=cols)
    ok_polls = polls.loc[polls["ok"], "ts"].sort_values().to_numpy() if len(polls) else pd.Series([], dtype="datetime64[ns]").to_numpy()
    s = snaps.sort_values(["job", "ts"]).copy()
    s["prev_qty"] = s.groupby("job")["qty"].shift()
    s["prev_units"] = s.groupby("job")["units"].shift()
    out = []
    for r in s.itertuples(index=False):
        before = ok_polls[ok_polls < r.ts.to_datetime64()]
        prev_poll = pd.Timestamp(before[-1]) if len(before) else None
        t0 = prev_poll
        if pd.isna(r.prev_qty):
            # first sighting: only a job opened since the previous poll has a known history,
            # and its window cannot start before it existed
            if prev_poll is None or pd.isna(r.created) or r.created <= prev_poll or r.qty <= 0:
                continue
            lbs, units, t0 = float(r.qty), int(r.units), max(prev_poll, r.created)
        else:
            lbs, units = float(r.qty - r.prev_qty), int(r.units - r.prev_units)
            if abs(lbs) < 0.5 and units == 0:
                continue
        out.append(dict(t0=t0 if t0 is not None else r.ts, t1=r.ts, job=int(r.job), machine=r.machine,
                        shift=r.shift, lbs=round(lbs), units=units, correction=bool(lbs < 0)))
    df = pd.DataFrame(out, columns=cols)
    return df.sort_values(["t1", "machine"]).reset_index(drop=True)


def production_day(ts: pd.Timestamp) -> pd.Timestamp:
    """The production day runs from 1st-shift start to the next; 5:59 AM belongs to yesterday."""
    return (pd.Timestamp(ts) - DAY_START).normalize()


def current_shift(now: pd.Timestamp) -> str | None:
    h = now.hour + now.minute / 60
    for name, (a, b) in SHIFT_HOURS.items():
        lo, hi = a % 24, b % 24
        if (lo < hi and lo <= h < hi) or (lo >= hi and (h >= lo or h < hi)):
            return name
    return None


def _curve(ch: pd.DataFrame, day_start: pd.Timestamp, end: pd.Timestamp) -> list:
    """[[minutes since day start, cumulative lbs], ...] as a step curve with a final flat point."""
    pts, cum = [[0, 0]], 0.0
    for r in ch.sort_values("t1").itertuples(index=False):
        m = int((r.t1 - day_start).total_seconds() // 60)
        pts.append([m, round(cum)])
        cum += r.lbs
        pts.append([m, round(cum)])
    tail = int((end - day_start).total_seconds() // 60)
    if tail > pts[-1][0]:
        pts.append([tail, round(cum)])
    return pts


def build_live(now: pd.Timestamp | None = None, api_dir: Path = CIETRADE_DATA_DIR) -> dict:
    now = pd.Timestamp(now) if now is not None else pd.Timestamp.now().floor("s")
    snaps, polls = load_snapshots(api_dir), load_polls(api_dir)
    ch = changes(snaps, polls)
    day = production_day(now)
    day_start = day + DAY_START
    today = ch[(ch["t1"] >= day_start) & (ch["t1"] < day_start + timedelta(days=1))]
    yday_start = day_start - timedelta(days=1)
    yesterday = ch[(ch["t1"] >= yday_start) & (ch["t1"] < day_start)]
    last_poll = polls["ts"].max() if len(polls) else None
    out = outage(polls)
    ok = out["poll_ok"]
    end = min(now, last_poll) if last_poll is not None else now
    shift_now = current_shift(now) if now.weekday() in WORKDAYS else None
    machines = []
    for m, g in today.groupby("machine"):
        last = g["t1"].max()
        quiet = int((now - last).total_seconds() // 60)
        this_shift = shift_now is not None and (g["shift"] == shift_now).any()
        machines.append(dict(m=m, lbs=int(g["lbs"].sum()), by_shift={s: int(v) for s, v in g.groupby("shift")["lbs"].sum().items()},
                             changes=int(len(g)), last_change=last.isoformat(), quiet_min=quiet,
                             flag=bool(this_shift and quiet >= LIVE_QUIET_MINUTES)))
    machines.sort(key=lambda x: -x["lbs"])
    series = {m: _curve(g, day_start, end) for m, g in today.groupby("machine")}
    feed = ch.sort_values("t1", ascending=False).head(LIVE_FEED_LENGTH)
    open_rows = snaps[snaps["ts"] == snaps["ts"].max()] if len(snaps) else snaps
    return dict(
        generated=now.isoformat(), day=day.strftime("%Y-%m-%d"), day_start=day_start.isoformat(),
        shift_hours={k: list(v) for k, v in SHIFT_HOURS.items()}, current_shift=shift_now,
        last_poll=last_poll.isoformat() if last_poll is not None else None, poll_ok=ok,
        last_ok_poll=out["last_ok_poll"], api_down_since=out["api_down_since"], failed_polls=out["failed_polls"], last_error=out["last_error"],
        polls_today=int((polls["ts"] >= day_start).sum()) if len(polls) else 0, changes_today=int(len(today)),
        lbs_today=int(today["lbs"].sum()) if len(today) else 0,
        open_jobs=int(open_rows["job"].nunique()) if len(open_rows) else 0,
        open_lbs=int(open_rows["qty"].sum()) if len(open_rows) else 0,
        machines=machines, series=series,
        yesterday=dict(day=(day - timedelta(days=1)).strftime("%Y-%m-%d"), total=_curve(yesterday, yday_start, day_start),
                       lbs=int(yesterday["lbs"].sum()) if len(yesterday) else 0),
        changes=[dict(t0=r.t0.isoformat(), t1=r.t1.isoformat(), m=r.machine, s=r.shift, lbs=int(r.lbs), units=int(r.units),
                      job=int(r.job), correction=bool(r.correction)) for r in feed.itertuples(index=False)],
    )


def write_live(path: Path = LIVE_JSON_PATH, now: pd.Timestamp | None = None, api_dir: Path = CIETRADE_DATA_DIR) -> dict:
    live = build_live(now, api_dir)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(live, separators=(",", ":")))
    tmp.replace(path)
    return live


def publish(path: Path = LIVE_JSON_PATH, gist_id: str = LIVE_GIST_ID, timeout: int = 60) -> bool:
    """Replace live.json in the gist through the GitHub API (gh is already signed in). Never fatal."""
    if not gist_id:
        return False
    try:
        r = subprocess.run(["gh", "api", "-X", "PATCH", f"/gists/{gist_id}", "-F", f"files[live.json][content]=@{path}"],
                           capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            LOG.error("gist publish failed: %s", (r.stderr or r.stdout).strip()[-300:])
            return False
        return True
    except Exception as e:  # noqa: BLE001
        LOG.error("gist publish error: %s", e)
        return False


if __name__ == "__main__":
    live = write_live()
    print(f"live.json: {live['changes_today']} changes today, {live['lbs_today']:,} lbs, {len(live['machines'])} machines, "
          f"current shift {live['current_shift']}, last poll {live['last_poll']}")
    for c in live["changes"][:8]:
        print(f"  {c['t0'][11:16]}–{c['t1'][11:16]}  {c['m']:<28s} {c['s']}  {c['lbs']:+,} lbs" + (f" ({c['units']:+d} units)" if c["units"] else ""))
