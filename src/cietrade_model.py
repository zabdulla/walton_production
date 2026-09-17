"""Daily production per machine and shift from cieTrade converting jobs.

Observations come from three places, all in the manual export's column layout:
  * data/cietrade_exports/converting_<time>.csv — manual Converting Inquiry exports
    (the posted history since January and two hand-made open-jobs snapshots);
  * data/cietrade/posted.csv — every posting the API poller has seen;
  * data/cietrade/snapshots/<date>.csv — the open-jobs list every time it changed,
    stamped with the poll time (see cietrade_poll.py).
An open-jobs observation is a snapshot: the job's Output Qty at that instant, so
consecutive snapshots of one job bracket the output produced between them.

Attribution, per line (one cieTrade machine name including its shift tag):
  1. Coverage — which shift-days a job owns — as validated against the hand-built
     workbooks (98.8% of Aug 3–21 shift-days to the pound): through Aug 21 the Job
     Date's shift; since Aug 24 the shifts between the job's creation and the next
     job's creation on that line (1st-shift jobs are opened after the shift, so they
     own the following days).
  2. A job with no snapshots spreads its posted output evenly over the shift-days it
     owns (one day = exact, several = averaged).
  3. A job with snapshots is a sequence of points: opened (0 lbs), each snapshot, the
     posting (final lbs). Each window between consecutive points carries its delta,
     split across the owned shift-days by hours of overlap with the shift's working
     hours (config.SHIFT_HOURS). A window overlapping no shift goes to the nearest
     owned shift-day. A shift-day whose value came from windows it had to itself is
     exact; one that shared a window is averaged.
  4. For a job still open: the shift-day in progress at the last snapshot is
     "partial" (output so far), later shift-days are "not yet posted". A shift that
     began before the last successful poll and has no job at all is idle, because a
     poll lists every open job.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from config import (
    CIETRADE_DATA_DIR, CIETRADE_EXPORT_DIR, CIETRADE_POSTED_LOOKBACK_DAYS, CIETRADE_SITES, DEFAULT_AGGREGATED_DATA, SHIFT_HOURS,
)

# cieTrade line -> series on the pilot page (the small lines share one)
MACHINE_MAP = {"AUTO-TIE BALER": "AUTO TIE BALER", "EXTRUDER": "EXTRUDER", "GUILLOTINE": "GUILLOTINE",
               "GREEN MAX (NEW)": "GREEN MAX DENSIFIER (NEW)", "SHREDDER": "SHREDDER+GRINDER",
               "SHREDDER/GRINDER": "SHREDDER+GRINDER", "SMALL GRINDER": "SHREDDER+GRINDER",
               "AVANGARD (OLD)": "OTHER LINES", "BALER2": "OTHER LINES", "BALER 2": "OTHER LINES",
               "BALER1": "OTHER LINES", "BALER 1": "OTHER LINES"}
MID = {k: (a + b) / 2 for k, (a, b) in SHIFT_HOURS.items()}   # coverage midpoint of each shift (validation unchanged)
WORKDAYS = {0, 1, 2, 3, 4}
REGIME_CHANGE = pd.Timestamp("2026-08-24")
CALENDAR_CLOSURES = ["2026-09-07", "2026-11-26", "2026-11-27", "2026-12-24", "2026-12-25", "2027-01-01"]
ACTIVE_WINDOW = 10          # working days without a job before a line stops reading as "awaiting"
VALIDATION = (pd.Timestamp("2026-08-03"), pd.Timestamp("2026-08-21"))
EXPORT_DATE_FMT = "%m/%d/%Y %I:%M:%S %p"
HOUR = pd.Timedelta(hours=1)
FAR = pd.Timestamp("2100-01-01")
SHIFT_RE = r"\((\d)(?:ST|ND|RD) SHIFT\)"
MODE_RANK = {"not yet posted": 4, "partial": 3, "spread": 2, "exact": 1, "no open job": 0}


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------

def export_time_of(path: Path) -> pd.Timestamp:
    return pd.Timestamp(datetime.strptime(path.stem[len("converting_"):][:19], "%Y-%m-%dT%H-%M-%S"))


def _parse_dates(s: pd.Series) -> pd.Series:
    """Manual exports say '8/24/2026 12:00:00 AM'; the API says '2026-08-24'."""
    s = s.astype(str).str.strip().replace({"nan": "", "None": "", "NaT": ""})
    out = pd.to_datetime(s, format=EXPORT_DATE_FMT, errors="coerce")
    rest = out.isna() & (s != "")
    if rest.any():
        out.loc[rest] = pd.to_datetime(s[rest], format="ISO8601", errors="coerce")
    return out


def read_export_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    for c in df.select_dtypes("object"):
        df[c] = df[c].astype(str).str.strip().replace({"nan": ""})
    df["Job Date"] = _parse_dates(df["Job Date"])
    df["Post Date"] = _parse_dates(df["Post Date"])
    df["Output Qty"] = pd.to_numeric(df["Output Qty"].astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)
    df["Job No"] = pd.to_numeric(df["Job No"], errors="coerce").astype("Int64")
    return df.dropna(subset=["Job No"]).astype({"Job No": int})


def load_observations(export_dir: Path = CIETRADE_EXPORT_DIR, api_dir: Path = CIETRADE_DATA_DIR) -> pd.DataFrame:
    """Every observation of every job, with the moment it was observed."""
    frames = []
    for f in sorted(export_dir.glob("converting_*.csv")):
        df = read_export_csv(f)
        df["export_ts"] = export_time_of(f)
        df["export_file"] = f.name
        frames.append(df)
    posted = api_dir / "posted.csv"
    if posted.exists():
        df = read_export_csv(posted)
        df["export_ts"] = pd.to_datetime(df["Last Seen"])
        df["export_file"] = "api posted"
        frames.append(df)
    for f in sorted((api_dir / "snapshots").glob("*.csv")):
        df = read_export_csv(f)
        df["export_ts"] = pd.to_datetime(df["Snapshot"])
        df["export_file"] = "api " + df["Snapshot"].astype(str)
        frames.append(df)
    if not frames:
        raise SystemExit(f"no cieTrade observations under {export_dir} or {api_dir}")
    raw = pd.concat(frames, ignore_index=True)
    raw["posted"] = raw["Status"].eq("Posted")
    raw["api"] = raw["export_file"].str.startswith("api")
    return raw


def load_polls(api_dir: Path = CIETRADE_DATA_DIR) -> pd.DataFrame:
    path = api_dir / "polls.jsonl"
    if not path.exists():
        return pd.DataFrame(columns=["ts", "ok", "changed"])
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


# ---------------------------------------------------------------------------
# Jobs, coverage, attribution
# ---------------------------------------------------------------------------

def _tod(s: pd.Series) -> pd.Series:
    t = pd.to_datetime(s.replace({"": None}), format="%I:%M%p", errors="coerce")
    return pd.to_timedelta(t.dt.hour * 60 + t.dt.minute, unit="m")


def job_master(raw: pd.DataFrame, sites: set = CIETRADE_SITES) -> tuple[pd.DataFrame, list[str]]:
    """One row per site job: its latest observation (a posted observation always wins)."""
    warnings: list[str] = []
    obs = raw[raw["Warehouse"].isin(sites)].sort_values(["Job No", "export_ts"])
    latest = obs.groupby("Job No").tail(1)
    posted_obs = obs[obs["posted"]]
    # a posting edited after the fact: quantity differs between two posted observations
    q = posted_obs.groupby("Job No")["Output Qty"].agg(["min", "max"])
    edited = q[(q["max"] - q["min"]) > 0.5]
    for j, r in edited.iterrows():
        warnings.append(f"posted job {j} changed quantity after posting ({r['min']:,.0f} -> {r['max']:,.0f} lbs); the latest value is used")
    posted = posted_obs.groupby("Job No").tail(1)
    m = pd.concat([posted, latest[~latest["Job No"].isin(posted["Job No"])]]).copy()
    m["line"] = m["Machine"].str.replace(r"\s*" + SHIFT_RE, "", regex=True)
    m["machine"] = m["line"].map(MACHINE_MAP)
    m["shift"] = m["Machine"].str.extract(SHIFT_RE)[0].map({"1": "1st", "2": "2nd", "3": "3rd"})
    unmapped = sorted(m.loc[m["machine"].isna() | m["shift"].isna(), "Machine"].unique())
    if unmapped:
        warnings.append("ignored (no machine/shift mapping): " + ", ".join(unmapped))
    m = m.dropna(subset=["machine", "shift"])
    m["start_dt"] = m["Job Date"] + _tod(m["Start-Time"]).fillna(pd.Timedelta(0))
    m["old_regime"] = m["Job Date"] < REGIME_CHANGE
    m = m.sort_values(["line", "shift", "start_dt", "Job No"]).reset_index(drop=True)
    m["next_start"] = m.groupby(["line", "shift"])["start_dt"].shift(-1)
    recent = m[~m["old_regime"]]
    usual_min = recent.groupby(["line", "shift"])["start_dt"].apply(lambda s: (s.dt.hour * 60 + s.dt.minute).median())
    usual = pd.Series([usual_min.get((ln, sh), 350.0) for ln, sh in zip(m["line"], m["shift"])], index=m.index)
    m["closed_at"] = m["next_start"].fillna(m["Post Date"] + pd.to_timedelta(usual, unit="m"))  # NaT: open, no successor
    return m, warnings


def closures(m: pd.DataFrame) -> set:
    opened = set(m["Job Date"].dt.normalize())
    c = {d for d in pd.date_range(m["Job Date"].min(), REGIME_CHANGE - pd.Timedelta(days=1), freq="D")
         if d.weekday() in WORKDAYS and d not in opened}
    return c | {pd.Timestamp(x) for x in CALENDAR_CLOSURES}


def snapshot_points(raw: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    s = raw[~raw["posted"] & raw["Job No"].isin(m["Job No"])]
    return s[["Job No", "export_ts", "export_file", "Output Qty"]].sort_values(["Job No", "export_ts"]).reset_index(drop=True)


def coverage(m: pd.DataFrame, closed_days: set, horizon: pd.Timestamp, last_seen: pd.Timestamp | None = None) -> pd.DataFrame:
    """Which job owns each shift-day of each line.

    ``last_seen`` is the last moment the full open-jobs list was known (last
    successful poll or open-jobs export): a shift that started before it and has
    no job is idle, not awaiting."""
    rows = []
    for (line, shift), jobs in m.groupby(["line", "shift"]):
        jobs = jobs.sort_values("start_dt")
        machine = jobs["machine"].iloc[0]
        last_activity = max(jobs["Job Date"].max(), jobs["Post Date"].max()) if jobs["Post Date"].notna().any() else jobs["Job Date"].max()
        line_active = np.busday_count(jobs["Job Date"].max().date(), horizon.date()) <= ACTIVE_WINDOW
        closed_at = jobs["closed_at"].fillna(FAR)
        post = jobs["Post Date"].fillna(FAR)
        for d in pd.date_range(jobs["Job Date"].min(), horizon, freq="D"):
            if d.weekday() not in WORKDAYS or d in closed_days:
                continue
            mid = d + pd.Timedelta(hours=MID[shift])
            by_date = (jobs["Job Date"] <= d) & (d < post)                 # daily era: the Job Date's shift
            by_time = (jobs["start_dt"] <= mid) & (mid < closed_at)         # multi-day era: creation-time partition
            cand = jobs[(jobs["old_regime"] & by_date) | (~jobs["old_regime"] & by_time)]
            if cand.empty:
                unseen = last_seen is None or d + pd.Timedelta(hours=SHIFT_HOURS[shift][0]) > last_seen
                mode = "not yet posted" if (line_active and d >= last_activity and unseen) else "no open job"
                rows.append(dict(Date=d, Shift=shift, line=line, Machine=machine, job=np.nan, mode=mode))
            else:
                rows.append(dict(Date=d, Shift=shift, line=line, Machine=machine, job=int(cand.iloc[-1]["Job No"]), mode=""))
    return pd.DataFrame(rows)


def attribute(m: pd.DataFrame, cov: pd.DataFrame, points: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fill lbs/mode per owned shift-day; return (coverage rows, windows log)."""
    cov = cov.copy()
    cov["lbs"] = np.nan
    cov["src"] = "job"
    by_job = m.set_index("Job No")
    n_days = cov.dropna(subset=["job"]).groupby("job").size()
    snap_jobs = set(points["Job No"])
    plain = cov["job"].notna() & ~cov["job"].isin(snap_jobs)
    cov.loc[plain, "lbs"] = cov.loc[plain, "job"].map(by_job["Output Qty"]) / cov.loc[plain, "job"].map(n_days)
    cov.loc[plain, "mode"] = np.where(cov.loc[plain, "job"].map(n_days) == 1, "exact", "spread")

    windows = []
    for j, pts in points.groupby("Job No"):
        job = by_job.loc[j]
        idx = list(cov.index[cov["job"] == j])
        seq = [(job["start_dt"], 0.0, "opened")]
        seq += [(t, float(q), f) for t, q, f in zip(pts["export_ts"], pts["Output Qty"], pts["export_file"])]
        if job["posted"]:
            t_end = seq[-1][0] + pd.Timedelta(minutes=1)
            if pd.notna(job["closed_at"]):
                t_end = max(job["closed_at"], t_end)
            seq.append((t_end, float(job["Output Qty"]), "posted"))
        t_last = seq[-1][0]
        span = {i: (cov.at[i, "Date"] + pd.Timedelta(hours=SHIFT_HOURS[cov.at[i, "Shift"]][0]),
                    cov.at[i, "Date"] + pd.Timedelta(hours=SHIFT_HOURS[cov.at[i, "Shift"]][1])) for i in idx}
        lbs = {i: 0.0 for i in idx}
        solo = {i: True for i in idx}
        for (t0, q0, f0), (t1, q1, f1) in zip(seq, seq[1:]):
            delta = q1 - q0
            shares: dict = {}
            if idx:
                ov = {i: max(0.0, (min(t1, e) - max(t0, s)) / HOUR) for i, (s, e) in span.items()}
                tot = sum(ov.values())
                if tot > 0:
                    shares = {i: v / tot for i, v in ov.items() if v > 0}
                else:
                    mid_w = t0 + (t1 - t0) / 2
                    pool = [i for i in idx if span[i][0] <= t1] or idx
                    near = min(pool, key=lambda i: abs((span[i][0] + (span[i][1] - span[i][0]) / 2) - mid_w))
                    shares = {near: 1.0}
                for i, sh in shares.items():
                    lbs[i] += delta * sh
                    if len(shares) > 1:
                        solo[i] = False
            if abs(delta) > 0 or f1 == "posted":
                windows.append(dict(job=int(j), line=job["line"], machine=job["machine"], shift=job["shift"], t0=t0, t1=t1,
                                    lbs=delta, correction=bool(delta < 0), from_file=f0, to_file=f1,
                                    days=", ".join(cov.at[i, "Date"].strftime("%a %b %d") for i in sorted(shares, key=lambda i: cov.at[i, "Date"]))))
        for i in idx:
            s, e = span[i]
            cov.at[i, "src"] = "snap"
            if not job["posted"] and s > t_last:
                cov.at[i, "mode"], cov.at[i, "lbs"] = "not yet posted", np.nan
            elif not job["posted"] and s <= t_last < e:
                cov.at[i, "mode"], cov.at[i, "lbs"] = "partial", lbs[i]
            else:
                cov.at[i, "mode"], cov.at[i, "lbs"] = ("exact" if solo[i] else "spread"), lbs[i]

    # jobs that own no shift-day: pin to the Job Date (their latest known output)
    orphans = set(m["Job No"]) - set(cov["job"].dropna().astype(int))
    extra = []
    for j in sorted(orphans):
        r = by_job.loc[j]
        extra.append(dict(Date=r["Job Date"], Shift=r["shift"], line=r["line"], Machine=r["machine"], job=int(j),
                          mode="exact", lbs=float(r["Output Qty"]), src="snap" if j in snap_jobs else "job"))
    if extra:
        cov = pd.concat([cov, pd.DataFrame(extra)], ignore_index=True)
    known = by_job["Output Qty"].sum()
    assert abs(cov["lbs"].sum() - known) < 1e-6, (cov["lbs"].sum(), known)   # every known pound lands somewhere
    cov["n_days"] = cov["job"].map(cov.dropna(subset=["job"]).groupby("job").size())
    return cov, pd.DataFrame(windows, columns=["job", "line", "machine", "shift", "t0", "t1", "lbs", "correction", "from_file", "to_file", "days"])


def to_cells(cov: pd.DataFrame) -> pd.DataFrame:
    """Collapse lines onto the pilot page's series (shredder + grinder become one)."""
    def one(g: pd.DataFrame) -> pd.Series:
        live = g[g["mode"] != "no open job"]
        if live.empty:
            return pd.Series(dict(lbs=np.nan, mode="no open job", job=None, n=None, src="job"))
        mode = live["mode"].map(MODE_RANK).max()
        jobs = sorted(int(x) for x in live["job"].dropna().unique())
        return pd.Series(dict(lbs=live["lbs"].sum(min_count=1), mode={v: k for k, v in MODE_RANK.items()}[mode],
                              job=(jobs[0] if len(jobs) == 1 else "+".join(map(str, jobs))) if jobs else None,
                              n=(int(live["n_days"].max()) if live["n_days"].notna().any() else None),
                              src="snap" if (live["src"] == "snap").any() else "job"))
    cells = cov.groupby(["Date", "Shift", "Machine"]).apply(one, include_groups=False).reset_index()
    return cells.sort_values(["Date", "Shift", "Machine"]).reset_index(drop=True)


def validate(cells: pd.DataFrame, aggregate: Path = DEFAULT_AGGREGATED_DATA) -> dict:
    """Shift-day match against the last hand-built workbooks (Guillotine: rolls in = slabs out)."""
    lo, hi = VALIDATION
    agg = pd.read_excel(aggregate, usecols=["Date", "Shift", "Machine_Name", "Actual_Input", "Actual_Output"])
    agg["Date"] = pd.to_datetime(agg["Date"])
    w = agg[(agg["Date"] >= lo) & (agg["Date"] <= hi)].copy()
    w["truth"] = np.where(w["Machine_Name"] == "GUILLOTINE", w["Actual_Input"], w["Actual_Output"])
    w["Machine"] = w["Machine_Name"].replace({"SHREDDER": "SHREDDER+GRINDER", "GRINDER": "SHREDDER+GRINDER", "SMALL GRINDER": "SHREDDER+GRINDER"})
    w = w[w["Machine"].isin({"AUTO TIE BALER", "EXTRUDER", "GUILLOTINE", "GREEN MAX DENSIFIER (NEW)", "SHREDDER+GRINDER"})]
    truth = w.groupby(["Date", "Shift", "Machine"], as_index=False)["truth"].sum()
    est = cells[(cells["Date"] >= lo) & (cells["Date"] <= hi) & cells["lbs"].notna() & cells["Machine"].isin(set(truth["Machine"]))]
    est = est.groupby(["Date", "Shift", "Machine"], as_index=False)["lbs"].sum()
    mm = est.merge(truth, how="outer").fillna(0)
    bad = mm[(mm["lbs"] - mm["truth"]).abs() > 1]
    return dict(pct_exact=round((len(mm) - len(bad)) / len(mm) * 100, 1), cells=len(mm), bad=bad,
                missing_job_lbs=int(round(bad.loc[bad["lbs"] == 0, "truth"].sum())),
                plant_est=float(mm["lbs"].sum()), plant_truth=float(mm["truth"].sum()))


# ---------------------------------------------------------------------------
# Log of what was observed when
# ---------------------------------------------------------------------------

def export_log(raw: pd.DataFrame, windows: pd.DataFrame, polls: pd.DataFrame, sites: set = CIETRADE_SITES) -> list[dict]:
    """One entry per manual export, plus one entry per day of API polling."""
    entries: list[dict] = []
    seen_posted: set = set()
    prev_snap = None
    site = raw[raw["Warehouse"].isin(sites)]
    manual = site[~site["api"]].groupby("export_file").agg(ts=("export_ts", "first"), posted=("posted", "all")).sort_values("ts")
    for f, r in manual.iterrows():
        rows = site[site["export_file"] == f]
        e = dict(file=f, ts=r["ts"].isoformat(), kind="posted" if r["posted"] else "in-process", jobs=int(rows["Job No"].nunique()))
        if r["posted"]:
            new = set(rows["Job No"]) - seen_posted
            e["new_posted"], e["new_posted_lbs"] = len(new), float(rows.loc[rows["Job No"].isin(new), "Output Qty"].sum())
            seen_posted |= set(rows["Job No"])
        else:
            w = windows[windows["to_file"] == f]
            e.update(prev_ts=prev_snap.isoformat() if prev_snap is not None else None,
                     hours_since=float((r["ts"] - prev_snap) / HOUR) if prev_snap is not None else None,
                     open_lbs=float(rows["Output Qty"].sum()), delta_total=float(w["lbs"].sum()),
                     lines=_lines(w), unchanged=int(rows["Job No"].nunique() - w["job"].nunique()))
            prev_snap = r["ts"]
        entries.append(e)
    # API activity, rolled up per calendar day
    api_snaps = site[site["api"] & ~site["posted"]]
    api_posted = raw[(raw["export_file"] == "api posted") & raw["Warehouse"].isin(sites)]
    days = set(api_snaps["export_ts"].dt.normalize())
    if len(polls):
        days |= set(polls["ts"].dt.normalize())
    if len(api_posted) and "First Seen" in api_posted:
        days |= set(pd.to_datetime(api_posted["First Seen"]).dt.normalize())
    for day in sorted(days):
        p = polls[polls["ts"].dt.normalize() == day] if len(polls) else polls
        snaps = api_snaps[api_snaps["export_ts"].dt.normalize() == day]
        w = windows[windows["to_file"].str.startswith("api " + day.strftime("%Y-%m-%d"))] if len(windows) else windows
        # postings first seen that day, excluding the one-off backfill of old postings
        posted_day = api_posted[(pd.to_datetime(api_posted["First Seen"]).dt.normalize() == day)
                                & (api_posted["Post Date"] >= day - pd.Timedelta(days=CIETRADE_POSTED_LOOKBACK_DAYS))] if len(api_posted) else api_posted
        e = dict(file=f"api {day:%Y-%m-%d}", ts=(p["ts"].min() if len(p) else snaps["export_ts"].min()).isoformat(), kind="api-day",
                 polls=int(len(p)), failed=int((~p["ok"]).sum()) if len(p) else 0, changes=int(snaps["export_ts"].nunique()),
                 last_poll=(p.loc[p["ok"], "ts"].max().isoformat() if len(p) and p["ok"].any() else None),
                 jobs=int(snaps["Job No"].nunique()), delta_total=float(w["lbs"].sum()), lines=_lines(w),
                 new_posted=int(posted_day["Job No"].nunique()), new_posted_lbs=float(posted_day["Output Qty"].sum()))
        entries.append(e)
    entries.sort(key=lambda e: e["ts"])
    return entries


def _lines(w: pd.DataFrame) -> list[dict]:
    if w is None or not len(w):
        return []
    g = w.groupby(["machine", "shift", "job"], as_index=False).agg(lbs=("lbs", "sum"), t0=("t0", "min"), t1=("t1", "max"),
                                                                    n=("lbs", "size"), correction=("correction", "any"))
    g = g[g["lbs"].abs() > 0].sort_values(["shift", "machine"])
    return [dict(m=r["machine"], s=r["shift"], job=int(r["job"]), lbs=float(r["lbs"]), t0=r["t0"].isoformat(), t1=r["t1"].isoformat(),
                 windows=int(r["n"]), correction=bool(r["correction"])) for _, r in g.iterrows()]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(export_dir: Path = CIETRADE_EXPORT_DIR, api_dir: Path = CIETRADE_DATA_DIR,
        aggregate: Path = DEFAULT_AGGREGATED_DATA, verbose: bool = True) -> dict:
    raw = load_observations(export_dir, api_dir)
    polls = load_polls(api_dir)
    m, warnings = job_master(raw)
    closed_days = closures(m)
    pts = snapshot_points(raw, m)
    last_poll = polls.loc[polls["ok"], "ts"].max() if len(polls) and polls["ok"].any() else None
    last_snap = pts["export_ts"].max() if len(pts) else None
    last_seen = max(t for t in (last_poll, last_snap) if t is not None) if (last_poll is not None or last_snap is not None) else None
    horizon = max(t for t in [raw["export_ts"].max().normalize(), m["Post Date"].max(), last_seen.normalize() if last_seen is not None else pd.NaT] if pd.notna(t))
    cov = coverage(m, closed_days, horizon, last_seen)
    cov, windows = attribute(m, cov, pts)
    if len(windows) and windows["correction"].any():
        n = int(windows["correction"].sum())
        warnings.append(f"{n} window(s) where a job's output went DOWN between observations (deleted or re-weighed items); carried as negative pounds")
    cells = to_cells(cov)
    val = validate(cells, aggregate) if Path(aggregate).exists() else None
    meta = dict(
        data_through=horizon.strftime("%Y-%m-%d"), export_date=raw["export_ts"].max().strftime("%Y-%m-%d"),
        first_day=cells["Date"].min().strftime("%Y-%m-%d"),
        last_snapshot=last_snap.isoformat() if last_snap is not None else None,
        last_poll=last_poll.isoformat() if last_poll is not None else None,
        polls=int(len(polls)), snapshots=int(pts["export_ts"].nunique()),
        exports=int(raw.loc[~raw["api"], "export_file"].nunique()),
        shift_hours={k: [a, b] for k, (a, b) in SHIFT_HOURS.items()},
        closures=sorted(d.strftime("%Y-%m-%d") for d in closed_days),
        validation_pct_exact=val["pct_exact"] if val else None, validation_window="Aug 3–21",
        missing_job_lbs=val["missing_job_lbs"] if val else None,
        open_jobs=int((~m["posted"]).sum()), open_lbs=float(m.loc[~m["posted"], "Output Qty"].sum()),
        warnings=warnings,
    )
    if verbose:
        print(f"observations: {meta['exports']} manual exports, {meta['polls']} API polls, {meta['snapshots']} snapshots "
              f"(last poll {meta['last_poll']}) · jobs {len(m)} (open {meta['open_jobs']}, {meta['open_lbs']:,.0f} lbs so far) "
              f"· data through {meta['data_through']}")
        for w in warnings:
            print("warning:", w)
        if val:
            print(f"validation {VALIDATION[0]:%b %d}–{VALIDATION[1]:%b %d}: {val['pct_exact']}% of {val['cells']} shift-days exact; "
                  f"plant {val['plant_est']:,.0f} vs {val['plant_truth']:,.0f}; {len(val['bad'])} disagreeing cell(s)")
    return dict(meta=meta, cells=cells, cov=cov, windows=windows, log=export_log(raw, windows, polls), jobs=m, polls=polls)


if __name__ == "__main__":
    run()
