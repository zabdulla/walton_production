"""Daily production from the cieTrade Converting Inquiry export archive.

Inputs: data/cietrade_exports/converting_<export time>.csv (see ingest.py). Two kinds of
export land there: the posted list (Status = Posted, the complete history) and the
open-jobs list (Status = Work, jobs still in process). Every open-jobs export is a
snapshot of each job's Output Qty at the export time, so consecutive snapshots of one
job bracket the output produced between them.

Attribution, per line (one cieTrade machine name including its shift tag):
  1. Coverage, which shift-days a job owns, exactly as validated earlier (98.8% of
     shift-days to the pound against the hand-built workbooks): through Aug 21 the
     Job Date's shift; since Aug 24 the shifts between the job's creation and the next
     job's creation on that line (1st-shift jobs are opened after the shift, so they
     own the following days).
  2. A job with no snapshots spreads its posted output evenly over the shift-days it
     owns (one day = exact, several = averaged).
  3. A job with snapshots is a sequence of points: opened (0 lbs), each snapshot, and
     the posting (final lbs). Each window between consecutive points carries its
     delta, split across the owned shift-days by hours of overlap with the shift's
     working hours (1st 07-15, 2nd 15-23, 3rd 23-07). A window that overlaps no shift
     goes to the nearest owned shift-day. A shift-day whose value came from windows
     it had to itself is exact; one that shared a window is averaged.
  4. For a job still open: the shift-day in progress at the last snapshot is
     "partial" (output so far), later shift-days are "not yet posted".
Five uploads of the same job therefore produce five windows, each with the pounds
made between those two export times.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
EXPORT_DIR = REPO / "data" / "cietrade_exports"
AGGREGATE = REPO / "data" / "aggregated_daily_data.xlsx"
SITE = {"Plus Monroe Warehouse", "Monroe Processing Warehouse"}
MACHINE_MAP = {"AUTO-TIE BALER": "AUTO TIE BALER", "EXTRUDER": "EXTRUDER", "GUILLOTINE": "GUILLOTINE",
               "GREEN MAX (NEW)": "GREEN MAX DENSIFIER (NEW)", "SHREDDER": "SHREDDER+GRINDER",
               "SHREDDER/GRINDER": "SHREDDER+GRINDER", "SMALL GRINDER": "SHREDDER+GRINDER",
               "AVANGARD (OLD)": "OTHER LINES", "BALER2": "OTHER LINES"}   # the small lines share one series
SHIFT_HOURS = {"1st": (7, 15), "2nd": (15, 23), "3rd": (23, 31)}   # hours from midnight of the shift's date
MID = {"1st": 10, "2nd": 19, "3rd": 27}                             # coverage midpoint (validated)
WORKDAYS = {0, 1, 2, 3, 4}
REGIME_CHANGE = pd.Timestamp("2026-08-24")
CALENDAR_CLOSURES = ["2026-09-07", "2026-11-26", "2026-11-27", "2026-12-24", "2026-12-25", "2027-01-01"]
ACTIVE_WINDOW = 10          # working days without a job before a line stops reading as "awaiting"
VALIDATION = (pd.Timestamp("2026-08-03"), pd.Timestamp("2026-08-21"))
HOUR = pd.Timedelta(hours=1)
FAR = pd.Timestamp("2100-01-01")


def export_time_of(path: Path) -> pd.Timestamp:
    return pd.Timestamp(datetime.strptime(path.stem[len("converting_"):][:19], "%Y-%m-%dT%H-%M-%S"))


def load_exports(export_dir: Path = EXPORT_DIR) -> pd.DataFrame:
    frames = []
    for f in sorted(export_dir.glob("converting_*.csv")):
        df = pd.read_csv(f)
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        for c in df.select_dtypes("object"):
            df[c] = df[c].astype(str).str.strip()
        df["Job Date"] = pd.to_datetime(df["Job Date"], format="%m/%d/%Y %I:%M:%S %p")
        df["Post Date"] = pd.to_datetime(df["Post Date"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
        df["export_ts"] = export_time_of(f)
        df["export_file"] = f.name
        frames.append(df)
    if not frames:
        raise SystemExit(f"no exports in {export_dir}; run ingest.py first")
    raw = pd.concat(frames, ignore_index=True)
    raw["posted"] = raw["Status"].eq("Posted")
    return raw


def _tod(s: pd.Series) -> pd.Series:
    t = pd.to_datetime(s, format="%I:%M%p", errors="coerce")
    return pd.to_timedelta(t.dt.hour * 60 + t.dt.minute, unit="m")


def job_master(raw: pd.DataFrame) -> pd.DataFrame:
    """One row per Monroe job: its latest observation (a posted observation always wins)."""
    obs = raw[raw["Warehouse"].isin(SITE)].sort_values(["Job No", "export_ts"])
    latest = obs.groupby("Job No").tail(1)
    posted = obs[obs["posted"]].groupby("Job No").tail(1)
    m = pd.concat([posted, latest[~latest["Job No"].isin(posted["Job No"])]]).copy()
    m["line"] = m["Machine"].str.replace(r"\s*\(\d(?:ST|ND|RD) SHIFT\)", "", regex=True)
    m["machine"] = m["line"].map(MACHINE_MAP)
    m["shift"] = m["Machine"].str.extract(r"\((\d)(?:ST|ND|RD) SHIFT\)")[0].map({"1": "1st", "2": "2nd", "3": "3rd"})
    unmapped = sorted(m.loc[m["machine"].isna() | m["shift"].isna(), "Machine"].unique())
    if unmapped:
        print("ignored (no machine/shift mapping):", unmapped)
    m = m.dropna(subset=["machine", "shift"])
    m["start_dt"] = m["Job Date"] + _tod(m["Start-Time"]).fillna(pd.Timedelta(0))
    m["old_regime"] = m["Job Date"] < REGIME_CHANGE
    m = m.sort_values(["line", "shift", "start_dt", "Job No"]).reset_index(drop=True)
    m["next_start"] = m.groupby(["line", "shift"])["start_dt"].shift(-1)
    recent = m[~m["old_regime"]]
    usual_min = recent.groupby(["line", "shift"])["start_dt"].apply(lambda s: (s.dt.hour * 60 + s.dt.minute).median())
    usual = pd.Series([usual_min.get((ln, sh), 350.0) for ln, sh in zip(m["line"], m["shift"])], index=m.index)
    m["closed_at"] = m["next_start"].fillna(m["Post Date"] + pd.to_timedelta(usual, unit="m"))  # NaT: open, no successor
    return m


def closures(m: pd.DataFrame) -> set:
    opened = set(m["Job Date"].dt.normalize())
    c = {d for d in pd.date_range(m["Job Date"].min(), REGIME_CHANGE - pd.Timedelta(days=1), freq="D")
         if d.weekday() in WORKDAYS and d not in opened}
    return c | {pd.Timestamp(x) for x in CALENDAR_CLOSURES}


def snapshot_points(raw: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    s = raw[~raw["posted"] & raw["Job No"].isin(m["Job No"])]
    return s[["Job No", "export_ts", "export_file", "Output Qty"]].sort_values(["Job No", "export_ts"]).reset_index(drop=True)


def coverage(m: pd.DataFrame, closed_days: set, horizon: pd.Timestamp, last_snap: pd.Timestamp | None = None) -> pd.DataFrame:
    """Which job owns each shift-day of each line. An open-jobs export lists every open job, so a
    shift that started before the last snapshot and has no job is idle, not awaiting."""
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
                unseen = last_snap is None or d + pd.Timedelta(hours=SHIFT_HOURS[shift][0]) > last_snap
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
            windows.append(dict(job=int(j), line=job["line"], machine=job["machine"], shift=job["shift"], t0=t0, t1=t1,
                                lbs=delta, from_file=f0, to_file=f1,
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
    if orphans:
        print("jobs pinned to their Job Date (own no shift-day):", sorted(orphans))
    # every posted pound lands somewhere; an open job's pounds equal its last snapshot
    known = by_job["Output Qty"].sum()
    assert abs(cov["lbs"].sum() - known) < 1e-6, (cov["lbs"].sum(), known)
    cov["n_days"] = cov["job"].map(cov.dropna(subset=["job"]).groupby("job").size())
    return cov, pd.DataFrame(windows)


MODE_RANK = {"not yet posted": 4, "partial": 3, "spread": 2, "exact": 1, "no open job": 0}


def to_cells(cov: pd.DataFrame) -> pd.DataFrame:
    """Collapse lines onto the dashboard's machine names (shredder + grinder become one)."""
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


def validate(cells: pd.DataFrame) -> dict:
    """Shift-day match against the last hand-built workbooks (Guillotine: rolls in = slabs out)."""
    lo, hi = VALIDATION
    agg = pd.read_excel(AGGREGATE, usecols=["Date", "Shift", "Machine_Name", "Actual_Input", "Actual_Output"])
    agg["Date"] = pd.to_datetime(agg["Date"])
    w = agg[(agg["Date"] >= lo) & (agg["Date"] <= hi)].copy()
    w["truth"] = np.where(w["Machine_Name"] == "GUILLOTINE", w["Actual_Input"], w["Actual_Output"])
    w["Machine"] = w["Machine_Name"].replace({"SHREDDER": "SHREDDER+GRINDER", "GRINDER": "SHREDDER+GRINDER", "SMALL GRINDER": "SHREDDER+GRINDER"})
    w = w[w["Machine"].isin(set(MACHINE_MAP.values()))]
    truth = w.groupby(["Date", "Shift", "Machine"], as_index=False)["truth"].sum()
    est = cells[(cells["Date"] >= lo) & (cells["Date"] <= hi) & cells["lbs"].notna()].groupby(["Date", "Shift", "Machine"], as_index=False)["lbs"].sum()
    mm = est.merge(truth, how="outer").fillna(0)
    bad = mm[(mm["lbs"] - mm["truth"]).abs() > 1]
    return dict(pct_exact=round((len(mm) - len(bad)) / len(mm) * 100, 1), cells=len(mm), bad=bad,
                missing_job_lbs=int(round(bad.loc[bad["lbs"] == 0, "truth"].sum())),
                plant_est=float(mm["lbs"].sum()), plant_truth=float(mm["truth"].sum()))


def export_log(raw: pd.DataFrame, windows: pd.DataFrame) -> list[dict]:
    """One entry per archived export; snapshots carry the windows they close."""
    entries = []
    seen_posted: set = set()
    prev_snap = None
    files = raw.groupby("export_file").agg(ts=("export_ts", "first"), posted=("posted", "all")).sort_values("ts")
    for f, r in files.iterrows():
        rows = raw[(raw["export_file"] == f) & raw["Warehouse"].isin(SITE)]
        e = dict(file=f, ts=r["ts"].isoformat(), kind="posted" if r["posted"] else "in-process", jobs=int(rows["Job No"].nunique()))
        if r["posted"]:
            new = set(rows["Job No"]) - seen_posted
            e["new_posted"] = len(new)
            e["new_posted_lbs"] = float(rows.loc[rows["Job No"].isin(new), "Output Qty"].sum())
            seen_posted |= set(rows["Job No"])
        else:
            w = windows[windows["to_file"] == f] if len(windows) else windows
            e["prev_ts"] = prev_snap.isoformat() if prev_snap is not None else None
            e["hours_since"] = float((r["ts"] - prev_snap) / HOUR) if prev_snap is not None else None
            e["open_lbs"] = float(rows["Output Qty"].sum())
            e["delta_total"] = float(w["lbs"].sum()) if len(w) else 0.0
            e["lines"] = [dict(m=x["machine"], s=x["shift"], job=x["job"], lbs=float(x["lbs"]), t0=x["t0"].isoformat(), t1=x["t1"].isoformat(), days=x["days"])
                          for _, x in w.sort_values(["shift", "machine"]).iterrows() if abs(x["lbs"]) > 0]
            e["unchanged"] = int((w["lbs"].abs() == 0).sum()) if len(w) else 0
            prev_snap = r["ts"]
        entries.append(e)
    return entries


def run(export_dir: Path = EXPORT_DIR, verbose: bool = True) -> dict:
    raw = load_exports(export_dir)
    m = job_master(raw)
    closed_days = closures(m)
    horizon = max(raw["export_ts"].max().normalize(), m["Post Date"].max())
    pts = snapshot_points(raw, m)
    cov = coverage(m, closed_days, horizon, pts["export_ts"].max() if len(pts) else None)
    cov, windows = attribute(m, cov, pts)
    cells = to_cells(cov)
    val = validate(cells) if AGGREGATE.exists() else None
    snaps = raw.loc[~raw["posted"], "export_ts"]
    meta = dict(
        data_through=horizon.strftime("%Y-%m-%d"), export_date=raw["export_ts"].max().strftime("%Y-%m-%d"),
        first_day=cells["Date"].min().strftime("%Y-%m-%d"),
        last_snapshot=snaps.max().isoformat() if len(snaps) else None, snapshots=int(snaps.nunique()),
        exports=int(raw["export_file"].nunique()), shift_hours={k: [a, b] for k, (a, b) in SHIFT_HOURS.items()},
        closures=sorted(d.strftime("%Y-%m-%d") for d in closed_days),
        validation_pct_exact=val["pct_exact"] if val else None, validation_window="Aug 3–21",
        missing_job_lbs=val["missing_job_lbs"] if val else None,
        open_jobs=int((~m["posted"]).sum()), open_lbs=float(m.loc[~m["posted"], "Output Qty"].sum()),
    )
    if verbose:
        print(f"exports {meta['exports']} (snapshots {meta['snapshots']}, latest {meta['last_snapshot']}) · jobs {len(m)} "
              f"(open {meta['open_jobs']}, {meta['open_lbs']:,.0f} lbs so far) · data through {meta['data_through']}")
        print("closures:", ", ".join(d.strftime("%b %d") for d in sorted(closed_days) if d <= horizon))
        if val:
            print(f"validation {VALIDATION[0]:%b %d}–{VALIDATION[1]:%b %d}: {val['pct_exact']}% of {val['cells']} shift-days exact; "
                  f"plant {val['plant_est']:,.0f} vs {val['plant_truth']:,.0f}; disagreeing cells:")
            print(val["bad"].assign(Date=val["bad"]["Date"].dt.date, diff=(val["bad"]["lbs"] - val["bad"]["truth"]).round(0)).to_string(index=False))
        if len(windows):
            print("\nsnapshot windows:")
            w = windows.assign(t0=windows["t0"].dt.strftime("%a %m/%d %H:%M"), t1=windows["t1"].dt.strftime("%a %m/%d %H:%M"), lbs=windows["lbs"].round(0))
            print(w[["job", "line", "shift", "t0", "t1", "lbs", "days"]].to_string(index=False))
    return dict(meta=meta, cells=cells, cov=cov, windows=windows, log=export_log(raw, windows), jobs=m)


if __name__ == "__main__":
    run()
