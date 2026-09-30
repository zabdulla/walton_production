"""Beta dashboard: model figures, chart labels, and the page written end to end. No network."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd

import build_beta_dashboard as bb


def _df() -> pd.DataFrame:
    rows = []
    for wk in range(8):
        for dow in range(5):
            d = pd.Timestamp("2026-09-28") - pd.Timedelta(weeks=wk) + pd.Timedelta(days=dow)
            if d > pd.Timestamp("2026-09-29"):
                continue
            for sh in ("1st", "2nd"):
                rows.append({"Date": d, "Shift": sh, "Machine_Name": "EXTRUDER", "Actual_Output": 5000 + 100 * wk, "Actual_Input": 0})
                rows.append({"Date": d, "Shift": sh, "Machine_Name": "GUILLOTINE", "Actual_Output": 0, "Actual_Input": 6000})
    return pd.DataFrame(rows)


STATUS = {"last_poll": "2026-09-30T06:00:00", "open_jobs": 3, "open_lbs": 12000, "end_of_shift": {"reports": [
    {"date": "2026-09-29", "shift": "1st", "by": "Tim", "machines": [
        {"machine": "EXTRUDER", "machine_hours": 7.0, "man_hours": 14.0, "operators": "Steven", "material": "BOPP", "downtime_min": 60, "reason": "Blades", "comment": ""}],
     "notes": ["clean-up"]},
    {"date": "2026-09-26", "shift": "3rd", "by": "Connor", "machines": [], "notes": []}]}}


def _load(tmp_path):
    p = tmp_path / "agg.xlsx"
    _df().to_excel(p, index=False)
    s = tmp_path / "status.json"
    s.write_text(json.dumps(STATUS))
    return bb.load(p, s)


def test_model_counts_guillotine_input_and_compares_like_for_like(tmp_path) -> None:
    df, status = _load(tmp_path)
    M = bb.model(df, status, today=date(2026, 9, 30))
    assert M["day"] == "2026-09-29"                                   # last complete day, not today
    assert M["yesterday"]["lbs"] == 2 * (5000 + 6000)                 # Guillotine's rolls count on the support basis
    assert M["yesterday"]["avg4"] == 2 * (6000 + (5100 + 5200 + 5300 + 5400) / 4)
    assert M["wtd"]["days"] == 2 and M["wtd"]["last_week"] == 2 * 2 * (5100 + 6000)
    assert M["filed"] == {"1st": "Tim", "2nd": None, "3rd": None}
    assert [f["m"] for f in M["facets"]] == ["GUILLOTINE", "EXTRUDER"]
    assert len(M["facets"][0]["weeks"]) == 7                           # complete weeks only


def test_facet_end_label_never_sits_on_a_tick_label() -> None:
    f = {"m": "GRINDER", "weeks": ["2026-08-03", "2026-08-10", "2026-08-17"], "raw": [1000, 1000, 1000], "avg": [1000, 1000, 1000]}
    svg = bb.svg_facet(f)                                             # latest value sits 1.5px from the upper (0.9 * vmax) tick
    right = [t for t in svg.split("<text")[1:] if 'x="266"' in t]      # labels in the right-hand gutter
    ys = sorted(float(t.split('y="')[1].split('"')[0]) for t in right)
    assert all(b - a > 8 for a, b in zip(ys, ys[1:])), ys
    assert 'class="endlab">1,000' in svg and ">972<" not in svg        # the tick yields, the end label stays
    assert svg.count('class="grid"') == 2                              # both gridlines still drawn


def test_main_writes_every_view(tmp_path) -> None:
    p = tmp_path / "agg.xlsx"; _df().to_excel(p, index=False)
    s = tmp_path / "status.json"; s.write_text(json.dumps(STATUS))
    out = bb.main(p, tmp_path / "beta" / "index.html", s)
    page = out.read_text()
    for view in ("today", "week", "shifts", "machines", "more"):
        assert f'id="view-{view}"' in page or f'href="#{view}"' in page
    assert 'class="tabbar"' in page and "Tim" in page and "Blades" in page
