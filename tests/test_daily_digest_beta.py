"""The beta daily email: the dashboard's bars drawn from table cells, no images."""
from datetime import date

import pandas as pd

from src import daily_digest_beta as db
from tests.test_daily_digest import STATUS, _df


def test_mix_blends_toward_white() -> None:
    assert db.mix("#2a78d6", 1.0) == "#2a78d6" and db.mix("#000000", 0.5) == "#808080"


def test_bars_follow_the_reading_order_with_shift_split_and_normal() -> None:
    b = db.bars_for_day(_df(), date(2026, 9, 21))
    assert [r["machine"] for r in b["rows"]] == ["EXTRUDER", "GUILLOTINE"]
    ext = b["rows"][0]
    assert ext["by_shift"]["1st"] == 5000 and ext["lbs"] == 10000 and ext["normal"] == 10500 and ext["pct"] == -5   # earlier Mondays: 5100..5400 per shift
    assert b["normal_days"] == 4 and b["total"] == 22000


def test_email_carries_the_bars_as_cells_and_leaves_the_regular_email_alone() -> None:
    df = _df()
    dg = db.dd.build_digest(df, STATUS, date(2026, 9, 21))
    beta = db.render_email_beta(dg, db.bars_for_day(df, date(2026, 9, 21)))
    assert "Pounds by machine" in beta and 'bgcolor="#2a78d6"' in beta and "<img" not in beta
    assert 'bgcolor="#4b5563"' in beta                       # the normal-day tick is a 2px cell
    assert "Pounds by machine" not in db.dd.render_email(dg)  # the current email is untouched
