"""Inbound dashboard model: receipt lines -> loads, due board, page. No network."""
from __future__ import annotations

from datetime import date

import cietrade_inbound as inb


def line(**kw):
    base = dict(WorksheetNo="PR-1", Department="Walton Domestic", Status="INVOICED", ShippingDt="2026-09-10", PoOrderDt="2026-09-04",
                Supplier="ACME", ShipTo="Plus Monroe Warehouse", ProductName="OCC bales", Units="20", UnitType="Bales",
                GrossWt="40000", TareWt="1000", NetWt="39000", PWeight="39000", PWeightUOM="LBS", PO="1001", PPrice="0.05", PAmount="1950", FreightExp="0",
                POShipVia="Vendor", **{"Buy-Rep": "Murad"}, EquipNo="")
    base.update(kw)
    return base


def test_lines_normalise_weights_units_and_flags() -> None:
    df = inb.normalize_lines([
        line(),
        line(WorksheetNo="PR-2", GrossWt="1000", TareWt="0", NetWt="1000", PWeight="1000", PWeightUOM="KG", PO=""),
        line(WorksheetNo="PR-3", GrossWt="0", TareWt="0", NetWt="0", PWeight="6", PWeightUOM="EA", Status="WORK", PoOrderDt="2026-09-12"),
        line(WorksheetNo="PR-4", GrossWt="9900", TareWt="0", NetWt="14600", PWeight="14600"),
    ])
    r = df.set_index("ws")
    assert r.loc["PR-1", "net"] == 39000 and r.loc["PR-1", "lead"] == 6 and r.loc["PR-1", "posted"] and r.loc["PR-1", "flags"] == []
    assert r.loc["PR-2", "net"] == round(1000 * inb.KG_TO_LBS) and "no PO" in r.loc["PR-2", "flags"] and "no tare" in r.loc["PR-2", "flags"]
    assert r.loc["PR-3", "net"] == 0 and "uom EA" in r.loc["PR-3", "flags"] and not r.loc["PR-3", "posted"] and "received before PO" in r.loc["PR-3", "flags"]
    assert "net > gross" in r.loc["PR-4", "flags"]


def test_loads_roll_up_lines_and_tare_pct() -> None:
    df = inb.normalize_lines([line(), line(ProductName="OCC loose", GrossWt="10000", TareWt="3000", NetWt="7000", PWeight="7000", Units="5"),
                              line(WorksheetNo="PR-9", Supplier="ZED", Status="WORK")])
    loads = inb.loads_from_lines(df)
    one = loads.set_index("ws").loc["PR-1"]
    assert one.net == 46000 and one.units == 25 and one.lines == 2 and one.grades == "OCC bales, OCC loose"
    assert one.tare_pct == 8.0 and bool(one.posted)
    assert not bool(loads.set_index("ws").loc["PR-9"].posted)


def test_due_board_lists_open_pos_without_receipts_oldest_first() -> None:
    loads = inb.loads_from_lines(inb.normalize_lines([line(PO="1001")]))
    pos = [dict(order_number="1001", account_name="ACME", department="Walton Domestic", product="OCC", order_date="2026-09-04", ship_via="Vendor", UDF1="Plus Monroe Warehouse", UDF2="$500"),
           dict(order_number="1002", account_name="BETA", department="Walton Logistics", product="PET", order_date="2026-08-20", ship_via="", UDF1="", UDF2=""),
           dict(order_number="1003", account_name="GAMMA", department="Walton Logistics", product="PET", order_date="2026-09-15", ship_via="", UDF1="", UDF2="")]
    due = inb.due_board(pos, loads, date(2026, 9, 18))
    assert [d["po"] for d in due] == ["1002", "1003", "1001"]
    assert due[0]["age"] == 29 and not due[0]["received"]
    assert due[2]["received"] and due[2]["last_receipt"] == "2026-09-10"


def test_payload_and_page_render_offline() -> None:
    payload = inb.build_payload([line(), line(WorksheetNo="PR-2", ShipTo="PRN Southeast", Department="PRN")],
                                [dict(order_number="7", account_name="X", department="PRN", product="P", order_date="2026-09-01", ship_via="", UDF1="", UDF2="")],
                                as_of=date(2026, 9, 18), days=91)
    assert payload["default_warehouse"] == "Plus Monroe Warehouse" and payload["warehouses"] == ["PRN Southeast", "Plus Monroe Warehouse"]
    html = inb.render_html(payload)
    assert "window.__P" in html and "Inbound due board" in html and "PR-2" in html


def test_bulk_processing_input_lines_are_not_loads() -> None:
    payload = inb.build_payload([line(), line(WorksheetNo="PR-5", Supplier="Walton logistics", ProductName="Processing Input [Bulk]", GrossWt="1000000", NetWt="1000000", PWeight="1000000")],
                                [], as_of=date(2026, 9, 18))
    assert [l["ws"] for l in payload["loads"]] == ["PR-1"] and payload["excluded_adjustment_lines"] == 1
