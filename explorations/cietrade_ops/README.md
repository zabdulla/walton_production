# cieTrade operations pilot (local only)

What else the cieTrade API can feed besides converting jobs — receiving, purchase and
sales orders, loads and shipments, inventory — for the Monroe sites and Walton Logistics.
Nothing here is published: `out/` is gitignored and the page is opened from disk. The repo
is public, so no cieTrade rows, supplier names or volumes are committed; only code.

    python3 explorations/cietrade_ops/explore.py     # probe the documented endpoints, save samples to out/
    python3 explorations/cietrade_ops/build.py       # raw profile of every endpoint that answered -> out/cietrade_ops_pilot.html
    python3 explorations/cietrade_ops/pilot.py       # curated page from out/analysis/*.json (see below)

`explore.py` needs the poller's credentials (`~/.config/walton/cietrade.json`, or the
`CIETRADE_USER_ID` / `CIETRADE_API_KEY` environment variables). It probes every documented
read endpoint (`API_REFERENCE.md`, distilled from cieTrade's help center) with the parameters
each needs, records HTTP status, error text, columns and row counts in `out/discovery.json`,
and keeps the rows in `out/samples/`. `--endpoint Name --param k=v --days N` for one call.

`out/analysis/{inbound,orders,onhand,loads}.{json,md}` are aggregates and findings produced
by four analysis passes on 2026-09-18 (inbound receipt lots, PO/SO headers joined to receipts,
on-hand lots with days of supply from converting-job input, worksheets/trading lines). They
are not yet scripted: to refresh, re-run those passes or promote the views that earn their
keep into `src/`.

## What answered (2026-09-18)

| Endpoint | What it is | Needs |
|---|---|---|
| ListConvertingJobs | one row per converting job (already the production feed) | dates |
| ListInventory | one row per lot: PR-* purchase receipts (inbound loads, PO link, gross/tare/net, supplier, warehouse, age, on-hand) and CJ-* job outputs | DateFrom/DateTo (received_date) |
| ListOrders / ListOrderDetails | PO and SO headers / grade lines | Source=PO or SO |
| ListWorksheets / ListWorksheetDetails / ListWorksheetExpenses | one row per load (receipt, inventory sale, brokerage), lines, freight | Status=ALL to include unposted |
| TradingInquiry | per-line trading view with PO/SO links, dates, weights, margin | Status=ALL |
| ListAdjustments, ListDispatchJobs | answer but empty for us | dates / Type |
| ListAccounts, ListAccountLocations, ListContacts | master data | — |

## ListConvertingJobs

Per job it returns: job number and
date, warehouse and warehouse status, machine (with shift in the name), status (WORK /
POSTED), post date, start/end/elapsed time, UOM, input/output/yield-loss quantities and
units, department, description, input/output value and expenses, two user-defined fields,
operator, finished product. Filters: JobNo, DateType (JOB | POST), DateFrom, DateTo,
Warehouse, WarehouseStatus, Status (WORK | POSTED | ALL), Machine, Dept, Operator,
FinishedProduct.

## Ideas to test once inbound / PO / SO data answers

1. **Receiving log** — loads received per day and week by supplier, material and warehouse;
   gross / tare / net; ticket weight vs PO expected (variance and short loads).
2. **PO fulfilment** — open purchase orders, expected vs received, aging; POs past due with no
   receipt yet.
3. **SO fulfilment / outbound** — shipped vs ordered per sales order, on-time rate, loads per
   day; what is due to ship this week and whether finished goods cover it.
4. **Inventory on hand** by material and warehouse, turned into *days of supply* using the
   converting-job input rate we already track — the number that says when a line will
   starve.
5. **Material balance** — inbound lbs vs converting input vs outbound lbs per material per
   month: yield and shrink the accounting never shows.
6. **Warehouse throughput** — receipts + shipments + jobs per day against End of Shift hours,
   for labor planning at Plus Monroe.
7. **Supplier scorecard** — weight variance, rejects, load frequency.
8. **Alerts** — PO due without a receipt; SO ship date near with no inventory; a load received
   with no PO.
