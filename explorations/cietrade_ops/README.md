# cieTrade operations pilot (local only)

What else the cieTrade API can feed besides converting jobs — receiving, purchase and
sales orders, shipments, inventory — for Walton Logistics and the Plus Monroe warehouse.
Nothing here is published: `out/` is gitignored and the page is opened from disk.

    python3 explorations/cietrade_ops/explore.py     # probe candidate endpoints, save samples to out/
    python3 explorations/cietrade_ops/build.py       # render out/cietrade_ops_pilot.html
    open explorations/cietrade_ops/out/cietrade_ops_pilot.html

`explore.py` needs the same credentials as the poller (`~/.config/walton/cietrade.json`).
It tries the screen names cieTrade would expose as `List<Screen>` (see `CANDIDATES`),
records HTTP status, error text, columns and row counts in `out/discovery.json`, and keeps
the rows in `out/samples/`. Re-run with `--endpoint Name --param k=v` once the real names
are known; cieTrade's own docs (<https://cietrade.helpscoutdocs.com/>, the API category)
list them.

## What we know today

Only `ListConvertingJobs` is documented and used. Per job it returns: job number and
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
