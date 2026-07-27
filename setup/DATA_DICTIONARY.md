# Data Dictionary

## `data/aggregated_daily_data.xlsx`

One row per (date, shift, machine, output product) observation, extracted from
the shift workbooks in `processing_reports/`. Written atomically with rolling
snapshots in `data/snapshots/`. All columns snake_case — this is the canonical
schema end-to-end; display labels are applied only at render time.

| Column | Type | Meaning |
|---|---|---|
| `Date` | date | Production day (from the sheet's date cell, with weekday-inference fallback) |
| `Day_of_Week` | str | Sheet name: Mon…Sat |
| `Week_Start`, `Week_End` | date | Week range parsed from the workbook filename |
| `Shift` | str | `1st` / `2nd` / `3rd` from the filename; `unspecified` in pre-2025 files |
| `Machine_Name` | str | One of the 10 machines in `config.MACHINE_DATA_RANGES` |
| `Input_Item` | str | Feedstock description as typed by the supervisor |
| `Actual_Input` | float | Weight (lbs) — see "one weight, two columns" below |
| `Output_Product` | str | Product as typed (typos normalized via `PRODUCT_TYPO_MAP`) |
| `Actual_Output` | float | Weight (lbs) — see "one weight, two columns" below |
| `Machine_Hours` | float | Machine run time that day |
| `Man_Hours` | float | **Crew** total labor hours, not one person's — divide by the number of names in `Operator` for anything per-person (see below) |
| `Operator` | str | First name(s), comma-separated when a crew shared the machine |
| `Comment` | str | Supervisor note, if any (also extracted to `aggregated_notes.xlsx`) |
| `Output_per_Hour` | float | `Actual_Output / Machine_Hours`, **NaN when hours are 0** — a 0 would read as "produced nothing per hour" and drag averages down |
| `Labor_Cost` | float | `Man_Hours × config.LABOR_RATE` ($25/hr) |
| `Total_Expense` | float | `Labor_Cost × overhead_multiplier` (currently 1.0) |
| `Cost_per_Pound` | float | `Total_Expense / Actual_Output`, **NaN when output is 0** — a 0 would read as free production |
| `Has_Machine_Hours` / `Has_Man_Hours` / `Has_Output` / `Has_Comment` | bool | Data-presence flags |
| `Data_Quality_Score` | int | See below |
| `Date_Corrected` | bool | True when the typed date cell disagreed with the sheet's tab label (see below) |

**Data_Quality_Score (0–100):**
`25·has_machine_hours + 25·has_man_hours + 40·has_output + 10·consistency`
where the consistency bonus applies when machine-hours presence matches
output presence (both or neither).

**One weight, two columns — there is no yield data.** `Actual_Input` and
`Actual_Output` are the *same measurement* wherever both are present:
identical on 3,594 of 3,595 rows (the one exception differs by 1 lb, a
typo). The material is weighed once. `Input_Item` and `Output_Product` do
describe genuinely different things (PP Film → PP resin), so the row
records *what it became*, not *how much was lost*.

Do **not** compute yield, shrinkage, or material loss from these columns —
it is 100.0% by construction on every machine, which is an artifact, not a
fact about the plant. Answering that question needs a second weighing that
the shift reports do not currently capture.

1,093 rows carry an input weight with no output weight (7.08M lbs). 1,076
of them are GUILLOTINE, whose output is routinely not re-weighed — this is
exactly what the "with Guillotine support" view in the interactive
dashboard exists to correct (`_apply_guillotine_support`). The remaining 17
are SHREDDER rows (80,213 lbs) and are **not** covered by that adjustment.

**`Man_Hours` is a crew total.** The median `Man_Hours / Machine_Hours`
ratio is 1.00 for one operator, 1.88 for two and 2.69 for three. Any
per-person figure must divide by the crew size (comma-split `Operator`);
`build_operator_dashboard.explode_operators` is the reference
implementation. Getting this wrong inflates summed labor ~1.9x.

**Which day a row belongs to.** The sheet tabs are a fixed template
(Mon–Sat), so the tab label is structural; the date cell in row 0 is typed
by hand each week and carries both month/day transpositions and copy-paste
errors. When the two disagree the label wins, `Date` is set to the matching
day inside the file's week, and `Date_Corrected` is set True. Validation
enforces the resulting invariant: `Date`'s weekday always equals
`Day_of_Week`, and a fresh violation blocks publication.

**Duplicate identity** (`config.DEDUP_SUBSET`): `Date, Shift, Machine_Name,
Output_Product, Actual_Output, Operator, Machine_Hours, Man_Hours`. Operator
and hours are included so two operators posting identical output are NOT
collapsed. Aggregation drops on this key; validation asserts none remain.

## `data/aggregated_notes.xlsx`

One row per supervisor comment: `Date, Shift, Machine_Name, Input_Item,
Operator, Note, Category`. Category is keyword-derived (`config.NOTE_CATEGORIES`):
`downtime`, `material`, `quality`, else `operational`.

## `data/aggregated_payroll.xlsx` (gitignored — PII)

One row per employee per bi-weekly pay period, parsed from the Walton
pay-period PDFs: `employee_name, first/last, department, reg, ot1, ot2, vac,
hol, sick, other, total, worked_hours (reg+ot), pto_hours (vac+hol+sick+other),
period_start, period_end`. Deduped on `(employee_name, period_start,
period_end)`.

## `data/employee_roster.json` (gitignored — PII)

Maps payroll names to production aliases and roles. Schema and all five roles
(`machine_operator`, `shipping_receiving`, `maintenance`, `hybrid_sr`,
`supervisor` — plus optional `shift_filter` and `pay_rate` overrides) are
documented in `data/employee_roster.example.json`.

## Excel source layout (`processing_reports/*.xlsx`)

Six daily sheets (Mon–Sat). Each machine owns a fixed row range
(`config.MACHINE_DATA_RANGES`); columns are fixed positions
(`config.COL_*`): machine hours, man hours, input item, input weight,
output product, output weight, operator, comment, and the sheet date in
row 0 column 9.
