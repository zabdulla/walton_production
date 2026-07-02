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
| `Actual_Input` | float | Feedstock weight (lbs) |
| `Output_Product` | str | Product as typed (typos normalized via `PRODUCT_TYPO_MAP`) |
| `Actual_Output` | float | Output weight (lbs) |
| `Machine_Hours` | float | Machine run time that day |
| `Man_Hours` | float | Total labor hours (can exceed 24 — multiple operators) |
| `Operator` | str | First name(s), comma-separated when a crew shared the machine |
| `Comment` | str | Supervisor note, if any (also extracted to `aggregated_notes.xlsx`) |
| `Output_per_Hour` | float | `Actual_Output / Machine_Hours` (0 when no hours) |
| `Labor_Cost` | float | `Man_Hours × config.LABOR_RATE` ($25/hr) |
| `Total_Expense` | float | `Labor_Cost × overhead_multiplier` (currently 1.0) |
| `Cost_per_Pound` | float | `Total_Expense / Actual_Output` (0 when no output) |
| `Has_Machine_Hours` / `Has_Man_Hours` / `Has_Output` / `Has_Comment` | bool | Data-presence flags |
| `Data_Quality_Score` | int | See below |

**Data_Quality_Score (0–100):**
`25·has_machine_hours + 25·has_man_hours + 40·has_output + 10·consistency`
where the consistency bonus applies when machine-hours presence matches
output presence (both or neither).

**Duplicate identity** (`config.DEDUP_SUBSET`): `Date, Shift, Machine_Name,
Output_Product, Actual_Output, Operator, Machine_Hours, Man_Hours`. Operator
and hours are included so two operators posting identical output are NOT
collapsed. Aggregation drops on this key; validation asserts none remain.

## `data/aggregated_notes.xlsx`

One row per supervisor comment: `Date, Shift, Machine_Name, Operator, Note,
Category`. Category is keyword-derived (`config.NOTE_CATEGORIES`):
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
