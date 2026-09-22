"""
Shared configuration for the processing analysis pipeline.

Central source of truth for constants used across aggregation,
dashboard building, and validation scripts.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "processing_reports"
DEFAULT_AGGREGATED_DATA = DATA_DIR / "aggregated_daily_data.xlsx"
DEFAULT_AGGREGATED_NOTES = DATA_DIR / "aggregated_notes.xlsx"
DEFAULT_PAYROLL_DATA = DATA_DIR / "aggregated_payroll.xlsx"
EMPLOYEE_ROSTER_PATH = DATA_DIR / "employee_roster.json"
PAYROLL_PDF_DIR = DATA_DIR / "payroll_pdfs"

# ---------------------------------------------------------------------------
# Financial
# ---------------------------------------------------------------------------
LABOR_RATE: float = 25.0  # $/hr — used in aggregation and profit dashboard
OT_MULTIPLIER_1: float = 1.5   # Overtime tier 1 multiplier
OT_MULTIPLIER_2: float = 2.0   # Overtime tier 2 multiplier

# ---------------------------------------------------------------------------
# Machine definitions
# ---------------------------------------------------------------------------

# Row ranges for each machine in the Excel processing sheets (used by aggregation)
MACHINE_DATA_RANGES: dict[str, tuple[int, int]] = {
    "AUTO TIE BALER": (4, 13),
    "BALER 1": (16, 25),
    "BALER 2": (28, 37),
    "GUILLOTINE": (40, 44),
    "SHREDDER": (47, 50),
    "AVANGUARD DENSIFIER (OLD)": (53, 55),
    "GREEN MAX DENSIFIER (NEW)": (58, 60),
    "EXTRUDER": (63, 66),
    "GRINDER": (69, 74),
    "SMALL GRINDER": (77, 79),
}

# Weekly capacity (hours) per machine — for utilization % calculation
MACHINE_WEEKLY_CAPACITY: dict[str, int] = {
    "EXTRUDER": 120,                    # 24h/day × 5 days
    "GUILLOTINE": 120,                  # 24h/day × 5 days
    "AUTO TIE BALER": 80,              # 16h/day × 5 days
    "BALER 1": 80,
    "BALER 2": 80,
    "SHREDDER": 80,
    "GRINDER": 80,
    "SMALL GRINDER": 80,
    "AVANGUARD DENSIFIER (OLD)": 80,
    "GREEN MAX DENSIFIER (NEW)": 80,
}
DEFAULT_WEEKLY_CAPACITY = 80

# Utilization target — dashed line on utilization charts
UTILIZATION_TARGET_PCT = 85

# Weekly output targets (lbs) — only tracked machines are charted
MACHINE_WEEKLY_OUTPUT_TARGETS: dict[str, int] = {
    "EXTRUDER": 100_000,
    "GUILLOTINE": 100_000,
    "AUTO TIE BALER": 80_000,
    "GRINDER": 80_000,
    "GREEN MAX DENSIFIER (NEW)": 10_000,
}

# Per-machine default slider presets for the profit dashboard: (sale, buy, overhead)
MACHINE_PRESETS: dict[str, tuple[float, float, float]] = {
    "EXTRUDER":                    (0.25, 0.05, 0.04),
    "GUILLOTINE":                  (0.15, 0.03, 0.03),
    "AUTO TIE BALER":             (0.20, 0.05, 0.03),
    "BALER 1":                     (0.18, 0.04, 0.03),
    "BALER 2":                     (0.18, 0.04, 0.03),
    "SHREDDER":                    (0.12, 0.03, 0.03),
    "GRINDER":                     (0.15, 0.04, 0.03),
    "SMALL GRINDER":               (0.15, 0.04, 0.03),
    "AVANGUARD DENSIFIER (OLD)":   (0.20, 0.02, 0.04),
    "GREEN MAX DENSIFIER (NEW)":   (0.20, 0.02, 0.04),
}

# ---------------------------------------------------------------------------
# Product name normalization
# ---------------------------------------------------------------------------

# Fix common typos/variations before category mapping
PRODUCT_TYPO_MAP: dict[str, str] = {
    "Tisue bales": "Tissue bales",
    "Tisuue bales": "Tissue bales",
    "PS regrdins": "PS regrinds",
    "LD brickx": "LD bricks",
    "LD Bales / HD bales": "LD Bales/HD bales",
    "LD Bales/HDPE bales": "LD Bales/HD bales",
    "PET slab": "PET slabs",
    "PET sheds": "PET shreds",
    "PP Resin": "PP resin",
    "PP Shreds": "PP shreds",
    "HDPE bales": "HD Bales",
    "OCC bales": "OCC Bales",
    # End of Shift app "Material run" spellings (free text on a phone) -> product names
    "Mixed Plastic": "Mixed plastic", "Mixed plastics": "Mixed plastic",
    "Cardboard": "OCC Bales", "SBS": "SBS bales", "LDPE": "LD Bales",
    "HDPE": "HDPE shreds", "Hdpe": "HDPE shreds", "HIPS": "HIPS regrinds", "PET regrind": "PET regrinds",
    "Ricoh Slabs": "BOPP slabs", "Ricoh Slabs/BOPP": "BOPP slabs", "Ricoh slabs": "BOPP slabs",
    "EPS fines": "EPS",
}

# Map cleaned product names → standardized categories
PRODUCT_CATEGORY_MAP: dict[str, str] = {
    # End of Shift materials with no workbook precedent
    "Mixed plastic": "Mixed - Bales", "Toll bags": "Toll Bag Bales", "Cores": "Core Bales",
    # LDPE
    "LD Bales": "LDPE - Bales", "LD Nylon Bales": "LDPE - Bales",
    "Mix Film Bales": "LDPE - Bales",
    "LD bricks": "LDPE - Bricks/Foam", "LD foam bricks": "LDPE - Bricks/Foam",
    "LDPE bricks": "LDPE - Bricks/Foam", "LDPE foam": "LDPE - Bricks/Foam",
    "LDPE foam bricks": "LDPE - Bricks/Foam", "PE bricks": "LDPE - Bricks/Foam",
    "PE foam bricks": "LDPE - Bricks/Foam", "Foam slabs": "LDPE - Bricks/Foam",
    "LDPE regrinds": "LDPE - Regrinds",
    "LDPE resin": "LDPE - Resin",
    "LDPE slabs": "LDPE - Slabs",
    "LDPE shreds": "LDPE - Shreds",
    "LDPE slabs / HDPE slabs": "LDPE - Slabs",
    # HDPE
    "HD Bales": "HDPE - Bales",
    "LD Bales/HD bales": "HDPE/LDPE - Mixed Bales",
    "HDPE pieces": "HDPE - Regrinds", "HDPE regrinds": "HDPE - Regrinds",
    "HDPE slabs": "HDPE - Slabs",
    "HDPE shreds": "HDPE - Shreds",
    # PP
    "PP Bales": "PP - Bales",
    "PP regrinds": "PP - Regrinds", "PP pallet regrinds": "PP - Regrinds",
    "PP resin": "PP - Resin",
    "PP shreds": "PP - Shreds",
    "Pallet slabs": "PP - Slabs",
    # PS
    "PS": "PS - Bales", "PS bales/purge": "PS - Bales",
    "PS regrinds": "PS - Regrinds",
    "PS shreds": "PS - Shreds",
    "PS slabs": "PS - Slabs",
    # PET
    "PET": "PET - Bales", "PET bales": "PET - Bales",
    "PET regrinds": "PET - Regrinds",
    "PET - Regrinds": "PET - Regrinds",  # already-mapped passthrough
    "PET shreds": "PET - Shreds",
    "PET slabs": "PET - Slabs",
    # EPS
    "EPS": "EPS - Densified", "EPS resin": "EPS - Resin", "EPS slabs": "EPS - Slabs",
    # BOPP
    "BOPP": "BOPP - Bales", "BOPP regrinds": "BOPP - Regrinds",
    "BOPP resin": "BOPP - Resin", "BOPP slabs": "BOPP - Slabs",
    # Paper / Fiber
    "OCC Bales": "OCC Bales", "Paper bales": "Paper Bales",
    "Tissue bales": "Tissue Bales",
    "SBS bales": "SBS Bales", "SOP bales": "SOP Bales",
    "Strapping bales": "Strapping Bales", "Supersack Bales": "Supersack Bales",
    # Specialty
    "Nylon regrinds": "Nylon - Regrinds", "EVA regrinds": "EVA - Regrinds",
    "HIPS regrinds": "HIPS - Regrinds",
    "Rotomold regrinds": "Rotomold - Regrinds", "Rotomold slabs": "Rotomold - Slabs",
    "Plastic slabs": "Mixed - Slabs",
    "Mixed plastic shreds": "Mixed - Shreds", "Mixed regrinds": "Mixed - Regrinds",
}

# ---------------------------------------------------------------------------
# Dashboard display defaults
# ---------------------------------------------------------------------------
CHART_PALETTE = [
    "#0B6E4F", "#2CA58D", "#84BCDA", "#33658A", "#F26419",
    "#FFAF87", "#3A3042", "#5BC0BE", "#C5283D", "#1f77b4",
    "#e377c2",
]

DEFAULT_WEEKS = 20          # number of weeks shown by default
RUNNING_AVG_WINDOW = 4      # running-average smoothing window (weeks)
COST_PER_POUND_THRESHOLD = 0.10  # highlight threshold in weekly table

# ---------------------------------------------------------------------------
# Aggregation sheet layout
# ---------------------------------------------------------------------------
COL_MACHINE_HOURS = 1
COL_MAN_HOURS = 2
COL_INPUT_ITEM = 3
COL_ACTUAL_INPUT = 4
COL_OUTPUT_PRODUCT = 5
COL_ACTUAL_OUTPUT = 6
COL_OPERATOR = 7
COL_COMMENT = 8
COL_DATE = 9

DAILY_SHEETS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

# A row covers one machine on one day, so a machine cannot run more than 24
# hours on it. Anything above is a typo (2026-08-07 EXTRUDER recorded 713.25).
# Such hours are treated as UNRECORDED for rate purposes — the output still
# counts, we just can't say how long it took.
MAX_MACHINE_HOURS_PER_DAY = 24.0

# Columns that identify a genuinely duplicated row (same report ingested
# twice). Operator and hours are included so that two operators who happen
# to post identical output on the same machine/shift/day are NOT collapsed.
# Used by BOTH aggregate_daily_data (drops dupes) and validate_data (asserts
# none remain), so the keys MUST stay in sync.
# Input_Item/Actual_Input are included because Guillotine output is routinely
# left unweighed (Actual_Output == 0), so two genuinely different runs on one
# machine/day/shift can be identical in every other column and differ only in
# what went in. Without them the second run is silently deleted.
DEDUP_SUBSET = [
    "Date", "Shift", "Machine_Name", "Output_Product", "Actual_Output",
    "Operator", "Machine_Hours", "Man_Hours",
    "Input_Item", "Actual_Input",
]

# Weeks (Monday, ISO) with no production report for any shift, confirmed
# absent at the source rather than lost in parsing. Acknowledged here so the
# missing-week check reports only NEW gaps — a permanent one-line warning
# trains people to ignore the whole block.
#   2025-11-10: no workbook was ever sent for any of the three shifts.
KNOWN_DATA_GAPS: set[str] = {
    "2025-11-10",
}

NOTE_CATEGORIES: dict[str, list[str]] = {
    "downtime": ["down", "stopped", "broken", "repair", "fix", "belt", "chiller", "filter"],
    "material": ["no material", "waiting for material", "material shortage", "ran out"],
    "quality": ["no weights", "missing", "not entered", "incomplete"],
}

# Key metrics shown by default (running average)
KEY_METRICS: dict[str, tuple[str, str]] = {
    "Actual_Output": ("Actual Output (Lbs)", "int"),
    "Output_per_Hour": ("Output per Hour", "float1"),
    "Production_Cost_per_Pound": ("Production Cost per Pound", "currency4"),
    "Total_Expense": ("Total Expense", "currency"),
}

# Full list of available metrics (toggle in dashboard)
ALL_METRICS: dict[str, tuple[str, str]] = {
    "Actual_Output": ("Actual Output (Lbs)", "int"),
    "Output_per_Hour": ("Output per Hour", "float1"),
    "Output_per_Man_Hour": ("Output per Man-Hour", "float1"),
    "Production_Cost_per_Pound": ("Production Cost per Pound", "currency4"),
    "Total_Machine_Hours": ("Total Machine Hours", "float1"),
    "Total_Man_Hours": ("Total Man Hours", "float1"),
    "Labor_Cost": ("Labor Cost", "currency"),
    "Total_Expense": ("Total Expense", "currency"),
}

# ---------------------------------------------------------------------------
# End-of-Shift labor capture (paper photo -> extractor, or Google Form)
# ---------------------------------------------------------------------------

WALTON_CONFIG_DIR = Path.home() / ".config" / "walton"
SHIFT_REPORT_DIR = DATA_DIR / "shift_reports"        # emailed photos land here (gitignored)

# Row labels as printed on the End of Shift sheet (and the Google Form's
# machine dropdown), keyed by their lowercase alphanumeric slug, mapped to the
# canonical machine names used everywhere else. Both supervisors' templates
# are covered: "Densifier"/"New Densifier" is the Green Max, "Big densifier"
# the Avanguard, and "Shredder/Grinder" is the grinder line.
SHIFT_FORM_MACHINE_MAP: dict[str, str] = {
    "autotie": "AUTO TIE BALER",
    "autotiebaler": "AUTO TIE BALER",
    "baler1": "BALER 1",
    "baler2": "BALER 2",
    "bigdensifier": "AVANGUARD DENSIFIER (OLD)",
    "avanguarddensifier": "AVANGUARD DENSIFIER (OLD)",
    "densifier": "GREEN MAX DENSIFIER (NEW)",
    "newdensifier": "GREEN MAX DENSIFIER (NEW)",
    "greenmax": "GREEN MAX DENSIFIER (NEW)",
    "greenmaxdensifier": "GREEN MAX DENSIFIER (NEW)",
    "extruder": "EXTRUDER",
    "guillotine": "GUILLOTINE",
    "shredder": "SHREDDER",
    "shreddergrinder": "GRINDER",
    "grinder": "GRINDER",
    "smallgrinder": "SMALL GRINDER",
}

# Machine choices offered on the Google Form, in the order the paper sheet
# lists them. Display label -> canonical name.
SHIFT_FORM_MACHINE_CHOICES: list[tuple[str, str]] = [
    ("Auto tie baler", "AUTO TIE BALER"),
    ("Baler 1", "BALER 1"),
    ("Baler 2", "BALER 2"),
    ("Big densifier (Avanguard)", "AVANGUARD DENSIFIER (OLD)"),
    ("New densifier (Green Max)", "GREEN MAX DENSIFIER (NEW)"),
    ("Extruder", "EXTRUDER"),
    ("Guillotine", "GUILLOTINE"),
    ("Shredder", "SHREDDER"),
    ("Shredder/Grinder", "GRINDER"),
    ("Small grinder", "SMALL GRINDER"),
]


# ---------------------------------------------------------------------------
# cieTrade API — Converting Job Inquiry (ListConvertingJobs)
# ---------------------------------------------------------------------------
# Credentials live outside the repo: {"base_url", "user_id", "api_key"} in
# ~/.config/walton/cietrade.json (chmod 600), or env CIETRADE_USER_ID /
# CIETRADE_API_KEY. The key goes in the Authorization header, never in a URL.
CIETRADE_CONFIG_PATH = WALTON_CONFIG_DIR / "cietrade.json"
CIETRADE_BASE_URL = "https://api.cietrade.net"
CIETRADE_DATA_DIR = DATA_DIR / "cietrade"                 # polls.jsonl, posted.csv, snapshots/  (committed)
CIETRADE_EXPORT_DIR = DATA_DIR / "cietrade_exports"       # manual Converting Inquiry CSV exports (history)
CIETRADE_SITES = {"Plus Monroe Warehouse", "Monroe Processing Warehouse"}
CIETRADE_POSTED_LOOKBACK_DAYS = 14                        # each poll re-reads postings this far back
CIETRADE_POLL_INTERVAL_MIN = 10

# First production day fed by cieTrade instead of the hand-built workbooks
# (the last workbook covers Fri 2026-08-21). Rows from this date on are
# regenerated from cieTrade + the End of Shift app on every run.
CIETRADE_FROM_DATE = "2026-08-24"

# cieTrade line (machine name without its "(1ST SHIFT)" tag) -> dashboard Machine_Name.
CIETRADE_LINE_TO_MACHINE: dict[str, str] = {
    "AUTO-TIE BALER": "AUTO TIE BALER",
    "BALER 1": "BALER 1", "BALER1": "BALER 1",
    "BALER 2": "BALER 2", "BALER2": "BALER 2",
    "GUILLOTINE": "GUILLOTINE",
    "SHREDDER": "SHREDDER",
    "AVANGARD (OLD)": "AVANGUARD DENSIFIER (OLD)",
    "GREEN MAX (NEW)": "GREEN MAX DENSIFIER (NEW)",
    "EXTRUDER": "EXTRUDER",
    "SHREDDER/GRINDER": "GRINDER",
    "SMALL GRINDER": "SMALL GRINDER",
}

# Working hours of each shift as hours after midnight of the shift's date
# (3rd shift runs into the next morning). Used to split a job's output
# between two API polls across the shifts that ran in between.
SHIFT_HOURS: dict[str, tuple[int, int]] = {"1st": (6, 14), "2nd": (14, 22), "3rd": (22, 30)}   # 6-2, 2-10, 10-6 (confirmed 2026-09-17)

# Where the poller drops a copy of the freshly built pilot page so it can be
# opened from a phone through OneDrive. Set to None to disable.
LIVE_PAGE_COPY = Path.home() / "Library" / "CloudStorage" / "OneDrive-PlusMaterials" / "Walton Live" / "production.html"

# Live feed: rewritten after every poll (gitignored) and published to a gist so the
# static dashboard can fetch today's state without a redeploy. Set LIVE_GIST_ID once
# after `gh gist create` (see setup/CIETRADE_API.md).
LIVE_JSON_PATH = CIETRADE_DATA_DIR / "live.json"
LIVE_GIST_ID = "ceed60b99ea8b27ddf163c80f10b7b7e"
LIVE_GIST_USER = "zabdulla"
LIVE_FEED_URL = f"https://gist.githubusercontent.com/{LIVE_GIST_USER}/{LIVE_GIST_ID}/raw/live.json" if LIVE_GIST_ID else ""
LIVE_QUIET_MINUTES = 60          # a machine that produced this shift but not for this long gets flagged
LIVE_FEED_LENGTH = 20
