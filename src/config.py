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

# ---------------------------------------------------------------------------
# Financial
# ---------------------------------------------------------------------------
LABOR_RATE: float = 25.0  # $/hr — used in aggregation and profit dashboard

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

ALL_MACHINES = list(MACHINE_DATA_RANGES.keys())

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
DEFAULT_PRESET: tuple[float, float, float] = (0.20, 0.05, 0.03)

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
    "PP Resin": "PP resin",
    "PP Shreds": "PP shreds",
    "HDPE bales": "HD Bales",
    "OCC bales": "OCC Bales",
}

# Map cleaned product names → standardized categories
PRODUCT_CATEGORY_MAP: dict[str, str] = {
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
