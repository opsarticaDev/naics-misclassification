"""
OSHA NAICS Validation Pipeline: Shared Configuration
======================================================
Paths, thresholds, and common I/O functions used across all gate scripts.
Import from this module to ensure consistent configuration.
"""

import csv
import os
import openpyxl

# ---------------------------------------------------------------------------
# Dataset Year: change when new data arrives
# ---------------------------------------------------------------------------
DATASET_YEAR = "2023"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
INPUT_FILE = os.path.join(PIPELINE_DIR, f"ita_data_{DATASET_YEAR}.csv")
OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"flagged_output_{DATASET_YEAR}.csv")
BLS_FILE = os.path.join(BASE_DIR, "Reference", "BLS_Table1_Injury.csv")
NAICS_2017_FILE = os.path.join(BASE_DIR, "Reference", "naics_2017_official.xlsx")
NAICS_2022_FILE = os.path.join(BASE_DIR, "Reference", "naics_2022_official.xlsx")
LOG_FILE = os.path.join(BASE_DIR, "Scripts", "pipeline.log")

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
INJURY_RATE_TOLERANCE = 2.0     # Flag if TRIR > 2x BLS benchmark
HIGH_EMP_THRESHOLD = 50         # "Large" establishment for zero-injury gate
HIGH_RISK_BLS_FLOOR = 2.0       # BLS rate above which industry is "high risk"
EMP_SIZE_FLOOR = 5              # Ignore tiny establishments for size gate
NAICS_RARITY_OSHA_FLOOR = 3     # Flag if NAICS appears fewer than N times


# ---------------------------------------------------------------------------
# Common I/O
# ---------------------------------------------------------------------------

def load_records(filepath=None):
    """Load a CSV file into a list of dicts."""
    if filepath is None:
        filepath = INPUT_FILE
    records = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    return records


def write_records(records, filepath=None, fieldnames=None):
    """Write records to a CSV file."""
    if filepath is None:
        filepath = OUTPUT_FILE
    if fieldnames is None:
        fieldnames = list(records[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


# ---------------------------------------------------------------------------
# Shared reference loaders
# ---------------------------------------------------------------------------

def load_valid_naics_codes():
    """Load NAICS codes from official 2017 and 2022 Census Bureau xlsx files.

    Returns:
        valid_6digit:  set of valid 6-digit codes (complete codes)
        valid_prefixes: set of all valid codes at any level 2-6 digits
    """
    valid_6digit = set()
    valid_prefixes = set()
    for filepath in [NAICS_2017_FILE, NAICS_2022_FILE]:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            code_val = row[1]  # Column B = NAICS code
            if code_val is not None:
                code_str = (str(int(code_val)) if isinstance(code_val, (int, float))
                            else str(code_val).strip())
                if code_str.isdigit() and 2 <= len(code_str) <= 6:
                    valid_prefixes.add(code_str)
                    if len(code_str) == 6:
                        valid_6digit.add(code_str)
        wb.close()
    return valid_6digit, valid_prefixes


def load_bls_rates():
    """Load BLS injury rates into a dict keyed by NAICS code string."""
    rates = {}
    with open(BLS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            naics = str(row["NAICS"]).strip()
            rate_str = str(row["recordables"]).strip()
            if naics and rate_str:
                try:
                    rates[naics] = float(rate_str)
                except ValueError:
                    continue
    return rates


def lookup_bls_rate(naics_code, bls_rates):
    """Find the best BLS rate match: try 6-digit down to 3-digit prefix."""
    for length in [6, 5, 4, 3]:
        prefix = naics_code[:length]
        if prefix in bls_rates:
            return bls_rates[prefix], prefix
    return None, None
