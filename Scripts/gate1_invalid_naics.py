"""
Gate 1: Invalid NAICS Code
==========================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_invalid_naics_code in flagged_output_{YEAR}.csv

Methodology
-----------
The most fundamental check: does the reported 6-digit NAICS code actually
exist in the official Census Bureau classification system?

This gate loads the complete NAICS structure files for both the 2017 and
2022 revisions. A code is accepted as valid if it appears in EITHER
revision, since OSHA 300A data spans multiple filing years.

A code is flagged as INVALID only if it cannot be matched to ANY level
(2-6 digit) of either revision. This catches:

  - SIC codes (pre-NAICS system, e.g., 179100, 283220)
  - Placeholders (999999, 999301)
  - Typos and digit transpositions
  - Completely fabricated codes

Codes that are valid NAICS prefixes but zero-padded (e.g., "238000" where
"238" = Specialty Trade Contractors) are NOT flagged here: those are
handled separately by Gate 2 (Incomplete NAICS Code). The distinction
matters: a zero-padded code means the filer knew their industry sector
but didn't drill down to 6 digits. A truly invalid code means the number
has no meaning in the NAICS system at all.

Reference files:
  - Reference/naics_2017_official.xlsx
  - Reference/naics_2022_official.xlsx
"""

from util_pipeline_config import load_valid_naics_codes


def is_invalid(naics_code, valid_6digit, valid_prefixes):
    """Return True if the code is genuinely invalid (not just incomplete).

    A code is invalid if:
      - It is not a recognized 6-digit NAICS code, AND
      - It is not a zero-padded form of a valid shorter prefix.

    A code like "238000" returns False here because "238" is a valid
    NAICS subsector: that record goes to Gate 2 instead.
    """
    if not naics_code or not naics_code.isdigit():
        return True

    # Short codes: invalid only if not a recognized prefix
    if len(naics_code) < 6:
        return naics_code not in valid_prefixes

    if len(naics_code) != 6:
        return True  # Longer than 6 digits

    # Exact 6-digit match
    if naics_code in valid_6digit:
        return False

    # Check if it's a zero-padded valid prefix (that's incomplete, not invalid)
    for prefix_len in [5, 4, 3, 2]:
        prefix = naics_code[:prefix_len]
        suffix = naics_code[prefix_len:]
        if suffix == "0" * len(suffix) and prefix in valid_prefixes:
            return False  # Incomplete, handled by Gate 2

    return True


def gate1_process(records, valid_6digit=None, valid_prefixes=None):
    """Add flag_invalid_naics_code to each record.

    Args:
        records: list of dicts (modified in place)
        valid_6digit: pre-loaded set (optional, loaded if not provided)
        valid_prefixes: pre-loaded set (optional, loaded if not provided)

    Returns:
        (records, flagged_count, valid_6digit, valid_prefixes)
    """
    if valid_6digit is None or valid_prefixes is None:
        valid_6digit, valid_prefixes = load_valid_naics_codes()

    flagged = 0
    for r in records:
        flag = is_invalid(r["naics_code"], valid_6digit, valid_prefixes)
        r["flag_invalid_naics_code"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged, valid_6digit, valid_prefixes


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 1: Invalid NAICS Code")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    records, flagged, v6, vp = gate1_process(records)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged as invalid ({100*flagged/len(records):.1f}%)")
    print(f"  {len(v6):,} valid 6-digit codes in reference")
    print(f"  {len(vp):,} valid codes at all levels (2-6 digit)")
