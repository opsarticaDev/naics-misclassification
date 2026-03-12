"""
Gate 2: Incomplete NAICS Code
==============================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_incomplete_naics_code in flagged_output_{YEAR}.csv

Methodology
-----------
Identifies codes that represent a valid NAICS sector or subsector but
are not drilled down to the full 6-digit specificity required.

These are typically zero-padded entries where the filer entered a 2-5
digit code and the submission system padded it to 6 digits:

    "238000"  →  "238" (Specialty Trade Contractors) + "000" padding
    "330000"  →  "33"  (Manufacturing) + "0000" padding
    "480000"  →  "48"  (Transportation) + "0000" padding

This is a different situation from Gate 1 (Invalid NAICS). An incomplete
code means the filer correctly identified their broad industry: they
just didn't (or couldn't) specify the exact 6-digit activity code. This
is imprecise, not wrong.

This flag is INFORMATIONAL: it does not affect priority tier calculation.
The record's sector is correct; it just needs refinement.

Also handles raw short codes (e.g., "23", "238") for portability to
other datasets where the submission system may not enforce 6-digit entry.
In this OSHA 300A dataset, all codes are already 6 digits.

Reference files: same as Gate 1
  - Reference/naics_2017_official.xlsx
  - Reference/naics_2022_official.xlsx
"""

from util_pipeline_config import load_valid_naics_codes


def is_incomplete(naics_code, valid_6digit, valid_prefixes):
    """Return True if the code is a valid NAICS prefix but not a complete
    6-digit code.

    Examples:
        "238000" → True  (prefix "238" is valid, rest is zero-padding)
        "238220" → False (valid 6-digit code)
        "179100" → False (not a valid prefix at any level: that's invalid)
        "23"     → True  (valid 2-digit sector, just short)
    """
    if not naics_code or not naics_code.isdigit():
        return False  # Non-numeric handled by Gate 1

    # Short codes: incomplete if they ARE a valid prefix
    if len(naics_code) < 6:
        return naics_code in valid_prefixes

    if len(naics_code) != 6:
        return False

    # Valid 6-digit code = complete, not incomplete
    if naics_code in valid_6digit:
        return False

    # Check for zero-padded valid prefix
    for prefix_len in [5, 4, 3, 2]:
        prefix = naics_code[:prefix_len]
        suffix = naics_code[prefix_len:]
        if suffix == "0" * len(suffix) and prefix in valid_prefixes:
            return True

    return False  # Not a valid prefix either: that's invalid (Gate 1)


def gate2_process(records, valid_6digit=None, valid_prefixes=None):
    """Add flag_incomplete_naics_code to each record.

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
        flag = is_incomplete(r["naics_code"], valid_6digit, valid_prefixes)
        r["flag_incomplete_naics_code"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged, valid_6digit, valid_prefixes


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    from collections import Counter
    print("Gate 2: Incomplete NAICS Code")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    records, flagged, _, _ = gate2_process(records)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged as incomplete ({100*flagged/len(records):.1f}%)")

    # Show the most common incomplete codes
    incomplete_codes = Counter()
    for r in records:
        if r["flag_incomplete_naics_code"]:
            incomplete_codes[r["naics_code"]] += 1
    print("\n  Most common incomplete codes:")
    for code, count in incomplete_codes.most_common(15):
        print(f"    {code}: {count:,} records")
