"""
Gate 3: Name-NAICS Keyword Mismatch
=====================================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_name_naics_mismatch in flagged_output_{YEAR}.csv

Methodology
-----------
Checks whether the company/establishment name contains strong industry
keywords that contradict the reported NAICS sector.

This catches cases like "Joe's Plumbing" reporting under NAICS 311xxx
(Food Manufacturing) or "Sunrise Daycare" under NAICS 238xxx (Construction).

Design principles:
  1. Keywords use regex word boundaries (\\b) to prevent false positives
     from place names ("Farmington" != "farm") and partial matches
     ("hospitality" != "hospital").

  2. Each keyword maps to ALL sectors where that business type legitimately
     operates. For example, "roofing" maps to construction (23), manufacturing
     (32/33 for shingle makers), and wholesale (42 for distributors). A flag
     fires only when the reported sector is NOT in this valid set.

  3. This gate intentionally errs on the side of NOT flagging. A keyword
     match in a valid sector is presumed correct (e.g., a janitorial company
     with "hospital" in its name operating under sector 56 is legitimate;
     they clean hospitals, they don't run them).

No reference files needed: rules are self-contained.
"""

import re


# Each entry: (regex_pattern, list_of_valid_sectors)
# If the reported 2-digit NAICS sector is NOT in the valid list, flag it.
KEYWORD_RULES = [
    # -----------------------------------------------------------------------
    # Sector lists are aligned with Step 9's _CORRECTION_RULES_RAW to prevent
    # false-positive flags. Gate 3 flags should be a strict subset of Step 9
    # corrections: if Step 9 considers a sector valid for a keyword, Gate 3
    # should not flag it.
    # -----------------------------------------------------------------------

    # Construction trades
    (r"\bplumbing\b", ["23", "42", "81", "56", "72", "32", "33", "44", "22", "49"]),
    (r"\belectrical\s+contractor", ["23"]),
    (r"\broofing\b", ["23", "32", "33", "42", "44", "49"]),
    (r"\bexcavating\b", ["23", "48", "49", "21", "56", "81"]),
    (r"\bdemolition\b", ["23", "56"]),
    (r"\bpaving\b", ["23", "32", "21", "48", "81"]),
    (r"\bconcrete\b", ["23", "32", "42", "44", "56", "21", "48", "72", "53"]),
    (r"\bmasonry\b", ["23", "42", "32", "33", "44", "72"]),
    (r"\bdrywall\b", ["23", "42", "44"]),
    (r"\bcarpentry\b", ["23", "32"]),
    (r"\bhvac\b", ["23", "33", "42", "81", "56", "72", "55"]),
    (r"\bgeneral\s+contractor", ["23"]),

    # Manufacturing
    (r"\bfoundry\b", ["33", "32", "23", "42", "53", "54", "56", "72", "49"]),
    (r"\bmachine\s+shop\b", ["33"]),
    (r"\bstamping\b", ["33"]),
    (r"\bwelding\b", ["33", "23", "81", "42", "72", "21", "54", "61"]),
    (r"\binjection\s+mold", ["33", "32"]),
    (r"\bsemiconductor\b", ["33", "42", "32"]),
    (r"\brefinery\b", ["32", "21", "23", "31", "56", "72", "48", "33"]),
    (r"\bbrewery\b", ["31", "72", "42", "44", "45"]),
    (r"\bbakery\b", ["31", "72", "44", "45", "33", "42", "56", "48", "49", "62"]),
    (r"\bslaughter", ["31"]),
    (r"\bmeat\s*pack", ["31"]),
    (r"\bdistillery\b", ["31", "72"]),
    (r"\bwinery\b", ["31", "72", "44", "45", "42", "62"]),

    # Healthcare
    (r"\bhospital\b", ["62", "72", "56", "54", "81", "92", "23", "42", "44", "45", "48"]),
    (r"\bclinic\b", ["62", "54", "92"]),
    (r"\bnursing\s+home\b", ["62", "56", "72"]),
    (r"\bassisted\s+living\b", ["62", "72"]),
    (r"\bhome\s+health\b", ["62", "44", "45", "53"]),
    (r"\bdental\b", ["62", "33", "42"]),
    (r"\burgent\s+care\b", ["62", "54", "92"]),
    (r"\bhospice\b", ["62", "44", "45", "53", "55"]),
    (r"\bphysical\s+therapy\b", ["62"]),
    (r"\bambulance\b", ["62", "48", "92"]),
    (r"\bdaycare\b", ["62", "92", "56", "72", "81"]),
    (r"\bchild\s+care\b", ["62", "92", "56", "72"]),

    # Accommodation & Food
    (r"\bhotel\b", ["72", "53", "56", "23", "71", "42", "81"]),
    (r"\bmotel\b", ["72", "56"]),
    (r"\bresort\b", ["72", "71", "53", "62", "56", "81", "23", "92", "42"]),
    (r"\brestaurant\b", ["72", "42", "55", "23", "56", "81"]),
    (r"\bpizzeria\b", ["72"]),
    (r"\bcatering\b", ["72", "56"]),

    # Retail & Wholesale
    (r"\bgrocery\b", ["44", "45"]),
    (r"\bsupermarket\b", ["44", "45"]),
    (r"\bgas\s+station\b", ["44", "45"]),
    (r"\bconvenience\s+store\b", ["44", "45"]),
    (r"\bauto\s+dealer", ["44"]),
    (r"\bhardware\s+store\b", ["44"]),

    # Transportation
    (r"\btrucking\b", ["48", "49", "81", "23", "42", "21", "11", "56", "32", "33"]),
    (r"\bfreight\b", ["48", "49", "42", "21", "54", "33", "44", "45", "53", "32"]),
    (r"\bcourier\b", ["49", "48", "42", "56", "32", "92"]),
    (r"\brailroad\b", ["48", "23", "81", "33", "53", "71", "22", "56"]),
    (r"\bairline\b", ["48", "81", "53", "44", "45", "22", "56"]),

    # Agriculture
    (r"\bfeedlot\b", ["11"]),
    (r"\bhatchery\b", ["11", "92", "31"]),
    (r"\bgreenhouse\b", ["11", "44", "33", "53", "54", "62", "56"]),

    # Mining
    (r"\bmining\b", ["21", "81", "33", "42", "23", "32", "72", "22"]),
    (r"\bquarry\b", ["21", "32", "23", "42", "44", "45", "53", "56", "62", "71", "72", "49", "92"]),

    # Utilities
    (r"\bpower\s+plant\b", ["22", "23", "56", "72"]),
    (r"\bwastewater\b", ["22", "92", "23", "56", "72", "33"]),
]

# Pre-compile for performance
_COMPILED_RULES = [(re.compile(pat, re.IGNORECASE), sectors)
                   for pat, sectors in KEYWORD_RULES]


def is_name_mismatch(name, naics_code):
    """Return True if the name contains a keyword whose expected sector
    does not match the reported NAICS sector.
    """
    if not name or not naics_code or len(naics_code) < 2:
        return False
    reported_sector = naics_code[:2]
    for pattern, expected_sectors in _COMPILED_RULES:
        if pattern.search(name):
            if reported_sector not in expected_sectors:
                return True
    return False


def gate3_process(records):
    """Add flag_name_naics_mismatch to each record.

    Returns:
        (records, flagged_count)
    """
    flagged = 0
    for r in records:
        name = r.get("establishment_name", "") or r.get("company_name", "")
        flag = is_name_mismatch(name, r["naics_code"])
        r["flag_name_naics_mismatch"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 3: Name-NAICS Keyword Mismatch")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    records, flagged = gate3_process(records)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged ({100*flagged/len(records):.1f}%)")
    print(f"  {len(KEYWORD_RULES)} keyword rules evaluated")
