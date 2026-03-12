"""
Gate 4: EIN Multi-NAICS
========================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_ein_multi_naics in flagged_output_{YEAR}.csv

Methodology
-----------
Flags records where the same Employer Identification Number (EIN) has
filed under more than one distinct 6-digit NAICS code across filings.

A single legal entity (one EIN) operating establishments in truly
different industries is uncommon. More often, multiple NAICS codes for
the same EIN indicate that one or more filings have the wrong code.

When this gate fires, ALL records for that EIN are flagged: not just
the minority code. This is because we cannot determine from the data
alone which code is correct; that requires external verification.

No reference files needed: this gate uses only the dataset itself.
"""

from collections import defaultdict


def build_ein_index(records):
    """Pre-scan: find all EINs that have multiple NAICS codes.

    Returns a set of EINs where len(distinct_naics_codes) > 1.
    """
    ein_naics = defaultdict(set)
    for r in records:
        ein = r.get("ein", "").strip()
        if ein:
            ein_naics[ein].add(r["naics_code"])
    return {ein for ein, codes in ein_naics.items() if len(codes) > 1}


def gate4_process(records):
    """Add flag_ein_multi_naics to each record.

    Returns:
        (records, flagged_count)
    """
    multi_naics_eins = build_ein_index(records)

    flagged = 0
    for r in records:
        ein = r.get("ein", "").strip()
        flag = bool(ein and ein in multi_naics_eins)
        r["flag_ein_multi_naics"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 4: EIN Multi-NAICS")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    records, flagged = gate4_process(records)
    multi_eins = build_ein_index(records)
    no_ein = sum(1 for r in records if not r.get("ein", "").strip())
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged ({100*flagged/len(records):.1f}%)")
    print(f"  {len(multi_eins):,} EINs with multiple NAICS codes")
    print(f"  {no_ein:,} records have no EIN (not flagged)")
