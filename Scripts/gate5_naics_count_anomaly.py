"""
Gate 5: NAICS Count Anomaly
=============================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_naics_count_anomaly in flagged_output_{YEAR}.csv

Methodology
-----------
Flags records whose 6-digit NAICS code is extremely rare in the dataset.

A NAICS code used by only 1-2 establishments in the national dataset is
unusual. It may indicate:
  - A typo (one digit off from a common code)
  - An obsolete or retired code
  - A misunderstanding of the classification system
  - A legitimate but very niche industry

This gate uses a simple frequency threshold: if a code appears fewer than
NAICS_RARITY_OSHA_FLOOR times (default: 3) in the entire OSHA dataset,
all records with that code are flagged.

This is a blunt instrument: some rare codes are legitimate (e.g., uranium
ore mining). But combined with other gates, rarity is a useful signal that
the code deserves a second look.

No reference files needed: this gate uses only the dataset itself.
"""

from collections import Counter
from util_pipeline_config import NAICS_RARITY_OSHA_FLOOR


def build_naics_counts(records):
    """Count occurrences of each 6-digit NAICS code in the dataset."""
    counts = Counter()
    for r in records:
        counts[r["naics_code"]] += 1
    return counts


def gate5_process(records):
    """Add flag_naics_count_anomaly to each record.

    Returns:
        (records, flagged_count)
    """
    naics_counts = build_naics_counts(records)

    flagged = 0
    for r in records:
        flag = naics_counts.get(r["naics_code"], 0) < NAICS_RARITY_OSHA_FLOOR
        r["flag_naics_count_anomaly"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 5: NAICS Count Anomaly")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    naics_counts = build_naics_counts(records)
    records, flagged = gate5_process(records)
    rare = sum(1 for c in naics_counts.values() if c < NAICS_RARITY_OSHA_FLOOR)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged ({100*flagged/len(records):.1f}%)")
    print(f"  {len(naics_counts):,} distinct NAICS codes in dataset")
    print(f"  {rare:,} codes appear fewer than {NAICS_RARITY_OSHA_FLOOR} times")
