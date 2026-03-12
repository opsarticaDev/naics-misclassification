"""
Gate 8: High-Employee Zero-Injury
===================================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_high_emp_zero_injury in flagged_output_{YEAR}.csv

Methodology
-----------
Flags establishments with 50+ employees that report zero total injuries
AND operate in an industry where the BLS benchmark rate is >= 2.0 per
100 full-time equivalent workers.

This is the least aggressive gate in the pipeline. It is entirely
feasible for a large company to have zero injuries in a given year,
especially with strong safety programs. This gate only flags the
combination of:
  - Large workforce (HIGH_EMP_THRESHOLD = 50)
  - Zero injuries
  - High-risk industry (BLS benchmark >= HIGH_RISK_BLS_FLOOR = 2.0)

A 50-person construction crew or meatpacking plant reporting zero
injuries in a year is statistically uncommon and warrants review,
but it is NOT necessarily wrong.

Office-based industries (tech, finance, professional services) are
excluded because zero injuries is entirely normal there: their BLS
rates are well below the 2.0 threshold.

Reference file:
  - Reference/BLS_Table1_Injury.csv (same as Gate 7)
"""

from util_pipeline_config import (load_bls_rates, lookup_bls_rate,
                                  HIGH_EMP_THRESHOLD, HIGH_RISK_BLS_FLOOR)


def is_high_emp_zero_injury(emp_str, injuries_str, naics_code, bls_rates):
    """Return True if a large establishment reports zero injuries in a
    high-risk industry.
    """
    try:
        emp = int(emp_str)
        injuries = int(injuries_str)
    except (ValueError, TypeError):
        return False
    if emp < HIGH_EMP_THRESHOLD or injuries > 0:
        return False
    benchmark, _ = lookup_bls_rate(naics_code, bls_rates)
    if benchmark is None:
        return False
    return benchmark >= HIGH_RISK_BLS_FLOOR


def gate8_process(records, bls_rates=None):
    """Add flag_high_emp_zero_injury to each record.

    Args:
        records: list of dicts (modified in place)
        bls_rates: pre-loaded dict (optional, loaded if not provided)

    Returns:
        (records, flagged_count, bls_rates)
    """
    if bls_rates is None:
        bls_rates = load_bls_rates()

    flagged = 0
    for r in records:
        flag = is_high_emp_zero_injury(
            r.get("annual_average_employees", ""),
            r.get("total_injuries", ""),
            r["naics_code"],
            bls_rates
        )
        r["flag_high_emp_zero_injury"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged, bls_rates


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 8: High-Employee Zero-Injury")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    records, flagged, _ = gate8_process(records)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged ({100*flagged/len(records):.1f}%)")
