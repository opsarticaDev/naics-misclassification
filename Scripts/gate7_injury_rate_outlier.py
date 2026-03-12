"""
Gate 7: Injury Rate Outlier
=============================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_injury_rate_outlier + computed trir/bls columns in flagged_output_{YEAR}.csv

Methodology
-----------
Flags records where the computed Total Recordable Incident Rate (TRIR)
exceeds the Bureau of Labor Statistics (BLS) benchmark rate by more than
INJURY_RATE_TOLERANCE (default: 2x).

    TRIR = (total_injuries / total_hours_worked) * 200,000

The BLS table provides benchmark injury rates at 3-6 digit NAICS levels.
We match each record to the most specific BLS rate available (6 → 5 → 4
→ 3 digit prefix). If no match exists, the record is not flagged.

Only HIGH rates are flagged (not low). A low injury rate could indicate
a good safety program. Zero-injury records are handled by Gate 8.

This gate also computes and stores the TRIR, BLS benchmark, and matched
NAICS prefix as additional columns for downstream analysis.

Extreme outliers are often data entry errors (e.g., an HVAC company that
reported 17 trillion hours worked). These are worth investigating but
represent reporting mistakes, not classification errors.

Records with total_hours_worked <= 0 are excluded.

Reference file:
  - Reference/BLS_Table1_Injury.csv
"""

from util_pipeline_config import load_bls_rates, lookup_bls_rate, INJURY_RATE_TOLERANCE


def gate7_process(records, bls_rates=None):
    """Add flag_injury_rate_outlier, trir, bls_benchmark, bls_matched_naics
    to each record.

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
        try:
            injuries = int(r.get("total_injuries", ""))
            hours = int(r.get("total_hours_worked", ""))
        except (ValueError, TypeError):
            r["flag_injury_rate_outlier"] = 0
            r["trir"] = ""
            r["bls_benchmark"] = ""
            r["bls_matched_naics"] = ""
            continue

        if hours <= 0:
            r["flag_injury_rate_outlier"] = 0
            r["trir"] = ""
            r["bls_benchmark"] = ""
            r["bls_matched_naics"] = ""
            continue

        trir = (injuries / hours) * 200000
        benchmark, matched_naics = lookup_bls_rate(r["naics_code"], bls_rates)

        if benchmark is None or benchmark <= 0:
            flag = False
        else:
            flag = trir > (benchmark * INJURY_RATE_TOLERANCE)

        r["flag_injury_rate_outlier"] = 1 if flag else 0
        r["trir"] = f"{trir:.2f}"
        r["bls_benchmark"] = f"{benchmark:.2f}" if benchmark is not None else ""
        r["bls_matched_naics"] = matched_naics or ""

        if flag:
            flagged += 1

    return records, flagged, bls_rates


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 7: Injury Rate Outlier")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    records, flagged, bls_rates = gate7_process(records)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged ({100*flagged/len(records):.1f}%)")
    print(f"  {len(bls_rates):,} BLS benchmark rates loaded")
