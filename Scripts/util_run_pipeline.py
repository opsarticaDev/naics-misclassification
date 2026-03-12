"""
OSHA NAICS Validation Pipeline: Runner
=========================================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flagged_output_{YEAR}.csv (all records with gate flags + priority tiers)

Orchestrates the 8 gate scripts in sequence, from most egregious
classification errors to least certain anomalies:

    Gate 1: Invalid NAICS Code         - code doesn't exist in NAICS system
    Gate 2: Incomplete NAICS Code      - valid prefix, not drilled to 6 digits
    Gate 3: Name-NAICS Mismatch        - company name contradicts sector
    Gate 4: EIN Multi-NAICS            - same employer, different codes
    Gate 5: NAICS Count Anomaly        - extremely rare code in dataset
    Gate 6: Employee Size Anomaly      - workforce size is extreme outlier
    Gate 7: Injury Rate Outlier        - TRIR exceeds 2x BLS benchmark
    Gate 8: High-Employee Zero-Injury  - large high-risk firm, zero injuries

Each gate is in its own script for auditability. Point to any gate file
to show exactly what was checked and how.

Priority tiers are computed after all gates run:
    Tier 1: 2+ quality flags (gates 1, 3, 4, 5 - code is likely wrong)
    Tier 2: 1 quality flag or employee size anomaly
    Tier 3: Injury/reporting anomaly only (gates 7, 8)
    Tier 4: Clean - no flags

Usage:
    python util_run_pipeline.py
"""

import logging
from collections import Counter
from datetime import datetime

from util_pipeline_config import (
    INPUT_FILE, OUTPUT_FILE, LOG_FILE,
    load_records, write_records,
    load_valid_naics_codes, load_bls_rates,
)

from gate1_invalid_naics import gate1_process
from gate2_incomplete_naics import gate2_process
from gate3_name_naics_mismatch import gate3_process
from gate4_ein_multi_naics import gate4_process
from gate5_naics_count_anomaly import gate5_process
from gate6_emp_size_anomaly import gate6_process
from gate7_injury_rate_outlier import gate7_process
from gate8_high_emp_zero_injury import gate8_process


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output column order
# ---------------------------------------------------------------------------
OUTPUT_FIELDS = [
    # Record identifiers
    "id", "establishment_name", "company_name",
    "street_address", "city", "state", "zip_code",
    "naics_code", "industry_description",
    "annual_average_employees", "total_hours_worked", "total_injuries",
    "ein",
    # Gate flags: in gate order (most egregious first)
    "flag_invalid_naics_code",        # Gate 1
    "flag_incomplete_naics_code",     # Gate 2
    "flag_name_naics_mismatch",       # Gate 3
    "flag_ein_multi_naics",           # Gate 4
    "flag_naics_count_anomaly",       # Gate 5
    "flag_emp_size_anomaly",          # Gate 6
    "flag_injury_rate_outlier",       # Gate 7
    "flag_high_emp_zero_injury",      # Gate 8
    # Gate 7 computed fields
    "trir", "bls_benchmark", "bls_matched_naics",
    # Aggregates
    "n_flags", "priority_tier",
]


def compute_tiers(records):
    """Compute n_flags and priority_tier for each record after all gates.

    Quality flags (suggest the NAICS code itself is wrong):
        - Gate 1: invalid_naics_code
        - Gate 3: name_naics_mismatch
        - Gate 4: ein_multi_naics
        - Gate 5: naics_count_anomaly

    Gate 2 (incomplete) is NOT a quality flag: the sector is correct,
    just imprecise.

    Tier logic:
        Tier 1: 2+ quality flags - code is almost certainly wrong
        Tier 2: 1 quality flag, or employee size anomaly
        Tier 3: Only injury/reporting anomalies (gates 7, 8)
        Tier 4: Clean - no flags at all
    """
    for r in records:
        f1 = r["flag_invalid_naics_code"]
        f2 = r["flag_incomplete_naics_code"]
        f3 = r["flag_name_naics_mismatch"]
        f4 = r["flag_ein_multi_naics"]
        f5 = r["flag_naics_count_anomaly"]
        f6 = r["flag_emp_size_anomaly"]
        f7 = r["flag_injury_rate_outlier"]
        f8 = r["flag_high_emp_zero_injury"]

        flags = [f1, f2, f3, f4, f5, f6, f7, f8]
        r["n_flags"] = sum(flags)

        quality_flags = sum([f1, f3, f4, f5])
        if quality_flags >= 2:
            r["priority_tier"] = 1
        elif quality_flags == 1 or f6:
            r["priority_tier"] = 2
        elif f7 or f8:
            r["priority_tier"] = 3
        else:
            r["priority_tier"] = 4


def run_pipeline():
    start = datetime.now()
    log.info("=" * 60)
    log.info("OSHA NAICS VALIDATION PIPELINE")
    log.info("=" * 60)

    # ------------------------------------------------------------------
    # Load data and shared references
    # ------------------------------------------------------------------
    log.info(f"Loading records from {INPUT_FILE}")
    records = load_records(INPUT_FILE)
    log.info(f"  {len(records):,} records loaded")

    log.info("Loading shared reference data...")
    valid_6digit, valid_prefixes = load_valid_naics_codes()
    log.info(f"  NAICS reference: {len(valid_6digit):,} six-digit codes, "
             f"{len(valid_prefixes):,} codes at all levels")
    bls_rates = load_bls_rates()
    log.info(f"  BLS benchmarks: {len(bls_rates):,} rates loaded")

    # ------------------------------------------------------------------
    # Run gates in order
    # ------------------------------------------------------------------
    gate_results = {}

    log.info("")
    log.info("--- Gate 1: Invalid NAICS Code ---")
    records, count, _, _ = gate1_process(records, valid_6digit, valid_prefixes)
    gate_results["invalid_naics_code"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 2: Incomplete NAICS Code ---")
    records, count, _, _ = gate2_process(records, valid_6digit, valid_prefixes)
    gate_results["incomplete_naics_code"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 3: Name-NAICS Mismatch ---")
    records, count = gate3_process(records)
    gate_results["name_naics_mismatch"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 4: EIN Multi-NAICS ---")
    records, count = gate4_process(records)
    gate_results["ein_multi_naics"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 5: NAICS Count Anomaly ---")
    records, count = gate5_process(records)
    gate_results["naics_count_anomaly"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 6: Employee Size Anomaly ---")
    records, count = gate6_process(records)
    gate_results["emp_size_anomaly"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 7: Injury Rate Outlier ---")
    records, count, _ = gate7_process(records, bls_rates)
    gate_results["injury_rate_outlier"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    log.info("--- Gate 8: High-Employee Zero-Injury ---")
    records, count, _ = gate8_process(records, bls_rates)
    gate_results["high_emp_zero_injury"] = count
    log.info(f"  {count:,} records flagged ({100*count/len(records):.1f}%)")

    # ------------------------------------------------------------------
    # Compute tiers
    # ------------------------------------------------------------------
    log.info("")
    log.info("Computing priority tiers...")
    compute_tiers(records)

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------
    log.info(f"Writing output to {OUTPUT_FILE}")
    write_records(records, OUTPUT_FILE, OUTPUT_FIELDS)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE RESULTS SUMMARY")
    log.info("=" * 60)

    for gate_name, count in gate_results.items():
        pct = 100 * count / len(records)
        log.info(f"  {gate_name}: {count:,} ({pct:.1f}%)")

    log.info("")
    flag_dist = Counter(r["n_flags"] for r in records)
    log.info("RECORDS BY FLAG COUNT:")
    for n in range(max(flag_dist.keys()) + 1):
        log.info(f"  {n} flags: {flag_dist.get(n, 0):,}")

    log.info("")
    tier_dist = Counter(r["priority_tier"] for r in records)
    log.info("RECORDS BY PRIORITY TIER:")
    for t in [1, 2, 3, 4]:
        log.info(f"  Tier {t}: {tier_dist.get(t, 0):,}")

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"\nPipeline completed in {elapsed:.1f} seconds")
    log.info(f"Output: {OUTPUT_FILE}")
    log.info(f"Log: {LOG_FILE}")

    return records


if __name__ == "__main__":
    run_pipeline()
