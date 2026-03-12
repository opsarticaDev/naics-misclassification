"""
Step 15: Materiality Analysis - BLS DART Rate Comparison
=========================================================
INPUT:  scrape_results_{YEAR}.csv  (post fan-out, ~43K rows)
        Reference/bls_dart_rates_{YEAR}.xlsx  (BLS SOII Table 1)
        Reference/osha_sst_naics.csv  (optional: OSHA SST target list)
OUTPUT: materiality_report_{YEAR}.csv  (all SUGGESTED/UNCERTAIN records + materiality)
        stdout summary for publication headline

Methodology
-----------
For every record with a SUGGESTED or UNCERTAIN scrape_match_status that has
both a reported NAICS code and a suggested NAICS code, this script:

  1. Looks up the BLS DART (Days Away, Restricted, Transferred) rate for both
     the reported and suggested NAICS codes using a cascading lookup:
     6-digit -> 5-digit -> 4-digit -> 3-digit -> 2-digit.

  2. Computes a DART ratio:
       dart_ratio = abs(reported - suggested) / max(reported, suggested)
     A ratio >0.50 means the rates differ by more than 50%.

  3. Checks whether the misclassification crosses a 2-digit NAICS sector
     boundary (e.g., Retail 44-45 -> Healthcare 62).

  4. (Optional) Checks OSHA SST list membership for both codes.

  5. Classifies each record:
       MATERIAL       : DART ratio >50% OR crosses on/off SST list
       IMMATERIAL     : DART ratio ≤25% and same sector
       MODERATE       : everything in between
       INDETERMINATE  : couldn't look up one or both DART rates

The publication headline:
  "X% of 300A records have wrong NAICS codes. Of those, Y% are materially
   wrong: placed in industry categories with significantly different injury rates, directly affecting regulatory benchmarking."


"""

import argparse
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util_pipeline_config import DATASET_YEAR
from util_analysis_config import (load_dart_rates, lookup_dart,
                                  get_sector, sector_name, BLS_DART_FILE)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
SCRAPE_RESULTS = os.path.join(PIPELINE_DIR, f"scrape_results_{DATASET_YEAR}.csv")
SST_FILE = os.path.join(BASE_DIR, "Reference", "osha_sst_naics.csv")
OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"materiality_report_{DATASET_YEAR}.csv")

# Thresholds
MATERIAL_RATIO = 0.50      # DART rates differ by >50%
IMMATERIAL_RATIO = 0.25    # DART rates differ by ≤25%

# Statuses to analyze
ANALYZE_STATUSES = {
    "SUGGESTED", "UNCERTAIN",
    "EDGAR_SUGGESTED", "SAM_SUGGESTED",
}


# ---------------------------------------------------------------------------
# OSHA SST List Loader (optional)
# ---------------------------------------------------------------------------

def load_sst_list(filepath):
    """Load OSHA SST targeted NAICS codes.

    Returns set of NAICS code strings, or empty set if file doesn't exist.
    Expected CSV with at least a 'naics_code' column.
    """
    if not os.path.exists(filepath):
        return set()

    sst = set()
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get("naics_code", "")).strip()
            if code:
                sst.add(code)
    print(f"  Loaded {len(sst)} SST NAICS codes")
    return sst


def check_sst(naics_code, sst_set):
    """Check if a NAICS code or any of its prefixes is on the SST list."""
    if not sst_set:
        return False
    code = str(naics_code).strip()
    for length in (6, 5, 4, 3, 2):
        if code[:length] in sst_set:
            return True
    return False


# ---------------------------------------------------------------------------
# Materiality Classification
# ---------------------------------------------------------------------------

def classify_materiality(reported_dart, suggested_dart,
                         crosses_sector, sst_reported, sst_suggested):
    """Classify a misclassification as MATERIAL, MODERATE, or IMMATERIAL.

    Returns (classification, reason).
    """
    # SST crossing is always material
    if sst_reported != sst_suggested:
        direction = "onto" if sst_suggested and not sst_reported else "off of"
        return "MATERIAL", f"SST list crossing ({direction} targeting list)"

    # DART ratio
    if reported_dart is None or suggested_dart is None:
        return "INDETERMINATE", "DART rate not available for one or both codes"

    max_rate = max(reported_dart, suggested_dart)
    if max_rate == 0:
        if reported_dart == suggested_dart:
            return "IMMATERIAL", "Both DART rates are zero"
        return "INDETERMINATE", "Cannot compute ratio (zero denominator)"

    ratio = abs(reported_dart - suggested_dart) / max_rate

    if ratio > MATERIAL_RATIO:
        direction = "higher" if suggested_dart > reported_dart else "lower"
        return "MATERIAL", (
            f"DART ratio {ratio:.0%} - suggested rate is {direction} "
            f"({reported_dart:.1f} -> {suggested_dart:.1f})"
        )

    if ratio <= IMMATERIAL_RATIO and not crosses_sector:
        return "IMMATERIAL", (
            f"DART ratio {ratio:.0%}, same sector "
            f"({reported_dart:.1f} -> {suggested_dart:.1f})"
        )

    return "MODERATE", (
        f"DART ratio {ratio:.0%}"
        f"{', crosses sector' if crosses_sector else ''} "
        f"({reported_dart:.1f} -> {suggested_dart:.1f})"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(args):
    print("Step 15: Materiality Analysis")
    print("=" * 60)

    # Load reference data
    print("\nLoading reference data:")
    dart_rates = load_dart_rates(BLS_DART_FILE)
    print(f"  Loaded {len(dart_rates)} DART rates from {os.path.basename(BLS_DART_FILE)}")
    sst_set = load_sst_list(SST_FILE)
    if not sst_set:
        print("  SST file not found: skipping SST layer")

    # Load scrape results
    print(f"\nLoading scrape results: {SCRAPE_RESULTS}")
    with open(SCRAPE_RESULTS, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_fields = list(reader.fieldnames)
        all_rows = list(reader)
    print(f"  {len(all_rows):,} total rows")

    # Filter to analyzable records
    analyze_rows = [
        r for r in all_rows
        if r.get("scrape_match_status") in ANALYZE_STATUSES
        and r.get("naics_code", "").strip()
        and r.get("suggested_naics", "").strip()
    ]
    print(f"  {len(analyze_rows):,} records to analyze "
          f"(SUGGESTED/UNCERTAIN with both codes)")

    # Analyze
    results = []
    stats = Counter()
    sector_crossings = Counter()

    for row in analyze_rows:
        reported = row["naics_code"].strip()
        suggested = row["suggested_naics"].strip()

        # DART lookups
        r_dart, r_match = lookup_dart(reported, dart_rates)
        s_dart, s_match = lookup_dart(suggested, dart_rates)

        # Sector crossing
        r_sector = get_sector(reported)
        s_sector = get_sector(suggested)
        crosses = r_sector != s_sector

        # SST check
        sst_r = check_sst(reported, sst_set)
        sst_s = check_sst(suggested, sst_set)

        # Classify
        classification, reason = classify_materiality(
            r_dart, s_dart, crosses, sst_r, sst_s
        )
        stats[classification] += 1

        if crosses:
            key = f"{r_sector}->{s_sector}"
            sector_crossings[key] += 1

        # Build output row
        out = dict(row)
        out["reported_dart_rate"] = f"{r_dart:.2f}" if r_dart is not None else ""
        out["reported_dart_match"] = r_match or ""
        out["suggested_dart_rate"] = f"{s_dart:.2f}" if s_dart is not None else ""
        out["suggested_dart_match"] = s_match or ""
        out["dart_ratio"] = ""
        if r_dart is not None and s_dart is not None and max(r_dart, s_dart) > 0:
            out["dart_ratio"] = f"{abs(r_dart - s_dart) / max(r_dart, s_dart):.3f}"
        out["crosses_sector"] = "Y" if crosses else "N"
        out["reported_sector"] = f"{r_sector} ({sector_name(r_sector)})"
        out["suggested_sector"] = f"{s_sector} ({sector_name(s_sector)})"
        out["on_sst_reported"] = "Y" if sst_r else "N"
        out["on_sst_suggested"] = "Y" if sst_s else "N"
        out["materiality_class"] = classification
        out["materiality_reason"] = reason
        results.append(out)

    # Summary
    total = len(results)
    print(f"\n{'=' * 60}")
    print(f"MATERIALITY SUMMARY")
    print(f"{'=' * 60}")
    for cls in ("MATERIAL", "MODERATE", "IMMATERIAL", "INDETERMINATE"):
        n = stats.get(cls, 0)
        pct = n / total * 100 if total else 0
        print(f"  {cls:15s}  {n:>7,}  ({pct:5.1f}%)")
    print(f"  {'TOTAL':15s}  {total:>7,}")

    # Sector crossings
    crosses_total = sum(1 for r in results if r["crosses_sector"] == "Y")
    print(f"\n  Cross-sector misclassifications: {crosses_total:,} "
          f"({crosses_total / total * 100:.1f}%)" if total else "")

    if sector_crossings:
        print(f"\n  Top sector crossings:")
        for key, n in sector_crossings.most_common(10):
            r2, s2 = key.split("->")
            print(f"    {sector_name(r2):20s} -> {sector_name(s2):20s}  {n:>5,}")

    # Publication headline
    print(f"\n{'=' * 60}")
    print(f"PUBLICATION HEADLINE")
    print(f"{'=' * 60}")

    # Full project numbers
    total_records = 394_234  # from resolution report
    wrong_pct = (total - stats.get("INDETERMINATE", 0)) / total_records * 100
    material_n = stats.get("MATERIAL", 0)
    material_pct = material_n / total * 100 if total else 0
    moderate_n = stats.get("MODERATE", 0)

    print(f"\n  Of {total_records:,} OSHA 300A records analyzed:")
    print(f"  - {total:,} have potentially incorrect NAICS codes ({total/total_records*100:.1f}%)")
    print(f"  - {material_n:,} are MATERIALLY wrong ({material_pct:.1f}% of misclassified)")
    print(f"    -> placed in industries with significantly different injury rates")
    print(f"  - {moderate_n:,} are MODERATELY wrong ({moderate_n/total*100:.1f}% of misclassified)")
    print(f"  - DART rate source: BLS SOII Table 1, Survey Year {DATASET_YEAR}")

    # DART rate impact distribution
    ratios = []
    for r in results:
        if r["dart_ratio"]:
            try:
                ratios.append(float(r["dart_ratio"]))
            except ValueError:
                pass
    if ratios:
        ratios.sort()
        print(f"\n  DART Ratio Distribution (n={len(ratios):,}):")
        import statistics
        print(f"    Median:  {statistics.median(ratios):.1%}")
        print(f"    Mean:    {statistics.mean(ratios):.1%}")
        print(f"    P25:     {ratios[len(ratios)//4]:.1%}")
        print(f"    P75:     {ratios[3*len(ratios)//4]:.1%}")
        print(f"    P90:     {ratios[int(len(ratios)*0.9)]:.1%}")

    if args.dry_run:
        print(f"\nDRY RUN: no files written")
        return

    # Write output
    out_fields = input_fields + [
        "reported_dart_rate", "reported_dart_match",
        "suggested_dart_rate", "suggested_dart_match",
        "dart_ratio", "crosses_sector",
        "reported_sector", "suggested_sector",
        "on_sst_reported", "on_sst_suggested",
        "materiality_class", "materiality_reason",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWritten: {OUTPUT_FILE}")
    print(f"Total rows: {len(results):,}")


def main():
    parser = argparse.ArgumentParser(
        description="Materiality analysis: BLS DART rates + OSHA SST crossings"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview analysis without writing output")
    parser.add_argument("--apply", action="store_true",
                        help="Write materiality_report.csv")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
