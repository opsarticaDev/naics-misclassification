"""
Report: Resolution Summary for OSHA NAICS Validation
=====================================================
INPUT:  scrape_results_{YEAR}.csv (post fan-out, ~43,969 rows)
        classified_output_{YEAR}.csv (full 394K records with classifier results)
OUTPUT: stdout (resolution counts for Telegram/logging)

Produces a concise resolution summary of all 394,234 OSHA records,
combining the classifier results with the web/EDGAR/SAM enrichment
results from the scrape pipeline.

"""

import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util_pipeline_config import DATASET_YEAR

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
CLASSIFIED_FILE = os.path.join(PIPELINE_DIR, f"classified_output_{DATASET_YEAR}.csv")
SCRAPE_FILE = os.path.join(PIPELINE_DIR, f"scrape_results_{DATASET_YEAR}.csv")


def main():
    # Load scrape results into lookup by record ID
    scrape_lookup = {}
    if os.path.exists(SCRAPE_FILE):
        with open(SCRAPE_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rid = row.get("id", "")
                if rid:
                    scrape_lookup[rid] = row.get("scrape_match_status", "")

    # Process classified output (all 394K records)
    resolution = Counter()
    total = 0

    with open(CLASSIFIED_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += 1
            rid = row.get("id", "")
            match_status = row.get("match_status", "")
            n_flags = int(row.get("n_flags", 0))

            if match_status == "CORRECTED":
                resolution["CORRECTED (keyword rule)"] += 1
            elif n_flags == 0:
                resolution["CLEAN (0 flags)"] += 1
            elif n_flags == 1 and match_status == "CONFIRMED":
                resolution["CONFIRMED (1 flag, classifier OK)"] += 1
            else:
                # This record was in the uncertain set: check scrape results
                scrape_status = scrape_lookup.get(rid, "")
                if scrape_status in ("CONFIRMED", "EDGAR_CONFIRMED", "SAM_CONFIRMED"):
                    resolution["CONFIRMED (web/EDGAR/SAM)"] += 1
                elif scrape_status in ("SUGGESTED", "EDGAR_SUGGESTED", "SAM_SUGGESTED"):
                    resolution["SUGGESTED (needs review)"] += 1
                elif scrape_status in ("UNCERTAIN",):
                    resolution["UNCERTAIN (inconclusive)"] += 1
                elif scrape_status in ("SCRAPE_FAILED", "NO_WEBSITE"):
                    resolution["UNRESOLVED (no data)"] += 1
                elif scrape_status:
                    resolution[f"OTHER ({scrape_status})"] += 1
                else:
                    resolution["UNCERTAIN (not in scrape results)"] += 1

    # Print report
    print("=" * 55)
    print(f"  NAICS VALIDATION -- {DATASET_YEAR} RESOLUTION SUMMARY")
    print("=" * 55)

    confident = 0
    suggested = 0
    needs_work = 0

    for label in [
        "CLEAN (0 flags)",
        "CONFIRMED (1 flag, classifier OK)",
        "CORRECTED (keyword rule)",
        "CONFIRMED (web/EDGAR/SAM)",
        "SUGGESTED (needs review)",
        "UNCERTAIN (inconclusive)",
        "UNCERTAIN (not in scrape results)",
        "UNRESOLVED (no data)",
    ]:
        count = resolution.get(label, 0)
        pct = 100 * count / total if total else 0
        print(f"  {label:<40s} {count:>8,}  ({pct:>5.1f}%)")

        if "CLEAN" in label or "CONFIRMED" in label or "CORRECTED" in label:
            confident += count
        elif "SUGGESTED" in label:
            suggested += count
        else:
            needs_work += count

    # Any OTHER statuses
    for label, count in sorted(resolution.items()):
        if label.startswith("OTHER"):
            pct = 100 * count / total if total else 0
            print(f"  {label:<40s} {count:>8,}  ({pct:>5.1f}%)")
            needs_work += count

    print(f"  {'':40s} {'-------':>8s}")
    print(f"  {'TOTAL':<40s} {total:>8,}")

    print()
    print(f"  Confident:  {confident:>8,}  ({100*confident/total:.1f}%)")
    print(f"  Suggested:  {suggested:>8,}  ({100*suggested/total:.1f}%)")
    print(f"  Needs work: {needs_work:>8,}  ({100*needs_work/total:.1f}%)")
    print("=" * 55)


if __name__ == "__main__":
    main()
