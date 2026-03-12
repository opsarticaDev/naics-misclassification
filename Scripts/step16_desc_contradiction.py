"""
Step 16: Industry Description Contradiction Analysis
=====================================================
INPUT:  scrape_results_{YEAR}.csv  (post fan-out, ~43K rows)
        Reference/naics_2017_official.xlsx  (US NAICS 2017 descriptions)
        Reference/naics_2022_official.xlsx  (US NAICS 2022 descriptions, fills gaps)
        Reference/bls_dart_rates_{YEAR}.xlsx  (BLS SOII Table 1, for DART enrichment)
        flagged_output_{YEAR}.csv  (pipeline flags for cross-reference)
OUTPUT: desc_contradictions_{YEAR}.csv  (records where own description
        contradicts reported NAICS and supports suggested NAICS)
        stdout summary for publication

Methodology
-----------
For every record with a suggested NAICS different from reported, this script:

  1. Builds a "description tree" for both the reported and suggested NAICS codes
     by collecting the official NAICS name at every level (6, 5, 4, 3, 2 digit).
     Combined sector codes (31-33, 44-45, 48-49) are expanded to individual codes.
     Example: 423450 -> ["Construction and Mining Machinery Equipment Wholesalers",
                          "Machinery, Equipment, and Supplies Wholesalers",
                          "Merchant Wholesalers, Durable Goods",
                          "Wholesale Trade"]

  2. Tokenizes the establishment's self-reported industry_description into
     a set of meaningful words (3+ chars, lowercased, stopwords removed,
     basic suffix stemming for vocabulary alignment, plus domain synonym
     expansion via SYNONYM_BRIDGE: e.g., "airport" injects "air",
     "transportation", "aviation" so it can match NAICS 481 tree).

  3. Tokenizes each NAICS description tree into words (same processing).

  4. Computes a match score: |desc_tokens & naics_tokens| / |desc_tokens|
     (what fraction of the establishment's own words appear in the NAICS tree)

  5. Flags records where:
     - suggested_score > reported_score (description fits suggested better)
     - AND reported_score == 0 or suggested_score >= 1.5x reported_score
       (the gap is meaningful, not noise)

These are DEFENSIBLE findings: the establishment's OWN industry description
contradicts the NAICS code they reported, and a different code fits better.

"""

import argparse
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util_pipeline_config import DATASET_YEAR
from util_analysis_config import (load_naics_descriptions, build_desc_tree,
                                  load_dart_rates, lookup_dart, tokenize,
                                  get_sector, sectors_match, sector_name,
                                  NAICS_2017_FILE, NAICS_2022_FILE, BLS_DART_FILE)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
SCRAPE_RESULTS = os.path.join(PIPELINE_DIR, f"scrape_results_{DATASET_YEAR}.csv")
FLAGGED_FILE = os.path.join(PIPELINE_DIR, f"flagged_output_{DATASET_YEAR}.csv")
OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"desc_contradictions_{DATASET_YEAR}.csv")

# Statuses worth analyzing (have both reported and suggested NAICS)
ANALYZE_STATUSES = {
    "SUGGESTED", "UNCERTAIN",
    "EDGAR_SUGGESTED", "SAM_SUGGESTED",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(args):
    print("Step 16: Industry Description Contradiction Analysis")
    print("=" * 60)

    # Load reference data
    print("\nLoading reference data:")
    naics_descs = load_naics_descriptions(NAICS_2017_FILE, NAICS_2022_FILE)
    print(f"  Loaded {len(naics_descs)} NAICS descriptions")
    dart_rates = load_dart_rates(BLS_DART_FILE)
    if dart_rates:
        print(f"  Loaded {len(dart_rates)} DART rates")

    # Load pipeline flags
    pipeline_flags = {}
    if os.path.exists(FLAGGED_FILE):
        with open(FLAGGED_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pipeline_flags[row.get("id", "")] = int(row.get("n_flags", "0") or "0")
        print(f"  Loaded pipeline flags for {len(pipeline_flags):,} records")

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
        and r.get("naics_code", "").strip() != r.get("suggested_naics", "").strip()
        and r.get("industry_description", "").strip()
    ]
    print(f"  {len(analyze_rows):,} records to analyze "
          f"(different codes + has description)")

    # Analyze
    results = []
    stats = Counter()
    both_zero = 0

    for row in analyze_rows:
        reported = row["naics_code"].strip()
        suggested = row["suggested_naics"].strip()
        desc = row["industry_description"].strip()

        # Tokenize
        desc_tokens = tokenize(desc)
        if not desc_tokens:
            stats["empty_tokens"] += 1
            continue

        reported_tree = build_desc_tree(reported, naics_descs)
        suggested_tree = build_desc_tree(suggested, naics_descs)

        reported_tokens = tokenize(reported_tree)
        suggested_tokens = tokenize(suggested_tree)

        # Score: fraction of desc tokens found in NAICS tree
        if not reported_tokens and not suggested_tokens:
            stats["no_naics_tokens"] += 1
            continue

        r_overlap = desc_tokens & reported_tokens
        s_overlap = desc_tokens & suggested_tokens

        r_score = len(r_overlap) / len(desc_tokens)
        s_score = len(s_overlap) / len(desc_tokens)

        if r_score == 0 and s_score == 0:
            both_zero += 1
            stats["both_zero"] += 1
            continue

        # Flag: suggested matches better
        if s_score > r_score:
            # Require meaningful gap
            if r_score == 0 or (r_score > 0 and s_score >= r_score * 1.5):
                r_sector = get_sector(reported)
                s_sector = get_sector(suggested)
                crosses_sector = not sectors_match(r_sector, s_sector)
                nflags = pipeline_flags.get(row.get("id", ""), -1)

                # DART rates
                r_dart, _ = lookup_dart(reported, dart_rates)
                s_dart, _ = lookup_dart(suggested, dart_rates)
                dart_ratio = ""
                if r_dart is not None and s_dart is not None:
                    max_rate = max(r_dart, s_dart)
                    if max_rate > 0:
                        dart_ratio = f"{abs(r_dart - s_dart) / max_rate:.3f}"

                result = {
                    "id": row.get("id", ""),
                    "company_name": row.get("company_name", ""),
                    "establishment_name": row.get("establishment_name", ""),
                    "city": row.get("city", ""),
                    "state": row.get("state", ""),
                    "industry_description": desc,
                    "reported_naics": reported,
                    "reported_naics_tree": reported_tree[:120],
                    "suggested_naics": suggested,
                    "suggested_naics_tree": suggested_tree[:120],
                    "reported_score": f"{r_score:.3f}",
                    "suggested_score": f"{s_score:.3f}",
                    "score_gap": f"{s_score - r_score:.3f}",
                    "reported_overlap": ", ".join(sorted(r_overlap)),
                    "suggested_overlap": ", ".join(sorted(s_overlap)),
                    "crosses_sector": "Y" if crosses_sector else "N",
                    "reported_sector": f"{r_sector} ({sector_name(r_sector)})",
                    "suggested_sector": f"{s_sector} ({sector_name(s_sector)})",
                    "reported_dart": f"{r_dart:.2f}" if r_dart is not None else "",
                    "suggested_dart": f"{s_dart:.2f}" if s_dart is not None else "",
                    "dart_ratio": dart_ratio,
                    "pipeline_flags": str(nflags),
                    "scrape_confidence": row.get("scrape_confidence", ""),
                    "scrape_match_status": row.get("scrape_match_status", ""),
                    "scrape_reasoning": row.get("scrape_reasoning", "")[:200],
                }
                results.append(result)
                stats["contradiction"] += 1
                if crosses_sector:
                    stats["cross_sector"] += 1
                if nflags == 0:
                    stats["new_finding"] += 1
            else:
                stats["gap_too_small"] += 1
        elif r_score > s_score:
            stats["reported_better"] += 1
        else:
            stats["tied"] += 1

    # Summary
    print(f"\n{'=' * 60}")
    print(f"DESCRIPTION CONTRADICTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Analyzed:              {len(analyze_rows):>8,}")
    print(f"  Empty desc tokens:     {stats.get('empty_tokens', 0):>8,}")
    print(f"  No NAICS tokens:       {stats.get('no_naics_tokens', 0):>8,}")
    print(f"  Both zero (vocab gap): {both_zero:>8,}")
    print(f"  Reported matches better:{stats.get('reported_better', 0):>7,}")
    print(f"  Tied:                  {stats.get('tied', 0):>8,}")
    print(f"  Gap too small:         {stats.get('gap_too_small', 0):>8,}")
    print(f"  CONTRADICTIONS:        {stats.get('contradiction', 0):>8,}")
    print(f"    Cross-sector:        {stats.get('cross_sector', 0):>8,}")
    print(f"    NEW (0 flags):       {stats.get('new_finding', 0):>8,}")

    # Sector crossing patterns
    cross_patterns = Counter()
    for r in results:
        if r["crosses_sector"] == "Y":
            cross_patterns[f"{r['reported_sector']} -> {r['suggested_sector']}"] += 1
    if cross_patterns:
        print(f"\n  Top sector crossing patterns:")
        for pattern, n in cross_patterns.most_common(15):
            print(f"    {pattern}: {n:>5,}")

    # Pipeline flag distribution
    flag_dist = Counter()
    for r in results:
        f = int(r["pipeline_flags"])
        if f == 0:
            flag_dist["0 flags (CLEAN)"] += 1
        elif f == 1:
            flag_dist["1 flag"] += 1
        else:
            flag_dist["2+ flags"] += 1
    print(f"\n  By pipeline flag count:")
    for label in ["0 flags (CLEAN)", "1 flag", "2+ flags"]:
        print(f"    {label}: {flag_dist.get(label, 0):>5,}")

    # Publication nuggets: cross-sector with high gap
    nuggets = [r for r in results
               if r["crosses_sector"] == "Y"
               and float(r["score_gap"]) >= 0.10]
    nuggets.sort(key=lambda r: float(r["score_gap"]), reverse=True)

    print(f"\n{'=' * 60}")
    print(f"PUBLICATION NUGGETS (cross-sector, gap >= 0.10): {len(nuggets)}")
    print(f"{'=' * 60}")
    for r in nuggets[:40]:
        dart_info = ""
        if r["dart_ratio"]:
            dart_info = f" DART ratio={r['dart_ratio']}"
        flag_label = "CLEAN" if r["pipeline_flags"] == "0" else f"{r['pipeline_flags']} flags"
        print(f"\n  [{flag_label}] {r['company_name'][:40]}")
        print(f"    Reported: {r['reported_naics']} {r['reported_sector']}")
        print(f"    Suggested: {r['suggested_naics']} {r['suggested_sector']}")
        print(f"    Desc: \"{r['industry_description'][:80]}\"")
        print(f"    Scores: reported={r['reported_score']} "
              f"suggested={r['suggested_score']} gap={r['score_gap']}{dart_info}")

    if args.dry_run:
        print(f"\nDRY RUN -- no files written")
        return

    # Write output
    out_fields = [
        "id", "company_name", "establishment_name", "city", "state",
        "industry_description",
        "reported_naics", "reported_naics_tree",
        "suggested_naics", "suggested_naics_tree",
        "reported_score", "suggested_score", "score_gap",
        "reported_overlap", "suggested_overlap",
        "crosses_sector", "reported_sector", "suggested_sector",
        "reported_dart", "suggested_dart", "dart_ratio",
        "pipeline_flags", "scrape_confidence",
        "scrape_match_status", "scrape_reasoning",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWritten: {OUTPUT_FILE}")
    print(f"Total rows: {len(results):,}")


def main():
    parser = argparse.ArgumentParser(
        description="Industry description contradiction analysis"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview analysis without writing output")
    parser.add_argument("--apply", action="store_true",
                        help="Write desc_contradictions.csv")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
