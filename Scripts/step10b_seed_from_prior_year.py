"""
Step 10b: Cross-Year Scrape Seeding
====================================
Seeds scrape_results_{YEAR}.csv for a new dataset year by matching
companies from that year's uncertain pool against an existing year's
scrape results (typically CY 2023).

How it works:
  1. Loads the SEED year's scrape_results (e.g., scrape_results_2023.csv)
  2. Loads the TARGET year's uncertain_for_websearch (e.g., uncertain_for_websearch_2021.csv)
  3. Matches by company_name + city (case-insensitive)
  4. For matched records, copies company-level scrape fields (website_url,
     scraped_keywords, suggested_naics, etc.) and re-evaluates the
     scrape_match_status based on the target record's reported NAICS
  5. Writes seeded scrape_results_{TARGET_YEAR}.csv and checkpoint file
  6. The web scraper (step10) can then --resume and only scrape unmatched companies

Usage:
  python Scripts/step10b_seed_from_prior_year.py --seed-year 2023 --target-year 2021 --dry-run
  python Scripts/step10b_seed_from_prior_year.py --seed-year 2023 --target-year 2021 --apply

Multiple seed years (cascading):
  python Scripts/step10b_seed_from_prior_year.py --seed-year 2023,2022 --target-year 2021 --apply
"""

import argparse
import csv
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
sys.path.insert(0, os.path.join(BASE_DIR, "Scripts"))


def normalize(text):
    """Normalize company name for matching."""
    return (text or "").strip().lower()


def build_seed_index(seed_years):
    """Build a lookup from (company_name, city) -> best seed row across all seed years."""
    index = {}  # (cn, city) -> row
    for year in seed_years:
        seed_file = os.path.join(PIPELINE_DIR, f"scrape_results_{year}.csv")
        if not os.path.exists(seed_file):
            print(f"  WARNING: {seed_file} not found, skipping")
            continue
        with open(seed_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                cn = normalize(row.get("company_name", ""))
                city = normalize(row.get("city", ""))
                if not cn or not city:
                    continue
                key = (cn, city)
                status = row.get("scrape_match_status", "")
                # Prefer rows with actual scrape results over SCRAPE_FAILED
                if key not in index or (index[key].get("scrape_match_status") in
                                        ("SCRAPE_FAILED", "NO_WEBSITE", "") and
                                        status not in ("SCRAPE_FAILED", "NO_WEBSITE", "")):
                    index[key] = row
                count += 1
        print(f"  Loaded {count:,} rows from scrape_results_{year}.csv")
    return index


def reevaluate_status(seed_row, target_naics):
    """Re-evaluate scrape_match_status for a different reported NAICS code.

    If the target record's NAICS differs from the seed, we adjust:
      - If target NAICS matches suggested_naics -> CONFIRMED
      - If target NAICS matches seed's original naics_code -> keep original status
      - Otherwise -> keep SUGGESTED/UNCERTAIN as appropriate
    """
    orig_status = seed_row.get("scrape_match_status", "")
    suggested = seed_row.get("suggested_naics", "").strip()
    seed_naics = seed_row.get("naics_code", "").strip()

    # If the seed had no useful result, keep as-is
    if orig_status in ("SCRAPE_FAILED", "NO_WEBSITE", ""):
        return orig_status

    # KC/EDGAR/SAM statuses are enrichment-based, not scrape-based.
    # Don't seed these: they'll be re-evaluated by steps 11-13b.
    if orig_status.startswith("KC_") or orig_status.startswith("EDGAR_") or orig_status.startswith("SAM_"):
        # Downgrade to the underlying scrape status
        # If the seed had scraped_keywords, it was at least UNCERTAIN
        if seed_row.get("scraped_keywords", "").strip():
            if suggested and suggested == target_naics:
                return "CONFIRMED"
            elif suggested:
                return "SUGGESTED"
            else:
                return "UNCERTAIN"
        return "SCRAPE_FAILED"

    # Same NAICS as seed -> keep original status
    if target_naics == seed_naics:
        return orig_status

    # Different NAICS: re-evaluate
    if suggested and suggested == target_naics:
        return "CONFIRMED"
    elif orig_status == "CONFIRMED" and target_naics != seed_naics:
        # Seed was confirmed for a different NAICS; now it's a suggestion
        return "SUGGESTED"
    else:
        return orig_status


def main():
    parser = argparse.ArgumentParser(description="Seed scrape results from prior year(s)")
    parser.add_argument("--seed-year", required=True,
                        help="Comma-separated seed year(s), e.g. '2023' or '2023,2022'")
    parser.add_argument("--target-year", required=True,
                        help="Target year to seed")
    parser.add_argument("--apply", action="store_true",
                        help="Write seeded files (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only (default)")
    args = parser.parse_args()

    seed_years = [y.strip() for y in args.seed_year.split(",")]
    target_year = args.target_year.strip()

    print(f"Cross-Year Scrape Seeding: {','.join(seed_years)} -> {target_year}")
    print("=" * 60)

    # Build seed index
    print(f"\nLoading seed data from {len(seed_years)} year(s)...")
    seed_index = build_seed_index(seed_years)
    print(f"  Total seed index: {len(seed_index):,} unique (company, city) pairs")

    # Load target uncertain pool
    target_file = os.path.join(PIPELINE_DIR, f"uncertain_for_websearch_{target_year}.csv")
    print(f"\nLoading target uncertain pool: {target_file}")
    with open(target_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        target_fields = reader.fieldnames
        target_rows = list(reader)
    print(f"  {len(target_rows):,} records")

    # Scrape result columns (superset of uncertain columns)
    scrape_extra_fields = [
        "n_flags", "naics_code_status", "naics_code_note",
        "website_url", "url_source", "scraped_keywords",
        "suggested_naics", "suggested_description",
        "scrape_confidence", "scrape_reasoning", "scrape_match_status",
    ]
    out_fields = target_fields + [f for f in scrape_extra_fields if f not in target_fields]

    # Match and seed
    seeded_rows = []
    unmatched_rows = []
    matched_companies = set()
    unmatched_companies = set()

    for row in target_rows:
        cn = normalize(row.get("company_name", ""))
        city = normalize(row.get("city", ""))
        key = (cn, city)
        target_naics = row.get("naics_code", "").strip()

        if cn and city and key in seed_index:
            seed = seed_index[key]
            # Build seeded row: target record fields + seed scrape fields
            out = dict(row)
            out["website_url"] = seed.get("website_url", "")
            out["url_source"] = seed.get("url_source", "")
            out["scraped_keywords"] = seed.get("scraped_keywords", "")
            out["suggested_naics"] = seed.get("suggested_naics", "")
            out["suggested_description"] = seed.get("suggested_description", "")
            out["scrape_confidence"] = seed.get("scrape_confidence", "")
            out["scrape_reasoning"] = seed.get("scrape_reasoning", "")
            out["naics_code_status"] = seed.get("naics_code_status", "")
            out["naics_code_note"] = seed.get("naics_code_note", "")
            out["n_flags"] = row.get("n_flags", "")

            # Re-evaluate match status for this record's NAICS
            out["scrape_match_status"] = reevaluate_status(seed, target_naics)

            seeded_rows.append(out)
            matched_companies.add(key)
        else:
            unmatched_rows.append(row)
            if cn and city:
                unmatched_companies.add(key)

    # Summary
    print(f"\n{'='*60}")
    print(f"SEEDING RESULTS for CY {target_year}")
    print(f"{'='*60}")
    print(f"  Total records:           {len(target_rows):,}")
    print(f"  Seeded (matched):        {len(seeded_rows):,} ({len(matched_companies):,} companies)")
    print(f"  Unmatched (need scrape): {len(unmatched_rows):,} ({len(unmatched_companies):,} companies)")
    print(f"  Match rate:              {len(seeded_rows)*100/len(target_rows):.1f}%")

    # Status distribution for seeded rows
    from collections import Counter
    status_dist = Counter(r.get("scrape_match_status", "") for r in seeded_rows)
    print(f"\n  Seeded status distribution:")
    for s, n in status_dist.most_common():
        print(f"    {s}: {n:,}")

    if not args.apply:
        print(f"\n  DRY RUN: no files written. Use --apply to write.")
        return

    # Write seeded scrape_results
    output_file = os.path.join(PIPELINE_DIR, f"scrape_results_{target_year}.csv")
    print(f"\n  Writing {output_file}...")
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(seeded_rows)
    print(f"    {len(seeded_rows):,} rows written")

    # Write checkpoint for scraper resume
    checkpoint_file = os.path.join(PIPELINE_DIR, f"scrape_checkpoint_{target_year}.json")
    checkpoint = {
        "processed_ids": [r["id"] for r in seeded_rows],
        "stats": {
            "guess_hits": 0,
            "search_hits": 0,
            "no_url": 0,
            "seeded_from": seed_years,
        }
    }
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f)
    print(f"    Checkpoint: {len(checkpoint['processed_ids']):,} IDs marked as processed")

    # Write unmatched records for reference
    unmatched_file = os.path.join(PIPELINE_DIR, f"uncertain_unmatched_{target_year}.csv")
    with open(unmatched_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=target_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unmatched_rows)
    print(f"    Unmatched: {unmatched_file} ({len(unmatched_rows):,} rows)")

    print(f"\n  Done. Run step10 with --resume to scrape remaining {len(unmatched_companies):,} companies.")


if __name__ == "__main__":
    main()
