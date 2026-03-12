"""
Step 13b: Known Company Enrichment + Junk URL Filter
=====================================================
INPUT:  scrape_results_{YEAR}.csv (company-level, post-merge)
OUTPUT: scrape_results_{YEAR}.csv (updated with known-company matches)

Run AFTER step13 (merge) and BEFORE step14 (fan-out).

This step does two things:

1. KNOWN COMPANY ENRICHMENT
   For records where the scraper failed or was uncertain, check if the
   company matches a curated known-company lookup table. Large retailers,
   grocery chains, government agencies, and other well-known organizations
   have predictable NAICS codes. If the reported NAICS is in the company's
   valid set, mark CONFIRMED. If not, suggest the most appropriate code
   using sub-unit keyword disambiguation.

   Guard: A Walmart Pharmacy gets 446110, not 452910. The sub-unit rules
   prevent over-broad classification of multi-sector companies.

2. JUNK URL FILTER
   Flag records whose website_url points to a known junk domain (zhihu.com,
   beeradvocate.com, etc.). These are wrong URLs from the search engine,
   not actual company websites. Flagged records get scrape_match_status
   reset to SCRAPE_FAILED so triage routes them to T5 (no data) rather
   than treating the junk URL as evidence.

CLI Flags:
  --dry-run   Show what would change without writing
  --apply     Write updated scrape_results.csv
"""

import argparse
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from util_scrape_config import SCRAPE_OUTPUT_FILE, OUTPUT_COLUMNS
from util_pipeline_config import DATASET_YEAR
from util_known_companies import (
    match_known_company, load_junk_domains, is_junk_url,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run(args):
    print("Step 13b: Known Company Enrichment + Junk URL Filter")
    print("=" * 60)

    # Load scrape results
    print(f"\nLoading: {SCRAPE_OUTPUT_FILE}")
    with open(SCRAPE_OUTPUT_FILE, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  {len(rows):,} rows")

    # Load junk domains
    junk_domains = load_junk_domains()
    print(f"  {len(junk_domains)} junk domains loaded")

    # Stats
    stats = Counter()
    kc_matches = []
    junk_hits = []

    for row in rows:
        status = row.get("scrape_match_status", "")
        company = row.get("company_name", "")
        estab = row.get("establishment_name", "")
        desc = row.get("industry_description", "")
        reported = row.get("naics_code", "").strip()
        url = row.get("website_url", "")

        # ── Junk URL filter ──
        if url and status in ("UNCERTAIN", "SCRAPE_FAILED", "SUGGESTED"):
            if is_junk_url(url, junk_domains):
                stats["junk_url"] += 1
                junk_hits.append((company, url, status))
                if not args.dry_run:
                    row["scrape_match_status"] = "SCRAPE_FAILED"
                    row["scrape_reasoning"] = (
                        f"Junk URL filtered: {url} is not a company website"
                    )
                    row["website_url"] = ""
                    row["url_source"] = ""
                    row["scraped_keywords"] = ""
                    row["suggested_naics"] = ""
                    row["suggested_description"] = ""
                    row["scrape_confidence"] = ""
                # Fall through to known-company check (status is now SCRAPE_FAILED)

        # ── Known company enrichment ──
        # Only enrich records that lack a resolution
        # Re-read status since junk URL filter may have changed it
        status = row.get("scrape_match_status", "")
        if status not in ("SCRAPE_FAILED", "UNCERTAIN", "NO_WEBSITE"):
            stats["skip_already_resolved"] += 1
            continue

        result = match_known_company(company, estab, desc, reported)
        if result is None:
            stats["no_kc_match"] += 1
            continue

        match_type = result["match_type"]
        stats[f"kc_{match_type}"] += 1
        kc_matches.append((
            company[:40],
            reported,
            result["resolved_naics"],
            match_type,
            result["company_id"],
        ))

        if not args.dry_run:
            row["scrape_match_status"] = match_type
            row["suggested_naics"] = result["resolved_naics"]
            row["suggested_description"] = result["resolved_desc"]
            row["scrape_reasoning"] = result["reasoning"]
            row["scrape_confidence"] = "high"
            # If confirmed, set the verified field too
            if match_type == "KC_CONFIRMED":
                row["naics_verified"] = result["resolved_naics"]
                row["naics_description"] = result["resolved_desc"]
                row["confidence"] = "high"

    # ── Report ──
    print(f"\n{'=' * 60}")
    print("ENRICHMENT SUMMARY")
    print(f"{'=' * 60}")

    print(f"\n  Junk URL filter:")
    print(f"    URLs flagged as junk:     {stats['junk_url']:>6,}")
    if junk_hits:
        # Show top junk domains
        from urllib.parse import urlparse
        junk_domain_counts = Counter()
        for _, url, _ in junk_hits:
            try:
                d = urlparse(url).netloc.lower()
                if d.startswith("www."):
                    d = d[4:]
                junk_domain_counts[d] += 1
            except Exception:
                pass
        print(f"    Top junk domains:")
        for d, c in junk_domain_counts.most_common(10):
            print(f"      {c:>5} {d}")

    print(f"\n  Known company enrichment:")
    print(f"    KC_CONFIRMED (reported NAICS valid):  {stats.get('kc_KC_CONFIRMED', 0):>6,}")
    print(f"    KC_SUGGESTED (better NAICS found):    {stats.get('kc_KC_SUGGESTED', 0):>6,}")
    print(f"    No match in known-company table:      {stats.get('no_kc_match', 0):>6,}")
    print(f"    Skipped (already resolved):           {stats.get('skip_already_resolved', 0):>6,}")

    total_enriched = stats.get("kc_KC_CONFIRMED", 0) + stats.get("kc_KC_SUGGESTED", 0)
    total_affected = total_enriched + stats["junk_url"]
    print(f"\n  Total records affected: {total_affected:,}")

    if kc_matches:
        print(f"\n  Known company match samples (first 20):")
        for company, reported, resolved, mtype, cid in kc_matches[:20]:
            arrow = "==" if mtype == "KC_CONFIRMED" else "->"
            print(f"    [{cid:20s}] {reported} {arrow} {resolved}  {company}")

    if args.dry_run:
        print(f"\nDRY RUN -- no files written")
        return

    # Write updated scrape results
    with open(SCRAPE_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWritten: {SCRAPE_OUTPUT_FILE}")
    print(f"Total rows: {len(rows):,}")


def main():
    parser = argparse.ArgumentParser(
        description="Known company enrichment + junk URL filter"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing")
    parser.add_argument("--apply", action="store_true",
                        help="Write updated scrape_results.csv")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
