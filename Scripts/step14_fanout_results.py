"""
Step 14: Fan-Out Company Results to All OSHA Records
=====================================================
INPUT:  scrape_results_{YEAR}.csv  (one row per unique company, ~13,549 rows)
        uncertain_for_websearch_{YEAR}.csv  (all uncertain OSHA records, ~43,767 rows)
OUTPUT: scrape_results_{YEAR}.csv  (expanded: one row per OSHA record, ~43,767 rows)

Methodology
-----------
The web scraper (Step 10) deduplicates by company name: it scrapes each
company once, regardless of how many OSHA 300A records that company has.
This is efficient for scraping, but creates a gap: 30,000+ records whose
company WAS scraped don't appear in scrape_results because the scraper
only emitted one representative row per company.

This script closes that gap by "fanning out" each company-level scrape
result to ALL OSHA records belonging to that company:

  1. Load scrape_results (company-level, ~13,549 rows)
  2. Load uncertain_for_websearch (record-level, ~43,767 rows)
     (If uncertain file is absent, i.e. fan-out already done, skip
     fan-out and proceed to status recomputation only.)
  3. For each uncertain record not already in scrape_results:
     - Find the company's scrape result by normalized company name
     - Create a new row: base fields from the uncertain record +
       enrichment fields from the scrape result
  4. Write expanded scrape_results with all records

Most enrichment fields are copied from the company's representative row:
  website_url, url_source, scraped_keywords, suggested_naics,
  suggested_description, scrape_confidence, scrape_reasoning,
  scrape_match_status

EXCEPTION: naics_code_status and naics_code_note are RE-COMPUTED per
sibling using each record's own naics_code, not copied from the
representative. This is critical because siblings within the same company
can have different NAICS codes (e.g., 238220 vs 283220), and blindly
copying the representative's "current" status to a sibling with an
invalid code produces wrong labels. Fixed 2026-02-20: 1,518 records had
wrong status from the original naive copy.

The base fields (id, company name, address, reported NAICS, flags) always
come from the individual OSHA record: never from the representative row.

A backup of the pre-fanout scrape_results is created automatically.

CLI Flags
---------
  --dry-run   Show what would change without writing
  --apply     Write expanded scrape_results.csv (creates .pre_fanout_backup)

"""

import argparse
import csv
import os
import shutil
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from util_scrape_config import (
    SCRAPE_OUTPUT_FILE, OUTPUT_COLUMNS,
    build_naics_keyword_index, classify_naics_code,
)
from util_pipeline_config import DATASET_YEAR

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
UNCERTAIN_FILE = os.path.join(PIPELINE_DIR, f"uncertain_for_websearch_{DATASET_YEAR}.csv")

# Fields that come from the scrape result (company-level enrichment)
ENRICHMENT_FIELDS = [
    "naics_code_status",
    "naics_code_note",
    "website_url",
    "url_source",
    "scraped_keywords",
    "suggested_naics",
    "suggested_description",
    "scrape_confidence",
    "scrape_reasoning",
    "scrape_match_status",
]


def normalize_name(record):
    """Normalize company name for matching: mirrors step10's dedup logic.

    Applies the same underscore replacement as step10's process_record()
    to prevent fanout mismatches when names contain underscores.
    """
    name = (record.get("company_name") or record.get("establishment_name") or "")
    return name.replace("_", " ").strip().lower()


def load_csv(filepath):
    """Load a CSV into a list of dicts."""
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run(args):
    """Fan out company-level scrape results to all OSHA records."""
    print("Step 14: Fan-Out Company Results")
    print("=" * 60)

    # Load data
    print(f"\nLoading scrape_results: {SCRAPE_OUTPUT_FILE}")
    scrape_rows = load_csv(SCRAPE_OUTPUT_FILE)
    print(f"  {len(scrape_rows):,} rows (company-level)")

    if os.path.exists(UNCERTAIN_FILE):
        print(f"Loading uncertain records: {UNCERTAIN_FILE}")
        uncertain_rows = load_csv(UNCERTAIN_FILE)
        print(f"  {len(uncertain_rows):,} rows (record-level)")
    else:
        print(f"  uncertain file not found (fan-out already done?), skipping fan-out")
        uncertain_rows = []

    # Build lookup: normalized company name → scrape result enrichment fields
    # If multiple scrape rows exist for the same company, use the one with
    # the highest-priority scrape_match_status
    status_priority = {
        "SAM_CONFIRMED": 100, "EDGAR_CONFIRMED": 90,
        "SAM_SUGGESTED": 80, "EDGAR_SUGGESTED": 70,
        "CONFIRMED": 60, "SUGGESTED": 50,
        "UNCERTAIN": 20, "SCRAPE_FAILED": 10,
        "NO_WEBSITE": 5,
    }

    company_enrichment = {}  # normalized_name → {enrichment fields}
    for row in scrape_rows:
        name = normalize_name(row)
        if not name:
            continue
        enrichment = {f: row.get(f, "") for f in ENRICHMENT_FIELDS}
        existing = company_enrichment.get(name)
        if existing is None:
            company_enrichment[name] = enrichment
        else:
            # Keep the higher-priority result
            new_pri = status_priority.get(enrichment.get("scrape_match_status", ""), 0)
            old_pri = status_priority.get(existing.get("scrape_match_status", ""), 0)
            if new_pri > old_pri:
                company_enrichment[name] = enrichment

    print(f"\n  {len(company_enrichment):,} unique company enrichments built")

    # Load NAICS index for per-sibling status recomputation
    print("  Loading NAICS index for per-sibling status recomputation...")
    naics_index = build_naics_keyword_index()

    # Build set of record IDs already in scrape_results
    existing_ids = {row.get("id", "") for row in scrape_rows}
    print(f"  {len(existing_ids):,} record IDs already in scrape_results")

    # Fan out: for each uncertain record not in scrape_results,
    # create a new row with base fields from uncertain + enrichment from company
    new_rows = []
    stats = Counter()
    unmatched = []

    for rec in uncertain_rows:
        rid = rec.get("id", "")
        if rid in existing_ids:
            stats["already_present"] += 1
            continue

        name = normalize_name(rec)
        enrichment = company_enrichment.get(name)
        if enrichment is None:
            stats["no_match"] += 1
            unmatched.append(rid)
            continue

        # Build new row: base from uncertain record + enrichment from company
        new_row = {}
        for col in OUTPUT_COLUMNS:
            if col in ENRICHMENT_FIELDS:
                new_row[col] = enrichment.get(col, "")
            else:
                new_row[col] = rec.get(col, "")

        # Fix: re-compute naics_code_status/note using THIS sibling's own
        # NAICS code, not the representative's. The representative may have
        # a different (valid) code while this sibling's code is retired/invalid.
        sibling_naics = rec.get("naics_code", "").strip()
        if sibling_naics:
            code_info = classify_naics_code(sibling_naics, naics_index)
            new_row["naics_code_status"] = code_info["status"]
            new_row["naics_code_note"] = code_info["suggestion"] or ""

        new_rows.append(new_row)
        stats["fanned_out"] += 1
        stats[f"status_{enrichment.get('scrape_match_status', 'UNKNOWN')}"] += 1

    # Report
    print(f"\n{'=' * 60}")
    print(f"FAN-OUT SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Already in scrape_results:  {stats.get('already_present', 0):>8,}")
    print(f"  Fanned out (new rows):      {stats.get('fanned_out', 0):>8,}")
    print(f"  No company match:           {stats.get('no_match', 0):>8,}")
    total = stats.get("already_present", 0) + stats.get("fanned_out", 0) + stats.get("no_match", 0)
    print(f"  Total uncertain records:    {total:>8,}")

    print(f"\n  New rows by scrape_match_status:")
    for key in sorted(stats.keys()):
        if key.startswith("status_"):
            status_name = key[7:]
            print(f"    {status_name}: {stats[key]:,}")

    final_total = len(scrape_rows) + len(new_rows)
    print(f"\n  Final scrape_results size: {len(scrape_rows):,} existing + {len(new_rows):,} new = {final_total:,}")

    if unmatched:
        print(f"\n  WARNING: {len(unmatched)} records had no company match (first 10):")
        for rid in unmatched[:10]:
            print(f"    {rid}")

    if args.dry_run:
        print(f"\nDRY RUN: no files written")
        return

    # Write expanded scrape_results
    backup = SCRAPE_OUTPUT_FILE + ".pre_fanout_backup"
    if not os.path.exists(backup):
        shutil.copy2(SCRAPE_OUTPUT_FILE, backup)
        print(f"\nBackup: {backup}")

    # Fix existing scrape rows too: re-compute naics_code_status per row
    status_fixes = 0
    for row in scrape_rows:
        row_naics = row.get("naics_code", "").strip()
        if row_naics:
            code_info = classify_naics_code(row_naics, naics_index)
            old_status = row.get("naics_code_status", "")
            new_status = code_info["status"]
            if old_status != new_status:
                status_fixes += 1
                row["naics_code_status"] = new_status
                row["naics_code_note"] = code_info["suggestion"] or ""
    if status_fixes:
        print(f"\n  Fixed {status_fixes:,} existing rows with wrong naics_code_status")

    # Combine: existing scrape rows + new fanned-out rows, dedup by ID
    all_rows = scrape_rows + new_rows
    seen_ids = set()
    deduped_rows = []
    dup_count = 0
    for row in all_rows:
        rid = row.get("id", "")
        if rid in seen_ids:
            dup_count += 1
            continue
        seen_ids.add(rid)
        deduped_rows.append(row)
    if dup_count:
        print(f"\n  Deduplicated: removed {dup_count} duplicate ID rows")
    all_rows = deduped_rows

    with open(SCRAPE_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"\nWritten: {SCRAPE_OUTPUT_FILE}")
    print(f"Total rows: {len(all_rows):,}")


def main():
    parser = argparse.ArgumentParser(
        description="Fan out company-level scrape results to all OSHA records"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing")
    parser.add_argument("--apply", action="store_true",
                        help="Write expanded scrape_results.csv")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
