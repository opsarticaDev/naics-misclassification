"""
Step 13: Merge EDGAR + SAM.gov Enrichments into scrape_results_{YEAR}.csv
==========================================================================
INPUT:  scrape_results_{YEAR}.csv (records from Step 10)
        edgar_results_{YEAR}.csv  (records from Step 11)
        sam_results_{YEAR}.csv    (records from Step 12)
OUTPUT: scrape_results_{YEAR}.csv (updated in-place, backup created)

Methodology
-----------
Left-joins enrichment results back into the scrape output by record ID.
EDGAR is applied first (lower priority), then SAM (higher priority,
overrides EDGAR if both enriched the same record).

Merge priority (highest to lowest):
    SAM_CONFIRMED > EDGAR_CONFIRMED > SAM_SUGGESTED > EDGAR_SUGGESTED >
    CONFIRMED > SUGGESTED > UNCERTAIN > SCRAPE_FAILED > *_NOT_FOUND

Rationale: SAM provides NAICS codes directly from the entity's federal
registration: the company self-reported these codes in a legal context.
EDGAR provides SIC codes that must be crosswalked to NAICS, introducing
translation uncertainty. Both are more authoritative than keyword scraping,
but SAM NAICS is the most direct evidence.

Only fields in ENRICHMENT_FIELDS are updated; the original record data
(company name, address, reported NAICS, gate flags) is never modified.

A backup of scrape_results.csv is created on first --apply run.

CLI Flags
---------
  --dry-run   Show what would change without writing
  --apply     Write updated scrape_results.csv (creates .pre_merge_backup)

"""

import argparse
import csv
import os
import sys
from collections import Counter

from util_scrape_config import (
    BASE_DIR, SCRAPE_OUTPUT_FILE,
    EDGAR_OUTPUT_FILE, SAM_OUTPUT_FILE,
    OUTPUT_COLUMNS,
)

# Fields to update from enrichment sources
ENRICHMENT_FIELDS = [
    "suggested_naics", "suggested_description",
    "scrape_confidence", "scrape_reasoning", "scrape_match_status",
]


def load_csv(filepath):
    """Load a CSV into a list of dicts."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_lookup(rows, key_field="id"):
    """Build a dict keyed by record ID for fast lookup."""
    lookup = {}
    for row in rows:
        rid = row.get(key_field, "")
        if rid:
            lookup[rid] = row
    return lookup


def should_update(existing_status, new_status):
    """Determine if new enrichment status should override existing.

    Priority (highest to lowest):
        SAM_CONFIRMED > EDGAR_CONFIRMED > SAM_SUGGESTED > EDGAR_SUGGESTED >
        CONFIRMED > SUGGESTED > UNCERTAIN > SCRAPE_FAILED > NOT_FOUND

    Only the relative ordering matters: the specific numeric values are
    arbitrary. A higher number means more authoritative evidence.
    """
    priority = {
        "SAM_CONFIRMED": 100,
        "EDGAR_CONFIRMED": 90,
        "SAM_SUGGESTED": 80,
        "EDGAR_SUGGESTED": 70,
        "CONFIRMED": 60,
        "SUGGESTED": 50,
        "UNCERTAIN": 20,
        "SCRAPE_FAILED": 10,
        "SAM_NOT_FOUND": 5,
        "EDGAR_NOT_FOUND": 5,
        "EDGAR_SIC_NO_CROSSWALK": 5,
        "SAM_NO_NAICS": 5,
        "NO_WEBSITE": 5,
    }
    return priority.get(new_status, 0) > priority.get(existing_status, 0)


def merge_record(base, enrichment, source_name):
    """Merge enrichment data into base record.

    Returns:
        tuple: (updated_record, changed: bool, change_description: str)
    """
    if not enrichment:
        return base, False, ""

    new_status = enrichment.get("scrape_match_status", "")
    old_status = base.get("scrape_match_status", "")

    if not should_update(old_status, new_status):
        return base, False, ""

    updated = dict(base)
    changes = []

    for field in ENRICHMENT_FIELDS:
        new_val = enrichment.get(field, "")
        old_val = base.get(field, "")
        if new_val and new_val != old_val:
            updated[field] = new_val
            if field == "scrape_match_status":
                changes.append(f"status: {old_val} -> {new_val}")
            elif field == "suggested_naics" and new_val != old_val:
                changes.append(f"naics: {old_val or '(empty)'} -> {new_val}")

    change_desc = f"[{source_name}] " + "; ".join(changes) if changes else ""
    return updated, bool(changes), change_desc


def run(args):
    """Load all data sources, apply enrichments by priority, write output.

    EDGAR is applied first (enrichment fields only), then SAM overrides
    where SAM has a higher-priority status. Original record data is
    never modified: only ENRICHMENT_FIELDS are updated.
    """
    # Load all data
    print("Loading data...")
    scrape_rows = load_csv(SCRAPE_OUTPUT_FILE)
    print(f"  scrape_results.csv: {len(scrape_rows)} records")

    edgar_rows = load_csv(EDGAR_OUTPUT_FILE)
    print(f"  edgar_results.csv: {len(edgar_rows)} records")

    sam_rows = load_csv(SAM_OUTPUT_FILE)
    print(f"  sam_results.csv: {len(sam_rows)} records")

    if not scrape_rows:
        print("ERROR: No scrape results to merge into")
        sys.exit(1)

    if not edgar_rows and not sam_rows:
        print("No enrichment data to merge. Run edgar_lookup.py and/or sam_lookup.py first.")
        return

    # Build lookups
    edgar_lookup = build_lookup(edgar_rows)
    sam_lookup = build_lookup(sam_rows)

    # Merge
    updated_rows = []
    stats = Counter()
    change_log = []

    for row in scrape_rows:
        rid = row.get("id", "")
        original_status = row.get("scrape_match_status", "")
        current = dict(row)

        # Apply EDGAR enrichment first (lower priority)
        edgar_rec = edgar_lookup.get(rid)
        if edgar_rec:
            current, changed, desc = merge_record(current, edgar_rec, "EDGAR")
            if changed:
                stats["edgar_updated"] += 1
                change_log.append(f"  {rid}: {desc}")

        # Apply SAM enrichment second (higher priority, overrides EDGAR)
        sam_rec = sam_lookup.get(rid)
        if sam_rec:
            current, changed, desc = merge_record(current, sam_rec, "SAM")
            if changed:
                stats["sam_updated"] += 1
                change_log.append(f"  {rid}: {desc}")

        final_status = current.get("scrape_match_status", "")
        if final_status != original_status:
            stats[f"from_{original_status}"] += 1
            stats[f"to_{final_status}"] += 1

        updated_rows.append(current)

    # Report
    print(f"\n{'='*60}")
    print(f"MERGE SUMMARY")
    print(f"{'='*60}")
    print(f"Records updated by EDGAR: {stats.get('edgar_updated', 0)}")
    print(f"Records updated by SAM: {stats.get('sam_updated', 0)}")
    total_updated = stats.get("edgar_updated", 0) + stats.get("sam_updated", 0)
    print(f"Total records changed: {total_updated}")

    # Status transitions
    final_statuses = Counter(r.get("scrape_match_status", "") for r in updated_rows)
    original_statuses = Counter(r.get("scrape_match_status", "") for r in scrape_rows)
    print(f"\nStatus distribution (before -> after):")
    all_statuses = set(list(original_statuses.keys()) + list(final_statuses.keys()))
    for s in sorted(all_statuses):
        before = original_statuses.get(s, 0)
        after = final_statuses.get(s, 0)
        diff = after - before
        indicator = f" (+{diff})" if diff > 0 else (f" ({diff})" if diff < 0 else "")
        print(f"  {s}: {before} -> {after}{indicator}")

    if args.dry_run:
        print(f"\nDRY RUN: no files written")
        if change_log:
            print(f"\nChange log (first 50):")
            for line in change_log[:50]:
                print(line)
            if len(change_log) > 50:
                print(f"  ... and {len(change_log) - 50} more")
        return

    # Write updated scrape_results.csv
    # Backup first
    backup = SCRAPE_OUTPUT_FILE + ".pre_merge_backup"
    if not os.path.exists(backup):
        import shutil
        shutil.copy2(SCRAPE_OUTPUT_FILE, backup)
        print(f"\nBackup: {backup}")

    with open(SCRAPE_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in updated_rows:
            writer.writerow(row)

    print(f"\nUpdated: {SCRAPE_OUTPUT_FILE}")
    print(f"Total records: {len(updated_rows)}")


def main():
    parser = argparse.ArgumentParser(description="Merge EDGAR + SAM enrichments")
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
