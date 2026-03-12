"""
Step 12: SAM.gov Bulk Extract Matcher for NAICS Validation
===========================================================
INPUT:  scrape_results_{YEAR}.csv (UNCERTAIN/SCRAPE_FAILED not resolved by Step 11)
        Reference/Entity_Registration_Extract.csv (federal contractors)
OUTPUT: sam_results_{YEAR}.csv (records with SAM match status)

Methodology
-----------
SAM.gov (System for Award Management) maintains NAICS codes for all federal
contractors. Unlike the EDGAR SIC→NAICS crosswalk, SAM provides NAICS codes
directly: no translation needed.

This gate uses a pre-downloaded bulk extract (not the rate-limited API) to
fuzzy-match company names locally:

  1. LOAD the SAM extract, indexed by state for efficient matching.
     360K entities with legal business name, state, primary NAICS, and
     full NAICS list.

  2. FUZZY MATCH using rapidfuzz token_sort_ratio within the same-state
     bucket only. Cross-state matching is disabled because common company
     names (e.g., "Quality Services LLC") produce too many false positives.
     - Threshold: 85 (higher than EDGAR's 80) because the 360K entity pool
       is 36x larger, increasing the chance of coincidental name overlaps.

  3. DETERMINE MATCH by comparing SAM NAICS against reported NAICS:
     - SAM primary or list contains reported code → SAM_CONFIRMED
     - Same 4-digit sector → SAM_SUGGESTED
     - Different code entirely → SAM_SUGGESTED

  4. Unmatched records get SAM_NOT_FOUND (no API fallback: bulk extract
     covers all active registrations).

Limitations
-----------
  - Only covers federal contractors (~360K). Match rate is ~7%.
  - Same-state restriction means multi-state companies only match in
    the state of their physical address in SAM (which may differ from
    the OSHA establishment state).
  - The extract file may be stale if not re-downloaded periodically.

CLI Flags
---------
  --dry-run   Preview top/bottom matches without writing output
  --apply     Write sam_results.csv

"""

import argparse
import csv
import os
import re
import sys
import time
from collections import Counter, defaultdict

try:
    from rapidfuzz import fuzz, process as rfprocess
    USE_RAPIDFUZZ = True
except ImportError:
    from difflib import SequenceMatcher
    USE_RAPIDFUZZ = False
    print("WARNING: rapidfuzz not installed, falling back to difflib (much slower)")

from util_scrape_config import (
    BASE_DIR, SCRAPE_OUTPUT_FILE, EDGAR_OUTPUT_FILE,
    SAM_OUTPUT_FILE, OUTPUT_COLUMNS,
)

SAM_EXTRACT_FILE = os.path.join(BASE_DIR, "Reference", "Entity_Registration_Extract.csv")
# Higher than EDGAR's 80 because 360K entities (vs 10K) increases false positive
# risk with common substrings. At 80, names like "ABC Services" match unrelated
# entities; 85 eliminates these while keeping genuine matches like
# "CHENEY BROTHERS INC" → "CHENEY BROTHERS, INC" (score 100).
FUZZY_THRESHOLD = 85

# ---------------------------------------------------------------------------
# Company name normalization (same logic as edgar_lookup.py)
# ---------------------------------------------------------------------------
_CORP_SUFFIXES = re.compile(
    r'\b(inc\.?|incorporated|llc|l\.l\.c\.?|ltd\.?|limited|corp\.?|corporation'
    r'|co\.?|company|group|holdings?|enterprises?|partners?|lp|l\.p\.?'
    r'|plc|n\.?a\.?|s\.?a\.?|gmbh|ag|d/?b/?a)\b',
    re.IGNORECASE
)
_PUNCT = re.compile(r'[^\w\s]')
_MULTI_SPACE = re.compile(r'\s+')


def normalize_name(name):
    """Normalize company name for fuzzy matching."""
    if not name:
        return ""
    name = _CORP_SUFFIXES.sub('', name)
    name = _PUNCT.sub(' ', name)
    name = _MULTI_SPACE.sub(' ', name).strip().upper()
    return name


# ---------------------------------------------------------------------------
# Load SAM extract into state-bucketed index for fast matching
# ---------------------------------------------------------------------------
def load_sam_extract():
    """Load SAM extract, indexed by state for efficient matching.

    Returns:
        dict: {state: [{"name": str, "name_norm": str, "primary_naics": str,
                         "naics_list": str, "uei": str}, ...]}
        Also a flat list for fallback matching.
    """
    if not os.path.exists(SAM_EXTRACT_FILE):
        print(f"ERROR: {SAM_EXTRACT_FILE} not found")
        sys.exit(1)

    by_state = defaultdict(list)
    total = 0

    with open(SAM_EXTRACT_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("name", "").strip()
            if not name:
                continue
            entry = {
                "name": name,
                "name_norm": normalize_name(name),
                "primary_naics": row.get("primary_naics", "").strip(),
                "naics_list": row.get("naics_list_tokens", "").strip(),
                "uei": row.get("record_id", "").strip(),
                "state": row.get("phys_state", "").strip().upper(),
            }
            if not entry["name_norm"]:
                continue
            by_state[entry["state"]].append(entry)
            total += 1

    print(f"  Loaded {total:,} SAM entities across {len(by_state)} states")
    return by_state


def parse_naics_list(naics_list_tokens):
    """Parse '423120Y;811121Y;811122Y' into list of NAICS codes."""
    if not naics_list_tokens:
        return []
    codes = []
    for token in naics_list_tokens.split(";"):
        token = token.strip()
        # Format: 6-digit code + flag letter(s)
        code = re.match(r'^(\d{6})', token)
        if code:
            codes.append(code.group(1))
    return codes


def find_best_match(company_name, state, sam_by_state):
    """Find best SAM match, searching state bucket first then neighbors."""
    norm = normalize_name(company_name)
    # Names shorter than 3 chars after normalization (e.g., "HP", "3M") are
    # too short for reliable fuzzy matching: they'd match everything
    if not norm or len(norm) < 3:
        return None

    # Search within same state first
    candidates = sam_by_state.get(state.upper(), [])

    if USE_RAPIDFUZZ and candidates:
        names = [c["name_norm"] for c in candidates]
        # Top 3 (vs EDGAR's 5): within a single state the candidate pool
        # is smaller (~700 avg), so fewer candidates suffice
        results = rfprocess.extract(norm, names, scorer=fuzz.token_sort_ratio, limit=3)
        for match_str, score, idx in results:
            if score >= FUZZY_THRESHOLD:
                match = dict(candidates[idx])
                match["_match_score"] = score
                match["_state_matched"] = True
                return match

    elif candidates:
        # difflib fallback
        best_score = 0
        best = None
        for c in candidates:
            ratio = SequenceMatcher(None, norm.lower(), c["name_norm"].lower()).ratio()
            score = int(ratio * 100)
            if score >= FUZZY_THRESHOLD and score > best_score:
                best_score = score
                best = dict(c)
                best["_match_score"] = score
                best["_state_matched"] = True
        if best:
            return best

    # No state match: skip cross-state search (too many false positives
    # with common company names across states)
    return None


def determine_match(sam_entry, reported_naics):
    """Determine match status between SAM NAICS and reported NAICS."""
    primary = sam_entry.get("primary_naics", "")
    naics_codes = parse_naics_list(sam_entry.get("naics_list", ""))

    if not primary and not naics_codes:
        return {
            "suggested_naics": "",
            "suggested_description": "",
            "match_status": "SAM_NO_NAICS",
            "reasoning": f"SAM entity '{sam_entry['name']}' found but no NAICS codes listed",
            "confidence": "low",
        }

    # Exact match on primary
    if primary == reported_naics:
        return {
            "suggested_naics": primary,
            "suggested_description": "",
            "match_status": "SAM_CONFIRMED",
            "reasoning": f"SAM primary NAICS {primary} matches reported code "
                         f"(entity: {sam_entry['name']})",
            "confidence": "high",
        }

    # Reported matches any code in their NAICS list
    if reported_naics in naics_codes:
        return {
            "suggested_naics": reported_naics,
            "suggested_description": "",
            "match_status": "SAM_CONFIRMED",
            "reasoning": f"Reported NAICS {reported_naics} found in SAM NAICS list "
                         f"(primary: {primary}, entity: {sam_entry['name']})",
            "confidence": "high",
        }

    # Same 4-digit sector
    if primary and reported_naics and primary[:4] == reported_naics[:4]:
        return {
            "suggested_naics": primary,
            "suggested_description": "",
            "match_status": "SAM_SUGGESTED",
            "reasoning": f"SAM primary NAICS {primary} same 4-digit sector as "
                         f"reported {reported_naics} (entity: {sam_entry['name']})",
            "confidence": "medium",
        }

    # Different code
    suggested = primary or (naics_codes[0] if naics_codes else "")
    return {
        "suggested_naics": suggested,
        "suggested_description": "",
        "match_status": "SAM_SUGGESTED",
        "reasoning": f"SAM primary NAICS {suggested} differs from "
                     f"reported {reported_naics} (entity: {sam_entry['name']})",
        "confidence": "medium",
    }


# ---------------------------------------------------------------------------
# Target record loading
# ---------------------------------------------------------------------------
def load_target_records():
    """Load records needing SAM lookup (UNCERTAIN/SCRAPE_FAILED not resolved by EDGAR)."""
    if not os.path.exists(SCRAPE_OUTPUT_FILE):
        print(f"ERROR: {SCRAPE_OUTPUT_FILE} not found")
        sys.exit(1)

    targets = []
    with open(SCRAPE_OUTPUT_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            status = row.get("scrape_match_status", "")
            # SAM runs before step13 merge, so scrape_results only contains
            # step10 statuses at this point. EDGAR_* statuses don't exist yet.
            # Process all unresolved records; step13 merge priority handles
            # any overlap with EDGAR results.
            if status in ("UNCERTAIN", "SCRAPE_FAILED"):
                targets.append(row)
    return targets


def run(args):
    """Main execution."""
    print("Loading target records...")
    targets = load_target_records()
    print(f"  Target records: {len(targets)}")

    if not targets:
        print("No records to process.")
        return

    print("Loading SAM extract...")
    sam_by_state = load_sam_extract()

    # Deduplicate by company name + state
    company_groups = {}
    for rec in targets:
        name = rec.get("company_name", "") or rec.get("establishment_name", "")
        state = rec.get("state", "")
        key = f"{normalize_name(name)}|{state.upper().strip()}"
        if key not in company_groups:
            company_groups[key] = {"name": name, "state": state, "records": []}
        company_groups[key]["records"].append(rec)

    print(f"  Unique companies to match: {len(company_groups)}")

    # Match
    t0 = time.time()
    matches = {}
    no_match = []

    for i, (key, group) in enumerate(company_groups.items()):
        match = find_best_match(group["name"], group["state"], sam_by_state)
        if match:
            matches[key] = match
        else:
            no_match.append(key)

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"  Matched {i+1}/{len(company_groups)} "
                  f"({len(matches)} found, {elapsed:.1f}s)")

    elapsed = time.time() - t0
    print(f"\nMatching complete in {elapsed:.1f}s")
    print(f"  Matched: {len(matches)} companies")
    print(f"  No match: {len(no_match)} companies")
    print(f"  Match rate: {len(matches)/len(company_groups)*100:.1f}%")

    # Determine NAICS match quality
    results = []
    status_counts = Counter()

    for key, sam_entry in matches.items():
        group = company_groups[key]
        reported_naics = group["records"][0].get("naics_code", "")
        match_result = determine_match(sam_entry, reported_naics)
        status_counts[match_result["match_status"]] += 1

        for rec in group["records"]:
            result = dict(rec)
            result["sam_uei"] = sam_entry.get("uei", "")
            result["sam_primary_naics"] = sam_entry.get("primary_naics", "")
            result["sam_naics_list"] = sam_entry.get("naics_list", "")
            result["sam_entity_name"] = sam_entry.get("name", "")
            result["sam_match_score"] = str(int(sam_entry.get("_match_score", 0)))

            if match_result["suggested_naics"]:
                result["suggested_naics"] = match_result["suggested_naics"]
                result["suggested_description"] = match_result["suggested_description"]
            result["scrape_match_status"] = match_result["match_status"]
            result["scrape_reasoning"] = match_result["reasoning"]
            result["scrape_confidence"] = match_result["confidence"]
            results.append(result)

    # Add unmatched as SAM_NOT_FOUND
    for key in no_match:
        group = company_groups[key]
        for rec in group["records"]:
            result = dict(rec)
            result["sam_uei"] = ""
            result["sam_primary_naics"] = ""
            result["sam_naics_list"] = ""
            result["sam_entity_name"] = ""
            result["sam_match_score"] = ""
            result["scrape_match_status"] = "SAM_NOT_FOUND"
            result["scrape_reasoning"] = "No match in SAM.gov entity registry"
            results.append(result)

    # Summary
    print(f"\n{'='*60}")
    print(f"SAM BULK MATCH RESULTS")
    print(f"{'='*60}")
    print(f"Total records: {len(results)}")
    matched_records = sum(len(company_groups[k]["records"]) for k in matches)
    print(f"Records with SAM match: {matched_records}")
    for status, count in status_counts.most_common():
        print(f"  {status}: {count} companies")

    if args.dry_run:
        print(f"\nDRY RUN: no files written")
        print(f"\nTop matches by score:")
        sorted_matches = sorted(matches.items(),
                                key=lambda x: x[1].get("_match_score", 0),
                                reverse=True)
        for key, sam in sorted_matches[:25]:
            group = company_groups[key]
            reported = group["records"][0].get("naics_code", "")
            result = determine_match(sam, reported)
            print(f"  {int(sam.get('_match_score',0)):3d}: "
                  f"\"{group['name']}\" ({group['state']}) -> "
                  f"\"{sam['name']}\" "
                  f"[{result['match_status']}] "
                  f"reported={reported} sam={sam.get('primary_naics','')}")

        print(f"\nLowest matches (review for false positives):")
        for key, sam in sorted_matches[-10:]:
            group = company_groups[key]
            print(f"  {int(sam.get('_match_score',0)):3d}: "
                  f"\"{group['name']}\" ({group['state']}) -> "
                  f"\"{sam['name']}\"")
        return

    # Write results
    sam_columns = OUTPUT_COLUMNS + [
        "sam_uei", "sam_primary_naics", "sam_naics_list",
        "sam_entity_name", "sam_match_score",
    ]
    with open(SAM_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sam_columns, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"\nOutput: {SAM_OUTPUT_FILE}")


def main():
    parser = argparse.ArgumentParser(description="SAM.gov bulk extract NAICS matching")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview matches without writing")
    parser.add_argument("--apply", action="store_true",
                        help="Write sam_results.csv")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
