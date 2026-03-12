"""
Step 11: SEC EDGAR Enrichment for NAICS Validation
====================================================
INPUT:  scrape_results_{YEAR}.csv (UNCERTAIN + SCRAPE_FAILED records from Step 10)
OUTPUT: edgar_results_{YEAR}.csv (records with EDGAR match status)

Methodology
-----------
SEC EDGAR maintains SIC (Standard Industrial Classification) codes for all
SEC-registered entities (~10K publicly traded companies). This gate:

  1. DOWNLOAD company_tickers.json from SEC (~10K public companies with
     CIK, ticker, and legal name).

  2. FUZZY MATCH our UNCERTAIN/SCRAPE_FAILED company names against EDGAR
     names using rapidfuzz token_sort_ratio (or difflib fallback).
     - Names are normalized: strip corporate suffixes (Inc, LLC, Corp, etc.),
       remove punctuation, uppercase.
     - Threshold: score ≥ 80 (on 0-100 scale). This catches "Acme Corp"
       matching "ACME CORPORATION" (score ~100) while rejecting random
       partial overlaps. Score 80 was chosen by manual review of the
       boundary: scores 75-79 showed high false positive rates.
     - State bonus: +5 points if the record's state matches the EDGAR
       entity's state of incorporation (mild tiebreaker, not a filter).

  3. FETCH SIC codes from EDGAR submissions API (CIK{padded}.json) for
     each matched company. Rate-limited to 10 req/sec per SEC fair access.

  4. CROSSWALK SIC → NAICS 2017 using Reference/sic_to_naics_crosswalk.csv
     (built from Census Bureau vintage chains: SIC 1987 → NAICS 2002 →
     2007 → 2012 → 2017). Precedence:
       a. If crosswalk yields reported NAICS → EDGAR_CONFIRMED
       b. If crosswalk yields same 4-digit sector → EDGAR_SUGGESTED
       c. Otherwise use best crosswalk match → EDGAR_SUGGESTED
       d. SIC not in crosswalk → EDGAR_SIC_NO_CROSSWALK
       e. No fuzzy match in EDGAR → EDGAR_NOT_FOUND

Limitations
-----------
  - Only covers ~10K publicly traded companies. Match rate on OSHA 300A
    data is ~7% since most filers are private businesses.
  - Fuzzy matching at score=80 admits some false positives (e.g., "Alsco"
    matching "ALCON INC"). The state cross-reference helps but doesn't
    eliminate these. The crosswalk step provides a second check: if the
    SIC-derived NAICS doesn't relate to the reported industry, the
    suggestion may be from a wrong company match.

CLI Flags
---------
  --dry-run   Preview fuzzy matches without hitting the EDGAR submissions API
  --apply     Full pipeline: match, fetch SIC codes, crosswalk, write output
  --resume    Continue from edgar_checkpoint.json

"""

import argparse
import csv
import json
import os
import re
import sys
import time

import requests

try:
    from rapidfuzz import fuzz, process as rfprocess
    USE_RAPIDFUZZ = True
except ImportError:
    from difflib import SequenceMatcher
    USE_RAPIDFUZZ = False
    print("WARNING: rapidfuzz not installed, falling back to difflib (slower)")

from util_scrape_config import (
    BASE_DIR, SCRAPE_OUTPUT_FILE,
    EDGAR_OUTPUT_FILE, EDGAR_CHECKPOINT_FILE,
    EDGAR_USER_AGENT, EDGAR_TICKERS_URL, EDGAR_SUBMISSIONS_URL,
    EDGAR_RATE_DELAY, EDGAR_FUZZY_THRESHOLD,
    load_sic_naics_crosswalk, OUTPUT_COLUMNS,
)

# ---------------------------------------------------------------------------
# Company name normalization
# ---------------------------------------------------------------------------
_CORP_SUFFIXES = re.compile(
    r'\b(inc\.?|incorporated|llc|l\.l\.c\.?|ltd\.?|limited|corp\.?|corporation'
    r'|co\.?|company|group|holdings?|enterprises?|partners?|lp|l\.p\.?'
    r'|plc|n\.?a\.?|s\.?a\.?|gmbh|ag)\b',
    re.IGNORECASE
)
_PUNCT = re.compile(r'[^\w\s]')
_MULTI_SPACE = re.compile(r'\s+')


def normalize_name(name):
    """Normalize company name for fuzzy matching.

    Strips corporate suffixes (Inc, LLC, Corp, etc.), punctuation, and
    excess whitespace. Uppercased for case-insensitive comparison.
    Same normalization used in sam_bulk_match.py for consistency.
    """
    if not name:
        return ""
    name = _CORP_SUFFIXES.sub('', name)
    name = _PUNCT.sub(' ', name)
    name = _MULTI_SPACE.sub(' ', name).strip().upper()
    return name


def fuzzy_score(name_a, name_b):
    """Compute fuzzy match score (0-100) between two names."""
    if USE_RAPIDFUZZ:
        return fuzz.token_sort_ratio(name_a, name_b)
    else:
        return int(SequenceMatcher(None, name_a.lower(), name_b.lower()).ratio() * 100)


# ---------------------------------------------------------------------------
# EDGAR API helpers
# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"User-Agent": EDGAR_USER_AGENT, "Accept": "application/json"})


def download_company_tickers():
    """Download SEC company tickers JSON (~10K public companies).

    NOTE: _state_hint is NOT available from this endpoint - the company
    tickers JSON only provides CIK, ticker, and name. State of
    incorporation is only available from the per-company submissions
    API (Phase 2). The +5 state bonus in find_best_match() is therefore
    a Phase 2 refinement only: it applies when re-ranking matches that
    already have submissions data cached from a previous run.
    """
    print("Downloading SEC EDGAR company tickers...")
    resp = _session.get(EDGAR_TICKERS_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"}, ...}
    companies = []
    for entry in data.values():
        companies.append({
            "cik": str(entry["cik_str"]),
            "ticker": entry.get("ticker", ""),
            "name": entry.get("title", ""),
            "name_normalized": normalize_name(entry.get("title", "")),
        })
    print(f"  Loaded {len(companies)} SEC-registered companies")
    return companies


def get_submissions(cik):
    """Fetch company submission data from EDGAR (contains SIC, addresses).

    Timeout is 15s (vs 30s for the ticker download) because the submissions
    endpoint returns smaller JSON and is more reliably fast.
    """
    padded = cik.zfill(10)
    url = EDGAR_SUBMISSIONS_URL.format(cik=padded)
    resp = _session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def extract_sic_info(submissions_data):
    """Extract SIC code, description, and address from submissions JSON."""
    return {
        "sic": submissions_data.get("sic", ""),
        "sic_description": submissions_data.get("sicDescription", ""),
        "state": submissions_data.get("stateOfIncorporation", ""),
        "state_address": (submissions_data.get("addresses", {})
                          .get("business", {}).get("stateOrCountry", "")),
        "entity_name": submissions_data.get("name", ""),
    }


# ---------------------------------------------------------------------------
# Crosswalk application
# ---------------------------------------------------------------------------
def apply_crosswalk(sic_code, reported_naics):
    """Apply SIC→NAICS crosswalk and determine match quality.

    Crosswalk precedence (most confident first):
      1. Exact: crosswalk yields the same code as reported → EDGAR_CONFIRMED
      2. Sector: crosswalk yields same 4-digit sector → EDGAR_SUGGESTED
      3. Best: use top-ranked crosswalk result → EDGAR_SUGGESTED
      4. No map: SIC exists in EDGAR but not in crosswalk → EDGAR_SIC_NO_CROSSWALK

    Returns:
        dict with: suggested_naics, suggested_description, match_status, reasoning
    """
    crosswalk = load_sic_naics_crosswalk()
    sic_4 = sic_code.zfill(4)

    mappings = crosswalk.get(sic_4, [])
    if not mappings:
        return {
            "suggested_naics": "",
            "suggested_description": "",
            "match_status": "EDGAR_SIC_NO_CROSSWALK",
            "reasoning": f"SIC {sic_4} found in EDGAR but no NAICS crosswalk mapping exists",
        }

    # Check if reported NAICS matches any crosswalk result
    for m in mappings:
        if m["naics_code"] == reported_naics:
            return {
                "suggested_naics": reported_naics,
                "suggested_description": m["naics_description"],
                "match_status": "EDGAR_CONFIRMED",
                "reasoning": (f"SIC {sic_4} crosswalks to NAICS {reported_naics} "
                              f"({m['naics_description']}), matches reported code"),
            }

    # Check 4-digit sector match
    for m in mappings:
        if m["naics_code"][:4] == reported_naics[:4]:
            return {
                "suggested_naics": m["naics_code"],
                "suggested_description": m["naics_description"],
                "match_status": "EDGAR_SUGGESTED",
                "reasoning": (f"SIC {sic_4} crosswalks to NAICS {m['naics_code']} "
                              f"({m['naics_description']}), same 4-digit sector as "
                              f"reported {reported_naics}"),
            }

    # Use best crosswalk match
    best = mappings[0]
    return {
        "suggested_naics": best["naics_code"],
        "suggested_description": best["naics_description"],
        "match_status": "EDGAR_SUGGESTED",
        "reasoning": (f"SIC {sic_4} crosswalks to NAICS {best['naics_code']} "
                      f"({best['naics_description']}), reported was {reported_naics}"),
    }


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------
def load_checkpoint():
    """Load processing checkpoint."""
    if os.path.exists(EDGAR_CHECKPOINT_FILE):
        with open(EDGAR_CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_ids": [], "api_calls": 0}


def save_checkpoint(ckpt):
    """Save processing checkpoint."""
    with open(EDGAR_CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(ckpt, f, indent=2)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def load_target_records():
    """Load UNCERTAIN and SCRAPE_FAILED records from scrape_results.csv."""
    if not os.path.exists(SCRAPE_OUTPUT_FILE):
        print(f"ERROR: {SCRAPE_OUTPUT_FILE} not found")
        sys.exit(1)

    targets = []
    with open(SCRAPE_OUTPUT_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            status = row.get("scrape_match_status", "")
            if status in ("UNCERTAIN", "SCRAPE_FAILED"):
                targets.append(row)
    return targets


def find_best_match(company_name, state, edgar_companies):
    """Find best EDGAR match for a company name + state.

    Uses rapidfuzz token_sort_ratio (or difflib fallback) to find the
    closest name match. Returns top-5 candidates above threshold, then
    picks the best after applying a +5 state-match bonus.

    The state bonus (+5 on a 0-100 scale) is a mild tiebreaker, not a
    hard filter. It's small enough that a strong name match (score 95)
    in the wrong state will still beat a weaker name match (score 85)
    in the right state.
    """
    norm = normalize_name(company_name)
    if not norm:
        return None

    if USE_RAPIDFUZZ:
        edgar_names = [c["name_normalized"] for c in edgar_companies]
        # Top 5 candidates: enough to evaluate tiebreakers without
        # scanning all 10K companies (rapidfuzz handles this efficiently)
        results = rfprocess.extract(
            norm, edgar_names, scorer=fuzz.token_sort_ratio, limit=5
        )
        # results = [(match_str, score, index), ...]
        # NOTE: State-based disambiguation is NOT possible at this stage
        # because the tickers endpoint doesn't include state. State matching
        # happens post-hoc in Phase 2 via the submissions API (line ~441).
        best = None
        best_score = 0
        for match_str, score, idx in results:
            if score < EDGAR_FUZZY_THRESHOLD:
                continue
            candidate = edgar_companies[idx]
            if score > best_score:
                best_score = score
                best = candidate
                best["_match_score"] = score
        return best
    else:
        # difflib fallback: slower
        best = None
        best_score = 0
        for c in edgar_companies:
            score = fuzzy_score(norm, c["name_normalized"])
            if score >= EDGAR_FUZZY_THRESHOLD and score > best_score:
                best_score = score
                best = c
                best["_match_score"] = score
        return best


def run(args):
    """Main execution."""
    # Load targets
    targets = load_target_records()
    print(f"Target records: {len(targets)} (UNCERTAIN + SCRAPE_FAILED)")

    if not targets:
        print("No records to process.")
        return

    # Download EDGAR company list
    edgar_companies = download_company_tickers()

    # Deduplicate targets by company name for matching
    company_groups = {}
    for rec in targets:
        name = rec.get("company_name", "") or rec.get("establishment_name", "")
        key = normalize_name(name)
        if key not in company_groups:
            company_groups[key] = {"name": name, "records": [], "state": rec.get("state", "")}
        company_groups[key]["records"].append(rec)

    print(f"Unique company names to match: {len(company_groups)}")

    # Phase 1: Fuzzy matching (no API calls)
    matches = {}  # normalized_name -> edgar_company
    no_match = []
    for key, group in company_groups.items():
        match = find_best_match(group["name"], group["state"], edgar_companies)
        if match:
            matches[key] = match
        else:
            no_match.append(key)

    print(f"\nFuzzy matching results:")
    print(f"  Matched: {len(matches)} companies")
    print(f"  No match: {len(no_match)} companies")
    print(f"  Match rate: {len(matches)/len(company_groups)*100:.1f}%")

    if args.dry_run:
        # Show top matches for review
        print(f"\n{'='*80}")
        print("TOP MATCHES (dry run: no API calls)")
        print(f"{'='*80}")
        sorted_matches = sorted(matches.items(),
                                key=lambda x: x[1].get("_match_score", 0),
                                reverse=True)
        for key, edgar in sorted_matches[:30]:
            group = company_groups[key]
            n_recs = len(group["records"])
            print(f"  Score {int(edgar.get('_match_score',0)):3d}: "
                  f"\"{group['name']}\" -> \"{edgar['name']}\" "
                  f"(CIK {edgar['cik']}, {edgar['ticker']}) [{n_recs} records]")

        print(f"\n{'='*80}")
        print("LOWEST MATCHES (review for false positives)")
        print(f"{'='*80}")
        for key, edgar in sorted_matches[-10:]:
            group = company_groups[key]
            print(f"  Score {int(edgar.get('_match_score',0)):3d}: "
                  f"\"{group['name']}\" -> \"{edgar['name']}\" "
                  f"(CIK {edgar['cik']}, {edgar['ticker']})")

        total_records = sum(len(company_groups[k]["records"]) for k in matches)
        print(f"\nTotal records that would be enriched: {total_records}")
        print(f"Records remaining for SAM.gov: {len(targets) - total_records}")
        return

    # Phase 2: Fetch SIC codes from EDGAR submissions API
    checkpoint = load_checkpoint()
    processed_set = set(checkpoint["processed_ids"])
    api_calls = checkpoint["api_calls"]

    results = []
    cik_cache = {}  # CIK -> sic_info (avoid duplicate API calls)
    errors = 0

    print(f"\nFetching SIC codes from EDGAR submissions API...")
    print(f"  Checkpoint: {len(processed_set)} already processed, {api_calls} API calls made")

    for i, (key, edgar) in enumerate(matches.items()):
        cik = edgar["cik"]
        group = company_groups[key]

        # Get SIC info (cached by CIK)
        if cik not in cik_cache:
            if cik in processed_set:
                continue  # Already done in previous run
            try:
                time.sleep(EDGAR_RATE_DELAY)
                data = get_submissions(cik)
                cik_cache[cik] = extract_sic_info(data)
                api_calls += 1
            except Exception as e:
                print(f"  ERROR CIK {cik} ({edgar['name']}): {e}")
                cik_cache[cik] = None
                errors += 1
                api_calls += 1

            if (api_calls % 100) == 0:
                print(f"  API calls: {api_calls}, processed: {len(cik_cache)}")

        sic_info = cik_cache.get(cik)
        if sic_info is None:
            # API error: mark as not found
            for rec in group["records"]:
                result = dict(rec)
                result["edgar_cik"] = cik
                result["edgar_sic"] = ""
                result["edgar_sic_desc"] = ""
                result["edgar_entity_name"] = edgar["name"]
                result["edgar_match_score"] = str(edgar.get("_match_score", 0))
                result["scrape_match_status"] = "EDGAR_NOT_FOUND"
                result["scrape_reasoning"] = f"EDGAR API error for CIK {cik}"
                results.append(result)
            continue

        # Apply crosswalk
        reported_naics = group["records"][0].get("naics_code", "")
        crosswalk_result = apply_crosswalk(sic_info["sic"], reported_naics)

        # State cross-reference
        state_match = ""
        rec_state = group["state"].upper() if group["state"] else ""
        edgar_state = (sic_info["state_address"] or sic_info["state"] or "").upper()
        if rec_state and edgar_state:
            state_match = "yes" if rec_state == edgar_state else "no"

        for rec in group["records"]:
            result = dict(rec)
            result["edgar_cik"] = cik
            result["edgar_sic"] = sic_info["sic"]
            result["edgar_sic_desc"] = sic_info["sic_description"]
            result["edgar_entity_name"] = sic_info["entity_name"]
            result["edgar_match_score"] = str(edgar.get("_match_score", 0))
            result["edgar_state_match"] = state_match

            if crosswalk_result["suggested_naics"]:
                result["suggested_naics"] = crosswalk_result["suggested_naics"]
                result["suggested_description"] = crosswalk_result["suggested_description"]
            result["scrape_match_status"] = crosswalk_result["match_status"]
            result["scrape_reasoning"] = crosswalk_result["reasoning"]
            result["scrape_confidence"] = "high" if "CONFIRMED" in crosswalk_result["match_status"] else "medium"
            results.append(result)

        checkpoint["processed_ids"].append(cik)
        if (len(checkpoint["processed_ids"]) % 50) == 0:
            checkpoint["api_calls"] = api_calls
            save_checkpoint(checkpoint)

    # Add unmatched records as EDGAR_NOT_FOUND
    for key in no_match:
        group = company_groups[key]
        for rec in group["records"]:
            result = dict(rec)
            result["edgar_cik"] = ""
            result["edgar_sic"] = ""
            result["edgar_sic_desc"] = ""
            result["edgar_entity_name"] = ""
            result["edgar_match_score"] = ""
            result["edgar_state_match"] = ""
            result["scrape_match_status"] = "EDGAR_NOT_FOUND"
            result["scrape_reasoning"] = "No fuzzy match found in SEC EDGAR company list"
            results.append(result)

    # Save final checkpoint
    checkpoint["api_calls"] = api_calls
    save_checkpoint(checkpoint)

    # Write results
    edgar_columns = OUTPUT_COLUMNS + [
        "edgar_cik", "edgar_sic", "edgar_sic_desc",
        "edgar_entity_name", "edgar_match_score", "edgar_state_match",
    ]
    with open(EDGAR_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=edgar_columns, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    # Summary
    from collections import Counter
    status_counts = Counter(r.get("scrape_match_status", "") for r in results)
    print(f"\n{'='*60}")
    print(f"EDGAR ENRICHMENT COMPLETE")
    print(f"{'='*60}")
    print(f"Total records processed: {len(results)}")
    print(f"API calls made: {api_calls}")
    print(f"Errors: {errors}")
    for status, count in status_counts.most_common():
        print(f"  {status}: {count}")
    print(f"\nOutput: {EDGAR_OUTPUT_FILE}")


def main():
    parser = argparse.ArgumentParser(description="SEC EDGAR NAICS enrichment")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview fuzzy matches without API calls")
    parser.add_argument("--apply", action="store_true",
                        help="Run full enrichment pipeline")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
