"""
Batch Validator for Web Scraping Pipeline
==========================================
INPUT:  scrape_results_{YEAR}.csv (records from Step 10)
OUTPUT: stdout (pass/fail report)

Rule-based QA: validates scrape results against NAICS reference,
checks for completeness, flags suspicious records.

Usage:
    python Scripts/util_validate_batch.py
    python Scripts/util_validate_batch.py --batch-size 1000   # validate last N records
    python Scripts/util_validate_batch.py --full               # validate everything

Exit codes:
    0 = pass (failure rate below threshold)
    1 = fail (failure rate above threshold or missing data)
"""

import argparse
import csv
import json
import os
import sys

from util_scrape_config import (
    BASE_DIR, SCRAPE_OUTPUT_FILE, CHECKPOINT_FILE,
    build_naics_keyword_index,
)

# Thresholds
MAX_FAILURE_RATE = 0.10       # >10% SCRAPE_FAILED = flag
MIN_ACTIONABLE_RATE = 0.25    # <25% (CONFIRMED+SUGGESTED) = flag
MAX_EMPTY_SUGGESTED = 0.50    # >50% empty suggested_naics (when URL found) = flag


def load_results():
    """Load scrape_results.csv."""
    if not os.path.exists(SCRAPE_OUTPUT_FILE):
        print(f"ERROR: {SCRAPE_OUTPUT_FILE} not found")
        sys.exit(1)
    with open(SCRAPE_OUTPUT_FILE, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_checkpoint():
    """Load checkpoint for batch boundary info."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def validate_naics_code(code, valid_codes):
    """Check if a 6-digit NAICS code exists in reference."""
    if not code or not code.strip():
        return "empty"
    code = code.strip()
    if not code.isdigit():
        return "non_numeric"
    if len(code) != 6:
        return f"wrong_length_{len(code)}"
    if code in valid_codes:
        return "valid"
    return "not_in_reference"


def validate_batch(results, valid_codes, label=""):
    """Validate a set of results. Returns (passed, report_lines)."""
    if not results:
        return True, ["  No records to validate."]

    lines = []
    if label:
        lines.append(f"\n{'='*60}")
        lines.append(f"  {label}")
        lines.append(f"{'='*60}")

    total = len(results)
    lines.append(f"  Records: {total}")

    # --- Status distribution ---
    status_counts = {}
    for r in results:
        s = r.get("scrape_match_status", "UNKNOWN")
        status_counts[s] = status_counts.get(s, 0) + 1

    lines.append(f"\n  Status distribution:")
    for status in ["CONFIRMED", "SUGGESTED", "UNCERTAIN", "NO_WEBSITE", "SCRAPE_FAILED"]:
        count = status_counts.get(status, 0)
        pct = count / total * 100
        flag = ""
        if status == "SCRAPE_FAILED" and pct > MAX_FAILURE_RATE * 100:
            flag = " *** HIGH"
        lines.append(f"    {status:15s} {count:5d} ({pct:5.1f}%){flag}")

    # Unknown statuses
    known = {"CONFIRMED", "SUGGESTED", "UNCERTAIN", "NO_WEBSITE", "SCRAPE_FAILED"}
    for s, c in sorted(status_counts.items()):
        if s not in known:
            lines.append(f"    {s:15s} {c:5d} ({c/total*100:5.1f}%) *** UNEXPECTED")

    # --- Actionable rate ---
    confirmed = status_counts.get("CONFIRMED", 0)
    suggested = status_counts.get("SUGGESTED", 0)
    actionable = confirmed + suggested
    actionable_rate = actionable / total if total else 0
    lines.append(f"\n  Actionable (CONFIRMED+SUGGESTED): {actionable}/{total} ({actionable_rate*100:.1f}%)")

    # --- Validate suggested NAICS codes ---
    has_url = [r for r in results if r.get("website_url")]
    bad_suggestions = []
    empty_suggestions = 0

    for r in results:
        suggested_code = r.get("suggested_naics", "").strip()
        if not suggested_code:
            if r.get("website_url"):
                empty_suggestions += 1
            continue
        validity = validate_naics_code(suggested_code, valid_codes)
        if validity not in ("valid", "empty"):
            bad_suggestions.append({
                "id": r.get("id", "?"),
                "company": r.get("company_name", r.get("establishment_name", "?")),
                "suggested": suggested_code,
                "issue": validity,
            })

    if bad_suggestions:
        lines.append(f"\n  Invalid suggested NAICS codes: {len(bad_suggestions)}")
        for b in bad_suggestions[:10]:
            lines.append(f"    ID {b['id']}: {b['company'][:30]} -> {b['suggested']} ({b['issue']})")
        if len(bad_suggestions) > 10:
            lines.append(f"    ... and {len(bad_suggestions) - 10} more")
    else:
        lines.append(f"\n  Suggested NAICS codes: all valid")

    if has_url:
        empty_rate = empty_suggestions / len(has_url)
        flag = " *** HIGH" if empty_rate > MAX_EMPTY_SUGGESTED else ""
        lines.append(f"  Empty suggestions (with URL): {empty_suggestions}/{len(has_url)} ({empty_rate*100:.1f}%){flag}")

    # --- Confidence distribution ---
    conf_counts = {}
    for r in results:
        c = r.get("scrape_confidence", "") or "none"
        conf_counts[c] = conf_counts.get(c, 0) + 1
    lines.append(f"\n  Confidence distribution:")
    for c in ["high", "medium", "low", "none"]:
        count = conf_counts.get(c, 0)
        lines.append(f"    {c:8s} {count:5d} ({count/total*100:5.1f}%)")

    # --- URL discovery ---
    url_found = len(has_url)
    no_url = total - url_found
    lines.append(f"\n  URL discovery: {url_found}/{total} ({url_found/total*100:.1f}%)")
    url_sources = {}
    for r in has_url:
        src = r.get("url_source", "unknown")
        url_sources[src] = url_sources.get(src, 0) + 1
    for src, count in sorted(url_sources.items()):
        lines.append(f"    {src}: {count}")

    # --- Pass/fail decision ---
    failed = status_counts.get("SCRAPE_FAILED", 0)
    failure_rate = failed / total if total else 0

    passed = True
    issues = []

    if failure_rate > MAX_FAILURE_RATE:
        issues.append(f"Failure rate {failure_rate*100:.1f}% exceeds {MAX_FAILURE_RATE*100:.0f}% threshold")
        passed = False

    if actionable_rate < MIN_ACTIONABLE_RATE:
        issues.append(f"Actionable rate {actionable_rate*100:.1f}% below {MIN_ACTIONABLE_RATE*100:.0f}% threshold")
        passed = False

    if bad_suggestions:
        issues.append(f"{len(bad_suggestions)} invalid suggested NAICS codes")
        passed = False

    lines.append(f"\n  {'='*40}")
    if passed:
        lines.append(f"  BATCH PASSED")
    else:
        lines.append(f"  BATCH FLAGGED: issues found:")
        for issue in issues:
            lines.append(f"    - {issue}")

    return passed, lines


def main():
    parser = argparse.ArgumentParser(description="Validate web scraping batch results")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="Validate last N records as current batch (default: 1000)")
    parser.add_argument("--full", action="store_true",
                        help="Validate all records, not just latest batch")
    args = parser.parse_args()

    print("=" * 60)
    print("BATCH VALIDATION REPORT")
    print("=" * 60)

    # Build NAICS reference
    print("Loading NAICS reference...")
    naics_index = build_naics_keyword_index()
    valid_codes = set(naics_index.keys())
    print(f"  {len(valid_codes)} valid 6-digit codes loaded")

    # Load results
    results = load_results()
    print(f"  {len(results)} total records in {SCRAPE_OUTPUT_FILE}")

    ckpt = load_checkpoint()
    if ckpt:
        print(f"  Checkpoint: {ckpt['total_processed']} processed, "
              f"last update: {ckpt.get('timestamp', '?')}")

    overall_passed = True

    # Validate latest batch
    if not args.full and len(results) > args.batch_size:
        batch = results[-args.batch_size:]
        passed, lines = validate_batch(batch, valid_codes,
                                        f"LATEST BATCH (last {args.batch_size} records)")
        for line in lines:
            print(line)
        if not passed:
            overall_passed = False

    # Validate full dataset
    passed, lines = validate_batch(results, valid_codes,
                                    "FULL DATASET" if not args.full else "ALL RECORDS")
    for line in lines:
        print(line)
    if not passed:
        overall_passed = False

    print(f"\n{'='*60}")
    if overall_passed:
        print("VALIDATION: PASSED")
    else:
        print("VALIDATION: FLAGGED - review issues above")
    print(f"{'='*60}")

    sys.exit(0 if overall_passed else 1)


if __name__ == "__main__":
    main()
