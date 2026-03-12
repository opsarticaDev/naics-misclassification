"""
Step 17: Comprehensive Triage - Auto-Resolve vs Human Review
=============================================================
INPUT:  scrape_results_{YEAR}.csv   (post fan-out, ~43K rows)
        Reference/naics_2017_official.xlsx
        Reference/naics_2022_official.xlsx
        Reference/bls_dart_rates_{YEAR}.xlsx
OUTPUT: triage_report_{YEAR}.csv    (every non-CONFIRMED record with tier)
        triage_human_review_{YEAR}.csv (only records requiring human eyes)
        stdout summary

Methodology
-----------
Each record is classified into the first matching tier (applied sequentially):

  AUTO-RESOLVE tiers:
  T0: ALREADY RESOLVED        - CONFIRMED, SAM_CONFIRMED, EDGAR_CONFIRMED
  T1: SAME CODE               - suggested == reported (scraper confirmed it)
  T2: TRIVIAL CORRECTION      - same 4-digit parent (retired code update)
  T2b: MULTI-ESTAB CONSISTENT - 5+ locations all report same NAICS (company
                                  knows its own code)
  T3: DESC SUPPORTS REPORTED  - industry_description tokens match reported NAICS
                                 tree better than (or equal to) suggested tree
  T4: SAME SECTOR             - same 2-digit NAICS sector (immaterial for safety)
  T5b: NO DESC SIGNAL         - no token overlap with either tree, cross-sector
                                 (insufficient evidence to contradict; includes
                                 empty descriptions and vocabulary gaps)
  T5c: NONCREDIBLE URL        - suggestion derived from dictionary/aggregator
                                 site, not the actual company website
  T5d: LOW CONFIDENCE GUESS   - URL was guessed (not search-verified) and
                                 scraper confidence is low

  NO DATA tier:
  T5: NO ALTERNATIVE          - SCRAPE_FAILED / NO_WEBSITE (no suggestion)

  HUMAN REVIEW tiers:
  T6: CROSS-SECTOR CONTRADICTION - description contradicts reported, supports
                                    suggested, crosses sector boundary
  T7a: INVALID CODE              - reported NAICS not in official 2017/2022 tree
  T7c: WEAK CONTRADICTION        - description slightly favors suggested,
                                    cross-sector, but gap < 1.5x threshold

"""

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from util_pipeline_config import DATASET_YEAR
from util_analysis_config import (load_naics_descriptions, build_desc_tree,
                                  load_dart_rates, lookup_dart, tokenize,
                                  get_sector, sectors_match)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
SCRAPE_RESULTS = os.path.join(PIPELINE_DIR, f"scrape_results_{DATASET_YEAR}.csv")
OUTPUT_FULL = os.path.join(PIPELINE_DIR, f"triage_report_{DATASET_YEAR}.csv")
OUTPUT_REVIEW = os.path.join(PIPELINE_DIR, f"triage_human_review_{DATASET_YEAR}.csv")

# Statuses that are already resolved
RESOLVED_STATUSES = {"CONFIRMED", "SAM_CONFIRMED", "EDGAR_CONFIRMED"}
# Statuses that have suggestions
SUGGESTED_STATUSES = {"SUGGESTED", "UNCERTAIN", "EDGAR_SUGGESTED", "SAM_SUGGESTED"}
# Statuses with no alternative
NO_ALT_STATUSES = {"SCRAPE_FAILED", "NO_WEBSITE"}

def is_valid_naics(code, naics_descs):
    """Check if a code or any of its prefixes exist in the NAICS tree."""
    code = str(code).strip()
    for length in (6, 5, 4, 3, 2):
        if code[:length] in naics_descs:
            return True
    return False


# Non-credible URL domains: these are reference sites, dictionaries, and
# business aggregators: NOT the company's actual website. Suggestions
# derived from scraping these URLs are unreliable.
NONCREDIBLE_DOMAINS = {
    # Dictionaries / encyclopedias
    "merriam-webster.com", "dictionary.com", "britannica.com",
    "wikipedia.org", "wiktionary.org",
    # Business data aggregators (not the actual company)
    "b2bhint.com", "dnb.com", "zoominfo.com", "manta.com",
    "corporationwiki.com", "buzzfile.com", "opencorporates.com",
    "sec.gov", "annualreports.com", "macrotrends.net",
    "companiesmarketcap.com",
    # Job / review sites
    "indeed.com", "glassdoor.com", "ziprecruiter.com",
    # Social media
    "facebook.com", "linkedin.com", "twitter.com", "x.com",
    "instagram.com", "youtube.com",
    # Directories
    "yelp.com", "yellowpages.com", "bbb.org", "mapquest.com",
    "tripadvisor.com",
    # Government
    "osha.gov", "bls.gov", "census.gov",
}


def _extract_domain(url):
    """Extract the registrable domain from a URL."""
    url = url.lower().strip()
    # Remove protocol
    if "://" in url:
        url = url.split("://", 1)[1]
    # Remove path
    domain = url.split("/", 1)[0]
    # Remove port
    domain = domain.split(":", 1)[0]
    return domain


def _is_noncredible_url(url):
    """Check if a URL is from a non-credible domain."""
    domain = _extract_domain(url)
    for nc in NONCREDIBLE_DOMAINS:
        if domain == nc or domain.endswith("." + nc):
            return True
    return False


def desc_scores(desc, reported, suggested, naics_descs):
    """Compute description match scores against reported and suggested NAICS."""
    desc_tokens = tokenize(desc)
    if not desc_tokens:
        return 0.0, 0.0

    reported_tree = build_desc_tree(reported, naics_descs)
    suggested_tree = build_desc_tree(suggested, naics_descs)

    r_tokens = tokenize(reported_tree)
    s_tokens = tokenize(suggested_tree)

    r_score = len(desc_tokens & r_tokens) / len(desc_tokens) if r_tokens else 0.0
    s_score = len(desc_tokens & s_tokens) / len(desc_tokens) if s_tokens else 0.0

    return r_score, s_score


def run(args):
    print("Step 17: Comprehensive Triage")
    print("=" * 60)

    # Load reference data
    print("\nLoading reference data:")
    naics_descs = load_naics_descriptions()
    print(f"  {len(naics_descs)} NAICS descriptions")
    dart_rates = load_dart_rates()
    print(f"  {len(dart_rates)} DART rates")

    # Load scrape results
    print(f"\nLoading: {SCRAPE_RESULTS}")
    with open(SCRAPE_RESULTS, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_fields = list(reader.fieldnames)
        all_rows = list(reader)
    print(f"  {len(all_rows):,} total rows")

    # Pre-compute multi-establishment consistency (T2b)
    # Group by normalized company name → set of reported NAICS codes
    company_naics = defaultdict(set)
    company_count = Counter()
    for row in all_rows:
        cname = (row.get("company_name") or row.get("establishment_name") or "").strip().lower()
        if cname:
            company_naics[cname].add(row.get("naics_code", "").strip())
            company_count[cname] += 1
    # A company is "consistent" if 5+ establishments all report the SAME NAICS
    consistent_companies = {
        name for name, codes in company_naics.items()
        if len(codes) == 1 and company_count[name] >= 5
    }
    print(f"  {len(consistent_companies):,} companies with 5+ consistent establishments")

    # Triage
    tiers = Counter()
    results = []

    for row in all_rows:
        status = row.get("scrape_match_status", "")
        reported = row.get("naics_code", "").strip()
        suggested = row.get("suggested_naics", "").strip()
        desc = row.get("industry_description", "").strip()
        confidence = row.get("scrape_confidence", "").strip().lower()

        tier = None
        reason = ""

        # T0: Already resolved
        if status in RESOLVED_STATUSES:
            tier = "T0_RESOLVED"
            reason = f"Status: {status}"

        # T1: Same code
        elif reported and suggested and reported == suggested:
            tier = "T1_SAME_CODE"
            reason = "Suggested == reported (scraper confirmed)"

        # T2: Trivial correction (same 4-digit parent)
        elif (reported and suggested and len(reported) >= 4 and len(suggested) >= 4
              and reported[:4] == suggested[:4]):
            tier = "T2_TRIVIAL_CORRECTION"
            reason = f"Same 4-digit parent: {reported[:4]}"

        # T2b: Multi-establishment consistency - company has 5+ locations all
        # reporting the same NAICS. The scraper suggested a different code,
        # but consistent self-reporting across many locations is strong evidence
        # the company knows its own industry code.
        elif reported and suggested and reported != suggested:
            cname = (row.get("company_name") or row.get("establishment_name") or "").strip().lower()
            if cname in consistent_companies:
                tier = "T2b_MULTI_ESTABLISHMENT_CONSISTENT"
                reason = (f"{company_count[cname]} locations all report {reported}, "
                          f"consistent self-reporting")
            else:
                tier = None  # Fall through to next checks

        # T5: No alternative available (check before T3/T4 since no suggestion)
        # Split into sub-tiers: truly no data vs. needs human review
        if tier is None and (status in NO_ALT_STATUSES or not suggested):
            url = row.get("website_url", "").strip()
            if status == "UNCERTAIN" or (status == "SCRAPE_FAILED" and url):
                # FM6/FM8: We found the company but couldn't extract signal
                # This is a human judgment call, not a data gap
                if status == "UNCERTAIN":
                    tier = "T5f_NO_NAICS_SIGNAL"
                    reason = f"Website scraped but no NAICS-identifying keywords found"
                else:
                    tier = "T5e_WEBSITE_UNSCRAPEABLE"
                    reason = f"Website found ({url[:50]}) but content extraction failed"
            else:
                tier = "T5_NO_ALTERNATIVE"
                reason = f"Status: {status}, no suggestion to compare"

        # For remaining, compute description scores
        if tier is None and suggested:
            r_score, s_score = 0.0, 0.0
            if desc and suggested:
                r_score, s_score = desc_scores(desc, reported, suggested, naics_descs)

            r_sector = get_sector(reported)
            s_sector = get_sector(suggested)
            same_sector = sectors_match(r_sector, s_sector)
            valid_reported = is_valid_naics(reported, naics_descs)
            has_desc_signal = desc and (r_score > 0 or s_score > 0)
            url = row.get("website_url", "").strip().lower()
            url_source = row.get("url_source", "").strip().lower()

            # T3: Description supports reported (r >= s with actual signal)
            # Establishment's own words validate their code -- defensible
            if has_desc_signal and r_score >= s_score and valid_reported:
                tier = "T3_DESC_SUPPORTS_REPORTED"
                reason = (f"Desc supports reported "
                          f"(r={r_score:.2f} >= s={s_score:.2f})")

            # T4: Same sector, no contradiction
            elif same_sector and valid_reported:
                tier = "T4_SAME_SECTOR"
                reason = (f"Same sector {r_sector} "
                          f"(r={r_score:.2f}, s={s_score:.2f})")

            # T7a: Invalid NAICS code (BEFORE T6 - invalid codes produce
            # empty/wrong description trees that trigger false T6 contradictions)
            elif not valid_reported:
                tier = "T7a_INVALID_CODE"
                reason = f"Reported {reported} not in NAICS tree"

            # T5c: Non-credible URL - reference sites, dictionaries, and
            # business aggregators are not the company's actual website.
            # Suggestions derived from these URLs are unreliable.
            elif url and _is_noncredible_url(url) and not same_sector:
                tier = "T5c_NONCREDIBLE_URL"
                reason = (f"URL {_extract_domain(url)} is not a company website, "
                          f"suggestion unreliable")

            # T5d: Low-confidence guess URL - the scraper guessed the URL
            # (e.g., companyname.com) and the result had low confidence.
            # Not verified as the actual company site.
            elif (url_source == "guess" and confidence == "low"
                  and not same_sector):
                tier = "T5d_LOW_CONFIDENCE_GUESS"
                reason = (f"URL from guess with low confidence, "
                          f"cross-sector {r_sector}->{s_sector}")

            # T6: Cross-sector with description contradiction
            # (includes both strong and weak contradictions: T7c merged in)
            elif not same_sector and s_score > r_score:
                r_dart, _ = lookup_dart(reported, dart_rates)
                s_dart, _ = lookup_dart(suggested, dart_rates)
                dart_info = ""
                if r_dart is not None and s_dart is not None and max(r_dart, s_dart) > 0:
                    dart_info = f", DART ratio={abs(r_dart-s_dart)/max(r_dart,s_dart):.0%}"
                tier = "T6_CROSS_SECTOR_CONTRADICTION"
                reason = (f"Desc contradicts reported (r={r_score:.2f} < s={s_score:.2f}), "
                          f"crosses {r_sector}->{s_sector}{dart_info}")

            # T5b: No description signal (vocab gap), cross-sector
            # Without description evidence, insufficient basis to overturn.
            # Scraper alone not reliable (e.g., Target->Home Health Care).
            elif not has_desc_signal and not same_sector:
                tier = "T5b_NO_DESC_SIGNAL"
                reason = (f"No desc evidence, cross-sector {r_sector}->{s_sector}, "
                          f"conf={confidence}")

            # T8: Everything else (same-sector without desc signal, etc.)
            else:
                tier = "T8_OTHER"
                reason = (f"r={r_score:.2f}, s={s_score:.2f}, "
                          f"{'same' if same_sector else 'cross'}-sector")

        # Fallback for any remaining unclassified
        if tier is None:
            tier = "T8_OTHER"
            reason = "Unclassified"

        tiers[tier] += 1
        out = dict(row)
        out["triage_tier"] = tier
        out["triage_reason"] = reason
        results.append(out)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"TRIAGE SUMMARY")
    print(f"{'=' * 60}")

    auto_resolved = 0
    no_data = 0
    human_review = 0
    uncategorized = 0

    tier_order = ["T0_RESOLVED", "T1_SAME_CODE", "T2_TRIVIAL_CORRECTION",
                  "T2b_MULTI_ESTABLISHMENT_CONSISTENT",
                  "T3_DESC_SUPPORTS_REPORTED", "T4_SAME_SECTOR",
                  "T5_NO_ALTERNATIVE",
                  "T5b_NO_DESC_SIGNAL",
                  "T5c_NONCREDIBLE_URL",
                  "T5d_LOW_CONFIDENCE_GUESS",
                  "T5e_WEBSITE_UNSCRAPEABLE",
                  "T5f_NO_NAICS_SIGNAL",
                  "T6_CROSS_SECTOR_CONTRADICTION",
                  "T7a_INVALID_CODE",
                  "T8_OTHER"]

    auto_resolve_tiers = {"T0_RESOLVED", "T1_SAME_CODE", "T2_TRIVIAL_CORRECTION",
                          "T2b_MULTI_ESTABLISHMENT_CONSISTENT",
                          "T3_DESC_SUPPORTS_REPORTED", "T4_SAME_SECTOR",
                          "T5b_NO_DESC_SIGNAL", "T5c_NONCREDIBLE_URL",
                          "T5d_LOW_CONFIDENCE_GUESS"}
    no_data_tiers = {"T5_NO_ALTERNATIVE"}
    uncategorized_tiers = {"T8_OTHER"}

    for tier in tier_order:
        n = tiers.get(tier, 0)
        if tier in auto_resolve_tiers:
            label = "AUTO-RESOLVE"
            auto_resolved += n
        elif tier in no_data_tiers:
            label = "NO DATA"
            no_data += n
        elif tier in uncategorized_tiers:
            label = "UNCATEGORIZED"
            uncategorized += n
        else:
            label = "HUMAN REVIEW"
            human_review += n
        print(f"  {tier:40s}  {n:>7,}  [{label}]")

    print(f"\n  AUTO-RESOLVED:  {auto_resolved:>7,}")
    print(f"  NO DATA:        {no_data:>7,}")
    print(f"  UNCATEGORIZED:  {uncategorized:>7,}")
    print(f"  HUMAN REVIEW:   {human_review:>7,}")
    print(f"  TOTAL:          {len(results):>7,}")

    # Breakdown of human review tiers
    print(f"\n  HUMAN REVIEW BREAKDOWN:")
    for tier in ("T5e_WEBSITE_UNSCRAPEABLE", "T5f_NO_NAICS_SIGNAL",
                 "T6_CROSS_SECTOR_CONTRADICTION", "T7a_INVALID_CODE"):
        n = tiers.get(tier, 0)
        print(f"    {tier}: {n:,}")

    # T6 top patterns
    t6 = [r for r in results if r["triage_tier"] == "T6_CROSS_SECTOR_CONTRADICTION"]
    if t6:
        print(f"\n  T6 (Strongest Evidence) Sample:")
        for r in t6[:10]:
            print(f"    {r.get('company_name','')[:30]:30s} "
                  f"{r.get('naics_code',''):>6s} -> {r.get('suggested_naics',''):>6s} "
                  f"\"{r.get('industry_description','')[:40]}\"")

    if args.dry_run:
        print(f"\nDRY RUN -- no files written")
        return

    # Write full triage report
    out_fields = input_fields + ["triage_tier", "triage_reason"]
    with open(OUTPUT_FULL, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    print(f"\nWritten: {OUTPUT_FULL} ({len(results):,} rows)")

    # Write human review subset
    review_rows = [r for r in results
                   if r["triage_tier"] in ("T5e_WEBSITE_UNSCRAPEABLE",
                                           "T5f_NO_NAICS_SIGNAL",
                                           "T6_CROSS_SECTOR_CONTRADICTION",
                                           "T7a_INVALID_CODE")]
    with open(OUTPUT_REVIEW, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(review_rows)
    print(f"Written: {OUTPUT_REVIEW} ({len(review_rows):,} rows)")


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive triage: auto-resolve vs human review"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview triage without writing output")
    parser.add_argument("--apply", action="store_true",
                        help="Write triage output files")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        print("\nSpecify --dry-run or --apply")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
