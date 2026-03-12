"""
Web Scraping Pipeline: Configuration
=======================================
Paths, thresholds, NAICS keyword index, and shared utilities
for the web scraping pipeline.
"""

import csv
import os
import re
import openpyxl

from util_pipeline_config import DATASET_YEAR

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
UNCERTAIN_FILE = os.path.join(PIPELINE_DIR, f"uncertain_for_websearch_{DATASET_YEAR}.csv")
SCRAPE_OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"scrape_results_{DATASET_YEAR}.csv")
CHECKPOINT_FILE = os.path.join(PIPELINE_DIR, f"scrape_checkpoint_{DATASET_YEAR}.json")
NAICS_2017_FILE = os.path.join(BASE_DIR, "Reference", "naics_2017_official.xlsx")
NAICS_2022_FILE = os.path.join(BASE_DIR, "Reference", "naics_2022_official.xlsx")
SIC_NAICS_CROSSWALK_FILE = os.path.join(BASE_DIR, "Reference", "sic_to_naics_crosswalk.csv")
EDGAR_OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"edgar_results_{DATASET_YEAR}.csv")
EDGAR_CHECKPOINT_FILE = os.path.join(PIPELINE_DIR, f"edgar_checkpoint_{DATASET_YEAR}.json")
SAM_OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"sam_results_{DATASET_YEAR}.csv")
SAM_CHECKPOINT_FILE = os.path.join(PIPELINE_DIR, f"sam_checkpoint_{DATASET_YEAR}.json")

# ---------------------------------------------------------------------------
# Brave Search API (Tier 2 fallback when algorithmic guess fails)
# ---------------------------------------------------------------------------
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
BRAVE_BUDGET_PER_RUN = 2000       # Max Brave API calls per run

# ---------------------------------------------------------------------------
# SearXNG (self-hosted, free alternative to Brave)
# ---------------------------------------------------------------------------
SEARXNG_ENDPOINT = os.environ.get("SEARXNG_ENDPOINT", "http://localhost:8888/search")
SEARXNG_ENGINES = "google,bing,duckduckgo"
SEARXNG_BUDGET_PER_RUN = 5000    # Soft safety limit (free, but do not abuse upstream)
SEARXNG_ENABLED = True

# ---------------------------------------------------------------------------
# Search Backend Selection
# ---------------------------------------------------------------------------
# "searxng" = self-hosted, free, unlimited (requires SearXNG instance)
# "brave"   = paid API, 2000 call budget per run
SEARCH_BACKEND = "searxng" if SEARXNG_ENABLED else "brave"

# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------
SEARCH_DELAY = 2.0 if SEARXNG_ENABLED else 1.2  # SearXNG needs more delay for upstream engines
SCRAPE_TIMEOUT = 10               # Seconds before giving up on a page
SCRAPE_SAME_DOMAIN_DELAY = 0.5    # Seconds between requests to same domain
MAX_PAGES_PER_SITE = 3            # Homepage + /about + /services
URL_GUESS_TIMEOUT = 5             # Seconds for HEAD request on guessed URL

# ---------------------------------------------------------------------------
# Scoring Thresholds
# ---------------------------------------------------------------------------
CONFIRM_MIN_SCORE = 0.4           # Reported NAICS must score at least this
CONFIRM_RATIO = 0.80              # Reported must be >= 80% of top candidate
SUGGEST_MIN_SCORE = 0.4           # Suggested NAICS must score at least this
SUGGEST_RATIO = 1.5               # Top must be > 1.5x reported to suggest

# ---------------------------------------------------------------------------
# URL Filtering: domains to skip in search results
# ---------------------------------------------------------------------------
SKIP_DOMAINS = {
    # Social media
    "linkedin.com", "facebook.com", "twitter.com", "x.com",
    "instagram.com", "youtube.com", "tiktok.com", "pinterest.com",
    # Directories / review sites
    "yelp.com", "yellowpages.com", "bbb.org", "glassdoor.com",
    "indeed.com", "ziprecruiter.com", "mapquest.com",
    "tripadvisor.com", "nextdoor.com", "waze.com", "groupon.com",
    # Business data aggregators (not the actual company site)
    "manta.com", "dnb.com", "zoominfo.com", "bloomberg.com",
    "crunchbase.com", "corporationwiki.com",
    "importgenius.com", "panjiva.com", "bizjournals.com",
    "chamberofcommerce.com",
    "rocketreach.co", "rocketreach.com",
    "govtribe.com",
    "intellispect.co",
    # Reference / government
    "wikipedia.org", "reddit.com",
    "osha.gov", "sec.gov", "sos.state",
}

# ---------------------------------------------------------------------------
# Stopwords for keyword density scoring
# ---------------------------------------------------------------------------
STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "need", "must",
    "it", "its", "this", "that", "these", "those", "we", "our", "us",
    "they", "their", "them", "he", "she", "his", "her", "you", "your",
    "i", "me", "my", "not", "no", "all", "any", "each", "every",
    "more", "most", "other", "some", "such", "than", "too", "very",
    "just", "also", "about", "up", "out", "so", "if", "as", "into",
    "what", "which", "who", "whom", "how", "when", "where", "why",
    "new", "one", "two", "first", "well", "way", "use", "her", "him",
    "see", "now", "only", "come", "make", "like", "get", "over", "many",
    "then", "them", "same", "here", "much", "take", "own", "still",
    "since", "after", "through", "most", "between", "been", "both",
    "www", "com", "http", "https", "org", "net", "html", "php",
    "inc", "llc", "ltd", "corp", "company", "services", "service",
}

# ---------------------------------------------------------------------------
# NAICS Keyword Index Builder
# ---------------------------------------------------------------------------
_NAICS_WORD_SPLIT = re.compile(r"[a-z]{3,}", re.IGNORECASE)


def build_naics_keyword_index():
    """Build a dict mapping 6-digit NAICS codes to their title keywords.

    Loads all codes at all levels (2-6 digit) from official reference files,
    then for any 6-digit code not directly found, walks up the hierarchy
    to use parent-level titles as keyword fallback.

    Returns:
        dict: {naics_code: {"title": str, "keywords": set[str]}}
    """
    all_codes = {}
    index = {}

    for filepath in [NAICS_2017_FILE, NAICS_2022_FILE]:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            code_val = row[1]
            title_val = row[2]
            if code_val is None or title_val is None:
                continue
            code_str = (str(int(code_val)) if isinstance(code_val, (int, float))
                        else str(code_val).strip())
            if not code_str.isdigit() or not (2 <= len(code_str) <= 6):
                continue
            title = str(title_val).strip()
            all_codes[code_str] = title

            if len(code_str) == 6:
                keywords = {w.lower() for w in _NAICS_WORD_SPLIT.findall(title)}
                keywords -= STOPWORDS
                if code_str not in index or len(keywords) > len(index[code_str]["keywords"]):
                    index[code_str] = {"title": title, "keywords": keywords}
        wb.close()

    build_naics_keyword_index._all_codes = all_codes
    return index


def classify_naics_code(code, naics_index):
    """Classify a NAICS code's validity and resolve keywords if possible.

    Returns dict with: status, title, keywords, parent, suggestion
    """
    if code in naics_index:
        return {
            "status": "current",
            "title": naics_index[code]["title"],
            "keywords": naics_index[code]["keywords"],
            "parent": None,
            "suggestion": None,
        }

    all_codes = getattr(build_naics_keyword_index, "_all_codes", {})

    is_zero_padded = (code.endswith("00") and code[:4] in all_codes) or \
                     (code.endswith("0") and code[:5] in all_codes)

    for length in [5, 4, 3, 2]:
        prefix = code[:length]
        if prefix in all_codes:
            title = all_codes[prefix]
            keywords = {w.lower() for w in _NAICS_WORD_SPLIT.findall(title)}
            keywords -= STOPWORDS

            if is_zero_padded:
                suggestion = (f"Code {code} appears to be a zero-padded "
                              f"{length}-digit code ({prefix}: {title}). "
                              f"Should be filed as a specific 6-digit code "
                              f"under {prefix}.")
            else:
                suggestion = (f"Code {code} was retired from NAICS. "
                              f"Nearest current parent: {prefix} ({title}). "
                              f"Update to a current 6-digit code under {prefix}.")

            return {
                "status": "retired",
                "title": f"{title} (parent {prefix})",
                "keywords": keywords,
                "parent": prefix,
                "suggestion": suggestion,
            }

    return {
        "status": "invalid",
        "title": "",
        "keywords": set(),
        "parent": None,
        "suggestion": (f"Code {code} does not exist in any NAICS edition. "
                       f"May be a legacy SIC code or data entry error. "
                       f"Requires manual review to assign a valid 6-digit NAICS."),
    }


def resolve_missing_naics(code, naics_index):
    """Backward-compatible wrapper around classify_naics_code."""
    result = classify_naics_code(code, naics_index)
    if result["status"] == "invalid":
        return None
    return {"title": result["title"], "keywords": result["keywords"]}


# ---------------------------------------------------------------------------
# Output CSV Columns (original + new)
# ---------------------------------------------------------------------------
ORIGINAL_COLUMNS = [
    "id", "establishment_name", "company_name",
    "street_address", "city", "state", "zip_code",
    "naics_code", "industry_description",
    "naics_verified", "naics_description",
    "confidence", "reasoning", "match_status",
    "priority_tier", "n_flags",
]

NEW_COLUMNS = [
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

OUTPUT_COLUMNS = ORIGINAL_COLUMNS + NEW_COLUMNS

# ---------------------------------------------------------------------------
# SEC EDGAR Configuration
# ---------------------------------------------------------------------------
EDGAR_USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "OSHA NAICS Research Project")
EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_RATE_DELAY = 0.12  # 10 req/sec max per SEC fair access policy
EDGAR_FUZZY_THRESHOLD = 80  # Minimum fuzzy match score (0-100)

# ---------------------------------------------------------------------------
# SAM.gov Configuration
# ---------------------------------------------------------------------------
SAM_API_KEY = os.environ.get("SAM_API_KEY", "")
SAM_ENDPOINT = "https://api.sam.gov/entity-information/v3/entities"
SAM_RATE_DELAY = 1.0     # Conservative for 1,000/day cap
SAM_DAILY_BUDGET = 950   # Leave 50 buffer from 1,000 hard cap

# ---------------------------------------------------------------------------
# SIC → NAICS Crosswalk Loader
# ---------------------------------------------------------------------------
_sic_crosswalk_cache = None


def load_sic_naics_crosswalk():
    """Load the SIC→NAICS 2017 crosswalk into a dict.

    Returns:
        dict: {sic_code: [{"naics_code": str, "naics_description": str,
                           "match_quality": str}, ...]}
              Ordered by match quality (exact > high > medium > low).
    """
    global _sic_crosswalk_cache
    if _sic_crosswalk_cache is not None:
        return _sic_crosswalk_cache

    _sic_crosswalk_cache = {}
    if not os.path.exists(SIC_NAICS_CROSSWALK_FILE):
        return _sic_crosswalk_cache

    with open(SIC_NAICS_CROSSWALK_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sic = row["sic_code"]
            if sic not in _sic_crosswalk_cache:
                _sic_crosswalk_cache[sic] = []
            _sic_crosswalk_cache[sic].append({
                "naics_code": row["naics_code"],
                "naics_description": row["naics_description"],
                "match_quality": row["match_quality"],
            })
    return _sic_crosswalk_cache
