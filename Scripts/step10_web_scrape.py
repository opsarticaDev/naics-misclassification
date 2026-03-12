"""
Step 10: Web Scraping Pipeline for NAICS Classification
========================================================
INPUT:  uncertain_for_websearch_{YEAR}.csv (UNCERTAIN records from Step 9)
OUTPUT: scrape_results_{YEAR}.csv (unique companies with NAICS verification)

Methodology
-----------
For each company, the pipeline:

  1. URL DISCOVERY: two tiers:
     a. Algorithmic guess: construct likely URLs from company name
        (e.g., "Acme Corp" → acmecorp.com). HEAD-validated.
     b. Search API fallback: SearXNG (self-hosted) or Brave Search.
        Filters out social media, directories, and government sites
        (see SKIP_DOMAINS in scrape_config.py).

  2. CONTENT SCRAPING: fetch homepage + up to 2 subpages (/about,
     /services). Extract visible text via BeautifulSoup, stripping
     nav, footer, script, and style elements.

  3. KEYWORD SCORING: compare scraped text against NAICS code titles.
     Each 6-digit NAICS code has a keyword set derived from its Census
     Bureau title (e.g., 238220 → {"plumbing", "heating", "conditioning"}).

     Scoring formula (see _score_keywords):
       score = 0.4 * coverage       : fraction of NAICS keywords found
             + 0.3 * abs_match_bonus : min(matched_count / 5, 1.0)
             + 0.2 * density         : matched keyword frequency / total tokens
             + 0.1 * sector_bonus    : 1.0 if coverage > 50%, else 0.0

     Weight rationale: coverage is dominant because a single keyword hit
     (e.g., "construction") is weak evidence, but 4/5 keywords matching is
     strong. The abs_match_bonus (saturating at 5 keywords) ensures codes
     with many keywords don't get penalized for partial matches. Density
     rewards sites that talk extensively about the industry. The sector
     bonus is a small tiebreaker for high-coverage matches.

  4. DECISION LOGIC (see decide function):
     - CONFIRMED:  reported NAICS scores ≥ 0.4 AND ≥ 80% of top candidate.
       Confidence: "high" if reported_score ≥ 0.5, else "medium".
     - SUGGESTED:  top candidate scores ≥ 0.4 AND > 1.5× reported score.
       Confidence: "high" if top_score ≥ 0.5, else "medium".
     - SUGGESTED (fallback): reported scores 0 but top candidate ≥ 0.4.
       Confidence always "medium" (weaker evidence: no comparative signal).
     - UNCERTAIN:  all other cases (low scores, inconclusive comparison).

     IMPORTANT LIMITATION: The 0.5 confidence boundary and the keyword
     scoring weights were tuned by manual inspection of ~200 pilot records,
     not by formal cross-validation. The SUGGESTED category has a high
     false-positive rate (~54% of SUGGESTED records have bad URLs or zero
     keyword matches for the reported code). See Gate 14 audit.

CLI Flags
---------
  --pilot N          Process N records (random sample)
  --all              Process all records
  --resume           Continue from checkpoint
  --limit N          Cap at N records per run
  --workers N        Parallel workers (default 1)
  --strategy diverse Diverse sampling across tiers (vs sequential)
  --no-brave         Disable search API tier (guess-only mode).
                     WARNING: despite the name, this disables ALL search
                     including SearXNG, not just Brave.

"""

import argparse
import csv
import json
import multiprocessing
import os
import re
import sys
import time
from collections import Counter
from urllib.parse import urlparse, quote_plus

import urllib3
import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from util_scrape_config import (
    BASE_DIR, UNCERTAIN_FILE, SCRAPE_OUTPUT_FILE, CHECKPOINT_FILE,
    BRAVE_API_KEY, BRAVE_ENDPOINT, BRAVE_BUDGET_PER_RUN,
    SEARXNG_ENDPOINT, SEARXNG_ENGINES, SEARXNG_BUDGET_PER_RUN,
    SEARCH_BACKEND,
    SEARCH_DELAY, SCRAPE_TIMEOUT, SCRAPE_SAME_DOMAIN_DELAY,
    MAX_PAGES_PER_SITE, URL_GUESS_TIMEOUT,
    CONFIRM_MIN_SCORE, CONFIRM_RATIO, SUGGEST_MIN_SCORE, SUGGEST_RATIO,
    SKIP_DOMAINS, STOPWORDS,
    build_naics_keyword_index, resolve_missing_naics, classify_naics_code,
    OUTPUT_COLUMNS,
)

# ---------------------------------------------------------------------------
# Global flag: skip search API tier (guess-only mode)
# Set via --no-brave / --no-search CLI flag
# ---------------------------------------------------------------------------
SKIP_SEARCH_API = False

# ---------------------------------------------------------------------------
# HTTP Session (for scraping, not search)
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
})

# Playwright browser instance (lazy-loaded)
_BROWSER = None
_PLAYWRIGHT = None


def _get_browser():
    """Lazy-load Playwright browser. Returns browser instance."""
    global _BROWSER, _PLAYWRIGHT
    if _BROWSER is None:
        from playwright.sync_api import sync_playwright
        _PLAYWRIGHT = sync_playwright().start()
        _BROWSER = _PLAYWRIGHT.chromium.launch(headless=True)
    return _BROWSER


def _close_browser():
    """Clean up Playwright resources."""
    global _BROWSER, _PLAYWRIGHT
    if _BROWSER:
        _BROWSER.close()
        _BROWSER = None
    if _PLAYWRIGHT:
        _PLAYWRIGHT.stop()
        _PLAYWRIGHT = None


# ===================================================================
# 1. URL Discovery: Tier 1: Algorithmic Guess
# ===================================================================

def _clean_company_name(name):
    """Strip legal suffixes and punctuation for domain guessing."""
    name = name.replace("_", " ").strip()
    name = re.sub(
        r",?\s*(Inc\.?|LLC|L\.L\.C\.?|Ltd\.?|Corp\.?|Co\.?|Company|"
        r"Incorporated|Corporation|Limited|Group|Holdings|Enterprises?|"
        r"International|Associates?|Partners?|LP|L\.P\.)\s*\.?$",
        "", name, flags=re.IGNORECASE
    ).strip()
    # Remove trailing punctuation
    name = re.sub(r"[.,;:!?]+$", "", name).strip()
    return name


def guess_urls(company_name, city="", state=""):
    """Generate probable domain URLs from company name.

    Handles standard companies, government entities, and "X of Y" patterns.
    Returns list of URLs to try, most likely first.
    """
    clean = _clean_company_name(company_name)
    lower = clean.lower()
    # Extract just alphanumeric words
    words = re.findall(r"[a-zA-Z0-9]+", lower)
    words = [w for w in words if len(w) >= 2]

    if not words:
        return []

    guesses = []
    joined = "".join(words)
    hyphenated = "-".join(words)

    # Detect government entities
    is_gov = bool(re.search(
        r"\b(city|county|town|village|state|department|dept|bureau|"
        r"district|authority|commission|police|fire|school)\b",
        lower
    ))

    # "City of X" / "County of X" / "Town of X" patterns
    gov_match = re.match(
        r"(city|county|town|village)\s+of\s+(.+)", lower
    )

    if gov_match:
        entity_type, place = gov_match.groups()
        place_words = re.findall(r"[a-z]+", place)
        place_joined = "".join(place_words)
        place_hyphenated = "-".join(place_words)
        # Most common government URL patterns
        guesses.append(f"https://www.{place_joined}.gov")
        guesses.append(f"https://www.{entity_type}of{place_joined}.com")
        guesses.append(f"https://www.{entity_type}of{place_joined}.org")
        guesses.append(f"https://www.{place_joined}{entity_type}.gov")
        guesses.append(f"https://www.{place_joined}.org")
        if city:
            city_clean = re.sub(r"[^a-z]", "", city.lower())
            guesses.append(f"https://www.{city_clean}.gov")
            guesses.append(f"https://www.{entity_type}of{city_clean}.com")
    elif is_gov:
        # Government entity but not "X of Y" pattern
        guesses.append(f"https://www.{joined}.gov")
        guesses.append(f"https://www.{joined}.org")
        if city:
            city_clean = re.sub(r"[^a-z]", "", city.lower())
            guesses.append(f"https://www.{city_clean}.gov")

    # Standard company patterns
    guesses.append(f"https://www.{joined}.com")
    if len(words) > 1:
        guesses.append(f"https://www.{hyphenated}.com")
        # First two words joined
        guesses.append(f"https://www.{''.join(words[:2])}.com")
    # First word only (brand names: Starbucks, Runnings, etc.)
    if len(words[0]) >= 4:
        guesses.append(f"https://www.{words[0]}.com")
    # .org and .net variants
    guesses.append(f"https://www.{joined}.org")
    guesses.append(f"https://www.{joined}.net")

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for url in guesses:
        if url not in seen:
            seen.add(url)
            unique.append(url)
    return unique


def try_url_guess(company_name, city="", state=""):
    """Try algorithmically guessed URLs with HEAD then validate with GET.

    After a HEAD succeeds, does a quick GET to verify the page title or body
    contains at least one significant company name word: prevents false
    positives like gabriel.com for "Gabriel Brothers".

    Returns (url, "guess") if one works, or (None, None).
    """
    urls = guess_urls(company_name, city, state)
    name_words = _name_words(company_name)

    for url in urls:
        try:
            resp = SESSION.head(url, timeout=URL_GUESS_TIMEOUT,
                                allow_redirects=True)
            ct = resp.headers.get("Content-Type", "")
            if resp.status_code != 200 or "text/html" not in ct:
                continue

            final_domain = _domain_of(resp.url)
            if _is_skip_domain(final_domain):
                continue

            # Validate: GET the page and check if company name appears
            try:
                vresp = SESSION.get(resp.url, timeout=URL_GUESS_TIMEOUT,
                                    allow_redirects=True)
            except requests.exceptions.SSLError:
                vresp = SESSION.get(resp.url, timeout=URL_GUESS_TIMEOUT,
                                    allow_redirects=True, verify=False)
            if vresp.status_code != 200:
                continue

            page_text = vresp.text[:10000].lower()
            sig_words = [w for w in name_words if len(w) >= 4]
            hits = sum(1 for w in sig_words if w in page_text)

            # For multi-word names, require at least 2 matches (or all if only 1-2 sig words)
            # For single-word names, that one word must appear
            if len(sig_words) >= 3:
                if hits >= 2:
                    return vresp.url, "guess"
            elif len(sig_words) >= 1:
                if hits >= len(sig_words):
                    return vresp.url, "guess"
            else:
                # No significant words (all < 4 chars): accept domain match
                return vresp.url, "guess"

        except requests.RequestException:
            continue
    return None, None


# ===================================================================
# 2. URL Discovery: Tier 2: Brave Search API
# ===================================================================

def brave_search(company_name, city, state, stats):
    """Search via Brave Search API.

    Returns list of {url, title, description} dicts.
    Uses stats["api_calls"] to track budget.
    """
    if stats.get("api_calls", 0) >= BRAVE_BUDGET_PER_RUN:
        print(f"    [SEARCH] Brave API budget exhausted ({BRAVE_BUDGET_PER_RUN} calls)")
        return []

    query = f'"{company_name}" {city} {state}'
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    params = {"q": query, "count": 5}

    try:
        resp = requests.get(BRAVE_ENDPOINT, headers=headers, params=params, timeout=10)
        stats["api_calls"] = stats.get("api_calls", 0) + 1
        resp.raise_for_status()
        data = resp.json()
        web_results = data.get("web", {}).get("results", [])
        results = []
        for r in web_results:
            results.append({
                "url": r.get("url", ""),
                "title": r.get("title", ""),
                "description": r.get("description", ""),
            })
        if results:
            print(f"    [SEARCH] Brave returned {len(results)} results")
        else:
            print(f"    [SEARCH] Brave returned no results")
        return results
    except Exception as e:
        print(f"    [SEARCH ERROR] Brave API: {e}")
        stats["api_calls"] = stats.get("api_calls", 0) + 1
        return []


def searxng_search(company_name, city, state, stats):
    """Search via self-hosted SearXNG instance.

    Drop-in replacement for brave_search(). Returns same format:
    list of {url, title, description} dicts.
    Uses stats["api_calls"] to track query count.
    """
    if stats.get("api_calls", 0) >= SEARXNG_BUDGET_PER_RUN:
        print(f"    [SEARCH] SearXNG budget exhausted ({SEARXNG_BUDGET_PER_RUN} calls)")
        return []

    query = f'"{company_name}" {city} {state}'
    params = {
        "q": query,
        "format": "json",
        "engines": SEARXNG_ENGINES,
        "safesearch": 0,
    }

    try:
        resp = requests.get(SEARXNG_ENDPOINT, params=params, timeout=15)
        stats["api_calls"] = stats.get("api_calls", 0) + 1
        resp.raise_for_status()
        data = resp.json()
        raw_results = data.get("results", [])
        results = []
        for r in raw_results[:5]:  # Cap at 5 results like Brave
            results.append({
                "url": r.get("url", ""),
                "title": r.get("title", ""),
                "description": r.get("content", ""),  # SearXNG uses "content" not "description"
            })
        if results:
            print(f"    [SEARCH] SearXNG returned {len(results)} results")
        else:
            print(f"    [SEARCH] SearXNG returned no results")
        return results
    except requests.exceptions.ConnectionError:
        print(f"    [SEARCH ERROR] SearXNG unreachable at {SEARXNG_ENDPOINT}")
        return []
    except Exception as e:
        print(f"    [SEARCH ERROR] SearXNG: {e}")
        stats["api_calls"] = stats.get("api_calls", 0) + 1
        return []


# ===================================================================
# 3. URL Selection: pick best URL from search results
# ===================================================================

def _domain_of(url):
    """Extract the registered domain from a URL."""
    try:
        host = urlparse(url).hostname or ""
        if host.startswith("www."):
            host = host[4:]
        return host.lower()
    except Exception:
        return ""


def _is_skip_domain(domain):
    """Check if domain is in the skip list."""
    for skip in SKIP_DOMAINS:
        if domain == skip or domain.endswith("." + skip):
            return True
    return False


def _name_words(company_name):
    """Extract significant words from company name for domain matching."""
    name = company_name.replace("_", " ").lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    words = [w for w in name.split() if w not in STOPWORDS and len(w) >= 3]
    return words


_DOCUMENT_EXTENSIONS = re.compile(
    r"\.(pdf|doc|docx|xls|xlsx|ppt|pptx|csv|zip|tar|gz)(\?|#|$)",
    re.IGNORECASE,
)


def pick_best_url(search_results, company_name):
    """Score and rank URLs to find the company's actual website.

    Skips document links (PDF, DOC, etc.) and deprioritizes .gov domains
    since they rarely contain useful company HTML from search results.

    Returns (url, score) or (None, 0).
    """
    if not search_results:
        return None, 0

    name_words = _name_words(company_name)
    scored = []

    for result in search_results:
        url = result["url"]
        domain = _domain_of(url)
        title = result.get("title", "").lower()

        if _is_skip_domain(domain):
            continue

        # Skip document links: these won't scrape as HTML
        if _DOCUMENT_EXTENSIONS.search(url):
            continue

        score = 1
        for word in name_words:
            if word in domain.replace(".", "").replace("-", ""):
                score += 3
            if word in title:
                score += 2

        # Deprioritize .gov domains: often thin content, not the company site
        if domain.endswith(".gov"):
            score -= 2

        scored.append((url, score))

    if not scored:
        return None, 0

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0]


def find_url(company_name, city, state, stats):
    """Two-tier URL discovery: guess first, then search API fallback.

    Returns (url, source) where source is "guess", "search", or None.
    Search backend is configured via SEARCH_BACKEND in scrape_config.py:
      "searxng" = self-hosted SearXNG instance (free, unlimited)
      "brave"   = Brave Search API (paid, 2000 call budget)
    """
    # Tier 1: Algorithmic guess (free, instant)
    url, source = try_url_guess(company_name, city, state)
    if url:
        stats["guess_hits"] += 1
        return url, "guess"

    # Tier 2: Search API fallback (skip if --no-brave / --no-search)
    if SKIP_SEARCH_API:
        stats["no_url"] += 1
        return None, None

    time.sleep(SEARCH_DELAY)
    if SEARCH_BACKEND == "searxng":
        search_results = searxng_search(company_name, city, state, stats)
    else:
        search_results = brave_search(company_name, city, state, stats)
    url, score = pick_best_url(search_results, company_name)
    if url:
        stats["search_hits"] += 1
        return url, "search"

    stats["no_url"] += 1
    return None, None


# ===================================================================
# 4. Website Scraping
# ===================================================================

def _clean_text(soup):
    """Extract meaningful text from a BeautifulSoup object."""
    for tag in soup.find_all(["script", "style", "nav", "footer", "header",
                              "aside", "noscript", "iframe"]):
        tag.decompose()

    parts = []
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        parts.append(meta["content"])
    if soup.title and soup.title.string:
        parts.append(soup.title.string)
    og = soup.find("meta", attrs={"property": "og:description"})
    if og and og.get("content"):
        parts.append(og["content"])
    body = soup.find("body")
    if body:
        parts.append(body.get_text(separator=" ", strip=True))

    text = " ".join(parts)
    return re.sub(r"\s+", " ", text).strip()


def _find_subpages(soup, base_url):
    """Find /about and /services type pages from nav links."""
    from urllib.parse import urljoin
    subpage_patterns = re.compile(
        r"/(about|services|what-we-do|our-work|industries|products|solutions|capabilities)",
        re.IGNORECASE,
    )
    domain = _domain_of(base_url)
    found = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Resolve relative URLs (both root-relative "/about" and
        # document-relative "about.html") against the base URL
        href = urljoin(base_url, href)
        if _domain_of(href) != domain:
            continue
        if subpage_patterns.search(href) and href not in found:
            found.add(href)
            if len(found) >= MAX_PAGES_PER_SITE - 1:
                break
    return list(found)


def _scrape_with_playwright(url):
    """Fallback scraper using Playwright for JS-rendered sites."""
    try:
        browser = _get_browser()
        page = browser.new_page()
        page.set_default_timeout(15000)
        page.goto(url, wait_until="domcontentloaded")
        # Give JS a moment to render
        page.wait_for_timeout(2000)
        html = page.content()
        page.close()
        soup = BeautifulSoup(html, "html.parser")
        return _clean_text(soup)
    except Exception as e:
        print(f"  [PLAYWRIGHT SCRAPE ERROR] {url}: {e}")
        try:
            page.close()
        except Exception:
            pass
        return None


def scrape_website(url):
    """Scrape homepage + up to 2 subpages. Falls back to Playwright for JS sites.

    Returns combined text or None.
    """
    all_text = []

    try:
        try:
            resp = SESSION.get(url, timeout=SCRAPE_TIMEOUT, allow_redirects=True)
        except requests.exceptions.SSLError:
            resp = SESSION.get(url, timeout=SCRAPE_TIMEOUT, allow_redirects=True,
                               verify=False)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        homepage_text = _clean_text(soup)

        # If requests got very little content, try Playwright (JS-rendered site)
        if len(homepage_text.strip()) < 200:
            print(f"    -> Thin content via requests ({len(homepage_text)} chars), trying Playwright...")
            pw_text = _scrape_with_playwright(url)
            if pw_text and len(pw_text.strip()) > len(homepage_text.strip()):
                homepage_text = pw_text

        all_text.append(homepage_text)

        for subpage_url in _find_subpages(soup, url):
            time.sleep(SCRAPE_SAME_DOMAIN_DELAY)
            try:
                try:
                    sub_resp = SESSION.get(subpage_url, timeout=SCRAPE_TIMEOUT,
                                           allow_redirects=True)
                except requests.exceptions.SSLError:
                    sub_resp = SESSION.get(subpage_url, timeout=SCRAPE_TIMEOUT,
                                           allow_redirects=True, verify=False)
                sub_resp.raise_for_status()
                sub_ct = sub_resp.headers.get("Content-Type", "")
                if "text/html" in sub_ct or "application/xhtml" in sub_ct:
                    sub_soup = BeautifulSoup(sub_resp.text, "html.parser")
                    all_text.append(_clean_text(sub_soup))
            except requests.RequestException:
                continue

        combined = " ".join(all_text)
        return combined[:50000] if len(combined) > 50000 else combined

    except requests.RequestException as e:
        print(f"  [SCRAPE ERROR] {url}: {e}")
        # Last resort: try Playwright
        print(f"    -> Trying Playwright fallback...")
        pw_text = _scrape_with_playwright(url)
        return pw_text


# ===================================================================
# 5. Keyword Density Scoring
# ===================================================================

_WORD_RE = re.compile(r"[a-z]{3,}")


def tokenize(text):
    """Tokenize text into lowercase words, removing stopwords."""
    words = _WORD_RE.findall(text.lower())
    return [w for w in words if w not in STOPWORDS]


def _score_keywords(matched, naics_keywords, word_freq, total_tokens):
    """Core keyword scoring formula.

    Returns a 0.0-1.0 composite score measuring how well scraped text
    matches a given NAICS code's keyword set.

    Components (see module docstring for weight rationale):
      coverage (0.4):    fraction of NAICS keywords found on the page
      abs_match (0.3):   absolute count of matched keywords, saturating
                         at 5 (prevents codes with many keywords from
                         getting artificially low coverage scores)
      density (0.2):     how often matched keywords appear relative to
                         total page text (rewards industry-focused pages)
      sector_bonus (0.1): binary bonus when >50% of keywords found
                         (tiebreaker for high-coverage matches)
    """
    n_matched = len(matched)
    n_keywords = len(naics_keywords)

    coverage = n_matched / n_keywords if n_keywords else 0
    abs_match_bonus = min(n_matched / 5.0, 1.0)  # saturates at 5 matched keywords
    matched_count = sum(word_freq[kw] for kw in matched)
    density = matched_count / total_tokens if total_tokens else 0
    sector_bonus = 1.0 if coverage > 0.5 else 0.0  # binary: majority of keywords found

    return 0.4 * coverage + 0.3 * abs_match_bonus + 0.2 * density + 0.1 * sector_bonus


def score_naics_match(scraped_text, naics_code, naics_index):
    """Score how well scraped text matches a specific NAICS code."""
    entry = naics_index.get(naics_code)
    if not entry:
        entry = resolve_missing_naics(naics_code, naics_index)
    if not entry or not entry["keywords"]:
        return 0.0, []

    tokens = tokenize(scraped_text)
    if not tokens:
        return 0.0, []

    word_freq = Counter(tokens)
    naics_keywords = entry["keywords"]
    matched = [kw for kw in naics_keywords if kw in word_freq]
    if not matched:
        return 0.0, []

    return _score_keywords(matched, naics_keywords, word_freq, len(tokens)), matched


def find_top_naics(scraped_text, naics_index, top_n=5):
    """Score scraped text against ALL NAICS codes, return top N."""
    tokens = tokenize(scraped_text)
    if not tokens:
        return []

    word_freq = Counter(tokens)
    results = []

    for code, entry in naics_index.items():
        keywords = entry["keywords"]
        if not keywords:
            continue
        matched = [kw for kw in keywords if kw in word_freq]
        if not matched:
            continue
        score = _score_keywords(matched, keywords, word_freq, len(tokens))
        results.append((code, score, entry["title"], matched))

    results.sort(key=lambda x: x[1], reverse=True)
    return results[:top_n]


# ===================================================================
# 6. Decision Logic
# ===================================================================

def decide(reported_naics, scraped_text, naics_index):
    """Determine whether scraped content supports the reported NAICS code.

    Decision tree (see module docstring for threshold definitions):
      1. No text scraped → SCRAPE_FAILED
      2. No NAICS keywords in text → UNCERTAIN (low confidence)
      3. Invalid/retired code → UNCERTAIN with best-guess suggestion
      4. reported_score ≥ 0.4 AND ≥ 80% of top → CONFIRMED
      5. top_score ≥ 0.4 AND > 1.5× reported → SUGGESTED
      6. reported_score = 0 AND top ≥ 0.4 → SUGGESTED (medium conf)
      7. Everything else → UNCERTAIN (inconclusive)

    Thresholds are imported from util_scrape_config.py:
      CONFIRM_MIN_SCORE = 0.4    (minimum absolute score to confirm)
      CONFIRM_RATIO = 0.80       (reported must be ≥80% of top to confirm)
      SUGGEST_MIN_SCORE = 0.4    (minimum absolute score to suggest)
      SUGGEST_RATIO = 1.5        (top must beat reported by 50% to suggest)

    The 0.5 boundary for high vs. medium confidence was determined by
    manual review: scores ≥ 0.5 typically had 3+ keyword matches with
    meaningful density; scores 0.4-0.5 often had only 2 keyword matches.
    """
    code_info = classify_naics_code(reported_naics, naics_index)

    base = {
        "naics_code_status": code_info["status"],
        "naics_code_note": code_info["suggestion"] or "",
    }

    if not scraped_text:
        base.update({
            "suggested_naics": "", "suggested_description": "",
            "scrape_confidence": "",
            "scrape_reasoning": "No text could be scraped from website",
            "scrape_match_status": "SCRAPE_FAILED", "scraped_keywords": "",
        })
        return base

    reported_score, _ = score_naics_match(scraped_text, reported_naics, naics_index)
    top_candidates = find_top_naics(scraped_text, naics_index)

    if not top_candidates:
        base.update({
            "suggested_naics": "", "suggested_description": "",
            "scrape_confidence": "low",
            "scrape_reasoning": "No NAICS keywords matched scraped content",
            "scrape_match_status": "UNCERTAIN", "scraped_keywords": "",
        })
        return base

    top_code, top_score, top_title, _ = top_candidates[0]

    tokens = tokenize(scraped_text)
    word_freq = Counter(tokens)
    # Extract top business-relevant words (≥4 chars excludes articles/prepositions)
    # for human-readable keyword summary in output CSV
    biz_words = [(w, c) for w, c in word_freq.most_common(50) if len(w) >= 4]
    top_keywords = ", ".join(w for w, _ in biz_words[:10])

    # Invalid code
    if code_info["status"] == "invalid":
        base.update({
            "suggested_naics": top_code, "suggested_description": top_title,
            "scrape_confidence": "low",
            "scrape_reasoning": (
                f"INVALID CODE: {reported_naics} not found in any NAICS edition. "
                f"Best web match: {top_code} ({top_title}), score {top_score:.3f}. "
                f"Requires manual correction."
            ),
            "scrape_match_status": "UNCERTAIN", "scraped_keywords": top_keywords,
        })
        return base

    retired_prefix = ""
    if code_info["status"] == "retired":
        retired_prefix = (
            f"RETIRED CODE: {reported_naics} scored via parent "
            f"{code_info['parent']} ({code_info['title']}). "
        )

    # CONFIRMED
    if (reported_score >= CONFIRM_MIN_SCORE and top_score > 0 and
            reported_score >= CONFIRM_RATIO * top_score):
        base.update({
            "suggested_naics": reported_naics,
            "suggested_description": code_info["title"],
            # 0.5 boundary: ≥0.5 typically means 3+ keywords w/ density; see module docstring
            "scrape_confidence": "high" if reported_score >= 0.5 else "medium",
            "scrape_reasoning": (
                f"{retired_prefix}Reported NAICS {reported_naics} scores "
                f"{reported_score:.3f} (top candidate {top_code} scores "
                f"{top_score:.3f})"
            ),
            "scrape_match_status": "CONFIRMED", "scraped_keywords": top_keywords,
        })
        return base

    # SUGGESTED
    if (top_score >= SUGGEST_MIN_SCORE and reported_score > 0 and
            top_score > SUGGEST_RATIO * reported_score):
        base.update({
            "suggested_naics": top_code, "suggested_description": top_title,
            "scrape_confidence": "high" if top_score >= 0.5 else "medium",
            "scrape_reasoning": (
                f"{retired_prefix}Top NAICS {top_code} ({top_title}) scores "
                f"{top_score:.3f} vs reported {reported_naics} at "
                f"{reported_score:.3f}"
            ),
            "scrape_match_status": "SUGGESTED", "scraped_keywords": top_keywords,
        })
        return base

    # Reported 0, top has signal
    if reported_score == 0 and top_score >= SUGGEST_MIN_SCORE:
        base.update({
            "suggested_naics": top_code, "suggested_description": top_title,
            "scrape_confidence": "medium",
            "scrape_reasoning": (
                f"{retired_prefix}Reported NAICS {reported_naics} keywords "
                f"not found in website; top candidate {top_code} ({top_title}) "
                f"scores {top_score:.3f}"
            ),
            "scrape_match_status": "SUGGESTED", "scraped_keywords": top_keywords,
        })
        return base

    # Inconclusive
    base.update({
        "suggested_naics": top_code, "suggested_description": top_title,
        "scrape_confidence": "low",
        "scrape_reasoning": (
            f"{retired_prefix}Inconclusive: reported {reported_naics} scores "
            f"{reported_score:.3f}, top candidate {top_code} scores "
            f"{top_score:.3f}"
        ),
        "scrape_match_status": "UNCERTAIN", "scraped_keywords": top_keywords,
    })
    return base


# ===================================================================
# 7. Checkpoint & Resume
# ===================================================================

def load_checkpoint():
    """Load processing checkpoint from disk, or None if no checkpoint exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_checkpoint(processed_ids, stats, results):
    """Save processing checkpoint for --resume capability."""
    state = {
        "processed_ids": processed_ids,
        "stats": stats,
        "total_processed": len(processed_ids),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def save_results_csv(results, output_file=None):
    """Full rewrite of results CSV (used at end of run for integrity)."""
    output_file = output_file or SCRAPE_OUTPUT_FILE
    if not results:
        return
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)


def append_result_csv(result, output_file=None):
    """Append a single result row to the CSV. Writes header if file is new."""
    output_file = output_file or SCRAPE_OUTPUT_FILE
    file_exists = os.path.exists(output_file) and os.path.getsize(output_file) > 0
    with open(output_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(result)


# ===================================================================
# 8. Sampling Strategies
# ===================================================================

def sample_diverse(records, n):
    """One record per unique company_name, sorted by flag count descending."""
    seen_names = set()
    unique = []
    sorted_recs = sorted(records, key=lambda r: int(r.get("n_flags", 0)), reverse=True)
    for r in sorted_recs:
        name = (r.get("company_name") or r.get("establishment_name") or "").strip().lower()
        if name and name not in seen_names:
            seen_names.add(name)
            unique.append(r)
            if n and len(unique) >= n:
                break
    return unique


def sample_sequential(records, n):
    return records[:n] if n else records


# ===================================================================
# 9. Main Pipeline
# ===================================================================

def _normalize_search_name(name, city="", state=""):
    """Normalize a company/establishment name for better URL discovery.

    Fixes common data quality issues:
      - Leading numeric codes:  "202175 - BIRMINGHAM FP AL10" → "BIRMINGHAM FP"
      - Trailing city/state:    "The Timken Company Norton Shores" → "The Timken Company"
      - Trailing location tags: "Acme Corp - Denver" → "Acme Corp"
      - Trailing zip/codes:     "Acme Corp AL10" → "Acme Corp"
    """
    # Strip leading numeric codes (e.g., "202175 - BIRMINGHAM FP AL10")
    name = re.sub(r"^\d{4,}\s*[-–]\s*", "", name).strip()

    # Strip trailing " - CITY" or " - CITY ST" location patterns
    name = re.sub(r"\s*[-–]\s+[A-Z][A-Za-z\s]{2,}$", "", name).strip()

    # Strip trailing state abbreviation + optional zip-like codes
    # e.g., "Acme Corp AL10", "Acme Corp TX 75001"
    name = re.sub(
        r"\s+[A-Z]{2}\s*\d{2,5}\s*$", "", name
    ).strip()

    # Strip trailing city name if it matches the record's city field
    if city:
        city_clean = city.replace("_", " ").strip()
        if city_clean and len(city_clean) >= 3:
            pattern = re.compile(r"\s+" + re.escape(city_clean) + r"\s*$", re.IGNORECASE)
            name = pattern.sub("", name).strip()

    # Strip trailing state abbreviation alone (2 uppercase letters at end)
    if state:
        state_clean = state.replace("_", " ").strip().upper()
        if len(state_clean) == 2:
            name = re.sub(r"\s+" + re.escape(state_clean) + r"\s*$", "", name).strip()

    return name


def process_record(record, naics_index, stats):
    """Process a single record through the full pipeline."""
    company = (record.get("company_name") or record.get("establishment_name") or "")
    estab = record.get("establishment_name", "")
    city = record.get("city", "")
    state = record.get("state", "")
    reported_naics = record.get("naics_code", "")

    company = company.replace("_", " ").strip()
    estab = estab.replace("_", " ").strip()
    city = city.replace("_", " ").strip()

    estab_clean = re.sub(r"\s+(ST\d+|HQ\d+|#\d+|\d{3,})\s*$", "", estab, flags=re.IGNORECASE)
    estab_clean = re.sub(r"\s*-\s*\d+\s*$", "", estab_clean)

    # Normalize both names: strip location suffixes and codes
    company_clean = _normalize_search_name(company, city, state)
    estab_clean = _normalize_search_name(estab_clean, city, state)

    generic_names = {"headquarters", "corporate office", "main office", "admin"}
    company_lower = company_clean.lower().strip()
    estab_lower = estab_clean.lower().strip()

    if company_lower and company_lower not in generic_names:
        search_name = company_clean
    elif estab_lower and estab_lower not in generic_names:
        search_name = estab_clean
    else:
        search_name = company_clean or estab_clean

    code_info = classify_naics_code(reported_naics, naics_index)

    print(f"  Finding URL: {search_name}, {city}, {state}")

    # Two-tier URL discovery
    url, url_source = find_url(search_name, city, state, stats)

    if not url:
        print(f"    -> NO_WEBSITE")
        result = dict(record)
        result.update({
            "naics_code_status": code_info["status"],
            "naics_code_note": code_info["suggestion"] or "",
            "website_url": "", "url_source": "",
            "scraped_keywords": "", "suggested_naics": "",
            "suggested_description": "", "scrape_confidence": "",
            "scrape_reasoning": "No company website found",
            "scrape_match_status": "NO_WEBSITE",
        })
        return result

    print(f"    -> URL: {url} (via {url_source})")

    # Scrape
    scraped_text = scrape_website(url)

    if not scraped_text or len(scraped_text.strip()) < 50:
        print(f"    -> SCRAPE_FAILED")
        result = dict(record)
        result.update({
            "naics_code_status": code_info["status"],
            "naics_code_note": code_info["suggestion"] or "",
            "website_url": url, "url_source": url_source,
            "scraped_keywords": "", "suggested_naics": "",
            "suggested_description": "", "scrape_confidence": "",
            "scrape_reasoning": "Website found but scraping failed or returned minimal content",
            "scrape_match_status": "SCRAPE_FAILED",
        })
        return result

    print(f"    -> Scraped {len(scraped_text):,} chars")

    decision = decide(reported_naics, scraped_text, naics_index)
    print(f"    -> {decision['scrape_match_status']} | {decision['scrape_reasoning'][:80]}")

    result = dict(record)
    result["website_url"] = url
    result["url_source"] = url_source
    result.update(decision)
    return result


def worker_process(worker_id, records_slice, worker_limit, skip_search=False):
    """Run in a separate process. Processes a slice of records and writes results.

    Each worker gets its own HTTP session, Playwright browser, NAICS index,
    and output files. Returns (results, processed_ids, stats) when done.
    """
    # Each process re-initialises its own globals (Windows uses 'spawn')
    # Propagate the search-skip flag explicitly: globals don't survive spawn
    global SKIP_SEARCH_API
    SKIP_SEARCH_API = skip_search
    worker_tag = f"[W{worker_id}]"
    output_file = SCRAPE_OUTPUT_FILE.replace(".csv", f"_w{worker_id}.csv")
    ckpt_file = CHECKPOINT_FILE.replace(".json", f"_w{worker_id}.json")

    # Remove stale worker files from a previous run
    for f in (output_file, ckpt_file):
        if os.path.exists(f):
            os.remove(f)

    print(f"{worker_tag} Building NAICS index...")
    naics_index = build_naics_keyword_index()

    stats = {"guess_hits": 0, "search_hits": 0, "no_url": 0}
    results = []
    processed_ids = []
    start_time = time.time()

    total = min(len(records_slice), worker_limit) if worker_limit else len(records_slice)

    for i, record in enumerate(records_slice, 1):
        if worker_limit and i > worker_limit:
            break

        rec_id = record["id"]
        company = (record.get("company_name") or
                   record.get("establishment_name") or "?").replace("_", " ")
        print(f"{worker_tag} [{i}/{total}] {company} (ID: {rec_id})")

        try:
            result = process_record(record, naics_index, stats)
            results.append(result)
            processed_ids.append(rec_id)
            append_result_csv(result, output_file)
        except Exception as e:
            print(f"{worker_tag}   [FATAL ERROR] {e}")
            ci = classify_naics_code(record.get("naics_code", ""), naics_index)
            result = dict(record)
            result.update({
                "naics_code_status": ci["status"],
                "naics_code_note": ci["suggestion"] or "",
                "website_url": "", "url_source": "",
                "scraped_keywords": "", "suggested_naics": "",
                "suggested_description": "", "scrape_confidence": "",
                "scrape_reasoning": f"Processing error: {e}",
                "scrape_match_status": "SCRAPE_FAILED",
            })
            results.append(result)
            processed_ids.append(rec_id)
            append_result_csv(result, output_file)

        # Heartbeat every 10 records
        if i % 10 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed * 60 if elapsed > 0 else 0
            heartbeat = {
                "type": "heartbeat",
                "worker": worker_id,
                "progress": f"{i}/{total}",
                "pct": round(i / total * 100, 1),
                "rate_per_min": round(rate, 1),
                "elapsed_min": round(elapsed / 60, 1),
                "guess": stats["guess_hits"],
                "search": stats["search_hits"],
                "miss": stats["no_url"],
            }
            print(f"HEARTBEAT: {json.dumps(heartbeat)}")

    _close_browser()

    elapsed = time.time() - start_time
    print(f"{worker_tag} Done: {len(results)} records in {elapsed/60:.1f} min")
    return results, processed_ids, stats


def merge_worker_outputs(num_workers, all_results, all_ids, all_stats):
    """Merge per-worker CSV files into the main scrape_results.csv and checkpoint."""
    # Combine stats
    merged_stats = {"guess_hits": 0, "search_hits": 0, "no_url": 0}
    for s in all_stats:
        for k in merged_stats:
            merged_stats[k] += s.get(k, 0)

    # Flatten results and IDs
    merged_results = []
    merged_ids = []
    for r in all_results:
        merged_results.extend(r)
    for ids in all_ids:
        merged_ids.extend(ids)

    # Clean up worker files
    for wid in range(num_workers):
        wf = SCRAPE_OUTPUT_FILE.replace(".csv", f"_w{wid}.csv")
        if os.path.exists(wf):
            os.remove(wf)

    return merged_results, merged_ids, merged_stats


def main():
    parser = argparse.ArgumentParser(description="Web scraping pipeline for NAICS validation")
    parser.add_argument("--pilot", type=int, default=None,
                        help="Number of records to process (omit for all)")
    parser.add_argument("--all", action="store_true",
                        help="Process all unique companies")
    parser.add_argument("--strategy", choices=["diverse", "sequential"], default="diverse",
                        help="Sampling strategy (default: diverse)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records to process this run (for batching)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers (default: 1)")
    parser.add_argument("--no-brave", "--no-search", action="store_true",
                        dest="no_search",
                        help="Skip search API tier (guess-only mode)")
    parser.add_argument("--retry-no-website", action="store_true",
                        help="Re-process NO_WEBSITE records with search API")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Re-process NO_WEBSITE and SCRAPE_FAILED records")
    args = parser.parse_args()

    # Set global search skip flag
    global SKIP_SEARCH_API
    if args.no_search:
        SKIP_SEARCH_API = True

    n = args.pilot
    if not n and not args.all:
        n = 200  # Default pilot size

    print("=" * 60)
    print("NAICS Web Scraping Pipeline (Playwright Edition)")
    print("=" * 60)

    print("Building NAICS keyword index...")
    naics_index = build_naics_keyword_index()
    print(f"  Indexed {len(naics_index):,} six-digit NAICS codes")

    print(f"Loading uncertain records from {UNCERTAIN_FILE}...")
    records = []
    with open(UNCERTAIN_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    print(f"  Loaded {len(records):,} records")

    stats = {"guess_hits": 0, "search_hits": 0, "no_url": 0}

    processed_ids = []
    results = []
    if args.resume or args.retry_no_website or args.retry_failed:
        ckpt = load_checkpoint()
        if ckpt:
            processed_ids = ckpt["processed_ids"]
            stats = ckpt.get("stats", stats)
            print(f"  Resumed: {len(processed_ids)} already processed")
            if os.path.exists(SCRAPE_OUTPUT_FILE):
                with open(SCRAPE_OUTPUT_FILE, "r", encoding="utf-8") as f:
                    results = list(csv.DictReader(f))
                print(f"  Loaded {len(results)} existing results")

            # --retry-failed: remove NO_WEBSITE and SCRAPE_FAILED IDs
            if args.retry_failed:
                retry_statuses = {"NO_WEBSITE", "SCRAPE_FAILED"}
                retry_ids = {
                    r["id"] for r in results
                    if r.get("scrape_match_status") in retry_statuses
                }
                if retry_ids:
                    processed_ids = [pid for pid in processed_ids
                                     if pid not in retry_ids]
                    results = [r for r in results
                               if r.get("scrape_match_status") not in retry_statuses]
                    stats["api_calls"] = 0
                    print(f"  Retry mode: removed {len(retry_ids)} "
                          f"NO_WEBSITE + SCRAPE_FAILED records from checkpoint")
                else:
                    print("  No failed records to retry")
                    return

            # --retry-no-website: remove NO_WEBSITE IDs from checkpoint
            # so they get reprocessed with search API enabled
            elif args.retry_no_website:
                no_website_ids = {
                    r["id"] for r in results
                    if r.get("scrape_match_status") == "NO_WEBSITE"
                }
                if no_website_ids:
                    processed_ids = [pid for pid in processed_ids
                                     if pid not in no_website_ids]
                    results = [r for r in results
                               if r.get("scrape_match_status") != "NO_WEBSITE"]
                    stats["api_calls"] = 0  # Reset API call counter
                    print(f"  Retry mode: removed {len(no_website_ids)} "
                          f"NO_WEBSITE records from checkpoint")
                else:
                    print("  No NO_WEBSITE records to retry")
                    return
        else:
            print("  No checkpoint found: starting fresh")

    print(f"\nSampling records (strategy: {args.strategy})...")
    if args.strategy == "diverse":
        sample = sample_diverse(records, n)
    else:
        sample = sample_sequential(records, n)

    if processed_ids:
        sample = [r for r in sample if r["id"] not in set(processed_ids)]
    print(f"  {len(sample)} records to process")

    if not sample:
        print("  Nothing to process!")
        return

    if SKIP_SEARCH_API:
        print(f"\nURL discovery: algorithmic guess only (search API disabled)")
    elif SEARCH_BACKEND == "searxng":
        api_budget = SEARXNG_BUDGET_PER_RUN - stats.get("api_calls", 0)
        print(f"\nURL discovery: algorithmic guess + SearXNG fallback")
        print(f"SearXNG budget remaining: {api_budget} calls")
    else:
        api_budget = BRAVE_BUDGET_PER_RUN - stats.get("api_calls", 0)
        print(f"\nURL discovery: algorithmic guess + Brave API fallback")
        print(f"Brave API budget remaining: {api_budget} calls")

    num_workers = max(1, args.workers)
    batch_limit = args.limit

    # Apply batch limit to sample before partitioning
    if batch_limit and len(sample) > batch_limit:
        sample = sample[:batch_limit]

    if num_workers > 1:
        # ---- PARALLEL MODE ----
        print(f"\nParallel mode: {num_workers} workers, {len(sample)} records total")
        print(f"Starting in 3 seconds...\n")
        time.sleep(3)

        # Round-robin partition so each worker gets a mix of record types
        slices = [[] for _ in range(num_workers)]
        for idx, record in enumerate(sample):
            slices[idx % num_workers].append(record)

        for wid, sl in enumerate(slices):
            print(f"  Worker {wid}: {len(sl)} records")

        start_time = time.time()

        # Spawn workers using multiprocessing.Pool
        # worker_limit=None because we already trimmed the sample
        pool = multiprocessing.Pool(processes=num_workers)
        async_results = []
        for wid in range(num_workers):
            ar = pool.apply_async(worker_process, (wid, slices[wid], None, SKIP_SEARCH_API))
            async_results.append(ar)
        pool.close()
        pool.join()

        # Collect results from all workers
        all_results = []
        all_ids = []
        all_stats = []
        for wid, ar in enumerate(async_results):
            try:
                w_results, w_ids, w_stats = ar.get()
                all_results.append(w_results)
                all_ids.append(w_ids)
                all_stats.append(w_stats)
            except Exception as e:
                print(f"  [ERROR] Worker {wid} failed: {e}")
                all_results.append([])
                all_ids.append([])
                all_stats.append({"guess_hits": 0, "search_hits": 0, "no_url": 0})

        # Merge
        new_results, new_ids, stats = merge_worker_outputs(
            num_workers, all_results, all_ids, all_stats
        )
        results.extend(new_results)
        processed_ids.extend(new_ids)

        save_checkpoint(processed_ids, stats, results)
        save_results_csv(results)

    else:
        # ---- SEQUENTIAL MODE (original) ----
        print(f"Starting in 3 seconds...\n")
        time.sleep(3)

        start_time = time.time()
        batch_count = 0
        for i, record in enumerate(sample, 1):
            rec_id = record["id"]
            company = (record.get("company_name") or
                       record.get("establishment_name") or "?").replace("_", " ")
            print(f"[{i}/{len(sample)}] {company} (ID: {rec_id})")

            try:
                result = process_record(record, naics_index, stats)
                results.append(result)
                processed_ids.append(rec_id)
                batch_count += 1
                # Write CSV BEFORE checkpoint: if crash occurs between the
                # two, we lose the checkpoint entry but keep the CSV row.
                # On resume, the record will be reprocessed (safe duplicate)
                # rather than silently lost.
                append_result_csv(result)
                save_checkpoint(processed_ids, stats, results)

            except Exception as e:
                print(f"  [FATAL ERROR] {e}")
                ci = classify_naics_code(record.get("naics_code", ""), naics_index)
                result = dict(record)
                result.update({
                    "naics_code_status": ci["status"],
                    "naics_code_note": ci["suggestion"] or "",
                    "website_url": "", "url_source": "",
                    "scraped_keywords": "", "suggested_naics": "",
                    "suggested_description": "", "scrape_confidence": "",
                    "scrape_reasoning": f"Processing error: {e}",
                    "scrape_match_status": "SCRAPE_FAILED",
                })
                results.append(result)
                processed_ids.append(rec_id)
                batch_count += 1
                append_result_csv(result)
                save_checkpoint(processed_ids, stats, results)

            if i % 10 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed * 60 if elapsed > 0 else 0
                api_used = stats.get("api_calls", 0)
                api_budget = (SEARXNG_BUDGET_PER_RUN if SEARCH_BACKEND == "searxng"
                              else BRAVE_BUDGET_PER_RUN)
                backend_name = "SearXNG" if SEARCH_BACKEND == "searxng" else "Brave"
                print(f"\n  --- Progress: {i}/{len(sample)} | "
                      f"{rate:.1f} records/min | "
                      f"guess={stats['guess_hits']} search={stats['search_hits']} "
                      f"miss={stats['no_url']} | "
                      f"{backend_name} calls: {api_used}/{api_budget} ---\n")
                # Machine-readable heartbeat for bot monitoring
                heartbeat = {
                    "type": "heartbeat",
                    "worker": 0,
                    "progress": f"{batch_count}/{len(sample)}",
                    "pct": round(batch_count / len(sample) * 100, 1),
                    "rate_per_min": round(rate, 1),
                    "elapsed_min": round(elapsed / 60, 1),
                    "guess": stats["guess_hits"],
                    "search": stats["search_hits"],
                    "miss": stats["no_url"],
                    "api_calls": api_used,
                }
                print(f"HEARTBEAT: {json.dumps(heartbeat)}")

        # Cleanup
        _close_browser()

        save_checkpoint(processed_ids, stats, results)
        save_results_csv(results)

    # Summary
    elapsed = time.time() - start_time
    status_counts = Counter(r.get("scrape_match_status", "UNKNOWN") for r in results)

    print("\n" + "=" * 60)
    print("SCRAPING SUMMARY")
    print("=" * 60)
    print(f"  Total processed:    {len(results)}")
    print(f"  Workers:            {num_workers}")
    print(f"  Time elapsed:       {elapsed/60:.1f} minutes")
    print()
    print("  URL discovery:")
    total_found = stats["guess_hits"] + stats["search_hits"]
    print(f"    Algorithmic guess: {stats['guess_hits']}")
    print(f"    Brave API search: {stats['search_hits']}")
    print(f"    No URL found:     {stats['no_url']}")
    if len(results) > 0:
        print(f"    Discovery rate:   {total_found}/{len(results)} "
              f"({total_found/len(results)*100:.1f}%)")
    print()
    print("  Results breakdown:")
    for status in ["CONFIRMED", "SUGGESTED", "UNCERTAIN", "NO_WEBSITE", "SCRAPE_FAILED"]:
        count = status_counts.get(status, 0)
        pct = count / len(results) * 100 if results else 0
        print(f"    {status:15s} {count:5d} ({pct:5.1f}%)")

    code_status_counts = Counter(r.get("naics_code_status", "unknown") for r in results)
    print("\n  NAICS code status:")
    for cs in ["current", "retired", "invalid"]:
        count = code_status_counts.get(cs, 0)
        pct = count / len(results) * 100 if results else 0
        print(f"    {cs:10s} {count:5d} ({pct:5.1f}%)")

    has_url = sum(1 for r in results if r.get("website_url"))
    if has_url > 0:
        scraped_ok = sum(1 for r in results
                         if r.get("website_url") and
                         r.get("scrape_match_status") not in ("SCRAPE_FAILED",))
        print(f"\n  Scrape success rate: {scraped_ok}/{has_url} "
              f"({scraped_ok/has_url*100:.1f}%)")

    api_used = stats.get("api_calls", 0)
    print(f"\n  Brave API calls used: {api_used}/{BRAVE_BUDGET_PER_RUN}")
    print(f"\n  Output: {SCRAPE_OUTPUT_FILE}")
    print(f"  Checkpoint: {CHECKPOINT_FILE}")


if __name__ == "__main__":
    main()
