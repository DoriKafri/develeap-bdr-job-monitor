#!/usr/bin/env python3
"""
Develeap BDR — LinkedIn FTS (Full-Text Search) Runner
=====================================================
Standalone process that slowly cycles through ALL FTS keyword categories,
one query at a time, with long random delays to stay under LinkedIn's radar.

Design:
  - Iterates through every category and every query (no rotation/skipping)
  - Generous random delays between searches (60-180 s by default)
  - Saves discovered posts to  fts_results.json  (append-only staging file)
  - The main pipeline (update_jobs.py) picks up results from that file
  - When a full cycle completes, it starts over from the beginning
  - State tracked in  fts_runner_state.json  so it can resume after restart

Usage:
  python fts_runner.py                      # Run one full cycle
  python fts_runner.py --continuous         # Loop forever (cycle after cycle)
  python fts_runner.py --min-delay 90 --max-delay 240   # Custom delay range
"""

import os
import re
import json
import sys
import time
import random
import logging
import argparse
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FTS] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fts_runner")

# ── Configuration ────────────────────────────────────────────────────────
SERPAPI_KEY    = os.environ.get("SERPAPI_KEY", "")
GOOGLE_CSE_KEY = os.environ.get("GOOGLE_CSE_KEY", "")
GOOGLE_CSE_CX  = os.environ.get("GOOGLE_CSE_CX", "")
BING_SEARCH_KEY = os.environ.get("BING_SEARCH_KEY", "")

_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH   = os.path.join(_DIR, "fts_runner_state.json")
RESULTS_PATH = os.path.join(_DIR, "fts_results.json")

# Default delay range between searches (seconds)
DEFAULT_MIN_DELAY = 60
DEFAULT_MAX_DELAY = 180

# ── FTS Query Definitions ────────────────────────────────────────────────
# Same categories & queries as update_jobs.py — single source of truth
LINKEDIN_FTS_QUERIES_PER_CATEGORY = {
    "devops": [
        'site:linkedin.com/posts "DevOps" hiring Israel',
        'site:linkedin.com/posts "DevOps Engineer" Israel',
        'site:linkedin.com/posts DevOps Israel "open role" OR "open position" OR "come work"',
        'site:linkedin.com/posts DevOps Israel "job alert" OR "is hiring" OR "we need"',
    ],
    "ai": [
        'site:linkedin.com/posts "AI Engineer" hiring Israel',
        'site:linkedin.com/posts "Machine Learning" hiring Israel',
        'site:linkedin.com/posts MLOps hiring Israel',
        'site:linkedin.com/posts "AI" Israel "hiring" OR "open role" OR "come work"',
    ],
    "cloud": [
        'site:linkedin.com/posts "Cloud Engineer" hiring Israel',
        'site:linkedin.com/posts "Cloud Architect" hiring Israel',
        'site:linkedin.com/posts cloud Israel "hiring" OR "open role" OR "job alert"',
        'site:linkedin.com/posts "Cloud" Israel "is hiring" OR "come work" OR "we need"',
    ],
    "platform": [
        'site:linkedin.com/posts "Platform Engineer" hiring Israel',
        'site:linkedin.com/posts "Platform Engineer" Israel',
        'site:linkedin.com/posts "Developer Platform" hiring Israel',
        'site:linkedin.com/posts platform engineer Israel "open role" OR "job alert" OR "come work"',
    ],
    "sre": [
        'site:linkedin.com/posts SRE hiring Israel',
        'site:linkedin.com/posts "Site Reliability" Israel hiring',
        'site:linkedin.com/posts SRE Israel "open role" OR "is hiring" OR "come work"',
    ],
    "security": [
        'site:linkedin.com/posts "Security Engineer" hiring Israel',
        'site:linkedin.com/posts "DevSecOps" Israel hiring',
        'site:linkedin.com/posts "Security Engineer" Israel "open role" OR "job alert" OR "come work"',
    ],
    "data": [
        'site:linkedin.com/posts "Data Engineer" hiring Israel',
        'site:linkedin.com/posts "Data Platform" Israel hiring',
        'site:linkedin.com/posts "Data Engineer" Israel "open role" OR "job alert" OR "come work"',
    ],
    "finops": [
        'site:linkedin.com/posts FinOps hiring Israel',
        'site:linkedin.com/posts "Cloud Cost" Israel hiring',
        'site:linkedin.com/posts FinOps Israel "open role" OR "is hiring" OR "come work"',
    ],
    "agentic": [
        'site:linkedin.com/posts "Agentic" hiring Israel',
        'site:linkedin.com/posts "AI Agent" Israel hiring',
        'site:linkedin.com/posts "Agentic" Israel "open role" OR "is hiring" OR "come work"',
    ],
}

# Category keywords for fallback role title extraction
CATEGORY_KEYWORDS = {
    "devops":   ["devops", "ci/cd", "kubernetes", "terraform", "jenkins", "docker", "helm", "argo", "ansible", "gitops"],
    "ai":       ["ai", "machine learning", "ml", "deep learning", "nlp", "computer vision", "llm", "gpt", "mlops"],
    "cloud":    ["cloud", "aws", "azure", "gcp", "cloud architect", "cloud engineer"],
    "platform": ["platform engineer", "developer experience", "internal developer", "developer platform"],
    "sre":      ["sre", "site reliability", "reliability engineer", "on-call", "incident"],
    "security": ["security engineer", "devsecops", "appsec", "cloud security", "infosec"],
    "data":     ["data engineer", "data platform", "data infrastructure", "etl", "data pipeline", "dataops"],
    "finops":   ["finops", "cloud cost", "cost optimization", "cloud financial", "cloud economics"],
    "agentic":  ["agentic", "ai agent", "autonomous agent", "agent framework"],
}

# ── Search Engine Functions ──────────────────────────────────────────────
# Minimal copies — only what FTS needs, no heavy dependencies on update_jobs.py

def search_google_cse(query: str, date_restrict: str = "") -> list[dict]:
    """Google Custom Search Engine (free tier: 100 queries/day)."""
    if not GOOGLE_CSE_KEY or not GOOGLE_CSE_CX:
        return []
    try:
        params = {
            "q": query, "key": GOOGLE_CSE_KEY, "cx": GOOGLE_CSE_CX,
            "num": 10, "gl": "il", "hl": "en",
        }
        if date_restrict:
            params["dateRestrict"] = date_restrict
        resp = requests.get("https://www.googleapis.com/customsearch/v1",
                            params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [{"title": i.get("title", ""), "snippet": i.get("snippet", ""), "url": i.get("link", "")}
                for i in data.get("items", [])]
    except Exception as e:
        log.warning(f"Google CSE failed: {e}")
        return []


def search_bing(query: str, freshness: str = "") -> list[dict]:
    """Bing Web Search API (free tier: 1000 calls/month)."""
    if not BING_SEARCH_KEY:
        return []
    try:
        headers = {"Ocp-Apim-Subscription-Key": BING_SEARCH_KEY}
        params = {"q": query, "count": 10, "mkt": "en-IL"}
        if freshness:
            params["freshness"] = freshness
        resp = requests.get("https://api.bing.microsoft.com/v7.0/search",
                            headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [{"title": i.get("name", ""), "snippet": i.get("snippet", ""), "url": i.get("url", "")}
                for i in data.get("webPages", {}).get("value", [])]
    except Exception as e:
        log.warning(f"Bing failed: {e}")
        return []


def search_serpapi(query: str, tbs: str = "") -> list[dict]:
    """SerpAPI Google search."""
    if not SERPAPI_KEY:
        return []
    try:
        params = {"q": query, "api_key": SERPAPI_KEY, "num": 10, "gl": "il", "hl": "en"}
        if tbs:
            params["tbs"] = tbs
        resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
        results = []
        for r in data.get("organic_results", []):
            snippet = r.get("snippet", "")
            rich = r.get("rich_snippet", {})
            if rich:
                for v in rich.values():
                    if isinstance(v, dict):
                        for sv in v.values():
                            if isinstance(sv, str) and sv not in snippet:
                                snippet = f"{snippet} {sv}"
            results.append({"title": r.get("title", ""), "snippet": snippet, "url": r.get("link", "")})
        return results
    except Exception as e:
        log.warning(f"SerpAPI failed: {e}")
        return []


def search_duckduckgo(query: str, timelimit: str = "") -> list[dict]:
    """DuckDuckGo HTML search (no API key needed)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        url_str = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        if timelimit:
            url_str += f"&df={quote_plus(timelimit)}"
        resp = requests.get(url_str, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for r in soup.select(".result"):
            title_el = r.select_one(".result__a")
            snippet_el = r.select_one(".result__snippet")
            if title_el:
                url = title_el.get("href", "")
                if "uddg=" in url:
                    from urllib.parse import parse_qs, urlparse
                    parsed = urlparse(url)
                    qs = parse_qs(parsed.query)
                    url = qs.get("uddg", [url])[0]
                results.append({
                    "title": title_el.get_text(strip=True),
                    "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                    "url": url,
                })
        return results[:10]
    except Exception as e:
        log.warning(f"DuckDuckGo failed: {e}")
        return []


def fts_search_one_engine(query: str) -> list[dict]:
    """Search ONE engine per query (round-robin) to minimize footprint.

    Instead of hitting all 4 engines per query (like the main pipeline),
    we pick a single engine randomly — spreading load across engines over time.
    This is the key "under the radar" strategy.
    """
    engines = []
    if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
        engines.append(("google_cse", lambda q: search_google_cse(q, date_restrict="m1")))
    if SERPAPI_KEY:
        engines.append(("serpapi", lambda q: search_serpapi(q, tbs="qdr:m1")))
    if BING_SEARCH_KEY:
        engines.append(("bing", lambda q: search_bing(q, freshness="Month")))
    # DuckDuckGo always available
    engines.append(("duckduckgo", lambda q: search_duckduckgo(q, timelimit="m")))

    if not engines:
        return []

    # Pick a random engine
    name, fn = random.choice(engines)
    log.info(f"    Engine: {name}")
    try:
        return fn(query)
    except Exception as e:
        log.warning(f"    {name} failed: {e}")
        return []


# ── LinkedIn Post Extraction (same logic as update_jobs.py) ──────────────

def _extract_linkedin_activity_date(url: str) -> str | None:
    """Extract post date from LinkedIn activity ID (Snowflake-like ID >> 22)."""
    m = re.search(r'activity-(\d{15,25})', url)
    if not m:
        return None
    try:
        activity_id = int(m.group(1))
        ts_ms = activity_id >> 22
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        if dt.year < 2020 or dt > now + timedelta(days=1):
            return None
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return None


def extract_fts_job_info(title: str, snippet: str, url: str) -> dict | None:
    """Extract job info from a LinkedIn post search result.

    Returns a dict with title, company, snippet, url or None if not valid.
    """
    combined = f"{title} {snippet}".lower()

    # Must be a LinkedIn post URL
    if "linkedin.com/posts/" not in url.lower() and "linkedin.com/feed/" not in url.lower():
        return None

    # Age gate: reject posts > 14 days old
    activity_date = _extract_linkedin_activity_date(url)
    if activity_date:
        try:
            post_dt = datetime.strptime(activity_date, "%Y-%m-%d")
            age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt).days
            if age_days > 14:
                return None
        except ValueError:
            pass

    # Fallback age check from snippet text
    age_match = re.search(r'\b(\d+)\s*(yr|year|mo|month|w|wk|week)s?\b', f"{title} {snippet}", re.IGNORECASE)
    if age_match:
        num = int(age_match.group(1))
        unit = age_match.group(2).lower()
        if unit in ("yr", "year", "mo", "month"):
            return None
        if unit in ("w", "wk", "week") and num > 2:
            return None

    # Must contain hiring signals
    hiring_signals = [
        "hiring", "is hiring", "we're hiring", "we are hiring", "join our team",
        "looking for", "open position", "open role", "new role",
        "come join", "join us", "growing our team", "expanding our team",
        "new opening", "hot job", "dream team", "seeking a",
        "come work with", "work with me", "work with us",
        "apply now", "apply here", "we need", "searching for",
        "position available", "role available", "opportunity",
        "talent acquisition", "recruiting", "want to join",
        "job alert", "great company", "amazing team",
        "remote position", "need a", "needs a",
    ]
    if not any(sig in combined for sig in hiring_signals):
        return None

    # Extract company name
    company = ""
    _from_hiring_context = False
    company_match = re.search(r'^(.+?)\s+posted\s+on\s+LinkedIn', title)
    if company_match:
        company = company_match.group(1).strip()
    else:
        at_match = re.search(r'(?:at|@|\|)\s+([A-Z][^:|\-]+?)(?:\s*[-:|]|\s+on\s+LinkedIn)', title)
        if at_match:
            company = at_match.group(1).strip()
        else:
            snip_match = re.search(
                r'^(?:at\s+)?([A-Z][A-Za-z0-9\s&.\-]+?)(?:\s*\([^)]*\)\s*)?'
                r'(?:\s*,\s*we|\s+is\s+(?:hiring|looking|growing|expanding))',
                snippet, re.IGNORECASE)
            if snip_match:
                company = snip_match.group(1).strip()
                company = re.sub(r'^(?:at)\s+', '', company, flags=re.IGNORECASE).strip()
                _from_hiring_context = True
        if not company:
            body_match = re.search(
                r'([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+)*)\s+(?:is\s+hiring|needs\s+a)',
                f"{title} {snippet}", re.IGNORECASE)
            if body_match:
                company = body_match.group(1).strip()
                _from_hiring_context = True

    if company:
        company = re.sub(r'\s+on\s+LinkedIn.*', '', company).strip()
        company = re.sub(r'\s*\|.*', '', company).strip()
        if not _from_hiring_context and re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', company):
            company = ""

    # Extract job title
    job_title = ""
    role_match = re.search(
        r'(?:hiring\s+(?:a\s+)?|looking\s+for\s+(?:a\s+)?|open\s+(?:role|position)\s*[-:]\s*|'
        r'seeking\s+(?:a\s+)?|new\s+role\s*[-:]\s*)'
        r'([A-Z][A-Za-z/\s&]+?)(?:\s+in\s+|\s+at\s+|\s*[!.,\-]|\s+to\s+|\s+who\s+|$)',
        f"{title} {snippet}"
    )
    if role_match:
        job_title = role_match.group(1).strip()
        job_title = re.sub(r'\s+(?:to|in|at|for|who|that|with)$', '', job_title, flags=re.IGNORECASE)
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', job_title):
            job_title = ""

    if not job_title:
        _cat_role_titles = {
            "devops": "DevOps Engineer", "ai": "AI Engineer", "cloud": "Cloud Engineer",
            "platform": "Platform Engineer", "sre": "Site Reliability Engineer",
            "security": "Security Engineer", "data": "Data Engineer",
            "finops": "FinOps Engineer", "agentic": "AI/Agentic Engineer",
        }
        for cat, keywords in CATEGORY_KEYWORDS.items():
            for kw in keywords:
                if kw.lower() in combined:
                    job_title = _cat_role_titles.get(cat, kw.title())
                    break
            if job_title:
                break

    if not job_title:
        return None

    display_title = f"{job_title} at {company}" if company else job_title

    # Extract author
    fts_author = ""
    author_match = re.search(r'^(.+?)\s+(?:posted\s+)?on\s+LinkedIn', title)
    if author_match:
        raw_author = author_match.group(1).strip()
        raw_author = re.sub(r'\s+(?:at|@|\|)\s+.*$', '', raw_author).strip()
        name_parts = raw_author.split()
        if 2 <= len(name_parts) <= 4 and all(p[0].isupper() for p in name_parts if p):
            fts_author = raw_author

    # Author LinkedIn URL
    fts_author_linkedin = ""
    if fts_author:
        post_url_match = re.search(r'linkedin\.com/posts/([a-zA-Z0-9\-]+?)[-_](?:activity|ugcPost)', url)
        if post_url_match:
            fts_author_linkedin = f"https://www.linkedin.com/in/{post_url_match.group(1)}/"

    # External job URL from snippet
    fts_job_url = ""
    job_link_domains = [
        "greenhouse.io", "lever.co", "ashbyhq.com", "comeet.com",
        "myworkdayjobs.com", "jobs.lever.co", "boards.greenhouse.io",
        "apply.workable.com", "jobs.ashbyhq.com",
        "smartrecruiters.com", "breezy.hr", "recruitee.com",
        "bamboohr.com", "icims.com", "jobvite.com",
    ]
    url_pattern = re.findall(r'https?://[^\s<>"\')\]]+', f"{title} {snippet}")
    for found_url in url_pattern:
        if any(d in found_url.lower() for d in job_link_domains):
            fts_job_url = found_url
            break

    desc = snippet[:120] if snippet else title[:120]

    return {
        "title": display_title[:80],
        "snippet": desc,
        "url": url,
        "company": company or "Unknown",
        "_source_override": "linkedin_fts",
        "_fts_author": fts_author,
        "_fts_author_linkedin": fts_author_linkedin,
        "_fts_job_url": fts_job_url,
        "_discovered_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ── State Management ─────────────────────────────────────────────────────

def _load_state() -> dict:
    """Load runner state: which category/query index we're at, seen URLs."""
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "cat_index": 0,        # Current category index
        "query_index": 0,      # Current query index within category
        "cycle_count": 0,      # How many full cycles completed
        "seen_urls": [],       # Dedup window (last 1000 URLs)
        "last_run": None,      # ISO timestamp of last search
    }


def _save_state(state: dict):
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.warning(f"Could not save state: {e}")


def _load_results() -> list[dict]:
    """Load existing FTS results staging file."""
    if os.path.exists(RESULTS_PATH):
        try:
            with open(RESULTS_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save_results(results: list[dict]):
    """Save FTS results staging file (the main pipeline reads this)."""
    try:
        with open(RESULTS_PATH, "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Could not save results: {e}")


# ── Build Ordered Query List ─────────────────────────────────────────────

def _build_query_plan() -> list[tuple[str, str]]:
    """Build a flat list of (category, query) tuples, shuffled within each category."""
    plan = []
    cats = list(LINKEDIN_FTS_QUERIES_PER_CATEGORY.keys())
    random.shuffle(cats)  # Shuffle category order each cycle
    for cat in cats:
        queries = list(LINKEDIN_FTS_QUERIES_PER_CATEGORY[cat])
        random.shuffle(queries)  # Shuffle query order within category
        for q in queries:
            plan.append((cat, q))
    return plan


# ── Main Runner ──────────────────────────────────────────────────────────

def run_cycle(min_delay: int, max_delay: int, single_query: bool = False):
    """Run one full cycle through all categories and queries.

    If single_query=True, only execute one query and exit (for GitHub Actions).
    """
    state = _load_state()
    seen_urls = set(state.get("seen_urls", [])[-1000:])
    existing_results = _load_results()
    existing_urls = {r["url"] for r in existing_results}

    # Build the query plan (all categories × all queries)
    plan = _build_query_plan()
    total_queries = len(plan)

    # Resume from where we left off
    start_index = state.get("query_index", 0)
    if start_index >= total_queries:
        start_index = 0
        state["cycle_count"] = state.get("cycle_count", 0) + 1

    log.info(f"=== FTS Runner: cycle #{state.get('cycle_count', 0) + 1} ===")
    log.info(f"Total queries: {total_queries}, starting from index {start_index}")

    engines_available = []
    if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
        engines_available.append("Google CSE")
    if SERPAPI_KEY:
        engines_available.append("SerpAPI")
    if BING_SEARCH_KEY:
        engines_available.append("Bing")
    engines_available.append("DuckDuckGo")
    log.info(f"Engines available: {', '.join(engines_available)}")

    new_found = 0

    for i in range(start_index, total_queries):
        cat, query = plan[i]
        progress = f"[{i + 1}/{total_queries}]"
        log.info(f"{progress} Category: {cat}")
        log.info(f"{progress} Query: {query}")

        # Search with ONE random engine
        results = fts_search_one_engine(query)
        log.info(f"{progress} Raw results: {len(results)}")

        # Process results
        for r in results:
            url = r.get("url", "")
            if url in seen_urls or url in existing_urls:
                continue
            seen_urls.add(url)

            job_info = extract_fts_job_info(r.get("title", ""), r.get("snippet", ""), url)
            if job_info:
                existing_results.append(job_info)
                existing_urls.add(url)
                new_found += 1
                log.info(f"  ✓ Found: {job_info['title'][:60]}")

        # Save state after each query (resume-safe)
        state["query_index"] = i + 1
        state["seen_urls"] = list(seen_urls)[-1000:]
        state["last_run"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _save_state(state)
        _save_results(existing_results)

        if single_query:
            log.info(f"Single-query mode: done. Found {new_found} new posts.")
            return new_found

        # Check if this is the last query
        if i < total_queries - 1:
            delay = random.uniform(min_delay, max_delay)
            log.info(f"{progress} Sleeping {delay:.0f}s before next query...")
            time.sleep(delay)

    # Full cycle complete
    state["query_index"] = 0
    state["cycle_count"] = state.get("cycle_count", 0) + 1
    _save_state(state)

    log.info(f"=== Cycle complete! Found {new_found} new posts. Total stored: {len(existing_results)} ===")
    return new_found


def run_continuous(min_delay: int, max_delay: int):
    """Run cycles forever, with a longer pause between cycles."""
    while True:
        try:
            run_cycle(min_delay, max_delay)
        except KeyboardInterrupt:
            log.info("Interrupted by user. Saving state and exiting.")
            break
        except Exception as e:
            log.error(f"Cycle failed: {e}", exc_info=True)

        # Pause between cycles (5-10 minutes)
        between_delay = random.uniform(300, 600)
        log.info(f"Cycle done. Sleeping {between_delay / 60:.1f} min before next cycle...")
        try:
            time.sleep(between_delay)
        except KeyboardInterrupt:
            log.info("Interrupted. Exiting.")
            break


# ── CLI Entry Point ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LinkedIn FTS Runner — slow keyword cycling")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously (cycle after cycle)")
    parser.add_argument("--single-query", action="store_true",
                        help="Execute just one query and exit (for GH Actions cron)")
    parser.add_argument("--min-delay", type=int, default=DEFAULT_MIN_DELAY,
                        help=f"Min delay between queries in seconds (default: {DEFAULT_MIN_DELAY})")
    parser.add_argument("--max-delay", type=int, default=DEFAULT_MAX_DELAY,
                        help=f"Max delay between queries in seconds (default: {DEFAULT_MAX_DELAY})")
    parser.add_argument("--reset", action="store_true",
                        help="Reset state and start fresh")
    args = parser.parse_args()

    if args.reset:
        log.info("Resetting state...")
        _save_state({"cat_index": 0, "query_index": 0, "cycle_count": 0, "seen_urls": [], "last_run": None})
        log.info("State reset. Run again without --reset to start.")
        return

    if args.single_query:
        run_cycle(args.min_delay, args.max_delay, single_query=True)
    elif args.continuous:
        run_continuous(args.min_delay, args.max_delay)
    else:
        run_cycle(args.min_delay, args.max_delay)


if __name__ == "__main__":
    main()
