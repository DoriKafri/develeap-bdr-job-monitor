#!/usr/bin/env python3
"""
Develeap BDR Job Monitor — Automated Update Script
Searches Israeli job boards, updates the HTML dashboard, deploys to Netlify,
and posts new listings to Slack #bdr-updates.
"""

import os
import re
import json
import time
import random
import hashlib
import zipfile
import io
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────
NETLIFY_SITE_ID = os.environ.get("NETLIFY_SITE_ID", "9533027e-5008-40ca-924c-dede933f0473")
NETLIFY_TOKEN = os.environ.get("NETLIFY_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")  # Optional: for better search results
DASHBOARD_PATH = os.environ.get("DASHBOARD_PATH", "dashboard/index.html")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Develeap customers (case-insensitive partial match) ────────────────────
DEVELEAP_CUSTOMERS = [
    "Akamai","Alzai","Amsalem Tours","Apester","AppsFlyer","Aqua","Armo","Ascending",
    "Autodesk","Automarky","BYON","Beacon Security","Blink Aid","Bluespine","Bond",
    "BridgeOver","Cal","Carebox","Cellebrite","Cellosign","Checkin Travels","Checkmarx",
    "Checkpoint","Cibus","CitrusX","Civ Robotics","Cloudzone","Cruise","Ctera","Curated-ai",
    "CurveTech","CyberArk","CyberRidge","Cylus","DriveTech","Edwards","Elmodis","Empathy",
    "Evogene","Ezbob","Fireblocks","Flexor","Foretellix","Gloat","Grain Finance","Harmonic",
    "Hexagon","Honeywell","Hyp","Imagry","Infinpoint","InfluenceAI","Inuitive","Isracard",
    "JFrog","Jedify","Knostic","LedderTech","Legion","Linx security","Matrix","Megureit",
    "Mobileye","Monday.com","monday.com","N2WS","NSO","NeoTech","Ness","NetNut","Networx",
    "Nintex","Nuvo cares","Odysight","OwlDuet","OwnPlay","Per-me","Philips","Pillar Security",
    "Planet9","Plus500","PrettyDamnQuick","Proceed","ProofPoint","Puzzlesoft","R.R Systems",
    "RSI","RapidAPI","Rapyd","Redis","Redwood","Revelator","Scytale","Sentrycs","Sightec",
    "Simplex3d","SkyCash","Solidus","Tactile","TailorMed","Transmit Security","Tufin","Vcita",
    "Verbit","Verifood","Vorlon","WalkMe","XMCyber","Zafran","Zerto","Zimark","eXLGx",
    "mPrest","Ness Technologies"
]

SEARCH_QUERIES = [
    "DevOps Engineer Israel hiring",
    "AI Engineer Israel job",
    "Agentic Developer Israel job",
    "Platform Engineer Israel hiring",
    "MLOps Engineer Israel",
    "Cloud Engineer Israel DevOps",
    "SRE Israel job",
    "site:linkedin.com/jobs DevOps Israel",
    "site:linkedin.com/jobs AI Engineer Israel",
    'site:alljobs.co.il "DevOps"',
    'site:drushim.co.il "DevOps" OR "AI Engineer"',
    "site:glassdoor.com DevOps Engineer Israel",
]

CATEGORY_KEYWORDS = {
    "agentic": ["agentic", "agent", "llm agent", "autonomous agent", "ai agent", "sales agent"],
    "ai": ["ai engineer", "machine learning", "ml engineer", "mlops", "data scientist",
            "deep learning", "nlp", "llm", "generative ai", "genai", "artificial intelligence"],
    "devops": ["devops", "sre", "site reliability", "platform engineer", "cloud engineer",
               "infrastructure", "ci/cd", "kubernetes", "terraform", "devsecops"],
}

SOURCE_MAP = {
    "linkedin.com": "linkedin",
    "glassdoor.com": "glassdoor",
    "alljobs.co.il": "alljobs",
    "drushim.co.il": "drushim",
    "builtin.com": "builtin",
    "facebook.com": "facebook",
    "t.me": "telegram",
    "goozali": "goozali",
}


# ── Search Functions ───────────────────────────────────────────────────────

def search_serpapi(query: str) -> list[dict]:
    """Search using SerpAPI (free tier: 100/month)."""
    if not SERPAPI_KEY:
        return []
    try:
        resp = requests.get("https://serpapi.com/search", params={
            "q": query, "api_key": SERPAPI_KEY, "gl": "il", "hl": "en", "num": 10
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for r in data.get("organic_results", []):
            results.append({
                "title": r.get("title", ""),
                "snippet": r.get("snippet", ""),
                "url": r.get("link", ""),
            })
        return results
    except Exception as e:
        log.warning(f"SerpAPI search failed: {e}")
        return []


def search_duckduckgo(query: str) -> list[dict]:
    """Search using DuckDuckGo HTML (no API key needed)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(
            f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
            headers=headers, timeout=15
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for r in soup.select(".result"):
            title_el = r.select_one(".result__a")
            snippet_el = r.select_one(".result__snippet")
            if title_el:
                url = title_el.get("href", "")
                # DuckDuckGo wraps URLs in a redirect
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
        log.warning(f"DuckDuckGo search failed for '{query}': {e}")
        return []


def search_jobs(query: str) -> list[dict]:
    """Search with SerpAPI first, fall back to DuckDuckGo."""
    results = search_serpapi(query)
    if not results:
        time.sleep(random.uniform(1.5, 3.0))  # Rate limiting
        results = search_duckduckgo(query)
    return results


# ── Parsing Functions ──────────────────────────────────────────────────────

def detect_source(url: str) -> str:
    """Detect job board source from URL."""
    url_lower = url.lower()
    for domain, source in SOURCE_MAP.items():
        if domain in url_lower:
            return source
    return "other"


def detect_category(title: str, snippet: str) -> str:
    """Detect job category from title and snippet."""
    text = f"{title} {snippet}".lower()
    # Check agentic first (more specific)
    for kw in CATEGORY_KEYWORDS["agentic"]:
        if kw in text:
            return "agentic"
    for kw in CATEGORY_KEYWORDS["ai"]:
        if kw in text:
            return "ai"
    for kw in CATEGORY_KEYWORDS["devops"]:
        if kw in text:
            return "devops"
    return "devops"  # Default


def is_develeap_customer(company: str) -> bool:
    """Check if company is a Develeap customer."""
    company_lower = company.lower()
    return any(c.lower() in company_lower for c in DEVELEAP_CUSTOMERS)


def _is_job_title(text: str) -> bool:
    """Return True if text looks like a job title rather than a company name."""
    t = text.lower().strip().rstrip(".")
    # Common job-title words / prefixes
    title_words = {
        "sr", "jr", "senior", "junior", "lead", "staff", "principal", "head",
        "chief", "director", "manager", "vp", "engineer", "developer", "architect",
        "analyst", "consultant", "specialist", "coordinator", "administrator",
        "technician", "intern", "trainee", "associate", "devops", "sre", "mlops",
        "cloud", "platform", "infrastructure", "data", "ai", "ml", "software",
        "backend", "frontend", "fullstack", "full-stack", "full stack",
        "technical", "tech", "site reliability", "security", "devsecops",
        "solution", "solutions", "product", "project", "program", "qa", "test",
        "automation", "release", "build", "deployment", "systems", "network",
        "database", "dba", "linux", "windows", "python", "java", "golang",
        "kubernetes", "terraform", "aws", "azure", "gcp", "remote", "hybrid",
        "israel", "tel aviv", "tel-aviv", "ramat gan", "herzliya", "haifa",
        "jerusalem", "beer sheva", "netanya", "petah tikva", "ra'anana",
        "hiring", "job", "jobs", "opening", "position",
        "vacancy", "career", "careers", "apply", "wanted", "looking for",
    }
    # Check if the entire text matches a known non-company phrase
    known_locations = {"tel aviv", "ramat gan", "herzliya", "haifa", "jerusalem",
                       "beer sheva", "netanya", "petah tikva", "ra'anana", "hod hasharon",
                       "israel", "remote", "hybrid", "tel aviv district", "tel aviv yaffo il",
                       "tel aviv yaffo", "il", "new", "2025", "2026", "2027"}
    if t in known_locations:
        return True

    # Contains Hebrew characters → not a valid company name for our purposes
    if re.search(r'[\u0590-\u05FF]', t):
        return True

    # Looks like a parenthetical description, not a company
    if t.startswith("(") or t.startswith("["):
        return True

    # Just a number/year
    if re.match(r'^\d+$', t):
        return True

    words = set(re.split(r"[\s/\-\.]+", t))
    # If most words are title-like, it's a job title
    if len(words) > 0 and len(words & title_words) / len(words) >= 0.5:
        return True
    # Starts with common title prefixes
    if re.match(r"^(sr\.?|jr\.?|senior|junior|lead|staff|principal|head of|chief|director)\b", t):
        return True
    return False


def extract_company(title: str, snippet: str, url: str = "") -> str:
    """Try to extract company name from search result."""

    # Helper: clean up company name casing
    def _fix_casing(name: str) -> str:
        """Fix common casing issues in extracted company names."""
        # Known abbreviations that should stay uppercase
        abbrev = {"ai", "it", "bmc", "ibm", "sap", "hp", "aws", "gcp", "nso"}
        words = name.split()
        fixed = []
        for w in words:
            if w.lower() in abbrev:
                fixed.append(w.upper())
            else:
                fixed.append(w)
        return " ".join(fixed)

    # 1. LinkedIn URL pattern: .../TITLE-at-COMPANY-1234567
    if "linkedin.com" in url:
        m = re.search(r"/jobs/view/.*?-at-(.+?)-\d{5,}", url)
        if m:
            company = _fix_casing(m.group(1).replace("-", " ").title())
            if not _is_job_title(company):
                return company

    # 1b. Known career site URL patterns: careers.COMPANY.com, jobs.COMPANY.com
    m = re.search(r"https?://(?:careers|jobs)\.([a-z0-9\-]+)\.", url)
    if m:
        domain_company = _fix_casing(m.group(1).replace("-", " ").title())
        if len(domain_company) > 2 and domain_company.lower() not in {"secret", "lhh"}:
            return domain_company

    # 1c. COMPANY.com/careers or similar career page patterns
    m = re.search(r"https?://(?:www\.)?([a-z0-9\-]+)\.(?:com|io|co\.il|ai)/.+", url)
    if m:
        domain_company = m.group(1).replace("-", " ").title()
        # Only use domain as company for known career-hosting patterns
        job_boards = {
            "builtin", "startup", "glassdoor", "indeed", "alljobs", "drushim",
            "facebook", "google", "jobify360", "machinelearning", "aidevtlv",
            "linkedin", "secrettelaviv", "aijobs", "efinancialcareers",
            "monster", "ziprecruiter", "dice", "stackoverflow", "hired",
            "angel", "wellfound", "lever", "greenhouse", "workday",
            "jobify360", "goozali", "lhh",
        }
        if len(domain_company) > 2 and domain_company.lower() not in job_boards:
            # Verify the URL looks like a career/job page, not a random page
            if re.search(r"/(careers|jobs|position|openings|join|hiring|vacancy)", url, re.IGNORECASE):
                return domain_company

    # 2. "Role at Company" pattern — use the LAST "at" in the title (strongest signal)
    m = re.search(r"\bat\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,]|\s+in\s+|\s+is\s+|\s*$)", title)
    # If there are multiple "at" matches, prefer the last one
    all_at_matches = list(re.finditer(r"\bat\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,]|\s+in\s+|\s+is\s+|\s*$)", title))
    if all_at_matches:
        m = all_at_matches[-1]
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    # 3. "Company - Role" or "Company | Role" (only if left side is NOT a job title)
    m = re.match(r"^([^-–|]{2,35}?)\s*[-–|]\s*(.+)", title)
    if m:
        left = m.group(1).strip()
        right = m.group(2).strip()
        # If left looks like a company (not a job title) → use it
        if not _is_job_title(left):
            return left
        # Otherwise try right side for "Role - Company" pattern
        # Take the last segment after the last dash/pipe
        parts = re.split(r"\s*[-–|]\s*", title)
        if len(parts) >= 2 and not _is_job_title(parts[-1].strip()):
            return parts[-1].strip()

    # 4. "Company is hiring" pattern
    m = re.search(r"([A-Z][A-Za-z0-9\.\-&]{1,25})\s+(?:is hiring|careers|jobs)", title + " " + snippet)
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    # 5. Try snippet with "at Company" pattern
    m = re.search(r"\bat\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,\.]|\s+in\s+|\s+is\s+|\s*$)", snippet)
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    return "Unknown"


def extract_location(title: str, snippet: str) -> str:
    """Extract location from text."""
    text = f"{title} {snippet}"
    locations = {
        "tel aviv": "Tel Aviv", "ramat gan": "Ramat Gan", "herzliya": "Herzliya",
        "haifa": "Haifa", "jerusalem": "Jerusalem", "beer sheva": "Beer Sheva",
        "be'er sheva": "Beer Sheva", "netanya": "Netanya", "petah tikva": "Petah Tikva",
        "ra'anana": "Ra'anana", "raanana": "Ra'anana", "hod hasharon": "Hod HaSharon",
        "remote": "Remote", "hybrid": "Hybrid",
    }
    text_lower = text.lower()
    for key, val in locations.items():
        if key in text_lower:
            return val
    return "Israel"


def parse_search_results(raw_results: list[dict]) -> list[dict]:
    """Parse raw search results into structured job listings."""
    jobs = []
    seen_urls = set()

    for r in raw_results:
        url = r.get("url", "")
        title = r.get("title", "")
        snippet = r.get("snippet", "")

        # Skip duplicates and non-job URLs
        if url in seen_urls or not url:
            continue
        seen_urls.add(url)

        # Skip results that are clearly not job listings
        title_lower = title.lower()
        skip_keywords = ["how to", "salary", "resume", "interview tips", "career advice",
                         "blog", "article", "guide", "tutorial", "top 10", "best companies",
                         "average salary", "job description template", "what is a",
                         "conference", "meetup", "event", "webinar", "course"]
        if any(kw in title_lower for kw in skip_keywords):
            continue

        # Skip Hebrew aggregator pages ("we found N job offers", "jobs wanted")
        hebrew_skip = ["מצאנו", "הצעות עבודה", "משרות אחרונות", "חיפוש משרות"]
        if any(kw in title for kw in hebrew_skip):
            continue

        # Skip search/aggregator pages — only allow individual job listing URLs
        url_lower = url.lower()
        skip_url_patterns = [
            # Search result pages
            "google.com/search", "indeed.com/q-", "indeed.com/jobs?",
            "linkedin.com/jobs/search",
            # LinkedIn job search pages (e.g. /jobs/devops-engineer-jobs)
            # Only /jobs/view/ are individual listings
            "glassdoor.com/Job/",
            # Generic job listing indexes
            "/jobs?q=", "/search?",
        ]
        if any(p in url for p in skip_url_patterns):
            continue

        # LinkedIn: only accept /jobs/view/ (individual listings)
        if "linkedin.com/jobs" in url_lower and "/jobs/view/" not in url_lower:
            continue

        # Skip generic job board index/search pages
        if re.search(r"(alljobs\.co\.il/SearchResults|drushim\.co\.il/.*\?)", url):
            continue

        # Skip pages that are clearly job indexes, not individual listings
        index_url_patterns = [
            r"/jobs/?$", r"/careers/?$", r"/openings/?$",
            r"/jobs/?\?", r"/location/", r"/locations/", r"/category/",
            r"/job-location-category/", r"/jobs/mena/",
            r"/list/", r"startup\.jobs/",
        ]
        if any(re.search(p, url_lower) for p in index_url_patterns):
            continue

        source = detect_source(url)
        category = detect_category(title, snippet)
        company = extract_company(title, snippet, url)
        location = extract_location(title, snippet)

        # Generate stable ID from URL
        job_id = hashlib.md5(url.encode()).hexdigest()[:8]

        jobs.append({
            "id": job_id,
            "title": title[:80],
            "subtitle": snippet[:60] if snippet else "",
            "company": company,
            "companyIndustry": "",
            "location": location,
            "locationSlug": location.lower().replace(" ", "-"),
            "source": source,
            "sourceUrl": url,
            "category": category,
            "posted": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "isNew": True,
            "isDeveleapCustomer": is_develeap_customer(company),
            "description": snippet[:120] if snippet else title,
            "skills": [],
        })

    return jobs


# ── Dashboard Update ───────────────────────────────────────────────────────

def load_existing_jobs(html: str) -> list[dict]:
    """Extract existing ALL_JOBS from dashboard HTML."""
    match = re.search(r'let ALL_JOBS\s*=\s*(\[.*?\]);\s*$', html, re.DOTALL | re.MULTILINE)
    if match:
        raw = match.group(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # JS objects may have unquoted keys — add quotes
            try:
                fixed = re.sub(r'(?<=[{,])\s*(\w+)\s*:', r' "\1":', raw)
                # Remove trailing commas before } or ]
                fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
                return json.loads(fixed)
            except json.JSONDecodeError:
                log.warning("Failed to parse existing ALL_JOBS, keeping as-is")
    return []


def merge_jobs(existing: list[dict], new_jobs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Merge new jobs with existing, return (merged, only_new)."""
    # Index existing by URL and company+title
    existing_urls = {j.get("sourceUrl", ""): j for j in existing if j.get("sourceUrl")}
    existing_keys = {f'{j.get("company","").lower()}|{j.get("title","").lower()}': j for j in existing}

    # Mark existing jobs as not new
    for j in existing:
        j["isNew"] = False

    truly_new = []
    for j in new_jobs:
        url = j.get("sourceUrl", "")
        key = f'{j.get("company","").lower()}|{j.get("title","").lower()}'
        if url not in existing_urls and key not in existing_keys:
            truly_new.append(j)

    merged = existing + truly_new
    # Sort by date descending
    merged.sort(key=lambda x: x.get("posted", ""), reverse=True)
    # Keep max 200 listings
    merged = merged[:200]

    return merged, truly_new


def update_dashboard_html(html: str, jobs: list[dict]) -> str:
    """Replace ALL_JOBS array and timestamp in dashboard HTML."""
    # Format jobs as JS array
    jobs_json = json.dumps(jobs, ensure_ascii=False, indent=2)
    # Replace ALL_JOBS
    html = re.sub(
        r'let ALL_JOBS\s*=\s*\[.*?\];\s*$',
        f'let ALL_JOBS = {jobs_json};',
        html,
        flags=re.DOTALL | re.MULTILINE
    )
    # Update timestamp
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = re.sub(
        r'Last updated:.*?<',
        f'Last updated: {now}<',
        html
    )
    return html


# ── Netlify Deploy ─────────────────────────────────────────────────────────

def deploy_to_netlify(html: str) -> bool:
    """Deploy dashboard HTML to Netlify."""
    if not NETLIFY_TOKEN:
        log.error("NETLIFY_TOKEN not set, skipping deploy")
        return False

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.html", html)
        # Netlify _headers file to ensure correct Content-Type
        zf.writestr("_headers", "/\n  Content-Type: text/html; charset=UTF-8\n/index.html\n  Content-Type: text/html; charset=UTF-8\n")
    buf.seek(0)

    try:
        resp = requests.post(
            f"https://api.netlify.com/api/v1/sites/{NETLIFY_SITE_ID}/deploys",
            headers={
                "Authorization": f"Bearer {NETLIFY_TOKEN}",
                "Content-Type": "application/zip",
            },
            data=buf.read(),
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        log.info(f"Deployed to {result.get('ssl_url', 'unknown')}")
        return True
    except Exception as e:
        log.error(f"Netlify deploy failed: {e}")
        return False


# ── Slack Notification ─────────────────────────────────────────────────────

def notify_slack(new_jobs: list[dict]) -> bool:
    """Post new listings to Slack #bdr-updates via incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False
    if not new_jobs:
        log.info("No new jobs to notify about")
        return True

    cat_labels = {"devops": "DevOps", "ai": "AI/ML", "agentic": "Agentic"}
    source_labels = {
        "linkedin": "LinkedIn", "glassdoor": "Glassdoor", "alljobs": "AllJobs",
        "drushim": "Drushim", "builtin": "BuiltIn", "facebook": "Facebook",
        "telegram": "Telegram", "goozali": "Goozali", "other": "Web",
    }

    if len(new_jobs) <= 5:
        # Individual messages for each job
        blocks = []
        for j in new_jobs:
            cat = cat_labels.get(j.get("category", ""), "DevOps")
            src = source_labels.get(j.get("source", ""), "Web")
            star = " :star:" if j.get("isDeveleapCustomer") else ""
            customer_line = ":rotating_light: *DEVELEAP CUSTOMER*\n" if j.get("isDeveleapCustomer") else ""

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{customer_line}"
                        f":briefcase: *New {cat} Listing*\n"
                        f"*{j['title']}* at *{j['company']}*{star}\n"
                        f":round_pushpin: {j.get('location', 'Israel')} | :link: {src}\n"
                        f"{j.get('description', '')[:100]}\n"
                        f"<{j.get('sourceUrl', '')}|View Original> | "
                        f"<https://develeap-bdr-jobs.netlify.app|View Dashboard>"
                    )
                }
            })
            blocks.append({"type": "divider"})

        payload = {"blocks": blocks}
    else:
        # Batch summary for many listings
        lines = []
        for j in new_jobs:
            cat = cat_labels.get(j.get("category", ""), "DevOps")
            star = " :star:" if j.get("isDeveleapCustomer") else ""
            lines.append(f"• *{j['title']}* at *{j['company']}* ({cat}){star}")

        payload = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f":newspaper: *{len(new_jobs)} New Job Listings Found*\n\n"
                            + "\n".join(lines[:20])
                            + ("\n..." if len(lines) > 20 else "")
                            + f"\n\n<https://develeap-bdr-jobs.netlify.app|Open Full Dashboard>"
                        )
                    }
                }
            ]
        }

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        log.info(f"Slack notification sent for {len(new_jobs)} new listings")
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    log.info("=== Develeap BDR Job Monitor Update ===")

    # 1. Search for jobs
    log.info(f"Searching with {len(SEARCH_QUERIES)} queries...")
    all_raw = []
    for query in SEARCH_QUERIES:
        results = search_jobs(query)
        all_raw.extend(results)
        log.info(f"  '{query}' → {len(results)} results")
        time.sleep(random.uniform(1.0, 2.5))

    log.info(f"Total raw results: {len(all_raw)}")

    # 2. Parse results into structured jobs
    new_jobs = parse_search_results(all_raw)
    log.info(f"Parsed {len(new_jobs)} unique job listings")

    # 3. Load existing dashboard
    if os.path.exists(DASHBOARD_PATH):
        with open(DASHBOARD_PATH, "r", encoding="utf-8") as f:
            html = f.read()
        existing = load_existing_jobs(html)
        log.info(f"Existing dashboard has {len(existing)} listings")
    else:
        log.error(f"Dashboard not found at {DASHBOARD_PATH}")
        return

    # 3b. Clean existing jobs: fix entries where company looks like a job title
    for j in existing:
        if _is_job_title(j.get("company", "")):
            # Try to re-extract from title/snippet/url
            fixed = extract_company(j.get("title", ""), j.get("description", ""), j.get("sourceUrl", ""))
            log.info(f"  Fixed company: '{j['company']}' → '{fixed}'")
            j["company"] = fixed
            j["isDeveleapCustomer"] = is_develeap_customer(fixed)

    # 4. Merge and identify new listings
    merged, truly_new = merge_jobs(existing, new_jobs)
    log.info(f"After merge: {len(merged)} total, {len(truly_new)} new")
    customer_new = [j for j in truly_new if j.get("isDeveleapCustomer")]
    if customer_new:
        log.info(f"  🌟 {len(customer_new)} new listings from Develeap customers!")

    # 5. Update dashboard HTML
    updated_html = update_dashboard_html(html, merged)
    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(updated_html)
    log.info("Dashboard HTML updated")

    # 6. Deploy to Netlify
    if deploy_to_netlify(updated_html):
        log.info("✅ Netlify deploy successful")
    else:
        log.warning("⚠️  Netlify deploy failed")

    # 7. Notify Slack
    if truly_new:
        notify_slack(truly_new)
    else:
        log.info("No new listings — skipping Slack notification")

    log.info("=== Update complete ===")


if __name__ == "__main__":
    main()
