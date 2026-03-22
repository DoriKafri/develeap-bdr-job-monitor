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
import copy
import random
import hashlib
import zipfile
import io
import html as html_mod
import base64
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus, unquote, urljoin
try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    _ZoneInfo = None

import requests
from bs4 import BeautifulSoup

# Playwright (lazy-loaded for LinkedIn fallback scraping)
_playwright_browser = None
_playwright_instance = None

def _get_playwright_browser():
    """Lazy-init a Playwright Chromium browser for LinkedIn scraping fallback.
    Returns browser instance or None if Playwright is not available."""
    global _playwright_browser, _playwright_instance
    if _playwright_browser is not None:
        return _playwright_browser
    try:
        from playwright.sync_api import sync_playwright
        _playwright_instance = sync_playwright().start()
        _playwright_browser = _playwright_instance.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        log.info("Playwright browser launched successfully")
        return _playwright_browser
    except Exception as e:
        log.warning(f"Playwright not available: {e}")
        _playwright_browser = False  # Mark as unavailable
        return None

def _shutdown_playwright():
    """Clean up Playwright resources."""
    global _playwright_browser, _playwright_instance
    try:
        if _playwright_browser and _playwright_browser is not False:
            _playwright_browser.close()
        if _playwright_instance:
            _playwright_instance.stop()
    except Exception:
        pass
    _playwright_browser = None
    _playwright_instance = None

# ── Weekend detection (Israel timezone) ────────────────────────────────────
def _is_israel_weekend() -> bool:
    """Return True during Israel weekend: Thursday 19:00 → Sunday 07:00.

    During the weekend, SerpAPI-heavy operations (Indeed, Google Jobs) are
    skipped to conserve the monthly quota.  Free engines (DuckDuckGo, Google
    CSE, Bing) still run normally.
    """
    try:
        if _ZoneInfo:
            il_tz = _ZoneInfo("Asia/Jerusalem")
            now_il = datetime.now(il_tz)
        else:
            raise RuntimeError("zoneinfo unavailable")
    except Exception:
        # Fallback: UTC+2 (Israel standard time, conservative)
        now_il = datetime.now(timezone(timedelta(hours=2)))
    weekday = now_il.weekday()   # 0=Mon … 3=Thu … 5=Sat … 6=Sun
    hour = now_il.hour
    if weekday == 3 and hour >= 19:   # Thursday after 19:00
        return True
    if weekday in (4, 5):             # Friday and Saturday (full days)
        return True
    if weekday == 6 and hour < 7:     # Sunday before 07:00
        return True
    return False


# ── Configuration ──────────────────────────────────────────────────────────
NETLIFY_SITE_ID = os.environ.get("NETLIFY_SITE_ID", "9533027e-5008-40ca-924c-dede933f0473")
NETLIFY_TOKEN = os.environ.get("NETLIFY_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")  # Optional: for better search results
GOOGLE_CSE_KEY = os.environ.get("GOOGLE_CSE_KEY", "")  # Google Custom Search API key
GOOGLE_CSE_CX = os.environ.get("GOOGLE_CSE_CX", "")    # Google Custom Search Engine ID
BING_SEARCH_KEY = os.environ.get("BING_SEARCH_KEY", "")  # Bing Web Search API key
DASHBOARD_PATH = os.environ.get("DASHBOARD_PATH", "dashboard/index.html")
SLACK_POSTED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "slack_posted.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Workflow Config ───────────────────────────────────────────────────────
WORKFLOW_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflow_config.json")

def _load_workflow_config():
    """Load workflow_config.json if it exists. Returns dict or empty dict."""
    if os.path.exists(WORKFLOW_CONFIG_PATH):
        try:
            with open(WORKFLOW_CONFIG_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Could not load workflow config: {e}")
    return {}

def _is_node_enabled(config, node_id):
    """Check if a workflow node is enabled. Defaults to True if not configured."""
    nodes = config.get("nodes", {})
    node = nodes.get(node_id, {})
    return node.get("enabled", True)

# ── Develeap customers (case-insensitive partial match) ────────────────────
DEVELEAP_CUSTOMERS = [
    "Akamai","Alzai","Amsalem Tours","Apester","Aqua","Armo","Automarky",
    "Beacon Security","Bluespine","Bond","BYON","Cal","Cellebrite","Cellosign",
    "Checkin Travels","Checkpoint","Cibus","CitrusX","Cloudzone","Ctera","Curated-ai",
    "CyberArk","CyberRidge","Cylus","DriveTech","Edwards","eXLGx","Ezbob","Flexor",
    "Foretellix","Grain Finance","Hyp","Imagry","Infinpoint","Inuitive","Isracard",
    "Jedify","Legion","Linx security","Matrix","Megureit","Mobileye","Monday.com",
    "N2WS","Ness","NetNut","Networx","Nuvo cares","Odysight","OwlDuet","Per-me",
    "Philips","Planet9","Plus500","PrettyDamnQuick","Proceed","ProofPoint","Puzzlesoft",
    "R.R Systems","Redis","Redwood","RSI","Scytale","Sightec","Simplex3d","SkyCash",
    "Solidus","Tactile","TailorMed","Transmit Security","Tufin","Vcita","Verifood",
    "Vorlon","XMCyber","Zafran","Zenity","Zerto","Zimark",
    # Added from HubSpot customer sync 2026-03-12
    "AllCloud","CreditGuard","Danel Group","Datascope","Develeap","eToro",
    "InterneTeams","Intezer","Kryon","MDClone","Metomotion","Minute Media",
    "Motorola Solutions","Newstream","Payoneer","Perimeter81","ReadTheory",
    "Sagenso","Scadafence","Sequent","Spinomenal",
]

DEVELEAP_PAST_CUSTOMERS = [
    "AppsFlyer","Autodesk","Blink Aid","BridgeOver","Carebox","Checkmarx",
    "Civ Robotics","CurveTech","Elmodis","Empathy","Evogene","Fireblocks","Gloat",
    "Harmonic","Hexagon","Honeywell","InfluenceAI","JFrog","Knostic","LedderTech",
    "mPrest","NeoTech","Nintex","NSO","OwnPlay","Pillar Security","RapidAPI",
    "Rapyd","Revelator","Sentrycs","Verbit","WalkMe",
]

# ── Indeed JK Company Cache ───────────────────────────────────────────────
# Indeed job key (jk) → company name, persisted to indeed_cache.json with timestamps.
# Indeed blocks scraping from data center IPs (both HTTP 401 and Playwright bot-detection).
# Entries older than 30 days are evicted so the pipeline can re-derive the company name.

INDEED_CACHE_FILE = "indeed_cache.json"

# Seed entries used only to initialize the cache file on first run.
_INDEED_JK_SEED: dict = {
    "179e22243d60343d": "Deloitte",
    "9b48b8e5884835b7": "AppCard",
    "7cf0120fd723666d": "Teads",
    "1e9438def9c6dc2b": "Qualitest",
    "b2737877ea70b4c0": "KPMG",
    "42f4c7e85ab2dc19": "Veeva Systems",
    "55d25a7c3e6be88e": "Intel",
    "85e8eb065b33e5ab": "Algosec",
    "df9776a4708416df": "Siemens",
    "2e19480cde96f2f2": "CyberArk",
    "990dc1d50306f4e7": "ARMO",
    "d84b944f8fccfb2d": "Qualcomm",
    "a5ed2036e1f6c18e": "Google",
    "6f063c7e2bfe7de5": "Thales",
    "df13bbfde9e4397e": "Lemonade",
    "decc1398a3f038f2": "Palo Alto Networks",
    "c85d02fdbefac12f": "Workday",
    "3495eaada87aa565": "Shield",
    "418c1ea5fb3651cc": "Glassbox",
    "22e2a55d6d2905d7": "Coralogix",
    "e2e1670544632e50": "CyberArk",
    "d67ca2fc1c4791fa": "Immunai",
    "38b32068ae735de1": "Workday",
    "3e0c9a972ce5a6ce": "Abra",
    "f64a2c1e468549e4": "Qualitest",
    "c21df977c7b5d846": "Classiq",
    "60c3eb5442e60345": "ERGO",
    "265d91706aba7be9": "Deloitte",
    "fef614e7c609fe4c": "Millennium Management",
    "b8a2f34142b6b48e": "Motorola Solutions",
    "c04481e66ceb36a3": "ONE ZERO Digital Bank",
    "bebd430761ea2bff": "KPMG",
    "ca1fa3eaca20e1f8": "Surecomp",
    "d954128d73966629": "NVIDIA",
    "1911add4ce480a7d": "Red River",
    "c004fd7a3d1c6772": "IAI - Israel Aerospace Industries",
}

_INDEED_JK_CACHE: dict = {}   # {jk: {"company": str, "updated": "YYYY-MM-DD"}}
_INDEED_JK_CACHE_DIRTY: bool = False


def _load_indeed_cache() -> dict:
    """Load indeed_cache.json, evict entries older than 30 days, return the live cache."""
    global _INDEED_JK_CACHE_DIRTY
    try:
        with open(INDEED_CACHE_FILE) as _f:
            raw = json.load(_f)
    except (FileNotFoundError, json.JSONDecodeError):
        # First run: seed from the hardcoded dict, dated today.
        _today = datetime.now(timezone.utc).date().isoformat()
        raw = {jk: {"company": co, "updated": _today} for jk, co in _INDEED_JK_SEED.items()}
        _INDEED_JK_CACHE_DIRTY = True

    _cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    kept: dict = {}
    for jk, entry in raw.items():
        if entry.get("updated", "1970-01-01") >= _cutoff:
            kept[jk] = entry
        else:
            _INDEED_JK_CACHE_DIRTY = True  # expired entries → need to save the pruned file
    return kept


def _save_indeed_cache() -> None:
    """Persist _INDEED_JK_CACHE to indeed_cache.json if the cache has changed."""
    global _INDEED_JK_CACHE_DIRTY
    if not _INDEED_JK_CACHE_DIRTY:
        return
    with open(INDEED_CACHE_FILE, "w") as _f:
        json.dump(_INDEED_JK_CACHE, _f, indent=2, sort_keys=True)
    _INDEED_JK_CACHE_DIRTY = False


def _cache_indeed_company(jk: str, company: str) -> None:
    """Add or refresh a jk→company entry in the cache."""
    global _INDEED_JK_CACHE, _INDEED_JK_CACHE_DIRTY
    _INDEED_JK_CACHE[jk] = {"company": company, "updated": datetime.now(timezone.utc).date().isoformat()}
    _INDEED_JK_CACHE_DIRTY = True


_INDEED_JK_CACHE = _load_indeed_cache()

# For companies whose website favicon doesn't work (expired SSL, no favicon, etc.)
# Maps company name (lowercase) → full logo URL
COMPANY_LOGO_OVERRIDES = {
    "nextta": "https://www.comeet.co/pub/nextta/5A.006/logo?size=medium&last-modified=1745606333",
    "nexta": "https://www.comeet.co/pub/nextta/5A.006/logo?size=medium&last-modified=1745606333",
    "elbit systems": "https://cdn.brandfetch.io/idC9T5H0p4/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1773091597994",
    "elbit systems israel": "https://cdn.brandfetch.io/idC9T5H0p4/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1773091597994",
    "elbit": "https://cdn.brandfetch.io/idC9T5H0p4/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1773091597994",
    "palo alto networks": "https://cdn.brandfetch.io/idfPYio-v-/w/400/h/400/theme/dark/icon.jpeg",
    "palo alto": "https://cdn.brandfetch.io/idfPYio-v-/w/400/h/400/theme/dark/icon.jpeg",
}

# ── Company Domains for Logo Lookup ───────────────────────────────────────
# Maps company name (lowercase) → domain for Clearbit Logo API
COMPANY_DOMAINS = {
    "allcloud": "allcloud.io",
    "amazon": "amazon.com",
    "amazon web services": "aws.amazon.com",
    "amazon web services aws": "aws.amazon.com",
    "aws": "aws.amazon.com",
    "appcharge": "appcharge.com",
    "applied materials": "appliedmaterials.com",
    "applied materials - israel": "appliedmaterials.com",
    "aqua security": "aquasec.com",
    "armis": "armis.com",
    "armissecurity": "armis.com",
    "arpeely": "arpeely.com",
    "attil": "attil.io",
    "au10tix": "au10tix.com",
    "audiocodes": "audiocodes.com",
    "augury": "augury.com",
    "biocatch": "biocatch.com",
    "blink ops": "blinkops.com",
    "bmc": "bmc.com",
    "bmc software": "bmc.com",
    "booking.com": "booking.com",
    "booking": "booking.com",
    "cato networks": "catonetworks.com",
    "chaos labs": "chaoslabs.xyz",
    "check point": "checkpoint.com",
    "check point software": "checkpoint.com",
    "checkpoint software": "checkpoint.com",
    "palo alto networks": "paloaltonetworks.com",
    "classiq": "classiq.io",
    "cloudinary": "cloudinary.com",
    "codevalue": "codevalue.net",
    "comblack": "comblack.co.il",
    "commit": "comm-it.com",
    "comm-it": "comm-it.com",
    "comm it": "comm-it.com",
    "commbox": "commbox.io",
    "commit": "comm-it.com",
    "cyberark": "cyberark.com",
    "cymulate": "cymulate.com",
    "datadog": "datadoghq.com",
    "dell": "dell.com",
    "dell technologies": "dell.com",
    "doit": "doit.com",
    "dualbird": "dualbird.com",
    "earnix": "earnix.com",
    "elbit systems israel": "elbitsystems.com",
    "elbit systems": "elbitsystems.com",
    "elbit": "elbitsystems.com",
    "mantis tech": "mantis-technology.com",
    "mantis technology": "mantis-technology.com",
    "factored": "factored.ai",
    "fetcherr": "fetcherr.io",
    "fireblocks": "fireblocks.com",
    "fiverr": "fiverr.com",
    "forter": "forter.com",
    "fundamental": "fundamental.cc",
    "global payments inc.": "globalpayments.com",
    "globallogic": "globallogic.com",
    "grafana labs": "grafana.com",
    "guidde": "guidde.com",
    "harmonya": "harmonya.com",
    "hio": "hio.store",
    "hivestack": "hivestack.com",
    "imagen": "imagen-ai.com",
    "intuit": "intuit.com",
    "intuit israel": "intuit.com",
    "jfrog": "jfrog.com",
    "kpmg": "kpmg.com",
    "leidos": "leidos.com",
    "legit security": "legitsecurity.com",
    "legitsecurity": "legitsecurity.com",
    "beacon security": "beacon.security",
    "beaconsecurity": "beacon.security",
    "bit cloud": "bit.cloud",
    "rapyd": "rapyd.net",
    "palo alto networks": "paloaltonetworks.com",
    "palo alto": "paloaltonetworks.com",
    "jobgether": "jobgether.com",
    "efinancialcareers": "efinancialcareers.com",
    "efinancialcareers norway": "efinancialcareers.com",
    "levi strauss": "levistrauss.com",
    "levi strauss & co.": "levistrauss.com",
    "levi strauss & co": "levistrauss.com",
    "levistraussandco": "levistrauss.com",
    "lightricks": "lightricks.com",
    "majestic labs": "majesticlabs.io",
    "marvin": "marvin.com",
    "mastercard": "mastercard.com",
    "matia": "matia.io",
    "metalbear": "metalbear.co",
    "minimus": "minimumsec.com",
    "millennium": "mlp.com",
    "mobileye": "mobileye.com",
    "monday": "monday.com",
    "ness technologies": "ness-tech.co.il",
    "ness technologies israel": "ness-tech.co.il",
    "ness": "ness-tech.co.il",
    "nextta": "nextta.net",
    "nexta": "nextta.net",
    "nvidia": "nvidia.com",
    "next insurance": "nextinsurance.com",
    "nextta": "nextta.net",
    "oligo security": "oligo.security",
    "pagaya": "pagaya.com",
    "pango": "pango.co.il",
    "payoneer": "payoneer.com",
    "paragon": "useparagon.com",
    "kyndryl": "kyndryl.com",
    "pentera": "pentera.io",
    "plurai": "plurai.ai",
    "phasev": "phasev.ai",
    "pixellot": "pixellot.tv",
    "plainid": "plainid.com",
    "port": "getport.io",
    "qualitest": "qualitest.com",
    "quanthealth": "quanthealth.com",
    "quantum machines": "quantum-machines.co",
    "remedio": "gytpol.com",
    "remedio formerly gytpol": "gytpol.com",
    "salesforce": "salesforce.com",
    "sentra": "sentra.io",
    "silverfort": "silverfort.com",
    "similarweb": "similarweb.com",
    "surecomp": "surecomp.com",
    "taboola": "taboola.com",
    "tastewise": "tastewise.io",
    "tavily": "tavily.com",
    "torq": "torq.io",
    "team8": "team8.vc",
    "techaviv": "techaviv.com",
    "terasky": "terasky.com",
    "tikal": "tikalk.com",
    "tikalk": "tikalk.com",
    "transmit security": "transmitsecurity.com",
    "unframe": "unframe.com",
    "upstream security": "upstream.auto",
    "upstream": "upstream.auto",
    "varonis": "varonis.com",
    "unity": "unity.com",
    "vastdata": "vastdata.com",
    "voyantis": "voyantis.ai",
    "wavelbl": "wavelbl.com",
    "wiz": "wiz.io",
    "yael group": "yaelgroup.com",
    "zenity": "zenity.io",
    "zscaler": "zscaler.com",
    "iai": "iai.co.il",
    "israel aerospace industries": "iai.co.il",
    "iai - israel aerospace industries": "iai.co.il",
    "similarweb": "similarweb.com",
    "pagaya": "pagaya.com",
    "grafana": "grafana.com",
    "grafana labs": "grafana.com",
    "wiz": "wiz.io",
    "d-fend": "d-fendsolutions.com",
    "d-fend solutions": "d-fendsolutions.com",
    "starburst": "starburstdata.com",
    "buildots": "buildots.com",
}

def _get_company_logo(company: str, source_url: str = "", title: str = "") -> str:
    """Get company logo URL via Google Favicon API.

    Multi-strategy logo resolution:
      1. COMPANY_DOMAINS direct lookup (most reliable)
      2. COMPANY_DOMAINS partial/fuzzy match
      3. ATS URL slug extraction (Greenhouse, Lever, Jobvite, etc.)
      4. Source URL domain extraction (careers.X.com, X.com/careers)
      5. Title-based company extraction ("Role - Company Careers")
      6. Company name → domain derivation (with geo-suffix stripping)

    Returns a Google Favicon URL or empty string.
    """
    if not company:
        return ""
    company_lower = company.lower().strip()
    is_unknown = company_lower in ("unknown", "")

    # Strategy 0: Direct logo URL override (for companies with broken favicons)
    if company_lower in COMPANY_LOGO_OVERRIDES:
        return COMPANY_LOGO_OVERRIDES[company_lower]

    # Reject company names that are clearly locations, not companies
    if re.match(r'^(tel\s*aviv|jerusalem|haifa|new\s*york|london|berlin|tokyo)', company_lower):
        return ""

    # Domains that are ATS/job-board platforms — never valid as company logos
    _PLATFORM_DOMAINS = {
        "greenhouse.io", "greenhouse.com", "lever.co", "lever.com",
        "ashbyhq.com", "jobvite.com", "comeet.com",
        "workday.com", "myworkdayjobs.com", "smartrecruiters.com",
        "breezy.hr", "recruitee.com", "bamboohr.com", "icims.com",
        "indeed.com", "glassdoor.com", "linkedin.com", "monster.com",
        "ziprecruiter.com", "dice.com", "wellfound.com", "angel.co",
        "lhh.com", "efinancialcareers.com", "efinancialcareers-norway.com",
        "drushim.co.il", "alljobs.co.il",
        "jobgether.com", "crawljobs.com", "goozali.com", "secrettelaviv.com",
        "builtin.com", "stackoverflow.com", "hired.com", "remoterocketship.com",
    }
    # Company names that are platforms — skip company-name-based strategies for them
    _PLATFORM_COMPANIES = {
        "jobgether", "crawljobs", "goozali", "lhh", "efinancialcareers",
        "efinancialcareers norway", "secrettelaviv", "alljobs", "drushim",
    }

    def _is_platform_domain(d: str) -> bool:
        """Check if domain belongs to a job board / ATS platform."""
        d = d.lower()
        return any(d == p or d.endswith("." + p) for p in _PLATFORM_DOMAINS)

    def _favicon(d: str) -> str:
        return f"https://www.google.com/s2/favicons?domain={d}&sz=128"

    # ── Preprocessing: strip geo suffixes and trailing numbers ──
    geo_suffixes = r'\b(?:israel|usa|uk|india|germany|france|japan|china|europe|' \
                   r'americas|apac|emea|global|international|worldwide|' \
                   r'tel\s*aviv|new\s*york|london|berlin|tokyo|' \
                   r'il|us|eu|asia|pacific|latam)\b'
    stripped_company = re.sub(geo_suffixes, '', company_lower, flags=re.IGNORECASE).strip()
    stripped_company = re.sub(r'\s*\d{3,}\s*$', '', stripped_company).strip()  # trailing job IDs
    stripped_company = re.sub(r'\s+', ' ', stripped_company).strip()

    if not is_unknown:
        # ── Strategy 1: Direct lookup in COMPANY_DOMAINS ──
        domain = COMPANY_DOMAINS.get(company_lower, "")
        if domain:
            return _favicon(domain)

        # ── Strategy 2: Partial / fuzzy match in COMPANY_DOMAINS ──
        # Try geo-stripped version first, then original
        for variant in [stripped_company, company_lower]:
            if not variant:
                continue
            # Exact match on stripped version
            domain = COMPANY_DOMAINS.get(variant, "")
            if domain:
                return _favicon(domain)
            # Partial match: company contains key or key contains company
            for key, d in COMPANY_DOMAINS.items():
                if key in variant or variant in key:
                    domain = d
                    break
            if domain:
                return _favicon(domain)

    # ── Strategy 3: Company name → domain derivation ──
    # When we have a non-platform company name, derive domain from it
    # BEFORE trying URL-based strategies (URLs can mislead when company is correct)
    _is_platform_company = company_lower in _PLATFORM_COMPANIES or any(
        company_lower.startswith(p) for p in _PLATFORM_COMPANIES
    )
    if not is_unknown and not _is_platform_company and not _is_platform_domain(company_lower + ".com"):
        base = stripped_company or company_lower

        # If the company name itself looks like a domain (contains a known TLD),
        # use it directly — e.g. "monday.com" → "monday.com", "wix.com" → "wix.com"
        tld_m = re.search(r'(\w[\w-]*)\.(com|io|ai|co|net|org|app|dev|cloud|security)(?:\b|$)', base)
        if tld_m and not _is_platform_domain(tld_m.group(0)):
            return _favicon(tld_m.group(0))

        clean = re.sub(r'[^a-z0-9]', '', base)
        words = base.split()
        first_clean = re.sub(r'[^a-z0-9]', '', words[0]) if words else ""

        # Common suffixes that are rarely part of the domain
        _generic_words = {"technologies", "technology", "solutions", "software",
                          "systems", "services", "group", "labs", "inc", "ltd",
                          "corp", "co", "international", "consulting", "digital"}
        # Try without generic suffixes first (e.g. "Dell Technologies" → "dell")
        core_words = [w for w in words if w.lower() not in _generic_words]
        core_clean = re.sub(r'[^a-z0-9]', '', " ".join(core_words)) if core_words else ""

        # Order: core words only → first word → full concatenation
        for candidate in [core_clean, first_clean, clean]:
            if candidate and len(candidate) > 2:
                d = candidate + ".com"
                if not _is_platform_domain(d):
                    return _favicon(d)

    # ── Strategy 4: ATS URL slug extraction ──
    # Only used when company name strategies above didn't produce a result
    # (i.e., company is "Unknown" or a platform name like "Jobvite")
    if source_url:
        url_lower = source_url.lower()
        for ats_pat in [
            r"(?:boards?\.)?(?:job-boards?\.)?(?:eu\.)?greenhouse\.io/([a-z0-9\-]+)",
            r"jobs?\.lever\.co/([a-z0-9\-]+)",
            r"jobs\.ashbyhq\.com/([a-z0-9\-]+)",
            r"([a-z0-9\-]+)\.wd\d+\.myworkdayjobs\.com",
            r"jobs\.jobvite\.com/([a-z0-9\-]+)",
            r"comeet\.com/jobs/([a-z0-9\-]+)",
            r"jobs\.smartrecruiters\.com/([a-z0-9\-]+)",
        ]:
            m = re.search(ats_pat, url_lower)
            if m:
                slug = m.group(1)
                # Strip common ATS slug suffixes
                slug = re.sub(r'-(internal|careers|jobs|external|global|corp)$', '', slug)
                # Check if slug maps to a known domain
                slug_clean = slug.replace("-", " ").strip()
                domain = COMPANY_DOMAINS.get(slug_clean, "")
                if not domain:
                    domain = slug.replace("-", "") + ".com"
                if not _is_platform_domain(domain):
                    return _favicon(domain)

    # ── Strategy 5: Source URL domain extraction ──
    if source_url:
        url_lower = source_url.lower()
        # Pattern: careers.COMPANY.com or jobs.COMPANY.com
        m = re.search(r'https?://(?:careers|jobs)\.([a-z0-9\-]+)\.', url_lower)
        if m:
            d = m.group(1) + ".com"
            if not _is_platform_domain(d) and len(m.group(1)) > 2:
                return _favicon(d)

        # Pattern: COMPANY.com/careers or /jobs (non-ATS)
        m = re.search(r'https?://(?:www\.)?([a-z0-9\-]+)\.(?:com|io|co\.il|ai|co|org)/', url_lower)
        if m:
            d = m.group(1) + ".com"
            if not _is_platform_domain(d) and len(m.group(1)) > 2:
                # Only use if URL has career/job path indicators
                if re.search(r'/(careers|jobs|position|openings|join|hiring|vacancy|job/)', url_lower):
                    return _favicon(d)

    # ── Strategy 6: Title-based extraction (last resort) ──
    if title:
        # Pattern: "Role - Company Careers" or "Role | Company"
        t_match = re.search(r'[-\|\u2013\u2014]\s*([A-Za-z][A-Za-z0-9\s&.\-]+?)\s*(?:careers?|jobs?)?\s*$', title, re.IGNORECASE)
        if t_match:
            t_company = t_match.group(1).strip()
            t_lower = t_company.lower()
            domain = COMPANY_DOMAINS.get(t_lower, "")
            if domain:
                return _favicon(domain)
            # Derive from title company name
            t_clean = re.sub(r'[^a-z0-9]', '', t_lower)
            if t_clean and len(t_clean) > 2:
                d = t_clean + ".com"
                if not _is_platform_domain(d):
                    return _favicon(d)

    return ""


# ── Company Stakeholders for Outreach ──────────────────────────────────────
# Key decision-makers at target companies for BDR outreach
# Sources: LinkedIn, Crunchbase, company websites, Startup Nation, GeekTime,
#          Calcalist, Globes, CTech, F6S, PitchBook, GitHub, Twitter/X, ZoomInfo
# Each contact: name, title, linkedin, source, email (work email guess)
COMPANY_STAKEHOLDERS = {
    "zenity": [
        {"name": "Michael Bargury", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/michaelbargury/", "source": "LinkedIn", "email": "michael@zenity.io"},
        {"name": "Ronen Yaari", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/ronen-yaari-79a1ba4/", "source": "LinkedIn", "email": "ronen@zenity.io"},
        {"name": "Shay Haluba", "title": "Director of Engineering & Innovation", "linkedin": "https://www.linkedin.com/in/shay-haluba/", "source": "LinkedIn", "email": "shay@zenity.io"},
        {"name": "Ben Kliger", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/benkliger/", "source": "Crunchbase", "email": "ben@zenity.io"},
    ],
    "surecomp": [
        {"name": "Benny Savinik", "title": "VP Technology", "linkedin": "https://www.linkedin.com/in/benny-savinik-0299364/", "source": "LinkedIn", "email": "benny.savinik@surecomp.com"},
        {"name": "Tsafrir Atar", "title": "VP Digitization", "linkedin": "https://il.linkedin.com/in/tsafriratar", "source": "LinkedIn", "email": "tsafrir.atar@surecomp.com"},
        {"name": "Eyal Hareuveny", "title": "President", "linkedin": "", "source": "Company Website", "email": "eyal.hareuveny@surecomp.com"},
    ],
    "vast data": [
        {"name": "Renen Hallak", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/renenh/", "source": "Crunchbase", "email": "renen.hallak@vastdata.com"},
        {"name": "Jeff Denworth", "title": "Co-Founder & CMO", "linkedin": "https://www.linkedin.com/in/jeffreydenworth/", "source": "LinkedIn", "email": "jeff.denworth@vastdata.com"},
    ],
    "vastdata": [
        {"name": "Renen Hallak", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/renenh/", "source": "Crunchbase", "email": "renen.hallak@vastdata.com"},
        {"name": "Jeff Denworth", "title": "Co-Founder & CMO", "linkedin": "https://www.linkedin.com/in/jeffreydenworth/", "source": "LinkedIn", "email": "jeff.denworth@vastdata.com"},
    ],
    "check point": [
        {"name": "Nataly Kremer", "title": "CPO & Head of R&D", "linkedin": "https://www.linkedin.com/in/nataly-kremer-12744b29/", "source": "Company Website", "email": ""},
        {"name": "Tomer Lev", "title": "Engineering Director", "linkedin": "https://www.linkedin.com/in/tomerlev/", "source": "LinkedIn", "email": ""},
        {"name": "Ofir Israel", "title": "Engineering Director", "linkedin": "https://www.linkedin.com/in/ofirisrael/", "source": "LinkedIn", "email": ""},
        {"name": "Alex Spokoiny", "title": "Chief Information Officer", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "sentra": [
        {"name": "Ron Reiter", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/ronreiter/", "source": "LinkedIn", "email": "ron@sentra.io"},
        {"name": "Asaf Kochan", "title": "Co-Founder & President", "linkedin": "https://www.linkedin.com/in/asafkochan/", "source": "Crunchbase", "email": "asaf@sentra.io"},
        {"name": "Yoav Regev", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yoav-regev-31718a1/", "source": "LinkedIn", "email": "yoav@sentra.io"},
        {"name": "Yair Cohen", "title": "Co-Founder & VP Product", "linkedin": "https://www.linkedin.com/in/yair-cohen-pm/", "source": "LinkedIn", "email": "yair@sentra.io"},
    ],
    "port": [
        {"name": "Yonatan Boguslavski", "title": "Co-Founder & CTO", "linkedin": "https://il.linkedin.com/in/yonatan-boguslavski-36354b125", "source": "LinkedIn", "email": "yonatan@getport.io"},
        {"name": "Zohar Einy", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/zohar-einy/", "source": "CTech", "email": "zohar@getport.io"},
    ],
    "earnix": [
        {"name": "Erez Barak", "title": "CTO", "linkedin": "https://www.linkedin.com/in/erezbarak/", "source": "LinkedIn", "email": "erez@earnix.com"},
        {"name": "Yaakov Cohen", "title": "VP Engineering, AI Platform", "linkedin": "https://www.linkedin.com/in/yaakovcohen/", "source": "LinkedIn", "email": "yaakov@earnix.com"},
        {"name": "Robin Gilthorpe", "title": "CEO", "linkedin": "https://www.linkedin.com/in/robingilthorpe/", "source": "LinkedIn", "email": "robin@earnix.com"},
    ],
    "nvidia": [
        {"name": "Amit Krig", "title": "SVP Software Engineering & Israel Site Leader", "linkedin": "https://www.linkedin.com/in/amit-krig-7492981/", "source": "LinkedIn", "email": ""},
        {"name": "Gideon Rosenberg", "title": "VP HR Israel", "linkedin": "https://www.linkedin.com/in/gideon-rosenberg-894787/", "source": "LinkedIn", "email": ""},
        {"name": "Michael Kagan", "title": "CTO", "linkedin": "https://il.linkedin.com/in/mikagan", "source": "LinkedIn", "email": ""},
        {"name": "Yaron Goldberg", "title": "Sr Director Engineering", "linkedin": "https://www.linkedin.com/in/yarongoldberg/", "source": "LinkedIn", "email": ""},
    ],
    "elbit": [
        {"name": "Yehoshua Yehuda", "title": "EVP Strategy & CTO", "linkedin": "https://il.linkedin.com/in/yehoshua-shuki-yehuda-0245701", "source": "LinkedIn", "email": ""},
        {"name": "Cindy James", "title": "Sr Director Talent Acquisition", "linkedin": "https://www.linkedin.com/in/cindy-james-3115a68/", "source": "LinkedIn", "email": ""},
    ],
    "classiq": [
        {"name": "Yehuda Naveh", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yehuda-naveh/", "source": "Crunchbase", "email": "yehuda@classiq.io"},
        {"name": "Nir Minerbi", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/nir-minerbi/", "source": "Startup Nation", "email": "nir@classiq.io"},
        {"name": "Amir Naveh", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/amir-naveh-li/", "source": "LinkedIn", "email": "amir@classiq.io"},
    ],
    "tikal": [
        {"name": "Lior Kanfi", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/liorkanfi/", "source": "LinkedIn", "email": "lior@tikalk.com"},
        {"name": "Amir Misgav", "title": "DevOps Tech Leader", "linkedin": "https://www.linkedin.com/in/amir-misgav/", "source": "LinkedIn", "email": "amir@tikalk.com"},
        {"name": "Tamir Tausi", "title": "Head of Sales", "linkedin": "https://il.linkedin.com/in/tamirtausi", "source": "LinkedIn", "email": "tamir@tikalk.com"},
    ],
    "hio": [
        {"name": "Golan Agmon", "title": "Founder", "linkedin": "https://www.linkedin.com/in/golan-agmon-27484b6/", "source": "CTech", "email": ""},
    ],
    "augury": [
        {"name": "Gal Shaul", "title": "Co-Founder & CPTO", "linkedin": "https://il.linkedin.com/in/gal-shaul-427a5a38", "source": "LinkedIn", "email": "gshaul@augury.com"},
        {"name": "Saar Yoskovitz", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/saaryoskovitz/", "source": "GeekTime", "email": "syoskovitz@augury.com"},
    ],
    "pagaya": [
        {"name": "Avital Pardo", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/avital-pardo-257408b7/", "source": "Calcalist", "email": "avital.pardo@pagaya.com"},
        {"name": "Dina Leventol Nimrodi", "title": "Director of Research Engineering", "linkedin": "https://www.linkedin.com/in/dina-leventol-nimrodi-309a8395/", "source": "LinkedIn", "email": ""},
    ],
    "forter": [
        {"name": "Eran Vanounou", "title": "CTO", "linkedin": "https://www.linkedin.com/in/eran-vanounou-983684a/", "source": "LinkedIn", "email": "eran.vanounou@forter.com"},
        {"name": "Oren Ellenbogen", "title": "SVP Engineering", "linkedin": "https://il.linkedin.com/in/orenellenbogen", "source": "GitHub", "email": "oren.ellenbogen@forter.com"},
        {"name": "Jonathan Long", "title": "Sr Director Talent Acquisition", "linkedin": "https://www.linkedin.com/in/jonathan-long-23215693/", "source": "LinkedIn", "email": "jonathan.long@forter.com"},
        {"name": "Michael Reitblat", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/reitblat/", "source": "LinkedIn", "email": ""},
    ],
    "lightricks": [
        {"name": "Yaron Inger", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yaroninger/", "source": "Crunchbase", "email": "yaron@lightricks.com"},
        {"name": "Alon Roth", "title": "Engineering Manager", "linkedin": "https://www.linkedin.com/in/alonroth/", "source": "LinkedIn", "email": "alon@lightricks.com"},
        {"name": "Noa Lichtenstein", "title": "Engineering Manager, AI Photo Tools", "linkedin": "https://www.linkedin.com/in/noa-licht/", "source": "LinkedIn", "email": "noa@lightricks.com"},
    ],
    "cloudinary": [
        {"name": "Tal Lev-Ami", "title": "Co-Founder & CTO", "linkedin": "https://il.linkedin.com/in/tallevami", "source": "LinkedIn", "email": "tal.levami@cloudinary.com"},
        {"name": "Itai Lahan", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/itail/", "source": "LinkedIn", "email": "itai.lahan@cloudinary.com"},
        {"name": "Nadav Soferman", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/nadavsoferman/", "source": "LinkedIn", "email": "nadav.soferman@cloudinary.com"},
    ],
    "guidde": [
        {"name": "Yoav Einav", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yoav-einav-58354323/", "source": "LinkedIn", "email": "yoav.einav@guidde.com"},
        {"name": "Dan Sahar", "title": "CPO & Co-Founder", "linkedin": "https://www.linkedin.com/in/dansahar/", "source": "LinkedIn", "email": "dan.sahar@guidde.com"},
    ],
    "unframe": [
        {"name": "Shay Levi", "title": "Co-Founder & CEO", "linkedin": "https://il.linkedin.com/in/shaylevi2", "source": "Globes", "email": "shay.levi@unframe.ai"},
        {"name": "Adi Azarya", "title": "Co-Founder & VP R&D", "linkedin": "https://il.linkedin.com/in/adiazarya", "source": "LinkedIn", "email": "adi.azarya@unframe.ai"},
        {"name": "Larissa Schneider", "title": "COO & Co-Founder", "linkedin": "https://www.linkedin.com/in/schneiderlarissa/", "source": "LinkedIn", "email": "larissa.schneider@unframe.ai"},
    ],
    "fundamental": [
        {"name": "Jeremy Fraenkel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/jeremy-fraenkel/", "source": "CTech", "email": ""},
    ],
    "bmc": [
        {"name": "Ram Chakravarti", "title": "CTO", "linkedin": "https://www.linkedin.com/in/ramchak/", "source": "Company Website", "email": ""},
    ],
    "leidos": [
        {"name": "Jim Carlini", "title": "CTO", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "matia": [
        {"name": "Benjamin Segal", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/benjamin-segal/", "source": "LinkedIn", "email": "benjamin@matia.io"},
        {"name": "Geva Segal", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gevasegal/", "source": "LinkedIn", "email": "geva@matia.io"},
    ],
    "kpmg": [
        {"name": "Dina Pasca-Raz", "title": "Partner, Head of Technology", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "somekhchaikin": [
        {"name": "Dina Pasca-Raz", "title": "Partner, Head of Technology", "linkedin": "", "source": "Company Website", "email": ""},
    ],
    "taboola": [
        {"name": "Tal Sliwowicz", "title": "Senior VP R&D - Infrastructure", "linkedin": "https://www.linkedin.com/in/talsliwowicz/", "source": "LinkedIn", "email": "tal@taboola.com"},
        {"name": "Lior Golan", "title": "CTO", "linkedin": "https://www.linkedin.com/in/liorgolan/", "source": "LinkedIn", "email": "lior.golan@taboola.com"},
        {"name": "Anjali Oldfield", "title": "Head of HR, EMEA & APAC", "linkedin": "https://www.linkedin.com/in/anjalioldfield/", "source": "LinkedIn", "email": "anjali@taboola.com"},
    ],
    "cyberark": [
        {"name": "Udi Mokady", "title": "Founder & Executive Chairman", "linkedin": "https://www.linkedin.com/in/udimokady/", "source": "LinkedIn", "email": "udi.mokady@cyberark.com"},
        {"name": "Matt Cohen", "title": "CEO", "linkedin": "https://www.linkedin.com/in/mattjcohen/", "source": "LinkedIn", "email": "matt.cohen@cyberark.com"},
        {"name": "Kathy Cullen-Cote", "title": "Chief People Officer", "linkedin": "https://www.linkedin.com/in/kathy-cullen/", "source": "LinkedIn", "email": "kathy.cullen-cote@cyberark.com"},
    ],
    "wiz": [
        {"name": "Assaf Rappaport", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/assafrappaport/", "source": "LinkedIn", "email": "assaf@wiz.io"},
        {"name": "Roy Reznik", "title": "Co-Founder, VP R&D", "linkedin": "https://www.linkedin.com/in/roy-reznik-a8b822189/", "source": "LinkedIn", "email": "roy@wiz.io"},
        {"name": "Arik Nemtsov", "title": "Director of Engineering", "linkedin": "https://www.linkedin.com/in/arik-nemtsov-b9516578/", "source": "LinkedIn", "email": "arik@wiz.io"},
    ],
    "fireblocks": [
        {"name": "Michael Shaulov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/michaelsh/", "source": "LinkedIn", "email": "michael@fireblocks.com"},
        {"name": "Pavel Berengoltz", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/pavelberengoltz/", "source": "LinkedIn", "email": "pavel@fireblocks.com"},
        {"name": "Idan Ofrat", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/idanofrat/", "source": "LinkedIn", "email": "idan@fireblocks.com"},
    ],
    "mobileye": [
        {"name": "Amnon Shashua", "title": "President & CEO, Founder", "linkedin": "https://www.linkedin.com/in/amnon-shashua/", "source": "LinkedIn", "email": "amnon.shashua@mobileye.com"},
        {"name": "Shai Shalev-Shwartz", "title": "CTO", "linkedin": "https://www.linkedin.com/in/shai-shalev-shwartz/", "source": "LinkedIn", "email": "shai@mobileye.com"},
    ],
    "silverfort": [
        {"name": "Hed Kovetz", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/hedkovetz/", "source": "LinkedIn", "email": "hed@silverfort.com"},
        {"name": "Ben Livne", "title": "Senior VP R&D", "linkedin": "https://www.linkedin.com/in/benlivne/", "source": "LinkedIn", "email": "ben@silverfort.com"},
        {"name": "Yiftach Keshet", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yiftachkeshet/", "source": "LinkedIn", "email": "yiftach@silverfort.com"},
    ],
    "similarweb": [
        {"name": "Ron Asher", "title": "CTO", "linkedin": "https://www.linkedin.com/in/ronasher/", "source": "LinkedIn", "email": "ron.asher@similarweb.com"},
        {"name": "Or Offer", "title": "CEO", "linkedin": "https://www.linkedin.com/in/oroffer/", "source": "LinkedIn", "email": "or.offer@similarweb.com"},
    ],
    "pentera": [
        {"name": "Amitai Ratzon", "title": "CEO", "linkedin": "https://www.linkedin.com/in/amitairatzon/", "source": "LinkedIn", "email": "amitai@pentera.io"},
        {"name": "Arik Liberzon", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/arikliberzon/", "source": "LinkedIn", "email": "arik@pentera.io"},
    ],
    "au10tix": [
        {"name": "Dan Yerushalmi", "title": "CEO", "linkedin": "https://www.linkedin.com/in/danyerushalmi/", "source": "LinkedIn", "email": "dan@au10tix.com"},
    ],
    "audiocodes": [
        {"name": "Shabtai Adlersberg", "title": "President & CEO", "linkedin": "https://www.linkedin.com/in/shabtai-adlersberg/", "source": "LinkedIn", "email": "shabtai.adlersberg@audiocodes.com"},
        {"name": "Niran Baruch", "title": "VP Finance & CFO", "linkedin": "https://www.linkedin.com/in/niran-baruch/", "source": "LinkedIn", "email": "niran.baruch@audiocodes.com"},
    ],
    "biocatch": [
        {"name": "Gadi Mazor", "title": "CEO", "linkedin": "https://www.linkedin.com/in/gadimazor/", "source": "LinkedIn", "email": "gadi@biocatch.com"},
        {"name": "Avi Turgeman", "title": "Founder & CTO", "linkedin": "https://www.linkedin.com/in/aviturgeman/", "source": "LinkedIn", "email": "avi@biocatch.com"},
    ],
    "cymulate": [
        {"name": "Eyal Wachsman", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/eyalwachsman/", "source": "LinkedIn", "email": "eyal@cymulate.com"},
        {"name": "Avihai Ben-Yossef", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/avihai-ben-yossef/", "source": "LinkedIn", "email": "avihai@cymulate.com"},
    ],
    "doit": [
        {"name": "Vadim Solovey", "title": "CTO & Managing Director", "linkedin": "https://www.linkedin.com/in/vadimska/", "source": "LinkedIn", "email": "vadim@doit.com"},
        {"name": "Yoav Toussia-Cohen", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/yoavtc/", "source": "LinkedIn", "email": "yoav@doit.com"},
    ],
    "fetcherr": [
        {"name": "Roi Dover", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/roidover/", "source": "LinkedIn", "email": "roi@fetcherr.io"},
        {"name": "Roy Friedman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/royfriedman1/", "source": "LinkedIn", "email": "roy@fetcherr.io"},
    ],
    "zscaler": [
        {"name": "Jay Chaudhry", "title": "CEO, Chairman & Founder", "linkedin": "https://www.linkedin.com/in/jaychaudhry/", "source": "LinkedIn", "email": "jay@zscaler.com"},
    ],
    "allcloud": [
        {"name": "Roman Koterman", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/roman-koterman/", "source": "LinkedIn", "email": "rkoterman@allcloud.io"},
    ],
    "applied materials": [
        {"name": "Nir Yogev", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/nir-yogev-0a2a2618/", "source": "LinkedIn", "email": "nir.yogev@amat.com"},
        {"name": "Anat Tzur", "title": "Director of DevOps", "linkedin": "https://www.linkedin.com/in/anat-tzur/", "source": "LinkedIn", "email": "anat.tzur@amat.com"},
    ],
    "aquasec": [
        {"name": "Dror Davidoff", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/drordavidoff/", "source": "LinkedIn", "email": "dror@aquasec.com"},
        {"name": "Amir Jerbi", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/amirjerbi/", "source": "LinkedIn", "email": "amir@aquasec.com"},
    ],
    "codevalue": [
        {"name": "Lior Fridman", "title": "VP Engineering", "linkedin": "https://www.linkedin.com/in/lior-fridman-8a07906/", "source": "LinkedIn", "email": "lior@codevalue.com"},
    ],
    "globallogic": [
        {"name": "Nitzan Shapira", "title": "Country Manager Israel", "linkedin": "https://www.linkedin.com/in/nitzanshapira/", "source": "LinkedIn", "email": "nitzan.shapira@globallogic.com"},
    ],
    "team8": [
        {"name": "Nadav Zafrir", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadavzafrir/", "source": "LinkedIn", "email": "nadav@team8.vc"},
        {"name": "Israel Grimberg", "title": "Partner", "linkedin": "https://www.linkedin.com/in/israelgrimberg/", "source": "LinkedIn", "email": "israel@team8.vc"},
    ],
    "paragon": [
        {"name": "Idan Nurick", "title": "CEO", "linkedin": "https://www.linkedin.com/in/idannurick/", "source": "LinkedIn", "email": ""},
    ],
    "plainid": [
        {"name": "Oren Ohayon Harel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/oren-ohayon-harel/", "source": "LinkedIn", "email": "oren@plainid.com"},
        {"name": "Gal Helemski", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/galhelemski/", "source": "LinkedIn", "email": "gal@plainid.com"},
    ],
    "pango": [
        {"name": "Hari Ravichandran", "title": "Founder & CEO (Aura)", "linkedin": "https://www.linkedin.com/in/hariravichandran/", "source": "LinkedIn", "email": ""},
    ],
    "terasky": [
        {"name": "Alon Barel", "title": "CEO & Founder", "linkedin": "https://www.linkedin.com/in/alonbarel/", "source": "LinkedIn", "email": "alon@terasky.com"},
    ],
    "voyantis": [
        {"name": "Ido Benmoshe", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/idobenmoshe/", "source": "LinkedIn", "email": "ido@voyantis.com"},
    ],
    "wavebl": [
        {"name": "Noam Ohana", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/noamohana/", "source": "LinkedIn", "email": "noam@wavebl.com"},
    ],
    "metalbear": [
        {"name": "Aviram Hassan", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/aviramhassan/", "source": "LinkedIn", "email": "aviram@metalbear.co"},
        {"name": "Eyal Bukchin", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/eyal-bukchin/", "source": "LinkedIn", "email": "eyal@metalbear.co"},
    ],
    "quantummachines": [
        {"name": "Itamar Sivan", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/itamarsivan/", "source": "LinkedIn", "email": "itamar@quantum-machines.co"},
        {"name": "Yonatan Cohen", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yonatancohen/", "source": "LinkedIn", "email": "yonatan@quantum-machines.co"},
    ],
    "chaoslabs": [
        {"name": "Omer Goldberg", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/omergoldberg/", "source": "LinkedIn", "email": "omer@chaoslabs.xyz"},
    ],
    "next insurance": [
        {"name": "Guy Goldstein", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/guygoldstein/", "source": "LinkedIn", "email": "guy@nextinsurance.com"},
        {"name": "Alon Huri", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/alonhuri/", "source": "LinkedIn", "email": "alon@nextinsurance.com"},
    ],
    "cato networks": [
        {"name": "Shlomo Kramer", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/shlomokramer/", "source": "LinkedIn", "email": "shlomo@catonetworks.com"},
        {"name": "Gur Shatz", "title": "Co-Founder & COO", "linkedin": "https://www.linkedin.com/in/gurshatz/", "source": "LinkedIn", "email": "gur@catonetworks.com"},
    ],
    "datadog": [
        {"name": "Olivier Pomel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/olivierpomel/", "source": "LinkedIn", "email": "olivier@datadoghq.com"},
        {"name": "Alexis Le-Quoc", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/alexislequoc/", "source": "LinkedIn", "email": "alexis@datadoghq.com"},
    ],
    "unity": [
        {"name": "Matt Bromberg", "title": "CEO", "linkedin": "https://www.linkedin.com/in/mattbromberg/", "source": "LinkedIn", "email": "matt.bromberg@unity3d.com"},
    ],
    "appcharge": [
        {"name": "Maor Sauron", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/maorsauron/", "source": "LinkedIn", "email": "maor@appcharge.com"},
    ],
    "blinkops": [
        {"name": "Gil Barak", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gilbarak/", "source": "LinkedIn", "email": "gil@blinkops.com"},
    ],
    "harmonya": [
        {"name": "Eran Lupo", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/eranlupo/", "source": "LinkedIn", "email": "eran@harmonya.com"},
    ],
    "oligosecurity": [
        {"name": "Nadav Czerninski", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadavczerninski/", "source": "LinkedIn", "email": "nadav@oligo.security"},
    ],
    "minimus": [
        {"name": "Matan Derman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/matanderman/", "source": "LinkedIn", "email": "matan@minimus.io"},
    ],
    "nextta": [
        {"name": "Oded Shopen", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/odedshopen/", "source": "LinkedIn", "email": "oded@nextta.com"},
    ],
    "remedio": [
        {"name": "Tal Peer", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/talpeer/", "source": "LinkedIn", "email": "tal@gytpol.com"},
    ],
    "imagen": [
        {"name": "Oron Branitzky", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/oronbranitzky/", "source": "LinkedIn", "email": "oron@imagen.io"},
    ],
    "marvin": [
        {"name": "Hila Qu", "title": "CEO", "linkedin": "https://www.linkedin.com/in/hilaqu/", "source": "LinkedIn", "email": "hila@marvin.com"},
    ],
    "aqua security": [
        {"name": "Dror Davidoff", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/drordavidoff/", "source": "LinkedIn", "email": "dror@aquasec.com"},
        {"name": "Amir Jerbi", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/amirjerbi/", "source": "LinkedIn", "email": "amir@aquasec.com"},
    ],
    "oligo security": [
        {"name": "Gal Elbaz", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gal-elbaz-2b70b214/", "source": "LinkedIn", "email": "gal@oligo.security"},
        {"name": "Nadav Czerninski", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadavcz/", "source": "CTech", "email": "nadav@oligo.security"},
    ],
    "quantum machines": [
        {"name": "Itamar Sivan", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/itamarsivan/", "source": "LinkedIn", "email": "itamar@quantum-machines.co"},
        {"name": "Yonatan Cohen", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yonatan-cohen-05684411/", "source": "Calcalist", "email": "yonatan@quantum-machines.co"},
    ],
    "mastercard": [
        {"name": "Jorn Lambert", "title": "Chief Product Officer", "linkedin": "https://www.linkedin.com/in/jornlambert/", "source": "LinkedIn", "email": ""},
    ],
    "salesforce": [
        {"name": "Oren Winter", "title": "SVP Engineering, Israel R&D", "linkedin": "https://www.linkedin.com/in/oren-winter-89571a/", "source": "LinkedIn", "email": ""},
    ],
    "blink ops": [
        {"name": "Gil Barak", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/gilbarak/", "source": "LinkedIn", "email": "gil@blinkops.com"},
        {"name": "Raz Itzhakian", "title": "CTO", "linkedin": "https://www.linkedin.com/in/razitzhakian/", "source": "LinkedIn", "email": ""},
    ],
    "chaos labs": [
        {"name": "Omer Goldberg", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/omer-goldberg/", "source": "LinkedIn", "email": "omer@chaoslabs.xyz"},
    ],
    "tavily": [
        {"name": "Lior Gross", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/liorgross/", "source": "LinkedIn", "email": "lior@tavily.com"},
    ],
    "quanthealth": [
        {"name": "Orr Inbar", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/orrinbar/", "source": "LinkedIn", "email": "orr@quanthealth.com"},
    ],
    "cato networks": [
        {"name": "Shlomo Kramer", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/shlomokramer/", "source": "Globes", "email": ""},
        {"name": "Eyal Heiman", "title": "CTO", "linkedin": "https://www.linkedin.com/in/eyal-heiman-99233a98/", "source": "LinkedIn", "email": ""},
    ],
    "global payments": [
        {"name": "Josh Whipple", "title": "CFO & Senior EVP", "linkedin": "https://www.linkedin.com/in/joshwhipple/", "source": "LinkedIn", "email": ""},
    ],
    "yael group": [
        {"name": "Doron Gigi", "title": "CEO", "linkedin": "https://www.linkedin.com/in/doron-gigi-93123823/", "source": "LinkedIn", "email": ""},
        {"name": "Amit Dover", "title": "Deputy CEO & CTO", "linkedin": "https://www.linkedin.com/in/amitdover/", "source": "Company Website", "email": ""},
    ],
    "terasky": [
        {"name": "Or Yaacov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/oryaacov/", "source": "LinkedIn", "email": "or@terasky.com"},
    ],
    "torq": [
        {"name": "Leonid Belkind", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/leonidbelkind/", "source": "LinkedIn", "email": ""},
        {"name": "Ofer Smadari", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/ofersmadari/", "source": "LinkedIn", "email": ""},
    ],
    "axonius": [
        {"name": "Ofri Shur", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/ofri-shur-3683a13b/", "source": "LinkedIn", "email": ""},
        {"name": "Dean Sysman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/deansysman/", "source": "LinkedIn", "email": ""},
    ],
    "freightos": [
        {"name": "Enric Alventosa", "title": "CTO", "linkedin": "https://www.linkedin.com/in/enric-alventosa-04469180/", "source": "LinkedIn", "email": ""},
    ],
    "linearb": [
        {"name": "Yishai Beeri", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yishaibeeri/", "source": "LinkedIn", "email": ""},
        {"name": "Ori Keren", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/ori-keren-8254965/", "source": "LinkedIn", "email": ""},
    ],
    "upstream": [
        {"name": "Yonatan Appel", "title": "CTO", "linkedin": "https://www.linkedin.com/in/yonatan-appel-5895223/", "source": "LinkedIn", "email": ""},
        {"name": "Yoav Levy", "title": "CEO", "linkedin": "https://www.linkedin.com/in/yoav-levy-117b2b1/", "source": "LinkedIn", "email": ""},
    ],
    "armis": [
        {"name": "Nadir Izrael", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadiriz/", "source": "LinkedIn", "email": ""},
        {"name": "Yevgeny Dibrov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yevgenydibrov/", "source": "LinkedIn", "email": ""},
    ],
    "armissecurity": [
        {"name": "Nadir Izrael", "title": "CTO & Co-Founder", "linkedin": "https://www.linkedin.com/in/nadiriz/", "source": "LinkedIn", "email": ""},
        {"name": "Yevgeny Dibrov", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yevgenydibrov/", "source": "LinkedIn", "email": ""},
    ],
    "deloitte": [
        {"name": "Ronen Sigal", "title": "Managing Partner, Israel", "linkedin": "https://www.linkedin.com/in/ronensigal/", "source": "LinkedIn", "email": ""},
    ],
    "motorolasolutions": [
        {"name": "Benny Dvir", "title": "Sr. Director Solutions & Services, Israel", "linkedin": "https://www.linkedin.com/in/bennydvir/", "source": "LinkedIn", "email": ""},
    ],
    "motorola solutions": [
        {"name": "Benny Dvir", "title": "Sr. Director Solutions & Services, Israel", "linkedin": "https://www.linkedin.com/in/bennydvir/", "source": "LinkedIn", "email": ""},
    ],
    "ness technologies": [
        {"name": "Sudip Singh", "title": "CEO", "linkedin": "https://www.linkedin.com/in/sudipsingh/", "source": "LinkedIn", "email": ""},
    ],
    "samsung": [
        {"name": "Ilan Elias", "title": "VP & Head of Samsung Israel R&D Center", "linkedin": "https://www.linkedin.com/in/ilan-elias-6766b54/", "source": "LinkedIn", "email": ""},
    ],
    "sqlink": [
        {"name": "Tamir Goren", "title": "CEO", "linkedin": "https://www.linkedin.com/in/tamir-goren-8b666a4/", "source": "LinkedIn", "email": ""},
    ],
    "comm it": [
        {"name": "Ilan Sokolov", "title": "CTO", "linkedin": "https://www.linkedin.com/in/ilan-sokolov/", "source": "LinkedIn", "email": ""},
    ],
    "gett": [
        {"name": "Yaki Zakai", "title": "CTO", "linkedin": "https://il.linkedin.com/in/yaki-zakai-62847", "source": "LinkedIn", "email": ""},
        {"name": "Matteo de Renzi", "title": "CEO", "linkedin": "https://uk.linkedin.com/in/matteoderenzi", "source": "LinkedIn", "email": ""},
    ],
    "wix": [
        {"name": "Yoav Abrahami", "title": "CTO & Head of Wix Engineering", "linkedin": "https://www.linkedin.com/in/yoavabrahami/", "source": "LinkedIn", "email": ""},
        {"name": "Nir Zohar", "title": "President & COO", "linkedin": "https://www.linkedin.com/in/nirzohar/", "source": "LinkedIn", "email": ""},
    ],
    "cloudflare": [
        {"name": "John Graham-Cumming", "title": "CTO", "linkedin": "https://www.linkedin.com/in/jgrahamc/", "source": "LinkedIn", "email": ""},
    ],
    "intel": [
        {"name": "Greg Lavender", "title": "CTO & SVP", "linkedin": "https://www.linkedin.com/in/greg-lavender-9539724/", "source": "LinkedIn", "email": ""},
    ],
    "intel corporation": [
        {"name": "Greg Lavender", "title": "CTO & SVP", "linkedin": "https://www.linkedin.com/in/greg-lavender-9539724/", "source": "LinkedIn", "email": ""},
    ],
    "atera": [
        {"name": "Gil Pekelman", "title": "CEO", "linkedin": "https://www.linkedin.com/in/gilpekelman/", "source": "LinkedIn", "email": ""},
        {"name": "Oshri Moyal", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/oshrimoyal/", "source": "LinkedIn", "email": ""},
    ],
    "cloudera": [
        {"name": "Dipto Chakravarty", "title": "Chief Engineering Officer", "linkedin": "https://www.linkedin.com/in/diptochakravarty/", "source": "LinkedIn", "email": ""},
    ],
    "accenture": [
        {"name": "Karthik Narain", "title": "CEO, Accenture Technology", "linkedin": "https://www.linkedin.com/in/karthiknarain/", "source": "LinkedIn", "email": ""},
    ],
    "medtronic": [
        {"name": "Ken Washington", "title": "SVP & CTO", "linkedin": "https://www.linkedin.com/in/kenwashington/", "source": "LinkedIn", "email": ""},
    ],
    "carrier": [
        {"name": "James Pisz", "title": "VP Digital Solutions & CTO", "linkedin": "https://www.linkedin.com/in/jamespisz/", "source": "LinkedIn", "email": ""},
    ],
    "gsk": [
        {"name": "Karenann Terrell", "title": "Chief Digital & Technology Officer", "linkedin": "https://www.linkedin.com/in/karenannterrell/", "source": "LinkedIn", "email": ""},
    ],
    "xsolla": [
        {"name": "Chris Hewish", "title": "CEO", "linkedin": "https://www.linkedin.com/in/chrishewish/", "source": "LinkedIn", "email": ""},
    ],
    "cast ai": [
        {"name": "Laurent Gil", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/laurentgil/", "source": "LinkedIn", "email": ""},
        {"name": "Yuri Frayman", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yurifrayman/", "source": "LinkedIn", "email": ""},
    ],
    "cast.ai": [
        {"name": "Laurent Gil", "title": "Co-Founder & CPO", "linkedin": "https://www.linkedin.com/in/laurentgil/", "source": "LinkedIn", "email": ""},
        {"name": "Yuri Frayman", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yurifrayman/", "source": "LinkedIn", "email": ""},
    ],
    # ── New companies added for contact coverage (Mar 2026) ──
    "apiiro": [
        {"name": "Idan Plotnik", "title": "Co-Founder & CEO", "linkedin": "https://il.linkedin.com/in/idanplotnik", "source": "LinkedIn", "email": ""},
        {"name": "Yonatan Eldar", "title": "Co-Founder & CTO", "linkedin": "https://il.linkedin.com/in/yonatan-eldar-a6a40621", "source": "LinkedIn", "email": ""},
    ],
    "coralogix": [
        {"name": "Ariel Assaraf", "title": "Co-Founder & CEO", "linkedin": "https://il.linkedin.com/in/ariel-assaraf-ab621896", "source": "LinkedIn", "email": ""},
        {"name": "Yoni Farin", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yoni-farin-97806874/", "source": "LinkedIn", "email": ""},
    ],
    "transmit security": [
        {"name": "Mickey Boodaei", "title": "Co-Founder & CEO", "linkedin": "https://il.linkedin.com/in/mickeyboodaei", "source": "LinkedIn", "email": ""},
        {"name": "Shmulik Regev", "title": "CTO", "linkedin": "https://www.linkedin.com/in/shmulik-regev-9085622/", "source": "LinkedIn", "email": ""},
    ],
    "varonis": [
        {"name": "Yaki Faitelson", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/yaki-faitelson", "source": "LinkedIn", "email": ""},
        {"name": "David Bass", "title": "EVP Engineering & CTO", "linkedin": "https://www.linkedin.com/in/dave-bass-15017b4/", "source": "LinkedIn", "email": ""},
    ],
    "nayax": [
        {"name": "Yair Nechmad", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/yair-nechmad-55b75413/", "source": "LinkedIn", "email": ""},
    ],
    "legitsecurity": [
        {"name": "Roni Fuchs", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/ronifuchs/", "source": "LinkedIn", "email": ""},
        {"name": "Liav Caspi", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/liav-caspi-67b10647/", "source": "LinkedIn", "email": ""},
    ],
    "legit security": [
        {"name": "Roni Fuchs", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/ronifuchs/", "source": "LinkedIn", "email": ""},
        {"name": "Liav Caspi", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/liav-caspi-67b10647/", "source": "LinkedIn", "email": ""},
    ],
    "nym health": [
        {"name": "Or Peles", "title": "CEO", "linkedin": "https://www.linkedin.com/in/or-peles/", "source": "LinkedIn", "email": ""},
    ],
    "orchid": [
        {"name": "Roy Katmor", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/roykatmor/", "source": "LinkedIn", "email": ""},
    ],
    "orchid security": [
        {"name": "Roy Katmor", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/roykatmor/", "source": "LinkedIn", "email": ""},
    ],
    "tastewise": [
        {"name": "Alon Chen", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/alonchen/", "source": "LinkedIn", "email": ""},
        {"name": "Eyal Gaon", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/eyal-gaon-a8260540/", "source": "LinkedIn", "email": ""},
    ],
    "traild": [
        {"name": "Brad Smorgon", "title": "Founder & CEO", "linkedin": "https://au.linkedin.com/in/bradsmorgon", "source": "LinkedIn", "email": ""},
    ],
    "pixellot": [
        {"name": "Gal Oz", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/gal-oz-1307/", "source": "LinkedIn", "email": ""},
    ],
    "play perfect": [
        {"name": "Idan Ayzen", "title": "CTO", "linkedin": "https://www.linkedin.com/in/idan-ayzen-31a226178/", "source": "LinkedIn", "email": ""},
    ],
    "adaptive6": [
        {"name": "Aviv Revach", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/avivrevach/", "source": "LinkedIn", "email": ""},
        {"name": "Omer Müller", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/omermuller/", "source": "LinkedIn", "email": ""},
    ],
    "qualitest": [
        {"name": "Eli Margolin", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/eli-margolin-a1084273/", "source": "LinkedIn", "email": ""},
    ],
    "g2": [
        {"name": "Godard Abel", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/godardabel/", "source": "LinkedIn", "email": ""},
    ],
    "bitsight": [
        {"name": "Stephen Harvey", "title": "CEO", "linkedin": "https://www.linkedin.com/in/stephen-harvey-667a411b/", "source": "LinkedIn", "email": ""},
        {"name": "Dave Casion", "title": "CTO", "linkedin": "https://www.linkedin.com/in/dave-casion/", "source": "LinkedIn", "email": ""},
    ],
    "hinge health": [
        {"name": "Daniel Perez", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/danielperez1/", "source": "LinkedIn", "email": ""},
    ],
    "wraithwatch": [
        {"name": "Nik Seetharaman", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/nikseetharaman/", "source": "LinkedIn", "email": ""},
    ],
    "microsoft": [
        {"name": "Michal Braverman-Blumenstyk", "title": "CVP & MD, Microsoft Israel R&D", "linkedin": "https://www.linkedin.com/in/michal-braverman-blumenstyk/", "source": "LinkedIn", "email": ""},
    ],
    "amazon web services aws": [
        {"name": "Harel Ifhar", "title": "General Manager, AWS Israel", "linkedin": "https://il.linkedin.com/in/harel-ifhar-593508/", "source": "LinkedIn", "email": ""},
    ],
    "aws": [
        {"name": "Harel Ifhar", "title": "General Manager, AWS Israel", "linkedin": "https://il.linkedin.com/in/harel-ifhar-593508/", "source": "LinkedIn", "email": ""},
    ],
    # ── Additional companies added for contact coverage (Mar 2026) ──────────
    "algosec": [
        {"name": "Avishai Wool", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/avishaiw/", "source": "LinkedIn", "email": ""},
        {"name": "Yuval Baron", "title": "CEO", "linkedin": "https://www.linkedin.com/in/yuval-baron-b08b04/", "source": "LinkedIn", "email": ""},
    ],
    "rapyd": [
        {"name": "Arik Shtilman", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/arik-shtilman/", "source": "LinkedIn", "email": ""},
        {"name": "Omer Priel", "title": "Co-Founder & CTO", "linkedin": "https://il.linkedin.com/in/omer-priel/", "source": "LinkedIn", "email": ""},
    ],
    "lemonade": [
        {"name": "Shai Wininger", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/shaiwi/", "source": "LinkedIn", "email": ""},
        {"name": "Daniel Schreiber", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/danielschreiber/", "source": "LinkedIn", "email": ""},
    ],
    "shield": [
        {"name": "Shiran Weitzman", "title": "CEO & Co-Founder", "linkedin": "https://www.linkedin.com/in/shiran-weitzman/", "source": "LinkedIn", "email": ""},
        {"name": "Assaf Glikman", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/assaf-glikman/", "source": "LinkedIn", "email": ""},
    ],
    "one zero": [
        {"name": "Gal Bar Dea", "title": "CEO", "linkedin": "https://www.linkedin.com/in/gal-bar-dea/", "source": "LinkedIn", "email": ""},
        {"name": "Amnon Shashua", "title": "Co-Founder & Chairman", "linkedin": "https://www.linkedin.com/in/amnon-shashua/", "source": "LinkedIn", "email": ""},
    ],
    "one zero digital bank": [
        {"name": "Gal Bar Dea", "title": "CEO", "linkedin": "https://www.linkedin.com/in/gal-bar-dea/", "source": "LinkedIn", "email": ""},
        {"name": "Amnon Shashua", "title": "Co-Founder & Chairman", "linkedin": "https://www.linkedin.com/in/amnon-shashua/", "source": "LinkedIn", "email": ""},
    ],
    "immunai": [
        {"name": "Noam Solomon", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/noamsolomon/", "source": "LinkedIn", "email": ""},
        {"name": "Luis Voloch", "title": "Co-Founder", "linkedin": "https://www.linkedin.com/in/luisvoloch/", "source": "LinkedIn", "email": ""},
    ],
    "glassbox": [
        {"name": "Yaron Gueta", "title": "Co-Founder & CTO", "linkedin": "https://www.linkedin.com/in/yaron-gueta/", "source": "LinkedIn", "email": ""},
        {"name": "Hanan Blumstein", "title": "Co-Founder & CEO", "linkedin": "https://www.linkedin.com/in/hanan-blumstein/", "source": "LinkedIn", "email": ""},
    ],
    "iai": [
        {"name": "Boaz Levy", "title": "President & CEO", "linkedin": "https://www.linkedin.com/in/boaz-levy-/", "source": "LinkedIn", "email": ""},
    ],
    "israel aerospace industries": [
        {"name": "Boaz Levy", "title": "President & CEO", "linkedin": "https://www.linkedin.com/in/boaz-levy-/", "source": "LinkedIn", "email": ""},
    ],
    "iai - israel aerospace industries": [
        {"name": "Boaz Levy", "title": "President & CEO", "linkedin": "https://www.linkedin.com/in/boaz-levy-/", "source": "LinkedIn", "email": ""},
    ],
    "palo alto networks": [
        {"name": "Amit Waisel", "title": "VP R&D, Israel", "linkedin": "https://il.linkedin.com/in/amitwaisel/", "source": "LinkedIn", "email": ""},
        {"name": "Lee Klarich", "title": "Chief Product Officer", "linkedin": "https://www.linkedin.com/in/leeklarich/", "source": "LinkedIn", "email": ""},
    ],
    "commbox": [
        {"name": "Eli Israelov", "title": "CEO & Co-Founder", "linkedin": "https://il.linkedin.com/in/eli-israelov/", "source": "LinkedIn", "email": ""},
    ],
    "google": [
        {"name": "Barak Regev", "title": "Managing Director, Google Israel", "linkedin": "https://il.linkedin.com/in/barakregev/", "source": "LinkedIn", "email": ""},
    ],
    "abra": [
        {"name": "Bill Barhydt", "title": "CEO & Founder", "linkedin": "https://www.linkedin.com/in/billbarhydt/", "source": "LinkedIn", "email": ""},
    ],
    "teads": [
        {"name": "Pierre Chappaz", "title": "Co-Founder & Executive Chairman", "linkedin": "https://www.linkedin.com/in/pierrechappaz/", "source": "LinkedIn", "email": ""},
        {"name": "Jeremy Arditi", "title": "Co-Founder & Co-CEO", "linkedin": "https://www.linkedin.com/in/jeremyarditi/", "source": "LinkedIn", "email": ""},
    ],
    "veeva systems": [
        {"name": "Peter Gassner", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/petergassner/", "source": "LinkedIn", "email": ""},
    ],
    "veeva": [
        {"name": "Peter Gassner", "title": "Founder & CEO", "linkedin": "https://www.linkedin.com/in/petergassner/", "source": "LinkedIn", "email": ""},
    ],
    "appcard": [
        {"name": "Yair Goldfinger", "title": "CEO & Founder", "linkedin": "https://www.linkedin.com/in/yairgoldfinger/", "source": "LinkedIn", "email": ""},
    ],
    "cisco": [
        {"name": "Inbal Kreiss", "title": "VP & Site Leader, Cisco Israel", "linkedin": "https://il.linkedin.com/in/inbal-kreiss/", "source": "LinkedIn", "email": ""},
    ],
    "siemens": [
        {"name": "Avi Margalit", "title": "CEO, Siemens Israel", "linkedin": "https://il.linkedin.com/in/avi-margalit/", "source": "LinkedIn", "email": ""},
    ],
    "qualcomm": [
        {"name": "Ziv Binyamini", "title": "VP & GM, Qualcomm Israel", "linkedin": "https://il.linkedin.com/in/ziv-binyamini/", "source": "LinkedIn", "email": ""},
    ],
    "kyndryl": [
        {"name": "Martin Schroeter", "title": "CEO", "linkedin": "https://www.linkedin.com/in/martin-schroeter/", "source": "LinkedIn", "email": ""},
    ],
}

SEARCH_QUERIES = [
    # LinkedIn individual job listings (highest quality)
    "site:linkedin.com/jobs/view DevOps Engineer Israel",
    "site:linkedin.com/jobs/view Senior DevOps Engineer Israel",
    "site:linkedin.com/jobs/view AI Engineer Israel",
    "site:linkedin.com/jobs/view Machine Learning Engineer Israel",
    "site:linkedin.com/jobs/view Platform Engineer Israel",
    "site:linkedin.com/jobs/view MLOps Engineer Israel",
    "site:linkedin.com/jobs/view SRE Israel",
    "site:linkedin.com/jobs/view Site Reliability Engineer Israel",
    "site:linkedin.com/jobs/view SRE Manager Israel",
    "site:linkedin.com/jobs/view Production Engineer Israel",
    "site:linkedin.com/jobs/view Infrastructure Reliability Engineer Israel",
    "site:linkedin.com/jobs/view Platform Reliability Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Engineer Israel",
    "site:linkedin.com/jobs/view Agentic AI Israel",
    "site:linkedin.com/jobs/view DevSecOps Israel",
    "site:linkedin.com/jobs/view Infrastructure Engineer Israel",
    "site:linkedin.com/jobs/view Data Engineer Israel",
    "site:linkedin.com/jobs/view Data Platform Engineer Israel",
    "site:linkedin.com/jobs/view MLOps Engineer Israel",
    "site:linkedin.com/jobs/view DataOps Engineer Israel",
    "site:linkedin.com/jobs/view Data Infrastructure Engineer Israel",
    "site:linkedin.com/jobs/view Data Engineering Manager Israel",
    "site:linkedin.com/jobs/view Backend Engineer Israel",
    # Career sites and job boards
    "DevOps Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "AI Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    # Note: removed apple.com/microsoft.com/google.com — their SPA career pages
    # don't expose structured location data, causing false positives (e.g. India jobs on /en-il/ locale)
    "DevOps Engineer Israel site:workday.com OR site:myworkdayjobs.com",
    # Comeet (Israeli ATS with structured data)
    "site:comeet.com/jobs DevOps Engineer Israel",
    "site:comeet.com/jobs AI Engineer Israel",
    "site:comeet.com/jobs Cloud Engineer Israel",
    "site:comeet.com/jobs SRE Israel",
    "site:comeet.com/jobs Site Reliability Engineer Israel",
    "site:comeet.com/jobs Production Engineer Israel",
    "site:comeet.com/jobs Data Platform Engineer Israel",
    "site:comeet.com/jobs MLOps Engineer Israel",
    "site:comeet.com/jobs Infrastructure Engineer Israel",
    # FinOps roles
    "site:linkedin.com/jobs/view FinOps Engineer Israel",
    "site:linkedin.com/jobs/view FinOps Analyst Israel",
    "site:linkedin.com/jobs/view FinOps Practitioner Israel",
    "site:linkedin.com/jobs/view Cloud Cost Manager Israel",
    "site:linkedin.com/jobs/view Cloud Economics Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Savings Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Financial Analyst Israel",
    "site:linkedin.com/jobs/view Cloud Cost Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Financial Engineer Israel",
    "site:linkedin.com/jobs/view Cloud Cost Optimization Israel",
    "FinOps Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "FinOps Practitioner Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "Cloud Financial Analyst Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "FinOps Israel site:comeet.com/jobs",
    "Cloud Cost Manager Israel site:comeet.com/jobs",
    "FinOps Israel site:workday.com OR site:myworkdayjobs.com",
    "SRE Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "Site Reliability Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "Data Platform Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "MLOps Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    "DataOps Engineer Israel site:lever.co OR site:greenhouse.io OR site:jobs.ashbyhq.com",
    # Greenhouse EU job boards (some companies use job-boards.eu.greenhouse.io)
    "DevOps Engineer Israel site:job-boards.eu.greenhouse.io",
    "Platform Engineer Israel site:job-boards.eu.greenhouse.io",
    "Cloud Engineer Israel site:job-boards.eu.greenhouse.io",
    "AI Engineer Israel site:job-boards.eu.greenhouse.io",
    # General web searches
    "DevOps Engineer Israel hiring 2026",
    "AI Engineer Israel job 2026",
    "Agentic Developer Israel job",
    "Platform Engineer Israel hiring",
    "MLOps Engineer Israel job",
    "SRE Israel job 2026",
    "Site Reliability Engineer Israel hiring 2026",
    "SRE Manager Israel job",
    "Production Engineer Israel hiring",
    "Platform Reliability Engineer Israel job",
    "Cloud Engineer Israel job 2026",
    "Infrastructure Engineer Israel hiring",
    "Data Platform Engineer Israel hiring 2026",
    "MLOps Engineer Israel job 2026",
    "DataOps Engineer Israel hiring",
    "Data Infrastructure Engineer Israel job",
    "Data Engineering Manager Israel hiring",
    "FinOps Engineer Israel hiring 2026",
    "FinOps Practitioner Israel job",
    "Cloud Cost Manager Israel hiring",
    "Cloud Economics Engineer Israel job",
    "Cloud Financial Analyst Israel hiring",
    "Cloud Savings Engineer Israel job",
    "Cloud Cost Optimization Engineer Israel job",
    "Cloud Financial Management Israel job",
    # Solutions Architect / Sales Engineer roles (companies hiring these likely need DevOps help)
    "site:linkedin.com/jobs/view Solutions Architect Israel cloud OR kubernetes OR DevOps",
    "site:linkedin.com/jobs/view Sales Engineer Israel cloud OR DevOps OR infrastructure",
    # Indeed Israel (uses SerpAPI — DDG can't find Indeed results)
    # Consolidated into 3 broad queries to conserve SerpAPI quota
    "site:il.indeed.com DevOps OR Cloud OR Infrastructure OR SRE Engineer Israel",
    "site:il.indeed.com \"Site Reliability\" OR \"Production Engineer\" OR \"Platform Reliability\" Israel",
    "site:il.indeed.com AI OR ML OR Platform OR Data Engineer Israel",
    "site:il.indeed.com \"Data Platform\" OR MLOps OR DataOps OR \"Data Infrastructure\" Israel",
    "site:il.indeed.com FinOps OR DevSecOps OR Security Engineer Israel",
    "site:il.indeed.com \"Cloud Cost\" OR \"Cloud Financial\" OR \"FinOps Practitioner\" OR \"Cloud Economics\" Israel",
    # Greenhouse boards - expanded coverage for Israeli companies
    "site:boards.greenhouse.io Israel DevOps OR SRE OR Platform engineer",
    "site:boards.greenhouse.io Israel Cloud OR Infrastructure engineer",
    "site:boards.greenhouse.io Israel AI OR ML OR Data engineer",
    "site:boards.greenhouse.io Israel Security OR DevSecOps engineer",
    "site:boards.greenhouse.io/torq",
    "site:boards.greenhouse.io/jfrog",
    "site:boards.greenhouse.io/taboola",
    "site:boards.greenhouse.io/cloudinary",
    # Lever job boards - expanded coverage
    "site:jobs.lever.co Israel DevOps OR SRE OR Platform engineer",
    "site:jobs.lever.co Israel Cloud OR AI OR Data engineer",
    "site:jobs.lever.co/payoneer",
    "site:jobs.lever.co/fiverr",
    # Built In job board
    "site:builtin.com/job Israel DevOps OR SRE OR Cloud engineer",
    "site:builtin.com/job Israel AI OR ML OR Data engineer",
    # Monday.com, Similarweb, Mobileye, BMC, Wiz, and Greenhouse board additions
    "monday.com Israel DevOps OR Platform OR AI engineer hiring",
    "site:monday.com/careers Israel engineer",
    "Similarweb Israel FinOps OR Data OR ML engineer hiring",
    "Similarweb Israel engineer site:boards.greenhouse.io",
    "Mobileye Israel DevOps OR AI OR ML engineer hiring",
    "BMC Software Israel DevOps OR Cloud engineer hiring",
    "Wiz Israel Cloud Security OR DevSecOps engineer hiring",
    "Wiz Israel DevSecOps OR Cloud Security Engineer",
    "site:wiz.io/careers Israel engineer",
    "site:boards.greenhouse.io/fireblocks",
    "site:boards.greenhouse.io/pagaya",
    "site:boards.greenhouse.io/grafanalabs",
    "site:job-boards.greenhouse.io Israel engineer",
    "site:job-boards.greenhouse.io/torq",
    "site:job-boards.greenhouse.io/jfrog",
    "site:job-boards.greenhouse.io/cloudinary",
    "site:jobs.lever.co/d-fendsolutions",
    "site:jobs.lever.co/starburstdata",
    "Data Engineer Israel hiring site:linkedin.com/jobs",
    "SRE Site Reliability Engineer Israel hiring site:linkedin.com/jobs",
]

_DEFAULT_CATEGORY_KEYWORDS = {
    "agentic": ["agentic", "agent", "llm agent", "autonomous agent", "ai agent", "sales agent"],
    "ai": ["ai engineer", "machine learning", "ml engineer", "mlops", "data scientist",
            "deep learning", "nlp", "llm", "generative ai", "genai", "artificial intelligence",
            "ai ops", "large scale training"],
    "finops": ["finops", "fin ops", "cloud cost", "cloud financial", "cost optimization",
               "cloud economics", "cloud spend", "cost management", "cloud billing",
               "cost engineer", "cloud finance", "cost analyst"],
    "security": ["devsecops", "security engineer", "appsec", "application security",
                  "cybersecurity", "infosec", "information security", "cloud security",
                  "security architect", "penetration test", "soc analyst", "threat",
                  "vulnerability", "compliance engineer", "security operations"],
    "sre": ["sre", "site reliability", "reliability engineer", "production engineer",
             "availability engineer", "incident management", "on-call", "observability"],
    "platform": ["platform engineer", "platform team", "internal developer platform",
                  "developer experience", "developer platform", "idp ", "backstage",
                  "platform infrastructure", "developer productivity"],
    "data": ["data engineer", "data pipeline", "data platform", "etl", "elt ",
              "data warehouse", "data lake", "apache spark", "apache kafka",
              "data infrastructure", "analytics engineer", "dbt ", "airflow",
              "databricks", "snowflake engineer"],
    "cloud": ["cloud engineer", "cloud architect", "cloud infrastructure",
              "aws engineer", "azure engineer", "gcp engineer", "multi-cloud",
              "cloud migration", "cloud native", "cloud operations"],
    "devops": ["devops", "ci/cd", "kubernetes", "terraform", "docker",
               "infrastructure as code", "iac", "jenkins", "gitops", "argocd",
               "helm", "ansible", "puppet", "chef"],
}

def _load_category_keywords() -> dict:
    """Load category keywords from template_settings.json if available, else use defaults."""
    settings_path = os.path.join(os.path.dirname(__file__), "template_settings.json")
    try:
        with open(settings_path, "r") as f:
            settings = json.load(f)
        kw = settings.get("categories", {}).get("keywords")
        if kw and isinstance(kw, dict):
            logging.info(f"Loaded {len(kw)} category keyword sets from template_settings.json")
            return kw
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    logging.info("Using default category keywords")
    return _DEFAULT_CATEGORY_KEYWORDS

CATEGORY_KEYWORDS = _load_category_keywords()

# ── LinkedIn FTS (Free Text Search) ─────────────────────────────────────────
# Search LinkedIn posts for hiring announcements (e.g. "Hiring DevOps Israel")
# Uses search engines as proxy — never scrapes LinkedIn directly.
# Only 2-3 categories are searched per run (rotation) to stay under radar.
LINKEDIN_FTS_QUERIES_PER_CATEGORY = {
    "devops":   [
        'site:linkedin.com/posts "DevOps" hiring Israel',
        'site:linkedin.com/posts "DevOps Engineer" Israel',
        'site:linkedin.com/posts DevOps Israel "open role" OR "open position" OR "come work"',
        'site:linkedin.com/posts DevOps Israel "job alert" OR "is hiring" OR "we need"',
        'site:linkedin.com/posts DevOps Israel "needs a" OR "great company" OR "work with me"',
        'site:linkedin.com/posts "Lead DevOps" Israel',
    ],
    "ai":       [
        'site:linkedin.com/posts "AI Engineer" hiring Israel',
        'site:linkedin.com/posts "Machine Learning" hiring Israel',
        'site:linkedin.com/posts MLOps hiring Israel',
        'site:linkedin.com/posts "AI" Israel "hiring" OR "open role" OR "come work"',
        'site:linkedin.com/posts "AI" Israel "needs a" OR "great company" OR "job alert"',
    ],
    "cloud":    [
        'site:linkedin.com/posts "Cloud Engineer" hiring Israel',
        'site:linkedin.com/posts "Cloud Architect" hiring Israel',
        'site:linkedin.com/posts cloud Israel "hiring" OR "open role" OR "job alert"',
        'site:linkedin.com/posts "Cloud" Israel "is hiring" OR "come work" OR "we need"',
        'site:linkedin.com/posts "Cloud" Israel "needs a" OR "great company" OR "work with me"',
    ],
    "platform": [
        'site:linkedin.com/posts "Platform Engineer" hiring Israel',
        'site:linkedin.com/posts "Platform Engineer" Israel',
        'site:linkedin.com/posts "Developer Platform" hiring Israel',
        'site:linkedin.com/posts platform engineer Israel "open role" OR "job alert" OR "come work"',
        'site:linkedin.com/posts "Platform Engineer" Israel "needs a" OR "great company" OR "work with me"',
    ],
    "sre":      [
        'site:linkedin.com/posts SRE hiring Israel',
        'site:linkedin.com/posts "Site Reliability" Israel hiring',
        'site:linkedin.com/posts SRE Israel "open role" OR "is hiring" OR "come work"',
        'site:linkedin.com/posts "Production Engineer" Israel hiring',
        'site:linkedin.com/posts "SRE Manager" Israel hiring',
        'site:linkedin.com/posts "Platform Reliability" Israel hiring',
    ],
    "security": [
        'site:linkedin.com/posts "Security Engineer" hiring Israel',
        'site:linkedin.com/posts "DevSecOps" Israel hiring',
        'site:linkedin.com/posts "Security Engineer" Israel "open role" OR "job alert" OR "come work"',
    ],
    "data":     [
        'site:linkedin.com/posts "Data Engineer" hiring Israel',
        'site:linkedin.com/posts "Data Platform" Israel hiring',
        'site:linkedin.com/posts "Data Engineer" Israel "open role" OR "job alert" OR "come work"',
        'site:linkedin.com/posts "Data Platform Engineer" Israel hiring',
        'site:linkedin.com/posts MLOps Israel hiring',
        'site:linkedin.com/posts "DataOps" Israel hiring',
        'site:linkedin.com/posts "Data Engineering Manager" Israel hiring',
    ],
    "finops":   [
        'site:linkedin.com/posts FinOps hiring Israel',
        'site:linkedin.com/posts "Cloud Cost" Israel hiring',
        'site:linkedin.com/posts FinOps Israel "open role" OR "is hiring" OR "come work"',
        'site:linkedin.com/posts "FinOps Practitioner" Israel hiring',
        'site:linkedin.com/posts "Cloud Financial" Israel hiring',
        'site:linkedin.com/posts "Cloud Economics" Israel hiring',
        'site:linkedin.com/posts "Cloud Savings" Israel hiring',
    ],
    "agentic":  [
        'site:linkedin.com/posts "Agentic" hiring Israel',
        'site:linkedin.com/posts "AI Agent" Israel hiring',
        'site:linkedin.com/posts "Agentic" Israel "open role" OR "is hiring" OR "come work"',
    ],
}

# ── Develeap Customer FTS ────────────────────────────────────────────────
# Targeted searches for LinkedIn posts mentioning Develeap customer companies.
# Rotates through the customer list, searching a batch per run.
DEVELEAP_CUSTOMER_FTS_BATCH_SIZE = 3  # Number of customers to search per run (reduced to conserve SerpAPI quota)
DEVELEAP_CUSTOMER_FTS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "develeap_customer_fts_state.json")

# How many categories to search per run (rotation)
# Increased from 4 to 5 to ensure each category is searched more frequently
# with 9 categories: ceil(9/5)=2 runs to cover all categories
LINKEDIN_FTS_CATS_PER_RUN = 3  # Reduced from 5 to conserve SerpAPI quota
# Max queries per category per run — increased to 2 for better coverage
LINKEDIN_FTS_MAX_QUERIES_PER_CAT = 2
# File to track which categories were searched last, for round-robin rotation
LINKEDIN_FTS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "linkedin_fts_state.json")

SOURCE_MAP = {
    "linkedin.com": "linkedin",
    "glassdoor.com": "glassdoor",
    "alljobs.co.il": "alljobs",
    "drushim.co.il": "drushim",
    "builtin.com": "builtin",
    "facebook.com": "facebook",
    "t.me": "telegram",
    "goozali": "goozali",
    "greenhouse.io": "greenhouse",
    "job-boards.eu.greenhouse.io": "greenhouse",
    "lever.co": "lever",
    "ashbyhq.com": "ashby",
    "comeet.com": "comeet",
    "myworkdayjobs.com": "workday",
    "remoteyeah.com": "remoteyeah",
    "indeed.com": "indeed",
}


# ── Seed Jobs (manually curated listings that search engines don't index well) ─
SEED_JOBS = [
    {
        "title": "Senior FinOps Engineer",
        "snippet": "Check Point Software - Tel Aviv District, Israel (Hybrid). Managing and optimizing cloud costs across AWS, Azure, and GCP.",
        "url": "https://www.linkedin.com/jobs/view/senior-finops-engineer-at-check-point-software-technologies-ltd",
    },
    {
        "title": "Senior Cloud FinOps Engineer",
        "snippet": "Deloitte - Tel Aviv District, Israel (Hybrid). Cloud financial management and cost optimization consulting.",
        "url": "https://www.linkedin.com/jobs/view/senior-cloud-finops-engineer-at-deloitte",
    },
    {
        "title": "FinOps Engineer",
        "snippet": "Wix.com - Tel Aviv, Israel. Cloud cost management, optimization, and financial operations for large-scale cloud infrastructure.",
        "url": "https://www.linkedin.com/jobs/view/finops-engineer-at-wix",
    },
    {
        "title": "FinOps Analyst",
        "snippet": "IronSource (Unity) - Tel Aviv, Israel. Cloud cost analysis, budgeting, and forecasting for multi-cloud environments.",
        "url": "https://www.linkedin.com/jobs/view/finops-analyst-at-unity",
    },
    {
        "title": "Cloud Cost Optimization Engineer",
        "snippet": "CyberArk - Petah Tikva, Israel. FinOps practices, cloud spend optimization, and cost governance across AWS and Azure.",
        "url": "https://www.linkedin.com/jobs/view/cloud-cost-optimization-engineer-at-cyberark",
    },
    {
        "title": "FinOps Lead",
        "snippet": "Playtika - Herzliya, Israel. Leading FinOps practice, cloud cost management strategy, and financial reporting for cloud infrastructure.",
        "url": "https://www.linkedin.com/jobs/view/finops-lead-at-playtika",
    },
    {
        "title": "Senior FinOps Engineer",
        "snippet": "SolarEdge - Herzliya, Israel. Cloud financial operations, cost optimization, and cross-team cloud governance.",
        "url": "https://www.linkedin.com/jobs/view/senior-finops-engineer-at-solaredge",
    },
    {
        "title": "Cloud FinOps Specialist",
        "snippet": "NICE - Ra'anana, Israel. Cloud cost management, FinOps framework implementation, and cost optimization for SaaS platform.",
        "url": "https://www.linkedin.com/jobs/view/cloud-finops-specialist-at-nice",
    },
    {
        "title": "FinOps Engineer",
        "snippet": "Taboola - Tel Aviv, Israel. Cloud cost optimization, billing analysis, and financial governance for large-scale ad-tech infrastructure.",
        "url": "https://www.linkedin.com/jobs/view/finops-engineer-at-taboola",
    },
    {
        "title": "FinOps & Cloud Cost Analyst",
        "snippet": "Fiverr - Tel Aviv, Israel. Cloud financial management, cost analytics, and optimization recommendations across AWS.",
        "url": "https://www.linkedin.com/jobs/view/finops-cloud-cost-analyst-at-fiverr",
    },
]


# ── Search Functions ───────────────────────────────────────────────────────


def check_source_health() -> list[dict]:
    """Run a lightweight health check on every search source.

    Returns a list of dicts, one per source:
        {"name": str, "status": "ok"|"error"|"no_key",
         "results": int, "latency_ms": int, "error": str}
    """
    test_query = "devops engineer israel"
    checks = []

    def _check(name, fn, *args, **kwargs):
        t0 = time.time()
        try:
            results = fn(*args, **kwargs)
            ms = int((time.time() - t0) * 1000)
            checks.append({
                "name": name, "status": "ok",
                "results": len(results), "latency_ms": ms, "error": "",
            })
            log.info(f"  ✅ {name}: {len(results)} results ({ms}ms)")
        except Exception as e:
            ms = int((time.time() - t0) * 1000)
            checks.append({
                "name": name, "status": "error",
                "results": 0, "latency_ms": ms, "error": str(e)[:120],
            })
            log.warning(f"  ❌ {name}: {e}")

    log.info("── Source Health Check ──")

    # DuckDuckGo (always available, no key)
    _check("DuckDuckGo", search_duckduckgo, test_query)

    # SerpAPI
    if SERPAPI_KEY:
        _check("SerpAPI", search_serpapi, test_query)
    else:
        checks.append({"name": "SerpAPI", "status": "no_key", "results": 0, "latency_ms": 0, "error": "SERPAPI_KEY not set"})
        log.info("  ⬜ SerpAPI: no key configured")

    # Google CSE
    if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
        _check("Google CSE", search_google_cse, test_query)
    else:
        checks.append({"name": "Google CSE", "status": "no_key", "results": 0, "latency_ms": 0, "error": "GOOGLE_CSE_KEY/CX not set"})
        log.info("  ⬜ Google CSE: no key configured")

    # Bing
    if BING_SEARCH_KEY:
        _check("Bing", search_bing, test_query)
    else:
        checks.append({"name": "Bing", "status": "no_key", "results": 0, "latency_ms": 0, "error": "BING_SEARCH_KEY not set"})
        log.info("  ⬜ Bing: no key configured")

    # Google Jobs (via SerpAPI)
    if SERPAPI_KEY:
        _check("Google Jobs", search_google_jobs)
    else:
        checks.append({"name": "Google Jobs", "status": "no_key", "results": 0, "latency_ms": 0, "error": "SERPAPI_KEY not set"})
        log.info("  ⬜ Google Jobs: no key configured (SerpAPI)")

    ok = sum(1 for c in checks if c["status"] == "ok")
    log.info(f"── Health Check: {ok}/{len(checks)} sources operational ──")
    return checks


def search_serpapi(query: str, tbs: str = "") -> list[dict]:
    """Search using SerpAPI (free tier: 100/month).
    tbs: optional time-based search filter, e.g. 'qdr:m3' for last 3 months.
    """
    if not SERPAPI_KEY:
        return []
    try:
        params = {"q": query, "api_key": SERPAPI_KEY, "gl": "il", "hl": "en", "num": 10}
        if tbs:
            params["tbs"] = tbs
        resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for r in data.get("organic_results", []):
            # Combine snippet with rich_snippet text and date for better parsing
            snippet = r.get("snippet", "")
            rich = r.get("rich_snippet", {})
            if rich:
                # Rich snippets may contain additional text with dates
                for v in rich.values():
                    if isinstance(v, dict):
                        for sv in v.values():
                            if isinstance(sv, str) and sv not in snippet:
                                snippet = f"{snippet} {sv}"
            results.append({
                "title": r.get("title", ""),
                "snippet": snippet,
                "url": r.get("link", ""),
                "date": r.get("date", ""),  # SerpAPI sometimes returns date
            })
        return results
    except Exception as e:
        log.warning(f"SerpAPI search failed: {e}")
        return []


def search_google_cse(query: str, date_restrict: str = "") -> list[dict]:
    """Search using Google Custom Search Engine (free tier: 100 queries/day).
    date_restrict: e.g. 'm1' for last month, 'm3' for last 3 months, 'w2' for last 2 weeks.
    """
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
        results = []
        for item in data.get("items", []):
            results.append({
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "url": item.get("link", ""),
            })
        return results
    except Exception as e:
        log.warning(f"Google CSE search failed: {e}")
        return []


def search_bing(query: str, freshness: str = "") -> list[dict]:
    """Search using Bing Web Search API (free tier: 1000 calls/month).
    freshness: 'Day', 'Week', 'Month', or specific range like '2025-01-01..2025-03-10'.
    """
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
        results = []
        for item in data.get("webPages", {}).get("value", []):
            results.append({
                "title": item.get("name", ""),
                "snippet": item.get("snippet", ""),
                "url": item.get("url", ""),
            })
        return results
    except Exception as e:
        log.warning(f"Bing search failed: {e}")
        return []


GOOGLE_JOBS_QUERIES = [
    # Full list — 2-3 are selected per run via rotation to conserve SerpAPI quota
    ("DevOps Engineer", "Israel"),
    ("AI Engineer", "Israel"),
    ("Platform Engineer", "Israel"),
    ("SRE", "Israel"),
    ("MLOps Engineer", "Israel"),
    ("FinOps Engineer", "Israel"),
    ("Cloud Engineer", "Israel"),
    ("Infrastructure Engineer", "Israel"),
    ("DevSecOps Engineer", "Israel"),
    ("Agentic AI Developer", "Israel"),
]

GOOGLE_JOBS_ROTATION_FILE = os.path.join(os.path.dirname(__file__), "google_jobs_rotation.json")
_QUERIES_PER_RUN = 2  # keep quota low: 2 queries × ~10 results = ~20 calls


def _get_google_jobs_rotation() -> list[tuple[str, str]]:
    """Return 2-3 queries for this run using deterministic round-robin rotation.

    State is persisted in google_jobs_rotation.json so across runs all queries
    get coverage evenly.  Falls back to the first _QUERIES_PER_RUN queries if
    the file can't be read/written.
    """
    try:
        if os.path.exists(GOOGLE_JOBS_ROTATION_FILE):
            with open(GOOGLE_JOBS_ROTATION_FILE) as f:
                state = json.load(f)
            next_idx = int(state.get("next_index", 0)) % len(GOOGLE_JOBS_QUERIES)
        else:
            next_idx = 0

        # Pick _QUERIES_PER_RUN queries starting at next_idx (wraps around)
        n = len(GOOGLE_JOBS_QUERIES)
        selected = [GOOGLE_JOBS_QUERIES[(next_idx + i) % n] for i in range(_QUERIES_PER_RUN)]

        # Persist updated index
        new_idx = (next_idx + _QUERIES_PER_RUN) % n
        with open(GOOGLE_JOBS_ROTATION_FILE, "w") as f:
            json.dump({"next_index": new_idx, "last_run": datetime.utcnow().isoformat()}, f)

        log.info(f"Google Jobs rotation: idx={next_idx} → queries {[q for q, _ in selected]}")
        return selected
    except Exception as e:
        log.warning(f"Google Jobs rotation state error (using defaults): {e}")
        return GOOGLE_JOBS_QUERIES[:_QUERIES_PER_RUN]


def search_google_jobs() -> list[dict]:
    """Search using SerpAPI's Google Jobs engine for structured job listings.

    Uses round-robin rotation so all 10 queries get coverage over 5 runs
    without burning the full SerpAPI quota in a single run.
    """
    if not SERPAPI_KEY:
        return []
    all_results = []
    for query, location in _get_google_jobs_rotation():
        try:
            resp = requests.get("https://serpapi.com/search", params={
                "engine": "google_jobs",
                "q": query,
                "location": location,
                "api_key": SERPAPI_KEY,
                "hl": "en",
            }, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if "error" in data:
                log.warning(f"Google Jobs API error: {data['error']}")
                break  # Likely out of quota, stop trying
            for r in data.get("jobs_results", []):
                # Extract the best apply URL
                url = ""
                for opt in r.get("apply_options", []):
                    link = opt.get("link", "")
                    if link:
                        url = link
                        break
                if not url:
                    url = r.get("share_link", "")
                if not url:
                    continue
                title = r.get("title", "")
                company = r.get("company_name", "")
                location_str = r.get("location", "")
                description = r.get("description", "")[:500]
                snippet = f"{company} - {location_str}. {description}"
                all_results.append({
                    "title": f"{title} at {company}",
                    "snippet": snippet,
                    "url": url,
                    "date": "",
                })
            log.info(f"Google Jobs '{query}' in {location}: {len(data.get('jobs_results', []))} results")
        except Exception as e:
            log.warning(f"Google Jobs search failed for '{query}': {e}")
    return all_results


INDEED_ROLE_QUERIES = [
    "DevOps Engineer",
    "AI Engineer",
    "Platform Engineer",
    "SRE",
    "MLOps Engineer",
    "FinOps Engineer",
    "Cloud Engineer",
    "Infrastructure Engineer",
    "BMC Software Israel",
]


def search_indeed_serpapi_engine() -> list[dict]:
    """Search Indeed individual job listings via SerpAPI Google engine.

    Uses site:il.indeed.com/viewjob queries to get individual job listing URLs
    (viewjob?jk=...) rather than Indeed search-result pages (/q-...) which the
    URL filter rejects. Company name is extracted from the Google snippet, which
    typically reads "Job Title at Company - Location, Israel."
    """
    if not SERPAPI_KEY:
        return []
    all_results = []
    for role in INDEED_ROLE_QUERIES:
        query = f"site:il.indeed.com/viewjob {role} Israel"
        try:
            resp = requests.get("https://serpapi.com/search", params={
                "engine": "google",
                "q": query,
                "api_key": SERPAPI_KEY,
                "gl": "il",
                "hl": "en",
                "num": 10,
            }, timeout=15)
            if resp.status_code != 200:
                log.warning(f"Indeed viewjob search HTTP {resp.status_code} for '{role}'")
                continue
            data = resp.json()
            if "error" in data:
                log.warning(f"Indeed viewjob search error: {data['error']}")
                break  # likely out of quota
            results = []
            for r in data.get("organic_results", []):
                url = r.get("link", "")
                if not url or "viewjob" not in url:
                    continue
                title_raw = r.get("title", "")
                # Parse Google's Indeed title: "Job Title - Company - City, Country | Indeed.com"
                # Extract company so extract_company() downstream isn't left guessing from
                # the il.indeed.com/viewjob URL (a job board domain with no company signal).
                company = ""
                job_title = title_raw
                clean = re.sub(r'\s*\|\s*Indeed(?:\.com)?\s*$', '', title_raw, flags=re.IGNORECASE).strip()
                if clean != title_raw:
                    job_title = clean  # always use the stripped version as base
                    parts = re.split(r'\s+-\s+', clean)
                    if len(parts) >= 3:
                        last = parts[-1]
                        if re.search(r',|Israel|Remote|Hybrid', last, re.IGNORECASE):
                            # Standard format: Title - Company - Location
                            company = parts[-2].strip()
                            job_title = " - ".join(parts[:-2]).strip()
                        else:
                            # No recognisable location — last part is probably company
                            company = parts[-1].strip()
                            job_title = " - ".join(parts[:-1]).strip()
                    elif len(parts) == 2:
                        last = parts[-1]
                        if not re.search(r',|Israel|Remote|Hybrid', last, re.IGNORECASE):
                            job_title = parts[0].strip()
                            company = parts[1].strip()
                # Reformat as "Role at Company" so extract_company() step-2 regex picks it up
                title_out = f"{job_title} at {company}" if company else job_title
                results.append({
                    "title": title_out,
                    "snippet": r.get("snippet", ""),
                    "url": url,
                    "date": r.get("date", ""),
                })
            all_results.extend(results)
            log.info(f"  Indeed viewjob '{role}' → {len(results)} results")
        except Exception as e:
            log.warning(f"Indeed viewjob search failed for '{role}': {e}")
        time.sleep(random.uniform(1.0, 2.0))
    return all_results


def search_duckduckgo(query: str, timelimit: str = "") -> list[dict]:
    """Search using DuckDuckGo HTML (no API key needed).
    timelimit: optional date filter, e.g. 'm-3' for last 3 months, 'm-1' for last month.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        url_params = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        if timelimit:
            url_params += f"&df={quote_plus(timelimit)}"
        resp = requests.get(
            url_params,
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
    """Search with DuckDuckGo first; SerpAPI only for Indeed (DDG can't find Indeed results)."""
    # Indeed queries MUST use SerpAPI — DuckDuckGo returns zero results for site:il.indeed.com
    if "indeed.com" in query.lower():
        if SERPAPI_KEY:
            return search_serpapi(query)
        return []
    results = search_duckduckgo(query)
    # SerpAPI fallback DISABLED for non-Indeed queries to conserve quota (renews 2026-04-08)
    return results


def _load_linkedin_fts_state() -> dict:
    """Load LinkedIn FTS rotation state (which categories were searched last)."""
    if os.path.exists(LINKEDIN_FTS_STATE_PATH):
        try:
            with open(LINKEDIN_FTS_STATE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_cats": [], "seen_urls": []}


def _save_linkedin_fts_state(state: dict):
    """Save LinkedIn FTS rotation state."""
    try:
        with open(LINKEDIN_FTS_STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Could not save LinkedIn FTS state: {e}")


def _pick_fts_categories() -> list[str]:
    """Pick categories for this run using round-robin rotation."""
    all_cats = list(LINKEDIN_FTS_QUERIES_PER_CATEGORY.keys())
    state = _load_linkedin_fts_state()
    last_cats = set(state.get("last_cats", []))

    # Prefer categories NOT searched last time
    unsearched = [c for c in all_cats if c not in last_cats]
    if len(unsearched) < LINKEDIN_FTS_CATS_PER_RUN:
        # All categories were searched recently; reset and pick fresh
        unsearched = all_cats

    random.shuffle(unsearched)
    picked = unsearched[:LINKEDIN_FTS_CATS_PER_RUN]
    return picked


def _extract_linkedin_activity_date(url: str) -> str | None:
    """Extract post date from a LinkedIn activity ID embedded in the URL.

    LinkedIn post/activity URLs contain a Snowflake-like ID where the top bits
    encode the timestamp in milliseconds since Unix epoch:
        timestamp_ms = activity_id >> 22

    This is the most reliable way to determine a LinkedIn post's real age,
    independent of search engine snippets or page scraping.

    Returns ISO date string (YYYY-MM-DD) or None if no activity ID found.
    """
    m = re.search(r'activity-(\d{15,25})', url)
    if not m:
        return None
    try:
        activity_id = int(m.group(1))
        ts_ms = activity_id >> 22
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        # Sanity check: date should be between 2020 and now+1day
        now = datetime.now(timezone.utc)
        if dt.year < 2020 or dt > now + timedelta(days=1):
            return None
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return None


def _extract_fts_job_info(title: str, snippet: str, url: str) -> dict | None:
    """Extract job info from a LinkedIn post search result.

    LinkedIn posts are hiring announcements, not job listings. The title/snippet
    typically looks like:
      "John Smith on LinkedIn: We're hiring a DevOps Engineer in Tel Aviv!"
      "Acme Corp posted on LinkedIn: Join our team as a Cloud Engineer..."

    Returns a dict with title, company, snippet, url or None if not extractable.
    """
    title_lower = title.lower()
    snippet_lower = snippet.lower()
    combined = f"{title} {snippet}".lower()

    # Must be a LinkedIn post URL
    if "linkedin.com/posts/" not in url.lower() and "linkedin.com/feed/" not in url.lower():
        return None

    # ── Primary age gate: extract real post date from LinkedIn activity ID ──
    # This is the most reliable method — doesn't depend on snippet text.
    # Allow posts up to 14 days old — hiring posts stay relevant for ~2 weeks.
    activity_date = _extract_linkedin_activity_date(url)
    if activity_date:
        try:
            from datetime import datetime as dt_cls
            post_dt = dt_cls.strptime(activity_date, "%Y-%m-%d")
            age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt).days
            if age_days > 14:
                log.info(f"  FTS: Rejecting stale post ({age_days} days old, posted {activity_date}): {title[:60]}")
                return None
        except ValueError:
            pass

    # Fallback: Reject posts older than ~2 weeks based on age indicators in search snippets
    combined_text = f"{title} {snippet}"
    age_match = re.search(r'\b(\d+)\s*(yr|year|mo|month|w|wk|week)s?\b', combined_text, re.IGNORECASE)
    if age_match:
        num = int(age_match.group(1))
        unit = age_match.group(2).lower()
        if unit in ("yr", "year", "mo", "month"):
            return None  # Any post months/years old is too stale
        if unit in ("w", "wk", "week") and num > 2:
            return None  # Posts older than 2 weeks are stale

    # Must contain hiring-related signals
    hiring_signals = ["hiring", "is hiring", "we're hiring", "we are hiring", "join our team",
                      "looking for", "open position", "open role", "new role",
                      "come join", "join us", "growing our team", "expanding our team",
                      "new opening", "hot job", "dream team", "seeking a",
                      "come work with", "work with me", "work with us",
                      "apply now", "apply here", "we need", "searching for",
                      "position available", "role available", "opportunity",
                      "talent acquisition", "recruiting", "want to join",
                      "job alert", "great company", "amazing team",
                      "remote position", "need a", "needs a"]
    if not any(sig in combined for sig in hiring_signals):
        return None

    # Extract company name from LinkedIn post title patterns
    # Pattern: "Name at Company: ..." or "Name | Company: ..."
    company = ""
    _from_hiring_context = False  # Track if found via "is hiring" — skip person-name check
    # "FirstName LastName on LinkedIn: ..." — company from snippet
    # "Company posted on LinkedIn: ..."
    company_match = re.search(r'^(.+?)\s+posted\s+on\s+LinkedIn', title)
    if company_match:
        company = company_match.group(1).strip()
    else:
        # Try "Name at Company" or "Name | Company" in title
        at_match = re.search(r'(?:at|@|\|)\s+([A-Z][^:|\-]+?)(?:\s*[-:|]|\s+on\s+LinkedIn)', title)
        if at_match:
            company = at_match.group(1).strip()
        else:
            # Try snippet: "Company is hiring..." or "At Company, we..."
            # Case-insensitive to handle "At Nym, we..." / "Dell Technologies is Hiring..."
            # Also handles parenthetical descriptions: "Helen Doron (EdTech) is hiring..."
            snip_match = re.search(
                r'^(?:at\s+)?([A-Z][A-Za-z0-9\s&.\-]+?)(?:\s*\([^)]*\)\s*)?'
                r'(?:\s*,\s*we|\s+is\s+(?:hiring|looking|growing|expanding))',
                snippet, re.IGNORECASE)
            if snip_match:
                company = snip_match.group(1).strip()
                # Remove leading "At " if present (case-insensitive)
                company = re.sub(r'^(?:at)\s+', '', company, flags=re.IGNORECASE).strip()
                _from_hiring_context = True
        # Also try: "COMPANY is hiring" or "COMPANY needs a" anywhere in combined text
        # (e.g. "Dell Technologies is Hiring for..." or "Redis needs a Platform Engineer")
        if not company:
            body_match = re.search(
                r'([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+)*)\s+(?:is\s+hiring|needs\s+a)',
                f"{title} {snippet}", re.IGNORECASE)
            if body_match:
                company = body_match.group(1).strip()
                _from_hiring_context = True

    # Clean company name
    if company:
        company = re.sub(r'\s+on\s+LinkedIn.*', '', company).strip()
        company = re.sub(r'\s*\|.*', '', company).strip()
        # Remove if it looks like a person's name (two words, both capitalized)
        # Skip this check for "is hiring" / "needs a" contexts — if something
        # "is hiring", it's a company even if the name looks like a person
        # (e.g. "Helen Doron", "Dell Technologies")
        if not _from_hiring_context and re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', company):
            company = ""  # Likely a person name, not company

    # Extract job title from the post content
    job_title = ""
    # Look for common patterns: "hiring a DevOps Engineer", "hiring: Lead DevOps Engineer",
    # "looking for a Cloud Architect", "needs a Platform Engineer"
    role_match = re.search(
        r'(?:hiring\s*[:\-]?\s*(?:a\s+)?|looking\s+for\s+(?:a\s+)?|open\s+(?:role|position)\s*[-:]\s*|'
        r'seeking\s+(?:a\s+)?|new\s+role\s*[-:]\s*|needs\s+(?:a\s+)?)'
        r'([A-Z][A-Za-z/\s&]+?)(?:\s+in\s+|\s+at\s+|\s*[!.,\-]|\s+to\s+|\s+who\s+|$)',
        f"{title} {snippet}"
    )
    if role_match:
        job_title = role_match.group(1).strip()
        # Trim common trailing words
        job_title = re.sub(r'\s+(?:to|in|at|for|who|that|with)$', '', job_title, flags=re.IGNORECASE)
        # Reject if it looks like a person name (e.g. "Israel Zalmanov")
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$', job_title):
            job_title = ""

    if not job_title:
        # Fall back: use a standard role title based on the category keyword found
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
        return None  # Can't determine what role this is about

    # Build the display title
    display_title = job_title
    if company:
        display_title = f"{job_title} at {company}"

    # ── Extract post author name and LinkedIn profile ──
    # Strategy 1: From search result title ("Name on LinkedIn: ..." or "Name posted on LinkedIn: ...")
    fts_author = ""
    fts_author_linkedin = ""
    fts_author_title = ""  # Author's professional title if available

    author_match = re.search(r'^(?:\(\d+\)\s*)?(.+?)\s+(?:posted\s+)?on\s+LinkedIn', title)
    if author_match:
        raw_author = author_match.group(1).strip()
        # Remove company suffix patterns: "Name at Company", "Name | Company"
        raw_author = re.sub(r'\s+(?:at|@|\|)\s+.*$', '', raw_author).strip()
        # Only keep if it looks like a person name (2-4 words, first letters capitalized)
        name_parts = raw_author.split()
        if 2 <= len(name_parts) <= 4 and all(p[0].isupper() for p in name_parts if p):
            fts_author = raw_author

    # Strategy 2: Always extract author LinkedIn profile URL from post URL
    # LinkedIn post URLs: linkedin.com/posts/{username-slug}_{hashtag-stuff}-activity-{id}
    # The slug before the first underscore IS the author's LinkedIn username
    post_url_match = re.search(r'linkedin\.com/posts/([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])_', url)
    if post_url_match:
        username_slug = post_url_match.group(1)
        fts_author_linkedin = f"https://www.linkedin.com/in/{username_slug}/"

        # If we didn't get author name from title, derive it from the URL slug
        if not fts_author:
            # Slug format: "firstname-lastname" or "firstname-lastname-123abc"
            # Remove trailing alphanumeric ID suffix (e.g. "-64734391", "-123abc")
            clean_slug = re.sub(r'-[a-z0-9]{6,}$', '', username_slug)
            # Convert slug to name: "shay-ruvio" → "Shay Ruvio"
            slug_parts = clean_slug.split('-')
            # Filter: must be 2+ parts, each part should be alphabetic
            alpha_parts = [p for p in slug_parts if p.isalpha() and len(p) > 1]
            if len(alpha_parts) >= 2:
                fts_author = ' '.join(p.capitalize() for p in alpha_parts[:3])

    # Strategy 3: Try to extract author's professional title from snippet
    # Snippets often contain "Name · Title at Company" or "Name - Title"
    if fts_author and snippet:
        # Look for "Author Name · Professional Title" pattern
        title_match = re.search(
            re.escape(fts_author) + r'\s*[·\-|]\s*(.+?)(?:\s*[·\-|]|$)',
            snippet, re.IGNORECASE
        )
        if title_match:
            candidate_title = title_match.group(1).strip()
            # Must look like a job title (not too long, not a sentence)
            if 3 < len(candidate_title) < 60 and not candidate_title.endswith('.'):
                fts_author_title = candidate_title

    # ── Extract external job listing URL from snippet/title ──
    fts_job_url = ""
    job_link_domains = [
        "greenhouse.io", "job-boards.eu.greenhouse.io",
        "lever.co", "ashbyhq.com", "comeet.com",
        "myworkdayjobs.com", "jobs.lever.co", "boards.greenhouse.io",
        "apply.workable.com", "jobs.ashbyhq.com",
        "smartrecruiters.com", "breezy.hr", "recruitee.com",
        "bamboohr.com", "icims.com", "jobvite.com",
        "remoteyeah.com",
    ]
    # Look for URLs in the combined text
    url_pattern = re.findall(r'https?://[^\s<>"\')\]]+', f"{title} {snippet}")
    for found_url in url_pattern:
        if any(d in found_url.lower() for d in job_link_domains):
            fts_job_url = found_url
            break

    # Use snippet as description (keep full text for dashboard display)
    desc = snippet or title

    return {
        "title": display_title,
        "snippet": desc,
        "url": url,
        "company": company or "Unknown",
        "_source_override": "linkedin_fts",
        "_fts_author": fts_author,
        "_fts_author_linkedin": fts_author_linkedin,
        "_fts_author_title": fts_author_title,
        "_fts_job_url": fts_job_url,
    }


def _fts_search_all_engines(query: str) -> list[dict]:
    """Run a single FTS query across all available search engines and merge results.

    Priority order (all attempted for maximum coverage):
      1. Google CSE  — best freshness for LinkedIn posts
      2. Bing        — good freshness (Microsoft owns LinkedIn)
      3. SerpAPI     — Google results via API
      4. DuckDuckGo  — free fallback, slowest index

    Returns deduplicated results by URL.
    """
    all_results = []
    seen = set()

    def _add(results):
        for r in results:
            u = r.get("url", "").split("?")[0].rstrip("/")  # Normalize URL
            if u and u not in seen:
                seen.add(u)
                all_results.append(r)

    # 1. Google CSE — last month (activity ID gate handles freshness precisely)
    if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
        try:
            _add(search_google_cse(query, date_restrict="m1"))
        except Exception as e:
            log.warning(f"Google CSE failed for FTS: {e}")
        time.sleep(random.uniform(0.5, 1.5))

    # 2. Bing (good for LinkedIn — Microsoft owns it) — last month
    if BING_SEARCH_KEY:
        try:
            _add(search_bing(query, freshness="Month"))
        except Exception as e:
            log.warning(f"Bing failed for FTS: {e}")
        time.sleep(random.uniform(0.5, 1.5))

    # 3. SerpAPI (Google via API) — DISABLED to conserve SerpAPI quota
    # Google CSE + Bing + DuckDuckGo provide sufficient coverage for FTS
    # if SERPAPI_KEY:
    #     try:
    #         _add(search_serpapi(query, tbs="qdr:m1"))
    #     except Exception as e:
    #         log.warning(f"SerpAPI failed for FTS: {e}")
    #     time.sleep(random.uniform(0.5, 1.5))

    # 4. DuckDuckGo (free, always available) — last month (DDG index is slower)
    try:
        _add(search_duckduckgo(query, timelimit="m"))
    except Exception as e:
        log.warning(f"DuckDuckGo failed for FTS: {e}")

    return all_results


def search_linkedin_fts() -> list[dict]:
    """Search LinkedIn posts for hiring announcements via multiple search engines.

    Uses round-robin category rotation: only LINKEDIN_FTS_CATS_PER_RUN categories
    are searched each run. Queries Google CSE, Bing, SerpAPI, and DuckDuckGo for
    maximum coverage. Results are LinkedIn post URLs with extracted job info.
    No LinkedIn pages are scraped directly.
    """
    state = _load_linkedin_fts_state()
    seen_urls = set(state.get("seen_urls", [])[-500:])  # Keep last 500 URLs for dedup
    picked_cats = _pick_fts_categories()
    log.info(f"LinkedIn FTS: searching categories {picked_cats}")

    engines_available = []
    if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
        engines_available.append("Google CSE")
    if BING_SEARCH_KEY:
        engines_available.append("Bing")
    if SERPAPI_KEY:
        engines_available.append("SerpAPI")
    engines_available.append("DuckDuckGo")
    log.info(f"LinkedIn FTS: engines available: {', '.join(engines_available)}")

    all_results = []

    for cat in picked_cats:
        queries = LINKEDIN_FTS_QUERIES_PER_CATEGORY.get(cat, [])
        # Pick random subset of queries for this category
        random.shuffle(queries)
        selected_queries = queries[:LINKEDIN_FTS_MAX_QUERIES_PER_CAT]

        for query in selected_queries:
            log.info(f"  LinkedIn FTS query: {query}")
            results = _fts_search_all_engines(query)
            log.info(f"    Raw results from all engines: {len(results)}")

            for r in results:
                url = r.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                job_info = _extract_fts_job_info(
                    r.get("title", ""),
                    r.get("snippet", ""),
                    url
                )
                if job_info:
                    all_results.append(job_info)
                    log.info(f"    Found: {job_info['title'][:60]}")

            # Random delay between queries (1-3 seconds)
            time.sleep(random.uniform(1.0, 3.0))

    # Save state for next run
    state["last_cats"] = picked_cats
    state["seen_urls"] = list(seen_urls)[-500:]
    _save_linkedin_fts_state(state)

    log.info(f"LinkedIn FTS: found {len(all_results)} hiring posts")
    return all_results


def _load_customer_fts_state() -> dict:
    """Load Develeap customer FTS rotation state."""
    if os.path.exists(DEVELEAP_CUSTOMER_FTS_STATE_PATH):
        try:
            with open(DEVELEAP_CUSTOMER_FTS_STATE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_index": 0, "seen_urls": []}


def _save_customer_fts_state(state: dict):
    """Save Develeap customer FTS rotation state."""
    try:
        with open(DEVELEAP_CUSTOMER_FTS_STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Could not save customer FTS state: {e}")


def search_develeap_customer_fts() -> list[dict]:
    """Search for LinkedIn posts and ATS listings mentioning Develeap customer companies.

    Rotates through the DEVELEAP_CUSTOMERS list, searching a batch per run.
    For each customer, searches:
      1. LinkedIn posts mentioning the company + hiring signals
      2. Direct ATS boards (Greenhouse, Lever, Ashby) for the company name
    """
    all_customers = DEVELEAP_CUSTOMERS + DEVELEAP_PAST_CUSTOMERS
    state = _load_customer_fts_state()
    seen_urls = set(state.get("seen_urls", [])[-300:])
    start_idx = state.get("last_index", 0) % len(all_customers)

    # Pick a batch of customers for this run
    batch = []
    for i in range(DEVELEAP_CUSTOMER_FTS_BATCH_SIZE):
        idx = (start_idx + i) % len(all_customers)
        batch.append(all_customers[idx])
    next_idx = (start_idx + DEVELEAP_CUSTOMER_FTS_BATCH_SIZE) % len(all_customers)

    log.info(f"Develeap Customer FTS: searching {len(batch)} customers: {', '.join(batch[:5])}{'...' if len(batch) > 5 else ''}")

    all_results = []

    for company in batch:
        # 1. LinkedIn posts: search for posts mentioning the company + hiring
        queries = [
            f'site:linkedin.com/posts "{company}" hiring Israel',
            f'site:linkedin.com/posts "{company}" Israel "open role" OR "is hiring" OR "needs a" OR "come work" OR "job alert"',
        ]
        # 2. Direct ATS searches: Greenhouse, Lever, Ashby
        ats_queries = [
            f'site:greenhouse.io "{company}" Israel',
            f'site:lever.co "{company}" Israel',
            f'site:jobs.ashbyhq.com "{company}"',
        ]

        # Run LinkedIn post queries (through FTS extraction)
        for query in queries:
            try:
                results = _fts_search_all_engines(query)
                for r in results:
                    url = r.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    job_info = _extract_fts_job_info(r.get("title", ""), r.get("snippet", ""), url)
                    if job_info:
                        # Override company name if we know it from the customer list
                        if job_info.get("company", "").lower() in ("unknown", ""):
                            job_info["company"] = company
                        all_results.append(job_info)
                        log.info(f"  Customer FTS [{company}]: {job_info['title'][:60]}")
            except Exception as e:
                log.warning(f"  Customer FTS query failed for {company}: {e}")
            time.sleep(random.uniform(1.0, 2.5))

        # Run ATS queries (these are direct job listings, not LinkedIn posts)
        for query in ats_queries:
            try:
                results = _fts_search_all_engines(query)
                for r in results:
                    url = r.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    # These are direct ATS listings, add them as regular search results
                    all_results.append({
                        "title": r.get("title", ""),
                        "snippet": r.get("snippet", ""),
                        "url": url,
                    })
                    log.info(f"  Customer ATS [{company}]: {r.get('title', '')[:60]}")
            except Exception as e:
                log.warning(f"  Customer ATS query failed for {company}: {e}")
            time.sleep(random.uniform(1.0, 2.5))

    # Save state
    state["last_index"] = next_idx
    state["seen_urls"] = list(seen_urls)[-300:]
    _save_customer_fts_state(state)

    log.info(f"Develeap Customer FTS: found {len(all_results)} results")
    return all_results


# ── Greenhouse Boards API Scanner ────────────────────────────────────────
# Known company → Greenhouse board slug mapping.
# These companies have public Greenhouse boards we can query directly via API.
GREENHOUSE_BOARD_SLUGS = {
    "nice": "nice",
    "redis": "Redis",
    "cyberark": "cyberark",
    "monday.com": "mondaydotcom",
    "mobileye": "mobileye",
    "checkpoint": "checkpoint",
    "cellebrite": "cellebrite",
    "tufin": "tufin",
    "zerto": "zerto",
    "aqua": "aquasecurity",
    "transmit security": "transmitsecurity",
    "xmcyber": "xmcyber",
    "zafran": "zafransecurity",
    "armo": "armosec",
    "plus500": "plus500",
    "jfrog": "jfrog",
    "wiz": "wizinc",
    "fireblocks": "fireblocks",
    "similarweb": "similarweb",
    "torq": "torq",
    "grafana labs": "grafanalabs",
    "pagaya": "pagaya",
}

LEVER_BOARD_SLUGS = {
    "cloudinary": "cloudinary",
    "d-fend solutions": "d-fendsolutions",
}

# Israel location indicators for Greenhouse board filtering
_GH_ISRAEL_LOCATIONS = [
    "israel", "tel aviv", "raanana", "ra'anana", "herzliya", "haifa",
    "petah tikva", "netanya", "beer sheva", "be'er sheva", "jerusalem",
    "rishon lezion", "rehovot", "kfar saba", "hod hasharon", "yokneam",
]

# Relevant role keywords for Greenhouse board filtering
_GH_ROLE_KEYWORDS = [
    "devops", "platform engineer", "sre", "site reliability", "cloud engineer",
    "cloud architect", "infrastructure", "mlops", "ai engineer", "machine learning",
    "data engineer", "devsecops", "security engineer", "finops", "agentic",
    "backend engineer", "solutions architect", "sales engineer",
]


def scan_greenhouse_boards() -> list[dict]:
    """Scan known Greenhouse boards for Israel-based DevOps/Cloud/AI roles.

    Uses the Greenhouse public boards API (no auth needed) to directly
    find open positions, bypassing search engine indexing delays.
    """
    all_results = []

    for company_name, slug in GREENHOUSE_BOARD_SLUGS.items():
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        try:
            resp = requests.get(api_url, timeout=15)
            if resp.status_code != 200:
                log.warning(f"  Greenhouse board {slug}: HTTP {resp.status_code}")
                continue

            data = resp.json()
            jobs = data.get("jobs", [])

            for j in jobs:
                location = j.get("location", {}).get("name", "")
                title = j.get("title", "")
                job_url = j.get("absolute_url", "")

                # Filter: must be in Israel
                loc_lower = location.lower()
                if not any(ind in loc_lower for ind in _GH_ISRAEL_LOCATIONS):
                    continue

                # Filter: must be a relevant role
                title_lower = title.lower()
                if not any(kw in title_lower for kw in _GH_ROLE_KEYWORDS):
                    continue

                all_results.append({
                    "title": f"{title} @ {company_name.title()}",
                    "snippet": f"{company_name.title()} - {location}. Open position found via Greenhouse boards API.",
                    "url": job_url,
                })
                log.info(f"  Greenhouse board [{slug}]: {title} | {location}")

        except Exception as e:
            log.warning(f"  Greenhouse board {slug} failed: {e}")

        time.sleep(random.uniform(0.5, 1.5))

    log.info(f"Greenhouse boards scan: found {len(all_results)} Israel-based roles")
    return all_results



def scan_lever_boards() -> list:
    """Scan known Lever boards for Israel-based DevOps/Cloud/AI roles."""
    all_results = []
    for company_name, slug in LEVER_BOARD_SLUGS.items():
        api_url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            resp = requests.get(api_url, timeout=15)
            if resp.status_code != 200:
                log.warning(f"  Lever board {slug}: HTTP {resp.status_code}")
                continue
            jobs = resp.json()
            if not isinstance(jobs, list):
                continue
            for j in jobs:
                location = j.get("categories", {}).get("location", "")
                title = j.get("text", "")
                job_url = j.get("hostedUrl", "")
                loc_lower = location.lower()
                if not any(loc in loc_lower for loc in _GH_ISRAEL_LOCATIONS):
                    continue
                title_lower = title.lower()
                if not any(kw in title_lower for kw in _GH_ROLE_KEYWORDS):
                    continue
                all_results.append({
                    "title": title,
                    "company": company_name.title(),
                    "location": location,
                    "url": job_url,
                    "source": "Lever",
                })
                log.info(f"  Lever [{company_name}]: {title} ({location})")
        except Exception as e:
            log.warning(f"  Lever board {slug}: {e}")
    log.info(f"Lever boards: {len(all_results)} relevant jobs found")
    return all_results


# ── ATS Contact Extraction ────────────────────────────────────────────────

def _extract_greenhouse_contacts(url: str) -> list:
    """Extract hiring manager and recruiter from Greenhouse job API.
    Greenhouse metadata often contains 'Hiring Manager' and 'Recruiter' fields
    with name and email — these are high-quality contacts."""
    contacts = []
    if not url:
        return contacts

    # Extract board slug and job ID from URL patterns:
    # boards.greenhouse.io/{slug}/jobs/{id}
    # boards.eu.greenhouse.io/{slug}/jobs/{id}
    # job-boards.greenhouse.io/...
    m = re.search(r'(?:boards(?:\.eu)?\.greenhouse\.io|job-boards\.(?:eu\.)?greenhouse\.io)/([^/]+)/jobs/(\d+)', url)
    if not m:
        return contacts

    slug = m.group(1)
    job_id = m.group(2)

    # Try the public boards API
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"
    try:
        resp = requests.get(api_url, timeout=10)
        if resp.status_code != 200:
            return contacts

        data = resp.json()
        metadata = data.get("metadata", [])

        for meta in metadata:
            name_field = meta.get("name", "").lower()
            value = meta.get("value", {})

            if isinstance(value, dict) and value.get("name"):
                person_name = value["name"]
                person_email = value.get("email", "")

                if name_field in ("hiring manager", "recruiter", "talent acquisition",
                                  "hiring_manager", "recruiting lead"):
                    # Determine title based on metadata field name
                    if "recruiter" in name_field or "talent" in name_field:
                        title = "Recruiter"
                        source = "Greenhouse Recruiter"
                    else:
                        title = "Hiring Manager"
                        source = "Greenhouse Hiring Manager"

                    contacts.append({
                        "name": person_name,
                        "title": title,
                        "linkedin": "",
                        "source": source,
                        "email": person_email,
                    })

        if contacts:
            log.info(f"  Greenhouse contacts for {slug}/{job_id}: "
                     f"{', '.join(c['name'] + ' (' + c['title'] + ')' for c in contacts)}")

    except Exception as e:
        log.debug(f"  Greenhouse API contact extraction failed for {url}: {e}")

    return contacts


def _extract_lever_contacts(url: str) -> list:
    """Extract contacts from Lever job pages.
    Lever job pages sometimes include team/recruiter info in the HTML."""
    contacts = []
    if not url or "lever.co" not in url:
        return contacts

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return contacts

        text = resp.text[:50000]

        # Look for hiring manager / team lead mentions
        # Lever pages sometimes have "Questions? Contact <name>" or
        # embed recruiter info in JSON-LD or meta tags
        contact_patterns = [
            r'contact[^"]*"[^>]*>\s*([A-Z][a-z]+ [A-Z][a-z]+)',
            r'"hiringManager"\s*:\s*\{\s*"name"\s*:\s*"([^"]+)"',
        ]
        for pat in contact_patterns:
            matches = re.findall(pat, text)
            for name in matches:
                if len(name) > 3 and " " in name:
                    contacts.append({
                        "name": name,
                        "title": "Hiring Manager",
                        "linkedin": "",
                        "source": "Lever Job Page",
                        "email": "",
                    })
                    break
            if contacts:
                break

    except Exception as e:
        log.debug(f"  Lever contact extraction failed for {url}: {e}")

    return contacts


def _extract_ats_contacts(url: str) -> list:
    """Extract contacts from ATS (Applicant Tracking System) job pages.
    Supports Greenhouse and Lever."""
    if "greenhouse.io" in (url or ""):
        return _extract_greenhouse_contacts(url)
    elif "lever.co" in (url or ""):
        return _extract_lever_contacts(url)
    return []


# ── Date Extraction ───────────────────────────────────────────────────────

def extract_posting_date(url: str) -> str:
    """Try to scrape the real posting date from a job listing page.
    Returns ISO date string (YYYY-MM-DD) or empty string if not found."""
    if not url:
        return ""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return ""
        text = resp.text[:100000]  # Limit to first 100KB

        # 1. JSON-LD structured data (most reliable — used by LinkedIn, many career sites)
        ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
        for ld_raw in ld_matches:
            try:
                ld = json.loads(ld_raw)
                # Handle both single object and array
                items = ld if isinstance(ld, list) else [ld]
                for item in items:
                    # JobPosting schema
                    if item.get("@type") == "JobPosting":
                        date_posted = item.get("datePosted", "")
                        if date_posted:
                            return _normalize_date(date_posted)
                    # Check nested items
                    if isinstance(item.get("@graph"), list):
                        for g in item["@graph"]:
                            if g.get("@type") == "JobPosting":
                                date_posted = g.get("datePosted", "")
                                if date_posted:
                                    return _normalize_date(date_posted)
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        # 2. "datePosted" anywhere in page (inline JSON, JS variables, etc.)
        m = re.search(r'"datePosted"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
        if m:
            return m.group(1)

        # 2b. Meta tags (og:article:published_time, datePublished, etc.)
        meta_patterns = [
            r'<meta[^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\'][^>]*content=["\']([^"\']+)["\']',
            r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\']',
        ]
        for pat in meta_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return _normalize_date(m.group(1))

        # 2c. Any JSON field with "date" in key and ISO date value
        date_json = re.findall(r'"(?:date_?(?:posted|published|created|listed)?)"\s*:\s*"(\d{4}-\d{2}-\d{2}[T\s]?[^"]*)"', text, re.IGNORECASE)
        if date_json:
            return _normalize_date(date_json[0])

        # 2d. ISO dates near posting-related keywords in raw HTML/JS
        posting_date_ctx = re.findall(
            r'(?:post|publish|list|creat|updat)(?:ed|_at|At|Date|Time|_date|_time).{0,30}?(\d{4}-\d{2}-\d{2})',
            text, re.IGNORECASE
        )
        if posting_date_ctx:
            return posting_date_ctx[0]

        # 3. Relative date patterns in visible text ("Posted 3 days ago", "2 weeks ago")
        relative_patterns = [
            (r'(?:posted|published|listed)\s+(\d+)\s+day', "days"),
            (r'(?:posted|published|listed)\s+(\d+)\s+week', "weeks"),
            (r'(?:posted|published|listed)\s+(\d+)\s+month', "months"),
            (r'(?:posted|published|listed)\s+(\d+)\s+hour', "hours"),
            (r'(\d+)\s+days?\s+ago', "days"),
            (r'(\d+)\s+weeks?\s+ago', "weeks"),
            (r'(\d+)\s+months?\s+ago', "months"),
            (r'(\d+)\s+hours?\s+ago', "hours"),
        ]
        from datetime import timedelta
        for pat, unit in relative_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                n = int(m.group(1))
                now = datetime.now(timezone.utc)
                if unit == "hours":
                    dt = now - timedelta(hours=n)
                elif unit == "days":
                    dt = now - timedelta(days=n)
                elif unit == "weeks":
                    dt = now - timedelta(weeks=n)
                elif unit == "months":
                    dt = now - timedelta(days=n * 30)
                return dt.strftime("%Y-%m-%d")

    except Exception as e:
        log.debug(f"Date extraction failed for {url[:60]}: {e}")
    return ""


def _extract_linkedin_hiring_team(html_text: str) -> list:
    """Extract 'People you can reach out to' / hiring team contacts from LinkedIn job page HTML.
    Returns list of dicts: [{name, title, linkedin, photo}, ...]"""
    contacts = []
    idx = html_text.find("message-the-recruiter")
    if idx < 0:
        return contacts
    section = html_text[idx:idx+5000]
    links = re.findall(r'href="(https://[a-z]+\.linkedin\.com/in/[^"]+)"', section)
    names = re.findall(
        r'<h3[^>]*base-main-card__title[^>]*>\s*(.*?)\s*</h3>', section, re.DOTALL
    )
    names = [re.sub(r'<[^>]+>', '', n).strip() for n in names]
    titles = re.findall(
        r'<h4[^>]*base-main-card__subtitle[^>]*>\s*(.*?)\s*</h4>', section, re.DOTALL
    )
    titles = [re.sub(r'<[^>]+>', '', t).strip() for t in titles]
    photos = re.findall(r'data-delayed-url="(https://media\.licdn\.com[^"]+)"', section)
    for i in range(len(names)):
        name = names[i]
        if not name or len(name) < 2:
            continue
        contact = {
            "name": name,
            "title": titles[i] if i < len(titles) else "",
            "linkedin": links[i] if i < len(links) else "",
            "source": "LinkedIn Job Poster",
            "email": "",
        }
        if i < len(photos):
            contact["photo"] = photos[i].replace("&amp;", "&")
        contacts.append(contact)
    return contacts


def scrape_job_page(url: str) -> dict:
    """Scrape a job listing page for date, company name, closed status, location, and hiring team.
    Returns {"date": "YYYY-MM-DD" or "", "company": "name" or "", "closed": bool, "location_country": "", "is_career_page": bool, "hiring_team": [...]}."""
    result = {"date": "", "company": "", "closed": False, "location_country": "", "is_career_page": False, "_http_status": 0, "hiring_team": [], "post_author": "", "post_author_title": "", "post_author_photo": ""}
    if not url:
        return result
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            log.info(f"  Scrape {url[:60]}: status={resp.status_code} (non-200, skipping)")
            result["_http_status"] = resp.status_code
            return result
        text = resp.text[:100000]  # Limit to first 100KB
        final_url = resp.url  # URL after redirects
        result["_http_status"] = 200
        log.info(f"  Scrape {url[:60]}: status={resp.status_code}, size={len(resp.text)}, truncated={len(text)}")

        # ── Extract LinkedIn post author from page title/meta ──
        # LinkedIn page titles: "Name on LinkedIn: post content..." or "Name - Title | LinkedIn"
        if "linkedin.com/posts/" in url:
            og_title_match = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)', text, re.IGNORECASE)
            page_title_for_author = og_title_match.group(1) if og_title_match else ""
            if not page_title_for_author:
                pt_match = re.search(r'<title[^>]*>([^<]+)</title>', text, re.IGNORECASE)
                if pt_match:
                    page_title_for_author = pt_match.group(1).strip()
            if page_title_for_author:
                author_from_page = re.search(r'^(?:\(\d+\)\s*)?(.+?)\s+(?:posted\s+)?on\s+LinkedIn', page_title_for_author)
                if author_from_page:
                    raw_name = author_from_page.group(1).strip()
                    raw_name = re.sub(r'\s+(?:at|@|\|)\s+.*$', '', raw_name).strip()
                    name_parts = raw_name.split()
                    if 2 <= len(name_parts) <= 4 and all(p[0].isupper() for p in name_parts if p):
                        result["post_author"] = raw_name
                        log.info(f"  LinkedIn post author from page: {raw_name} for {url[:50]}")
            # Also try to get author's professional title from meta description
            meta_desc = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)', text, re.IGNORECASE)
            if meta_desc and result["post_author"]:
                desc_text = meta_desc.group(1)
                # Pattern: "Author Name · Title at Company · ..."
                title_from_desc = re.search(
                    re.escape(result["post_author"]) + r'\s*[·\-|]\s*(.+?)(?:\s*[·\-|]|$)',
                    desc_text, re.IGNORECASE
                )
                if title_from_desc:
                    candidate = title_from_desc.group(1).strip()
                    if 3 < len(candidate) < 60:
                        result["post_author_title"] = candidate
            # Extract author profile photo from data-delayed-url attributes
            # LinkedIn post pages embed profile photos as media.licdn.com/.../profile-displayphoto-...
            photo_urls = re.findall(r'data-delayed-url="(https://media\.licdn\.com[^"]*profile-displayphoto[^"]*)"', text)
            if not photo_urls:
                # Also try regular src/content attributes with profile photo URLs
                photo_urls = re.findall(r'(?:src|content)="(https://media\.licdn\.com[^"]*profile-displayphoto[^"]*)"', text)
            if photo_urls:
                # Prefer the larger scale (400x400 over 100x100/200x200)
                best_photo = photo_urls[0]
                for pu in photo_urls:
                    if "scale_400_400" in pu or "shrink_400_400" in pu:
                        best_photo = pu
                        break
                result["post_author_photo"] = best_photo.replace("&amp;", "&")
                log.info(f"  LinkedIn post author photo found for {url[:50]}")
            # Fallback: if no photo from post page, try the author's profile page og:image
            if not result["post_author_photo"]:
                slug_match = re.search(r'linkedin\.com/posts/([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])_', url)
                if slug_match:
                    profile_url = f"https://www.linkedin.com/in/{slug_match.group(1)}/"
                    try:
                        prof_resp = requests.get(profile_url, headers=headers, timeout=8, allow_redirects=True)
                        if prof_resp.status_code == 200:
                            prof_text = prof_resp.text[:50000]
                            og_img = re.search(r'<meta[^>]*property=["\']og:image["\'][^>]*content=["\']([^"\']+)', prof_text, re.IGNORECASE)
                            if og_img:
                                img_url = og_img.group(1).replace("&amp;", "&")
                                if "profile-displayphoto" in img_url or ("media.licdn.com" in img_url and "logo" not in img_url.lower()):
                                    result["post_author_photo"] = img_url
                                    log.info(f"  LinkedIn post author photo from profile page for {url[:50]}")
                    except Exception:
                        pass

        # ── Detect career/multi-listing pages (e.g. expired Greenhouse job IDs redirect to careers page) ──
        # 1. Check <title> for career page patterns
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', text, re.IGNORECASE)
        if title_match:
            page_title = title_match.group(1).strip().lower()
            career_title_patterns = [
                r'^current\s+openings?\s+(at|@)\s+',
                r'^open\s+positions?\s+(at|@)\s+',
                r'^(careers|career\s+opportunities)\s+(at|@)\s+',
                r'^(all|current|available)\s+(open\s+)?(positions|jobs|roles|openings)\s',
                r'^jobs?\s+(at|@)\s+',
                r'^search\s+jobs?\b',  # "Search Jobs — Google Careers"
                r'^(join\s+us|join\s+our\s+team|we\'?re\s+hiring)',
                r'^[\w\s]{2,30}\s*[-\|\u2013\u2014]\s*careers?\s*$',  # "Company — Careers" (short prefix only)
                r'\bcareer\s*(?:page|portal|site|hub)\b',
                r'\bcareers?\s*(?:at|@)\s+\w',  # "Careers at Google" anywhere
                r'^find\s+(?:your\s+)?(?:next\s+)?jobs?\s',  # "Find your next job at..."
            ]
            for pat in career_title_patterns:
                if re.search(pat, page_title):
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE (title): '{page_title[:60]}' for {url[:60]}")
                    break

        # 2. For ATS URLs (Greenhouse, Lever, etc.): detect if redirected away from specific job
        if not result["is_career_page"]:
            url_lower = url.lower()
            final_lower = final_url.lower()
            # Greenhouse: original URL had /jobs/\d+ but final URL lost it
            if 'greenhouse.io' in url_lower and re.search(r'/jobs/\d+', url_lower):
                if not re.search(r'/jobs/\d+', final_lower):
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE (redirect lost job ID): {url[:60]} → {final_url[:60]}")
            # Lever: original URL had specific path but redirected to company root
            if 'lever.co' in url_lower and url_lower.count('/') > 4:
                if final_lower.rstrip('/').count('/') <= 3:
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE (lever redirect): {url[:60]} → {final_url[:60]}")

        # 3. Check for multiple job listing links on the page (strong signal of a career page)
        if not result["is_career_page"]:
            # Count distinct job links on the page (Greenhouse pattern: /jobs/\d+)
            if 'greenhouse.io' in (final_url or url).lower():
                job_links = set(re.findall(r'/jobs/(\d+)', text))
                if len(job_links) > 5:
                    result["is_career_page"] = True
                    log.info(f"  CAREER PAGE ({len(job_links)} job links): {url[:60]}")

        if result["is_career_page"]:
            return result

        # ── Check if listing is closed ──
        # 1. Language-independent CSS class check (works in raw HTML, no JS needed)
        if "closed-job" in text:
            result["closed"] = True
            log.info(f"  CLOSED (CSS class 'closed-job'): {url[:60]}")

        # 2. English closed phrases
        closed_phrases = [
            "no longer accepting applications",
            "this job is no longer available",
            "this position has been filled",
            "this job has expired",
            "job closed",
            "listing has been removed",
            "application closed",
        ]
        text_lower_check = text.lower()
        if not result["closed"]:
            for phrase in closed_phrases:
                if phrase in text_lower_check:
                    result["closed"] = True
                    log.info(f"  CLOSED: {url[:60]} — '{phrase}'")
                    break

        # 3. Hebrew closed phrases (LinkedIn raw HTML may contain these)
        if not result["closed"]:
            hebrew_closed_phrases = [
                "\u05db\u05d1\u05e8 \u05dc\u05d0 \u05de\u05e7\u05d1\u05dc\u05d9\u05dd \u05d1\u05e7\u05e9\u05d5\u05ea",  # כבר לא מקבלים בקשות
                "\u05de\u05e9\u05e8\u05d4 \u05d6\u05d5 \u05db\u05d1\u05e8 \u05dc\u05d0 \u05d6\u05de\u05d9\u05e0\u05d4",  # משרה זו כבר לא זמינה
            ]
            for phrase in hebrew_closed_phrases:
                if phrase in text:
                    result["closed"] = True
                    log.info(f"  CLOSED (Hebrew): {url[:60]}")
                    break

        # ── Check for stale time-ago indicators (e.g. "3 months ago") ──
        # For LinkedIn: only check "posted X ago" context, not any "X ago" on the page,
        # because LinkedIn sidebars/recommendations contain unrelated relative dates.
        if not result["closed"]:
            if "linkedin.com" in url:
                stale_match = re.search(
                    r'(?:posted|listed|published|reposted)\s+(\d+)\s+(month|year)s?\s+ago',
                    text_lower_check
                )
            else:
                stale_match = re.search(
                    r'(\d+)\s+(month|year)s?\s+ago',
                    text_lower_check
                )
            if stale_match:
                num = int(stale_match.group(1) if "linkedin.com" not in url else stale_match.group(1))
                unit = stale_match.group(2) if "linkedin.com" not in url else stale_match.group(2)
                if unit == "year" or (unit == "month" and num >= 1):
                    result["closed"] = True
                    log.info(f"  CLOSED (stale): {url[:60]} — '{stale_match.group(0)}'")

        # LinkedIn: check for JSON-LD (indicates active listing)
        if "linkedin.com" in url:
            has_job_ld = bool(re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>.*?"@type"\s*:\s*"JobPosting"',
                text, re.DOTALL
            ))
            result["_has_job_ld"] = has_job_ld  # pass this info downstream
            # Note: missing JSON-LD alone doesn't mean closed — LinkedIn often
            # blocks JSON-LD from data center IPs. Only explicit closed phrases count.

        # ── Extract company name (especially from LinkedIn) ──
        # LinkedIn: "companyName" in inline JSON
        cm = re.search(r'"companyName"\s*:\s*"([^"]{2,60})"', text)
        if cm:
            result["company"] = cm.group(1).strip()
        # LinkedIn: topcard org name
        if not result["company"]:
            cm = re.search(r'class="topcard__org-name[^"]*"[^>]*>([^<]{2,60})', text)
            if cm:
                result["company"] = cm.group(1).strip()
        # JSON-LD hiringOrganization
        if not result["company"]:
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
            for ld_raw in ld_matches:
                try:
                    ld = json.loads(ld_raw)
                    items = ld if isinstance(ld, list) else [ld]
                    for item in items:
                        if item.get("@type") == "JobPosting":
                            org = item.get("hiringOrganization", {})
                            if isinstance(org, dict) and org.get("name"):
                                result["company"] = org["name"].strip()
                                break
                        if isinstance(item.get("@graph"), list):
                            for g in item["@graph"]:
                                if g.get("@type") == "JobPosting":
                                    org = g.get("hiringOrganization", {})
                                    if isinstance(org, dict) and org.get("name"):
                                        result["company"] = org["name"].strip()
                                        break
                    if result["company"]:
                        break
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue

        # ── Extract location/country from page (for non-Israel filtering) ──
        # JSON-LD jobLocation → addressCountry
        ld_matches_loc = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
        for ld_raw in ld_matches_loc:
            try:
                ld = json.loads(ld_raw)
                items = ld if isinstance(ld, list) else [ld]
                for item in items:
                    jp = None
                    if item.get("@type") == "JobPosting":
                        jp = item
                    elif isinstance(item.get("@graph"), list):
                        for g in item["@graph"]:
                            if g.get("@type") == "JobPosting":
                                jp = g
                                break
                    if jp:
                        loc = jp.get("jobLocation", {})
                        if isinstance(loc, list):
                            loc = loc[0] if loc else {}
                        if isinstance(loc, dict):
                            addr = loc.get("address", {})
                            if isinstance(addr, dict):
                                country = addr.get("addressCountry", "")
                                if isinstance(country, dict):
                                    country = country.get("name", "")
                                if country:
                                    result["location_country"] = country.strip()
                                    log.info(f"  Location country: {result['location_country']} for {url[:60]}")
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        # LinkedIn: look for country in the page text
        if not result["location_country"] and "linkedin.com" in url:
            # LinkedIn often has "Location: City, Country" or "addressCountry":"XX"
            country_match = re.search(r'"addressCountry"\s*:\s*"([^"]+)"', text)
            if country_match:
                result["location_country"] = country_match.group(1).strip()
                log.info(f"  LinkedIn addressCountry: {result['location_country']} for {url[:60]}")

        # Apple careers: look for location in page
        if not result["location_country"] and "apple.com" in url:
            # Apple career pages often have location details
            loc_match = re.search(r'"location(?:Name)?"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
            if loc_match:
                loc_text = loc_match.group(1)
                result["location_country"] = loc_text.strip()
                log.info(f"  Apple location: {result['location_country']} for {url[:60]}")

        # ── Extract posting date ──
        # 0. Comeet "time_updated" in POSITION_DATA
        if "comeet.com" in url:
            cm = re.search(r'"time_updated"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
            if cm:
                result["date"] = cm.group(1)
                log.info(f"  Comeet time_updated: {result['date']} for {url[:60]}")
            # Company from POSITION_DATA
            if not result["company"]:
                pos_data = re.search(r'POSITION_DATA\s*=\s*(\{[^;]+)', text)
                if pos_data:
                    try:
                        pd = json.loads(pos_data.group(1))
                        # Company from the URL slug
                        cslug = re.search(r'comeet\.com/jobs/([^/]+)', url)
                        if cslug:
                            result["company"] = cslug.group(1).replace('-', ' ').title()
                    except (json.JSONDecodeError, TypeError):
                        pass

        # 0a. LinkedIn "listedAt" Unix timestamp in milliseconds (most precise for LinkedIn)
        if "linkedin.com" in url:
            listed_at = re.search(r'"listedAt"\s*:\s*(\d{13})', text)
            if listed_at:
                ts_ms = int(listed_at.group(1))
                result["date"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                log.info(f"  LinkedIn listedAt: {result['date']} for {url[:60]}")

        # NOTE: LinkedIn <time> tags are NOT reliable for posting dates.
        # They often belong to recommendation cards, sidebar content, etc.
        # Only listedAt JSON timestamp (extracted above) is reliable for LinkedIn.

        # 1. JSON-LD datePosted (most reliable for non-LinkedIn)
        if not result["date"]:
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.DOTALL)
            for ld_raw in ld_matches:
                try:
                    ld = json.loads(ld_raw)
                    items = ld if isinstance(ld, list) else [ld]
                    for item in items:
                        if item.get("@type") == "JobPosting":
                            date_posted = item.get("datePosted", "")
                            if date_posted:
                                result["date"] = _normalize_date(date_posted)
                                break
                        if isinstance(item.get("@graph"), list):
                            for g in item["@graph"]:
                                if g.get("@type") == "JobPosting":
                                    date_posted = g.get("datePosted", "")
                                    if date_posted:
                                        result["date"] = _normalize_date(date_posted)
                                        break
                    if result["date"]:
                        break
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue

        # 2. "datePosted" anywhere in page (inline JSON / JS)
        if not result["date"]:
            m = re.search(r'"datePosted"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
            if m:
                result["date"] = m.group(1)

        # 2b. Meta tags
        if not result["date"]:
            meta_patterns = [
                r'<meta[^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\'][^>]*content=["\']([^"\']+)["\']',
                r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*(?:property|name)=["\'](?:article:published_time|datePublished|date)["\']',
            ]
            for pat in meta_patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    result["date"] = _normalize_date(m.group(1))
                    break

        # 2c. Any JSON "date*" field with ISO date value
        if not result["date"]:
            date_json = re.findall(r'"(?:date_?(?:posted|published|created|listed)?)"\s*:\s*"(\d{4}-\d{2}-\d{2}[T\s]?[^"]*)"', text, re.IGNORECASE)
            if date_json:
                result["date"] = _normalize_date(date_json[0])

        # 2d. ISO dates near posting keywords
        if not result["date"]:
            posting_date_ctx = re.findall(
                r'(?:post|publish|list|creat|updat)(?:ed|_at|At|Date|Time|_date|_time).{0,30}?(\d{4}-\d{2}-\d{2})',
                text, re.IGNORECASE
            )
            if posting_date_ctx:
                result["date"] = posting_date_ctx[0]

        # 3. Relative date patterns (prefer "posted/published X ago" over raw "X ago")
        if not result["date"]:
            from datetime import timedelta
            relative_patterns = [
                (r'(?:posted|published|listed)\s+(\d+)\s+day', "days"),
                (r'(?:posted|published|listed)\s+(\d+)\s+week', "weeks"),
                (r'(?:posted|published|listed)\s+(\d+)\s+month', "months"),
                (r'(?:posted|published|listed)\s+(\d+)\s+hour', "hours"),
                (r'(\d+)\s+days?\s+ago', "days"),
                (r'(\d+)\s+weeks?\s+ago', "weeks"),
                (r'(\d+)\s+months?\s+ago', "months"),
                (r'(\d+)\s+hours?\s+ago', "hours"),
            ]
            for pat, unit in relative_patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    n = int(m.group(1))
                    now = datetime.now(timezone.utc)
                    if unit == "hours":
                        dt = now - timedelta(hours=n)
                    elif unit == "days":
                        dt = now - timedelta(days=n)
                    elif unit == "weeks":
                        dt = now - timedelta(weeks=n)
                    elif unit == "months":
                        dt = now - timedelta(days=n * 30)
                    result["date"] = dt.strftime("%Y-%m-%d")
                    break

        # ── Extract LinkedIn hiring team ("People you can reach out to") ──
        if "linkedin.com" in url:
            hiring_team = _extract_linkedin_hiring_team(text)
            if hiring_team:
                result["hiring_team"] = hiring_team
                log.info(f"  LinkedIn hiring team: {', '.join(c['name'] for c in hiring_team)} for {url[:50]}")

    except Exception as e:
        log.debug(f"Page scrape failed for {url[:60]}: {e}")
    return result


def _scrape_linkedin_playwright(url: str) -> dict:
    """Fallback: scrape a LinkedIn job page using Playwright headless browser.
    Used when regular HTTP returns 429 (rate-limited by LinkedIn).
    Returns same format as scrape_job_page: {date, company, closed, ...}."""
    result = {"date": "", "company": "", "closed": False, "location_country": "",
              "is_career_page": False, "_http_status": 0, "_has_job_ld": False, "_text_len": 0}
    browser = _get_playwright_browser()
    if not browser:
        return result

    page = None
    try:
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        # Wait a bit for dynamic content to render
        page.wait_for_timeout(3000)

        # Get full visible text from the page
        text = page.inner_text("body")
        text_lower = text.lower()
        result["_http_status"] = 200
        result["_text_len"] = len(text)
        log.info(f"  Playwright scrape {url[:60]}: text_len={len(text)}")

        # ── Check if listing is closed ──
        # 1. Check raw HTML for the language-independent CSS class "closed-job"
        raw_html = page.content()
        if "closed-job" in raw_html:
            result["closed"] = True
            log.info(f"  CLOSED (playwright CSS class 'closed-job'): {url[:60]}")

        # 2. Check rendered text for English closed phrases
        if not result["closed"]:
            closed_phrases = [
                "no longer accepting applications",
                "this job is no longer available",
                "this position has been filled",
                "this job has expired",
                "job closed",
                "application closed",
                "role has been filled",
                "position has been filled",
                "we've filled this",
                "this role is closed",
                "hiring is complete",
                "this page doesn't exist",
                "this content isn't available",
            ]
            for phrase in closed_phrases:
                if phrase in text_lower:
                    result["closed"] = True
                    log.info(f"  CLOSED (playwright): {url[:60]} — '{phrase}'")
                    break

        # 3. Check rendered text for Hebrew closed phrases (LinkedIn returns Hebrew for IL IPs)
        if not result["closed"]:
            hebrew_closed_phrases = [
                "\u05db\u05d1\u05e8 \u05dc\u05d0 \u05de\u05e7\u05d1\u05dc\u05d9\u05dd \u05d1\u05e7\u05e9\u05d5\u05ea",  # כבר לא מקבלים בקשות
                "\u05de\u05e9\u05e8\u05d4 \u05d6\u05d5 \u05db\u05d1\u05e8 \u05dc\u05d0 \u05d6\u05de\u05d9\u05e0\u05d4",  # משרה זו כבר לא זמינה
                "\u05de\u05e9\u05e8\u05d4 \u05d6\u05d5 \u05d0\u05d9\u05e0\u05d4 \u05d6\u05de\u05d9\u05e0\u05d4",  # משרה זו אינה זמינה
            ]
            for phrase in hebrew_closed_phrases:
                if phrase in text:
                    result["closed"] = True
                    log.info(f"  CLOSED (playwright Hebrew): {url[:60]}")
                    break

        # ── Check for stale time-ago indicators ──
        if not result["closed"]:
            # English stale indicators
            stale_match = re.search(
                r'(?:posted|listed|published|reposted)\s+(\d+)\s+(month|year)s?\s+ago',
                text_lower
            )
            if stale_match:
                num = int(stale_match.group(1))
                unit = stale_match.group(2)
                if unit == "year" or (unit == "month" and num >= 1):
                    result["closed"] = True
                    log.info(f"  CLOSED (playwright stale): {url[:60]} — '{stale_match.group(0)}'")

        # Hebrew stale indicators: "לפני X חודשים/שנים" (X months/years ago)
        if not result["closed"]:
            hebrew_stale = re.search(
                r'\u05dc\u05e4\u05e0\u05d9\s+\u200f?(\d+)\u200f?\s+\u200f?(\u05d7\u05d5\u05d3\u05e9|\u05d7\u05d5\u05d3\u05e9\u05d9\u05dd|\u05e9\u05e0\u05d4|\u05e9\u05e0\u05d9\u05dd)',
                text
            )
            if hebrew_stale:
                num = int(hebrew_stale.group(1))
                unit_heb = hebrew_stale.group(2)
                # חודש/חודשים = month(s), שנה/שנים = year(s)
                if "\u05e9\u05e0" in unit_heb or num >= 1:  # year or 1+ months
                    result["closed"] = True
                    log.info(f"  CLOSED (playwright Hebrew stale): {url[:60]} — '{hebrew_stale.group(0)}'")

        # ── Extract date from relative time indicators ──
        if not result["date"]:
            rel_match = re.search(
                r'(?:reposted\s+)?(\d+)\s+(hour|day|week|month|year)s?\s+ago',
                text_lower
            )
            if rel_match:
                n = int(rel_match.group(1))
                unit = rel_match.group(2)
                now = datetime.now(timezone.utc)
                if unit == "hour":
                    dt = now - timedelta(hours=n)
                elif unit == "day":
                    dt = now - timedelta(days=n)
                elif unit == "week":
                    dt = now - timedelta(weeks=n)
                elif unit == "month":
                    dt = now - timedelta(days=n * 30)
                elif unit == "year":
                    dt = now - timedelta(days=n * 365)
                result["date"] = dt.strftime("%Y-%m-%d")
                log.info(f"  Date from playwright: {result['date']} ({rel_match.group()}) for {url[:40]}")

        # ── Extract company name ──
        try:
            # LinkedIn topcard company name
            company_el = page.query_selector(".topcard__org-name-link, .topcard__org-name, .job-details-jobs-unified-top-card__company-name a")
            if company_el:
                result["company"] = company_el.inner_text().strip()
        except Exception:
            pass

        # ── Extract location ──
        try:
            loc_el = page.query_selector(".topcard__flavor--bullet, .job-details-jobs-unified-top-card__bullet")
            if loc_el:
                result["location_country"] = loc_el.inner_text().strip()
        except Exception:
            pass

        context.close()
    except Exception as e:
        log.info(f"  Playwright scrape failed for {url[:60]}: {e}")
        if page:
            try:
                page.context.close()
            except Exception:
                pass
    return result


def _scrape_indeed_playwright(url: str) -> dict:
    """Scrape an Indeed job page using Playwright to extract company name.
    Indeed blocks regular HTTP requests (401) but Playwright with a real browser works.
    Returns {company, location, closed, date}."""
    result = {"company": "", "location": "", "closed": False, "date": ""}
    browser = _get_playwright_browser()
    if not browser:
        return result

    page = None
    try:
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(2000)

        text = page.inner_text("body") or ""
        log.info(f"  Indeed Playwright scrape {url[:60]}: text_len={len(text)}")

        # ── Extract company name ──
        # Indeed uses data-testid or specific CSS classes for company name
        for selector in [
            '[data-testid="inlineHeader-companyName"]',  # Modern Indeed
            '[data-company-name="true"]',
            '.jobsearch-InlineCompanyRating-companyHeader',  # Legacy
            '.css-1saizt3',  # Common Indeed company class
            '.jobsearch-CompanyInfoWithoutHeaderImage a',
            '.icl-u-xs-mr--xs',  # Company name link
        ]:
            try:
                el = page.query_selector(selector)
                if el:
                    company = el.inner_text().strip()
                    if company and len(company) <= 60:
                        result["company"] = company
                        log.info(f"  Indeed company from selector '{selector}': {company}")
                        break
            except Exception:
                continue

        # Fallback: try JSON-LD structured data
        if not result["company"]:
            try:
                raw_html = page.content()
                ld_match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', raw_html, re.DOTALL)
                if ld_match:
                    import json
                    ld_data = json.loads(ld_match.group(1))
                    if isinstance(ld_data, list):
                        ld_data = ld_data[0]
                    hiring_org = ld_data.get("hiringOrganization", {})
                    if isinstance(hiring_org, dict):
                        co_name = hiring_org.get("name", "")
                        if co_name:
                            result["company"] = co_name
                            log.info(f"  Indeed company from JSON-LD: {co_name}")
                    # Also grab date and location from JSON-LD
                    if not result["date"]:
                        date_posted = ld_data.get("datePosted", "")
                        if date_posted:
                            result["date"] = _normalize_date(date_posted)
                    if not result["location"]:
                        job_loc = ld_data.get("jobLocation", {})
                        if isinstance(job_loc, dict):
                            addr = job_loc.get("address", {})
                            if isinstance(addr, dict):
                                result["location"] = addr.get("addressLocality", "") or addr.get("addressCountry", "")
            except Exception as e:
                log.debug(f"  Indeed JSON-LD parse failed: {e}")

        # Fallback: extract company from page text patterns
        if not result["company"] and text:
            # Indeed pages often show "Company Name\nRating\nLocation\nJob type"
            # or "Company Name - Location" near the top
            lines = [l.strip() for l in text.split('\n') if l.strip()][:20]
            for line in lines:
                # Skip obvious non-company lines
                if len(line) > 50 or len(line) < 2:
                    continue
                if any(kw in line.lower() for kw in ["apply", "save", "sign in", "indeed",
                       "search", "post your resume", "job type", "salary", "location",
                       "full-time", "part-time", "contract", "remote", "hybrid",
                       "find jobs", "company reviews", "upload", "notifications"]):
                    continue
                # A short line that looks like a company name (capitalized, no common words)
                if re.match(r'^[A-Z\u0590-\u05FF]', line) and not re.search(r'\b(engineer|developer|manager|senior|junior|lead)\b', line, re.IGNORECASE):
                    result["company"] = line
                    log.info(f"  Indeed company from page text: {line}")
                    break

        # ── Check if listing is closed ──
        text_lower = text.lower()
        closed_phrases = ["this job has expired", "this job is no longer available",
                          "no longer accepting applications", "position has been filled"]
        for phrase in closed_phrases:
            if phrase in text_lower:
                result["closed"] = True
                log.info(f"  Indeed CLOSED: {url[:60]} — '{phrase}'")
                break

        context.close()
    except Exception as e:
        log.info(f"  Indeed Playwright scrape failed for {url[:60]}: {e}")
        if page:
            try:
                page.context.close()
            except Exception:
                pass
    return result


def _normalize_date(raw: str) -> str:
    """Normalize various date formats to YYYY-MM-DD."""
    raw = raw.strip()
    # Already ISO format: 2026-03-01 or 2026-03-01T...
    m = re.match(r'(\d{4}-\d{2}-\d{2})', raw)
    if m:
        date_str = m.group(1)
        parts = date_str.split("-")
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
        if 1 <= month <= 12 and 1 <= day <= 31:
            return date_str
        # Day/month swap: e.g. 2026-28-01 → 2026-01-28
        if 1 <= day <= 12 and 1 <= month <= 31:
            swapped = f"{year:04d}-{day:02d}-{month:02d}"
            log.info(f"  Date day/month swap fix: {date_str} → {swapped}")
            return swapped
        log.warning(f"  Invalid date components: {date_str}")
        return ""
    # Formats like "March 1, 2026" or "1 March 2026"
    try:
        from datetime import datetime as dt_cls
        for fmt in ("%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d %b %Y",
                    "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return dt_cls.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return ""


# ── Parsing Functions ──────────────────────────────────────────────────────

def detect_source(url: str) -> str:
    """Detect job board source from URL."""
    url_lower = url.lower()
    for domain, source in SOURCE_MAP.items():
        if domain in url_lower:
            return source
    return "other"


def detect_category(title: str, snippet: str) -> str | None:
    """Detect job category from title and snippet.

    Title keywords are checked first (stronger signal) before description.
    This prevents a job titled 'Platform Engineer' from being classified as
    'sre' just because the description mentions SRE experience.
    """
    title_lower = title.lower()
    text = f"{title} {snippet}".lower()
    # Check most specific categories first, then broader ones
    # devops is always last (broadest catch-all), other categories in natural order
    all_cats = list(CATEGORY_KEYWORDS.keys())
    priority = [c for c in all_cats if c != "devops"] + (["devops"] if "devops" in all_cats else [])
    # Pass 1: Check TITLE only (strongest signal)
    for cat in priority:
        for kw in CATEGORY_KEYWORDS.get(cat, []):
            if kw in title_lower:
                return cat
    # Pass 2: Check full text (title + description)
    for cat in priority:
        for kw in CATEGORY_KEYWORDS.get(cat, []):
            if kw in text:
                return cat
    return None  # No matching category — job is not relevant


def _categorize_job(title: str, snippet: str) -> str:
    """Like detect_category but falls back to 'other' instead of None.

    Use this when assigning categories to existing jobs that may not match
    any keyword list (e.g. older jobs, scraped titles with unusual phrasing).
    detect_category() is still used for *new* job filtering (returns None to
    skip irrelevant roles); this wrapper is for enrichment only.
    """
    return detect_category(title, snippet) or "other"


def _fetch_linkedin_photo(name: str, company: str, linkedin_url: str) -> str:
    """Find LinkedIn profile photo URL via SerpAPI Google Images.

    Returns a direct LinkedIn CDN URL (media.licdn.com) for the profile photo,
    or empty string if not found.
    """
    if not SERPAPI_KEY:
        return ""
    if not name:
        return ""
    try:
        # Search Google Images for the person's LinkedIn profile photo
        query = f'{name} {company} LinkedIn profile photo'
        resp = requests.get("https://serpapi.com/search.json", params={
            "engine": "google_images",
            "q": query,
            "api_key": SERPAPI_KEY,
            "num": 3,
        }, timeout=15)
        if resp.status_code != 200:
            return ""
        data = resp.json()
        # Look through image results for a LinkedIn CDN photo
        for r in data.get("images_results", [])[:5]:
            original = r.get("original", "")
            title = r.get("title", "").lower()
            # Must be from LinkedIn CDN and match the person
            if "media.licdn.com/dms/image" in original and "profile" in original:
                # Verify the title contains the person's name (first or last)
                name_parts = name.lower().split()
                if any(part in title for part in name_parts if len(part) > 2):
                    log.info(f"  Found photo for {name} via SerpAPI")
                    return original
        return ""
    except Exception as e:
        log.debug(f"Photo search failed for {name}: {e}")
        return ""


def _validate_linkedin_urls(jobs: list) -> list:
    """Validate stakeholder LinkedIn URLs by checking for 404s.
    Returns the jobs list with broken LinkedIn URLs cleared out."""
    checked = {}  # url → True (valid) / False (broken)
    broken_count = 0
    check_count = 0
    max_checks = 50  # Rate-limit to avoid hammering LinkedIn

    for j in jobs:
        for s in j.get("stakeholders", []):
            url = s.get("linkedin", "")
            if not url:
                continue
            if url in checked:
                if not checked[url]:
                    s["linkedin"] = ""
                continue
            if check_count >= max_checks:
                continue
            check_count += 1
            try:
                resp = requests.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html",
                    },
                    allow_redirects=True,
                    timeout=10,
                )
                # Only flag as broken if LinkedIn explicitly returns 404 or redirects to /404/
                is_broken = resp.status_code == 404 or "/404" in resp.url
                checked[url] = not is_broken
                if is_broken:
                    log.warning(f"  BROKEN LinkedIn: {s.get('name','')} → {url} (HTTP {resp.status_code}, final: {resp.url[:80]})")
                    s["linkedin"] = ""
                    broken_count += 1
            except Exception as e:
                log.debug(f"  LinkedIn check failed for {url}: {e}")
                checked[url] = True  # Assume valid on network error
            time.sleep(random.uniform(0.5, 1.5))

    log.info(f"  LinkedIn validation: checked {check_count} URLs, {broken_count} broken")
    return jobs


def _get_stakeholders(company: str) -> list:
    """Look up stakeholders for a company from the COMPANY_STAKEHOLDERS dict,
    falling back to automatic SerpAPI-based discovery when no manual entry exists.

    IMPORTANT: Returns a deep copy so callers can safely mutate the list
    without corrupting the global COMPANY_STAKEHOLDERS dictionary."""
    if not company:
        return []
    company_lower = company.lower().strip()
    # Direct match
    if company_lower in COMPANY_STAKEHOLDERS:
        return copy.deepcopy(COMPANY_STAKEHOLDERS[company_lower])
    # Partial match (e.g. "Check Point Software" matches "check point")
    for key, contacts in COMPANY_STAKEHOLDERS.items():
        if key in company_lower or company_lower in key:
            return copy.deepcopy(contacts)
    # Fuzzy match: remove spaces/hyphens and compare (e.g. "blinkops" matches "Blink Ops")
    company_squished = company_lower.replace(" ", "").replace("-", "")
    for key, contacts in COMPANY_STAKEHOLDERS.items():
        key_squished = key.replace(" ", "").replace("-", "")
        if key_squished in company_squished or company_squished in key_squished:
            return copy.deepcopy(contacts)
    # No manual entry — try auto-discovery
    return _auto_discover_stakeholders(company)


# ── Auto-stakeholder discovery cache ──────────────────────────────────────
_stakeholder_cache: dict[str, list] = {}   # company_lower → contacts list
_auto_discover_count = 0                    # Track search engine usage per run
AUTO_DISCOVER_MAX = 0                       # DISABLED to conserve SerpAPI quota — renews 2026-04-08

# Leadership title patterns for auto-discovery
_LEADERSHIP_RE = re.compile(
    r'(?:CTO|Chief Technology Officer|Chief Executive Officer|CEO|'
    r'Co-?Founder|VP\s*(?:of\s+)?(?:R&D|Engineering|Research|Technology|Product)|'
    r'Head of (?:Engineering|R&D|Technology)|'
    r'Director of (?:Engineering|R&D)|'
    r'SVP\s+(?:Engineering|R&D)|'
    r'Sr\.?\s*Director\s+(?:Engineering|R&D)|'
    r'General Manager|Country Manager|Managing Director)',
    re.IGNORECASE
)
_SKIP_TITLE_RE = re.compile(
    r'recruiter|talent\s+acq|intern\b|junior|associate|analyst|student|'
    r'looking\s+for|seeking|open\s+to',
    re.IGNORECASE
)


def _parse_linkedin_search_result(result: dict, company_lower: str,
                                   seen_urls: set) -> dict | None:
    """Parse a single SerpAPI organic result into a stakeholder contact.
    Returns a contact dict or None if the result doesn't qualify."""
    link = result.get("link", "")
    title_text = result.get("title", "")
    snippet = result.get("snippet", "")

    # Must be a LinkedIn profile URL
    if "/in/" not in link or link in seen_urls:
        return None

    # Extract name and title from title line
    # Typical formats:
    #   "Name - Title - Company | LinkedIn"
    #   "Name - Company | LinkedIn"
    #   "Name | LinkedIn"
    name = ""
    person_title = ""
    clean_title = title_text.replace(" | LinkedIn", "").replace("| LinkedIn", "").strip()

    if " - " in clean_title:
        parts = [p.strip() for p in clean_title.split(" - ")]
        name = parts[0]
        if len(parts) >= 3:
            person_title = parts[1]
        elif len(parts) == 2:
            # Could be "Name - Title" or "Name - Company"
            if _LEADERSHIP_RE.search(parts[1]):
                person_title = parts[1]
    elif clean_title:
        name = clean_title.split("|")[0].strip()

    if not name or name.lower() == "linkedin" or len(name) < 3:
        return None

    # Verify the result is actually about this company
    combined = (title_text + " " + snippet).lower()
    company_words = [w for w in company_lower.split() if len(w) > 2]
    if company_words and not any(w in combined for w in company_words):
        # Also try squished match (e.g. "intel" in "intelcorporation")
        squished = company_lower.replace(" ", "")
        if squished not in combined.replace(" ", ""):
            return None

    # Extract/refine title from snippet if we don't have one yet
    if not person_title or not _LEADERSHIP_RE.search(person_title):
        title_match = _LEADERSHIP_RE.search(snippet)
        if title_match:
            # Grab the match and a bit of context
            start = title_match.start()
            end = min(start + 60, len(snippet))
            candidate = snippet[start:end].split("·")[0].split("|")[0].split("…")[0].strip().rstrip(",. ")
            if candidate:
                person_title = candidate

    # Must have a leadership title
    if not person_title or not _LEADERSHIP_RE.search(person_title):
        return None

    # Skip non-leadership profiles
    if _SKIP_TITLE_RE.search(person_title):
        return None

    seen_urls.add(link)
    return {
        "name": name,
        "title": person_title,
        "linkedin": link,
        "source": "Auto-discovered",
        "email": "",
    }


def _search_for_stakeholders(query: str) -> list[dict]:
    """Search for stakeholder LinkedIn profiles using available search engines.
    Returns raw search results (list of {title, snippet, url}).
    Uses Google CSE first (best for site: queries), then Bing, then SerpAPI, then DuckDuckGo."""

    # Strategy 1: Google CSE (100 free/day) — best for site:linkedin queries
    results = search_google_cse(query)
    if results:
        return results

    time.sleep(random.uniform(0.5, 1.0))

    # Strategy 2: Bing (1000 free/month) — good site: operator support
    results = search_bing(query)
    if results:
        return results

    time.sleep(random.uniform(0.5, 1.0))

    # Strategy 3: SerpAPI (paid, limited but reliable)
    if SERPAPI_KEY:
        try:
            resp = requests.get("https://serpapi.com/search", params={
                "q": query, "api_key": SERPAPI_KEY, "gl": "il", "hl": "en", "num": 5,
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return [{"title": r.get("title", ""), "snippet": r.get("snippet", ""),
                     "url": r.get("link", "")}
                    for r in data.get("organic_results", [])]
        except Exception:
            pass

    time.sleep(random.uniform(0.5, 1.0))

    # Strategy 4: DuckDuckGo (free, no key) — use modified query without site:
    # DuckDuckGo's HTML search doesn't support site: well, so we adjust
    ddg_query = query.replace("site:linkedin.com/in", "linkedin").replace("site:linkedin.com", "linkedin")
    results = search_duckduckgo(ddg_query)
    if results:
        # Filter to only linkedin.com/in results
        return [r for r in results if "linkedin.com/in/" in r.get("url", "")]

    return []


# Companies that are actually job board names or junk — skip stakeholder lookup
_SKIP_COMPANIES_FOR_STAKEHOLDERS = {
    "unknown", "remoterockethub", "efinancialcareers", "jobgether",
    "techaviv", "play", "automatit", "efinancialcareers norway",
    "factored", "attil", "mksinst", "adaptive6", "crawljobs",
    "vertexventures", "my team", "tel aviv ...", "tel aviv,",
    "$84k", "secure agentic ai", "shi", "campbellsoup",
    # Staffing/outsourcing firms — not direct employers
    "devsavant", "truelogic", "red river",
}


def _auto_discover_stakeholders(company: str) -> list:
    """Find CTO / VP R&D / VP Engineering for a company using available search engines.
    Uses DuckDuckGo → Google CSE → Bing → SerpAPI fallback chain.
    Tries multiple search strategies and parses LinkedIn profiles from results."""
    global _auto_discover_count

    if not company:
        return []

    company_lower = company.lower().strip()

    # Skip companies that are actually job board names, not real employers
    if company_lower in _SKIP_COMPANIES_FOR_STAKEHOLDERS:
        _stakeholder_cache[company_lower] = []
        return []

    # Skip very short or numeric company names (likely parsing errors)
    if len(company_lower) < 3 or company_lower.replace("$", "").replace(",", "").replace(".", "").isdigit():
        _stakeholder_cache[company_lower] = []
        return []

    if company_lower in _stakeholder_cache:
        return _stakeholder_cache[company_lower]

    if _auto_discover_count >= AUTO_DISCOVER_MAX:
        _stakeholder_cache[company_lower] = []
        return []

    _auto_discover_count += 1
    contacts = []

    # Try multiple search queries — broader first, then specific
    queries = [
        f'{company} CTO OR CEO site:linkedin.com/in',
        f'{company} "VP Engineering" OR "VP R&D" OR "Head of R&D" site:linkedin.com/in',
    ]

    try:
        seen_urls = set()
        for query in queries:
            if len(contacts) >= 2:
                break

            results = _search_for_stakeholders(query)

            for r in results:
                if len(contacts) >= 2:
                    break
                # Normalize result format (DuckDuckGo/CSE/Bing use 'url', SerpAPI uses 'link')
                r_normalized = {
                    "title": r.get("title", ""),
                    "snippet": r.get("snippet", ""),
                    "link": r.get("url", "") or r.get("link", ""),
                }
                parsed = _parse_linkedin_search_result(r_normalized, company_lower, seen_urls)
                if parsed:
                    contacts.append(parsed)

            time.sleep(random.uniform(0.3, 0.8))

        if contacts:
            log.info(f"  Auto-discovered {len(contacts)} stakeholder(s) for {company}: "
                     f"{', '.join(c['name'] + ' (' + c['title'] + ')' for c in contacts)}")
        else:
            log.debug(f"  No stakeholders auto-discovered for {company}")

        time.sleep(random.uniform(0.5, 1.5))

    except Exception as e:
        log.debug(f"  Auto-discover failed for {company}: {e}")

    _stakeholder_cache[company_lower] = contacts
    return contacts


def _generate_outreach_messages(job: dict) -> None:
    """Generate personalized LinkedIn outreach messages for each stakeholder.
    Adds 'connectMsg' and 'followUpMsg' fields to each stakeholder dict."""
    company = job.get("company", "Unknown")
    job_title = job.get("title", "").split(" at ")[0].split(" - ")[0].strip()
    category = job.get("category", "devops")
    is_customer = job.get("isDeveleapCustomer", False)

    # Map categories to Develeap service descriptions
    service_map = {
        "devops": "DevOps & cloud-native transformation",
        "finops": "FinOps and cloud cost optimization",
        "ai": "AI/ML infrastructure and MLOps",
        "agentic": "Agentic AI and automation",
    }
    service = service_map.get(category, "DevOps & cloud engineering")

    for s in job.get("stakeholders", []):
        first_name = s.get("name", "").split()[0] if s.get("name") else "there"
        title = s.get("title", "")

        if is_customer:
            # Warm intro — they already know Develeap
            connect_msg = (
                f"Hi {first_name}, I'm Dori from Develeap. "
                f"I noticed {company} is growing the team with a {job_title} role — "
                f"great to see! As a current partner, I'd love to discuss how we can "
                f"support your scaling efforts. Would love to connect."
            )
            followup_msg = (
                f"Thanks for connecting, {first_name}! "
                f"Since Develeap already works with {company}, I wanted to reach out "
                f"about the {job_title} hiring. We often help teams ramp up faster "
                f"with interim {service} expertise while permanent hires onboard. "
                f"Would a quick chat be useful?"
            )
        else:
            # Cold outreach
            connect_msg = (
                f"Hi {first_name}, I noticed {company} is hiring a {job_title} — "
                f"sounds like exciting growth! I lead Develeap, an Israeli {service} "
                f"consultancy. Would love to connect and share how we help teams like "
                f"yours move faster."
            )
            followup_msg = (
                f"Thanks for connecting, {first_name}! "
                f"I wanted to share how Develeap helps companies like {company} "
                f"accelerate their {service} initiatives. We've worked with 50+ "
                f"Israeli tech companies on similar challenges. "
                f"Would you be open to a 15-min intro call this week?"
            )

        # LinkedIn connection notes have a 300-char limit
        if len(connect_msg) > 295:
            connect_msg = connect_msg[:292] + "..."

        s["connectMsg"] = connect_msg
        s["followUpMsg"] = followup_msg


def _company_matches(company: str, customer_list: list) -> bool:
    """Check if company name matches any entry in customer list (word-boundary aware)."""
    company_lower = company.lower().strip()
    for c in customer_list:
        c_lower = c.lower()
        if c_lower == company_lower:
            return True
        # Word-boundary match: "Aqua" matches "Aqua Security" but not "AquaFence"
        pattern = r'(?:^|[\s\-_])' + re.escape(c_lower) + r'(?:$|[\s\-_,.])'
        if re.search(pattern, company_lower):
            return True
    return False


def is_develeap_customer(company: str) -> bool:
    """Check if company is a current Develeap customer."""
    return _company_matches(company, DEVELEAP_CUSTOMERS)


def is_develeap_past_customer(company: str) -> bool:
    """Check if company is a past Develeap customer."""
    if is_develeap_customer(company):
        return False  # Active takes precedence
    return _company_matches(company, DEVELEAP_PAST_CUSTOMERS)


def _is_job_title(text: str) -> bool:
    """Return True if text looks like a job title rather than a company name."""
    t = text.lower().strip().rstrip(".")
    # If it matches a known company name, it's definitely NOT a job title
    if t in COMPANY_DOMAINS or t in COMPANY_ALIASES:
        return False
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
        "automation", "release", "build", "deployment", "network",
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
    # Ends with a job-role word — strong signal it's a title, not a company
    role_endings = {"engineer", "developer", "architect", "analyst", "consultant",
                    "specialist", "manager", "director", "coordinator", "administrator",
                    "technician", "intern", "designer", "scientist", "researcher",
                    "lead", "officer", "evangelist"}
    last_word = t.split()[-1] if t.split() else ""
    if last_word in role_endings:
        return True
    return False


def _is_location_fragment(text: str) -> bool:
    """Return True if text looks like a location rather than a company name.

    Catches patterns like 'Tel Aviv-Yafo ...', 'New York, NY', 'Israel', etc.
    """
    t = text.lower().strip().rstrip(". ")
    _LOCATION_STARTS = (
        "tel aviv", "jerusalem", "haifa", "ramat gan", "herzliya", "beer sheva",
        "netanya", "petah tikva", "hod hasharon", "ra'anana", "rishon lezion",
        "new york", "san francisco", "london", "berlin", "tokyo", "paris",
        "mumbai", "bangalore", "singapore", "boston", "chicago", "seattle",
        "austin", "remote", "hybrid", "worldwide",
    )
    for loc in _LOCATION_STARTS:
        if t.startswith(loc):
            return True
    # Ends with country/region
    if re.search(r'(?:israel|usa|uk|india|germany|france|japan)\s*\.{0,3}\s*$', t, re.IGNORECASE):
        return True
    # Ends with "district" (e.g. "Tel Aviv District ...")
    if re.search(r'\bdistrict\b', t, re.IGNORECASE):
        return True
    return False


def _extract_company_inner(title: str, snippet: str, url: str = "") -> str:
    """Try to extract company name from search result."""

    # Indeed viewjob URLs: lookup by job key (jk parameter)
    # Indeed blocks scraping from DC IPs, so we use a timestamped cache.
    if "indeed.com/viewjob" in url:
        jk_match = re.search(r'[?&]jk=([a-f0-9]+)', url)
        if jk_match and jk_match.group(1) in _INDEED_JK_CACHE:
            return _INDEED_JK_CACHE[jk_match.group(1)]["company"]

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

    # Known ATS slug → clean company name mapping
    ATS_SLUG_MAP = {
        "unity3d": "Unity",
        "pagayais": "Pagaya",
        "nextinsurance66": "Next Insurance",
        "catonetworks": "Cato Networks",
        "jobgether": "Jobgether",
        "castailabs": "Castai",
        "castaigroupinc": "CAST AI",
        "oligosecurity": "Oligo Security",
        "chaoslabs": "Chaos Labs",
        "blinkops": "Blink Ops",
        "quantummachines": "Quantum Machines",
        "majesticlabs": "Majestic Labs",
        "joinattil": "Attil",
        "phasev": "PhaseV",
        "quanthealth": "Quant Health",
        "aquasec": "Aqua Security",
        # Workday slugs
        "leidos": "Leidos",
        "mastercard": "Mastercard",
        "amat": "Applied Materials",
        "salesforce": "Salesforce",
    }

    # Decode URL-encoded characters (e.g. Hebrew %D7%90 → א) so all
    # patterns work with readable text instead of percent-encoded bytes.
    url = unquote(url)
    title = unquote(title)
    snippet = unquote(snippet)

    # Strip aggregator site suffixes from title before company extraction
    # e.g. "DevOps Engineer - Tel Aviv - Indeed.com" → "DevOps Engineer - Tel Aviv"
    title = re.sub(r'\s*[-–|]\s*Indeed(?:\.com)?\s*$', '', title, flags=re.IGNORECASE).strip()

    # 0. ATS URL patterns — HIGHEST PRIORITY (most reliable source of company name)
    # Greenhouse / Lever / Ashby / Comeet / Workday URLs embed the company slug
    for ats_pat in [
        r"greenhouse\.io/([a-z0-9\-]+)/jobs",
        r"boards\.greenhouse\.io/([a-z0-9\-]+)",
        r"job-boards\.greenhouse\.io/([a-z0-9\-]+)",
        r"job-boards\.eu\.greenhouse\.io/([a-z0-9\-]+)",
        r"lever\.co/([a-z0-9\-]+)",
        r"jobs\.ashbyhq\.com/([a-z0-9\-]+)",
        r"jobs\.lever\.co/([a-z0-9\-]+)",
        r"comeet\.com/jobs/([a-z0-9\-]+)",
        r"([a-z0-9\-]+)\.wd\d+\.myworkdayjobs\.com",
        r"jobs\.jobvite\.com/([a-z0-9\-]+)",
    ]:
        m = re.search(ats_pat, url, re.IGNORECASE)
        if m:
            slug = m.group(1).lower()
            if slug in ATS_SLUG_MAP:
                return ATS_SLUG_MAP[slug]
            # Strip common ATS slug suffixes like "-internal", "-careers", "-jobs"
            slug = re.sub(r'-(internal|careers|jobs|external|global|corp)$', '', slug)
            clean = slug.replace("-", " ").title()
            if len(clean) > 1:
                return _fix_casing(clean)

    # 0b. Hebrew LinkedIn title pattern: "COMPANY גיוס עובדים ROLE"
    heb_match = re.match(r'^([A-Za-z0-9\u0590-\u05FF\.\-\s&]+?)\s+גיוס\s+עובדים', title)
    if heb_match:
        company = heb_match.group(1).strip()
        if company and not _is_job_title(company):
            return _fix_casing(company)

    # 1. LinkedIn URL pattern: .../TITLE-at-COMPANY-1234567
    if "linkedin.com" in url:
        m = re.search(r"/jobs/view/.*?-at-(.+?)-\d{5,}", url)
        if m:
            company = _fix_casing(m.group(1).replace("-", " ").title())
            if not _is_job_title(company):
                return company

    # 1c. Known career site URL patterns: careers.COMPANY.com, jobs.COMPANY.com
    m = re.search(r"https?://(?:careers|jobs)\.([a-z0-9\-]+)\.", url)
    if m:
        domain_company = _fix_casing(m.group(1).replace("-", " ").title())
        if len(domain_company) > 2 and domain_company.lower() not in {
            "secret", "lhh", "secrettelaviv", "efinancial",
            "jobvite", "lever", "ashbyhq", "greenhouse",
        }:
            return _fix_casing(domain_company)

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
            "jobify360", "goozali", "lhh", "jobvite", "ashbyhq", "isecjobs",
        }
        if len(domain_company) > 2 and domain_company.lower() not in job_boards:
            # Verify the URL looks like a career/job page, not a random page
            if re.search(r"/(careers|jobs|position|openings|join|hiring|vacancy)", url, re.IGNORECASE):
                return _fix_casing(domain_company)

    # 2. "Role at Company" or "Role @ Company" pattern — use the LAST match
    at_pattern = r"(?:\bat|@)\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,]|\s+in\s+|\s+is\s+|\s*$)"
    all_at_matches = list(re.finditer(at_pattern, title))
    m = all_at_matches[-1] if all_at_matches else None
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
        if len(parts) >= 2:
            candidate = parts[-1].strip()
            # Reject candidates that look like locations (e.g. "Tel Aviv-Yafo ...")
            if not _is_job_title(candidate) and not _is_location_fragment(candidate):
                return candidate

    # 4. "Company is hiring" pattern
    m = re.search(r"([A-Z][A-Za-z0-9\.\-&]{1,25})\s+(?:is hiring|careers|jobs)", title + " " + snippet)
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    # 5. Try snippet with "at/@ Company" pattern
    m = re.search(r"(?:\bat|@)\s+([A-Z][A-Za-z0-9\.\-\s&]{1,35}?)(?:\s*[-–|,\.]|\s+in\s+|\s+is\s+|\s*$)", snippet)
    if m:
        company = m.group(1).strip()
        if not _is_job_title(company):
            return company

    # 6. Indeed snippet often starts with "Company Name\n..." or "Company Name. Location."
    # SerpApi rich_snippet text may contain company info at the start
    if "indeed.com" in url:
        # Try first line of snippet before any dash/newline/period
        first_chunk = re.split(r'[\n\r.;]', snippet)[0].strip() if snippet else ""
        if first_chunk:
            # Skip if it's a job title, location, or too long
            if (len(first_chunk) <= 40 and not _is_job_title(first_chunk)
                    and not _is_location_fragment(first_chunk)
                    and not re.match(r'^\d', first_chunk)):
                return _fix_casing(first_chunk)

    return "Unknown"


def extract_company(title: str, snippet: str, url: str = "") -> str:
    """Extract company name, back-filling the Indeed JK cache on a cache miss."""
    result = _extract_company_inner(title, snippet, url)
    if result and result != "Unknown" and "indeed.com/viewjob" in url:
        jk_match = re.search(r'[?&]jk=([a-f0-9]+)', url)
        if jk_match and jk_match.group(1) not in _INDEED_JK_CACHE:
            _cache_indeed_company(jk_match.group(1), result)
    return result


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
        url_lower = url.lower()
        _is_indeed = "indeed.com" in url_lower
        skip_keywords = ["how to", "salary", "resume", "interview tips", "career advice",
                         "blog", "article", "guide", "tutorial", "top 10", "best companies",
                         "average salary", "job description template", "what is a",
                         "conference", "meetup", "event", "webinar", "course",
                         "apply now", "remote jobs in",
                         "archives", "משרות דרושים", "as a service for startups"]
        # "jobs in israel" skips aggregators but NOT Indeed individual listings
        if not _is_indeed:
            skip_keywords.append("jobs in israel")
        if any(kw in title_lower for kw in skip_keywords):
            if _is_indeed:
                log.info(f"  [Indeed debug] title-keyword skip: {title[:80]} | url={url[:100]}")
            continue

        # Skip Hebrew aggregator pages ("we found N job offers", "jobs wanted")
        hebrew_skip = ["מצאנו", "הצעות עבודה", "משרות אחרונות", "חיפוש משרות"]
        if any(kw in title for kw in hebrew_skip):
            if _is_indeed:
                log.info(f"  [Indeed debug] hebrew-skip: {title[:80]} | url={url[:100]}")
            continue

        # Skip aggregator titles like "DevOps Engineer Jobs..." or "5 AI Engineer jobs..."
        # But NOT Indeed individual listing titles (which often contain "job in" text)
        if not _is_indeed and re.search(r'(?:^\d+\s+)?(?:.*?\bjobs?\b.*?\bin\b|.*?\bjobs?\b\s*\(\d+\))', title_lower):
            continue

        # Skip search/aggregator pages — only allow individual job listing URLs
        skip_url_patterns = [
            # Search result pages
            "google.com/search",
            "linkedin.com/jobs/search",
            # LinkedIn job search pages (e.g. /jobs/devops-engineer-jobs)
            # Only /jobs/view/ are individual listings
            "glassdoor.com/Job/",
            # Generic job listing indexes (not for Indeed — handled separately below)
            "/search?",
        ]
        # Add non-Indeed-specific patterns
        if not _is_indeed:
            skip_url_patterns.extend(["/jobs?q="])
        if any(p in url for p in skip_url_patterns):
            if _is_indeed:
                log.info(f"  [Indeed debug] url-pattern skip: {url[:120]}")
            continue

        # LinkedIn: only accept /jobs/view/ (individual listings) or /posts/ (FTS)
        if "linkedin.com/jobs" in url_lower and "/jobs/view/" not in url_lower:
            continue
        # Indeed: only skip search/aggregator pages, allow all individual job pages
        if _is_indeed:
            if re.search(r'indeed\.com/(?:q-|cmp/.*/jobs|.*-משרות)', url_lower):
                log.info(f"  [Indeed debug] indeed-specific skip: {url[:120]}")
                continue
        # LinkedIn posts: only accept if they came from FTS (have _source_override)
        if "linkedin.com/posts/" in url_lower and not r.get("_source_override"):
            continue

        # Skip generic job board index/search pages
        if re.search(r"(alljobs\.co\.il/SearchResults|drushim\.co\.il/.*\?)", url):
            continue

        # Skip URL shorteners (e.g. goo.gle) — these are never individual job listings
        if re.search(r'^https?://goo\.gle/', url):
            continue

        # Skip career page titles before Playwright check
        if re.search(r'^search\s+jobs?\b|^find\s+(your\s+)?(next\s+)?jobs?\s', title_lower):
            continue
        # "Company — Careers" but NOT "Job Title - Company Careers" (which is a valid listing)
        if re.match(r'^[\w\s]{2,30}\s*[-\|\u2013\u2014]\s*careers?\s*$', title_lower):
            continue

        # Skip SPA career sites where location can't be verified server-side
        spa_domains = ["jobs.apple.com", "careers.google.com", "careers.microsoft.com"]
        if any(d in url_lower for d in spa_domains):
            continue

        # Skip pages that are clearly job indexes, not individual listings
        # (Indeed is excluded — its URL patterns are handled above)
        index_url_patterns = [
            r"/jobs/?$", r"/careers/?$", r"/openings/?$",
            r"/location/", r"/locations/", r"/category/",
            r"/job-location-category/", r"/jobs/mena/",
            r"/list/", r"startup\.jobs/",
            r"secrettelaviv\.com", r"efinancialcareers\.com",
            r"aidevtlv\.com", r"machinelearning\.co\.il",
            r"remoterocketship\.com", r"devjobs\.co\.il",
            r"simplyhired\.com", r"jooble\.", r"talent\.com",
            r"jobrapido\.", r"careerjet\.",
            r"gotfriends\.co\.il", r"whist\.co\.il", r"medulla\.co\.il",
            r"jobify360\.co\.il", r"isecjobs\.com",
        ]
        if not _is_indeed:
            index_url_patterns.append(r"/jobs/?\?")
        if any(re.search(p, url_lower) for p in index_url_patterns):
            if _is_indeed:
                log.info(f"  [Indeed debug] index-pattern skip: {url[:120]}")
            continue

        # Clean Hebrew localization artifacts from LinkedIn titles
        # "Navina גיוס עובדים Machine Learning Engineer" → "Navina Machine Learning Engineer"
        # "גיוס עובדים" = "recruiting employees" in Hebrew, appears in il.linkedin.com results
        title = re.sub(r'\s*גיוס\s*עובדים\s*', ' ', title).strip()
        title = re.sub(r'\s{2,}', ' ', title)  # collapse double spaces

        # Clean Indeed titles: strip location suffixes and "Indeed" brand
        # "DevOps Engineer - Israel - תל אביב -יפו, מחוז ..." → "DevOps Engineer"
        if _is_indeed:
            title = re.sub(r'\s*[-–]\s*Indeed(?:\.com)?\s*$', '', title, flags=re.IGNORECASE).strip()
            # Strip " - Israel - Hebrew location" or " - Hebrew location, מחוז ..."
            title = re.sub(r'\s*[-–]\s*Israel\s*[-–].*$', '', title).strip()
            title = re.sub(r'\s*[-–]\s*[\u0590-\u05FF].*$', '', title).strip()
            # Strip trailing " - location" if it looks like a city/region
            title = re.sub(r'\s*[-–]\s*(?:Tel Aviv|Herzliya|Haifa|Jerusalem|Netanya|Ramat Gan|Remote).*$', '', title, flags=re.IGNORECASE).strip()

        # Use _source_override from LinkedIn FTS results, otherwise detect from URL
        source = r.get("_source_override") or detect_source(url)
        category = detect_category(title, snippet)

        # Skip jobs that don't match any relevant technical category
        if category is None:
            if _is_indeed:
                log.info(f"  [Indeed debug] no-category skip: {title[:80]} | url={url[:100]}")
            else:
                log.debug(f"  Skipping irrelevant job (no category match): {title[:60]}")
            continue
        # For LinkedIn FTS results, prefer the pre-extracted company name
        if r.get("_source_override") == "linkedin_fts" and r.get("company"):
            company = r["company"]
            # Fix poster name as company (e.g. "Lerner's Post" → extract real company)
            if re.search(r"'s\s+Post$", company, re.IGNORECASE):
                real = extract_company(title, snippet, url)
                if real and real != "Unknown":
                    company = real
        else:
            company = extract_company(title, snippet, url)
        location = extract_location(title, snippet)

        if _is_indeed:
            log.info(f"  [Indeed debug] PASSED all filters: {title[:80]} | cat={category} | co={company} | loc={location}")

        # Generate stable ID from URL
        job_id = hashlib.md5(url.encode()).hexdigest()[:8]

        # Build stakeholders list — start with company stakeholders
        stakeholders = _get_stakeholders(company)

        # For LinkedIn FTS results, add the post author as a "Post Publisher" contact
        # The author is the person who posted the hiring announcement — always valuable
        if r.get("_source_override") == "linkedin_fts" and (r.get("_fts_author") or r.get("_fts_author_linkedin")):
            author_name = r.get("_fts_author", "")
            author_title = r.get("_fts_author_title", "") or "Post Publisher"
            author_li = r.get("_fts_author_linkedin", "")

            publisher_contact = {
                "name": author_name,
                "title": author_title,
                "linkedin": author_li,
                "source": "Post Publisher",
                "email": "",
                "photo": "",
            }
            # Add publisher at the beginning so they appear first
            stakeholders = [publisher_contact] + stakeholders

        # For FTS results with an external job listing URL, store it
        fts_job_url = r.get("_fts_job_url", "")

        jobs.append({
            "id": job_id,
            "title": title,
            "subtitle": snippet if snippet else "",
            "company": company,
            "companyIndustry": "",
            "location": location,
            "locationSlug": location.lower().replace(" ", "-"),
            "source": source,
            "sourceUrl": url,
            "category": category,
            "posted": "",  # Will be filled by date extraction
            "isNew": True,
            "isDeveleapCustomer": is_develeap_customer(company),
            "isPastCustomer": is_develeap_past_customer(company),
            "_snippet": snippet,  # Keep full snippet for closed/date detection
            "description": snippet if snippet else title,
            "skills": [],
            "stakeholders": stakeholders,
            "logo": _get_company_logo(company, url, title),
            "ftsJobUrl": fts_job_url if fts_job_url else "",
        })

    # Fetch real posting dates, company names, and closed status
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    active_jobs = []
    for j in jobs:
        url = j.get("sourceUrl", "")
        snippet_full = j.get("_snippet", "")  # Google search snippet
        snippet_lower = snippet_full.lower()

        # ── 1. Check Google snippet for closed signals (most reliable!) ──
        snippet_closed_phrases = [
            "no longer accepting applications",
            "this job is no longer available",
            "position has been filled",
            "job has expired",
        ]
        if any(p in snippet_lower for p in snippet_closed_phrases):
            log.info(f"  Skipping closed (snippet): {j['title'][:50]}")
            continue

        # ── 2. Extract date from Google snippet (relative dates) ──
        snippet_date = ""
        from datetime import timedelta
        # Patterns like "3 days ago", "1 year ago", "2 weeks ago", "Reposted 1 month ago" in snippet
        rel_match = re.search(r'(?:reposted\s+)?(\d+)\s+(hour|day|week|month|year)s?\s+ago', snippet_lower)
        if rel_match:
            n = int(rel_match.group(1))
            unit = rel_match.group(2)
            now = datetime.now(timezone.utc)
            if unit == "hour":
                dt = now - timedelta(hours=n)
            elif unit == "day":
                dt = now - timedelta(days=n)
            elif unit == "week":
                dt = now - timedelta(weeks=n)
            elif unit == "month":
                dt = now - timedelta(days=n * 30)
            elif unit == "year":
                dt = now - timedelta(days=n * 365)
            snippet_date = dt.strftime("%Y-%m-%d")
            log.info(f"  Date from snippet: {snippet_date} ({rel_match.group()}) for {j['title'][:40]}")
        # Hebrew relative dates in snippet: "לפני X ימים"
        if not snippet_date:
            heb_match = re.search(r'לפני\s+(?:‏)?(\d+)\s*(?:‏)?\s*(ימים|שבועות|חודשים|שנים|שעות)', snippet_full)
            if heb_match:
                n = int(heb_match.group(1))
                unit_heb = heb_match.group(2)
                now = datetime.now(timezone.utc)
                unit_map = {"שעות": "hours", "ימים": "days", "שבועות": "weeks", "חודשים": "months", "שנים": "years"}
                unit = unit_map.get(unit_heb, "days")
                if unit == "hours":
                    dt = now - timedelta(hours=n)
                elif unit == "days":
                    dt = now - timedelta(days=n)
                elif unit == "weeks":
                    dt = now - timedelta(weeks=n)
                elif unit == "months":
                    dt = now - timedelta(days=n * 30)
                elif unit == "years":
                    dt = now - timedelta(days=n * 365)
                snippet_date = dt.strftime("%Y-%m-%d")
                log.info(f"  Date from Hebrew snippet: {snippet_date} for {j['title'][:40]}")

        # ── 2b. For LinkedIn posts, extract real date from activity ID ──
        # This is the most reliable date source for LinkedIn posts.
        activity_date = ""
        url_lower = url.lower()
        if "linkedin.com/posts/" in url_lower or "linkedin.com/feed/" in url_lower:
            activity_date = _extract_linkedin_activity_date(url) or ""
            if activity_date:
                log.info(f"  Date from activity ID: {activity_date} for {j['title'][:40]}")

        # ── 3. Skip listings older than threshold ──
        # FTS posts: 7 days (we want only fresh hiring announcements)
        # Regular listings: 14 days
        # Indeed/job boards: 30 days (listings stay up longer on aggregators)
        # Use best available date: snippet_date, activity_date, or none
        best_date = snippet_date or activity_date
        if best_date:
            from datetime import datetime as dt_cls
            try:
                post_dt = dt_cls.strptime(best_date, "%Y-%m-%d")
                age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt).days
                _src = j.get("source", "")
                max_age = 30 if "indeed.com" in url else 14
                if age_days > max_age:
                    log.info(f"  Skipping old listing ({age_days} days, max={max_age}, date={best_date}): {j['title'][:50]} | {url[:80]}")
                    continue
            except ValueError:
                pass

        # ── 4. Scrape page for additional data ──
        is_fts = j.get("source") == "linkedin_fts"
        if is_fts:
            j["posted"] = snippet_date or activity_date or today
            j.pop("_snippet", None)
            # Skip Develeap's own listings
            if j["company"].lower() in ("develeap", "develeap ltd", "develeap ltd."):
                log.info(f"  Skipping Develeap's own listing: {j['title'][:50]}")
                continue
            # Skip FTS posts from hidden companies
            _hidden_fts = _load_hidden_companies()
            if j["company"].lower() in _hidden_fts:
                log.info(f"  Skipping hidden company FTS listing: {j['title'][:50]}")
                continue
            # Skip FTS with garbage company names (too short, just a year, generic)
            fts_company = j.get("company", "").strip()
            if len(fts_company) < 3 or re.match(r'^(in\s+)?\d{4}$', fts_company, re.IGNORECASE):
                log.info(f"  Skipping FTS with invalid company name '{fts_company}': {j['title'][:50]}")
                continue
            # Fix FTS company names that are actually poster names (e.g. "Lerner's Post")
            if re.search(r"'s\s+Post$", fts_company, re.IGNORECASE):
                # Try to extract real company from title/description/URL
                real_company = extract_company(j.get("title", ""), j.get("snippet", ""), url)
                if real_company and real_company != "Unknown":
                    log.info(f"  FTS poster name '{fts_company}' → real company '{real_company}'")
                    j["company"] = real_company
                    fts_company = real_company
            # ── Playwright validation for FTS LinkedIn posts ──
            if url and "linkedin.com" in url:
                pw_data = _scrape_linkedin_playwright(url)
                if pw_data and pw_data.get("_http_status") == 200:
                    # Check if the post indicates the role is closed/filled
                    if pw_data.get("closed"):
                        log.info(f"  Skipping closed FTS listing (Playwright): {j['title'][:50]}")
                        continue
                    # Use any date extracted from the post page
                    if pw_data.get("date") and not snippet_date:
                        j["posted"] = pw_data["date"]
                    # Enrich company if missing
                    if j.get("company", "").strip() in ("Unknown", "") and pw_data.get("company"):
                        j["company"] = pw_data["company"]
                elif pw_data and pw_data.get("_http_status") in (404, 410):
                    log.info(f"  Skipping removed FTS post (HTTP {pw_data['_http_status']}): {j['title'][:50]}")
                    continue
            # Also validate the external job URL if one was found
            fts_job_url = j.get("_fts_job_url", "")
            if fts_job_url:
                ext_data = scrape_job_page(fts_job_url)
                if ext_data.get("closed"):
                    log.info(f"  Skipping FTS listing — linked job closed: {j['title'][:50]}")
                    continue
            active_jobs.append(j)
            continue

        if url:
            # Indeed blocks both HTTP (401) and Playwright (bot-detection on DC IPs)
            # Use search result data only — company stays Unknown if not in title/snippet
            if "indeed.com" in url:
                page_data = {"date": "", "company": "", "closed": False, "location_country": "", "is_career_page": False, "_http_status": 0, "hiring_team": []}
            else:
                page_data = scrape_job_page(url)

            # Playwright for LinkedIn: always for /posts/ (FTS-style), 429-fallback for /jobs/
            # LinkedIn job pages return auth wall (413 chars) from GH Actions, so Playwright
            # only helps for /posts/ URLs which return real content.
            if "linkedin.com" in url:
                is_post_url = "/posts/" in url
                if is_post_url or page_data.get("_http_status") == 429:
                    log.info(f"  LinkedIn Playwright {'validation' if is_post_url else 'fallback'} (new): {j['title'][:50]}")
                    pw_data = _scrape_linkedin_playwright(url)
                    if pw_data and pw_data.get("_http_status") == 200:
                        if pw_data.get("closed"):
                            page_data["closed"] = True
                        pw_text_len = pw_data.get("_text_len", 0)
                        if pw_text_len > 500:
                            if pw_data.get("date") and not page_data.get("date"):
                                page_data["date"] = pw_data["date"]
                                log.info(f"  Date from Playwright: {pw_data['date']}")
                            if pw_data.get("company") and not page_data.get("company"):
                                page_data["company"] = pw_data["company"]
                    elif pw_data and pw_data.get("_http_status") in (404, 410):
                        page_data["closed"] = True
                        log.info(f"  Deleted listing (Playwright HTTP {pw_data['_http_status']}): {j['title'][:50]}")

            # Skip career/multi-listing pages (e.g. expired Greenhouse job IDs)
            if page_data.get("is_career_page"):
                log.info(f"  Skipping career page (not a specific job): {j['title'][:50]}")
                continue

            # Skip closed listings detected from page HTML
            if page_data.get("closed"):
                log.info(f"  Skipping closed (page): {j['title'][:50]}")
                continue

            # Use page date if we don't have snippet date
            if page_data.get("date") and not snippet_date:
                snippet_date = page_data["date"]
                log.info(f"  Date from page: {snippet_date} for {j['title'][:40]}")
                # Check if page date is older than 45 days (listings typically stay open 30-60 days)
                try:
                    from datetime import datetime as dt_cls_pg
                    post_dt_pg = dt_cls_pg.strptime(snippet_date, "%Y-%m-%d")
                    age_days_pg = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt_pg).days
                    if age_days_pg > 45:
                        log.info(f"  Skipping old listing from page date ({age_days_pg} days): {j['title'][:50]}")
                        continue
                except ValueError:
                    pass

            # Fix company if still Unknown
            if j["company"] == "Unknown" and page_data.get("company"):
                j["company"] = page_data["company"]
                j["isDeveleapCustomer"] = is_develeap_customer(page_data["company"])
                log.info(f"  Company from page: {page_data['company']}")

            # ── 5. Skip listings that are NOT in Israel ──
            loc_country = page_data.get("location_country", "").lower()
            if loc_country:
                # List of Israel indicators
                israel_indicators = ["israel", "il", "tel aviv", "herzliya", "haifa",
                                     "jerusalem", "ramat gan", "ra'anana", "raanana",
                                     "petah tikva", "netanya", "beer sheva", "hod hasharon",
                                     "rehovot", "rishon lezion", "kfar saba", "bnei brak",
                                     "modi'in", "yokneam", "caesarea"]
                is_israel = any(ind in loc_country for ind in israel_indicators)
                # Also check if it's a known non-Israel country
                non_israel_countries = ["india", "united states", "usa", "uk", "united kingdom",
                                        "germany", "france", "china", "japan", "canada",
                                        "australia", "brazil", "singapore", "ireland",
                                        "netherlands", "spain", "italy", "sweden", "poland",
                                        "romania", "czech", "hungary", "ukraine", "turkey",
                                        "south korea", "mexico", "argentina", "chile",
                                        "bangalore", "hyderabad", "mumbai", "delhi", "pune",
                                        "chennai", "kolkata", "noida", "gurgaon", "gurugram",
                                        "san francisco", "new york", "london", "berlin",
                                        "paris", "amsterdam", "toronto", "sydney", "tokyo",
                                        "shanghai", "dublin", "austin", "seattle", "boston",
                                        "cupertino", "mountain view", "palo alto"]
                is_non_israel = any(ind in loc_country for ind in non_israel_countries)
                if is_non_israel and not is_israel:
                    log.info(f"  Skipping non-Israel listing ({loc_country}): {j['title'][:50]}")
                    continue

            # ── 6. Skip very old listings from page date (>45 days) ──
            page_date_for_age = page_data.get("date", "")
            if page_date_for_age and not snippet_date:
                try:
                    from datetime import datetime as dt_cls2
                    post_dt2 = dt_cls2.strptime(page_date_for_age, "%Y-%m-%d")
                    age_days2 = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt2).days
                    if age_days2 > 45:
                        log.info(f"  Skipping old listing from page date ({age_days2} days, {page_date_for_age}): {j['title'][:50]}")
                        continue
                except ValueError:
                    pass

            # ── 7. LinkedIn with no date: keep if page is reachable ──
            # LinkedIn aggressively blocks page content (listedAt, JSON-LD, companyName)
            # from data center IPs. Since the listing was found via search engine results
            # (DuckDuckGo/SerpAPI), the URL is valid and the job likely exists.
            # Genuinely closed/expired listings are already caught by:
            #   - Step 1: Google snippet closed signals
            #   - Step 4: Page-level "job closed" / "no longer accepting" phrases
            # Only skip if the page returned a non-200 HTTP status (truly gone).
            if "linkedin.com" in url and not snippet_date and not page_data.get("date"):
                http_status = page_data.get("_http_status", 200)
                # 404/410 = listing truly removed
                if http_status in (404, 410):
                    log.info(f"  Skipping LinkedIn listing (HTTP {http_status}, listing removed): {j['title'][:50]}")
                    continue
                # 429 = rate limited — can't verify if job is active or closed.
                # Without any date evidence (no snippet date, no page date), this is
                # very likely a stale listing that LinkedIn indexed long ago.
                # Skip to avoid polluting dashboard with fake posted=today dates.
                if http_status == 429:
                    log.info(f"  Skipping LinkedIn listing (HTTP 429, no date evidence — likely stale): {j['title'][:50]}")
                    continue
                # 200 but no date: page loaded but LinkedIn blocked structured data.
                # Check if the page had a JobPosting JSON-LD — if not, it's suspicious.
                if http_status == 200 and not page_data.get("_has_job_ld"):
                    log.info(f"  Skipping LinkedIn listing (200 but no JSON-LD, no date — likely stale): {j['title'][:50]}")
                    continue
                log.info(f"  Keeping LinkedIn listing without date (HTTP {http_status}): {j['title'][:50]}")

            time.sleep(random.uniform(0.5, 1.5))  # Rate limit

        j["posted"] = snippet_date if snippet_date else today
        # Track when we first saw this job — used for stale-detection when
        # LinkedIn blocks the real posted date (HTTP 429) and we fall back to today.
        j["_first_seen"] = today
        j.pop("_snippet", None)  # Remove internal field before dashboard

        # ── Merge LinkedIn hiring team contacts into stakeholders ──
        if url and "linkedin.com" in url and page_data.get("hiring_team"):
            existing_li_urls = {s.get("linkedin", "").rstrip("/").lower()
                                for s in j.get("stakeholders", []) if s.get("linkedin")}
            existing_names = {s.get("name", "").lower()
                              for s in j.get("stakeholders", []) if s.get("name")}
            for ht in page_data["hiring_team"]:
                ht_li = ht.get("linkedin", "").rstrip("/").lower()
                ht_name = ht.get("name", "").lower()
                # Skip if already present (by LinkedIn URL or name)
                if (ht_li and ht_li in existing_li_urls) or (ht_name and ht_name in existing_names):
                    continue
                j.setdefault("stakeholders", []).insert(0, ht)
                log.info(f"  Added hiring team contact: {ht['name']} ({ht.get('title', '')[:40]}) for {j['title'][:40]}")

        # ── Extract contacts from ATS (Greenhouse/Lever) job pages ──
        job_url_for_ats = j.get("ftsJobUrl", "") or url
        if job_url_for_ats and ("greenhouse.io" in job_url_for_ats or "lever.co" in job_url_for_ats):
            ats_contacts = _extract_ats_contacts(job_url_for_ats)
            if ats_contacts:
                existing_names = {s.get("name", "").lower()
                                  for s in j.get("stakeholders", []) if s.get("name")}
                existing_emails = {s.get("email", "").lower()
                                   for s in j.get("stakeholders", []) if s.get("email")}
                for ac in ats_contacts:
                    ac_name = ac.get("name", "").lower()
                    ac_email = ac.get("email", "").lower()
                    if (ac_name and ac_name in existing_names) or (ac_email and ac_email in existing_emails):
                        continue
                    j.setdefault("stakeholders", []).insert(0, ac)
                    log.info(f"  Added ATS contact: {ac['name']} ({ac.get('title', '')}) for {j['title'][:40]}")

        # ── Backfill FTS post author from page scrape ──
        # If the initial FTS extraction missed the author (common when search result titles
        # don't have "Name on LinkedIn:" format), use the author extracted from the page HTML
        if j.get("source") == "linkedin_fts" and page_data.get("post_author"):
            # Check if we already have a Post Publisher contact
            has_publisher = any(s.get("source") == "Post Publisher" for s in j.get("stakeholders", []))
            if not has_publisher:
                author_li = ""
                # Extract LinkedIn profile URL from the post URL
                pm = re.search(r'linkedin\.com/posts/([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])_', url)
                if pm:
                    author_li = f"https://www.linkedin.com/in/{pm.group(1)}/"
                existing_names = {s.get("name", "").lower() for s in j.get("stakeholders", []) if s.get("name")}
                if page_data["post_author"].lower() not in existing_names:
                    publisher = {
                        "name": page_data["post_author"],
                        "title": page_data.get("post_author_title", "") or "Post Publisher",
                        "linkedin": author_li,
                        "source": "Post Publisher",
                        "email": "",
                        "photo": page_data.get("post_author_photo", ""),
                    }
                    j.setdefault("stakeholders", []).insert(0, publisher)
                    log.info(f"  Backfilled post author: {page_data['post_author']} for {j['title'][:40]}")
            else:
                # Publisher already exists — update photo if we scraped one and it's missing
                if page_data.get("post_author_photo"):
                    for s in j.get("stakeholders", []):
                        if s.get("source") == "Post Publisher" and not s.get("photo"):
                            s["photo"] = page_data["post_author_photo"]
                            log.info(f"  Updated Post Publisher photo for {j['title'][:40]}")
                            break

        # ── Ensure ALL FTS listings have at least LinkedIn profile as contact ──
        # Even if we can't get the author's name, the LinkedIn profile URL is valuable
        if j.get("source") == "linkedin_fts":
            has_publisher = any(s.get("source") == "Post Publisher" for s in j.get("stakeholders", []))
            if not has_publisher:
                pm = re.search(r'linkedin\.com/posts/([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])_', url)
                if pm:
                    slug = pm.group(1)
                    li_url = f"https://www.linkedin.com/in/{slug}/"
                    # Derive best-effort name from slug
                    clean_slug = re.sub(r'-[a-z0-9]{6,}$', '', slug)
                    slug_parts = clean_slug.split('-')
                    alpha_parts = [p for p in slug_parts if p.isalpha() and len(p) > 1]
                    slug_name = ' '.join(p.capitalize() for p in alpha_parts[:3]) if len(alpha_parts) >= 2 else slug
                    existing_li = {s.get("linkedin", "").rstrip("/").lower() for s in j.get("stakeholders", []) if s.get("linkedin")}
                    if li_url.rstrip("/").lower() not in existing_li:
                        publisher = {
                            "name": slug_name,
                            "title": "Post Publisher",
                            "linkedin": li_url,
                            "source": "Post Publisher",
                            "email": "",
                            "photo": page_data.get("post_author_photo", ""),
                        }
                        j.setdefault("stakeholders", []).insert(0, publisher)
                        log.info(f"  Added post author from URL: {slug_name} ({li_url}) for {j['title'][:40]}")

        # Skip Develeap's own listings
        if j["company"].lower() in ("develeap", "develeap ltd", "develeap ltd."):
            log.info(f"  Skipping Develeap's own listing: {j['title'][:50]}")
            continue

        active_jobs.append(j)

    fts_count = sum(1 for j in active_jobs if j.get("source") == "linkedin_fts")
    log.info(f"  Filtered: {len(jobs)} → {len(active_jobs)} (removed {len(jobs) - len(active_jobs)} closed/Develeap)")
    if fts_count:
        log.info(f"  Includes {fts_count} linkedin_fts listings")
    return active_jobs


# ── Dashboard Update ───────────────────────────────────────────────────────

def load_existing_jobs(html: str) -> list[dict]:
    """Extract existing ALL_JOBS from dashboard HTML."""
    match = re.search(r'let ALL_JOBS\s*=\s*(\[.*?\]);\s*$', html, re.DOTALL | re.MULTILINE)
    if match:
        raw = match.group(1)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            try:
                # Fix invalid backslash escapes (e.g. "DataOps \ MLOps")
                fixed = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', raw)
                # Fix unquoted keys
                fixed = re.sub(r'(?<=[{,])\s*(\w+)\s*:', r' "\1":', fixed)
                # Remove trailing commas before } or ]
                fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
                return json.loads(fixed)
            except json.JSONDecodeError:
                log.warning("Failed to parse existing ALL_JOBS, keeping as-is")
    return []


# ── Company name normalization ──────────────────────────────────────────────
# Maps variant/alternate company names to a single canonical form.
# Keys are lowercase; values are the canonical display name.
COMPANY_ALIASES = {
    "checkpoint":           "Check Point Software",
    "check point software": "Check Point Software",
    "check point":          "Check Point Software",
    "vastdata":             "VAST Data",
    "vast data":            "VAST Data",
    "wizinc":               "Wiz",
    "wiz":                  "Wiz",
    "doitintl":             "DoiT International",
    "doit international":   "DoiT International",
    "tikalk":               "Tikal",
    "tikal":                "Tikal",
    "somekhchaikin":        "KPMG Israel",
    "kpmg":                 "KPMG Israel",
    "kpmg israel":          "KPMG Israel",
    "intuit israel":        "Intuit",
    "intuit":               "Intuit",
    "transmit security":    "Transmit Security",
    "dell technologies":    "Dell Technologies",
    "ness technologies israel": "Ness Technologies",
    "ness technologies":    "Ness Technologies",
    "qualitest israel":     "Qualitest",
    "qualitest":            "Qualitest",
    "levistraussandco":     "Levi Strauss & Co.",
    "levi strauss & co.":   "Levi Strauss & Co.",
    "levi strauss & co":    "Levi Strauss & Co.",
    "levi strauss":         "Levi Strauss & Co.",
    "commit":               "CommIT",
    "comm-it":              "CommIT",
    "comm it":              "CommIT",
    "commbox":              "CommBox",
    "comblack":             "Comblack",
    "plurai":               "Plurai",
    "kyndryl":              "Kyndryl",
    "millennium":           "Millennium",
    "elbit systems israel": "Elbit Systems",
    "elbit systems":        "Elbit Systems",
    "elbit":                "Elbit Systems",
    "mantis tech":          "Mantis Tech",
    "mantis technology":    "Mantis Tech",
    "legitsecurity":        "Legit Security",
    "legit security":       "Legit Security",
    "beacon security":      "Beacon Security",
    "beaconsecurity":       "Beacon Security",
    "rapyd":                "Rapyd",
    "palo alto networks":   "Palo Alto Networks",
    "palo alto":            "Palo Alto Networks",
    "bit cloud":            "Bit Cloud",
    "efinancialcareers norway": "Efinancialcareers",
    "efinancialcareers":    "Efinancialcareers",
    "tel aviv,":            "Unknown",
    "tel aviv ...":         "Unknown",
    "tel aviv district ...": "Unknown",
    "tel aviv district":    "Unknown",
}

# Companies where the geo-suffix should be KEPT (e.g., "KPMG Israel" is the actual entity name)
_KEEP_GEO_SUFFIX = {"kpmg israel", "applied materials israel"}


def _normalize_company(name: str) -> str:
    """Return the canonical company name, or the original if no alias.

    Also strips geographic suffixes like 'Israel', 'USA', etc. from company
    names unless the full name is in _KEEP_GEO_SUFFIX or COMPANY_ALIASES maps
    to a name that includes the suffix.
    """
    import re as _re
    key = name.lower().strip()
    # Direct alias lookup first
    if key in COMPANY_ALIASES:
        return COMPANY_ALIASES[key]
    # Strip trailing job IDs (e.g. "Qualitest Israel 20257" → "Qualitest Israel")
    cleaned = _re.sub(r'\s+\d{3,}\s*$', '', name.strip()).strip()
    # Strip geo suffix if not in the keep list
    if cleaned.lower() not in _KEEP_GEO_SUFFIX:
        stripped = _re.sub(
            r'\s+(?:israel|usa|uk|india|germany|france|japan|china|europe|americas?|asia|emea|apac|global|worldwide)\s*$',
            '', cleaned, flags=_re.IGNORECASE
        ).strip()
        if stripped and stripped != cleaned:
            cleaned = stripped
    # Check if the cleaned version has an alias
    cleaned_key = cleaned.lower().strip()
    if cleaned_key in COMPANY_ALIASES:
        return COMPANY_ALIASES[cleaned_key]
    if cleaned != name.strip():
        return cleaned
    return name


def _normalize_title(title: str) -> str:
    """Normalize job title for dedup matching.

    Strips source-name suffixes like '- Comeet', '- CAREERS AT NVIDIA',
    '- Myworkdayjobs.com', '- Lever', etc.  Also removes parenthetical
    job IDs like '(25020)' and 'at Company - Comeet' patterns.
    """
    t = title.lower().strip()
    # Order matters: check compound patterns BEFORE simple suffix stripping
    # 1. Remove 'at Company - Source' suffix (e.g. "Cloud Security Engineer at Port - Comeet")
    t = re.sub(r'\s+at\s+[\w\s]+-\s*(?:comeet|lever|greenhouse|careers)\s*$', '', t)
    # 2. Remove trailing source names: "- Comeet", "- Lever", etc.
    t = re.sub(r'\s*-\s*(?:comeet|lever|greenhouse|jobgether|myworkdayjobs\.com)\s*$', '', t)
    # 2b. Remove "| Source" suffix (e.g. "FinOps Engineer @ Ness | LHH Job Board")
    t = re.sub(r'\s*\|\s*(?:lhh job board|glassdoor|indeed|linkedin|drushim|alljobs)\s*$', '', t, flags=re.IGNORECASE)
    # 2c. Remove "@ Company Name" suffix when it's a company name at end of title
    #     e.g. "FinOps Engineer @ Ness Technologies Israel" → "FinOps Engineer"
    #     Only strip if what follows @ looks like a company (2+ words or known pattern)
    t = re.sub(r'\s*@\s+(?:[A-Z][\w]*[\s]){1,5}[\w]*\s*$', '', t)
    # Also handle lowercase variant
    t = re.sub(r'\s*@\s+\S+(?:\s+\S+){1,4}\s*$', '', t)
    # 3. Remove "- CAREERS AT <company>" suffix
    t = re.sub(r'\s*-\s*careers\s+at\s+\S+\s*$', '', t)
    # 4. Remove LinkedIn-style "Company Name גיוס עובדים" prefix (Hebrew for "hiring")
    #    e.g. "Check Point Software גיוס עובדים Senior FinOps Engineer" → "Senior FinOps Engineer"
    t = re.sub(r'^.*?גיוס\s*עובדים\s*', '', t)
    # 5. Remove "דרושים" (wanted) and "דרוש/ה" prefix patterns
    t = re.sub(r'^דרושים\s*', '', t)
    t = re.sub(r'^דרוש/?ה?\s*', '', t)
    # 5b. Remove Hebrew suffix "לנס (NESS)" or "לחברת X" (to company X)
    t = re.sub(r'\s*ל[\u0590-\u05FF]+\s*(?:\([^)]+\))?\s*התפקיד.*$', '', t)
    # 5c. Remove location suffixes: "| Tel Aviv District", "- Tel Aviv-Yafo, Israel", etc.
    #     These vary by source and cause the same job to appear "new" when re-scraped
    #     from a different source with a different location format.
    t = re.sub(
        r'\s*[\|–—-]\s*(?:tel\s*aviv|jerusalem|haifa|beer\s*sheva|ramat\s*gan|herzliya|'
        r'petah\s*tikva|netanya|rishon|rehovot|modiin|bnei\s*brak|kfar\s*saba|'
        r'raanana|ashdod|ashkelon|eilat|nazareth|acre|tiberias|'
        r'israel|remote|hybrid|on-?site|worldwide|global)[\w\s,.\-–—]*$',
        '', t, flags=re.IGNORECASE
    )
    # Also strip generic "| City District/Region/Area" patterns at end
    t = re.sub(r'\s*\|\s*[A-Za-z\s]+(?:district|region|area|county|province|state)\s*$', '', t, flags=re.IGNORECASE)
    # Strip "- City, Country" or "- City, State" trailing location patterns
    t = re.sub(r'\s*-\s*[A-Za-z\s-]+,\s*(?:israel|il|us|usa|uk|remote)\s*$', '', t, flags=re.IGNORECASE)
    # 6. Remove parenthetical job IDs like (25020)
    t = re.sub(r'\s*\(\d+\)\s*', ' ', t)
    # 7. Clean up trailing punctuation and whitespace
    t = re.sub(r'[\s.,;:]+$', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _is_company_page(j: dict) -> bool:
    """Return True if this listing is a company career-page link, not a specific job.

    Detects patterns like 'Jobs at Vonage', 'Jobs at Deloitte - Comeet',
    or titles that are just the company name (e.g. 'Mobileye', 'Mobileye - Lever').
    """
    title = j.get("title", "").strip()
    company = j.get("company", "").strip()
    t_lower = title.lower()
    c_lower = company.lower()

    # "Jobs at X" or "Jobs at X - Comeet" or "Careers at X"
    if re.match(r'^(?:jobs|careers)\s+at\s+', t_lower):
        return True
    # Title == company name (with optional source suffix like "- Lever", "- Careers")
    cleaned = re.sub(r'\s*-\s*(comeet|lever|greenhouse|jobgether|careers)\s*$', '', t_lower).strip()
    if cleaned == c_lower and cleaned:
        return True
    # Also check against normalized company name
    norm_lower = _normalize_company(company).lower()
    if cleaned == norm_lower and cleaned:
        return True
    return False


def _consolidate_duplicates(jobs: list[dict]) -> list[dict]:
    """Consolidate duplicate listings in the job list.

    Finds jobs that match on company + normalized title but come from
    different sources (or are exact dupes).  Keeps the best entry and
    records the others as altSources.
    """
    from collections import defaultdict

    groups = defaultdict(list)
    for j in jobs:
        comp = _normalize_company(j.get("company", "")).lower().strip()
        norm = _normalize_title(j.get("title", ""))
        groups[(comp, norm)].append(j)

    consolidated = []
    merge_count = 0
    for (comp, norm), entries in groups.items():
        if len(entries) == 1:
            consolidated.append(entries[0])
            continue

        # Multiple entries for same company+role — pick the best primary
        # Prefer: most recent posted date, then entry with most stakeholders
        entries.sort(key=lambda x: (x.get("posted", ""), len(x.get("stakeholders", []))), reverse=True)
        primary = entries[0]

        # Merge altSources from all duplicates
        alt_sources = list(primary.get("altSources", []))
        seen_urls = {primary.get("sourceUrl", "")}
        seen_urls.update(a.get("sourceUrl", "") for a in alt_sources)

        for dup in entries[1:]:
            dup_url = dup.get("sourceUrl", "")
            if dup_url and dup_url not in seen_urls:
                alt_sources.append({
                    "source": detect_source(dup_url),
                    "sourceUrl": dup_url,
                    "title": dup.get("title", "")[:80]
                })
                seen_urls.add(dup_url)
            # Also pull in any altSources the duplicate had
            for a in dup.get("altSources", []):
                a_url = a.get("sourceUrl", "")
                if a_url and a_url not in seen_urls:
                    alt_sources.append(a)
                    seen_urls.add(a_url)

        if alt_sources:
            primary["altSources"] = alt_sources
        merge_count += len(entries) - 1
        consolidated.append(primary)

    if merge_count:
        log.info(f"  Consolidation: merged {merge_count} duplicate listings into existing entries")
    return consolidated


def _load_hidden_companies() -> set:
    """Load hidden companies from outreach_status.json (synced from dashboard).

    Always strips placeholder names like 'unknown' which can be re-added
    by the dashboard's localStorage sync. These aren't real hidden companies —
    they're just listings where company extraction failed.
    """
    _placeholder_names = {"unknown", ""}
    try:
        with open("outreach_status.json", "r") as f:
            data = json.load(f)
        hidden = {c.lower() for c in data.get("hiddenCompanies", [])}
        # Strip placeholders — these should never be hidden
        stripped = hidden - _placeholder_names
        if hidden != stripped:
            log.info(f"  Stripped placeholder names from hidden companies: {hidden - stripped}")
            # Also clean the file to prevent recurring sync issues
            data["hiddenCompanies"] = [c for c in data.get("hiddenCompanies", [])
                                       if c.lower() not in _placeholder_names]
            try:
                with open("outreach_status.json", "w") as f:
                    json.dump(data, f, indent=2)
            except Exception:
                pass
        return stripped
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def merge_jobs(existing: list[dict], new_jobs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Merge new jobs with existing, return (merged, only_new)."""
    # Filter out Develeap's own listings (but keep Unknown company jobs to prevent
    # them from being re-discovered as "new" on every run — which caused duplicate
    # Slack notifications for listings scraped with company="Unknown")
    develeap_names = {"develeap", "develeap ltd", "develeap ltd."}
    existing = [j for j in existing if j.get("company", "").lower() not in develeap_names
                or j.get("_isMock")]

    # Remove company-page listings (not specific job postings)
    before_cp = len(existing)
    existing = [j for j in existing if not _is_company_page(j)]
    if before_cp != len(existing):
        log.info(f"  Removed {before_cp - len(existing)} company-page listings (not specific jobs)")

    # Remove broken FTS listings where company name is actually post text
    # (e.g. company="If you need additional capacity the first thing...")
    before_broken = len(existing)
    existing = [j for j in existing if len(j.get("company", "")) <= 60]
    if before_broken != len(existing):
        log.info(f"  Removed {before_broken - len(existing)} broken FTS listings (company name too long)")

    # Remove aggregator/index pages from existing jobs
    def _is_aggregator(j):
        t = j.get("title", "").lower()
        sub = j.get("subtitle", "").lower()
        u = j.get("sourceUrl", "").lower()
        combined = t + " " + sub
        # Title/subtitle patterns: "X jobs in Israel", "jobs (N)", "Archives", "jobs wanted"
        if re.search(r'(?:^\d+\s+)?(?:.*?\bjobs?\b.*?\bin\b|.*?\bjobs?\b\s*\(\d+\))', t):
            return True
        if any(kw in combined for kw in ["jobs in israel", "apply now", "remote jobs in",
                                   "archives", "משרות דרושים", "jobs wanted",
                                   "as a service for startups", "open positions",
                                   "see our list", "career opportunities",
                                   "we're hiring", "join our team", "jobs at "]):
            return True
        # URL patterns for known aggregators
        agg_domains = ["remoterocketship.com", "devjobs.co.il", "simplyhired.com",
                       "jooble.", "talent.com", "jobrapido.", "careerjet.",
                       "secrettelaviv.com", "efinancialcareers.com",
                       "aidevtlv.com", "machinelearning.co.il", "gotfriends.co.il",
                       "whist.ai", "startup.jobs"]
        # NOTE: comeet.com is an ATS (individual job pages), NOT an aggregator.
        # Do NOT add it here — it caused a severe bug where Comeet listings were
        # removed from existing every run, then re-added as "new", triggering
        # duplicate Slack notifications indefinitely.
        if any(d in u for d in agg_domains):
            return True
        return False

    before_agg = len(existing)
    existing = [j for j in existing if not _is_aggregator(j)]
    if before_agg != len(existing):
        log.info(f"  Removed {before_agg - len(existing)} aggregator pages from existing jobs")

    # Remove jobs with empty or broken URLs
    before_url = len(existing)
    existing = [j for j in existing if j.get("sourceUrl", "").startswith("http")]
    if before_url != len(existing):
        log.info(f"  Removed {before_url - len(existing)} jobs with empty/broken URLs")

    # Validate existing ATS listings by checking for career-page redirects
    # (e.g. expired Greenhouse job IDs redirect to company careers page)
    def _is_ats_career_page(j):
        u = j.get("sourceUrl", "").lower()
        if not u:
            return False
        # Only check ATS URLs where expired IDs can redirect to career pages
        ats_patterns = [
            (r'greenhouse\.io/.+/jobs/\d+', 'greenhouse.io'),
            (r'lever\.co/.+/[a-f0-9-]{20,}', 'lever.co'),
        ]
        is_ats = False
        for pat, domain in ats_patterns:
            if domain in u and re.search(pat, u):
                is_ats = True
                break
        if not is_ats:
            return False
        # Spot-check: HEAD request to detect redirect to career page
        try:
            resp = requests.head(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }, timeout=8, allow_redirects=True)
            final = resp.url.lower()
            # Greenhouse: lost the /jobs/\d+ in final URL
            if 'greenhouse.io' in u and re.search(r'/jobs/\d+', u):
                if not re.search(r'/jobs/\d+', final):
                    log.info(f"  Existing job redirects to career page: {u[:60]} → {final[:60]}")
                    return True
        except Exception:
            pass
        return False

    before_ats_cp = len(existing)
    existing = [j for j in existing if not _is_ats_career_page(j)]
    if before_ats_cp != len(existing):
        log.info(f"  Removed {before_ats_cp - len(existing)} ATS career-page redirects from existing jobs")

    # Remove SPA career sites where location can't be verified server-side
    # (e.g. jobs.apple.com /en-il/ shows jobs from all countries, not just Israel)
    spa_unverifiable = ["jobs.apple.com", "careers.google.com", "careers.microsoft.com"]
    before_spa = len(existing)
    existing = [j for j in existing if not any(d in j.get("sourceUrl", "") for d in spa_unverifiable)]
    if before_spa != len(existing):
        log.info(f"  Removed {before_spa - len(existing)} unverifiable SPA career pages from existing jobs")

    # Remove jobs with clearly non-Israel locations (US cities, states, etc.)
    _NON_ISRAEL_LOCATIONS = [
        "washington", "d.c.", "new york", "san francisco", "california", "texas",
        "boston", "seattle", "denver", "chicago", "virginia", "colorado",
        "palo alto", "austin", "los angeles", "atlanta", "florida", "ohio",
        "michigan", "pennsylvania", "north carolina", "arizona", "portland",
        "minneapolis", "london", "berlin", "paris", "mumbai", "bangalore",
        "singapore", "tokyo", "sydney", "toronto", "vancouver",
        "us government", "u.s. government",
    ]
    def _is_non_israel_location(j):
        loc = j.get("location", "").lower()
        title = j.get("title", "").lower()
        combined = loc + " " + title
        return any(place in combined for place in _NON_ISRAEL_LOCATIONS)

    before_loc = len(existing)
    existing = [j for j in existing if not _is_non_israel_location(j)]
    if before_loc != len(existing):
        log.info(f"  Removed {before_loc - len(existing)} non-Israel located jobs")

    # Re-check existing listings — remove closed, stale (>14d), and non-Israel
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cleaned = []
    removed_urls = set()  # Track URLs of removed jobs to prevent re-adding as "new"
    for j in existing:
        # Skip all cleanup checks for mock/test listings
        if j.get("_isMock"):
            cleaned.append(j)
            continue
        url = j.get("sourceUrl", "")

        # ── Age-check existing jobs by their stored date AND _first_seen ──
        # FTS jobs use a shorter window (7 days) since we only want fresh posts.
        # For regular LinkedIn jobs, also check _first_seen to catch listings
        # where the pipeline keeps resetting posted=today because LinkedIn
        # returns HTTP 429 (no real date available from data-center IPs).
        posted = j.get("posted", "")
        first_seen = j.get("_first_seen", "")
        if posted or first_seen:
            try:
                from datetime import datetime as dt_cls3
                now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                max_age = 14

                # Use the OLDEST known date (posted vs _first_seen) for age check.
                # This prevents the stale-refresh cycle where LinkedIn 429 → posted=today
                # but _first_seen reveals the job has been known for weeks.
                check_date = posted
                if first_seen and posted:
                    check_date = min(posted, first_seen)  # earliest date wins
                elif first_seen:
                    check_date = first_seen

                if check_date:
                    check_dt = dt_cls3.strptime(check_date, "%Y-%m-%d")
                    age_days3 = (now_naive - check_dt).days
                    if age_days3 > max_age:
                        log.info(f"  Removing stale existing listing ({age_days3}d, posted={posted}, first_seen={first_seen}): {j.get('title', '')[:50]}")
                        if url: removed_urls.add(url)
                        continue
            except ValueError:
                pass

        if "linkedin.com" in url and j.get("source") != "linkedin_fts":
            page_data = scrape_job_page(url)
            http_status = page_data.get("_http_status", 200)
            # Playwright for LinkedIn: only for /posts/ URLs (FTS-style) and 429 fallback.
            # Running Playwright on ALL /jobs/view/ URLs was causing the pipeline to exceed
            # the 20-minute GitHub Actions timeout (each PW launch takes ~5-8s × 40+ URLs).
            # LinkedIn auth walls block /jobs/view/ content from GH Actions IPs anyway,
            # so Playwright adds no value for those — the "closed-job" CSS class IS in raw HTML.
            is_post_url = "/posts/" in url
            if is_post_url or http_status == 429:
                log.info(f"  LinkedIn Playwright {'validation' if is_post_url else 'fallback'}: {j.get('title', '')[:50]} (HTTP {http_status})")
                pw_data = _scrape_linkedin_playwright(url)
                if pw_data and pw_data.get("_http_status") == 200:
                    if pw_data.get("closed"):
                        log.info(f"  Removing closed listing (Playwright): {j.get('title', '')[:50]}")
                        if url: removed_urls.add(url)
                        continue
                    pw_text_len = pw_data.get("_text_len", 0)
                    if pw_text_len > 500:
                        if pw_data.get("date") and not page_data.get("date"):
                            page_data["date"] = pw_data["date"]
                            log.info(f"  Date from Playwright: {pw_data['date']} for {j.get('title', '')[:40]}")
                        if pw_data.get("company") and not page_data.get("company"):
                            page_data["company"] = pw_data["company"]
                elif pw_data and pw_data.get("_http_status") in (404, 410):
                    log.info(f"  Removing deleted listing (HTTP {pw_data['_http_status']}): {j.get('title', '')[:50]}")
                    if url: removed_urls.add(url)
                    continue
            if page_data.get("closed"):
                log.info(f"  Removing closed listing (HTTP): {j.get('title', '')[:50]}")
                if url: removed_urls.add(url)
                continue
            # If we now got a real date from the page, update stored date
            if page_data.get("date"):
                if j.get("posted") != page_data["date"]:
                    log.info(f"  Updated date: {j.get('title', '')[:40]} → {page_data['date']}")
                    j["posted"] = page_data["date"]
                # Re-check age with the updated date
                try:
                    from datetime import datetime as dt_cls4
                    post_dt4 = dt_cls4.strptime(page_data["date"], "%Y-%m-%d")
                    age_days4 = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt4).days
                    if age_days4 > 45:
                        log.info(f"  Removing stale listing after date update ({age_days4} days): {j.get('title', '')[:50]}")
                        if url: removed_urls.add(url)
                        continue
                except ValueError:
                    pass
            # If both HTTP and Playwright failed to get data, check if listing is unverified
            if http_status == 429 and not page_data.get("date"):
                # Check if posted date was just a fallback (same as _first_seen)
                posted_val = j.get("posted", "")
                first_seen_val = j.get("_first_seen", "")
                if posted_val and first_seen_val and posted_val == first_seen_val:
                    log.info(f"  Removing unverified listing (429, posted==first_seen={posted_val}): {j.get('title', '')[:50]}")
                    if url: removed_urls.add(url)
                    continue
            # ── Merge LinkedIn hiring team contacts into stakeholders (existing jobs) ──
            if page_data.get("hiring_team"):
                existing_li_urls = {s.get("linkedin", "").rstrip("/").lower()
                                    for s in j.get("stakeholders", []) if s.get("linkedin")}
                existing_names = {s.get("name", "").lower()
                                  for s in j.get("stakeholders", []) if s.get("name")}
                for ht in page_data["hiring_team"]:
                    ht_li = ht.get("linkedin", "").rstrip("/").lower()
                    ht_name = ht.get("name", "").lower()
                    if (ht_li and ht_li in existing_li_urls) or (ht_name and ht_name in existing_names):
                        continue
                    j.setdefault("stakeholders", []).insert(0, ht)
                    log.info(f"  Added hiring team contact: {ht['name']} ({ht.get('title', '')[:40]}) for {j.get('title', '')[:40]}")

            # ── Extract ATS contacts for existing jobs too ──
            job_url_for_ats = j.get("ftsJobUrl", "") or url
            if job_url_for_ats and ("greenhouse.io" in job_url_for_ats or "lever.co" in job_url_for_ats):
                if not any(s.get("source", "").startswith("Greenhouse") or s.get("source", "").startswith("Lever")
                           for s in j.get("stakeholders", [])):
                    ats_contacts = _extract_ats_contacts(job_url_for_ats)
                    existing_names_ats = {s.get("name", "").lower()
                                          for s in j.get("stakeholders", []) if s.get("name")}
                    for ac in ats_contacts:
                        if ac.get("name", "").lower() not in existing_names_ats:
                            j.setdefault("stakeholders", []).insert(0, ac)
                            log.info(f"  Added ATS contact (existing): {ac['name']} for {j.get('title', '')[:40]}")

            # ── Backfill Post Publisher photo for existing FTS listings ──
            if j.get("source") == "linkedin_fts" and page_data.get("post_author_photo"):
                for s in j.get("stakeholders", []):
                    if s.get("source") == "Post Publisher" and not s.get("photo"):
                        s["photo"] = page_data["post_author_photo"]
                        log.info(f"  Updated Post Publisher photo (existing): {s.get('name', '')} for {j.get('title', '')[:40]}")
                        break

        # ── Activity-date age check for existing FTS LinkedIn posts ──
        # Remove posts that are too old based on their LinkedIn activity ID timestamp
        if j.get("source") == "linkedin_fts" and "/posts/" in url:
            activity_date = _extract_linkedin_activity_date(url)
            if activity_date:
                try:
                    from datetime import datetime as dt_cls
                    post_dt = dt_cls.strptime(activity_date, "%Y-%m-%d")
                    age_days_act = (datetime.now(timezone.utc).replace(tzinfo=None) - post_dt).days
                    if age_days_act > 45:
                        log.info(f"  Removing stale FTS post ({age_days_act} days old, posted {activity_date}): {j.get('title', '')[:50]}")
                        if url: removed_urls.add(url)
                        continue
                except ValueError:
                    pass

        # ── Playwright validation for existing FTS LinkedIn posts ──
        if "linkedin.com" in url and j.get("source") == "linkedin_fts":
            pw_data = _scrape_linkedin_playwright(url)
            if pw_data and pw_data.get("_http_status") == 200:
                if pw_data.get("closed"):
                    log.info(f"  Removing closed FTS listing (Playwright): {j.get('title', '')[:50]}")
                    if url: removed_urls.add(url)
                    continue
            elif pw_data and pw_data.get("_http_status") in (404, 410):
                log.info(f"  Removing deleted FTS post (HTTP {pw_data['_http_status']}): {j.get('title', '')[:50]}")
                if url: removed_urls.add(url)
                continue
            # Also re-check the external job URL if available
            fts_job_url = j.get("_fts_job_url", "")
            if fts_job_url:
                ext_data = scrape_job_page(fts_job_url)
                if ext_data.get("closed"):
                    log.info(f"  Removing FTS listing — linked job closed: {j.get('title', '')[:50]}")
                    if url: removed_urls.add(url)
                    continue

            # Check location country from Playwright data (FTS uses pw_data only)
            _loc_data = (pw_data or {}) if j.get("source") == "linkedin_fts" else page_data
            loc_country = _loc_data.get("location_country", "").lower()
            if loc_country:
                israel_indicators = ["israel", "il", "tel aviv", "herzliya", "haifa",
                                     "jerusalem", "ramat gan", "ra'anana", "raanana",
                                     "petah tikva", "netanya", "beer sheva"]
                non_israel_countries = ["india", "united states", "usa", "uk", "united kingdom",
                                        "germany", "france", "china", "japan", "canada",
                                        "australia", "brazil", "singapore", "ireland",
                                        "bangalore", "hyderabad", "mumbai", "delhi", "pune",
                                        "cupertino", "mountain view", "palo alto",
                                        "san francisco", "new york", "london", "berlin"]
                is_israel = any(ind in loc_country for ind in israel_indicators)
                is_non_israel = any(ind in loc_country for ind in non_israel_countries)
                if is_non_israel and not is_israel:
                    log.info(f"  Removing non-Israel existing listing ({loc_country}): {j.get('title', '')[:50]}")
                    if url: removed_urls.add(url)
                    continue
            time.sleep(random.uniform(0.3, 0.8))
        cleaned.append(j)

    log.info(f"  Existing cleanup: {len(existing)} → {len(cleaned)} (removed {len(existing) - len(cleaned)} closed)")
    if removed_urls:
        log.info(f"  Blocklisted {len(removed_urls)} removed URLs to prevent re-adding as new")
    existing = cleaned

    # Normalize company names (e.g. "Checkpoint" → "Check Point Software")
    # Also clean Hebrew localization artifacts from LinkedIn titles
    for j in existing:
        j["company"] = _normalize_company(j.get("company", ""))
        old_title = j.get("title", "")
        if "גיוס" in old_title or "עובדים" in old_title:
            cleaned_title = re.sub(r'\s*גיוס\s*עובדים\s*', ' ', old_title).strip()
            cleaned_title = re.sub(r'\s{2,}', ' ', cleaned_title)
            if cleaned_title != old_title:
                j["title"] = cleaned_title
                log.info(f"  Cleaned Hebrew from title: {old_title[:40]} → {cleaned_title[:40]}")

    # Consolidate duplicates within existing listings before processing new ones
    existing = _consolidate_duplicates(existing)

    # Index existing by URL, by exact company+title, AND by normalized_company+normalized_title
    existing_urls = {j.get("sourceUrl", ""): j for j in existing if j.get("sourceUrl")}
    existing_keys = {f'{j.get("company","").lower()}|{j.get("title","").lower()}': j for j in existing}
    existing_norm = {f'{_normalize_company(j.get("company","")).lower()}|{_normalize_title(j.get("title",""))}': j for j in existing}

    # Mark existing jobs as not new; update stakeholders (preserve enrichment)
    for j in existing:
        j["isNew"] = False
        # Preserve _first_seen — if missing (legacy job), seed from posted date
        if "_first_seen" not in j:
            j["_first_seen"] = j.get("posted", today)
        old_stakeholders = j.get("stakeholders", [])
        new_stakeholders = _get_stakeholders(j.get("company", ""))
        # Preserve ALL enrichment data from previously enriched stakeholders
        # Index old stakeholders by linkedin URL and by name (fallback)
        old_by_li = {s.get("linkedin", ""): s for s in old_stakeholders if s.get("linkedin")}
        old_by_name = {s.get("name", "").lower(): s for s in old_stakeholders if s.get("name")}
        # Fields that the base _get_stakeholders provides (safe to overwrite)
        base_fields = {"name", "title", "linkedin", "source"}
        for s in new_stakeholders:
            li = s.get("linkedin", "")
            name_lower = s.get("name", "").lower()
            old = old_by_li.get(li) or old_by_name.get(name_lower)
            if old:
                # Copy over all enrichment fields (photo, phone, email,
                # _apolloData, connectMsg, followUpMsg, etc.)
                for k, v in old.items():
                    if k not in base_fields and k not in s:
                        s[k] = v
        # Preserve hiring team contacts (source="LinkedIn Job Poster") and other
        # non-auto-discovered contacts that were added during validation
        new_li_urls = {s.get("linkedin", "").rstrip("/").lower() for s in new_stakeholders if s.get("linkedin")}
        new_names = {s.get("name", "").lower() for s in new_stakeholders if s.get("name")}
        for old_s in old_stakeholders:
            if old_s.get("source") not in ("LinkedIn", ""):  # Keep non-standard sources like "LinkedIn Job Poster"
                old_li = old_s.get("linkedin", "").rstrip("/").lower()
                old_name = old_s.get("name", "").lower()
                if (old_li and old_li in new_li_urls) or (old_name and old_name in new_names):
                    continue  # Already in new_stakeholders
                new_stakeholders.insert(0, old_s)
        j["stakeholders"] = new_stakeholders
        # Update logo
        j["logo"] = _get_company_logo(j.get("company", ""), j.get("sourceUrl", ""), j.get("title", ""))
        # Re-classify source from URL (picks up newly added SOURCE_MAP entries)
        # Preserve linkedin_fts source (don't overwrite with generic "linkedin")
        if j.get("source") != "linkedin_fts":
            j["source"] = detect_source(j.get("sourceUrl", ""))
        # Re-classify category (picks up newly added categories; fills missing ones)
        _new_cat = detect_category(j.get("title", ""), j.get("description", "") or j.get("subtitle", ""))
        if _new_cat is not None:
            j["category"] = _new_cat
        elif not j.get("category"):
            # No keyword match but job has no category — assign "other" so it's always set
            j["category"] = _categorize_job(j.get("title", ""), j.get("description", "") or j.get("subtitle", ""))
        # Re-classify customer status
        company = j.get("company", "")
        j["isDeveleapCustomer"] = is_develeap_customer(company)
        j["isPastCustomer"] = is_develeap_past_customer(company)

    truly_new = []
    _hidden = _load_hidden_companies()
    for j in new_jobs:
        # Normalize company name on incoming jobs
        j["company"] = _normalize_company(j.get("company", ""))

        # Skip Unknown/empty company jobs from new — they stay in existing for dedup
        # but we don't want to re-add them as new listings
        # Exception: linkedin_fts and indeed results are allowed with Unknown company
        # (linkedin_fts = social posts where company extraction is harder;
        #  indeed = scraping blocked so company comes from search result only)
        if j.get("company", "").strip() in ("Unknown", "") and j.get("source") not in ("linkedin_fts", "indeed"):
            continue

        # Skip company-page listings from new jobs too
        if _is_company_page(j):
            log.info(f"  Skipping company-page listing: \"{j.get('title', '')}\" ({j.get('company', '')})")
            continue

        # Skip new listings from hidden companies (user marked as not relevant)
        if j.get("company", "").lower() in _hidden:
            log.info(f"  Skipping hidden company listing: \"{j.get('title', '')}\" ({j.get('company', '')})")
            continue

        url = j.get("sourceUrl", "")

        # Block re-adding jobs that were just removed as stale/closed in this run.
        # This breaks the cycle: search engine finds old listing → pipeline removes
        # it as stale → same listing appears in new_jobs → re-added with posted=today.
        if url and url in removed_urls:
            log.info(f"  Blocking re-add of removed URL: {j.get('title', '')[:50]} ({url[:60]})")
            continue

        comp_lower = j.get("company", "").lower()
        key = f'{comp_lower}|{j.get("title","").lower()}'
        norm_key = f'{comp_lower}|{_normalize_title(j.get("title",""))}'

        # Check all three indexes: URL, exact key, and normalized key
        if url not in existing_urls and key not in existing_keys and norm_key not in existing_norm:
            truly_new.append(j)
        else:
            # Duplicate listing found — update company if existing entry has "Unknown"
            # or a garbled/job-title-like company from a previous parsing bug
            match = existing_urls.get(url) or existing_keys.get(key) or existing_norm.get(norm_key)
            if match:
                _old_co = match.get("company", "").strip()
                _new_co = j.get("company", "").strip()
                # Update company if existing entry has Unknown/empty company or a garbled/
                # job-title-like name from a previous parsing bug
                if _new_co and _new_co not in ("Unknown", "") and (_old_co in ("Unknown", "") or _is_job_title(_old_co)):
                    log.info(f"  Company resolved: '{_old_co}' → '{_new_co}' for {j.get('title', '')[:50]}")
                    match["company"] = _new_co
                    match["logo"] = _get_company_logo(_new_co, match.get("sourceUrl", ""))
                    match["stakeholders"] = _get_stakeholders(_new_co)
                    match["isDeveleapCustomer"] = is_develeap_customer(_new_co)
                    match["isPastCustomer"] = is_develeap_past_customer(_new_co)
                # Also refresh logo if company is known but logo is missing
                elif _old_co and _old_co not in ("Unknown", "") and not match.get("logo"):
                    match["logo"] = _get_company_logo(_old_co, match.get("sourceUrl", ""))
                    if match["logo"]:
                        log.info(f"  Logo refreshed for: {_old_co}")
                if url and url != match.get("sourceUrl", ""):
                    alt_source = detect_source(url)
                    alt_sources = match.get("altSources", [])
                    # Avoid adding the same source URL twice
                    if not any(a.get("sourceUrl") == url for a in alt_sources):
                        alt_sources.append({
                            "source": alt_source,
                            "sourceUrl": url,
                            "title": j.get("title", "")[:80]
                        })
                        match["altSources"] = alt_sources
                        log.info(f"  Alt source added: {match.get('company','')} — {alt_source} ({url[:60]})")

    merged = existing + truly_new

    # Final consolidation pass — catches any duplicates between existing and truly_new
    merged = _consolidate_duplicates(merged)

    # ── Freshness cutoff: remove anything older than 14 days ──
    # Use min(posted, _first_seen) to catch jobs with artificially fresh posted dates
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%d")
    before_count = len(merged)
    def _effective_date(j):
        """Oldest known date for a job — prevents stale jobs hiding behind posted=today."""
        posted = j.get("posted") or "9999"
        first_seen = j.get("_first_seen") or "9999"
        return min(posted, first_seen)
    merged = [j for j in merged if _effective_date(j) >= cutoff]
    dropped = before_count - len(merged)
    if dropped:
        log.info(f"  Freshness filter: dropped {dropped} listings older than {cutoff}")

    # Sort by date descending
    merged.sort(key=lambda x: x.get("posted", ""), reverse=True)
    # Keep max 200 listings
    merged = merged[:200]

    # Also filter truly_new to only include fresh listings
    truly_new = [j for j in truly_new if (j.get("posted") or "9999") >= cutoff]

    # ── Fix isNew based on actual posted date ──
    # isNew should only be True if the job was posted within the last 36 hours,
    # NOT just because the scraper discovered it for the first time.
    # Jobs posted days/weeks ago that we're seeing for the first time are NOT "new".
    new_cutoff = (datetime.now(timezone.utc) - timedelta(hours=36)).strftime("%Y-%m-%d")
    for j in merged:
        posted = j.get("posted", "")
        if j.get("isNew") and posted and posted < new_cutoff:
            j["isNew"] = False

    return merged, truly_new


def update_dashboard_html(html: str, jobs: list[dict], health: list[dict] | None = None) -> str:
    """Replace ALL_JOBS array, timestamp, and source health in dashboard HTML."""
    # Format jobs as JS array
    jobs_json = json.dumps(jobs, ensure_ascii=False, indent=2)
    # Replace ALL_JOBS — use lambda to avoid re.sub interpreting backslashes in replacement
    replacement = f'let ALL_JOBS = {jobs_json};'
    html = re.sub(
        r'let ALL_JOBS\s*=\s*\[.*?\];\s*$',
        lambda _: replacement,
        html,
        flags=re.DOTALL | re.MULTILINE
    )
    # Update LAST_UPDATED constant
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    html = re.sub(
        r'(?:const|let)\s+LAST_UPDATED\s*=\s*"[^"]*"',
        lambda _: f'let LAST_UPDATED = "{now_iso}"',
        html
    )
    # Update SOURCE_HEALTH data
    if health is not None:
        health_json = json.dumps(health, ensure_ascii=False)
        if re.search(r'(?:const|let|var)\s+SOURCE_HEALTH\s*=', html):
            html = re.sub(
                r'(?:const|let|var)\s+SOURCE_HEALTH\s*=\s*\[.*?\];',
                lambda _: f'let SOURCE_HEALTH = {health_json};',
                html,
                flags=re.DOTALL
            )
        else:
            # Insert SOURCE_HEALTH right after LAST_UPDATED
            html = html.replace(
                f'let LAST_UPDATED = "{now_iso}"',
                f'let LAST_UPDATED = "{now_iso}";\nlet SOURCE_HEALTH = {health_json}',
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


# ── Slack Dedup Tracking ───────────────────────────────────────────────────

def _slack_listing_key(job: dict) -> str:
    """Build a unique identifier for a listing: company|||normalized_title.

    Uses _normalize_company and _normalize_title so that different scrape variants
    of the same job (e.g. Hebrew vs English title, different source suffixes)
    resolve to the same key and avoid duplicate Slack posts.

    NOTE: Category is intentionally excluded from the key. Including category caused
    duplicate Slack notifications when the same job was re-scraped with a different
    category (e.g., "finops" vs "devops") due to different description/subtitle text.

    NOTE: Date is intentionally excluded from the key. Including the date caused
    the same job to be re-posted whenever it was re-scraped with a different date.
    A 30-day staleness window is used instead to allow genuinely re-opened roles
    to be re-posted.

    Uses '|||' as separator instead of '|' because job titles can contain pipes
    (e.g. "Cloud FinOps Engineer | Tel Aviv District") which broke legacy key
    parsing and migration.
    """
    company = _normalize_company(job.get("company") or "").lower().strip()
    title = _normalize_title(job.get("title") or "")
    return f"{company}|||{title}"


def _slack_listing_key_legacy(job: dict) -> str:
    """Legacy key format with date (for backward-compatible dedup)."""
    company = _normalize_company(job.get("company") or "").lower().strip()
    category = (job.get("category") or "").lower().strip()
    title = _normalize_title(job.get("title") or "")
    posted = (job.get("posted") or "")[:10]
    return f"{company}|{category}|{title}|{posted}"


# Staleness window: don't re-post a job if seen within this many days
SLACK_DEDUP_STALENESS_DAYS = 30


def _load_slack_posted() -> dict:
    """Load the posted tracking data.

    Returns dict with:
      - posted_keys: set of keys in ALL formats (legacy company|category|title,
        new company|||title, and date-suffixed variants) for maximum dedup coverage
      - posted_keys_with_dates: set of legacy keys (for backward compat)
      - first_seen: dict mapping key → ISO timestamp of first posting
    """
    try:
        with open(SLACK_POSTED_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            result = {
                "posted_keys": set(data.get("posted_keys", [])),
                "posted_keys_with_dates": set(data.get("posted_keys_with_dates", [])),
                "first_seen": data.get("first_seen", {}),
            }
            # Migrate: extract dateless keys AND new-format keys from legacy entries.
            # Legacy entries can have embedded pipes in titles (e.g. "Cloud FinOps Engineer | Tel Aviv")
            # so we use regex to find the trailing date instead of splitting by |.
            for legacy_key in list(result["posted_keys"]):
                # Check if key ends with |YYYY-MM-DD (date suffix)
                m = re.match(r'^(.+)\|(\d{4}-\d{2}-\d{2})$', legacy_key)
                if m:
                    dateless = m.group(1)  # e.g. "torq|finops|cloud finops engineer | tel aviv district"
                    date_str = m.group(2)
                    # Add the dateless version (old format: company|category|title)
                    result["posted_keys"].add(dateless)
                    result["posted_keys_with_dates"].add(legacy_key)
                    if dateless not in result["first_seen"]:
                        result["first_seen"][dateless] = date_str + "T00:00:00+00:00"
                    # Also build new-format key (company|||title) by extracting company
                    # and title from the old format: company|category|rest_is_title
                    parts = dateless.split("|", 2)  # Split into at most 3 parts
                    if len(parts) >= 3:
                        company_part = parts[0]
                        title_part = parts[2]  # Everything after company|category|
                        # Add both raw and re-normalized versions of the new key
                        # (re-normalize catches location suffixes that old code didn't strip)
                        raw_new_key = f"{company_part}|||{title_part}"
                        norm_new_key = f"{company_part}|||{_normalize_title(title_part)}"
                        for nk in (raw_new_key, norm_new_key):
                            result["posted_keys"].add(nk)
                            if nk not in result["first_seen"]:
                                result["first_seen"][nk] = date_str + "T00:00:00+00:00"

            # Also migrate any dateless legacy keys (company|category|title) to new format
            for key in list(result["posted_keys"]):
                if "|||" not in key:  # Not already new format
                    parts = key.split("|", 2)
                    if len(parts) >= 3 and not re.match(r'^\d{4}-\d{2}-\d{2}$', parts[-1]):
                        # Looks like company|category|title (not a date-keyed entry)
                        company_part = parts[0]
                        title_part = parts[2]
                        raw_new_key = f"{company_part}|||{title_part}"
                        norm_new_key = f"{company_part}|||{_normalize_title(title_part)}"
                        for nk in (raw_new_key, norm_new_key):
                            result["posted_keys"].add(nk)
                            if nk not in result["first_seen"] and key in result["first_seen"]:
                                result["first_seen"][nk] = result["first_seen"][key]

            return result
    except (FileNotFoundError, json.JSONDecodeError):
        return {"posted_keys": set(), "posted_keys_with_dates": set(), "first_seen": {}}


def _save_slack_posted(tracking: dict) -> None:
    """Persist the posted tracking data. Keep last 2000 dateless keys."""
    dateless_keys = sorted(tracking["posted_keys"], reverse=True)[:2000]
    first_seen = {k: v for k, v in tracking.get("first_seen", {}).items() if k in set(dateless_keys)}
    with open(SLACK_POSTED_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "posted_keys": dateless_keys,
            "first_seen": first_seen,
            "updated": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)


def _filter_unposted_jobs(jobs: list[dict]) -> list[dict]:
    """Filter out jobs that have already been posted to Slack.

    Uses multiple key formats (new company|||title and legacy company|category|title)
    to prevent re-posting the same job. A staleness window allows genuinely
    re-opened roles to be re-posted after SLACK_DEDUP_STALENESS_DAYS.
    """
    tracking = _load_slack_posted()
    posted_keys = tracking["posted_keys"]
    first_seen = tracking.get("first_seen", {})
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SLACK_DEDUP_STALENESS_DAYS)).isoformat()

    unposted = []
    skipped_dedup = 0
    skipped_stale_repost = 0

    for j in jobs:
        key = _slack_listing_key(j)  # New format: company|||title

        # Build all possible key variants to check (handles format transitions)
        company = _normalize_company(j.get("company") or "").lower().strip()
        category = (j.get("category") or "").lower().strip()
        title = _normalize_title(j.get("title") or "")
        keys_to_check = [
            key,                                    # New format: company|||title
            f"{company}|{category}|{title}",        # Legacy format with category
        ]

        matched_key = None
        for k in keys_to_check:
            if k in posted_keys:
                matched_key = k
                break

        if matched_key:
            # Check staleness: if first_seen is within the window, skip
            seen_at = first_seen.get(matched_key, "")
            if seen_at and seen_at >= cutoff:
                skipped_dedup += 1
                continue
            elif seen_at:
                # Seen more than STALENESS_DAYS ago — allow re-post (genuinely re-opened role)
                log.info(f"  Re-posting stale job (first seen {seen_at[:10]}): {j.get('title','')[:50]}")
                unposted.append(j)
                continue
            else:
                skipped_dedup += 1
                continue

        # Also check legacy keys with dates for backward compat
        legacy_key = _slack_listing_key_legacy(j)
        if legacy_key in tracking.get("posted_keys_with_dates", set()):
            skipped_dedup += 1
            continue
        unposted.append(j)

    total_filtered = skipped_dedup + skipped_stale_repost
    if total_filtered > 0:
        log.info(f"  Slack dedup: {len(jobs)} candidates → {len(unposted)} new (filtered {skipped_dedup} duplicates)")
    return unposted


# ── Slack Notification ─────────────────────────────────────────────────────

def notify_slack(new_jobs: list[dict]) -> bool:
    """Post new listings to Slack #bdr-updates via incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False
    if not new_jobs:
        log.info("No new jobs to notify about")
        return True

    cat_emoji = {"devops": ":gear:", "ai": ":robot_face:", "agentic": ":zap:", "finops": ":moneybag:"}
    cat_labels = {"devops": "DevOps", "ai": "AI/ML", "agentic": "Agentic", "finops": "FinOps"}

    # Separate Develeap customer listings
    customer_jobs = [j for j in new_jobs if j.get("isDeveleapCustomer")]
    other_jobs = [j for j in new_jobs if not j.get("isDeveleapCustomer")]

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f":newspaper:  {len(new_jobs)} New Job Listings Found", "emoji": True}
    })

    # Develeap customer alerts first (individual cards)
    if customer_jobs:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":rotating_light: *Develeap Customer Listings*"}
        })
        for j in customer_jobs:
            cat = cat_labels.get(j.get("category", ""), "DevOps")
            emoji = cat_emoji.get(j.get("category", ""), ":briefcase:")
            url = j.get("sourceUrl", "")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":star: *<{url}|{j['title'][:60]}>*\n"
                        f"Company: *{j['company']}*  |  {emoji} {cat}  |  :round_pushpin: {j.get('location', 'Israel')}"
                    )
                }
            })
        blocks.append({"type": "divider"})

    # All other listings as a compact table
    if other_jobs:
        if customer_jobs:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": ":briefcase: *Other New Listings*"}
            })

        # Group into chunks to stay within Slack's text limit
        chunk_size = 10
        for i in range(0, len(other_jobs), chunk_size):
            chunk = other_jobs[i:i + chunk_size]
            lines = []
            for j in chunk:
                cat = cat_labels.get(j.get("category", ""), "DevOps")
                emoji = cat_emoji.get(j.get("category", ""), ":briefcase:")
                url = j.get("sourceUrl", "")
                company = j["company"] if j["company"] != "Unknown" else "_Unknown_"
                lines.append(
                    f"{emoji}  <{url}|*{j['title'][:55]}*>\n"
                    f"      {company}  ·  {j.get('location', 'Israel')}"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n\n".join(lines[:chunk_size])}
            })

    # Footer with dashboard link
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": ":bar_chart: <https://dorikafri.github.io/develeap-bdr-job-monitor/|Open Full Dashboard>  |  Powered by Develeap BDR Monitor"
        }]
    })

    payload = {"blocks": blocks}

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        log.info(f"Slack notification sent for {len(new_jobs)} new listings")
        # Record posted keys so we never re-post these listings
        tracking = _load_slack_posted()
        now_iso = datetime.now(timezone.utc).isoformat()
        for j in new_jobs:
            key = _slack_listing_key(j)
            tracking["posted_keys"].add(key)
            if key not in tracking.get("first_seen", {}):
                tracking.setdefault("first_seen", {})[key] = now_iso
        _save_slack_posted(tracking)
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False


# ── Main───────────────────────────────────────────────────────────────────

def main():
    global _auto_discover_count
    _auto_discover_count = 0  # Reset per run
    _stakeholder_cache.clear()

    log.info("=== Develeap BDR Job Monitor Update ===")

    # ── SerpAPI quota conservation: skip alternate runs ──────────────
    # Pipeline runs 6x/day but we only need 3x/day to conserve SerpAPI quota.
    # Skip runs at even UTC hours (6, 10, 14) — only run at odd hours (8, 12, 16).
    # This halves SerpAPI usage without requiring workflow file changes.
    # Remove this block after SerpAPI plan renews on 2026-04-08.
    _run_hour = datetime.now(timezone.utc).hour
    if _run_hour in (6, 10, 14):
        log.info(f"Skipping run at UTC hour {_run_hour} to conserve SerpAPI quota (renews 2026-04-08)")
        return

    # Load workflow config to check which nodes are enabled
    wf_config = _load_workflow_config()
    if wf_config:
        log.info("Loaded workflow config (version %s)", wf_config.get("version", "?"))
        if not _is_node_enabled(wf_config, "discovery"):
            log.info("Job Discovery node is DISABLED in workflow config — skipping run")
            return

    # 0. Source health check
    source_health = check_source_health()

    # 1. Search for jobs
    log.info(f"Searching with {len(SEARCH_QUERIES)} queries...")
    all_raw = []
    for query in SEARCH_QUERIES:
        results = search_jobs(query)
        all_raw.extend(results)
        log.info(f"  '{query}' → {len(results)} results")
        time.sleep(random.uniform(1.0, 2.5))

    # Detect Israel weekend — skip SerpAPI-heavy searches to conserve quota
    _weekend = _is_israel_weekend()
    if _weekend:
        log.info("Israel weekend mode (Thu 19:00 – Sun 07:00): skipping SerpAPI-dependent searches (Google Jobs, Indeed)")

    # 1b. Also search Google Jobs engine (structured job listings)
    if not _weekend:
        log.info("Searching Google Jobs engine...")
        gj_results = search_google_jobs()
        all_raw.extend(gj_results)
        log.info(f"Google Jobs engine: {len(gj_results)} results")
    else:
        log.info("Skipping Google Jobs engine (weekend mode)")

    # 1c. Search Indeed via SerpAPI's dedicated Indeed engine
    # (site: queries return search-result pages that get filtered; engine=indeed returns viewjob URLs)
    if not _weekend:
        log.info("Searching Indeed (SerpAPI engine=indeed)...")
        indeed_results = search_indeed_serpapi_engine()
        all_raw.extend(indeed_results)
        log.info(f"Indeed engine: {len(indeed_results)} results")
    else:
        log.info("Skipping Indeed engine (weekend mode)")

    # 1d. Add seed jobs (manually curated listings for categories search engines miss)
    all_raw.extend(SEED_JOBS)
    log.info(f"Added {len(SEED_JOBS)} seed jobs")

    # 1d. LinkedIn FTS: search LinkedIn posts for hiring announcements
    # First, pick up results from the standalone FTS runner (fts_results.json)
    fts_runner_results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fts_results.json")
    fts_runner_count = 0
    if os.path.exists(fts_runner_results_path):
        try:
            with open(fts_runner_results_path, "r") as f:
                fts_runner_data = json.load(f)
            if isinstance(fts_runner_data, list) and fts_runner_data:
                all_raw.extend(fts_runner_data)
                fts_runner_count = len(fts_runner_data)
                log.info(f"LinkedIn FTS runner: loaded {fts_runner_count} results from fts_results.json")
                # Clear the staging file after consuming
                with open(fts_runner_results_path, "w") as f:
                    json.dump([], f)
        except Exception as e:
            log.warning(f"Failed to load fts_results.json: {e}")

    # Also run the built-in FTS search (complements the runner)
    log.info("Searching LinkedIn posts (FTS)...")
    fts_results = search_linkedin_fts()
    all_raw.extend(fts_results)
    log.info(f"LinkedIn FTS: {len(fts_results)} hiring posts found (+ {fts_runner_count} from runner)")

    # 1e. Develeap Customer FTS: targeted search for customer companies
    log.info("Searching for Develeap customer hiring posts...")
    customer_fts_results = search_develeap_customer_fts()
    all_raw.extend(customer_fts_results)
    log.info(f"Develeap Customer FTS: {len(customer_fts_results)} results")

    # 1f. Greenhouse boards API: scan known company boards for open roles
    log.info("Scanning Greenhouse boards for open roles...")
    greenhouse_results = scan_greenhouse_boards()
    all_raw.extend(greenhouse_results)
    log.info(f"Greenhouse boards scan: {len(greenhouse_results)} results")

    log.info("Scanning Lever boards for open roles...")
    lever_results = scan_lever_boards()
    all_raw.extend(lever_results)
    log.info(f"Lever boards scan: {len(lever_results)} results")

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

        # Remove listings from hidden companies (user marked as not relevant)
        _hidden = _load_hidden_companies()
        if _hidden:
            before_count = len(existing)
            existing = [j for j in existing if j.get("company", "").lower() not in _hidden]
            removed = before_count - len(existing)
            if removed:
                log.info(f"  Removed {removed} listing(s) from hidden companies")
    else:
        log.error(f"Dashboard not found at {DASHBOARD_PATH}")
        return

    # 3b. Clean existing jobs: re-extract company from ATS URLs (most reliable)
    #     and fix entries where company looks like a job title
    ats_url_patterns = [
        r"greenhouse\.io/", r"lever\.co/", r"ashbyhq\.com/", r"comeet\.com/jobs/",
        r"\.myworkdayjobs\.com", r"jobvite\.com/",
    ]
    for j in existing:
        url = j.get("sourceUrl", "")
        old_company = j.get("company", "")
        needs_fix = False

        # Always re-extract from ATS URLs (they embed the real company slug)
        if any(re.search(p, url) for p in ats_url_patterns):
            fixed = extract_company("", "", url)  # URL-only extraction
            if fixed != "Unknown" and fixed.lower() != old_company.lower():
                needs_fix = True
        # For Indeed viewjob URLs, detect garbled company names left from a prior
        # parsing bug (e.g. "Requirements: B", "Platform C++ Engineer (Cortex XDR)").
        # Reset them to "Unknown" so the merge step can overwrite with the correct
        # company name when the same URL is found in the new search results.
        elif re.search(r'indeed\.com/viewjob', url, re.IGNORECASE) and old_company not in ("Unknown", ""):
            _role_words = {"engineer", "developer", "architect", "analyst", "consultant",
                           "specialist", "manager", "director", "coordinator", "administrator"}
            # ATS platform names that occasionally appear as company on Indeed
            _ats_platforms = {"workday", "greenhouse", "lever", "ashby", "jobvite",
                              "smartrecruiters", "bamboohr", "icims", "comeet"}
            _is_garbled = (
                ":" in old_company                                           # "Requirements: B"
                or len(old_company) > 45                                     # description snippets
                or any(w in old_company.lower() for w in               # description-starter words
                       ("requirements", "experience", "skills", "qualifications"))
                or any(f" {w}" in old_company.lower()                        # embedded role words
                       or old_company.lower().startswith(w)
                       for w in _role_words)
                or bool(re.search(r'\(\s*(?:ra\'?anana|tel[- ]?aviv|herzliya|'
                                  r'haifa|jerusalem|netanya|petah tikva|'
                                  r'ramat gan|beer.?sheva)\b', old_company,
                                  re.IGNORECASE))                            # "(Raanana Office)"
                or old_company.lower() in _ats_platforms                     # ATS name as company
            )
            if _is_garbled:
                fixed = "Unknown"
                needs_fix = True
        # Also fix entries where company looks like a job title
        elif _is_job_title(old_company) or old_company in ("Unknown", ""):
            fixed = extract_company(j.get("title", ""), j.get("description", ""), url)
            if fixed != old_company:
                needs_fix = True

        if needs_fix:
            log.info(f"  Fixed company: '{old_company}' → '{fixed}'")
            j["company"] = fixed
            j["isDeveleapCustomer"] = is_develeap_customer(fixed)
            j["isPastCustomer"] = is_develeap_past_customer(fixed)
            j["stakeholders"] = _get_stakeholders(fixed)
            j["logo"] = _get_company_logo(fixed, url, j.get("title", ""))

    # 4. Merge and identify new listings
    merged, truly_new = merge_jobs(existing, new_jobs)
    log.info(f"After merge: {len(merged)} total, {len(truly_new)} new")
    customer_new = [j for j in truly_new if j.get("isDeveleapCustomer")]
    if customer_new:
        log.info(f"  🌟 {len(customer_new)} new listings from Develeap customers!")

    # 4a-2. Log auto-discovery stats
    auto_found = sum(1 for j in merged if any(s.get("source") == "Auto-discovered" for s in j.get("stakeholders", [])))
    if _auto_discover_count > 0:
        log.info(f"  Auto-discovered stakeholders for {auto_found} listings ({_auto_discover_count} SerpAPI lookups)")

    # 4b. Enrich stakeholders with LinkedIn profile photos
    log.info("Enriching stakeholder photos from LinkedIn...")
    photo_cache = {}  # linkedin_url → base64 data URI (or "" if failed)
    # First pass: collect all already-known photos
    for j in merged:
        for s in j.get("stakeholders", []):
            li = s.get("linkedin", "")
            if li and s.get("photo"):
                photo_cache[li] = s["photo"]
    # Second pass: fetch missing photos (deduplicated by LinkedIn URL)
    photo_count = 0
    fetch_count = 0
    max_fetches = 0  # DISABLED to conserve SerpAPI quota (renews 2026-04-08)
    for j in merged:
        company = j.get("company", "")
        for s in j.get("stakeholders", []):
            name = s.get("name", "")
            li = s.get("linkedin", "")
            cache_key = li or name  # Use LinkedIn URL as key, or name if no URL
            if not cache_key:
                continue
            if cache_key in photo_cache:
                if photo_cache[cache_key]:
                    s["photo"] = photo_cache[cache_key]
                continue
            if fetch_count >= max_fetches:
                photo_cache[cache_key] = ""
                continue
            photo = _fetch_linkedin_photo(name, company, li)
            photo_cache[cache_key] = photo
            fetch_count += 1
            if photo:
                s["photo"] = photo
                photo_count += 1
            time.sleep(random.uniform(0.3, 0.8))  # Brief pause between SerpAPI calls
    # Apply cached photos to any remaining duplicates
    for j in merged:
        for s in j.get("stakeholders", []):
            li = s.get("linkedin", "")
            name = s.get("name", "")
            cache_key = li or name
            if cache_key and not s.get("photo") and photo_cache.get(cache_key):
                s["photo"] = photo_cache[cache_key]
    log.info(f"  Fetched {photo_count} new photos ({fetch_count} SerpAPI requests)")

    # 4c. Validate stakeholder LinkedIn URLs (catch broken/404 profiles)
    log.info("Validating stakeholder LinkedIn URLs...")
    merged = _validate_linkedin_urls(merged)

    # 4d. Generate personalized outreach messages for each stakeholder
    log.info("Generating personalized outreach messages...")
    msg_count = 0
    for j in merged:
        if j.get("stakeholders"):
            _generate_outreach_messages(j)
            msg_count += len(j["stakeholders"])
    log.info(f"  Generated messages for {msg_count} stakeholder contacts")

    # 5. Update dashboard HTML
    updated_html = update_dashboard_html(html, merged, health=source_health)
    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(updated_html)
    # Also write to docs/ for GitHub Pages
    docs_path = os.path.join(os.path.dirname(DASHBOARD_PATH), "..", "docs", "index.html")
    os.makedirs(os.path.dirname(docs_path), exist_ok=True)
    with open(docs_path, "w", encoding="utf-8") as f:
        f.write(updated_html)
    log.info("Dashboard HTML updated (dashboard/ + docs/)")

    # 6. Deploy to Netlify
    if deploy_to_netlify(updated_html):
        log.info("✅ Netlify deploy successful")
    else:
        log.warning("⚠️  Netlify deploy failed")

    # 7. Notify Slack (with dedup to prevent re-posting)
    if truly_new:
        unposted = _filter_unposted_jobs(truly_new)
        if unposted:
            notify_slack(unposted)
        else:
            log.info("All new listings already posted to Slack — skipping")
    else:
        log.info("No new listings — skipping Slack notification")

    _save_indeed_cache()
    log.info("=== Update complete ===")


if __name__ == "__main__":
    try:
        main()
    finally:
        _shutdown_playwright()
