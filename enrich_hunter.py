"""
Hunter.io Enrichment - Enriches stakeholder contacts and companies with Hunter data.

Reads stakeholders from ALL_JOBS in docs/index.html, calls Hunter People Find
and Company Enrichment APIs, writes results to apollo_data.json for backwards compat.

Environment variables:
  HUNTER_API_KEY - Hunter.io API key (from Settings > Integrations > API Keys)
"""

import os
import sys
import json
import time
import re
import base64
import logging
import requests
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "").strip()
HUNTER_BASE = "https://api.hunter.io/v2"
OUTPUT_FILE = "apollo_data.json"
ARCHIVE_FILE = "apollo_data_archive.json"
DOCS_HTML = "docs/index.html"
ARCHIVE_DAYS = 90  # contacts/orgs not updated in this many days are archived

# Rate limiting: Hunter allows 500 calls/min ≈ 8.33/sec
REQUEST_DELAY = 0.15  # seconds between API calls (0.2 * 5 calls per second = ~300/min with some headroom)

# LinkedIn default avatar URL — not a real profile photo, skip it
_LINKEDIN_DEFAULT_AVATAR = "https://static.licdn.com/aero-v1/sc/h/9c8pery4andzj6ohjkjp54ma2"

# Known LinkedIn CDN prefixes that frequently rotate/expire — skip downloading
_LINKEDIN_CDN_PREFIXES = (
    "https://media.licdn.com/",
    "https://media-exp",
    "https://static.licdn.com/",
)

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 60  # seconds; doubles each retry


def _hunter_request_with_retry(method, url, **kwargs):
    """Execute a Hunter API request with exponential backoff on 429."""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, **kwargs)
        except requests.Timeout:
            log.warning("Request timed out: %s %s (attempt %d/%d)", method, url, attempt + 1, _MAX_RETRIES + 1)
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BASE_DELAY * (2 ** attempt))
                continue
            raise
        except requests.ConnectionError as exc:
            log.warning("Connection error: %s %s — %s (attempt %d/%d)", method, url, exc, attempt + 1, _MAX_RETRIES + 1)
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BASE_DELAY * (2 ** attempt))
                continue
            raise

        if resp.status_code == 429:
            wait = _RETRY_BASE_DELAY * (2 ** attempt)
            log.warning("Rate limited (429). Waiting %ds before retry %d/%d...", wait, attempt + 1, _MAX_RETRIES)
            if attempt < _MAX_RETRIES:
                time.sleep(wait)
                continue
            log.error("Rate limit retries exhausted for %s %s", method, url)
            return resp

        return resp

    return resp  # unreachable, but satisfies type checkers


def _download_photo_b64(url):
    """Download a profile photo URL and return a base64 data URI, or None on failure."""
    if not url:
        return None
    if url == _LINKEDIN_DEFAULT_AVATAR:
        return None
    # LinkedIn CDN URLs rotate frequently and are not reliably downloadable
    if any(url.startswith(prefix) for prefix in _LINKEDIN_CDN_PREFIXES):
        log.debug("Skipping LinkedIn CDN photo (may be expired/rotated): %s", url[:80])
        return None
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Referer": ""},
            timeout=10,
            allow_redirects=True,
        )
        if r.status_code != 200:
            log.debug("Photo download returned HTTP %d for %s", r.status_code, url[:80])
            return None
        ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        if not ct.startswith("image/"):
            log.debug("Photo URL returned non-image content-type '%s': %s", ct, url[:80])
            return None
        b64 = base64.b64encode(r.content).decode("ascii")
        return f"data:{ct};base64,{b64}"
    except requests.Timeout:
        log.debug("Timeout downloading photo: %s", url[:80])
        return None
    except requests.ConnectionError as exc:
        log.debug("Connection error downloading photo: %s — %s", url[:80], exc)
        return None
    except requests.RequestException as exc:
        log.warning("Unexpected error downloading photo %s: %s", url[:80], exc)
        return None


# ── Workflow Config ───────────────────────────────────────────────────────
WORKFLOW_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflow_config.json")

def _load_workflow_config():
    """Load workflow_config.json if it exists."""
    if os.path.exists(WORKFLOW_CONFIG_PATH):
        try:
            with open(WORKFLOW_CONFIG_PATH, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as exc:
            log.warning("workflow_config.json is invalid JSON: %s", exc)
        except OSError as exc:
            log.warning("Could not read workflow_config.json: %s", exc)
    return {}

def _is_node_enabled(config, node_id):
    """Check if a workflow node is enabled. Defaults to True if not configured."""
    return config.get("nodes", {}).get(node_id, {}).get("enabled", True)


def extract_stakeholders_from_html(path):
    """Extract stakeholders from ALL_JOBS in the HTML file.
    Returns list of {name, title, email, company, linkedin} dicts."""
    stakeholders = []
    seen = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as exc:
        log.warning("Could not read %s: %s", path, exc)
        return []

    try:
        # Find ALL_JOBS array (may use let or const)
        match = re.search(r'(?:let|const|var)\s+ALL_JOBS\s*=\s*\[', content)
        if not match:
            log.warning("Could not find ALL_JOBS in HTML")
            return []

        # Extract company names and stakeholders from ALL_JOBS entries
        # Pattern: "company": "..." and "stakeholders": [...]
        job_pattern = re.compile(
            r'"company"\s*:\s*"([^"]+)".*?"stakeholders"\s*:\s*\[(.*?)\]',
            re.DOTALL,
        )
        for job_m in job_pattern.finditer(content):
            company = job_m.group(1)
            stakeholders_json = job_m.group(2).strip()
            if not stakeholders_json:
                continue

            # Parse individual stakeholder objects
            sh_pattern = re.compile(r'\{([^}]+)\}')
            for sh_m in sh_pattern.finditer(stakeholders_json):
                sh_text = sh_m.group(1)
                name = _extract_field(sh_text, "name")
                title = _extract_field(sh_text, "title")
                email = _extract_field(sh_text, "email")
                linkedin = _extract_field(sh_text, "linkedin")

                if not name:
                    continue

                key = f"{name.lower().strip()}|{company.lower().strip()}"
                if key in seen:
                    continue
                seen.add(key)

                stakeholders.append({
                    "name": name,
                    "title": title or "",
                    "email": email or "",
                    "linkedin": linkedin or "",
                    "company": company,
                    "key": key,
                })
    except re.error as exc:
        log.error("Regex error parsing HTML: %s", exc)

    return stakeholders


def _extract_field(text, field):
    """Extract a JSON field value from a text snippet."""
    m = re.search(rf'"{field}"\s*:\s*"([^"]*)"', text)
    return m.group(1) if m else ""


def extract_companies_from_html(path):
    """Extract unique company names from ALL_JOBS."""
    companies = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        for m in re.finditer(r'"company"\s*:\s*"([^"]+)"', content):
            companies.add(m.group(1))
    except OSError as exc:
        log.warning("Could not read %s: %s", path, exc)
    return sorted(companies)


def _extract_linkedin_handle(linkedin_url):
    """Extract LinkedIn handle from a LinkedIn profile URL."""
    if not linkedin_url:
        return None
    # Handle both /in/ and /company/ URLs
    m = re.search(r'/(?:in|company)/([a-z0-9\-]+)', linkedin_url.lower())
    return m.group(1) if m else None


def _find_email_for_person(first_name, last_name, company_domain):
    """Use Hunter email-finder to find an email for a person at a company."""
    if not company_domain:
        return None

    try:
        resp = _hunter_request_with_retry(
            "GET",
            f"{HUNTER_BASE}/email-finder",
            params={
                "domain": company_domain,
                "first_name": first_name,
                "last_name": last_name,
                "api_key": HUNTER_API_KEY,
            },
            timeout=15,
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        log.debug("Email finder network error for %s %s at %s: %s", first_name, last_name, company_domain, exc)
        return None

    if resp.status_code == 200:
        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            log.debug("Email finder returned invalid JSON: %s", exc)
            return None

        person = data.get("data")
        if person and person.get("email"):
            return person.get("email")

    return None


def enrich_person(name, company, email=None, linkedin_url=None, company_domain=None):
    """Enrich a person via Hunter People Find API.

    Strategy:
    1. If email provided → use /people/find?email=...
    2. If linkedin_url provided → extract handle, use /people/find?linkedin_handle=...
    3. Otherwise → try email-finder with domain, then use that email
    """
    parts = name.strip().split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    # Try to find email if we don't have one
    if not email or "@" not in email:
        linkedin_handle = _extract_linkedin_handle(linkedin_url)
        if not linkedin_handle and company_domain:
            # Try email-finder
            email = _find_email_for_person(first_name, last_name, company_domain)

    # Call people/find with whatever we have
    params = {"api_key": HUNTER_API_KEY}

    if email and "@" in email:
        params["email"] = email
    elif linkedin_url:
        linkedin_handle = _extract_linkedin_handle(linkedin_url)
        if linkedin_handle:
            params["linkedin_handle"] = linkedin_handle
    else:
        # Can't enrich without email or linkedin_handle
        return None

    try:
        resp = _hunter_request_with_retry(
            "GET",
            f"{HUNTER_BASE}/people/find",
            params=params,
            timeout=15,
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        log.error("People find network error for %s @ %s: %s", name, company, exc)
        return None

    if resp.status_code == 200:
        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            log.error("People find returned invalid JSON for %s @ %s: %s", name, company, exc)
            return None

        person = data.get("data")
        if person:
            # Map Hunter response to Apollo-compatible format
            name_obj = person.get("name", {})
            given_name = name_obj.get("givenName", "")
            family_name = name_obj.get("familyName", "")
            full_name = name_obj.get("fullName", "")

            location = person.get("location", "")
            geo = person.get("geo", {}) or {}
            employment = person.get("employment", {}) or {}
            linkedin_obj = person.get("linkedin", {}) or {}

            # Build LinkedIn URL from handle if available
            linkedin_url_out = ""
            linkedin_handle = linkedin_obj.get("handle", "")
            if linkedin_handle:
                linkedin_url_out = f"https://linkedin.com/in/{linkedin_handle}"

            # Map verification status to email status
            verification = person.get("verification", {}) or {}
            email_status = verification.get("status", "unknown")

            photo_url = person.get("avatar", "")
            photo_data = _download_photo_b64(photo_url) if photo_url else ""

            return {
                "apolloId": f"hunter_{person.get('id', '')}",
                "email": person.get("email", ""),
                "emailStatus": email_status,
                "title": employment.get("title", ""),
                "linkedin_url": linkedin_url_out,
                "phone": person.get("phone", ""),
                "phoneType": "other" if person.get("phone") else "",
                "allPhones": [{"number": person.get("phone"), "type": "other"}] if person.get("phone") else [],
                "photoUrl": photo_url,
                "photoData": photo_data or "",
                "city": geo.get("city", ""),
                "country": geo.get("country", ""),
                "seniority": employment.get("seniority", ""),
                "departments": [],
                "headline": "",
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        else:
            return None
    else:
        log.debug("People find returned HTTP %d for %s @ %s", resp.status_code, name, company)
        return None


def enrich_organization(company_name, domain=None):
    """Enrich a company via Hunter Company Find API."""
    try:
        # Hunter prefers domain; try provided domain or guess common patterns
        if domain:
            search_domain = domain
        else:
            # Try common domain patterns as a guess
            clean = company_name.lower().strip().replace(" ", "")
            search_domain = f"{clean}.com"

        resp = _hunter_request_with_retry(
            "GET",
            f"{HUNTER_BASE}/companies/find",
            params={
                "domain": search_domain,
                "api_key": HUNTER_API_KEY,
            },
            timeout=15,
        )

        # If domain guess failed, don't retry with company name (Hunter requires domain)
        if resp.status_code != 200 and not domain:
            # Try domain without .com extension
            resp = _hunter_request_with_retry(
                "GET",
                f"{HUNTER_BASE}/companies/find",
                params={
                    "domain": company_name.lower().strip().replace(" ", ""),
                    "api_key": HUNTER_API_KEY,
                },
                timeout=15,
            )

    except (requests.Timeout, requests.ConnectionError) as exc:
        log.error("Company find network error for '%s': %s", company_name, exc)
        return None

    if resp.status_code == 200:
        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            log.error("Company find returned invalid JSON for '%s': %s", company_name, exc)
            return None

        org = data.get("data")
        if org:
            geo = org.get("geo", {}) or {}
            category = org.get("category", {}) or {}
            metrics = org.get("metrics", {}) or {}
            linkedin_obj = org.get("linkedin", {}) or {}

            # Build LinkedIn URL from handle if available
            linkedin_url = ""
            linkedin_handle = linkedin_obj.get("handle", "")
            if linkedin_handle:
                linkedin_url = f"https://linkedin.com/company/{linkedin_handle}"

            # Parse employee count (Hunter returns as string)
            employee_count = None
            emp_str = metrics.get("employees", "")
            if emp_str:
                try:
                    employee_count = int(emp_str)
                except (ValueError, TypeError):
                    pass

            techs = org.get("tech", []) or []
            techs = [t for t in techs[:15] if t]  # Take first 15, filter empty

            return {
                "apolloId": f"hunter_{org.get('id', '')}",
                "name": org.get("name", ""),
                "domain": org.get("domain", ""),
                "website": f"https://{org.get('domain', '')}" if org.get('domain') else "",
                "industry": category.get("industry", ""),
                "employeeCount": employee_count,
                "annualRevenue": None,
                "foundedYear": org.get("foundedYear"),
                "technologies": techs,
                "linkedinUrl": linkedin_url,
                "city": geo.get("city", ""),
                "country": geo.get("country", ""),
                "shortDescription": "",
                "logoUrl": org.get("logo", ""),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        else:
            return None
    else:
        log.debug("Company find returned HTTP %d for '%s'", resp.status_code, company_name)
        return None


def prune_apollo_data():
    """Archive contacts/orgs older than ARCHIVE_DAYS from apollo_data.json.

    Entries without a last_updated timestamp are stamped with today's date so
    they are preserved until they naturally age out in a future run.
    The archive file is cumulative — new archived entries are merged in so
    history is never lost.  The archive is never loaded during normal runs.
    """
    if not os.path.exists(OUTPUT_FILE):
        return

    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
    except Exception as e:
        log.warning("Prune: could not read %s: %s", OUTPUT_FILE, e)
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=ARCHIVE_DAYS)
    now_iso = datetime.now(timezone.utc).isoformat()

    def _is_stale(entry):
        ts = entry.get("last_updated")
        if not ts:
            return False  # will be stamped below; keep for now
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt < cutoff
        except Exception:
            return False

    contacts = data.get("contacts", {})
    orgs = data.get("organizations", {})

    # Stamp entries that are missing last_updated so they age out naturally later
    stamped = 0
    for entry in list(contacts.values()) + list(orgs.values()):
        if isinstance(entry, dict) and "last_updated" not in entry:
            entry["last_updated"] = now_iso
            stamped += 1

    stale_contacts = {k: v for k, v in contacts.items() if isinstance(v, dict) and _is_stale(v)}
    stale_orgs = {k: v for k, v in orgs.items() if isinstance(v, dict) and _is_stale(v)}

    if not stale_contacts and not stale_orgs:
        if stamped:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            log.info("Prune: stamped %d entries with last_updated (no archives needed yet)", stamped)
        else:
            log.info("Prune: nothing to archive (all entries are < %d days old)", ARCHIVE_DAYS)
        return

    # Load existing archive (cumulative)
    archive = {"contacts": {}, "organizations": {}}
    if os.path.exists(ARCHIVE_FILE):
        try:
            with open(ARCHIVE_FILE, "r") as f:
                archive = json.load(f)
        except Exception:
            pass

    archive.setdefault("contacts", {})
    archive.setdefault("organizations", {})
    archive["contacts"].update(stale_contacts)
    archive["organizations"].update(stale_orgs)
    archive["last_archived"] = now_iso

    with open(ARCHIVE_FILE, "w", encoding="utf-8") as f:
        json.dump(archive, f, indent=2, ensure_ascii=False)

    # Remove stale entries from active data
    for k in stale_contacts:
        del contacts[k]
    for k in stale_orgs:
        del orgs[k]

    if stamped:
        log.info("Prune: stamped %d entries with last_updated", stamped)

    log.info(
        "Prune: archived %d contacts, %d orgs (older than %d days) → %s",
        len(stale_contacts), len(stale_orgs), ARCHIVE_DAYS, ARCHIVE_FILE,
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    if not HUNTER_API_KEY:
        print("HUNTER_API_KEY not set, skipping enrichment")
        sys.exit(0)

    # Check workflow config
    wf_config = _load_workflow_config()
    if wf_config and not _is_node_enabled(wf_config, "enrichment"):
        print("Contact Enrichment node is DISABLED in workflow config — skipping")
        sys.exit(0)

    # Apply rate limit from workflow config if available
    enrichment_node = wf_config.get("nodes", {}).get("enrichment", {}) if wf_config else {}
    rate_limit = enrichment_node.get("rateLimit", 500)
    global REQUEST_DELAY
    REQUEST_DELAY = max(0.1, 60.0 / rate_limit)  # Convert calls/min to delay
    print(f"  Rate limit: {rate_limit} calls/min (delay: {REQUEST_DELAY:.2f}s)")

    print("Starting Hunter.io enrichment...")
    print(f"  API key: {HUNTER_API_KEY[:8]}...{HUNTER_API_KEY[-4:]}")

    # Load existing Hunter data to avoid re-enriching
    existing = {}
    existing_orgs = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                prev = json.load(f)
                existing = prev.get("contacts", {})
                existing_orgs = prev.get("organizations", {})
                print(f"  Loaded existing data: {len(existing)} contacts, {len(existing_orgs)} orgs")
        except json.JSONDecodeError as exc:
            log.warning("Existing %s is invalid JSON, starting fresh: %s", OUTPUT_FILE, exc)
        except OSError as exc:
            log.warning("Could not read %s, starting fresh: %s", OUTPUT_FILE, exc)

    # 1. Extract stakeholders and companies
    stakeholders = extract_stakeholders_from_html(DOCS_HTML)
    companies = extract_companies_from_html(DOCS_HTML)
    print(f"  Found {len(stakeholders)} stakeholders, {len(companies)} companies")

    # 2. Enrich contacts (skip already-enriched)
    contacts_enriched = {k: v for k, v in existing.items() if v.get("apolloId")}
    skipped_contacts = len(contacts_enriched)
    new_contacts = 0
    for i, sh in enumerate(stakeholders):
        key = sh["key"]
        if key in contacts_enriched:
            continue  # already enriched

        print(f"  [{i+1}/{len(stakeholders)}] Enriching: {sh['name']} @ {sh['company']}")

        # Try to get domain from company in existing orgs if available
        company_domain = None
        company_key = sh["company"].lower().strip()
        if company_key in existing_orgs:
            company_domain = existing_orgs[company_key].get("domain")

        result = enrich_person(
            sh["name"],
            sh["company"],
            email=sh.get("email"),
            linkedin_url=sh.get("linkedin"),
            company_domain=company_domain,
        )
        if result:
            contacts_enriched[key] = result
            new_contacts += 1
            print(f"    -> Found: {result.get('email', 'no email')} | {result.get('title', 'no title')}")
        else:
            # Store empty marker so we don't retry next time
            contacts_enriched[key] = {"apolloId": "", "_notFound": True}
            print(f"    -> Not found")

        time.sleep(REQUEST_DELAY)

    # 3. Enrich organizations (skip already-enriched)
    orgs_enriched = {k: v for k, v in existing_orgs.items() if v.get("apolloId")}
    skipped_orgs = len(orgs_enriched)
    new_orgs = 0
    for i, company in enumerate(companies):
        key = company.lower().strip()
        if key in orgs_enriched:
            continue  # already enriched

        print(f"  [{i+1}/{len(companies)}] Enriching org: {company}")
        result = enrich_organization(company)
        if result:
            orgs_enriched[key] = result
            new_orgs += 1
            print(f"    -> Found: {result.get('domain', '?')} | {result.get('employeeCount', '?')} employees")
        else:
            orgs_enriched[key] = {"apolloId": "", "_notFound": True}
            print(f"    -> Not found")

        time.sleep(REQUEST_DELAY)

    # 4. Write output
    total_contacts = sum(1 for v in contacts_enriched.values() if v.get("apolloId"))
    total_orgs = sum(1 for v in orgs_enriched.values() if v.get("apolloId"))

    output = {
        "provider": "hunter",
        "syncedAt": datetime.utcnow().isoformat() + "Z",
        "contacts": contacts_enriched,
        "organizations": orgs_enriched,
        "stats": {
            "totalStakeholders": len(stakeholders),
            "contactsEnriched": total_contacts,
            "newContactsThisRun": new_contacts,
            "totalCompanies": len(companies),
            "orgsEnriched": total_orgs,
            "newOrgsThisRun": new_orgs,
        },
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nHunter.io enrichment complete!")
    print(f"  Contacts enriched: {total_contacts} ({new_contacts} new)")
    print(f"  Organizations enriched: {total_orgs} ({new_orgs} new)")
    print(f"  Output: {OUTPUT_FILE}")

    # Prune stale entries to keep apollo_data.json lean
    log.info("Pruning entries older than %d days...", ARCHIVE_DAYS)
    prune_apollo_data()


if __name__ == "__main__":
    main()
