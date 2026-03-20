"""
Apollo.io Enrichment - Enriches stakeholder contacts and companies with Apollo data.

Reads stakeholders from ALL_JOBS in docs/index.html, calls Apollo People Match
and Organization Enrichment APIs, writes results to apollo_data.json.

Environment variables:
  APOLLO_API_KEY - Apollo.io API key (from Settings > Integrations > API Keys)
"""

import os
import sys
import json
import time
import re
import base64
import requests
from datetime import datetime

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "").strip()
APOLLO_WEBHOOK_URL = os.environ.get("APOLLO_WEBHOOK_URL", "").strip()
APOLLO_BASE = "https://api.apollo.io/api/v1"
OUTPUT_FILE = "apollo_data.json"
DOCS_HTML = "docs/index.html"

# Rate limiting: Apollo allows 600 calls/hour ≈ 10/min
REQUEST_DELAY = 0.25  # seconds between API calls

# LinkedIn default avatar URL — not a real profile photo, skip it
_LINKEDIN_DEFAULT_AVATAR = "https://static.licdn.com/aero-v1/sc/h/9c8pery4andzj6ohjkjp54ma2"


def _download_photo_b64(url):
    """Download a profile photo URL and return a base64 data URI, or None on failure."""
    if not url or url == _LINKEDIN_DEFAULT_AVATAR:
        return None
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Referer": ""},
            timeout=10,
            allow_redirects=True,
        )
        if r.status_code != 200:
            return None
        ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        if not ct.startswith("image/"):
            return None
        b64 = base64.b64encode(r.content).decode("ascii")
        return f"data:{ct};base64,{b64}"
    except Exception:
        return None

# ── Workflow Config ───────────────────────────────────────────────────────
WORKFLOW_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflow_config.json")

def _load_workflow_config():
    """Load workflow_config.json if it exists."""
    if os.path.exists(WORKFLOW_CONFIG_PATH):
        try:
            with open(WORKFLOW_CONFIG_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _is_node_enabled(config, node_id):
    """Check if a workflow node is enabled. Defaults to True if not configured."""
    return config.get("nodes", {}).get(node_id, {}).get("enabled", True)


def apollo_post_headers():
    """Headers for POST requests (people match)."""
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "x-api-key": APOLLO_API_KEY,
    }


def apollo_get_headers():
    """Headers for GET requests (org enrichment) — no Content-Type."""
    return {
        "Cache-Control": "no-cache",
        "x-api-key": APOLLO_API_KEY,
    }


def extract_stakeholders_from_html(path):
    """Extract stakeholders from ALL_JOBS in the HTML file.
    Returns list of {name, title, email, company, linkedin} dicts."""
    stakeholders = []
    seen = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        # Find ALL_JOBS array (may use let or const)
        match = re.search(r'(?:let|const|var)\s+ALL_JOBS\s*=\s*\[', content)
        if not match:
            print("Warning: Could not find ALL_JOBS in HTML")
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

    except Exception as e:
        print(f"Warning: Could not read {path}: {e}")

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
    except Exception as e:
        print(f"Warning: Could not read {path}: {e}")
    return sorted(companies)


def enrich_person(name, company, email=None, linkedin_url=None, _retried=False):
    """Enrich a person via Apollo People Match API."""
    parts = name.strip().split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    # Enable phone reveal when webhook is configured (async delivery via webhook)
    has_webhook = bool(APOLLO_WEBHOOK_URL)
    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "organization_name": company,
        "reveal_personal_emails": False,
        "reveal_phone_number": has_webhook,
    }
    if has_webhook:
        payload["webhook_url"] = APOLLO_WEBHOOK_URL

    if email and "@" in email:
        payload["email"] = email
    if linkedin_url and linkedin_url.startswith("http"):
        payload["linkedin_url"] = linkedin_url

    try:
        resp = requests.post(
            f"{APOLLO_BASE}/people/match",
            headers=apollo_post_headers(),
            json=payload,
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            person = data.get("person")
            if person:
                # Extract phone numbers — prefer mobile for WhatsApp
                phones = person.get("phone_numbers") or []
                mobile_phone = ""
                primary_phone = ""
                all_phones = []
                for ph in phones:
                    num = ph.get("sanitized_number", "")
                    ph_type = ph.get("type", "").lower()
                    if num:
                        all_phones.append({"number": num, "type": ph_type})
                        if ph_type == "mobile" and not mobile_phone:
                            mobile_phone = num
                        if not primary_phone:
                            primary_phone = num

                photo_url = person.get("photo_url", "")
                photo_data = _download_photo_b64(photo_url)
                return {
                    "apolloId": person.get("id", ""),
                    "email": person.get("email", ""),
                    "emailStatus": person.get("email_status", ""),
                    "title": person.get("title", ""),
                    "linkedin_url": person.get("linkedin_url", ""),
                    "phone": mobile_phone or primary_phone,
                    "phoneType": "mobile" if mobile_phone else ("other" if primary_phone else ""),
                    "allPhones": all_phones,
                    "photoUrl": photo_url,
                    "photoData": photo_data or "",
                    "city": person.get("city", ""),
                    "country": person.get("country", ""),
                    "seniority": person.get("seniority", ""),
                    "departments": person.get("departments", []),
                    "headline": person.get("headline", ""),
                }
            else:
                return None
        elif resp.status_code == 429 and not _retried:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
            return enrich_person(name, company, email, linkedin_url, _retried=True)
        else:
            try:
                err_body = resp.text[:200]
            except Exception:
                err_body = ""
            print(f"    People match failed: HTTP {resp.status_code} {err_body}")
            return None
    except Exception as e:
        print(f"    People match error: {e}")
        return None


def enrich_organization(company_name, domain=None, _retried=False):
    """Enrich a company via Apollo Organization Enrichment API.
    Tries domain first (preferred), falls back to organization_name."""
    try:
        # Apollo prefers domain for org enrichment; fall back to name
        params = {}
        if domain:
            params["domain"] = domain
        else:
            # Try common domain patterns as a guess
            clean = company_name.lower().strip().replace(" ", "")
            params["domain"] = f"{clean}.com"

        resp = requests.get(
            f"{APOLLO_BASE}/organizations/enrich",
            headers=apollo_get_headers(),
            params=params,
            timeout=15,
        )

        # If domain guess failed, retry with organization_name
        if resp.status_code != 200 and not domain:
            resp = requests.get(
                f"{APOLLO_BASE}/organizations/enrich",
                headers=apollo_get_headers(),
                params={"domain": company_name.lower().strip()},
                timeout=15,
            )

        if resp.status_code == 200:
            data = resp.json()
            org = data.get("organization")
            if org:
                techs = []
                for t in (org.get("current_technologies") or [])[:15]:
                    techs.append(t.get("name", ""))

                return {
                    "apolloId": org.get("id", ""),
                    "name": org.get("name", ""),
                    "domain": org.get("primary_domain", ""),
                    "website": org.get("website_url", ""),
                    "industry": org.get("industry", ""),
                    "employeeCount": org.get("estimated_num_employees"),
                    "annualRevenue": org.get("annual_revenue"),
                    "foundedYear": org.get("founded_year"),
                    "technologies": techs,
                    "linkedinUrl": org.get("linkedin_url", ""),
                    "city": org.get("city", ""),
                    "country": org.get("country", ""),
                    "shortDescription": org.get("short_description", ""),
                    "logoUrl": org.get("logo_url", ""),
                }
            else:
                return None
        elif resp.status_code == 429 and not _retried:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
            return enrich_organization(company_name, domain, _retried=True)
        else:
            try:
                err_body = resp.text[:200]
            except Exception:
                err_body = ""
            print(f"    Org enrichment failed: HTTP {resp.status_code} {err_body}")
            return None
    except Exception as e:
        print(f"    Org enrichment error: {e}")
        return None


def main():
    if not APOLLO_API_KEY:
        print("APOLLO_API_KEY not set, skipping enrichment")
        sys.exit(0)

    # Check workflow config
    wf_config = _load_workflow_config()
    if wf_config and not _is_node_enabled(wf_config, "enrichment"):
        print("Contact Enrichment node is DISABLED in workflow config — skipping")
        sys.exit(0)

    # Apply rate limit from workflow config if available
    enrichment_node = wf_config.get("nodes", {}).get("enrichment", {}) if wf_config else {}
    rate_limit = enrichment_node.get("rateLimit", 600)
    global REQUEST_DELAY
    REQUEST_DELAY = max(0.1, 3600.0 / rate_limit)  # Convert calls/hour to delay
    print(f"  Rate limit: {rate_limit} calls/hour (delay: {REQUEST_DELAY:.2f}s)")

    print("Starting Apollo.io enrichment...")
    print(f"  API key: {APOLLO_API_KEY[:8]}...{APOLLO_API_KEY[-4:]}")
    if APOLLO_WEBHOOK_URL:
        print(f"  Phone webhook: {APOLLO_WEBHOOK_URL[:40]}...")
    else:
        print("  Phone webhook: not configured (phone numbers will be limited)")

    # Load existing Apollo data to avoid re-enriching
    existing = {}
    existing_orgs = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                prev = json.load(f)
                existing = prev.get("contacts", {})
                existing_orgs = prev.get("organizations", {})
                print(f"  Loaded existing data: {len(existing)} contacts, {len(existing_orgs)} orgs")
        except Exception:
            pass

    # 1. Extract stakeholders and companies
    stakeholders = extract_stakeholders_from_html(DOCS_HTML)
    companies = extract_companies_from_html(DOCS_HTML)
    print(f"  Found {len(stakeholders)} stakeholders, {len(companies)} companies")

    # 2. Enrich contacts (skip already-enriched, retry not-found from previous runs)
    #    If webhook is available, re-enrich contacts missing phone numbers
    has_webhook = bool(APOLLO_WEBHOOK_URL)
    contacts_enriched = {k: v for k, v in existing.items() if v.get("apolloId")}
    skipped_contacts = len(contacts_enriched)
    phone_re_enrich = 0
    new_contacts = 0
    for i, sh in enumerate(stakeholders):
        key = sh["key"]
        if key in contacts_enriched:
            # If webhook available and contact has no phone, re-enrich for phone
            if has_webhook and not contacts_enriched[key].get("phone"):
                phone_re_enrich += 1
                print(f"  [{i+1}/{len(stakeholders)}] Re-enriching for phone: {sh['name']} @ {sh['company']}")
            else:
                continue  # already enriched with phone or no webhook

        is_re_enrich = key in contacts_enriched and contacts_enriched[key].get("apolloId")
        if not is_re_enrich:
            print(f"  [{i+1}/{len(stakeholders)}] Enriching: {sh['name']} @ {sh['company']}")
        result = enrich_person(
            sh["name"],
            sh["company"],
            email=sh.get("email"),
            linkedin_url=sh.get("linkedin"),
        )
        if result:
            if is_re_enrich:
                # Merge: keep existing data, add/update phone fields
                existing_entry = contacts_enriched[key]
                for field in ("phone", "phoneType", "allPhones"):
                    if result.get(field):
                        existing_entry[field] = result[field]
                print(f"    -> Phone request sent via webhook")
            else:
                contacts_enriched[key] = result
                new_contacts += 1
                print(f"    -> Found: {result.get('email', 'no email')} | {result.get('title', 'no title')}")
        else:
            if not is_re_enrich:
                # Store empty marker so we don't retry next time
                contacts_enriched[key] = {"apolloId": "", "_notFound": True}
                print(f"    -> Not found")
            else:
                print(f"    -> Re-enrich failed, keeping existing data")

        time.sleep(REQUEST_DELAY)

    # 3. Enrich organizations (skip already-enriched, retry not-found)
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
        "provider": "apollo",
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

    print(f"\nApollo enrichment complete!")
    print(f"  Contacts enriched: {total_contacts} ({new_contacts} new)")
    if phone_re_enrich > 0:
        print(f"  Phone re-enrichments: {phone_re_enrich} (via webhook)")
    print(f"  Organizations enriched: {total_orgs} ({new_orgs} new)")
    print(f"  Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
