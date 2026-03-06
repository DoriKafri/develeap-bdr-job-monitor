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
import requests
from datetime import datetime

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "").strip()
APOLLO_BASE = "https://api.apollo.io/api/v1"
OUTPUT_FILE = "apollo_data.json"
DOCS_HTML = "docs/index.html"

# Rate limiting: Apollo allows 600 calls/hour ≈ 10/min
REQUEST_DELAY = 0.25  # seconds between API calls


def apollo_headers():
    return {
        "Content-Type": "application/json",
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

        # Find ALL_JOBS array
        match = re.search(r'const\s+ALL_JOBS\s*=\s*\[', content)
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


def enrich_person(name, company, email=None, linkedin_url=None):
    """Enrich a person via Apollo People Match API."""
    parts = name.strip().split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "organization_name": company,
        "reveal_personal_emails": False,
        "reveal_phone_number": False,
    }

    if email and "@" in email:
        payload["email"] = email
    if linkedin_url and linkedin_url.startswith("http"):
        payload["linkedin_url"] = linkedin_url

    try:
        resp = requests.post(
            f"{APOLLO_BASE}/people/match",
            headers=apollo_headers(),
            json=payload,
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            person = data.get("person")
            if person:
                return {
                    "apolloId": person.get("id", ""),
                    "email": person.get("email", ""),
                    "emailStatus": person.get("email_status", ""),
                    "title": person.get("title", ""),
                    "linkedin_url": person.get("linkedin_url", ""),
                    "phone": (person.get("phone_numbers") or [{}])[0].get("sanitized_number", "") if person.get("phone_numbers") else "",
                    "photoUrl": person.get("photo_url", ""),
                    "city": person.get("city", ""),
                    "country": person.get("country", ""),
                    "seniority": person.get("seniority", ""),
                    "departments": person.get("departments", []),
                    "headline": person.get("headline", ""),
                }
            else:
                return None
        elif resp.status_code == 429:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
            return enrich_person(name, company, email, linkedin_url)  # retry once
        else:
            print(f"    People match failed: HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"    People match error: {e}")
        return None


def enrich_organization(company_name):
    """Enrich a company via Apollo Organization Enrichment API."""
    try:
        resp = requests.get(
            f"{APOLLO_BASE}/organizations/enrich",
            headers=apollo_headers(),
            params={"organization_name": company_name},
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
        elif resp.status_code == 429:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
            return enrich_organization(company_name)  # retry once
        else:
            print(f"    Org enrichment failed: HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"    Org enrichment error: {e}")
        return None


def main():
    if not APOLLO_API_KEY:
        print("APOLLO_API_KEY not set, skipping enrichment")
        sys.exit(0)

    print("Starting Apollo.io enrichment...")
    print(f"  API key: {APOLLO_API_KEY[:8]}...{APOLLO_API_KEY[-4:]}")

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

    # 2. Enrich contacts
    contacts_enriched = dict(existing)  # preserve existing
    new_contacts = 0
    for i, sh in enumerate(stakeholders):
        key = sh["key"]
        if key in contacts_enriched and contacts_enriched[key].get("apolloId"):
            continue  # already enriched

        print(f"  [{i+1}/{len(stakeholders)}] Enriching: {sh['name']} @ {sh['company']}")
        result = enrich_person(
            sh["name"],
            sh["company"],
            email=sh.get("email"),
            linkedin_url=sh.get("linkedin"),
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

    # 3. Enrich organizations
    orgs_enriched = dict(existing_orgs)
    new_orgs = 0
    for i, company in enumerate(companies):
        key = company.lower().strip()
        if key in orgs_enriched and orgs_enriched[key].get("apolloId"):
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
    print(f"  Organizations enriched: {total_orgs} ({new_orgs} new)")
    print(f"  Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
