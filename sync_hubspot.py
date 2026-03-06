"""
HubSpot CRM Sync - Fetches company/deal/contact data and writes crm_data.json

This script is provider-agnostic in design. To add another CRM:
1. Add a new sync_<provider>() function
2. Output the same JSON schema
3. Update the workflow to call it

Environment variables:
  HUBSPOT_TOKEN     - HubSpot Private App or Personal Access Key
  HUBSPOT_PORTAL_ID - HubSpot portal/hub ID (for building UI links)
"""

import os
import sys
import json
import time
import re
import requests
from datetime import datetime

HUBSPOT_TOKEN_RAW = os.environ.get("HUBSPOT_TOKEN", "").strip()
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "").strip()
BASE_URL = "https://api.hubapi.com"


def _decode_token(raw):
    """HubSpot tokens normally start with 'pat-' or 'eu1-' or 'na1-'.
    If the value looks base64-encoded, decode it first."""
    import base64 as b64mod
    # If it already looks like a raw token, use it as-is
    if raw.startswith("pat-") or raw.startswith("eu1-") or raw.startswith("na1-"):
        return raw
    # Try base64 decode
    try:
        decoded = b64mod.b64decode(raw).decode("utf-8", errors="replace")
        # Strip any leading non-printable / protobuf framing bytes
        cleaned = decoded.lstrip("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f $\"")
        if cleaned.startswith("eu1-") or cleaned.startswith("na1-") or cleaned.startswith("pat-"):
            print(f"  Token was base64-encoded, decoded to {len(cleaned)} chars starting with {cleaned[:10]}...")
            return cleaned
    except Exception:
        pass
    return raw


HUBSPOT_TOKEN = _decode_token(HUBSPOT_TOKEN_RAW)
OUTPUT_FILE = "crm_data.json"

# Read existing job listings to know which companies to look up
DOCS_HTML = "docs/index.html"


def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


def _detect_region():
    """Auto-detect region by trying a simple API call on both US and EU endpoints."""
    global BASE_URL
    print(f"  Token length: {len(HUBSPOT_TOKEN)}, starts with: {HUBSPOT_TOKEN[:10]}..., ends with: ...{HUBSPOT_TOKEN[-6:]}")
    for base in ["https://api.hubapi.com", "https://api-eu1.hubapi.com"]:
        try:
            print(f"  Trying {base}...")
            resp = requests.get(
                f"{base}/crm/v3/pipelines/deals",
                headers=hubspot_headers(),
                timeout=10,
            )
            print(f"    -> HTTP {resp.status_code}")
            if resp.status_code == 200:
                BASE_URL = base
                print(f"  Detected API region: {base}")
                return
        except Exception as e:
            print(f"    -> Error: {e}")
            continue
    # Default to EU since the account is EU
    BASE_URL = "https://api-eu1.hubapi.com"
    print(f"  WARNING: Both endpoints returned errors. Defaulting to: {BASE_URL}")
    print(f"  This usually means the HUBSPOT_TOKEN is invalid or expired.")


def extract_companies_from_html(path):
    """Extract unique company names from ALL_JOBS in the HTML file."""
    companies = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        # Match "company": "..." patterns in the ALL_JOBS array
        for m in re.finditer(r'"company"\s*:\s*"([^"]+)"', content):
            companies.add(m.group(1))
    except Exception as e:
        print(f"Warning: Could not read {path}: {e}")
    return sorted(companies)


def search_company(name):
    """Search HubSpot for a company by name."""
    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/companies/search",
        headers=hubspot_headers(),
        json={
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "name",
                            "operator": "CONTAINS_TOKEN",
                            "value": name,
                        }
                    ]
                }
            ],
            "properties": [
                "name",
                "domain",
                "industry",
                "lifecyclestage",
                "num_associated_deals",
                "num_associated_contacts",
            ],
            "limit": 5,
        },
    )
    if resp.status_code != 200:
        print(f"  Warning: Search failed for '{name}': HTTP {resp.status_code}")
        return []
    return resp.json().get("results", [])


def get_deals_for_company(company_id):
    """Get deals associated with a company."""
    # Get associations
    resp = requests.get(
        f"{BASE_URL}/crm/v4/objects/companies/{company_id}/associations/deals",
        headers=hubspot_headers(),
    )
    if resp.status_code != 200:
        return []

    deal_ids = [r["toObjectId"] for r in resp.json().get("results", [])]
    if not deal_ids:
        return []

    # Batch read deal details (max 100)
    batch_resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/deals/batch/read",
        headers=hubspot_headers(),
        json={
            "inputs": [{"id": str(did)} for did in deal_ids[:20]],
            "properties": [
                "dealname",
                "dealstage",
                "pipeline",
                "amount",
                "closedate",
                "hubspot_owner_id",
            ],
        },
    )
    if batch_resp.status_code != 200:
        return []
    return batch_resp.json().get("results", [])


def get_contacts_for_company(company_id):
    """Get contacts associated with a company."""
    resp = requests.get(
        f"{BASE_URL}/crm/v4/objects/companies/{company_id}/associations/contacts",
        headers=hubspot_headers(),
    )
    if resp.status_code != 200:
        return []

    contact_ids = [r["toObjectId"] for r in resp.json().get("results", [])]
    if not contact_ids:
        return []

    batch_resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/contacts/batch/read",
        headers=hubspot_headers(),
        json={
            "inputs": [{"id": str(cid)} for cid in contact_ids[:20]],
            "properties": [
                "firstname",
                "lastname",
                "email",
                "jobtitle",
                "lifecyclestage",
            ],
        },
    )
    if batch_resp.status_code != 200:
        return []
    return batch_resp.json().get("results", [])


def get_deal_stages():
    """Fetch all pipelines and deal stages."""
    resp = requests.get(
        f"{BASE_URL}/crm/v3/pipelines/deals", headers=hubspot_headers()
    )
    if resp.status_code != 200:
        print(f"Warning: Could not fetch pipelines: HTTP {resp.status_code}")
        return {}

    stage_map = {}
    colors = [
        "#00CA72", "#FFB100", "#32BBD7", "#E44258", "#9B59B6",
        "#3498DB", "#E67E22", "#1ABC9C", "#95A5A6", "#2ECC71",
    ]
    color_idx = 0
    for pipeline in resp.json().get("results", []):
        for stage in pipeline.get("stages", []):
            stage_map[stage["id"]] = {
                "label": stage["label"],
                "pipeline": pipeline["label"],
                "color": colors[color_idx % len(colors)],
            }
            color_idx += 1
    return stage_map


def best_match(company_name, results):
    """Pick the best HubSpot company match for a given name."""
    if not results:
        return None
    name_lower = company_name.lower().strip()
    # Prefer exact match
    for r in results:
        if r["properties"].get("name", "").lower().strip() == name_lower:
            return r
    # Otherwise first result
    return results[0]


def main():
    if not HUBSPOT_TOKEN:
        print("HUBSPOT_TOKEN not set, skipping sync")
        sys.exit(0)

    print("Starting HubSpot CRM sync...")
    print("Detecting API region...")
    _detect_region()

    # 1. Get deal stages
    print("Fetching deal stages...")
    deal_stage_map = get_deal_stages()
    print(f"  Found {len(deal_stage_map)} deal stages")

    # 2. Extract companies from job listings
    companies = extract_companies_from_html(DOCS_HTML)
    print(f"Found {len(companies)} unique companies in job listings")

    # 3. Load existing CRM data (to preserve cache)
    existing_data = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                existing_data = json.load(f)
        except Exception:
            pass

    # 4. Look up each company
    crm_companies = {}
    for i, company in enumerate(companies):
        key = company.lower().strip()
        print(f"  [{i+1}/{len(companies)}] Looking up: {company}")

        try:
            results = search_company(company)
            match = best_match(company, results)

            if match:
                company_id = match["id"]
                props = match.get("properties", {})

                deals_raw = get_deals_for_company(company_id)
                contacts_raw = get_contacts_for_company(company_id)

                deals = []
                for d in deals_raw:
                    dp = d.get("properties", {})
                    stage_id = dp.get("dealstage", "")
                    stage_info = deal_stage_map.get(stage_id, {})
                    deals.append({
                        "id": d["id"],
                        "name": dp.get("dealname", ""),
                        "stage": stage_id,
                        "stageName": stage_info.get("label", stage_id),
                        "stageColor": stage_info.get("color", "#999"),
                        "pipeline": dp.get("pipeline", ""),
                        "pipelineName": deal_stage_map.get(dp.get("dealstage", ""), {}).get("pipeline", ""),
                        "amount": dp.get("amount", ""),
                        "closedate": dp.get("closedate", ""),
                    })

                contacts = []
                for c in contacts_raw:
                    cp = c.get("properties", {})
                    contacts.append({
                        "id": c["id"],
                        "name": f'{cp.get("firstname", "")} {cp.get("lastname", "")}'.strip(),
                        "email": cp.get("email", ""),
                        "title": cp.get("jobtitle", ""),
                    })

                crm_companies[key] = {
                    "companyId": company_id,
                    "companyName": props.get("name", company),
                    "domain": props.get("domain", ""),
                    "industry": props.get("industry", ""),
                    "lifecyclestage": props.get("lifecyclestage", ""),
                    "deals": deals,
                    "contacts": contacts,
                }
            else:
                crm_companies[key] = {
                    "companyId": None,
                    "companyName": company,
                    "deals": [],
                    "contacts": [],
                }

        except Exception as e:
            print(f"    Error: {e}")
            crm_companies[key] = {
                "companyId": None,
                "companyName": company,
                "deals": [],
                "contacts": [],
            }

        # Rate limiting: HubSpot allows 100 requests per 10 seconds
        if (i + 1) % 10 == 0:
            time.sleep(1)

    # 5. Write output
    output = {
        "provider": "hubspot",
        "portalId": HUBSPOT_PORTAL_ID,
        "syncedAt": datetime.utcnow().isoformat() + "Z",
        "dealStageMap": deal_stage_map,
        "companies": crm_companies,
        "stats": {
            "totalCompanies": len(companies),
            "matchedInCrm": sum(1 for v in crm_companies.values() if v.get("companyId")),
            "withDeals": sum(1 for v in crm_companies.values() if v.get("deals")),
            "withContacts": sum(1 for v in crm_companies.values() if v.get("contacts")),
        },
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nSync complete!")
    print(f"  Companies checked: {output['stats']['totalCompanies']}")
    print(f"  Matched in CRM: {output['stats']['matchedInCrm']}")
    print(f"  With deals: {output['stats']['withDeals']}")
    print(f"  With contacts: {output['stats']['withContacts']}")
    print(f"  Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
