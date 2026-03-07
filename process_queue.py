"""
BDR Queue Processor — Server-side automation for outreach queue

Handles:
1. HubSpot CRM: Create companies, contacts, deals for outreach targets
2. HubSpot Email Sequences: Enroll contacts, check for replies
3. SOS Schedule Management: Calculate next send dates, mark items ready
4. Opportunity Detection: AI analysis of replies for buying signals
5. Queue Execution Logging: Track rate limits, errors, stats

Environment variables:
  HUBSPOT_TOKEN       - HubSpot Private App / PAT
  HUBSPOT_PORTAL_ID   - HubSpot portal ID
  ANTHROPIC_API_KEY   - For opportunity detection via Claude API
"""

import os
import sys
import json
import time
import re
import requests
from datetime import datetime, timezone, timedelta

# ── Config ─────────────────────────────────────────────────────────────────

HUBSPOT_TOKEN_RAW = os.environ.get("HUBSPOT_TOKEN", "").strip()
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "").strip()
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
BASE_URL = "https://api.hubapi.com"

QUEUE_FILE = "linkedin_queue.json"
EXEC_LOG_FILE = "queue_execution_log.json"
CRM_DATA_FILE = "crm_data.json"
DOCS_HTML = "docs/index.html"
WORKFLOW_CONFIG_PATH = "workflow_config.json"
WOLFPACK_FILE = "wolfpack_campaigns.json"

# Rate limits
MAX_CRM_CREATES_PER_RUN = 20
MAX_EMAIL_ENROLLMENTS_PER_RUN = 10
HUBSPOT_RATE_DELAY = 0.15  # seconds between API calls

# ── Helpers ────────────────────────────────────────────────────────────────

def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default=None):
    if default is None:
        default = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"  Warning: Could not read {path}: {e}")
    return default


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {path}")


def _load_workflow_config():
    return _load_json(WORKFLOW_CONFIG_PATH)


def _is_node_enabled(config, node_id):
    return config.get("nodes", {}).get(node_id, {}).get("enabled", True)


def _decode_token(raw):
    """Handle HubSpot PAT formats."""
    if raw.startswith("pat-"):
        return raw
    if not raw:
        return raw
    print(f"  WARNING: Token does not start with 'pat-'. Using as-is.")
    return raw


def _detect_region():
    """Auto-detect HubSpot API region."""
    global BASE_URL
    for base in ["https://api.hubapi.com", "https://api-eu1.hubapi.com"]:
        try:
            resp = requests.get(
                f"{base}/crm/v3/pipelines/deals",
                headers=hubspot_headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                BASE_URL = base
                print(f"  HubSpot API region: {base}")
                return
        except Exception:
            continue
    BASE_URL = "https://api-eu1.hubapi.com"
    print(f"  WARNING: Defaulting to EU endpoint")


HUBSPOT_TOKEN = _decode_token(HUBSPOT_TOKEN_RAW)


def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


# ── HubSpot CRM Create Functions ──────────────────────────────────────────

def create_hubspot_company(company_name, domain=""):
    """Create a new company in HubSpot. Returns company ID or None."""
    payload = {
        "properties": {
            "name": company_name,
            "lifecyclestage": "opportunity",
        }
    }
    if domain:
        payload["properties"]["domain"] = domain

    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/companies",
        headers=hubspot_headers(),
        json=payload,
    )
    if resp.status_code in (200, 201):
        company_id = resp.json().get("id")
        print(f"    Created company: {company_name} (ID: {company_id})")
        return company_id
    else:
        print(f"    Failed to create company {company_name}: HTTP {resp.status_code} - {resp.text[:200]}")
        return None


def create_hubspot_contact(first_name, last_name, email="", title="", company_id=None, phone=""):
    """Create a new contact in HubSpot. Returns contact ID or None."""
    props = {
        "firstname": first_name,
        "lastname": last_name,
        "lifecyclestage": "lead",
    }
    if email:
        props["email"] = email
    if title:
        props["jobtitle"] = title
    if phone:
        props["phone"] = phone

    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/contacts",
        headers=hubspot_headers(),
        json={"properties": props},
    )
    if resp.status_code in (200, 201):
        contact_id = resp.json().get("id")
        print(f"    Created contact: {first_name} {last_name} (ID: {contact_id})")

        # Associate with company if provided
        if company_id and contact_id:
            _associate_contact_to_company(contact_id, company_id)

        return contact_id
    elif resp.status_code == 409:
        # Contact already exists (duplicate email) — try to find existing
        print(f"    Contact already exists: {first_name} {last_name} ({email})")
        existing = _find_contact_by_email(email) if email else None
        return existing
    else:
        print(f"    Failed to create contact: HTTP {resp.status_code} - {resp.text[:200]}")
        return None


def _associate_contact_to_company(contact_id, company_id):
    """Associate a contact with a company in HubSpot."""
    resp = requests.put(
        f"{BASE_URL}/crm/v4/objects/contacts/{contact_id}/associations/companies/{company_id}",
        headers=hubspot_headers(),
        json=[{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 1}],
    )
    if resp.status_code in (200, 201):
        print(f"    Associated contact {contact_id} with company {company_id}")
    else:
        print(f"    Warning: Association failed: HTTP {resp.status_code}")


def _find_contact_by_email(email):
    """Find existing contact by email. Returns contact ID or None."""
    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/contacts/search",
        headers=hubspot_headers(),
        json={
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email,
                }]
            }],
            "properties": ["firstname", "lastname", "email"],
            "limit": 1,
        },
    )
    if resp.status_code == 200:
        results = resp.json().get("results", [])
        if results:
            return results[0]["id"]
    return None


def create_hubspot_deal(company_name, contact_name, job_title, company_id=None, contact_id=None):
    """Create a deal in HubSpot for an outreach opportunity."""
    deal_name = f"BDR: {company_name} — {job_title}"
    props = {
        "dealname": deal_name,
        "pipeline": "default",
        "dealstage": "appointmentscheduled",  # Initial stage
        "dealtype": "newbusiness",
    }

    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/deals",
        headers=hubspot_headers(),
        json={"properties": props},
    )
    if resp.status_code in (200, 201):
        deal_id = resp.json().get("id")
        print(f"    Created deal: {deal_name} (ID: {deal_id})")

        # Associate with company and contact
        if company_id:
            requests.put(
                f"{BASE_URL}/crm/v4/objects/deals/{deal_id}/associations/companies/{company_id}",
                headers=hubspot_headers(),
                json=[{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 341}],
            )
        if contact_id:
            requests.put(
                f"{BASE_URL}/crm/v4/objects/deals/{deal_id}/associations/contacts/{contact_id}",
                headers=hubspot_headers(),
                json=[{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}],
            )
        return deal_id
    else:
        print(f"    Failed to create deal: HTTP {resp.status_code} - {resp.text[:200]}")
        return None


def search_hubspot_company(name):
    """Search for existing company. Returns company ID or None."""
    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/companies/search",
        headers=hubspot_headers(),
        json={
            "filterGroups": [{
                "filters": [{
                    "propertyName": "name",
                    "operator": "CONTAINS_TOKEN",
                    "value": name,
                }]
            }],
            "properties": ["name", "domain"],
            "limit": 3,
        },
    )
    if resp.status_code == 200:
        results = resp.json().get("results", [])
        # Prefer exact match
        name_lower = name.lower().strip()
        for r in results:
            if r["properties"].get("name", "").lower().strip() == name_lower:
                return r["id"]
        if results:
            return results[0]["id"]
    return None


# ── HubSpot Email Sequence Functions ──────────────────────────────────────

def get_hubspot_sequences():
    """List available email sequences. Returns list of {id, name}."""
    resp = requests.get(
        f"{BASE_URL}/automation/v3/sequences",
        headers=hubspot_headers(),
    )
    if resp.status_code == 200:
        seqs = resp.json().get("results", [])
        return [{"id": s["id"], "name": s.get("name", "")} for s in seqs]
    print(f"  Warning: Could not fetch sequences: HTTP {resp.status_code}")
    return []


def enroll_in_sequence(contact_id, sequence_id, sender_email=""):
    """Enroll a contact in a HubSpot email sequence."""
    payload = {
        "contactId": contact_id,
        "sequenceId": sequence_id,
    }
    if sender_email:
        payload["senderEmail"] = sender_email

    resp = requests.post(
        f"{BASE_URL}/automation/v3/sequences/{sequence_id}/enrollments",
        headers=hubspot_headers(),
        json=payload,
    )
    if resp.status_code in (200, 201, 204):
        print(f"    Enrolled contact {contact_id} in sequence {sequence_id}")
        return True
    else:
        print(f"    Failed to enroll: HTTP {resp.status_code} - {resp.text[:200]}")
        return False


def check_contact_engagement(contact_id):
    """Check if a contact has replied to emails. Returns engagement data."""
    resp = requests.get(
        f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}",
        headers=hubspot_headers(),
        params={
            "properties": "hs_email_last_reply_date,hs_email_replied,hs_sales_email_last_replied,notes_last_updated"
        }
    )
    if resp.status_code == 200:
        props = resp.json().get("properties", {})
        return {
            "lastReplyDate": props.get("hs_email_last_reply_date") or props.get("hs_sales_email_last_replied"),
            "hasReplied": bool(props.get("hs_email_replied") or props.get("hs_email_last_reply_date")),
        }
    return {"lastReplyDate": None, "hasReplied": False}


# ── Opportunity Detection ─────────────────────────────────────────────────

def detect_opportunity(message_text, contact_name, company, expertise="DevOps"):
    """Use Claude API to detect buying signals in a message.
    Returns: {type, confidence, summary, requiresHumanReview}
    """
    if not ANTHROPIC_API_KEY:
        return None
    if not message_text or len(message_text.strip()) < 10:
        return None

    prompt = f"""Analyze this message from {contact_name} at {company} in the context of selling {expertise} consulting services.

Message:
"{message_text}"

Classify it as one of:
- buying_signal: Shows interest in purchasing/scheduling a call/learning more about services
- interest_signal: Engaged but not yet expressing purchase intent
- objection: Raises concerns or pushback
- neutral: Generic response with no signal

Rate confidence 0.0-1.0 and provide a brief summary.

Respond with ONLY valid JSON:
{{"type":"buying_signal|interest_signal|objection|neutral","confidence":0.8,"summary":"brief description"}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if resp.status_code == 200:
            content = resp.json()["content"][0]["text"].strip()
            # Extract JSON from response
            json_match = re.search(r'\{[^}]+\}', content)
            if json_match:
                result = json.loads(json_match.group(0))
                result["requiresHumanReview"] = result.get("confidence", 0) > 0.7 and result.get("type") != "neutral"
                return result
    except Exception as e:
        print(f"    Opportunity detection error: {e}")

    return None


# ── SOS Schedule Management ───────────────────────────────────────────────

def process_sos_schedules(queue):
    """Check SOS entries and mark ready-to-send items."""
    now = datetime.now(timezone.utc)
    updated = 0

    for entry in queue:
        if entry.get("status") != "sos_waiting":
            continue
        sos = entry.get("sosSequence")
        if not sos or not sos.get("enabled"):
            continue

        next_at = sos.get("nextScheduledAt")
        if not next_at:
            continue

        try:
            scheduled_time = datetime.fromisoformat(next_at.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        if now >= scheduled_time:
            # Mark as ready to send (browser automation will pick it up)
            entry["status"] = "pending"
            entry["_sosReadyToSend"] = True
            _add_log(entry, "sos_ready", "success", f"SOS step {sos.get('currentStep', '?')} ready to send")
            updated += 1
            print(f"    SOS ready: {entry.get('contactName')} at {entry.get('company')} (step {sos.get('currentStep')})")

    if updated:
        print(f"  Marked {updated} SOS entries as ready to send")
    return updated


def calculate_next_sos_date(entry):
    """Calculate the next SOS send date for an entry."""
    sos = entry.get("sosSequence", {})
    if not sos.get("enabled"):
        return

    days = sos.get("days", [0, 2, 4, 6, 9])
    current_step = sos.get("currentStep", 0)

    if current_step >= len(days):
        # SOS complete
        entry["status"] = "completed"
        _add_log(entry, "sos_complete", "success", "All SOS steps sent")
        return

    # Calculate next date from the sequence start or last sent
    sent_dates = sos.get("sentDates", [])
    if sent_dates:
        last_sent = datetime.fromisoformat(sent_dates[-1].replace("Z", "+00:00"))
    else:
        last_sent = datetime.fromisoformat(entry.get("createdAt", _now_iso()).replace("Z", "+00:00"))

    next_day_offset = days[current_step]
    if current_step > 0:
        prev_day_offset = days[current_step - 1]
        delta_days = next_day_offset - prev_day_offset
    else:
        delta_days = next_day_offset

    next_date = last_sent + timedelta(days=delta_days)
    sos["nextScheduledAt"] = next_date.isoformat()
    entry["status"] = "sos_waiting"


# ── Queue Processing ──────────────────────────────────────────────────────

def _add_log(entry, action, status, details=""):
    """Add an execution log entry."""
    if "executionLog" not in entry:
        entry["executionLog"] = []
    entry["executionLog"].append({
        "timestamp": _now_iso(),
        "action": action,
        "status": status,
        "details": details,
    })
    # Keep last 50 log entries
    if len(entry["executionLog"]) > 50:
        entry["executionLog"] = entry["executionLog"][-50:]


def process_crm_creates(queue, crm_data):
    """Create companies and contacts in HubSpot for queue entries."""
    created = 0
    for entry in queue:
        if created >= MAX_CRM_CREATES_PER_RUN:
            break

        # Process entries that need CRM sync
        sync_status = entry.get("hubspotSyncStatus", "not_synced")
        if sync_status != "not_synced":
            continue
        if entry.get("status") in ("completed", "failed"):
            continue

        company = entry.get("company", "")
        contact_name = entry.get("contactName", "")
        if not company:
            continue

        print(f"  CRM create: {contact_name} at {company}")

        # Check if company already in CRM
        company_key = company.lower().strip()
        existing = crm_data.get("companies", {}).get(company_key, {})
        company_id = existing.get("companyId")

        if not company_id:
            # Search HubSpot first
            company_id = search_hubspot_company(company)

        if not company_id:
            # Create new company
            domain = ""  # Could enrich from Apollo data
            company_id = create_hubspot_company(company, domain)

        if company_id:
            entry["companyId"] = str(company_id)

            # Create contact if we have name
            if contact_name:
                name_parts = contact_name.strip().split(" ", 1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ""
                email = entry.get("email", "")
                title = entry.get("contactTitle", "")
                phone = entry.get("whatsappPhone", "")

                contact_id = create_hubspot_contact(
                    first_name, last_name, email, title, company_id, phone
                )
                if contact_id:
                    entry["contactId"] = str(contact_id)

            entry["hubspotSyncStatus"] = "synced"
            _add_log(entry, "crm_create", "success", f"Company: {company_id}, Contact: {entry.get('contactId', 'N/A')}")
            created += 1
        else:
            entry["hubspotSyncStatus"] = "error"
            _add_log(entry, "crm_create", "failed", "Could not create or find company")

        time.sleep(HUBSPOT_RATE_DELAY)

    print(f"  CRM creates: {created}")
    return created


def process_email_enrollments(queue):
    """Enroll contacts in HubSpot email sequences."""
    enrolled = 0
    for entry in queue:
        if enrolled >= MAX_EMAIL_ENROLLMENTS_PER_RUN:
            break

        if entry.get("type") != "email" or entry.get("status") != "pending":
            continue

        contact_id = entry.get("contactId")
        sequence_id = entry.get("sequenceId")

        if not contact_id or not sequence_id:
            _add_log(entry, "email_enroll", "failed", "Missing contactId or sequenceId")
            continue

        print(f"  Email enroll: {entry.get('contactName')} in sequence {sequence_id}")

        if enroll_in_sequence(contact_id, sequence_id):
            entry["status"] = "email_sent"
            entry["messageSequence"] = entry.get("messageSequence", {})
            entry["messageSequence"]["enrolledAt"] = _now_iso()
            _add_log(entry, "email_enroll", "success", f"Enrolled in sequence {sequence_id}")
            enrolled += 1
        else:
            entry["attempts"] = entry.get("attempts", 0) + 1
            entry["lastAttemptAt"] = _now_iso()
            if entry["attempts"] >= 3:
                entry["status"] = "failed"
            _add_log(entry, "email_enroll", "failed", f"Attempt {entry['attempts']}")

        time.sleep(HUBSPOT_RATE_DELAY)

    print(f"  Email enrollments: {enrolled}")
    return enrolled


def check_replies(queue):
    """Check HubSpot for email replies to enrolled contacts."""
    replies_found = 0
    for entry in queue:
        if entry.get("status") != "email_sent":
            continue

        contact_id = entry.get("contactId")
        if not contact_id:
            continue

        engagement = check_contact_engagement(contact_id)
        if engagement.get("hasReplied"):
            entry["status"] = "reply_received"
            _add_log(entry, "email_reply", "success", f"Reply detected at {engagement.get('lastReplyDate')}")
            replies_found += 1

            # Run opportunity detection on reply
            # (In practice, we'd need the actual reply text — HubSpot doesn't expose this directly)
            opp = detect_opportunity(
                f"Email reply detected from {entry.get('contactName')} at {entry.get('company')}",
                entry.get("contactName", ""),
                entry.get("company", ""),
            )
            if opp and opp.get("type") != "neutral":
                if "opportunities" not in entry:
                    entry["opportunities"] = []
                opp["detectedAt"] = _now_iso()
                opp["status"] = "open"
                entry["opportunities"].append(opp)
                if opp.get("requiresHumanReview"):
                    entry["status"] = "opportunity_flagged"

            print(f"    Reply from: {entry.get('contactName')} at {entry.get('company')}")

        time.sleep(HUBSPOT_RATE_DELAY)

    print(f"  Replies found: {replies_found}")
    return replies_found


def process_opportunity_detection(queue):
    """Scan queue entries with replies for buying signals."""
    if not ANTHROPIC_API_KEY:
        print("  Skipping opportunity detection (no ANTHROPIC_API_KEY)")
        return 0

    detected = 0
    for entry in queue:
        # Check entries that have replies but no opportunity analysis yet
        if entry.get("status") not in ("reply_received", "message_sent", "conn_accepted"):
            continue
        if entry.get("_opportunityChecked"):
            continue

        # Check execution log for reply content
        reply_text = ""
        for log in reversed(entry.get("executionLog", [])):
            if log.get("action") in ("reply_received", "whatsapp_reply", "email_reply"):
                reply_text = log.get("details", "")
                break

        if not reply_text:
            continue

        opp = detect_opportunity(
            reply_text,
            entry.get("contactName", ""),
            entry.get("company", ""),
        )
        entry["_opportunityChecked"] = True

        if opp and opp.get("type") != "neutral":
            if "opportunities" not in entry:
                entry["opportunities"] = []
            opp["detectedAt"] = _now_iso()
            opp["status"] = "open"
            entry["opportunities"].append(opp)
            detected += 1

            if opp.get("requiresHumanReview"):
                entry["status"] = "opportunity_flagged"
                print(f"    🚨 Opportunity: {entry.get('contactName')} at {entry.get('company')} — {opp.get('summary')}")

    print(f"  Opportunities detected: {detected}")
    return detected


# ── Execution Log Management ──────────────────────────────────────────────

def update_execution_log(exec_log, queue, stats):
    """Update the centralized execution log with current run stats."""
    exec_log["lastProcessed"] = _now_iso()
    exec_log["processRunId"] = f"run-{int(time.time())}"

    # Update daily stats
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = exec_log.get("dailyStats", {})
    if daily.get("date") != today:
        daily = {"date": today, "connectsSent": 0, "messagesSent": 0, "repliesReceived": 0, "opportunitiesDetected": 0, "crmSynced": 0, "emailsEnrolled": 0}
    daily["crmSynced"] = daily.get("crmSynced", 0) + stats.get("crmCreated", 0)
    daily["emailsEnrolled"] = daily.get("emailsEnrolled", 0) + stats.get("emailsEnrolled", 0)
    daily["repliesReceived"] = daily.get("repliesReceived", 0) + stats.get("repliesFound", 0)
    daily["opportunitiesDetected"] = daily.get("opportunitiesDetected", 0) + stats.get("opportunitiesDetected", 0)
    exec_log["dailyStats"] = daily

    # Collect open opportunities
    open_opps = []
    for entry in queue:
        for opp in entry.get("opportunities", []):
            if opp.get("status") == "open":
                open_opps.append({
                    "queueEntryId": entry.get("id", ""),
                    "contactName": entry.get("contactName", ""),
                    "company": entry.get("company", ""),
                    "type": opp.get("type", ""),
                    "confidence": opp.get("confidence", 0),
                    "summary": opp.get("summary", ""),
                    "detectedAt": opp.get("detectedAt", ""),
                    "status": "open",
                })
    exec_log["opportunities"] = open_opps

    # Rate limit status (tracks for browser automation)
    exec_log["rateLimitStatus"] = exec_log.get("rateLimitStatus", {
        "connections": {"today": 0, "limit": 20, "resetDate": today},
        "messages": {"today": 0, "limit": 50, "resetDate": today},
    })
    # Reset if new day
    rl = exec_log["rateLimitStatus"]
    for key in ("connections", "messages"):
        if rl.get(key, {}).get("resetDate") != today:
            rl[key] = {"today": 0, "limit": rl.get(key, {}).get("limit", 20 if key == "connections" else 50), "resetDate": today}

    return exec_log


# ── Main ──────────────────────────────────────────────────────────────────

def _migrate_queue_to_multiuser(queue_data):
    """Migrate old flat queue format to multi-user v2.0 format."""
    if queue_data.get("version") == "2.0" and "users" in queue_data:
        return queue_data  # Already migrated

    # Old format: { queue: [...], lastProcessed: "..." }
    old_queue = queue_data.get("queue", [])
    admin_email = "dori.kafri@develeap.com"

    new_data = {
        "version": "2.0",
        "users": {
            admin_email: {
                "queue": old_queue,
                "lastProcessed": queue_data.get("lastProcessed", "")
            }
        },
        "metadata": {
            "migratedAt": _now_iso(),
            "migratedFrom": "flat"
        }
    }
    print(f"  Migrated old queue ({len(old_queue)} entries) to multi-user format under {admin_email}")
    return new_data


def _collect_all_entries(queue_data):
    """Collect all entries from all users into a flat list with _ownerEmail tag."""
    entries = []
    for user_email, user_data in queue_data.get("users", {}).items():
        for entry in user_data.get("queue", []):
            entry["_ownerEmail"] = user_email
            entries.append(entry)
    return entries


def _distribute_entries(queue_data, entries):
    """Distribute entries back to their owner sections after processing."""
    # Clear all queues
    for user_email in queue_data.get("users", {}):
        queue_data["users"][user_email]["queue"] = []

    # Distribute
    for entry in entries:
        owner = entry.pop("_ownerEmail", "dori.kafri@develeap.com")
        if owner not in queue_data["users"]:
            queue_data["users"][owner] = {"queue": [], "lastProcessed": ""}
        queue_data["users"][owner]["queue"].append(entry)


## ── Wolf Pack Campaign Processing ───────────────────────────────────────

def process_wolf_pack_campaigns(queue_data):
    """Main campaign processing loop — runs alongside existing queue processing."""
    wp_data = _load_json(WOLFPACK_FILE, {"version": 1, "campaigns": []})
    campaigns = wp_data.get("campaigns", [])
    active_count = 0
    nodes_executed = 0
    responses_tracked = 0

    for campaign in campaigns:
        if campaign.get("status") != "active":
            continue
        active_count += 1

        # 1. Track responses across all channels
        responses_tracked += _track_campaign_responses(campaign)

        # 2. Apply adaptive rules
        _apply_adaptive_rules(campaign)

        # 3. Execute ready nodes — push actions to the outreach queue
        nodes_executed += _execute_campaign_nodes(campaign, queue_data)

    if active_count > 0:
        _save_json(WOLFPACK_FILE, wp_data)

    return {
        "activeCampaigns": active_count,
        "nodesExecuted": nodes_executed,
        "responsesTracked": responses_tracked,
    }


def _track_campaign_responses(campaign):
    """Check for new responses across channels for this campaign."""
    tracked = 0
    touchpoints = campaign.get("responseTracking", {}).get("touchpoints", [])

    for tp in touchpoints:
        if tp.get("status") == "completed":
            continue

        channel = tp.get("channel", "")
        contact_id = tp.get("contactId", "")

        # Check HubSpot for email responses
        if channel == "email" and HUBSPOT_TOKEN:
            contact_email = _find_campaign_contact_email(campaign, contact_id)
            if contact_email:
                replied = _check_hubspot_email_reply(contact_email)
                if replied:
                    tp["status"] = "completed"
                    tp["responses"] = tp.get("responses", []) + [{
                        "type": "email_reply",
                        "timestamp": _now_iso(),
                        "sentiment": "unknown"
                    }]
                    tracked += 1

        # Check LinkedIn status (from outreach_status.json if exists)
        if channel == "linkedin":
            status_data = _load_json("outreach_status.json", {})
            contact_email = _find_campaign_contact_email(campaign, contact_id)
            if contact_email and contact_email in status_data:
                entry = status_data[contact_email]
                if entry.get("connected"):
                    tp["status"] = "completed"
                    tp["responses"] = tp.get("responses", []) + [{
                        "type": "linkedin_connected",
                        "timestamp": _now_iso()
                    }]
                    tracked += 1

    # Recalculate engagement score
    if touchpoints:
        total = len(touchpoints)
        completed = sum(1 for t in touchpoints if t.get("status") == "completed")
        replied = sum(1 for t in touchpoints if t.get("responses"))
        score = ((completed * 0.3 + replied * 0.7) / max(total, 1)) * 100
        campaign.setdefault("responseTracking", {})["engagementScore"] = round(score, 1)

    return tracked


def _apply_adaptive_rules(campaign):
    """Evaluate and apply adaptive rules based on campaign state."""
    rules = campaign.get("adaptiveRules", [])
    touchpoints = campaign.get("responseTracking", {}).get("touchpoints", [])

    for rule in rules:
        trigger = rule.get("trigger", "")
        action = rule.get("action", "")

        fired = False

        if trigger == "any_positive_reply":
            fired = any(
                tp.get("responses") and
                any(r.get("sentiment") in ("positive", "unknown") for r in tp["responses"])
                for tp in touchpoints
            )
        elif trigger == "all_connections_declined":
            li_tps = [tp for tp in touchpoints if tp.get("channel") == "linkedin"]
            fired = (
                len(li_tps) > 0 and
                all(tp.get("status") == "declined" for tp in li_tps)
            )
        elif trigger == "email_opened_3x":
            opened_tps = [
                tp for tp in touchpoints
                if tp.get("channel") == "email" and tp.get("openCount", 0) >= 3
            ]
            fired = len(opened_tps) > 0
        elif trigger == "no_response_7days":
            exec_state = campaign.get("executionState", {})
            if exec_state.get("nextExecutionAt"):
                next_exec = datetime.fromisoformat(exec_state["nextExecutionAt"].replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - next_exec > timedelta(days=7):
                    has_any_reply = any(tp.get("responses") for tp in touchpoints)
                    fired = not has_any_reply

        if fired:
            print(f"    Wolf Pack rule fired: {trigger} → {action} (campaign {campaign.get('id', '?')})")
            if action == "pause_remaining":
                campaign["status"] = "paused"
                campaign.setdefault("executionState", {})["pausedAt"] = _now_iso()
            elif action == "archive":
                campaign["status"] = "archived"
            elif action == "escalate_priority":
                # Mark as escalated in execution state
                campaign.setdefault("executionState", {})["escalated"] = True


def _execute_campaign_nodes(campaign, queue_data):
    """Check execution state and execute nodes whose wait time has elapsed."""
    executed = 0
    exec_state = campaign.get("executionState", {})
    current_node_id = exec_state.get("currentNodeId")
    flow = campaign.get("flow", {})
    nodes = flow.get("nodes", {})

    if not current_node_id or current_node_id not in nodes:
        return 0

    node = nodes[current_node_id]
    now = datetime.now(timezone.utc)

    # Check if it's time to execute
    next_exec_str = exec_state.get("nextExecutionAt")
    if next_exec_str:
        try:
            next_exec = datetime.fromisoformat(next_exec_str.replace("Z", "+00:00"))
            if now < next_exec:
                return 0  # Not yet time
        except (ValueError, TypeError):
            pass

    node_type = node.get("type", "")
    config = node.get("config", {})

    if node_type == "wait":
        # Advance past wait node
        wait_days = config.get("days", 1)
        _advance_campaign_node(campaign, wait_days)
        executed += 1

    elif node_type.startswith("action_"):
        # Push action to the outreach queue
        sender = config.get("sender", "")
        contact_idx = config.get("contactIdx", 0)
        contacts = campaign.get("targetContacts", [])
        contact = contacts[contact_idx] if contact_idx < len(contacts) else {}

        # Determine channel
        if "linkedin_connect" in node_type:
            channel = "linkedin"
            q_type = "connect"
        elif "linkedin_message" in node_type:
            channel = "linkedin"
            q_type = "outreach"
        elif "email" in node_type:
            channel = "email"
            q_type = "outreach"
        elif "whatsapp" in node_type:
            channel = "whatsapp"
            q_type = "outreach"
        else:
            channel = "other"
            q_type = "outreach"

        # Create queue entry
        queue_entry = {
            "id": f"wp_{campaign['id']}_{current_node_id}_{int(time.time())}",
            "contactName": contact.get("name", ""),
            "contactTitle": contact.get("title", ""),
            "company": campaign.get("company", {}).get("name", ""),
            "linkedinUrl": contact.get("linkedinUrl", ""),
            "email": contact.get("email", ""),
            "message": "",  # Will be filled by template
            "type": q_type,
            "status": "pending",
            "createdAt": _now_iso(),
            "campaignId": campaign["id"],
            "campaignNodeId": current_node_id,
            "actingAs": sender,
            "createdBy": campaign.get("createdBy", sender),
        }

        # Push to the right user's queue
        target_user = sender or campaign.get("createdBy", "dori.kafri@develeap.com")
        users = queue_data.setdefault("users", {})
        if target_user not in users:
            users[target_user] = {"queue": [], "lastProcessed": ""}
        users[target_user]["queue"].append(queue_entry)

        # Record touchpoint
        campaign.setdefault("responseTracking", {}).setdefault("touchpoints", []).append({
            "id": f"tp_{int(time.time())}",
            "nodeId": current_node_id,
            "timestamp": _now_iso(),
            "sender": sender,
            "channel": channel,
            "action": node_type,
            "contactId": contact.get("id", ""),
            "status": "pending",
            "responses": [],
        })

        _advance_campaign_node(campaign, 1)
        executed += 1
        print(f"    Wolf Pack: queued {node_type} for {contact.get('name', '?')} via {sender}")

    elif node_type.startswith("logic_"):
        # Evaluate logic node
        _evaluate_campaign_logic(campaign, current_node_id, node)
        executed += 1

    elif node_type == "trigger_end":
        campaign["status"] = "completed"
        exec_state["completedNodes"] = exec_state.get("completedNodes", []) + [current_node_id]
        print(f"    Wolf Pack: campaign {campaign.get('id', '?')} completed")

    elif node_type == "trigger_start":
        _advance_campaign_node(campaign, 0)
        executed += 1

    return executed


def _advance_campaign_node(campaign, wait_days=0):
    """Move to the next node in the flow."""
    exec_state = campaign.setdefault("executionState", {})
    current = exec_state.get("currentNodeId")
    flow = campaign.get("flow", {})
    nodes = flow.get("nodes", {})
    node_order = flow.get("nodeOrder", [])

    # Mark current as completed
    completed = exec_state.setdefault("completedNodes", [])
    if current and current not in completed:
        completed.append(current)

    # Find next node
    current_node = nodes.get(current, {})
    next_nodes = current_node.get("next", [])
    if next_nodes:
        exec_state["currentNodeId"] = next_nodes[0]
    else:
        # Fall back to nodeOrder
        idx = node_order.index(current) if current in node_order else -1
        if idx >= 0 and idx + 1 < len(node_order):
            exec_state["currentNodeId"] = node_order[idx + 1]
        else:
            exec_state["currentNodeId"] = None

    # Set next execution time
    exec_state["nextExecutionAt"] = (
        datetime.now(timezone.utc) + timedelta(days=max(wait_days, 0))
    ).isoformat()


def _evaluate_campaign_logic(campaign, node_id, node):
    """Evaluate a logic node and choose the branch."""
    config = node.get("config", {})
    touchpoints = campaign.get("responseTracking", {}).get("touchpoints", [])

    has_response = any(tp.get("responses") for tp in touchpoints)

    exec_state = campaign.setdefault("executionState", {})
    completed = exec_state.setdefault("completedNodes", [])
    if node_id not in completed:
        completed.append(node_id)

    if has_response:
        # True branch
        true_branch = node.get("trueBranch", node.get("next", []))
        exec_state["currentNodeId"] = true_branch[0] if true_branch else None
    else:
        # Check timeout
        timeout_days = config.get("timeoutDays", 7)
        created = campaign.get("createdAt", _now_iso())
        try:
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            elapsed = (datetime.now(timezone.utc) - created_dt).days
        except (ValueError, TypeError):
            elapsed = 0

        if elapsed >= timeout_days:
            # Timeout → false branch
            false_branch = node.get("falseBranch", [])
            if false_branch:
                exec_state["currentNodeId"] = false_branch[0]
            else:
                _advance_campaign_node(campaign, 0)
        # else: stay on this node, waiting for response


def _find_campaign_contact_email(campaign, contact_id):
    """Find email for a contact by ID."""
    for tc in campaign.get("targetContacts", []):
        if tc.get("id") == contact_id:
            return tc.get("email", "")
    return ""


def _check_hubspot_email_reply(contact_email):
    """Check HubSpot for email replies from a specific contact."""
    if not HUBSPOT_TOKEN or not contact_email:
        return False
    try:
        url = f"{BASE_URL}/crm/v3/objects/contacts/search"
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": contact_email
                }]
            }],
            "properties": ["hs_email_last_reply_date", "hs_email_replied"]
        }
        resp = requests.post(url, headers=_hs_headers(), json=body, timeout=10)
        if resp.ok:
            results = resp.json().get("results", [])
            if results:
                props = results[0].get("properties", {})
                return props.get("hs_email_replied") == "true"
    except Exception as e:
        print(f"    HubSpot reply check error for {contact_email}: {e}")
    return False


def _hs_headers():
    """Return HubSpot API headers."""
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }


def main():
    print("=" * 60)
    print("BDR Queue Processor — Server-side automation (multi-user)")
    print("=" * 60)

    # Check workflow config
    wf_config = _load_workflow_config()

    # Load queue (supports both old flat and new multi-user format)
    queue_data = _load_json(QUEUE_FILE, {"queue": []})
    is_multiuser = queue_data.get("version") == "2.0"
    queue_data = _migrate_queue_to_multiuser(queue_data)

    # Collect all entries from all users for processing
    queue = _collect_all_entries(queue_data)
    user_count = len(queue_data.get("users", {}))
    print(f"\nLoaded queue: {len(queue)} entries across {user_count} user(s)")
    for user_email, user_data in queue_data.get("users", {}).items():
        uq = user_data.get("queue", [])
        print(f"  {user_email}: {len(uq)} entries")

    # Load execution log
    exec_log = _load_json(EXEC_LOG_FILE)

    # Load CRM data for company lookups
    crm_data = _load_json(CRM_DATA_FILE)

    stats = {
        "crmCreated": 0,
        "emailsEnrolled": 0,
        "repliesFound": 0,
        "sosUpdated": 0,
        "opportunitiesDetected": 0,
    }

    # 1. CRM creates (if crmCreate node is enabled)
    if HUBSPOT_TOKEN and _is_node_enabled(wf_config, "crmCreate"):
        print("\n── Step 1: CRM Creates ──")
        _detect_region()
        stats["crmCreated"] = process_crm_creates(queue, crm_data)
    else:
        print("\n── Step 1: CRM Creates — SKIPPED (no token or disabled) ──")

    # 2. Email sequence enrollments (if emailSequence node is enabled)
    if HUBSPOT_TOKEN and _is_node_enabled(wf_config, "emailSequence"):
        print("\n── Step 2: Email Sequence Enrollments ──")
        stats["emailsEnrolled"] = process_email_enrollments(queue)
    else:
        print("\n── Step 2: Email Sequences — SKIPPED ──")

    # 3. Check for email replies
    if HUBSPOT_TOKEN and _is_node_enabled(wf_config, "responseTracking"):
        print("\n── Step 3: Check Email Replies ──")
        stats["repliesFound"] = check_replies(queue)
    else:
        print("\n── Step 3: Reply Checking — SKIPPED ──")

    # 4. SOS schedule management
    if _is_node_enabled(wf_config, "followUp"):
        print("\n── Step 4: SOS Schedule Management ──")
        stats["sosUpdated"] = process_sos_schedules(queue)
    else:
        print("\n── Step 4: SOS Scheduling — SKIPPED ──")

    # 5. Opportunity detection
    if ANTHROPIC_API_KEY and _is_node_enabled(wf_config, "opportunityAI"):
        print("\n── Step 5: Opportunity Detection ──")
        stats["opportunitiesDetected"] = process_opportunity_detection(queue)
    else:
        print("\n── Step 5: Opportunity Detection — SKIPPED ──")

    # 6. Wolf Pack campaign processing
    print("\n── Step 6: Wolf Pack Campaigns ──")
    wp_stats = process_wolf_pack_campaigns(queue_data)
    print(f"  Active campaigns:  {wp_stats['activeCampaigns']}")
    print(f"  Nodes executed:    {wp_stats['nodesExecuted']}")
    print(f"  Responses tracked: {wp_stats['responsesTracked']}")

    # Update execution log (include user attribution)
    exec_log = update_execution_log(exec_log, queue, stats)

    # Distribute entries back to their user sections
    _distribute_entries(queue_data, queue)

    # Update per-user lastProcessed timestamps
    for user_email in queue_data.get("users", {}):
        queue_data["users"][user_email]["lastProcessed"] = _now_iso()

    # Save outputs
    print("\n── Saving Results ──")
    _save_json(QUEUE_FILE, queue_data)
    _save_json(EXEC_LOG_FILE, exec_log)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Queue Processor Complete (multi-user)")
    print(f"  Users processed:  {user_count}")
    print(f"  CRM created:      {stats['crmCreated']}")
    print(f"  Emails enrolled:  {stats['emailsEnrolled']}")
    print(f"  Replies found:    {stats['repliesFound']}")
    print(f"  SOS updated:      {stats['sosUpdated']}")
    print(f"  Opportunities:    {stats['opportunitiesDetected']}")
    print(f"  Wolf Pack active: {wp_stats['activeCampaigns']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
