# QA Test Plan — Queue & Wolf Pack Mechanisms
## BDR Job Monitor Dashboard

**Date**: 2026-03-20
**Scope**: LinkedIn Queue and Wolf Pack Campaign mechanisms
**Test Contact**: Dori Kafri | dori.kafri@develeap.com | +972542289888 | [LinkedIn](https://www.linkedin.com/in/dori-kafri/)
**Test Company**: Develeap

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Queue Mechanism](#queue-mechanism)
3. [Wolf Pack Mechanism](#wolf-pack-mechanism)
4. [Test Cases — Queue](#test-cases--queue)
5. [Test Cases — Wolf Pack](#test-cases--wolf-pack)
6. [Integration Tests](#integration-tests)
7. [Edge Cases & Error Handling](#edge-cases--error-handling)
8. [Backend / GitHub Actions Tests](#backend--github-actions-tests)

---

## Architecture Overview

### Data Files
| File | Purpose |
|------|---------|
| `linkedin_queue.json` | Multi-user v2.0 queue with per-user entries |
| `wolfpack_campaigns.json` | Campaign definitions with flows, contacts, team |
| `outreach_status.json` | LinkedIn connection status per contact |

### Key Frontend Functions (docs/index.html)
| Function | Line | Purpose |
|----------|------|---------|
| `_pushToLinkedInQueue(entry)` | 9303 | Add entry to queue and persist to GitHub |
| `renderQueueView()` | 14348 | Render full queue UI |
| `_buildVisibleQueue()` | 14187 | Aggregate queue across visible users |
| `pauseQueueEntry(idx)` | 14654 | Set status → `paused`, save, toast |
| `resumeQueueEntry(idx)` | 14661 | Set status → `pending`, save, toast |
| `retryQueueEntry(idx)` | 14668 | Clear error/completedAt, set → `pending`, save, toast |
| `deleteQueueEntry(idx)` | 14677 | Splice entry, save, toast |
| `markConnAccepted(idx)` | 14685 | Status → `completed`, update outreach_status, advance SOS |
| `advanceSOSSequence(seqId)` | 14705 | Move first `sos_waiting` entry → `pending` |
| `toggleQueueGlobal()` | 14715 | Pause/resume all pending entries |
| `clearCompletedQueue()` | 14733 | Remove all completed/sent entries |
| `retryAllFailed()` | 14744 | Set all failed → `pending` |
| `_wpRenderCurrentView()` | 16885 | Route to list/detail/editor Wolf Pack sub-view |
| `_wpRenderList()` | 17018 | Render campaign list |
| `_wpRenderCampaignCard(c)` | 17757 | Render single campaign card |
| `_wpRenderWizardStep()` | 18046 | Render 5-step campaign creation wizard |
| `_wpGetCampaignQueueItems(id)` | 17432 | Get queue entries for a campaign |
| `_wpContactChannelSummary(id, name)` | 17442 | Aggregate contact status across channels |
| `_renderQueueWpContext(entry, idx)` | 17490 | Inline Wolf Pack context in queue card |

### Key Backend Functions (process_queue.py)
| Function | Line | Purpose |
|----------|------|---------|
| `process_wolf_pack_campaigns(queue_data)` | 778 | Main campaign processing loop |
| `_track_campaign_responses(campaign)` | 810 | Detect email/LinkedIn responses |
| `_apply_adaptive_rules(campaign)` | 861 | Fire rule triggers and actions |
| `_execute_campaign_nodes(campaign, queue_data)` | 910 | Execute current node, push to queue |
| `_advance_campaign_node(campaign, wait_days)` | 1026 | Move execution to next node |

---

## Queue Mechanism

### What It Does
The LinkedIn queue is a per-user outreach action backlog stored in `linkedin_queue.json`. It buffers connection requests, messages, emails, and WhatsApp messages that need manual or automated execution. The backend (`process_queue.py`) processes entries every 2 hours via GitHub Actions.

### Queue Entry Schema
```json
{
  "id": "unique_entry_id",
  "contactName": "Dori Kafri",
  "contactTitle": "CEO",
  "company": "Develeap",
  "linkedinUrl": "https://linkedin.com/in/dori-kafri",
  "email": "dori.kafri@develeap.com",
  "whatsappPhone": "+972542289888",
  "message": "Hi Dori, ...",
  "type": "connect",
  "status": "pending",
  "isConnected": false,
  "customerType": "customer",
  "campaignId": null,
  "campaignNodeId": null,
  "templateIdx": 0,
  "createdAt": "2026-03-20T10:00:00Z",
  "completedAt": null,
  "sequenceId": null,
  "sosStep": null,
  "createdBy": "dori.kafri@develeap.com"
}
```

### Status Flow
```
pending → processing → completed
pending → paused → pending (resume)
processing → failed → pending (retry)
pending → conn_pending → conn_accepted → completed
conn_pending → pending (resend)
sos_waiting → pending (advanceSOSSequence)
```

### "Clear Done" Statuses
`completed`, `message_sent`, `email_sent`, `reply_received`, `conn_accepted`

---

## Wolf Pack Mechanism

### What It Does
Wolf Pack is a multi-contact, multi-channel, multi-user outreach campaign engine. Campaigns define a visual flow (sequence of wait/action/logic nodes) that the backend executes automatically, pushing actions into each team member's queue.

### Campaign Creation Wizard (5 Steps)
1. **Target Company** — Name, domain, industry
2. **Job Listing** — Title, URL, category (or select from dashboard)
3. **Target Contacts** — Add contacts manually or from stakeholder suggestions
4. **Team Members** — Select users and their channels (LinkedIn / Email / WhatsApp)
5. **Review & Launch** — Campaign name, select flow template, launch

### Flow Node Types
| Type | Description |
|------|-------------|
| `trigger_start` | Entry point — immediately advances |
| `wait` | Pause execution for N days |
| `action_linkedin_connect` | Push a LinkedIn connect request to queue |
| `action_linkedin_message` | Push a LinkedIn message to queue |
| `action_email` | Push an email outreach to queue |
| `action_whatsapp` | Push a WhatsApp message to queue |
| `logic_if_response` | Branch: response received → trueBranch, else → falseBranch |
| `logic_wait_response` | Wait for response with timeout |
| `action_escalate` | Flag for escalation |
| `trigger_end` | Mark campaign `completed` |

### Engagement Score Formula
```
score = ((completed_touchpoints * 0.3 + replied_touchpoints * 0.7) / total_touchpoints) * 100
```

### Adaptive Rule Triggers
| Trigger | Action |
|---------|--------|
| `any_positive_reply` | `pause_remaining` |
| `all_connections_declined` | `archive` |
| `email_opened_3x` | `escalate_priority` |
| `no_response_7days` | (custom action) |

---

## Test Cases — Queue

### TC-Q-001: Add Entry to Queue
**Objective**: Verify a new queue entry is created and persisted.

**Preconditions**: Logged in as dori.kafri@develeap.com, Queue view is visible.

**Steps**:
1. Navigate to a job listing for Develeap (or use Outreach modal).
2. Fill in contact: Dori Kafri, dori.kafri@develeap.com, +972542289888.
3. Set type = `connect`, enter a connect message.
4. Click "Add to Queue" / submit.
5. Navigate to Queue tab.

**Expected Result**:
- Entry appears in queue with status `pending`.
- Toast shows "Queued for Dori Kafri".
- `linkedin_queue.json` on GitHub has a new entry under `users.dori.kafri@develeap.com.queue`.
- Entry has `id`, `createdAt`, `createdBy: "dori.kafri@develeap.com"`.

**Error Scenarios**:
- GitHub token missing → entry not saved → error toast appears.
- SHA conflict (stale SHA) → 409 from GitHub API → error toast "GitHub push failed: 409".

---

### TC-Q-002: Pause a Queue Entry
**Objective**: Verify pause transitions status and persists.

**Preconditions**: At least one `pending` entry for Dori Kafri in queue.

**Steps**:
1. Go to Queue tab.
2. Locate Dori Kafri's pending entry.
3. Click "⏸ Pause".

**Expected Result**:
- Entry status changes to `paused`.
- Button changes to "▶ Resume".
- Toast: "⏸ Paused: Dori Kafri".
- `linkedin_queue.json` updated with `status: "paused"`.
- Paused badge count increments by 1.

---

### TC-Q-003: Resume a Paused Entry
**Objective**: Verify resume transitions status back to pending.

**Preconditions**: A `paused` entry exists for Dori Kafri.

**Steps**:
1. Locate the paused entry.
2. Click "▶ Resume".

**Expected Result**:
- Status → `pending`.
- Toast: "▶ Resumed: Dori Kafri".
- Entry now shows Pause button again.
- Pending count increments, paused decrements.

---

### TC-Q-004: Retry a Failed Entry
**Objective**: Verify retry clears error state and resets to pending.

**Preconditions**: An entry with status `failed` or `error` exists.

**Steps**:
1. Locate the failed entry for Dori Kafri.
2. Click "↻ Retry".

**Expected Result**:
- `status` → `pending`.
- `error` field → `null`.
- `completedAt` → `null`.
- Toast: "↻ Retrying: Dori Kafri".
- Entry reappears in pending section.

---

### TC-Q-005: Delete a Queue Entry
**Objective**: Verify delete removes entry from queue and persists.

**Preconditions**: Any entry for Dori Kafri in queue.

**Steps**:
1. Locate the entry.
2. Click "🗑" delete button.

**Expected Result**:
- Entry disappears from queue list.
- Toast: "🗑 Removed: Dori Kafri".
- `linkedin_queue.json` no longer contains that entry `id`.
- Total count decrements.

---

### TC-Q-006: Mark Connection Accepted
**Objective**: Verify `markConnAccepted` completes entry and updates outreach status.

**Preconditions**: Entry with status `conn_pending` for Dori Kafri.

**Steps**:
1. Locate the `conn_pending` entry.
2. Click "✓ Accepted".

**Expected Result**:
- Status → `completed`.
- `completedAt` set to current ISO timestamp.
- `outreach_status.json` entry for Dori Kafri has `connected: true`.
- `_connectedContacts` set updated in memory.
- Toast: "✅ Dori Kafri is now connected!".
- If entry has `sequenceId`: the next `sos_waiting` entry for that sequence → `pending`.

---

### TC-Q-007: Pause All (Global Toggle)
**Objective**: Verify global pause sets all pending/processing entries to paused.

**Preconditions**: Multiple pending entries in queue.

**Steps**:
1. Click "⏸ Pause All" button (id: `queueGlobalToggle`).

**Expected Result**:
- All entries with status `pending` or `processing` → `paused`.
- Button label changes to "▶ Resume All".
- `localStorage.queuePaused` = `true`.
- `linkedin_queue.json` updated.
- Toast: "⏸ Queue paused".

**Steps to Resume**:
1. Click "▶ Resume All".

**Expected Result**:
- All `paused` entries → `pending`.
- Toast: "▶ Queue resumed".

---

### TC-Q-008: Clear Completed Entries
**Objective**: Verify bulk clear removes all "done" statuses.

**Preconditions**: Entries with statuses `completed`, `message_sent`, `email_sent`, `reply_received`, `conn_accepted` exist.

**Steps**:
1. Click "🗑 Clear Done" button.

**Expected Result**:
- All done-status entries removed.
- Toast: "🗑 Cleared N completed/sent entries".
- If no completed entries: toast "No completed entries to clear".
- Completed count badge → 0.

---

### TC-Q-009: Retry All Failed
**Objective**: Verify bulk retry resets all failed entries.

**Preconditions**: Multiple entries with status `failed` or `error`.

**Steps**:
1. Click "↻ Retry Failed" button.

**Expected Result**:
- All `failed`/`error` entries → `pending`, `error: null`, `completedAt: null`.
- Toast: "↻ Retrying N failed entries".
- If no failed entries: toast "No failed entries to retry".
- Failed count badge → 0.

---

### TC-Q-010: SOS Sequence — Add and Advance
**Objective**: Verify SOS (Sequence Of Sends) creates chained entries and auto-advances.

**Preconditions**: Logged in, Dori Kafri contact available.

**Steps**:
1. Open SOS modal (click "SOS" button on a contact).
2. Select SOS type (e.g., "prospect").
3. Confirm SOS queueing.

**Expected Result**:
- Multiple queue entries created for Dori Kafri, all with same `sequenceId`.
- Entry 1 (day 0): status `pending`.
- Entries 2–5 (days 2, 4, 6, 9): status `sos_waiting`.
- Each entry has `sosStep` (1–5).

**Advance Test**:
1. Find entry with status `conn_pending` and `sequenceId` set.
2. Click "✓ Accepted" → `markConnAccepted`.

**Expected Result**:
- `advanceSOSSequence(sequenceId)` finds first `sos_waiting` entry.
- That entry's status → `pending`.
- Sequence continues automatically.

---

### TC-Q-011: Queue User Switching
**Objective**: Verify queue displays correct entries when switching between users.

**Preconditions**: Multiple users in `_allQueueData.users`.

**Steps**:
1. Open Queue tab.
2. In the user selector, choose "All Users".
3. Verify all users' entries are shown.
4. Switch to "dori.kafri@develeap.com (You)".

**Expected Result**:
- "All Users" shows aggregated queue with `_ownerEmail` attribute on entries.
- Single-user view shows only Dori's entries.
- No entries from other users visible.

---

### TC-Q-012: Queue Persistence After Reload
**Objective**: Verify queue state survives page reload.

**Steps**:
1. Add an entry for Dori Kafri to the queue.
2. Pause the entry.
3. Hard reload the page (Ctrl+Shift+R).
4. Navigate to Queue tab.

**Expected Result**:
- Entry still present with status `paused`.
- Data loaded fresh from `linkedin_queue.json` on GitHub.

---

## Test Cases — Wolf Pack

### TC-WP-001: Create Campaign (Full Wizard)
**Objective**: Verify 5-step wizard creates a valid campaign.

**Preconditions**: Logged in as dori.kafri@develeap.com.

**Steps**:
1. Navigate to Wolf Pack tab.
2. Click "New Campaign" / "+".
3. **Step 1 — Company**: Enter "Develeap", domain "develeap.com", category "DevOps".
4. Click "Next →".
5. **Step 2 — Job Listing**: Enter "DevOps Engineer", select category "devops".
6. Click "Next →".
7. **Step 3 — Target Contacts**: Add contact: Name "Dori Kafri", Title "CEO", Email "dori.kafri@develeap.com", LinkedIn "https://linkedin.com/in/dori-kafri". Click "+ Add Contact".
8. Click "Next →".
9. **Step 4 — Team Members**: Select "Dori Kafri", enable LinkedIn + Email channels.
10. Click "Next →".
11. **Step 5 — Review**: Campaign name auto-filled. Select a flow template.
12. Click "🚀 Launch Campaign".

**Expected Result**:
- Campaign created in `_wpCampaigns` array.
- Campaign card appears in Wolf Pack list with status `active`.
- `wolfpack_campaigns.json` updated on GitHub.
- Campaign has: `id`, `name`, `company.name: "Develeap"`, `targetContacts` with Dori Kafri, `teamMembers` with dori.kafri@develeap.com, `flow` with selected template nodes.
- Toast or redirect to campaign detail.

**Edge Cases**:
- Warning shown if another Develeap campaign already exists (Step 1).
- Wizard validates: cannot proceed past Step 3 with 0 contacts.
- Back button navigates to previous step without losing data.

---

### TC-WP-002: Campaign Card Rendering
**Objective**: Verify campaign card shows correct stats.

**Preconditions**: An active campaign for Develeap exists.

**Steps**:
1. View Wolf Pack list.
2. Locate Develeap campaign card.

**Expected Result**:
- Company name: "Develeap".
- Target job title shown.
- Team avatars: "DK" for Dori Kafri (initials).
- Progress bar: completed_nodes / total_nodes %.
- Sent count: touchpoints where `status !== "pending"`.
- Replied count: touchpoints with `responses.length > 0`.
- Engagement score (0–100).
- Days running: based on `createdAt`.
- Action buttons: View | Edit Flow | ⏸ Pause | 🗄 Archive.

---

### TC-WP-003: Pause / Resume Campaign
**Objective**: Verify campaign pause/resume changes status and stops processing.

**Preconditions**: Active Develeap campaign.

**Steps**:
1. Click "⏸ Pause" on the campaign card.

**Expected Result**:
- Campaign status → `paused`.
- Card updates to show "▶ Launch" button.
- `wolfpack_campaigns.json` updated.
- Backend: `process_wolf_pack_campaigns` skips campaigns with `status !== "active"`.

**Resume**:
1. Click "▶ Launch".

**Expected Result**:
- Status → `active`.
- Backend will process on next run.

---

### TC-WP-004: Archive Campaign
**Objective**: Verify archiving moves campaign out of active list.

**Steps**:
1. Click 🗄 archive button on Develeap campaign card.

**Expected Result**:
- Campaign status → `archived`.
- Campaign disappears from "Active" tab.
- Campaign still visible in "All" tab.
- `wolfpack_campaigns.json` updated.

---

### TC-WP-005: Campaign Detail View
**Objective**: Verify detail view shows full campaign information.

**Steps**:
1. Click "View" on a Develeap campaign card (or `_wpOpenDetail(id)`).

**Expected Result**:
- Detail view shows: company name, job title, status, engagement score.
- Target contacts listed: Dori Kafri with email and LinkedIn.
- Team members listed: Dori Kafri with channel badges.
- Flow visualization or node list displayed.
- Touchpoints / response tracking section visible.
- Back button returns to list view.

---

### TC-WP-006: Flow Editor — Add / Remove Nodes
**Objective**: Verify flow editor allows node manipulation.

**Steps**:
1. Click "Edit Flow" on Develeap campaign.
2. Add a `wait` node (e.g., 3 days).
3. Connect it to existing nodes.
4. Remove a node using "🗑 Remove Node".
5. Save flow.

**Expected Result**:
- New node appears in `flow.nodes` and `flow.nodeOrder`.
- Removed node no longer in `flow.nodes`.
- `wolfpack_campaigns.json` updated.
- `executionState.currentNodeId` not pointing to removed node.

**Edge Cases**:
- Cannot delete `trigger_start` or `trigger_end` without confirming.
- Removing a node mid-flow disconnects it; editor should handle orphaned branches.

---

### TC-WP-007: Backend — Node Execution Creates Queue Entry
**Objective**: Verify backend pushes action nodes to user's queue.

**Preconditions**: Active campaign with `executionState.currentNodeId` pointing to an `action_linkedin_connect` node, and `nextExecutionAt` in the past.

**Steps**:
1. Run `python process_queue.py` (or wait for GitHub Action).
2. Check `linkedin_queue.json` after run.

**Expected Result**:
- New entry in `users["dori.kafri@develeap.com"].queue` with:
  - `id`: starts with `"wp_"`.
  - `contactName: "Dori Kafri"`.
  - `company: "Develeap"`.
  - `type: "connect"`.
  - `status: "pending"`.
  - `campaignId` matching the campaign.
  - `campaignNodeId` matching the node.
- Campaign `responseTracking.touchpoints` has new entry.
- `executionState.currentNodeId` advanced to next node.

---

### TC-WP-008: Backend — Wait Node Advancement
**Objective**: Verify wait nodes are skipped after delay elapsed.

**Preconditions**: Campaign with current node = `wait` node, `nextExecutionAt` in the past.

**Steps**:
1. Run `process_queue.py`.

**Expected Result**:
- `_execute_campaign_nodes` detects `node_type == "wait"`.
- Calls `_advance_campaign_node(campaign, wait_days)`.
- `executionState.currentNodeId` moves to next node.
- `nextExecutionAt` set to `now + wait_days`.
- No queue entry created (wait node itself doesn't enqueue).

---

### TC-WP-009: Backend — Response Tracking
**Objective**: Verify backend detects LinkedIn connections and email replies.

**Preconditions**: Campaign with pending touchpoints. `outreach_status.json` has Dori's entry set to `connected: true`.

**Steps**:
1. Run `process_queue.py`.

**Expected Result**:
- `_track_campaign_responses` finds LinkedIn touchpoint for Dori Kafri.
- Touchpoint `status` → `"completed"`.
- `responses` gets `{"type": "linkedin_connected", "timestamp": "..."}`.
- Engagement score recalculated:
  - 1 completed touchpoint (0.3 weight), 0 replied = score ~30%.

---

### TC-WP-010: Backend — Adaptive Rules
**Objective**: Verify adaptive rules fire and change campaign state.

**Test A — Positive Reply Pauses Campaign**:
- Setup: Campaign with rule `{trigger: "any_positive_reply", action: "pause_remaining"}`.
- Condition: A touchpoint has `responses` with `sentiment: "positive"`.
- Run backend.
- **Expected**: Campaign status → `paused`. `executionState.pausedAt` set.

**Test B — No Response 7 Days**:
- Setup: Campaign created > 7 days ago with 0 responses.
- Rule: `{trigger: "no_response_7days", action: ...}`.
- Run backend.
- **Expected**: Rule fires and applies defined action.

---

### TC-WP-011: Queue Card — Wolf Pack Context Display
**Objective**: Verify queue entries from campaigns show inline campaign context.

**Preconditions**: Queue entry with `campaignId` set for a Develeap campaign.

**Steps**:
1. Go to Queue tab.
2. Find Dori Kafri entry created by Wolf Pack.

**Expected Result**:
- Queue card shows Wolf Pack context section (via `_renderQueueWpContext`).
- Displays: campaign name, target job title.
- Mini progress dots showing completed nodes.
- Channel summary badges (LinkedIn, Email, WhatsApp status).
- Expandable sequence view (via `_wpRenderSequence`).
- Link to view / edit campaign.

---

### TC-WP-012: Multi-Channel Summary
**Objective**: Verify `_wpContactChannelSummary` correctly aggregates status.

**Steps**:
1. Create campaign with Dori Kafri as contact.
2. Add queue entries: 1 LinkedIn connect (completed), 1 email (pending), 1 WhatsApp (pending).
3. Render queue card.

**Expected Result**:
- `_wpContactChannelSummary("campId", "Dori Kafri")` returns:
  - LinkedIn: completed.
  - Email: pending.
  - WhatsApp: pending.
- Priority status = pending (not all done).
- Channel badges in queue card reflect above.

---

## Integration Tests

### TC-INT-001: End-to-End Queue → Campaign Flow
**Objective**: Full journey from campaign creation to queue action to completion.

**Steps**:
1. Create Wolf Pack campaign for Develeap / Dori Kafri (TC-WP-001).
2. Verify campaign active.
3. Simulate backend execution: set `executionState.nextExecutionAt` to a past time in `wolfpack_campaigns.json`.
4. Run `python process_queue.py` (or manually trigger).
5. Check `linkedin_queue.json` for new entry.
6. Navigate to Queue tab; locate Dori Kafri entry with Wolf Pack context.
7. Mark entry as "✓ Accepted".
8. Verify: outreach_status updated, SOS advances if applicable.
9. Re-run backend; verify next campaign node executes.

**Expected Result**: Full pipeline from campaign → queue entry → user action → campaign advancement works without errors.

---

### TC-INT-002: SOS + Wolf Pack Combined
**Objective**: Verify SOS sequence started from Wolf Pack campaign works correctly.

**Steps**:
1. Campaign action node creates a `connect` queue entry for Dori Kafri with `sequenceId`.
2. Entry 1 status = `pending`; entries 2–5 = `sos_waiting`.
3. User accepts connection (TC-Q-006).
4. Verify entry 2 advances to `pending`.
5. User completes entry 2; entry 3 advances.
6. Continue through all 5 steps.

**Expected Result**: SOS sequence completes all 5 touchpoints for Dori Kafri without manual intervention per step.

---

## Edge Cases & Error Handling

### TC-EC-001: Empty Queue
**Steps**: Clear all entries, open Queue tab.
**Expected**: "Queue is empty" message shown. No errors. Badge counts all show 0.

### TC-EC-002: Queue Entry With Missing Campaign
**Steps**: Add queue entry with `campaignId: "nonexistent_id"`. Render queue.
**Expected**: `_renderQueueWpContext` returns empty/no context. No JS crash. Entry renders normally without Wolf Pack section.

### TC-EC-003: GitHub SHA Conflict
**Steps**: Two browser tabs both load queue. Tab 1 saves an entry. Tab 2 tries to save an entry (stale SHA).
**Expected**: Tab 2 gets "GitHub push failed: 409". Error toast shown. Queue not corrupted. User should reload and retry.

### TC-EC-004: Wolf Pack Campaign With Zero Contacts
**Steps**: Create campaign with 0 contacts, attempt to launch.
**Expected**: Wizard prevents launch at Step 3/5 — validation error "Add at least one contact". Campaign not created.

### TC-EC-005: Delete Node That Is Current Execution Target
**Steps**: Edit flow, delete the node currently in `executionState.currentNodeId`. Save flow.
**Expected**: Backend `_execute_campaign_nodes` returns 0 (node not in nodes map). Campaign does not crash. Log: "currentNodeId not found in nodes".

### TC-EC-006: Bulk Operations With No Matching Entries
**Steps**:
- Click "Retry Failed" when no failed entries exist.
- Click "Clear Done" when no completed entries exist.
**Expected**: Toast "No failed entries to retry" / "No completed entries to clear". No unnecessary GitHub saves.

### TC-EC-007: Queue Global Pause Then Add Entry
**Steps**:
1. Click "Pause All" (global pause active).
2. Add a new entry for Dori Kafri.
**Expected**: New entry is added as `pending` (not auto-paused). Global pause only affects entries at the moment the button is clicked, not future entries.

### TC-EC-008: Campaign Status = `completed` Not Re-Executed
**Steps**: Set a campaign `status: "completed"`. Run `process_queue.py`.
**Expected**: `process_wolf_pack_campaigns` skips it (`if campaign.get("status") != "active": continue`). No new queue entries created.

---

## Backend / GitHub Actions Tests

### TC-BA-001: GitHub Action Trigger
**Objective**: Verify `hubspot-sync.yml` runs the full pipeline.

**Steps**:
1. Check GitHub Actions tab for `hubspot-sync.yml`.
2. Observe the workflow steps: `sync_hubspot.py` → `enrich_apollo.py` → `process_queue.py`.
3. Verify `linkedin_queue.json` and `wolfpack_campaigns.json` are committed after the run.

**Expected Result**:
- All three scripts exit 0.
- Commit message references queue/wolf pack processing.
- `queue_execution_log.json` updated with run stats.

### TC-BA-002: Rate Limiting Respected
**Steps**: Set up >20 pending `connect` entries. Run backend.
**Expected**: Backend processes at most 20 CRM creates per run (see `MAX_CRM_CREATES_PER_RUN`). Remaining entries stay `pending` for next run.

### TC-BA-003: Wolf Pack Stats Logged
**Steps**: Run `process_queue.py` with active campaigns.
**Expected**: Output includes Wolf Pack stats:
```
Wolf Pack: activeCampaigns=1, nodesExecuted=1, responsesTracked=0
```
Stats saved to `queue_execution_log.json`.

---

## Test Data Reference

All tests should use **only** the following test contact and company:

| Field | Value |
|-------|-------|
| Name | Dori Kafri |
| Email | dori.kafri@develeap.com |
| Phone | +972542289888 |
| LinkedIn | Dori Kafri on LinkedIn |
| Company | Develeap |
| Role | CEO |
| Customer Type | customer |

---

## Test Checklist

### Queue
- [ ] TC-Q-001: Add entry to queue
- [ ] TC-Q-002: Pause entry
- [ ] TC-Q-003: Resume entry
- [ ] TC-Q-004: Retry failed entry
- [ ] TC-Q-005: Delete entry
- [ ] TC-Q-006: Mark connection accepted
- [ ] TC-Q-007: Global pause / resume
- [ ] TC-Q-008: Clear completed entries
- [ ] TC-Q-009: Retry all failed
- [ ] TC-Q-010: SOS sequence advance
- [ ] TC-Q-011: User switching
- [ ] TC-Q-012: Persistence after reload

### Wolf Pack
- [ ] TC-WP-001: Create campaign (full wizard)
- [ ] TC-WP-002: Campaign card rendering
- [ ] TC-WP-003: Pause / resume campaign
- [ ] TC-WP-004: Archive campaign
- [ ] TC-WP-005: Detail view
- [ ] TC-WP-006: Flow editor
- [ ] TC-WP-007: Backend node execution
- [ ] TC-WP-008: Backend wait node
- [ ] TC-WP-009: Backend response tracking
- [ ] TC-WP-010: Adaptive rules
- [ ] TC-WP-011: Queue card Wolf Pack context
- [ ] TC-WP-012: Multi-channel summary

### Integration
- [ ] TC-INT-001: End-to-end queue → campaign flow
- [ ] TC-INT-002: SOS + Wolf Pack combined

### Edge Cases
- [ ] TC-EC-001: Empty queue
- [ ] TC-EC-002: Missing campaign ID
- [ ] TC-EC-003: GitHub SHA conflict
- [ ] TC-EC-004: Zero contacts in wizard
- [ ] TC-EC-005: Delete current execution node
- [ ] TC-EC-006: Bulk operations with no matches
- [ ] TC-EC-007: Add entry while globally paused
- [ ] TC-EC-008: Completed campaign not re-executed

### Backend
- [ ] TC-BA-001: GitHub Action trigger
- [ ] TC-BA-002: Rate limiting
- [ ] TC-BA-003: Wolf Pack stats logged
