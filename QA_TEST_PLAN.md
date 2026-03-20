# QA Test Plan — Queue & Wolf Pack Mechanisms
**Project:** BDR Job Monitor (Develeap)
**Scope:** LinkedIn Outreach Queue + Wolf Pack Campaign Engine
**Tester:** _______________  
**Date:** _______________  
**Environment:** https://develeap-bdr-jobs.netlify.app

---

## Test Contact (use ONLY this contact for all tests)

| Field    | Value                      |
|----------|----------------------------|
| Name     | Dori Kafri                 |
| Phone    | 972542289888               |
| Email    | dori.kafri@develeap.com    |
| LinkedIn | Dori Kafri                 |
| Company  | Develeap                   |

---

## Part 1 — LinkedIn Outreach Queue

### Pre-requisites

- [ ] GitHub Personal Access Token (PAT) is configured in Settings → Integrations
- [ ] You are signed in with a `@develeap.com` Google account
- [ ] At least one job listing for **Develeap** is visible in the Jobs table

---

### TC-Q-01 — Queue a LinkedIn connect (happy path)

**Description:** Verify that clicking the LinkedIn connect button for Dori Kafri creates a queue entry in GitHub and updates the button state.

**Steps:**
1. Open the dashboard and navigate to the Jobs view.
2. Find a Develeap job listing and open its outreach panel (click the row or outreach button).
3. Locate the stakeholder card for **Dori Kafri**.
4. Click the LinkedIn **Connect** button on Dori Kafri's card.

**Expected Result:**
- Button immediately changes to `⏳ Queued` (orange).
- After the GitHub write completes, button turns `✅ Queued` (green).
- Toast notification: `✅ Dori Kafri added to LinkedIn queue — Claude will send automatically.`
- Navigate to the **Queue** view → a new card for Dori Kafri appears with status `pending` (orange left border).

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-Q-02 — Queue with missing GitHub token (error path)

**Description:** Verify that a clear error is shown when no GitHub PAT is configured.

**Steps:**
1. Go to **Settings → Integrations** and clear / remove the GitHub token.
2. Navigate to a Develeap listing's outreach panel.
3. Click the LinkedIn **Connect** button for Dori Kafri.

**Expected Result:**
- Button turns `❌ Failed` (red) immediately after the attempt.
- Toast error message: `❌ Add your GitHub token in Settings → Integrations` (or similar).
- **No** network request is made to GitHub.
- Queue view does NOT show a new pending entry.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

**Cleanup:** Restore the GitHub token before continuing.

---

### TC-Q-03 — Queue view status display

**Description:** Verify that all queue status badges render correctly.

**Steps:**
1. Open the **Queue** view.
2. Observe any existing queue entries for Dori Kafri.
3. Verify the color-coded status chips match the table below:

| Status          | Expected chip colour / label       |
|-----------------|------------------------------------|
| `pending`       | Orange — "Pending"                 |
| `conn_pending`  | Purple — "Conn Pending"            |
| `failed`        | Red — "Failed"                     |
| `message_sent`  | Blue — "Message Sent"              |
| `email_sent`    | Pink — "Email Sent"                |
| `completed`     | Grey / green — "Completed"         |

4. Confirm the sidebar badge (`Queue N`) reflects the number of **pending/processing** entries only.

**Expected Result:** Each visible card matches the colour scheme above; sidebar count is accurate.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-Q-04 — Retry a single failed queue item

**Description:** Verify that a failed queue item can be manually retried.

**Steps:**
1. If no failed entry exists, temporarily break the GitHub token, queue a connect for Dori Kafri, then restore the token — this may leave an entry in a failed-like state. Alternatively use a known failed entry.
2. Open the **Queue** view.
3. Locate the failed entry for Dori Kafri.
4. Click the **Retry** (or re-queue) action on that card.

**Expected Result:**
- The entry's status resets to `pending`.
- The card left border changes to orange.
- A toast confirms the retry was queued.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-Q-05 — Retry All Failed

**Description:** Verify the "↻ Retry Failed" bulk action resets all failed entries.

**Steps:**
1. Ensure at least one failed queue entry exists (see TC-Q-04 setup).
2. Click the **↻ Retry Failed** button in the Queue toolbar.
3. Observe the `Failed` count badge on the button.

**Expected Result:**
- All failed entries are reset to `pending`.
- The failed count badge drops to `0`.
- Toast confirms the bulk retry.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-Q-06 — Clear Completed

**Description:** Verify the "🗑 Clear Done" button removes only completed entries.

**Steps:**
1. Ensure at least one `completed` entry exists in the queue (may need to wait for Claude automation to process a pending entry, or manually set one to completed via GitHub).
2. Note the count shown on the **Clear Done** badge.
3. Click **🗑 Clear Done**.
4. Confirm the dialog/warning (if any) and proceed.

**Expected Result:**
- All entries with status `completed` (and `message_sent`, `email_sent` as applicable) are removed.
- Pending/failed entries remain untouched.
- `Clear Done` badge resets to `0`.
- Queue view refreshes automatically.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-Q-07 — Switch queue user (delegation)

**Description:** Verify the user-select dropdown switches the queue view between users.

**Steps:**
1. Open the **Queue** view.
2. Use the user selector dropdown at the top of the page.
3. Switch between `dori.kafri@develeap.com` and any other available user.

**Expected Result:**
- Displayed queue entries change to reflect the selected user's queue.
- Sidebar badge updates accordingly.
- No entries from other users bleed into the view.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-Q-08 — Manual Refresh

**Description:** Verify the ↻ Refresh button re-fetches the queue from GitHub.

**Steps:**
1. In a second browser tab, manually edit `linkedin_queue.json` in GitHub (add a dummy entry), or simply wait for a GitHub Actions run to update the file.
2. Return to the Queue view.
3. Click **↻ Refresh**.

**Expected Result:**
- The new/updated entry appears without a full page reload.
- No error toast is shown.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

## Part 2 — Wolf Pack Campaign Mechanism

### Pre-requisites

- [ ] GitHub PAT configured (same as queue pre-req)
- [ ] Signed in with a `@develeap.com` Google account
- [ ] Wolf Pack view is accessible via the left sidebar

---

### TC-WP-01 — Create a new campaign (wizard happy path)

**Description:** Create a Wolf Pack campaign targeting Develeap with Dori Kafri as a contact.

**Steps:**
1. Click **Wolf Pack** in the left sidebar.
2. Click **+ New Campaign**.
3. **Step 1 — Company:**
   - Company Name: `Develeap`
   - Domain: `develeap.com`
   - Category / Industry: `DevOps / Cloud`
   - Click **Next**.
4. **Step 2 — Job:**
   - Job Title: `Test QA Position`
   - Leave URL blank.
   - Click **Next**.
5. **Step 3 — Contacts:**
   - Add Dori Kafri: Name `Dori Kafri`, Title `Head of BDR`, LinkedIn URL (Dori's profile URL).
   - Click **Next**.
6. **Step 4 — Review / Save:**
   - Confirm details look correct.
   - Click **Create Campaign** (or **Save**).

**Expected Result:**
- Campaign appears in the Wolf Pack list as a card labelled **Develeap**.
- Campaign card shows 1 contact.
- No error toasts.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-02 — Add a node in the flow editor

**Description:** Open the campaign in the flow editor and add an outreach step node.

**Steps:**
1. Open the Develeap campaign created in TC-WP-01.
2. The flow editor (canvas) opens.
3. In the sidebar node palette, click **LinkedIn Connect** (or drag it onto the canvas).
4. Verify a node card appears on the canvas.

**Expected Result:**
- Node appears at a reasonable position on the canvas.
- Node is labelled correctly (e.g., "LinkedIn Connect").
- **Undo** button in the toolbar becomes enabled.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-03 — Add multiple outreach step nodes

**Description:** Add one node for each outreach channel.

**Steps:**
1. With the Develeap campaign flow editor open, add the following nodes (in order):
   - **LinkedIn Connect**
   - **Email**
   - **WhatsApp**
2. Observe the canvas.

**Expected Result:**
- All three nodes appear on the canvas.
- Each node has the correct icon / label for its channel.
- Nodes do not overlap (or are at least distinguishable).

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-04 — Undo (Ctrl+Z) in the flow editor

**Description:** Verify that Ctrl+Z reverses the last node addition.

**Steps:**
1. With the Develeap campaign flow editor open (and at least one node added — from TC-WP-03).
2. Press **Ctrl+Z** (or click the **↩ Undo** toolbar button).
3. Observe the canvas.

**Expected Result:**
- The most recently added node disappears.
- **↪ Redo** button becomes enabled.
- **↩ Undo** button remains enabled if more actions exist in the stack.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-05 — Redo (Ctrl+Y) in the flow editor

**Description:** Verify that Ctrl+Y re-applies an undone action.

**Steps:**
1. Immediately after TC-WP-04 (Undo was performed).
2. Press **Ctrl+Y** (or **Ctrl+Shift+Z**, or click **↪ Redo**).

**Expected Result:**
- The previously removed node reappears in the same position.
- **↩ Undo** button re-enables.
- **↪ Redo** button disables (stack exhausted) if no further redo state exists.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-06 — Undo stack cleared after new action

**Description:** After undoing and then adding a new node, the redo stack should be cleared.

**Steps:**
1. Start with a fresh flow (or clear the existing one).
2. Add Node A.
3. Press Ctrl+Z (undo Node A).
4. Add Node B (new action).
5. Press Ctrl+Y (attempt redo).

**Expected Result:**
- Node A does NOT reappear (redo stack was cleared when Node B was added).
- Only Node B is visible.
- **↪ Redo** button remains disabled after the attempt.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-07 — Save campaign flow

**Description:** Verify that clicking **Save Flow** persists the campaign to GitHub.

**Steps:**
1. With the Develeap campaign flow editor open and nodes added.
2. Click **Save Flow** in the toolbar.

**Expected Result:**
- Toast: "Campaign saved" (or similar success message).
- No error toast.
- After a hard page reload, navigate back to the Develeap campaign — the nodes are still present.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-08 — Load / reopen saved campaign

**Description:** Verify that a saved campaign loads correctly after a page reload.

**Steps:**
1. Save the Develeap campaign (TC-WP-07 completed).
2. Do a full browser page reload (Cmd+Shift+R / Ctrl+Shift+R).
3. Navigate to Wolf Pack.
4. Click on the Develeap campaign card.

**Expected Result:**
- The flow editor opens with all previously saved nodes intact.
- Contact (Dori Kafri) is still listed.
- No console errors.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-09 — Campaign execution flow (queue integration)

**Description:** Verify that launching a Wolf Pack campaign queues outreach items in the Queue view.

**Steps:**
1. Open the Develeap campaign.
2. Locate the **Execute** / **Run** / **Launch** action (button in the campaign card or detail view).
3. Confirm the execution prompt if shown.
4. Navigate to the **Queue** view.

**Expected Result:**
- New queue entries appear for **Dori Kafri** corresponding to each outreach step in the campaign.
- Each entry shows the correct status (`pending`).
- Entries are attributed to the currently logged-in user.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-10 — Fit to screen (canvas zoom)

**Description:** Verify the Fit / zoom controls work correctly.

**Steps:**
1. Open the Develeap campaign flow editor with several nodes.
2. Use the **−** and **＋** zoom buttons to zoom in and out.
3. Click **⊞ Fit** to reset the view.

**Expected Result:**
- Zooming changes the canvas scale smoothly.
- **Fit** button resets the view so all nodes are visible within the viewport.
- No layout corruption after zoom changes.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-11 — Edge case: Empty campaign execution

**Description:** Verify that attempting to run a campaign with no contacts / no nodes shows an appropriate error.

**Steps:**
1. Create a new campaign (wizard) for Develeap but **skip adding contacts** (if the wizard allows).
2. OR open an existing campaign and remove all nodes from the canvas.
3. Attempt to execute / launch the campaign.

**Expected Result:**
- An error message or validation warning is shown (e.g., "Add at least one contact before running").
- No queue entries are created.
- The campaign is not marked as active/running.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-12 — Edge case: Duplicate contact node

**Description:** Verify behavior when the same contact (Dori Kafri) is added to the flow more than once.

**Steps:**
1. Open the Develeap campaign flow editor.
2. Add Dori Kafri as a contact node.
3. Attempt to add Dori Kafri again (same name/email).

**Expected Result:**
- Either: A warning is shown ("Dori Kafri is already in this campaign") and the duplicate is not added.
- Or: The duplicate is added but clearly distinguished (e.g., a visual warning indicator).
- Running the campaign does NOT send duplicate outreach messages.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

### TC-WP-13 — Mobile layout

**Description:** Verify the Wolf Pack editor is usable on a narrow viewport.

**Steps:**
1. Open the dashboard in a browser window resized to ≤ 768 px wide (or use DevTools mobile emulation).
2. Navigate to Wolf Pack.
3. Open the Develeap campaign.

**Expected Result:**
- The editor layout switches to a single-column stacked view (sidebar on top, canvas below, config panel at bottom).
- Toolbar buttons remain accessible.
- No horizontal overflow / content cut off.

**Status:** ☐ Pass  ☐ Fail  ☐ Blocked

---

## Defect Log

| ID | TC Reference | Severity | Description | Reproducible | Reported By |
|----|-------------|----------|-------------|--------------|-------------|
|    |             |          |             |              |             |
|    |             |          |             |              |             |

---

## Sign-off

| Role      | Name | Signature | Date |
|-----------|------|-----------|------|
| Tester    |      |           |      |
| Reviewer  |      |           |      |
