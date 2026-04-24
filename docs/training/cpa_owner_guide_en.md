# CPA Firm Owner Guide — OtoCPA

For the person who sets up the firm, adds clients, manages employees,
and oversees all operations. If you're a CPA employee (not owner),
see [cpa_employee_guide_en.md](cpa_employee_guide_en.md) instead.

## 1. First-time setup

1. Receive your welcome email and click **Set up my account**.
2. Set a password of at least 10 characters (1 digit and 1 letter
   minimum).
3. Enable **two-factor authentication** (required for owners). Use an
   app like Google Authenticator or Authy.
4. Complete your profile: display name, WhatsApp number (if you want
   alerts there), preferred language.

## 2. Adding clients

Menu **Clients → + New client**. Each client has:

- a unique **client code** (technical identifier),
- a **display name** (visible on the dashboard),
- a **contact email** (for automated sends),
- a **fiscal year-end** (for close wizard and returns),
- a **portal mode**: *single* (shared link) or *multi* (personal
  invitations).

## 3. Enabling the multi-user portal

For each client whose bookkeeping team is more than one person:

1. Open the client record.
2. Click **Portal mode → Multi**.
3. The first admin user is auto-created from the contact email; they
   receive a personal link.
4. They can invite the rest of their team from inside their portal.

**Benefit**: every upload is tagged with the uploader; you can filter
the queue by person with the *Uploaded by* dropdown.

## 4. Managing your internal team

Menu **Settings → Employees**:

- **Invite a new employee**: email + role (admin / reviewer /
  bookkeeper).
- **Suspend** an employee (access blocked but history preserved).
- **Remove** an employee (tokens invalidated immediately).
- **Change role** for an existing employee.

## 5. Review queue

The **Home** page lists all pending documents:

- **Filter** by client, status, period, or uploader (dropdown with
  colored badges).
- **Assign** a document to an employee: *Assign* button.
- **Approve / Reject / Escalate** each document.
- **Bulk approve** via the header checkbox.

## 6. Month-end close wizard

Menu **Close → New close**. Six guided steps:

1. **Verify balances** — bank and cash.
2. **Reconcile** unreconciled transactions.
3. **Trial balance** — detect anomalies.
4. **Accruals** — wages, depreciation, prepaid expenses. You can
   edit each line before posting.
5. **Financial statements** — BS, P&L, cash flow, SOCE.
6. **Post** — idempotent (a double-click won't double-post).

## 7. Connecting QuickBooks (per client)

Menu **Clients → [Client] → Integrations → QuickBooks**:

1. **Authorize OAuth** to QBO (one click, Intuit redirect).
2. **Flow direction**: bidirectional by default. You can choose
   *pull only* or *push only* per client.
3. **Conflict resolution**: automatic; unresolved conflicts show up
   in the review queue.

## 8. Smart bank routing

For each client:

- If the bank is **already connected to QuickBooks**, OtoCPA pulls
  transactions from QBO automatically. **No Plaid connection
  needed.**
- Otherwise, OtoCPA invites the client to connect via **Plaid**
  (secure portal, no bank credentials stored with you or us).

## 9. Reports and financial statements

Menu **Reports**:

- **Trial balance** (TB)
- **Profit & Loss** (P&L)
- **Balance sheet** (BS)
- **Cash flow**
- **SOCE** (statement of changes in equity)
- **By-uploader report** (who submitted how many receipts this month)
- **Aging** (A/R, A/P)

All exportable to PDF or CSV (CSV with UTF-8 BOM for Excel).

## 10. Audit engagements

Menu **Audit → New engagement**:

- **Materiality** (CAS 320)
- **Risk assessment** (CAS 315)
- **Statistical sampling**
- **Representation letters** (PDF output in FR or EN)
- **Review notes** per account
- **Structured working papers**

## 11. Tax returns

- **T2** (federal corporate) — auto-prefill of schedules 1, 8, 50,
  100, 125.
- **CO-17** (Quebec) — auto-mapped from T2.
- **GST/QST** — calculation and pre-fill.
- **T5013** (partnership) — per-partner slips.
- **T661** (SR&ED) — structured narratives.

## 12. Client messaging

Each client has a two-way message thread:

- Send from **Clients → [Client] → Messages**.
- Pick recipient (all, or a specific user in multi mode).
- Client gets a notification (email and/or WhatsApp per their
  preferences).

## 13. Seeing who sent what

Every document in the review queue carries:

- **Uploader badge** (coloured chip with the person's name).
- **Channel badge** (Portal / WhatsApp / Email / API / Manual) —
  the portal chip is hidden to reduce noise, so a WhatsApp chip
  means exactly that: this receipt arrived via Twilio.

**Queue filters**

- *By uploader* — multi-select dropdown, one entry per uploader
  currently in scope with counts.
- *By channel* — same pattern. Use it when you want to verify all
  WhatsApp submissions in a date range (handy for debugging Twilio
  routing or auditing a single contributor).

**Per-uploader report**

**Reports → By uploader** shows each contributor's volume with a
*Channel* column: "Marie: 45 portal / 23 WhatsApp" gives you the
channel mix at a glance. CSV export includes the channel
breakdown too.

**CPA override for WhatsApp numbers**

When a client admin is unavailable, **Clients → [Client] →
Portal users** lets you reassign or clear a WhatsApp number
directly. Every override is logged with `(cpa)` suffixed to the
actor email so the audit trail distinguishes CPA actions from
client actions.

## 14. Correcting document lines

OCR produces one line per invoice row, but it isn't always right: a
mixed purchase can land on one line, a single item's name can be cut
in two, a service may need to be allocated across two GL accounts.
The document detail page offers three actions from the *Line items*
card:

**Split** — one line → several lines on the same document. Example:
*Metro Plus $127.50* split into *Grocery $84.00* (tax Z) and
*Cleaning supplies $43.50* (tax T). The sum of the new amounts must
equal the original, to the cent.

**Merge** — several lines → one. Example: OCR read *Pain aux* and
*raisins* as two rows when it's a single item. Tick the checkboxes
on the left of the rows to merge; a toolbar appears at the top with
a *Merge* button.

**Allocate** — one line → several GL accounts, by amount or
percentage. Example: Internet $100 allocated 60% to Operating
expenses (5500) and 40% to Taxable benefits (2320).

**Badge**: every CPA-modified line carries a small badge (*Split*,
*Merged*, *Allocated*) next to its description, so you can tell it
apart from the original OCR line.

**Audit trail**: the document detail page shows a *Line item history*
section (open by default when modifications exist) listing each
operation with who, when, why, and before/after state. Nothing is
ever deleted — original lines remain in the database, soft-deleted
only, so the audit trail is complete.

Each operation enforces version-based concurrency: if two reviewers
touch the same line, one wins and the other sees a *Reload page*
message. A client request id makes each operation idempotent —
replaying the same request does not double-split.

## Migration from a previous product (Scope 2)

If you're moving existing clients in from Caseware / Sage / Excel /
QuickBooks:

1. **Bulk CSV import**: Menu **Clients → Bulk import**. Download the
   template, fill one row per client, upload, preview validation
   errors, then commit or dry-run. Rejected rows come back as a
   downloadable CSV with reasons.
2. **QBO historical pull**: for each QBO-connected client, menu
   **Clients → Imported data**. Pulls two complete years + YTD,
   shows totals per year, lets you **Confirm** or **Rollback**.
3. **Opening balances**: menu **Clients → Opening balances**.
   Enter the as-of date and per-account balance; the trial balance
   must balance (|Σ debits − Σ credits| ≤ $0.01) or posting is
   refused. Attach supporting PDFs.
4. **Other formats**: menu **Clients → Historical import** accepts
   generic CSV, IIF (QuickBooks Desktop), Excel trial balance,
   Sage 50, and Caseware TB exports. Auto-detects format, shows
   a 20-row preview, prompts for account mapping when the source
   CoA differs from the client's OtoCPA CoA, and retains the raw
   file (sha256 recorded).
5. **Prior-year comparative**: financial statements show the
   imported prior-year column with a provenance label ("imported
   from Caseware / Sage 50 / …") and a disclosure note.

## Lifecycle events (Scope 3)

- **Archive a client**: Menu **Clients → Edit → Archive**. Pick a
  reason (left_firm / business_closed / dormant / other). Portal
  tokens are revoked, new uploads are refused, history is
  preserved for the regulatory 7-year retention window. List +
  reactivate from **Clients → Archived**.
- **Employee out of office**: Menu **Settings → Out of office**.
  Set start/end date and coverage colleague; new documents route
  to the coverage employee (existing assignments are untouched).
  End early by clicking **End / Terminer**.
- **Employee leaves the firm**: Menu **Admin → Team → Depart**.
  Specify a replacement; every open workflow, client primary, and
  secondary is rebalanced to them. Dashboard access is revoked.
- **Workload balancing**: Menu **Admin → Workload** shows per-
  employee queue depth with green/yellow/red colour coding.
  Inline form to bulk-reassign from one employee to another.
- **Recurring reminders**: On each client page, **Schedule a
  recurring reminder** — one-time, weekly, monthly, quarterly,
  annually. Pick from the canned templates (monthly bank
  statement, quarterly GST/QST, annual T4 summary, year-end docs)
  or write your own bilingual title + body.
- **Inactive clients**: Menu **Admin → At-risk clients** lists
  anyone dormant for 90+ days. Nudge or archive.

## FAQ

**Q: How do I revoke a client employee's access immediately?**
A: Open the client record → Portal users → *Remove* button. Tokens
are invalidated on the spot.

**Q: Can a client have their own admin?**
A: Yes, in multi mode. The first admin can promote others.

**Q: Do legacy `/c/{token}` links still work?**
A: Yes in *single* mode; in *multi* mode they go to a "Use your
personal link" page.

## Getting help

- **Docs**: `docs/` in the repo.
- **Email**: support@otocpa.com
- **In-app**: help icon top-right (shortcuts + guided tour).
