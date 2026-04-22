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
