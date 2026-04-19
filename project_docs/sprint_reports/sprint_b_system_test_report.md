# Sprint B — Full System End-to-End Test Report
**Date:** 2026-04-19
**Scope:** 13 major workflows, pre-CPA-friend readiness check
**Method:** live-server HTTP probes + code inspection (3 parallel Explore agents + manual verification of cited line numbers). No bug fixes applied per instruction.

## Status summary

| Phase | Workflow | Status |
|---|---|---|
| 1 | CPA onboarding (Stripe → firm → login) | ⚠️ PARTIAL |
| 2 | Client management + portal | ✅ PASS |
| 3 | Receipt review workflow | ⚠️ PARTIAL |
| 4 | Bank integration + auto-match | ⚠️ PARTIAL |
| 5 | QBO posting | ⚠️ PARTIAL |
| 6 | Reconciliation | ⚠️ PARTIAL (not deeply probed) |
| 7 | Financial statements (TB, P&L, BS) | ❌ FAIL |
| 8 | Period close | ⚠️ PARTIAL |
| 9 | Journal entries | ❌ FAIL |
| 10 | Audit (engagements, rep letter, working papers) | ⚠️ PARTIAL |
| 11 | Tax (GST/QST, T2) | ✅ PASS |
| 12 | Reports (aging, cash flow, fixed assets/CCA) | ✅ PASS |
| 13 | Multi-user / roles | ❌ FAIL |

**Counts:** PASS = 3, PARTIAL = 7, FAIL = 3.

---

## Phase 1 — CPA onboarding ⚠️ PARTIAL

**Works:** /signup and /signup/checkout redirect to Stripe; /signup/success imports the Stripe session; `_handle_stripe_event` at `scripts/review_dashboard.py:12217` creates firm + first user + sends set-password email. `/change_password` and set-password token paths exist.

**Doesn't work / risky:**
- **`/health` reports `users_count=0`** — verified at `scripts/review_dashboard.py:15344` — `SELECT COUNT(*) AS c FROM users` queries a non-existent table. Table is `dashboard_users` (6 rows actually).
- Stripe webhook has no `event.id` idempotency table; if Stripe retries, the firm/user gets created twice.
- Two different password validators: `/set-password` requires 10 chars + letter + digit; `/change_password` only 8 chars — new users can downgrade password strength on first login.

## Phase 2 — Client management ✅ PASS

Portal-token auto-generation on `/clients/save` works. `_require_client_in_firm` is used consistently on edit/delete/message paths. `/c/{token}` portal (unauthenticated) is token-scoped via `resolve_portal_token`. Uploads from the portal now go through the async queue (Sprint A).

## Phase 3 — Receipt review ⚠️ PARTIAL

**Works:** /queue loads; /document?id= renders; /document/update triggers `record_learning_corrections` which now writes to `vendor_learning` and `correction_log` (Sprint A). Status transitions include "Ready to Post".

**Bugs:**
- **`gst_amount` / `qst_amount` not editable per line.** `/document/line_item/save` at `scripts/review_dashboard.py:18838` only updates `gl_account`, `tax_code`, `description`. Line-level tax is display-only.
- Document edit form (line ~14800) doesn't expose `subtotal`, `tax_total`, `gst`, `qst` as inputs. If the CPA disagrees with the extracted numbers she has to edit via raw SQL or a JE.

## Phase 4 — Bank integration ⚠️ PARTIAL

**Works:** `/bank/connect`, `/bank/callback`, `/bank/sync` actually call Plaid via `src/integrations/plaid_client.py`. `bank_connections` table populated (1 row exists).

**Bugs:**
- **No `/bank/unmatch` route.** Once `/bank/match` sets `reconciled=1` there is no undo. CPA has to SQL-delete to retry.
- **Auto-match tolerance is ±$0.02 absolute on amount and ±7 days on date** — too loose for large transactions ($10,000 invoice would match a $9,999.98 transaction). `review_dashboard.py:329-371`.
- **No confidence threshold on auto-apply.** Matches apply immediately on word overlap; low-confidence matches should queue for review. `review_dashboard.py:402-436`.

## Phase 5 — QBO posting ⚠️ PARTIAL

**Works:** `/qbo/connect`, `/qbo/callback`, `/qbo/build`, `/qbo/approve` exist; per-client `qbo_connections` model exists in `qbo_online_adapter.py`. `qbo_connections` table is empty (no one has actually connected).

**Bugs:**
- If a client has no `qbo_connections` row, `post_one_ready_job` returns `skipped_no_connection` but still marks the document `post_failed`. UI shows "failed" misleadingly. `qbo_online_adapter.py:716-729`.
- Period-lock check is inconsistent across approval paths — some `/qbo/*` handlers don't call `_check_period_not_locked_for_doc`.

## Phase 6 — Reconciliation ⚠️ PARTIAL (not deeply probed)

Route exists at `/reconciliation` and `/reconciliation_report/*`; queries `bank_reconciliations` + `reconciliation_items` tables (both present in schema). **Did not fully probe** matched/unmatched split and PDF export; flag for deeper pass in a follow-up sprint.

## Phase 7 — Financial statements ❌ FAIL

**Works:** Routes exist. Tax/T2 engines pull from GL correctly. Chart of accounts and aging pipelines are wired.

**Critical bugs:**
- **Balance-sheet structure mismatch with the renderer.** `generate_financial_statements()` returns `{"assets": {"current": [...], "non_current": [...]}}` (`audit_engine.py:1293`). The renderer iterates `bs.get("current_assets")` (flat). **Balance sheet renders empty.** Same shape mismatch for liabilities, equity, revenue, expenses.
- **Trial balance is never validated to balance.** `generate_trial_balance()` at `audit_engine.py:1183` sums by account type but does not assert `sum(debits) == sum(credits)`. An unbalanced TB sails through silently.
- Financial-statement PDF export silently renders unbalanced statements (no guard).

## Phase 8 — Period close ⚠️ PARTIAL

**Works:** `/period_close` page renders a checklist; `_check_period_not_locked_for_doc` is called in document update / line-item save / JE post. `period_close` table is empty (never exercised).

**Bugs:**
- **No automated reversal / accrual entry creation at period end** — checklist items are manual text only.
- Period-lock check is called from document paths but missing from `/qbo/approve` and at least one `/assign` variant (see Phase 5).

## Phase 9 — Journal entries ❌ FAIL

**Critical bugs:**
- **JE save does not enforce debit == credit.** Single-line schema (debit_account, credit_account, amount) — both sides share one `amount`. If a CPA enters amounts per side in the UI they can be unbalanced and save succeeds silently. `scripts/review_dashboard.py:20700-20762`.
- **Posted JEs do not write to any general-ledger table.** `POST /journal_entries action=post` at `scripts/review_dashboard.py:20773-20784` only flips `manual_journal_entries.status='posted'`. Nothing flows into a GL ledger, so trial balance + P&L never reflect manual JEs.
- Phantom-tax check is limited to ITC/ITR accounts (2200, 2210); any GST/QST on expense outside those codes slips through.

## Phase 10 — Audit ⚠️ PARTIAL

**Works:** Full engagement lifecycle (`planning → fieldwork → review → complete → issued` in `audit_engine.py:1765`), risk/materiality/sample/analytical/evidence endpoints all wired. Bilingual EN/FR rep-letter generator exists.

**Bugs:**
- **Rep letter is plain-text only** — no PDF export, no signature capture (`cas_engine.py:837-905`).
- Engagement state machine doesn't block `fieldwork` until `materiality_locked=1`.

## Phase 11 — Tax ✅ PASS

GST/QST return numbers derive from documents/GL via `tax_engine.py:604-700`. T2 pre-fill covers Schedules 1, 8, 50, 100, 125 with real GL-based derivation. CO-17 (Quebec) mapping present.

## Phase 12 — Reports ✅ PASS

AR and AP aging are correctly separated (distinct tables), 30/60/90/120+ buckets, over_60 / over_90 flags.
Cash flow uses indirect method; fixed-assets engine implements 16 CCA classes with half-year rule, recapture, and terminal loss. No critical gaps.

## Phase 13 — Multi-user / roles ❌ FAIL

**Critical bug:**
- **`firm_admin` can create another `firm_admin` inside their firm** — no owner-only check on role elevation at `scripts/review_dashboard.py:19320-19339`. A compromised firm_admin can onboard an accomplice.

**Medium bug:**
- `/aging` and `/fixed_assets` rely on endpoint-level `view_all_clients` gate but do not push `firm_code` into the data-layer queries. Any future route bypass exposes cross-firm data.

**Works:** Role matrix (`scripts/review_dashboard.py:176-223`), `_can_do`, user-set-password check that firm_admin can only edit users in their own firm_code.

---

## Top 10 most critical bugs (prioritised for fix)

| # | Sev | Phase | Description | Location | Est. fix |
|---|---|---|---|---|---|
| 1 | CRITICAL | 9 | JE save never validates debit == credit | review_dashboard.py:20700 | 30 min |
| 2 | CRITICAL | 9 | Posted JEs don't write to any GL table | review_dashboard.py:20773 | 2 h (needs schema choice) |
| 3 | CRITICAL | 7 | Balance sheet renderer expects flat keys, engine returns nested | audit_engine.py:1293 vs review_dashboard.py:7204 | 30 min |
| 4 | CRITICAL | 13 | firm_admin can create other firm_admins (privilege escalation) | review_dashboard.py:19320 | 15 min |
| 5 | HIGH | 7 | Trial balance not validated to balance | audit_engine.py:1183 | 15 min |
| 6 | HIGH | 1 | Stripe webhook has no idempotency table | review_dashboard.py:17880 | 45 min |
| 7 | HIGH | 3 | Line-item gst/qst not persisted on edit | review_dashboard.py:18838 | 30 min |
| 8 | HIGH | 4 | No /bank/unmatch route | review_dashboard.py:18482 | 20 min |
| 9 | HIGH | 5 | QBO posts without connection check, marks "failed" misleadingly | qbo_online_adapter.py:716 | 20 min |
| 10 | MEDIUM | 1 | `/health` users_count queries non-existent `users` table | review_dashboard.py:15344 | 2 min |

**Estimate total work for top 10:** ~7 hours for a focused engineer.

## Ready for CPA friend right now

- Upload + extraction pipeline (Sprints A/refactor + sanity rules).
- Client management, portal, multi-firm isolation (Phase 2).
- GST/QST return generation, T2 pre-fill (Phase 11).
- Aging / cash flow / fixed-asset / CCA reports (Phase 12).
- Document review + correction + self-learning (Phase 3, minus the per-line tax edit).

## Would embarrass us if CPA touched tomorrow

- **Journal entries** — unbalanced JEs can be saved; posting does nothing to the GL (Phase 9).
- **Balance sheet** — renders empty because of a structure mismatch (Phase 7).
- **Trial balance** — can produce unbalanced output without warning (Phase 7).
- **Bank unmatch** — a single mis-click is unrecoverable from the UI (Phase 4).
- **Privilege escalation** — firm_admin can spawn peer admins (Phase 13).

## Recommended next sprint (Sprint C fix order)

1. Fix #1 (JE balance) + #2 (JE → GL) together — can't ship JEs without both.
2. Fix #3 + #5 (financial statement correctness).
3. Fix #4 (role escalation).
4. Fix #6 (webhook idempotency) **before** any paid signup is exposed.
5. The rest (#7–#10) in a batch.

---

*Generated by Sprint B audit; no code changes applied.*
