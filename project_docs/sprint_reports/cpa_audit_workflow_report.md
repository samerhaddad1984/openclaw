# CPA Audit Workflow Deep Test — Sprint D Report
**Date:** 2026-04-19
**Scope:** 18 audit / financial / tax workflows probed end-to-end
**Method:** 5 parallel Explore agents on code + schema + DB state; route, persistence, computation, output, integration for each phase. No code changes applied per instruction.

---

## Executive Summary

| Classification | Count | Phases |
|---|---:|---|
| **FULLY_WORKING** | 5 | 12 (bank recon), 13 (fixed assets), 14 (aging), 15 (AR), 16 (cash flow) |
| **PARTIAL** | 10 | 2, 3, 6, 7, 8, 9, 10, 11, 17, 18 |
| **COSMETIC** | 2 | 1 (engagement letter), 4 (evidence) |
| **NOT_IMPLEMENTED** | 1 | 5 (CAS 530 sampling) |
| **BROKEN** | 0 | — |

**Net verdict:** no feature is outright broken. 5 are ready; 10 are useful-but-incomplete planning tools; 2 are cosmetic renderings that imply more than they deliver; 1 is advertised but not real.

---

## Critical issues (MUST fix before the CPA touches it)

| # | Sev | Phase | Issue | Location |
|---|---|---|---|---|
| 1 | CRITICAL | 5 | CAS 530 sampling is pseudo-random with user-supplied count — no formula, no MUS, no projection. If the CPA uses it for audit sampling she'll produce undefended conclusions. | `src/engines/audit_engine.py:1119` |
| 2 | CRITICAL | 18 | `generate_filing_summary()` sets `gst_collected`/`qst_collected = 0` — no revenue-side GL integration. GST/QST return numbers are materially wrong without CPA override. | `src/engines/tax_engine.py:644` |
| 3 | HIGH | 17 | `/t2/pdf` returns **plaintext**, not PDF. Wrong content-type; can't hand to a client. | `scripts/review_dashboard.py:17745` |
| 4 | HIGH | 9 | Rep letter is plaintext only; no PDF export and `period_end_date` never populated → no date-vs-audit-report validation. CAS 580 audit-file requirement unmet. | `src/engines/cas_engine.py:837` / `:108` |
| 5 | HIGH | 3 | Materiality cannot be reassessed mid-engagement — trigger at `cas_engine.py:192` blocks UPDATE. CAS 320 requires reassessment on significant change. | `src/engines/cas_engine.py:192` |
| 6 | HIGH | 1 | PDF generator uses hardcoded `fontname="hebo"` 12 places in `audit_engine.py` — crashes on hosts missing that font. | `src/engines/audit_engine.py:846` and 11 siblings |
| 7 | HIGH | 17 | T2 pre-fill never persists to `gst_filings` / `filing_history` — a restart loses the work. | `src/engines/t2_engine.py:489` |
| 8 | HIGH | 18 | `/filing_summary` never calls `_mark_as_filed()` — no audit trail of when a return was generated. | `scripts/review_dashboard.py:16657` |
| 9 | HIGH | 11 | Statement of Changes in Equity (SOCE) completely missing from the financial-statement package. | `src/engines/audit_engine.py:1308` |
| 10 | HIGH | 2 | `_get_account_risk_profile()` referenced at `cas_engine.py:599` but never defined — risk profiles fall back to hardcoded dict. | `src/engines/cas_engine.py:599` |
| 11 | HIGH | 7 | Control testing has no deficiency-severity classification (significant vs. material) and no management-letter generator. CAS 330 output incomplete. | `src/engines/cas_engine.py:1059` |
| 12 | HIGH | 8 | Related-party `normal_amount` column exists but is never populated — arm's-length pricing check is not enforced. | `src/engines/cas_engine.py:171` |
| 13 | MEDIUM | 10 | Working papers have no immutability trigger after `sign_off_at` — a reviewer can edit a signed paper. | `src/engines/audit_engine.py:61` |
| 14 | MEDIUM | 4 | CAS 500 evidence categories are only the 3 OtoCPA invented (PO/invoice/payment) — the 5 standard categories (confirmation/inspection/observation/inquiry/reperformance) are absent. | `src/engines/audit_engine.py:974` |
| 15 | MEDIUM | 6 | Analytical procedure variance thresholds are hardcoded (`pct > 10 % AND amt > 1000`) instead of materiality-driven. | `src/engines/audit_engine.py:1643` |

---

## Feature-by-feature detail

### Phase 1 — Engagement letter (CAS 210) — **COSMETIC**
- ✅ Routes `/engagements`, `/engagements/new`, `/engagements/create`, `/engagements/update`, `/engagements/issue` present and persist to `engagements`.
- ✅ `generate_engagement_pdf()` at `audit_engine.py:2031` outputs a PDF.
- ❌ PDF is a **status sheet**, not a CAS 210 engagement letter: no scope of work, no management/auditor responsibilities, no fee basis, no timeline.
- ❌ No e-signature flow. No `signed_at` / `client_signature_blob` columns.
- ❌ No `ENGAGEMENT_STAGES` enum — status hardcoded `'planning'` at creation.
- ❌ PDF uses `fontname="hebo"` — breaks on hosts without it.
- **CAS 210 verdict:** Does not meet the standard.

### Phase 2 — Risk assessment (CAS 315) — **PARTIAL**
- ✅ `risk_assessments` table populated; inherent × control → combined risk via `_combine_risk` and `_is_significant`.
- ❌ Assertions stored as **free text**; no validation against CAS 315 assertions (existence, completeness, accuracy, valuation, cutoff, classification, rights/obligations).
- ❌ `_get_account_risk_profile()` referenced but never defined.
- ❌ No fraud-risk, going-concern, or related-party factors fed into the matrix.
- ❌ No PDF export.
- **CAS 315 verdict:** Partial — three-risk model present; assertion-level mapping and explicit risk response procedures missing.

### Phase 3 — Materiality (CAS 320) — **PARTIAL**
- ✅ Formulas correct: planning = benchmark × rate (5 % pre-tax income / 0.5 % total assets / 2 % revenue), performance = 75 % of planning, clearly trivial = 5 % of planning.
- ❌ Trigger at `cas_engine.py:192` blocks **any** reassessment — CAS 320 explicitly requires reassessment on significant change.
- ❌ `PERFORMANCE_RATE` / `CLEARLY_TRIVIAL_RATE` globals, not configurable per engagement.
- ❌ No standalone materiality-memo PDF.
- **CAS 320 verdict:** Mostly correct math, rigid workflow.

### Phase 4 — Evidence (CAS 500) — **COSMETIC**
- ✅ `audit_evidence` table with `evidence_type`, `linked_document_ids`, three-way match.
- ❌ `evidence_type` values are the app's 3 invented categories (PO/invoice/payment), not CAS 500's 5 (confirmation/inspection/observation/inquiry/reperformance).
- ❌ No file-upload or retention metadata (`file_hash`, `retention_until`).
- ❌ No PDF output.
- ❌ No FK from working papers to evidence.
- **CAS 500 verdict:** Does not meet — category taxonomy wrong.

### Phase 5 — Sampling (CAS 530) — **NOT_IMPLEMENTED**
- ❌ `get_sample()` at `audit_engine.py:1119` is `random.Random(paper_id).sample(...)` with user-supplied `n` (default 10).
- ❌ No CAS 530 sample-size formula (`n = RF × population / tolerable_misstatement`).
- ❌ No Monetary Unit Sampling.
- ❌ No attribute sampling for controls.
- ❌ No projection of observed errors to the population.
- **CAS 530 verdict:** Not implemented. Using this feature would produce undefended audit conclusions. **Flag as CRITICAL.**

### Phase 6 — Analytical procedures (CAS 520) — **PARTIAL**
- ✅ `_calculate_ratios()` computes current, quick, gross margin, net margin, AP days.
- ✅ Prior-period trend comparison via `trial_balance`.
- ✅ PDF via `generate_analytical_report_pdf()`.
- ❌ Investigation / explanation log missing — CAS 520 requires documented reasons for flagged variances.
- ❌ Thresholds hardcoded (`>10 %` and `>$1000`) instead of materiality-driven.
- **CAS 520 verdict:** Partial — outputs ratios, but without documented investigation trail.

### Phase 7 — Controls (CAS 330) — **PARTIAL**
- ✅ `control_tests` table captures test_type, items_tested, exceptions_found, conclusion.
- ✅ 15 pre-defined standard controls; effectiveness summary count.
- ❌ No deficiency severity classification (significant vs. material).
- ❌ No reliance-decision capture.
- ❌ No management-letter generator output.
- **CAS 330 verdict:** Partial — documenting works; output requirements unmet.

### Phase 8 — Related parties (CAS 550) — **PARTIAL**
- ✅ `related_parties` + `related_party_transactions` tables with `measurement_basis` and `disclosure_required`.
- ✅ `generate_related_party_disclosure()` outputs bilingual plaintext note.
- ❌ `normal_amount` column exists but never populated — no arm's-length comparison enforced.
- ❌ No consolidation hierarchy (`consolidation_required` missing).
- ❌ `auto_detect_related_parties()` results never merged into the UI party list.
- **CAS 550 verdict:** Partial — identifications & disclosure OK, arm's-length + consolidation missing.

### Phase 9 — Rep letter (CAS 580) — **PARTIAL**
- ✅ `management_representation_letters` table.
- ✅ Bilingual plaintext output with 6 hardcoded representations.
- ❌ No PDF — CAS 580 audit-file format unmet.
- ❌ `period_end_date` never populated from engagement.
- ❌ No per-representation checklist or auditor countersignature.
- ❌ Signature is a plaintext name string.
- **CAS 580 verdict:** Partial.

### Phase 10 — Working papers — **PARTIAL**
- ✅ `working_papers` + `working_paper_items` tables with sign-off columns.
- ✅ `/working_papers`, `/working_papers/pdf`, `/working_papers/signoff`, `/working_papers/save_assertions`, `/working_papers/create_from_coa` all present.
- ❌ No A/B/C hierarchical indexing.
- ❌ No immutability lock after sign-off.
- ❌ No archival column (`archived_at`).
- ❌ Assertion-coverage capture is a stub (`review_dashboard.py:6899`).

### Phase 11 — Financial statements — **PARTIAL**
- ✅ Post Sprint C: flat-key BS and IS render; TB equality + BS identity warning banners fire.
- ✅ PDF export via `generate_financial_statements_pdf()` (PyMuPDF or minimal).
- ❌ **Statement of Changes in Equity completely missing.**
- ❌ Comparatives (prior period) are a stub; `prior_period` string is computed but never rendered.
- ❌ Cash flow statement is in its own engine; not wrapped into the FS package.
- ❌ No consolidated disclosure checklist (accounting policies, contingencies, subsequent events).

### Phase 12 — Reconciliation — **FULLY_WORKING** (bank only)
- ✅ `bank_reconciliations` + `reconciliation_items`, outstanding cheques / deposits in transit, calc + PDF + status.
- ⚠ Scope-limited: no GL-to-subledger, intercompany, tax-account, payroll.

### Phase 13 — Fixed assets (CCA) — **FULLY_WORKING**
- ✅ All 16 CCA classes with correct rates.
- ✅ Half-year rule, recapture, terminal loss, capital gain.
- ✅ Schedule 8 generator (grouped by class with totals).
- ✅ CSV export.

### Phase 14 — Aging — **FULLY_WORKING**
- ✅ AR + AP aging, 30/60/90/120+ buckets, CSV export.
- ⚠ DSO not computed explicitly but data is available.

### Phase 15 — Accounts receivable — **FULLY_WORKING** (minor cosmetic)
- ✅ Full CRUD, payment application, status transitions, UI.
- ⚠ COSMETIC: No PDF invoice generator wired into `/ar/send`. No credit-note / reversal schema.

### Phase 16 — Cash flow statement — **FULLY_WORKING**
- ✅ Indirect method per ASPE 1540: net income + depreciation ± WC → operating; capex ↔ proceeds → investing; debt + equity → financing.
- ✅ `validate_closing_cash()` reconciles to GL 1000–1099 at period end (tolerance $0.01); shows warning badge if unreconciled.
- ⚠ `/cashflow/pdf` is actually `text/plain`, not PDF. `/cashflow/excel` is CSV. Cosmetic.

### Phase 17 — T2 pre-fill — **PARTIAL**
- ✅ Schedules 1, 8, 50, 100, 125 all computed from GL (no mocks). CO-17 mapping real.
- ❌ `/t2/pdf` outputs plaintext. `/t2/excel` outputs CSV.
- ❌ Zero persistence; a restart drops the work.
- ❌ Filing status not written.
- **Verdict: planning tool only, not submission-ready.**

### Phase 18 — GST/QST returns — **PARTIAL**
- ✅ ITC / ITR calculated correctly via `_itc_itr_from_total` + tax-code registry.
- ✅ Quick-method rates defined. Revenu Québec PDF pre-fill exists via `revenu_quebec.py`.
- ❌ `gst_collected` / `qst_collected` default to $0 — no revenue-side GL integration. Numbers are materially wrong without CPA override.
- ❌ `/filing_summary` never calls `_mark_as_filed()` — no audit trail.
- ❌ QM form-field switching incomplete; footer says "Manual entry required".
- ❌ `validate_quebec_tax_compliance()` exists but isn't called during filing summary.
- **Verdict: planning tool only, not submission-ready.**

---

## CAS compliance summary

| Standard | Verdict | One-line |
|---|---|---|
| CAS 210 (engagement terms) | Not met | PDF is a status sheet, not an engagement letter. |
| CAS 315 (risk assessment) | Partial | Three-risk model works; no FS-assertion mapping or fraud/going-concern factors. |
| CAS 320 (materiality) | Partial | Formulas correct; reassessment blocked by trigger. |
| CAS 330 (controls) | Partial | Tests recorded; no deficiency severity or management letter. |
| CAS 500 (evidence) | Not met | Category taxonomy is the app's invention, not CAS 500's 5 categories. |
| CAS 520 (analytical procedures) | Partial | Ratios computed; investigation trail not captured. |
| CAS 530 (sampling) | **Not implemented** | Pseudo-random pick with user-supplied n. Critical gap. |
| CAS 550 (related parties) | Partial | Identifications + disclosure OK; arm's-length enforcement and consolidation missing. |
| CAS 580 (rep letter) | Partial | Plaintext output + plaintext signature; no PDF, no per-rep checklist. |

---

## Is it safe to show this to a CPA tomorrow?

**Conditionally yes — for bookkeeping-adjacent workflows only.** The following features will impress:

- Upload → extract → correct → learn loop.
- AR aging, AP aging, cash flow (indirect), fixed-asset CCA with all 16 classes.
- Bank reconciliation (including post-Sprint C sliding-scale match + unmatch).
- Materiality + risk matrix as **planning tools**.
- Balance sheet + income statement (post Sprint C).

**Do NOT let her use these for billable audit work tomorrow:**

- **CAS 530 sampling** — produces numbers you can't defend to a reviewer. Must add formula + MUS + projection before it's safe.
- **GST/QST filing** — revenue-side is $0; it's a planner, not a return.
- **T2 pre-fill** — same story; also no PDF.
- **Rep letter** — plaintext signature won't satisfy any partner review.

If the goal is "CPA friend kicks the tires, doesn't submit anything," keep the above four routes hidden from the navigation or banner them "PLANNING / PREVIEW ONLY."

---

## Work estimate to fix all CRITICAL and HIGH issues

| Priority | Item | Est. |
|---|---|---|
| 1 | Implement CAS 530 sample-size formula + MUS + projection | **8 h** |
| 2 | Revenue-side GL integration for GST/QST filing summary | **4 h** |
| 3 | Real PDF for `/t2/pdf` and persistence to filing_history | **3 h** |
| 4 | Real PDF for rep letter + `period_end_date` wiring + auditor countersignature | **3 h** |
| 5 | Remove the materiality-reassessment trigger + add `reassessment_reason` | **1 h** |
| 6 | Fallback font in all 12 `fontname="hebo"` calls | **1 h** |
| 7 | Wire `_mark_as_filed()` into `/filing_summary` | **1 h** |
| 8 | Add SOCE to financial statements + prior-period comparatives | **3 h** |
| 9 | Define `_get_account_risk_profile()` or remove the reference | **30 min** |
| 10 | Control-deficiency severity enum + management-letter generator | **3 h** |
| 11 | Populate `normal_amount` + arm's-length comparison for related parties | **2 h** |
| 12 | Working-paper immutability trigger after sign-off | **30 min** |
| 13 | CAS 500 evidence category taxonomy (add 5 standard types) | **1 h** |
| 14 | Materiality-driven variance thresholds for analytical procedures | **1 h** |

**Total: ~32 hours for one engineer**, or ~4 focused days. After that, phases 2, 3, 6–11, 17, 18 move from PARTIAL → CPA-ready, and phase 5 moves from NOT_IMPLEMENTED → defensible. Phases 1 (COSMETIC→MET) and 4 (COSMETIC→MET) also clear with items 6+13.

---

*Generated by Sprint D audit; no code changes applied.*
