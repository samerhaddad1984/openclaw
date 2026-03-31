# OtoCPA - Developer Guide

> Comprehensive technical reference for developers joining the OtoCPA project.
> Last updated: 2026-03-25

---

## Table of Contents

1. [Project Overview](#section-1--project-overview)
2. [Architecture Deep Dive](#section-2--architecture-deep-dive)
3. [File Reference](#section-3--file-reference)
4. [Database Schema](#section-4--database-schema)
5. [API Reference](#section-5--api-reference)
6. [Adding New Features](#section-6--adding-new-features)
7. [Test Suite Guide](#section-7--test-suite-guide)
8. [Deployment Guide](#section-8--deployment-guide)
9. [Known Limitations](#section-9--known-limitations)
10. [Security Model](#section-10--security-model)

---

# Section 1 -- Project Overview

## What OtoCPA Is

OtoCPA is an accounting automation platform built for Canadian bookkeeping firms. It ingests documents (invoices, receipts, bank statements, credit memos) from email, SharePoint, a client portal, or a watched folder, extracts structured data, applies tax rules (GST/QST/HST), detects fraud, classifies economic substance, and posts transactions to QuickBooks Online -- all with a bilingual (French/English) review dashboard where accountants approve, correct, or escalate.

The system is designed to run on-premise on a single Windows machine per firm. All client data stays on the firm's hardware; only AI API calls leave the network.

## The 3-Layer Architecture

OtoCPA enforces a strict separation between deterministic logic, AI assistance, and human judgment:

| Layer | Name | Guarantee | Examples |
|-------|------|-----------|----------|
| **Layer 1** | Deterministic | No hallucination risk. Outputs derive from explicit rules or database state. Uses `Decimal` arithmetic for all monetary values. | Fraud engine (13 rules), tax engine (all Canadian tax codes), bank parser, reconciliation engine, payroll engine, customs engine |
| **Layer 2** | AI-Assisted | AI calls with deterministic fallback. Every AI output is validated by the hallucination guard before it reaches the database. Confidence scores gate auto-approval. | OCR engine (Vision API with pdfplumber fallback), substance classifier (keywords first, AI second), tax code resolver (regex first, AI second), line-item extraction |
| **Layer 3** | Human Review | Dashboard-driven approval workflow. Fraud flags, low confidence, missing fields, or substance issues force human review. Period locks and audit trails prevent unauthorized changes. | Review dashboard, client portal, period close, audit working papers, engagement management |

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11 |
| Database | SQLite (single file: `data/otocpa_agent.db`) |
| PDF Generation | ReportLab |
| Image Processing | Pillow, pdf2image, pytesseract |
| AI Providers | OpenRouter (Claude Sonnet for complex tasks), DeepSeek (routine tasks) |
| OCR | Claude Vision API (primary), Tesseract (fallback), pdfminer/pdfplumber (text PDFs) |
| Authentication | bcrypt password hashing, HTTP-only session cookies |
| Web Framework | Python `http.server` (stdlib `BaseHTTPRequestHandler`, no Flask) |
| Microsoft 365 | MSAL device flow, Microsoft Graph API (Mail, SharePoint, Lists) |
| QuickBooks | QBO REST API v75 (sandbox and production) |
| Tunnel | Cloudflare Tunnel (cloudflared) |
| Installer | PyInstaller (`.spec` file), PowerShell installer script |
| Testing | pytest with property-based and red-team test suites |

## Repository Structure

```
OtoCPAAi/
+-- src/
|   +-- agents/
|   |   +-- core/          # Business logic: task store, approval engine, duplicate guard,
|   |   |                  #   vendor memory, learning memory, exception router, dashboard auth
|   |   +-- tools/         # 38 integration modules: OCR, PDF extract, rules engine,
|   |   |                  #   posting builder, QBO adapter, Graph API, fingerprinting,
|   |   |                  #   review policy, vendor intelligence, amount policy
|   |   +-- prompts/       # AI prompt templates for extraction and classification
|   |   +-- data/
|   |       +-- rules/     # JSON rule files: vendors.json, gl_map.json, vendor_intel.json,
|   |       |              #   client_map.json, client_registry.json, qbo_mappings.json
|   |       +-- state/     # Runtime state: processed fingerprints, SharePoint item IDs
|   +-- engines/           # 18 specialized engines (see Section 2)
|   +-- i18n/              # Internationalization: en.json, fr.json, __init__.py (t() function)
|   +-- integrations/      # External system bridges
+-- scripts/               # 50+ operational scripts: dashboard, portal, migration,
|                          #   user management, posting queue, data generation
+-- tests/                 # 45+ test files
|   +-- red_team/          # 20 adversarial/destruction test suites
|   +-- documents/         # Test document fixtures
|   +-- documents_real/    # Real-world document samples
+-- docs/                  # Documentation and generated manuals
+-- project_docs/          # Technical specifications
+-- reports/               # Rebuild and diagnostic reports
+-- data/                  # Runtime data directory
|   +-- otocpa_agent.db   # SQLite database (all tables)
|   +-- incoming_documents/   # Watched folder for ingestion
+-- exports/               # Generated CSV/JSON exports
+-- build/ / dist/         # PyInstaller output
+-- otocpa.config.json # Main application configuration
+-- client_config.json     # Per-client QBO configuration
+-- .env.template          # Environment variable template
```

---

# Section 2 -- Architecture Deep Dive

## Complete Data Flow: Document Intake to QuickBooks Posting

```
Document Sources                Processing Pipeline                  Output
==================             =====================                ======

Email (Graph API)  ----+
SharePoint folder  ----+---> OCR Engine ---------> Rules Engine -----+
Client Portal      ----+     (Vision/pdfplumber)   (regex patterns)  |
Watched Folder     ----+                                             |
                              +--------------------------------------+
                              |
                              v
                   Client Router ------------> Vendor Intelligence
                   (signal scoring)            (GL/tax code mapping)
                              |                        |
                              v                        v
                   Substance Engine            Amount Policy
                   (CapEx/prepaid/loan)        (bookkeeping amount)
                              |                        |
                              +----------+-------------+
                                         |
                                         v
                              Fraud Engine (13 rules)
                                         |
                                         v
                              Hallucination Guard
                              (numeric validation)
                                         |
                                         v
                              Uncertainty Engine
                              (can post / partial / block)
                                         |
                                         v
                              Review Policy Decision
                              (Ready / NeedsReview / Exception)
                                         |
                           +-------------+-------------+
                           |             |             |
                           v             v             v
                        Ready      NeedsReview    Exception
                           |             |             |
                           v             v             v
                   Posting Builder   Dashboard    Exception Queue
                           |        (human review)
                           v
                   QBO Online Adapter
                           |
                           v
                   QuickBooks Online
```

### Stage-by-Stage Detail

1. **Ingestion** -- Documents arrive via email (Graph API polling), SharePoint (folder scan), the client portal (HTTP upload on port 8788), or the watched folder (`scripts/folder_watcher.py`). Each source produces raw bytes and metadata.

2. **OCR Engine** (`src/engines/ocr_engine.py`) -- Detects file format from magic bytes (PDF, JPEG, PNG, TIFF, WebP, HEIC). For PDFs, extracts text via pdfplumber; if fewer than 20 words, falls back to Claude Vision API. For images, uses Vision directly. Handwriting detection uses pixel-variance heuristics; if probability > 0.6, a handwriting-specific Vision prompt is used.

3. **Rules Engine** (`src/agents/tools/rules_engine.py`) -- Pattern-matches extracted text against `vendors.json` regex rules. Extracts vendor name, document type, amount, date, and currency deterministically.

4. **Client Router** (`src/agents/tools/client_router.py`) -- Scores document text against `client_map.json` using weighted signals: sender email (+10), account number (+10), address fragment (+6), client name (+5), keyword (+2). Returns client_code if score exceeds the client's min_score threshold.

5. **Vendor Intelligence** (`src/agents/tools/vendor_intelligence.py`) -- Maps vendor + doc_type to GL account, tax code, and category using `vendor_intel.json`. Falls back to doc_type defaults, then global defaults.

6. **Amount Policy** (`src/agents/tools/amount_policy.py`) -- Determines the bookkeeping amount based on document type. Handles multi-settlement, credit memo classification, and tax-aware payment splits.

7. **Substance Engine** (`src/engines/substance_engine.py`) -- Classifies transactions as CapEx (GL 1500), prepaid (GL 1300), loan (GL 2500), tax remittance (GL 2200-2215), personal/shareholder (blocked), customer deposit (GL 2400), or intercompany. Uses 150+ bilingual regex patterns with accent normalization before falling back to AI.

8. **Fraud Engine** (`src/engines/fraud_engine.py`) -- Runs all 13 deterministic fraud rules (see below). Returns a list of fraud flag dictionaries with severity (CRITICAL/HIGH/MEDIUM/LOW).

9. **Hallucination Guard** -- Validates AI-extracted numeric totals against arithmetic (subtotal + tax = total). Records mismatches in the audit log. Flags `hallucination_suspected` on the document record.

10. **Uncertainty Engine** (`src/engines/uncertainty_engine.py`) -- Evaluates posting readiness using 28 structured reason codes. Confidence < 0.60 = must block; 0.60-0.79 = partial post with flags; >= 0.80 = safe to post.

11. **Review Policy** (`src/agents/tools/review_policy.py`) -- Final decision gate. Applies fraud caps (max 0.60 confidence), substance caps (CapEx 0.70, intercompany 0.60, mixed tax 0.50), large-amount caps ($25K+ = 0.75, credit > $5K = 0.65). Returns Ready, NeedsReview, or Exception with reasons.

12. **Posting Builder** (`src/agents/tools/posting_builder.py`) -- Syncs document data into `posting_jobs` table with a complete JSON payload. Handles optimistic locking via version columns.

13. **QBO Online Adapter** (`src/agents/tools/qbo_online_adapter.py`) -- Maps vendor names and GL accounts to QBO references, creates Purchase/Bill transactions via the QBO REST API. Verifies posted transactions.

## Database Schema Overview

The SQLite database (`data/otocpa_agent.db`) contains 44+ tables organized into functional groups:

- **Core:** `documents`, `posting_jobs`
- **Authentication:** `dashboard_users`, `dashboard_sessions`
- **Learning:** `vendor_memory`, `learning_memory_patterns`, `learning_corrections`
- **Banking:** `bank_statements`, `bank_transactions`, `bank_reconciliations`, `reconciliation_items`
- **Tax/Filing:** `client_config`, `gst_filings`
- **Audit:** `working_papers`, `working_paper_items`, `audit_evidence`, `engagements`, `control_tests`, `management_representation_letters`, `related_parties`, `related_party_transactions`, `materiality_assessments`, `risk_assessments`
- **Time/Invoicing:** `time_entries`, `invoices`
- **Period Management:** `period_close`, `period_close_locks`
- **Correction/Amendment:** `correction_chains`, `amendment_flags`, `document_snapshots`, `posting_snapshots`, `document_clusters`, `document_cluster_members`, `overlap_anomalies`, `rollback_log`
- **Communication:** `client_communications`, `messaging_log`
- **Other:** `audit_log`, `vendor_aliases`, `boc_fx_rates`, `manual_journal_entries`, `credit_memo_invoice_link`, `chart_of_accounts`, `trial_balance`, `match_decisions`, `invoice_lines`

See [Section 4](#section-4--database-schema) for complete column-level documentation.

## The AI Router System

OtoCPA uses a two-tier AI routing strategy configured in `otocpa.config.json`:

| Task Type | Provider | Model | Cost Profile |
|-----------|----------|-------|-------------|
| **Routine** (document classification, vendor extraction, GL mapping, date parsing, duplicate evaluation, category assignment, memo generation) | DeepSeek | deepseek-chat | Low cost |
| **Complex** (anomaly explanation, escalation decisions, compliance narrative, tax ambiguity resolution, fraud analysis, audit commentary, client communication drafting) | OpenRouter | claude-sonnet | Higher cost, higher quality |

The `ai_client_router.py` module handles prompt dispatch. All prompts are sanitized before sending (SIN numbers redacted). Responses are validated by the hallucination guard before being written to the database. Every AI call is logged to `audit_log` with provider, task type, prompt snippet, and latency.

## The Learning Memory System

OtoCPA learns from human corrections through three storage layers:

1. **Vendor Memory** (`vendor_memory` table) -- When a human approves a posting, the system records the vendor + client_code combination with its GL account, tax code, category, and confidence. Future documents from the same vendor start with higher confidence. Lookups use normalized vendor keys (`vendor_key`) and client code keys (`client_code_key`) with indexes for fast retrieval.

2. **Learning Corrections** (`learning_corrections` table) -- When a human changes a field (e.g., corrects a GL account from 5100 to 5200), the system records the old value, new value, field name, vendor, client, and doc type. Over time, repeated corrections build patterns. The `support_count` column tracks how many times the same correction has been made.

3. **Learning Memory Patterns** (`learning_memory_patterns` table) -- Aggregated patterns derived from corrections and approvals. Tracks outcome counts, success rates, review rates, and average confidence per (vendor, client, doc_type, field) combination.

**Backfill scripts** (`scripts/backfill_learning_memory.py`, `scripts/backfill_vendor_memory.py`) can retroactively mine corrections from historical documents and posting jobs.

## The Hallucination Guard

The hallucination guard validates AI-extracted data before it reaches the database:

- **Numeric total verification** -- Checks that subtotal + tax_total = total within a tolerance. Records math mismatches in `audit_log`.
- **Vendor name validation** -- Flags suspiciously long or empty vendor names.
- **Amount boundary checks** -- Flags amounts that are negative when they should not be, or unreasonably large.
- **Date validation** -- Flags dates that are in the far future or impossibly old.
- **Document-level flag** -- Sets `hallucination_suspected = 1` on the document row when any check fails.

## The Uncertainty Engine

The uncertainty engine (`src/engines/uncertainty_engine.py`) tracks provenance-preserving uncertainty with 28 structured reason codes:

**Basic reasons:** `VENDOR_IDENTITY_UNPROVEN`, `ALLOCATION_GAP_UNEXPLAINED`, `TAX_REGISTRATION_INCOMPLETE`, `VENDOR_NAME_CONFLICT`, `INVOICE_NUMBER_OCR_CONFLICT`, `DATE_AMBIGUOUS`, `SETTLEMENT_STATE_UNRESOLVED`

**Complex reasons:** `CUSTOMS_NOTE_SCOPE_LIMITED`, `BOILERPLATE_TAX_DISCLAIMER`, `MISSING_SUPPORTING_VENDOR_DOCUMENT`, `DUPLICATE_INGESTION_CANDIDATE`

**Trap-specific reasons:** `FILED_PERIOD_AMENDMENT_NEEDED`, `CREDIT_MEMO_TAX_SPLIT_UNPROVEN`, `SUBCONTRACTOR_WORK_SCOPE_OVERLAP`, `RECOGNITION_TIMING_DEFERRED`, `DUPLICATE_CLUSTER_NON_HEAD`, `STALE_VERSION_DETECTED`, `MANUAL_JOURNAL_COLLISION`, `REIMPORT_BLOCKED_AFTER_ROLLBACK`

**Decision thresholds:**
- Confidence < 0.60 = `BLOCK_PENDING_REVIEW` (must block)
- Confidence 0.60-0.79 = `PARTIAL_POST_WITH_FLAGS` (post with human-visible warnings)
- Confidence >= 0.80 = `SAFE_TO_POST`

## The Substance Engine

The substance engine classifies transactions by economic nature:

| Category | GL Range | Block? | Example Keywords |
|----------|----------|--------|-----------------|
| CapEx | 1500 | No (flag for review) | equipment, machinery, vehicles, HVAC, renovations |
| Prepaid | 1300 | No (flag) | insurance, annual subscription, advance rent |
| Loan/Financing | 2500 | No (flag) | mortgage, line of credit, capital lease |
| Tax Remittance | 2200-2215 | No (flag) | GST, QST, source deductions, CNESST |
| Personal/Shareholder | N/A | **Yes** | groceries, clothing, Netflix, vacations |
| Customer Deposit | 2400 | No (flag) | deposit, advance payment, retainer ($500+ threshold) |
| Intercompany | Varies | Yes (review required) | Related entity transactions |

Each category uses bilingual (FR/EN) keyword patterns with accent normalization and negative overrides (e.g., "repair" prevents a CapEx classification). Known CapEx vendors (Dell, HP, Lenovo, Apple, Cisco) have a lower $1,500 threshold.

## The Fraud Engine -- All 13 Rules

| # | Rule ID | Severity | Description | Thresholds |
|---|---------|----------|-------------|------------|
| 1 | `vendor_amount_anomaly` | MEDIUM | Amount > 2 standard deviations from vendor mean | Min 5 prior transactions |
| 2 | `vendor_timing_anomaly` | LOW | Invoice day-of-month > 14 days from vendor norm | Min 5 prior transactions |
| 3 | `duplicate_exact` | HIGH | Same vendor + same amount within 30 days | Exact amount match |
| 4 | `duplicate_cross_vendor` | MEDIUM | Different vendor + same amount within 7 days | Exact amount match |
| 5 | `weekend_transaction` | MEDIUM | Transaction on Saturday or Sunday | Amount > $200 |
| 6 | `holiday_transaction` | MEDIUM | Transaction on Quebec statutory holiday | Amount > $200 |
| 7 | `round_number_flag` | LOW | Perfectly round amount from vendor with irregular invoices | Vendor has non-round history |
| 8 | `new_vendor_large_amount` | MEDIUM | First invoice from vendor over $2,000 (or $2,000 cumulative in 30 days) | Cumulative threshold |
| 9 | `bank_account_change` | CRITICAL | Vendor bank details changed between invoices | Any change triggers |
| 10 | `invoice_after_payment` | HIGH | Invoice date is after matching bank payment date | Date comparison |
| 11 | `tax_registration_contradiction` | HIGH | Vendor charges GST/QST but is historically unregistered/exempt | Historical pattern |
| 12 | `vendor_category_shift` | MEDIUM | Vendor category contradicts >= 80% historical pattern | 80% threshold |
| 13 | `vendor_payee_mismatch` | HIGH | Bank transaction payee differs significantly from invoice vendor | String similarity |

**Plus:** `orphan_credit_note` -- Credit memo with no matching original invoice.

Quebec holidays are computed dynamically using the Easter algorithm and include: New Year's Day, Good Friday, Easter Monday, National Patriots' Day, Saint-Jean-Baptiste, Canada Day, Labour Day, Thanksgiving, Christmas Day, Boxing Day.

---

# Section 3 -- File Reference

## src/engines/ (18 Engine Modules)

### `src/engines/fraud_engine.py`
**Purpose:** Layer 1 deterministic fraud detection with 13 rule-based algorithms.
**Key Functions:**
- `run_fraud_detection(document_id: str, db_path: Path) -> list[dict]` -- Runs all 13 rules against a document
- `get_fraud_flags(document_id: str, db_path: Path) -> list[dict]` -- Retrieves stored fraud flags
- `check_related_party(vendor, related_parties, client_code, db_path) -> dict` -- CAS 550 related party check
- `evaluate_cross_entity_payment(invoice_vendor, bank_payee, ...) -> dict` -- Cross-entity validation
- `_rule_vendor_amount_anomaly(amount, history, fuzzy_history) -> dict | None`
- `_rule_duplicate(conn, document_id, vendor, client_code, amount, doc_date) -> list[dict]`
- `_rule_bank_account_change(raw_result_json, history) -> dict | None`
- `_rule_orphan_credit_note(conn, vendor, client_code, abs_amount, exclude_doc_id) -> dict | None`
**Dependencies:** json, logging, math, sqlite3, datetime, difflib, pathlib, unicodedata
**DB Tables:** documents (read), bank_transactions (read), vendor_memory (read)

### `src/engines/tax_engine.py`
**Purpose:** Layer 1 deterministic tax calculation for all Canadian tax codes using Decimal arithmetic.
**Key Functions:**
- `calculate_gst_qst(amount_before_tax: Decimal) -> dict` -- Calculates GST + QST on a pre-tax amount
- `extract_tax_from_total(total: Decimal) -> dict` -- Reverse-engineers taxes from a tax-inclusive total
- `validate_tax_code(tax_code: str, province: str, gl_account: str) -> dict` -- Validates code + province + GL
- `calculate_itc_itr(amount_before_tax, tax_code, province) -> dict` -- Input tax credit/rebate calculation
- `generate_filing_summary(client_code, period_start, period_end, db_path) -> dict` -- GST/QST filing summary
**Dependencies:** sqlite3, decimal, pathlib
**DB Tables:** documents (read for filing summary), posting_jobs (read)
**Constants:** `GST_RATE = 0.05`, `QST_RATE = 0.09975`, `HST_RATE_ON = 0.13`, `HST_RATE_ATL = 0.15`

### `src/engines/tax_code_resolver.py`
**Purpose:** Mixed taxable/exempt invoice detection using bilingual keyword matching with AI fallback.
**Key Functions:**
- `resolve_mixed_tax(memo, line_items, invoice_text, vendor) -> dict` -- Returns `{mixed_tax_invoice, tax_code, block_auto_approval, review_notes, confidence}`
**Dependencies:** logging, re
**DB Tables:** None

### `src/engines/uncertainty_engine.py`
**Purpose:** Provenance-preserving uncertainty tracking with 28 structured reason codes.
**Key Functions:**
- `evaluate_uncertainty(confidence_by_field: dict, reasons: list) -> UncertaintyState` -- Returns can_post / partial / block decision
- `evaluate_posting_readiness(document_state) -> PostingDecision`
- `build_date_resolution(raw_date, language) -> DateResolutionState` -- Handles DD/MM vs MM/DD ambiguity
**Dependencies:** dataclasses, datetime, typing
**DB Tables:** None (stateless evaluation)

### `src/engines/substance_engine.py`
**Purpose:** Economic substance classifier for CapEx, prepaids, loans, tax remittances, personal expenses.
**Key Functions:**
- `classify_substance(vendor, memo, doc_type, amount, client_code, db_path) -> dict` -- Returns `{substance_type, gl_suggestion, confidence, deterministic_match}`
**Dependencies:** logging, re, sqlite3, pathlib
**DB Tables:** vendor_memory (read for historical patterns)

### `src/engines/ocr_engine.py`
**Purpose:** Multi-format document ingestion with Vision API extraction and handwriting detection.
**Key Functions:**
- `process_file(file_bytes, filename, client_code, document_id, ...) -> dict` -- Full ingestion pipeline
- `detect_format(data: bytes) -> str` -- Magic byte format detection
- `extract_pdf_text(pdf_bytes) -> str` -- pdfplumber extraction
- `detect_handwriting(image_bytes) -> float` -- Handwriting probability 0.0-1.0
- `call_vision(image_bytes, mime_type) -> dict` -- Claude Vision extraction
- `upsert_document(record, db_path) -> None` -- Insert or update documents table
**Dependencies:** Many (pdfplumber, Pillow, httpx or requests for Vision API)
**DB Tables:** documents (write)

### `src/engines/reconciliation_engine.py`
**Purpose:** Bank reconciliation with create/populate/calculate/finalize and PDF reports.
**Key Functions:**
- `ensure_reconciliation_tables(conn) -> None`
- `create_reconciliation(client_code, account_name, ...) -> str` -- Returns reconciliation_id
- `add_reconciliation_item(reconciliation_id, item_type, description, amount, ...) -> str`
- `calculate_reconciliation(reconciliation_id, conn) -> dict` -- Returns adjusted balances + reconciled flag
- `finalize_reconciliation(reconciliation_id, reviewed_by, conn) -> None`
- `generate_pdf_report(reconciliation_id, conn) -> bytes`
**Dependencies:** sqlite3, decimal, uuid, reportlab
**DB Tables:** bank_reconciliations (read/write), reconciliation_items (read/write)

### `src/engines/reconciliation_validator.py`
**Purpose:** Invoice total reconciliation and FX conversion validation with gap classification.
**Key Functions:**
- `reconcile_invoice_total(lines, invoice_total_shown, currency, fx_rate, vendor_markup) -> dict` -- Classifies any gap as FX rounding, tax ambiguity, missing line, markup, or unresolvable
**Dependencies:** decimal
**DB Tables:** None

### `src/engines/bank_parser.py`
**Purpose:** Bank statement parser for major Quebec banks (Desjardins, National Bank, BMO, TD, RBC).
**Key Functions:**
- `import_statement(file_bytes, filename, client_code, imported_by, db_path) -> dict` -- Full import pipeline with smart matching
**Dependencies:** sqlite3, csv, difflib, decimal, pathlib
**DB Tables:** documents (write), bank_transactions (write), bank_statements (write)

### `src/engines/amendment_engine.py`
**Purpose:** Filed-period amendment lifecycle (Traps 1, 4, 9) with full audit lineage.
**Key Functions:**
- `is_period_filed(conn, client_code, period_label) -> bool`
- `flag_amendment_needed(conn, client_code, filed_period, description, affected_documents) -> str`
- `take_filing_snapshot(conn, client_code, period_label, filed_by) -> str`
- `snapshot_document(conn, document_id, reason) -> dict`
- `get_belief_at_time(conn, document_id, belief_timestamp) -> dict`
- `validate_recognition_timing(conn, document_id, activation_date) -> dict`
**Dependencies:** sqlite3, json, datetime, pathlib
**DB Tables:** gst_filings, amendment_flags, document_snapshots, posting_snapshots

### `src/engines/correction_chain.py`
**Purpose:** Correction chain graph and economic event tracking (Traps 2, 3, 5, 8).
**Key Functions:**
- `decompose_credit_memo_safe(conn, ...) -> dict` -- Decomposes credit memo only as far as evidence allows
- `detect_overlap_anomaly(conn, vendor1, vendor2, ...) -> dict` -- Flags cross-vendor work overlap
- `cluster_documents(conn, document_ids) -> str` -- Persistent n-way duplicate clustering
- `build_correction_chain_link(conn, source_id, target_id, ...) -> str`
- `rollback_correction(conn, correction_id, rolled_back_by, reason) -> None` -- Explicit, audited rollback
- `check_reimport_after_rollback(conn, document_id) -> dict` -- Safe re-import gate
**Dependencies:** sqlite3, json, datetime, pathlib
**DB Tables:** correction_chains, document_clusters, document_cluster_members, rollback_log, overlap_anomalies

### `src/engines/concurrency_engine.py`
**Purpose:** Optimistic locking and journal collision detection (Traps 6, 7).
**Key Functions:**
- `read_version(conn, entity_type, entity_id) -> int`
- `check_version_or_raise(conn, entity_type, entity_id, expected_version) -> None`
- `approve_with_version_check(conn, entity_type, entity_id, expected_version, approved_by) -> None`
- `detect_manual_journal_collision(conn, posting_id, manual_journal_id) -> list[dict]`
- `quarantine_manual_journal(conn, manual_journal_id, reason) -> None`
**Dependencies:** sqlite3, datetime
**DB Tables:** documents, posting_jobs (version columns), manual_journal_entries

### `src/engines/audit_engine.py`
**Purpose:** CPA audit support: working papers, three-way matching, statistical sampling, trial balance, financial statements.
**Key Functions:**
- `ensure_audit_tables(conn) -> None`
- `create_working_paper(client_code, period, account_code, ...) -> str`
- `create_three_way_match(po_id, invoice_id, payment_id, conn) -> str`
- `statistical_sample(population_size, confidence_level, error_margin, conn, seed) -> list[str]`
- Trial balance generation, financial statements (balance sheet + income statement)
- Analytical procedures (variance analysis, ratio calculations)
**Dependencies:** sqlite3, decimal, random, uuid
**DB Tables:** working_papers, working_paper_items, audit_evidence, trial_balance, chart_of_accounts

### `src/engines/cas_engine.py`
**Purpose:** CAS-compliant materiality assessment (CAS 320) and risk assessment matrix (CAS 315).
**Key Functions:**
- `ensure_cas_tables(conn) -> None`
- `assess_materiality(engagement_id, basis, basis_amount, conn) -> str`
- `assess_risk(engagement_id, account_code, assertion, inherent_risk, control_risk, conn) -> str`
- `generate_management_rep_letter(engagement_id, language, conn) -> str` -- CAS 580
**Dependencies:** sqlite3, decimal, uuid, datetime
**DB Tables:** materiality_assessments, risk_assessments, management_representation_letters

### `src/engines/customs_engine.py`
**Purpose:** CBSA customs value determination, import GST/QST, remote services place of supply, FX validation.
**Key Functions:**
- `calculate_customs_value(invoice_amount, discount, ...) -> dict` -- CBSA Customs Act Section 45
- `calculate_import_gst_qst(customs_value, origin_country, destination_province) -> dict`
- `determine_place_of_supply(service_type, supplier_location, consumer_location) -> str`
- `validate_fx_rate(currency, fx_rate, rate_date) -> dict` -- Bank of Canada validation
- `decompose_credit_memo_complete(credit_memo_amount, tax_breakdown) -> dict`
**Dependencies:** decimal, datetime
**DB Tables:** boc_fx_rates (read)

### `src/engines/line_item_engine.py`
**Purpose:** Per-line invoice extraction with tax regime determination and total reconciliation.
**Key Functions:**
- `extract_invoice_lines(document_id, raw_ocr_text, conn) -> list[dict]` -- AI extraction via OpenRouter
- `assign_line_tax_regime(line, place_of_supply) -> str`
- `calculate_line_tax(line, tax_regime, is_tax_included) -> dict`
- `reconcile_invoice_lines(document_id, conn) -> dict`
- `allocate_deposit_proportionally(document_id, deposit_amount, conn) -> dict`
**Dependencies:** sqlite3, decimal, re, json
**DB Tables:** invoice_lines (write), documents (read)

### `src/engines/payroll_engine.py`
**Purpose:** Quebec payroll compliance: HSF tiers, QPP/CPP, QPIP/EI, RL-1/T4 reconciliation, CNESST.
**Key Functions:**
- `validate_hsf_rate(total_payroll, rate_used) -> dict` -- Health Services Fund rate tier validation
- `validate_qpp_rate(employee_amount, employee_rate) -> dict`
- `validate_qpip_rate(insurable_earnings, employee_rate) -> dict`
- `validate_source_deductions(payroll_summary, conn) -> list[dict]`
- `reconcile_rl1_t4(rl1_total, t4_total) -> dict`
- `validate_cnesst_premium(insurable_payroll, industry_unit, rate) -> dict`
**Dependencies:** decimal
**DB Tables:** None (validation only)

### `src/engines/license_engine.py`
**Purpose:** License key generation, validation, and tier-based feature gating.
**Key Functions:**
- `validate_license(license_key, conn) -> dict`
- `check_feature_access(conn, feature_name) -> bool`
- `generate_license_key(tier, firm_name, ...) -> str` -- HMAC-SHA256 signed
**Dependencies:** base64, hashlib, hmac, json, sqlite3
**DB Tables:** license_keys, license_audit_log
**Tiers:** essentiel (10 clients/3 users), professionnel (30/5), cabinet (75/15), entreprise (unlimited)

## src/agents/tools/ (38 Modules)

### `src/agents/tools/rules_engine.py`
**Purpose:** Pattern-based extraction of vendor, doc type, amounts, and dates using regex rules from `vendors.json`.
**Key Functions:**
- `RulesEngine.__init__(rules_dir: Path)` -- Loads `vendors.json`
- `RulesEngine.extract(text: str) -> RulesResult` -- Returns doc_type, confidence, vendor_name, total, document_date, currency
**DB Tables:** None (reads JSON rule files)

### `src/agents/tools/review_policy.py`
**Purpose:** Determines review status and effective confidence with fraud/substance blocking.
**Key Functions:**
- `decide_review_status(**kwargs) -> ReviewDecision` -- Full multi-stage decision
- `effective_confidence(rules_confidence, final_method, has_required, ai_confidence, fraud_flags, substance_flags) -> float`
- `should_auto_approve(confidence, fraud_flags, substance_flags) -> bool`
- `check_fraud_flags(fraud_flags: list[dict]) -> bool` -- Returns True if CRITICAL/HIGH
**DB Tables:** None (stateless evaluation)

### `src/agents/tools/posting_builder.py`
**Purpose:** Builds and syncs posting payloads from document and posting_jobs records.
**Key Functions:**
- `sync_posting_payload(conn, document_id, posting_id, refresh_updated_at) -> dict`
- `build_payload_from_sources(posting_row, document_row) -> dict`
- `fetch_document_row(conn, document_id) -> dict`
- `upsert_posting_job(conn, document_id, payload) -> str`
- `sync_all_posting_payloads() -> dict`
**DB Tables:** documents (read), posting_jobs (read/write)

### `src/agents/tools/duplicate_detector.py`
**Purpose:** Duplicate document detection using OCR-normalized scoring.
**Key Functions:**
- `score_pair(left, right) -> DuplicateCandidate` -- Scores two documents (0.0-1.0+)
- `find_duplicate_candidates(documents, min_score=0.85) -> list[DuplicateCandidate]`
- `normalize_invoice_number(invoice_number) -> str` -- OCR noise normalization (O->0, I->1, S->5)
**DB Tables:** documents (read via TaskStore)

### `src/agents/tools/exception_queue.py`
**Purpose:** Builds exception queue for documents requiring manual review.
**Key Functions:**
- `build_exception_items_for_document(doc, duplicate_map) -> list[ExceptionWorkItem]`
- `build_exception_queue(store: TaskStore) -> list[ExceptionWorkItem]`
**Buckets:** ocr_failed, processing_exception, needs_human_review, missing_client, missing_vendor, missing_doc_type, missing_amount, zero_amount, missing_document_date, possible_duplicate

### `src/agents/tools/fingerprint_utils.py`
**Purpose:** Physical and logical fingerprinting for deduplication and conflict detection.
**Key Functions:**
- `compute_file_sha256(file_path) -> str`
- `build_logical_fingerprint(vendor, date, amount, doc_type) -> str` -- SHA-256[:24]
- `build_physical_identity(file_name, file_hash) -> str` -- SHA-256[:24]
- `source_fingerprint(document) -> str` -- Cross-channel dedup (32 hex chars)
- `detect_reingest_conflict(new_document, existing_document, similarity_threshold) -> dict`
**DB Tables:** None

### `src/agents/tools/amount_policy.py`
**Purpose:** Determines bookkeeping amounts with multi-settlement and credit memo classification.
**Key Functions:**
- `choose_bookkeeping_amount(**kwargs) -> AmountPolicyResult`
- `choose_split_bookkeeping_amounts(**kwargs) -> list[SplitAmountResult]` -- Tax-aware splits
- `detect_credit_note_settlement(**kwargs) -> dict`
- `classify_credit_document(**kwargs) -> CreditClassification`
**DB Tables:** None

### `src/agents/tools/client_router.py`
**Purpose:** Routes documents to clients using scored signal matching.
**Key Functions:**
- `ClientRouter.route(text, sender_email) -> ClientRouteResult`
**Scoring:** sender_email (+10), account_number (+10), address (+6), client_name (+5), keyword (+2)
**DB Tables:** None (reads `client_map.json`)

### `src/agents/tools/vendor_intelligence.py`
**Purpose:** Maps vendor + doc_type to GL account, tax code, and category.
**Key Functions:**
- `VendorIntelligenceEngine.classify(vendor_name, doc_type) -> VendorIntelResult`
**DB Tables:** None (reads `vendor_intel.json`)

### `src/agents/tools/gl_mapper.py`
**Purpose:** Maps vendors and document types to GL accounts and tax codes.
**Key Functions:**
- `GLMapper.map(vendor_name, doc_type) -> GLMapResult`
**DB Tables:** None (reads `gl_map.json`)

### `src/agents/tools/qbo_online_adapter.py`
**Purpose:** QBO posting job management, vendor/account mapping, and transaction creation.
**Key Functions:**
- `ensure_posting_jobs_table(db_path) -> None`
- `list_ready_qbo_jobs(db_path) -> list`
- `update_posting_job_after_attempt(**kwargs) -> None`
- `apply_vendor_mapping(vendor_name, mappings) -> str`
**DB Tables:** posting_jobs (read/write)

### `src/agents/tools/qbo_reference_resolver.py`
**Purpose:** QBO authentication, configuration loading, and reference resolution.
**Key Functions:**
- `load_qbo_config(config_path) -> QBOConfig`
- `check_qbo_auth_status(config_path) -> dict`
**DB Tables:** None (reads `qbo_config.json`)

### `src/agents/tools/qbo_verify_transaction.py`
**Purpose:** Verifies posted QBO transactions by ID across entity types.
**Key Functions:**
- `verify_transaction(txn_id, qbo_config) -> dict` -- Tries purchase, bill, journalentry
**DB Tables:** None

### `src/agents/tools/qbo_list_refs.py`
**Purpose:** Lists QBO vendors and accounts via query API.
**Key Functions:**
- `list_vendors(limit=100) -> list[dict]`
- `list_accounts(limit=200) -> list[dict]`
**DB Tables:** None

### `src/agents/tools/ocr_engine.py` (alias)
See `src/engines/ocr_engine.py` above.

### `src/agents/tools/pdf_extract.py`
**Purpose:** PDF text extraction with OCR fallback using Tesseract and pdf2image.
**Key Functions:**
- `extract_pdf_text(pdf_path: Path) -> str` -- pdfminer first, Tesseract fallback (eng+fra)
**DB Tables:** None

### `src/agents/tools/doc_ai.py`
**Purpose:** AI-based document classification and extraction using OpenRouter.
**Key Functions:**
- `classify_and_extract(text, province="QC", language="EN") -> dict`
**DB Tables:** None

### `src/agents/tools/doc_extract.py`
**Purpose:** Text extraction from .txt, .csv, .json files.
**Key Functions:**
- `extract_text_from_file(path: Path) -> str`
**DB Tables:** None

### `src/agents/tools/local_document_processor.py`
**Purpose:** Local document processing with heuristic extraction (no LLM).
**Key Functions:**
- `process_document(file_path: Path) -> dict | None` -- Returns vendor, doc_type, amount, date, confidence
**DB Tables:** None

### `src/agents/tools/openrouter_client.py`
**Purpose:** OpenRouter API client for JSON-returning LLM calls.
**Key Functions:**
- `OpenRouterClient.chat_json(system, user, temperature=0.0) -> dict`
**Constants:** Default model: `deepseek/deepseek-chat`

### `src/agents/tools/ai_client_router.py`
**Purpose:** Maps document text to client registry using OpenRouter LLM with confidence scoring.
**Key Functions:**
- `AIClientRouter.route(text: str) -> AIClientRouteResult`
**DB Tables:** None (reads `client_map.json`)

### `src/agents/tools/graph_auth.py`
**Purpose:** Microsoft Graph authentication using MSAL device flow.
**Key Functions:**
- `GraphAuth.acquire_token(token_cache_file: Path) -> str`

### `src/agents/tools/graph_mail.py`
**Purpose:** Email operations via Microsoft Graph API.
**Key Functions:**
- `GraphMail.list_messages(mailbox, folder_name, top, unread_only) -> list[dict]`
- `GraphMail.download_file_attachments(mailbox, message_id) -> list[AttachmentFile]`
- `GraphMail.mark_read(mailbox, message_id) -> None`
- `GraphMail.move_message(mailbox, message_id, dest_folder_id) -> None`

### `src/agents/tools/graph_sharepoint.py`
**Purpose:** SharePoint file operations via Microsoft Graph API.
**Key Functions:**
- `GraphSharePoint.upload_bytes(drive_id, folder_path, filename, content, content_type) -> dict`
- `GraphSharePoint.list_folder_children(drive_id, folder_path, top=200) -> list[dict]`
- `GraphSharePoint.download_item_bytes(drive_id, item_id) -> bytes`
- `GraphSharePoint.move_item(drive_id, item_id, dest_folder_path, new_name) -> dict`

### `src/agents/tools/graph_list.py`
**Purpose:** SharePoint list CRUD via Microsoft Graph API.
**Key Functions:**
- `GraphList.get_list_by_name(site_id, display_name) -> ListRef`
- `GraphList.create_item(site_id, list_id, fields) -> dict`
- `GraphList.list_items(site_id, list_id, top=200) -> list[dict]`

### `src/agents/tools/client_registry.py`
**Purpose:** Client registry from SharePoint list via Graph API.
**Key Functions:**
- `ClientRegistry.load() -> None`
- `ClientRegistry.get(client_code) -> ClientRegistryEntry | None`

### `src/agents/tools/sharepoint_processor.py`
**Purpose:** Orchestrates SharePoint document ingestion with duplicate detection.
**Key Functions:**
- `process_sharepoint_once(max_files=10) -> dict` -- Full pipeline from SharePoint inbox to DB
**DB Tables:** documents (write via pipeline)

### `src/agents/tools/draft_csv_writer.py`
**Purpose:** Writes draft posting records to CSV with deduplication.
**Key Functions:**
- `append_draft_row(client_code: str, row: dict) -> bool`

### `src/agents/tools/explain_decision_formatter.py`
**Purpose:** Formats raw document decision data into human-readable bilingual explanations.
**Key Functions:**
- `build_human_decision_summary(raw_result: dict) -> str`

### `src/agents/tools/fingerprint_registry.py`
**Purpose:** JSON registry of processed fingerprints for deduplication.
**Key Functions:**
- `has_fingerprint(fp: str) -> bool`
- `add_fingerprint(fp: str) -> None`

### `src/agents/tools/document_processing_tool.py`
**Purpose:** Subprocess wrapper for document processing.
**Key Functions:**
- `DocumentProcessingTool.process_document(file_path: str) -> dict`

### `src/agents/tools/run_logger.py`
**Purpose:** JSONL logging utility for run records.
**Key Functions:**
- `append_jsonl(log_path: Path, obj: dict) -> None`

### `src/agents/tools/otocpa_runner.py`
**Purpose:** Orchestrates multi-stage pipeline runs with subprocess execution.
**Key Functions:**
- `run_python_script(script_name, args, timeout_seconds) -> StageResult`
- `get_review_queue_summary(db_path) -> dict` -- Counts by review_status
- `get_posting_queue_summary(db_path) -> dict` -- Counts by posting_status
**DB Tables:** documents (read), posting_jobs (read)

### `src/agents/tools/otocpa_workflow_runner.py`
**Purpose:** Email and SharePoint ingestion workflow management.
**DB Tables:** documents (via subprocess pipeline)

### `src/agents/tools/login_microsoft.py`
**Purpose:** Device flow login script for Microsoft Graph.

### `src/agents/tools/setup_wizard.py` (tools version)
**Purpose:** Interactive setup wizard for tenant configuration and Graph API authentication.
**Key Functions:**
- `run_setup_wizard() -> None` -- Multi-step interactive configuration

### `src/agents/tools/test_list_write.py`
**Purpose:** Test script for writing items to SharePoint list.

## src/i18n/

### `src/i18n/__init__.py`
**Purpose:** Translation function `t(key, lang, **kwargs)` with template substitution and cache.
**Key Functions:**
- `t(key: str, lang: str = "en", **kwargs) -> str` -- Returns translated string with `{placeholder}` substitution
- `switch_lang(lang: str) -> str` -- Returns "en" or "fr"
- `reload_cache() -> None` -- Clears translation cache
- `get_user_lang(user: dict) -> str` -- Extracts language preference from user dict

### `src/i18n/en.json` and `src/i18n/fr.json`
**Purpose:** Complete English and French translation dictionaries. Contain 300+ keys covering all dashboard labels, error messages, button text, email templates, tax terms, audit labels, and system messages.

## scripts/ (50+ Operational Scripts)

### HTTP Servers

| Script | Port | Purpose |
|--------|------|---------|
| `scripts/review_dashboard.py` | 8787 | Internal review dashboard (250+ routes, full RBAC) |
| `scripts/client_portal.py` | 8788 | Client self-service document upload portal |
| `scripts/setup_wizard.py` (scripts version) | 8790 | Interactive 6-step onboarding wizard |

### Database & Migration

| Script | Purpose |
|--------|---------|
| `scripts/migrate_db.py` | Safe additive schema migration -- adds missing columns, creates tables, never modifies existing data |
| `scripts/manage_dashboard_users.py` | CLI for user account management: init, add-user, reset-password, deactivate, list |
| `scripts/manage_clients.py` | CLI for client portal account management |
| `scripts/set_password.py` | Legacy SHA-256 password setter (deprecated) |

### Pipeline & Processing

| Script | Purpose | CLI Args |
|--------|---------|----------|
| `scripts/run_bookkeeper_agent.py` | Queue and process documents via BookkeeperAgent | `add <file>`, `run`, `run-all` |
| `scripts/run_posting_queue.py` | Execute approved posting jobs to QBO/Xero | `--target-system`, `--stop-on-error` |
| `scripts/run_review_queue_builder.py` | Rebuild review queue from document statuses | None |
| `scripts/run_auto_review_classifier.py` | Auto-classify documents as Ready/NeedsReview | None |
| `scripts/rebuild_document_store.py` | Full pipeline rebuild: enrichment, dedup, routing, posting | None |
| `scripts/folder_watcher.py` | Background folder monitoring service | `--folder`, `--default-client` |
| `scripts/ingest_folder_to_store.py` | Batch ingest from `data/incoming_documents/` | None |

### Banking & Matching

| Script | Purpose |
|--------|---------|
| `scripts/run_bank_matcher.py` | Match documents against bank transactions |
| `scripts/run_match_review_queue.py` | Build review queue from match results |
| `scripts/apply_match_decision.py` | Apply human decisions to matches (`--document-id`, `--decision`) |

### Learning & Knowledge

| Script | Purpose |
|--------|---------|
| `scripts/backfill_learning_memory.py` | Mine corrections from historical documents |
| `scripts/backfill_vendor_memory.py` | Mine vendor rules from posted jobs |
| `scripts/rebuild_learning_patterns.py` | Aggregate patterns from corrections |
| `scripts/seed_vendor_knowledge.py` | Seed initial vendor rules |
| `scripts/accelerate_learning.py` | Accelerate learning engine training |

### Export & Audit

| Script | Purpose |
|--------|---------|
| `scripts/export_ready_documents.py` | Export Ready documents to CSV |
| `scripts/export_qbo_transactions.py` | Export as QBO JSON transactions |
| `scripts/scan_duplicates.py` | Identify duplicate documents (min_score 0.85) |
| `scripts/audit_document_store.py` | Audit for missing critical fields |
| `scripts/build_exception_queue.py` | Build prioritized exception work queue |
| `scripts/run_document_identity_audit.py` | Audit fingerprint integrity |
| `scripts/check_posted_jobs.py` | View posted jobs |
| `scripts/cleanup_bad_documents.py` | Mark low-quality documents as Ignored |

### Vendor Rule Management

| Script | Purpose |
|--------|---------|
| `scripts/upsert_dell_rules.py` | Add Dell Canada vendor rules |
| `scripts/upsert_vendor_rules_wave2.py` | Add Amazon, Microsoft, CompanyCam rules |
| `scripts/upsert_vendor_rules_wave5.py` | Add Google, LastPass, OpenAI rules |
| `scripts/fix_vendor_amount_regexes.py` | Fix amount extraction regexes |
| `scripts/fix_wave3_routing_and_totals.py` | Fix routing and total extraction |
| `scripts/fix_wave4_dates.py` | Fix date extraction |
| `scripts/fix_wave6_rule_collisions.py` | Tighten rules to prevent false positives |
| `scripts/upgrade_client_map_soussol.py` | Enhance SOUSSOL client mapping |

### Operations & Setup

| Script | Purpose |
|--------|---------|
| `scripts/daily_digest.py` | Generate and send bilingual email digests |
| `scripts/setup_cloudflare.py` | Interactive Cloudflare Tunnel setup |
| `scripts/generate_license.py` | Generate license keys (`--tier`, `--firm`, `--months`) |
| `scripts/load_demo_data.py` | Mark 50 curated demo documents |
| `scripts/openclaw_case_diagnostics.py` | Diagnose OpenClaw case decisions |
| `scripts/run_openclaw_queue.py` | Run OpenClaw case orchestration |

### Test Data Generation

| Script | Purpose |
|--------|---------|
| `scripts/generate_test_data.py` | Generate synthetic test documents |
| `scripts/advanced_training_data.py` | Generate advanced training data |
| `scripts/generate_messy_images.py` | Create degraded images for OCR testing |
| `scripts/generate_canada_quebec_stress_test.py` | Generate Canada/Quebec stress test data |
| `scripts/benchmark_ocr.py` | Benchmark OCR accuracy |
| `scripts/run_stress_test.py` | Run full stress test suite |

---

# Section 4 -- Database Schema

All tables are stored in a single SQLite database at `data/otocpa_agent.db`. Schema is managed by `scripts/migrate_db.py` which performs safe, additive migrations (adds missing columns and tables without modifying existing data).

## Core Document Management

### `documents`
Primary table for all scanned/ingested documents.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `document_id` | TEXT | PK | -- | Unique document identifier |
| `file_name` | TEXT | Yes | -- | Original filename |
| `file_path` | TEXT | Yes | -- | Original file path |
| `client_code` | TEXT | Yes | -- | Associated client code |
| `vendor` | TEXT | Yes | -- | Vendor/supplier name |
| `doc_type` | TEXT | Yes | -- | Document type (invoice, receipt, etc.) |
| `amount` | REAL | Yes | -- | Document total amount |
| `document_date` | TEXT | Yes | -- | Transaction date (ISO format) |
| `gl_account` | TEXT | Yes | -- | Chart of accounts mapping |
| `tax_code` | TEXT | Yes | -- | Tax classification (T/Z/E/M/I/HST/etc.) |
| `category` | TEXT | Yes | -- | Expense category |
| `review_status` | TEXT | Yes | -- | Workflow status (Ready/NeedsReview/Exception/Ignored/Posted) |
| `confidence` | REAL | Yes | -- | OCR/extraction confidence score (0.0-1.0) |
| `raw_result` | TEXT | Yes | -- | Raw OCR/extraction JSON blob |
| `invoice_number` | TEXT | Yes | -- | OCR-extracted invoice number |
| `invoice_number_normalized` | TEXT | Yes | -- | OCR-normalized invoice number (O->0, I->1) |
| `currency` | TEXT | Yes | -- | Document currency (CAD, USD, etc.) |
| `subtotal` | REAL | Yes | -- | Subtotal before tax |
| `tax_total` | REAL | Yes | -- | Total tax amount |
| `extraction_method` | TEXT | Yes | -- | Method used (ocr, manual, rules, vision) |
| `ingest_source` | TEXT | Yes | -- | Source (email, folder, portal, sharepoint) |
| `fraud_flags` | TEXT | Yes | -- | JSON array of fraud flag dictionaries |
| `fraud_override_reason` | TEXT | Yes | -- | Reason for fraud override |
| `fraud_override_locked` | INTEGER | No | 0 | Whether fraud override is locked |
| `substance_flags` | TEXT | Yes | -- | JSON array of substance classification |
| `entry_kind` | TEXT | Yes | -- | Entry type (expense, receipt, refund, etc.) |
| `review_history` | TEXT | No | '[]' | JSON array of review status changes |
| `raw_ocr_text` | TEXT | Yes | -- | Raw OCR text output |
| `hallucination_suspected` | INTEGER | No | 0 | AI hallucination flag |
| `correction_count` | INTEGER | No | 0 | Number of manual corrections |
| `handwriting_low_confidence` | INTEGER | No | 0 | Handwriting uncertainty flag |
| `handwriting_sample` | INTEGER | No | 0 | Handwriting-only sample flag |
| `created_at` | TEXT | No | '' | ISO timestamp |
| `has_line_items` | INTEGER | No | 0 | Whether invoice has itemized lines |
| `lines_reconciled` | INTEGER | No | 0 | Whether line items reconcile to total |
| `line_total_sum` | REAL | Yes | -- | Sum of all line item totals |
| `invoice_total_gap` | REAL | Yes | -- | Gap between line sum and invoice total |
| `deposit_allocated` | INTEGER | No | 0 | Whether deposit amounts allocated |
| `personal_use_percentage` | REAL | Yes | -- | Disallowed personal-use percentage |
| `version` | INTEGER | No | 1 | Optimistic locking version |
| `activation_date` | TEXT | Yes | -- | Recognition timing: when to activate |
| `recognition_period` | TEXT | Yes | -- | Recognition timing: accounting period |
| `recognition_status` | TEXT | No | 'immediate' | Recognition: immediate or deferred |
| `submitted_by` | TEXT | Yes | -- | Client portal: who submitted |
| `client_note` | TEXT | Yes | -- | Client portal: attached note |

**Trigger:** `trg_document_version_increment` -- Auto-increments `version` on UPDATE when version unchanged.

### `posting_jobs`
Job queue for posting documents to QuickBooks Online.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `posting_id` | TEXT | PK | -- | Unique posting job identifier |
| `document_id` | TEXT | Yes | -- | Reference to documents table |
| `file_name` | TEXT | Yes | -- | Original filename |
| `file_path` | TEXT | Yes | -- | Original file path |
| `client_code` | TEXT | Yes | -- | Client code |
| `vendor` | TEXT | Yes | -- | Vendor name |
| `document_date` | TEXT | Yes | -- | Document date |
| `amount` | REAL | Yes | -- | Amount to post |
| `currency` | TEXT | Yes | -- | Currency code |
| `doc_type` | TEXT | Yes | -- | Document type |
| `category` | TEXT | Yes | -- | Expense category |
| `gl_account` | TEXT | Yes | -- | GL account code |
| `tax_code` | TEXT | Yes | -- | Tax code |
| `memo` | TEXT | Yes | -- | Transaction memo |
| `review_status` | TEXT | Yes | -- | Review status |
| `confidence` | REAL | Yes | -- | Confidence score |
| `approval_state` | TEXT | Yes | -- | Approval workflow state |
| `posting_status` | TEXT | Yes | -- | Posting status (pending/posted/failed) |
| `reviewer` | TEXT | Yes | -- | Reviewer username |
| `blocking_issues` | TEXT | Yes | -- | JSON array of blocking issues |
| `notes` | TEXT | Yes | -- | User notes |
| `error_text` | TEXT | Yes | -- | Error message if posting failed |
| `assigned_to` | TEXT | Yes | -- | Assigned reviewer |
| `target_system` | TEXT | Yes | -- | Target system (qbo, wave, xero) |
| `entry_kind` | TEXT | Yes | -- | Entry type |
| `external_id` | TEXT | Yes | -- | External system transaction ID |
| `created_at` | TEXT | Yes | -- | ISO timestamp |
| `updated_at` | TEXT | Yes | -- | ISO timestamp |
| `version` | INTEGER | No | 1 | Optimistic locking version |

**Trigger:** `trg_posting_version_increment` -- Auto-increments `version` on UPDATE when version unchanged.

## Authentication

### `dashboard_users`
User accounts for the review dashboard and client portal.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `username` | TEXT | PK | -- | Login username (normalized) |
| `display_name` | TEXT | Yes | -- | Display name |
| `password_hash` | TEXT | No | -- | bcrypt hash |
| `role` | TEXT | No | -- | Role: owner, manager, employee, client |
| `is_active` | INTEGER | No | 1 | Account active flag |
| `created_at` | TEXT | No | -- | ISO timestamp |
| `updated_at` | TEXT | No | '' | ISO timestamp |
| `last_login_at` | TEXT | Yes | -- | Last login |
| `must_reset_password` | INTEGER | No | 0 | Force password reset on legacy hash |
| `client_code` | TEXT | Yes | -- | Associated client (for portal users) |
| `language` | TEXT | Yes | -- | Preferred language (en/fr) |
| `whatsapp_number` | TEXT | Yes | -- | WhatsApp sender for OpenClaw bridge |
| `telegram_id` | TEXT | Yes | -- | Telegram ID for OpenClaw bridge |

### `dashboard_sessions`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `session_token` | TEXT | PK | -- | Session token |
| `username` | TEXT | No | -- | FK to dashboard_users |
| `role` | TEXT | No | '' | User role |
| `created_at` | TEXT | No | -- | ISO timestamp |
| `expires_at` | TEXT | No | -- | Expiration timestamp |
| `last_seen_at` | TEXT | No | '' | Last activity |

**Indexes:** `idx_dashboard_sessions_username`, `idx_dashboard_sessions_expires_at`
**Foreign Key:** `username` REFERENCES `dashboard_users(username)`

## Learning & Memory

### `vendor_memory`
Learned vendor accounting preferences from approvals.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | INTEGER | PK AUTO | -- | Primary key |
| `client_code` | TEXT | Yes | -- | Client code |
| `vendor` | TEXT | No | -- | Vendor name |
| `vendor_key` | TEXT | No | '' | Normalized vendor key |
| `client_code_key` | TEXT | No | '' | Normalized client code key |
| `gl_account` | TEXT | Yes | -- | Preferred GL account |
| `tax_code` | TEXT | Yes | -- | Preferred tax code |
| `doc_type` | TEXT | Yes | -- | Typical document type |
| `category` | TEXT | Yes | -- | Typical category |
| `approval_count` | INTEGER | No | 0 | Number of approved transactions |
| `confidence` | REAL | No | 0.0 | Learned confidence |
| `last_amount` | REAL | Yes | -- | Last transaction amount |
| `last_document_id` | TEXT | Yes | -- | Last referenced document |
| `last_source` | TEXT | Yes | -- | Last extraction source |
| `last_used` | TEXT | Yes | -- | Last usage timestamp |
| `created_at` | TEXT | No | -- | ISO timestamp |
| `updated_at` | TEXT | No | -- | ISO timestamp |

**Indexes:** `idx_vendor_memory_vendor_key`, `idx_vendor_memory_client_vendor`, `idx_vendor_memory_lookup`

### `learning_memory_patterns`
Aggregated learning patterns from corrections and approvals.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | INTEGER | PK AUTO | -- | Primary key |
| `memory_key` | TEXT | No, UNIQUE | -- | Composite lookup key |
| `event_type` | TEXT | No | '' | Event type (approval, correction) |
| `vendor` | TEXT | Yes | -- | Vendor name |
| `vendor_key` | TEXT | No | '' | Normalized vendor key |
| `client_code` | TEXT | Yes | -- | Client code |
| `client_code_key` | TEXT | No | '' | Normalized client code key |
| `doc_type` | TEXT | Yes | -- | Document type |
| `category` | TEXT | Yes | -- | Category |
| `gl_account` | TEXT | Yes | -- | GL account |
| `tax_code` | TEXT | Yes | -- | Tax code |
| `outcome_count` | INTEGER | No | 0 | Total outcomes |
| `success_count` | INTEGER | No | 0 | Successful outcomes |
| `review_count` | INTEGER | No | 0 | Times reviewed |
| `posted_count` | INTEGER | No | 0 | Times posted |
| `avg_confidence` | REAL | No | 0.0 | Average confidence |
| `avg_amount` | REAL | Yes | -- | Average amount |
| `last_document_id` | TEXT | Yes | -- | Last document reference |
| `last_payload_json` | TEXT | Yes | -- | Last payload snapshot |
| `created_at` | TEXT | No | -- | ISO timestamp |
| `updated_at` | TEXT | No | -- | ISO timestamp |

**Index:** `idx_learning_memory_patterns_lookup`

### `learning_corrections`
Field-level correction tracking.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | INTEGER | PK AUTO | -- | Primary key |
| `document_id` | TEXT | No | '' | Document reference |
| `client_code` / `client_code_key` | TEXT | No | '' | Client identification |
| `vendor` / `vendor_key` | TEXT | No | '' | Vendor identification |
| `doc_type` / `doc_type_key` | TEXT | No | '' | Document type |
| `category` / `category_key` | TEXT | No | '' | Category |
| `field_name` / `field_name_key` | TEXT | No | '' | Corrected field name |
| `old_value` / `old_value_key` | TEXT | No | '' | Original value |
| `new_value` / `new_value_key` | TEXT | No | '' | Corrected value |
| `reviewer` | TEXT | No | '' | Who made the correction |
| `source` | TEXT | No | '' | Correction source |
| `confidence_before` | REAL | Yes | -- | Confidence before correction |
| `support_count` | INTEGER | No | 1 | Times this correction repeated |
| `created_at` | TEXT | No | '' | ISO timestamp |
| `updated_at` | TEXT | No | '' | ISO timestamp |

**Indexes:** `idx_learning_corrections_lookup`, `idx_learning_corrections_vendor_field`

## Banking

### `bank_statements`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `statement_id` | TEXT | PK | -- | Unique identifier |
| `bank_name` | TEXT | Yes | -- | Detected bank name |
| `file_name` | TEXT | Yes | -- | Original filename |
| `client_code` | TEXT | Yes | -- | Client code |
| `imported_by` | TEXT | Yes | -- | Username |
| `imported_at` | TEXT | Yes | -- | ISO timestamp |
| `period_start` / `period_end` | TEXT | Yes | -- | Statement period |
| `transaction_count` | INTEGER | -- | 0 | Total transactions |
| `matched_count` | INTEGER | -- | 0 | Matched count |
| `unmatched_count` | INTEGER | -- | 0 | Unmatched count |

### `bank_transactions`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | INTEGER | PK AUTO | -- | Primary key |
| `statement_id` | TEXT | No | '' | FK to bank_statements |
| `document_id` | TEXT | No | '' | FK to documents |
| `txn_date` | TEXT | Yes | -- | Transaction date |
| `description` | TEXT | Yes | -- | Bank description |
| `debit` / `credit` | REAL | Yes | -- | Amounts |
| `balance` | REAL | Yes | -- | Running balance |
| `matched_document_id` | TEXT | Yes | -- | Matched document |
| `match_confidence` | REAL | Yes | -- | Match confidence |
| `match_reason` | TEXT | Yes | -- | Match explanation |

### `bank_reconciliations`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `reconciliation_id` | TEXT | PK | -- | Unique identifier |
| `client_code` | TEXT | No | -- | Client code |
| `account_name` | TEXT | No | -- | Bank account name |
| `account_number` | TEXT | Yes | -- | Account number |
| `period_end_date` | TEXT | No | -- | Period end date |
| `statement_ending_balance` | REAL | No | -- | Statement balance |
| `gl_ending_balance` | REAL | No | -- | GL balance |
| `adjusted_bank_balance` / `adjusted_book_balance` | REAL | Yes | -- | Adjusted balances |
| `difference` | REAL | Yes | -- | Remaining difference |
| `status` | TEXT | No | 'open' | open / finalized |
| `prepared_by` / `reviewed_by` | TEXT | Yes | -- | Personnel |
| `notes` | TEXT | Yes | -- | Notes |

### `reconciliation_items`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `item_id` | TEXT | PK | -- | Unique identifier |
| `reconciliation_id` | TEXT | No | -- | FK to bank_reconciliations |
| `item_type` | TEXT | No | -- | deposit_in_transit / outstanding_cheque / bank_error / book_error |
| `description` | TEXT | No | -- | Item description |
| `amount` | REAL | No | -- | Amount |
| `transaction_date` / `cleared_date` | TEXT | Yes | -- | Dates |
| `document_id` | TEXT | Yes | -- | FK to documents |
| `status` | TEXT | No | 'outstanding' | Status |

**Index:** `idx_recon_items_recon`

## Tax & Filing

### `client_config`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `client_code` | TEXT | PK | -- | Client code |
| `quick_method` | INTEGER | No | 0 | Using Quick Method for GST/QST |
| `quick_method_type` | TEXT | No | 'retail' | retail / service |
| `filing_frequency` | TEXT | No | 'monthly' | monthly / quarterly / annual |
| `gst_registration_number` | TEXT | Yes | -- | GST number |
| `qst_registration_number` | TEXT | Yes | -- | QST number |
| `fiscal_year_end` | TEXT | No | '12-31' | MM-DD format |

### `gst_filings`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | INTEGER | PK AUTO | -- | Primary key |
| `client_code` | TEXT | No | -- | Client code |
| `period_label` | TEXT | No | -- | Filing period |
| `deadline` | TEXT | No | -- | Filing deadline |
| `filed_at` / `filed_by` | TEXT | Yes | -- | Filing record |
| `is_amended` | INTEGER | No | 0 | Amendment flag |

**Constraint:** UNIQUE(client_code, period_label)

## Audit & Engagement

### `working_papers`

| Column | Type | Description |
|--------|------|-------------|
| `paper_id` | TEXT PK | Unique identifier |
| `client_code` | TEXT | Client code |
| `period` | TEXT | Audit period |
| `engagement_type` | TEXT | Default 'audit' |
| `account_code` / `account_name` | TEXT | Account reference |
| `balance_per_books` / `balance_confirmed` / `difference` | REAL | Balances |
| `tested_by` / `reviewed_by` | TEXT | Personnel |
| `sign_off_at` | TEXT | Sign-off timestamp |
| `status` | TEXT | open / completed |

### `engagements`

| Column | Type | Description |
|--------|------|-------------|
| `engagement_id` | TEXT PK | Unique identifier |
| `client_code` | TEXT | Client code |
| `period` | TEXT | Engagement period |
| `engagement_type` | TEXT | Default 'audit' |
| `status` | TEXT | planning / in_progress / completed |
| `partner` / `manager` / `staff` | TEXT | Team members |
| `planned_hours` / `actual_hours` | REAL | Time budget |
| `budget` / `fee` | REAL | Financial |

### Other Audit Tables
- **`audit_evidence`** -- Evidence documentation with linked documents and match status
- **`working_paper_items`** -- Individual test items with tick marks
- **`trial_balance`** -- Generated trial balance by account code
- **`chart_of_accounts`** -- Master chart of accounts (account_code PK, account_type, normal_balance)
- **`materiality_assessments`** -- CAS 320 materiality (basis, planning/performance/trivial amounts)
- **`risk_assessments`** -- CAS 315 risk matrix (assertions, inherent/control/combined risk)
- **`control_tests`** -- CAS 330 control testing documentation
- **`management_representation_letters`** -- CAS 580 bilingual rep letters
- **`related_parties`** -- CAS 550 identified related parties
- **`related_party_transactions`** -- Related party transaction analysis

## Correction & Amendment

### `correction_chains`
Links original documents to corrections, credit memos, and refunds.

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | INTEGER PK AUTO | Chain link ID |
| `chain_root_id` | TEXT | Root document of chain |
| `source_document_id` / `target_document_id` | TEXT | Linked documents |
| `link_type` | TEXT | credit_memo / correction / refund |
| `economic_effect` | TEXT | reduction / reversal |
| `amount` | REAL | Effect amount |
| `tax_impact_gst` / `tax_impact_qst` | REAL | Tax effects |
| `uncertainty_flags` | TEXT | JSON array |
| `status` | TEXT | active / superseded |
| `superseded_by` / `rollback_of` | INTEGER | Chain references |

### `amendment_flags`
Tracks filed periods needing amendment.

| Column | Type | Description |
|--------|------|-------------|
| `flag_id` | INTEGER PK AUTO | Flag ID |
| `client_code` / `filed_period` | TEXT | Period reference |
| `trigger_document_id` | TEXT | Triggering document |
| `trigger_type` | TEXT | credit_memo / correction |
| `reason_en` / `reason_fr` | TEXT | Bilingual reasons |
| `status` | TEXT | open / resolved |

**Constraint:** UNIQUE(client_code, filed_period, trigger_document_id)

### Other Correction Tables
- **`document_snapshots`** / **`posting_snapshots`** -- Point-in-time state snapshots
- **`document_clusters`** / **`document_cluster_members`** -- Persistent duplicate grouping
- **`overlap_anomalies`** -- Cross-vendor work overlap flags
- **`rollback_log`** -- Explicit rollback audit trail
- **`credit_memo_invoice_link`** -- Credit memo to invoice linking

## Other Tables

- **`time_entries`** -- Billable time tracking per document/client
- **`invoices`** -- Generated invoices from time entries
- **`period_close`** / **`period_close_locks`** -- Month-end close checklist and locks
- **`audit_log`** -- AI API call tracking and system events
- **`vendor_aliases`** -- DBA alias mapping for vendor reconciliation
- **`boc_fx_rates`** -- Bank of Canada FX rate cache
- **`manual_journal_entries`** -- Manual journal entries with collision detection
- **`match_decisions`** -- Bank matching decision records
- **`invoice_lines`** -- Per-line invoice parsing results
- **`client_communications`** -- Accountant-to-client message log
- **`messaging_log`** -- Inbound/outbound messaging events

## DB Triggers

| Trigger | Table | Action |
|---------|-------|--------|
| `trg_document_version_increment` | documents | Auto-increments `version` on UPDATE when version is unchanged |
| `trg_posting_version_increment` | posting_jobs | Auto-increments `version` on UPDATE when version is unchanged |

These triggers enforce optimistic locking. The concurrency engine (`src/engines/concurrency_engine.py`) reads the version, performs work, then attempts an UPDATE with `WHERE version = expected_version`. If another process modified the row, the version will have already incremented and the WHERE clause fails.

---

# Section 5 -- API Reference

## Review Dashboard (Port 8787)

The review dashboard is implemented as a Python `BaseHTTPRequestHandler` in `scripts/review_dashboard.py`. All routes require authentication via session cookie unless noted.

### Authentication

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/login` | None | -- | Login form | HTML |
| POST | `/login` | None | username, password, lang | Authenticate user | Redirect to `/` or `/change_password` |
| POST | `/logout` | Any | -- | End session | Redirect to `/login` |
| GET | `/change_password` | Any | -- | Password change form | HTML |
| POST | `/change_password` | Any | new_password, confirm_password | Update password | Redirect with flash |
| POST | `/set_language` | Any | lang (en/fr) | Change UI language | Redirect with lang cookie |

**Rate limiting:** 5 failed login attempts per IP in 15 minutes triggers HTTP 429.

### Document Review Queue

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/` | Any | -- | Home page / review queue | HTML |
| GET | `/document?id={id}` | Any | id | Document detail view | HTML |
| GET | `/pdf?id={id}` | Any | id | Serve document PDF | PDF |
| POST | `/document/update` | Any | vendor, client_code, doc_type, amount, document_date, gl_account, tax_code, category, review_status | Update document fields | Redirect |
| POST | `/document/hold` | Any | document_id, hold_reason | Place document on hold | Redirect |
| POST | `/document/return_ready` | Any | document_id | Return document to Ready | Redirect |
| POST | `/assign` | Any | document_id, assigned_to | Assign document to user | Redirect |
| POST | `/claim` | Any | document_id | Self-assign document | Redirect |
| POST | `/apply_suggestion` | Any | document_id, field, value | Apply learning suggestion | Redirect |

### QuickBooks Posting

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| POST | `/qbo/build` | Any | document_id | Create posting job | Redirect |
| POST | `/qbo/approve` | Any | document_id, fraud_override_reason?, fraud_override_ack? | Approve for QBO posting | Redirect |
| POST | `/qbo/post` | Any | document_id | Post transaction to QBO | Redirect |
| POST | `/qbo/retry` | Any | document_id | Retry failed posting | Redirect |

### Banking & Reconciliation

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/bank_import` | Manager+ | -- | Bank import form | HTML |
| POST | `/bank_import` | Manager+ | client_code, file (multipart) | Import bank statement | HTML |
| POST | `/bank_import/match` | Manager+ | bank_document_id, invoice_document_id | Match bank txn to invoice | Redirect |
| POST | `/bank_import/confirm_split` | Manager+ | transaction_id, invoice_ids[] | Confirm split payment | Redirect |
| GET | `/reconciliation?client_code={cc}&period={p}` | Manager+ | client_code, period | Reconciliation list | HTML |
| GET | `/reconciliation/new` | Manager+ | -- | New reconciliation form | HTML |
| POST | `/reconciliation/create` | Manager+ | client_code, account_name, account_number, period_end_date, statement_balance, gl_balance | Create reconciliation | Redirect |
| GET | `/reconciliation/detail?id={id}` | Manager+ | id | Reconciliation detail | HTML |
| POST | `/reconciliation/add_item` | Manager+ | reconciliation_id, item_type, description, amount, transaction_date | Add line item | Redirect |
| POST | `/reconciliation/clear_item` | Manager+ | item_id, reconciliation_id | Mark item cleared | Redirect |
| POST | `/reconciliation/finalize` | Manager+ | reconciliation_id | Finalize reconciliation | Redirect |
| GET | `/reconciliation/pdf?id={id}` | Manager+ | id | Download reconciliation PDF | PDF |

### Tax & Filing

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/filing_summary?client_code={cc}&period_start={ps}&period_end={pe}` | Manager+ | -- | Tax filing summary | HTML |
| GET | `/revenu_quebec?client_code={cc}&period_start={ps}&period_end={pe}` | Owner | -- | RQ tax filing view | HTML |
| GET | `/revenu_quebec/pdf?...` | Owner | -- | Download RQ tax PDF | PDF |
| POST | `/revenu_quebec/set_config` | Owner | client_code, quick_method, quick_method_type | Save RQ config | Redirect |
| GET | `/calendar` | Manager+ | -- | Filing calendar | HTML |
| POST | `/calendar/mark_filed` | Manager+ | client_code, period_label, deadline | Mark period as filed | Redirect |
| POST | `/calendar/save_config` | Manager+ | client_code, filing_frequency, gst/qst numbers, fiscal_year_end | Save filing config | Redirect |

### Period Close

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/period_close?client_code={cc}&period={p}` | Manager+ | -- | Period close checklist | HTML |
| GET | `/period_close/pdf?...` | Manager+ | -- | Period close PDF | PDF |
| POST | `/period_close/check_item` | Manager+ | item_id, status, notes, responsible_user, due_date, client_code, period | Update checklist item | Redirect |
| POST | `/period_close/lock` | Manager+ | client_code, period | Lock period | Redirect |

### Time Tracking & Invoicing

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/time?client_code={cc}&period_start={ps}&period_end={pe}&hourly_rate={r}` | Manager+ | -- | Time tracking summary | HTML |
| POST | `/time/start` | Any | document_id, client_code | Start time entry | JSON `{entry_id}` |
| POST | `/time/stop` | Any | entry_id, duration_minutes | Stop time entry | 204 |
| POST | `/invoice/generate` | Manager+ | client_code, period_start, period_end, hourly_rate, firm details | Generate invoice | PDF |

### Audit Module

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/working_papers?...` | Manager+ | client_code, period, engagement_type | Working papers list | HTML |
| GET | `/working_papers/pdf?...` | Manager+ | -- | Working papers PDF | PDF |
| POST | `/working_papers/signoff` | Manager+ | paper_id, balance_confirmed | Sign off working paper | Redirect |
| POST | `/working_papers/create_from_coa` | Manager+ | client_code, period, engagement_type | Generate WPs from chart of accounts | Redirect |
| GET | `/audit/evidence?...` | Manager+ | client_code, period | Audit evidence | HTML |
| POST | `/audit/evidence/link` | Manager+ | evidence_id, linked_doc_ids | Link documents to evidence | Redirect |
| GET | `/audit/sample?...` | Manager+ | client_code, period, account_code, sample_size | Audit sampling | HTML |
| POST | `/audit/sample/mark` | Manager+ | paper_id, document_id, tick_mark | Mark sample tested | Redirect |
| GET | `/engagements?...` | Manager+ | client_code, status | Engagement list | HTML |
| POST | `/engagements/create` | Manager+ | client_code, period, type, team, budget | Create engagement | Redirect |
| POST | `/engagements/issue` | Manager+ | engagement_id | Issue engagement letter | PDF |
| GET | `/audit/materiality?engagement_id={id}` | Manager+ | -- | Materiality assessment | HTML |
| POST | `/audit/materiality/save` | Manager+ | engagement_id, basis, basis_amount, notes | Save materiality | Redirect |
| GET | `/audit/risk?engagement_id={id}` | Manager+ | -- | Risk assessment matrix | HTML |
| POST | `/audit/risk/generate` | Manager+ | engagement_id | Generate risk matrix | Redirect |
| GET | `/audit/rep_letter?engagement_id={id}` | Owner | -- | Management rep letter | HTML |
| POST | `/audit/rep_letter/generate` | Owner | engagement_id | Generate rep letter | Redirect |
| POST | `/audit/rep_letter/sign` | Owner | letter_id, management_name, management_title | Sign rep letter | Redirect |
| GET | `/audit/controls?engagement_id={id}` | Manager+ | -- | Control tests | HTML |
| POST | `/audit/controls/add` | Manager+ | engagement_id, control_name, objective, test_type | Add control test | Redirect |
| POST | `/audit/controls/results` | Manager+ | test_id, items_tested, exceptions, conclusion | Record test results | Redirect |
| GET | `/audit/related_parties?engagement_id={id}` | Manager+ | -- | Related parties | HTML |
| POST | `/audit/related_parties/add` | Manager+ | engagement_id, party details | Add related party | Redirect |
| POST | `/audit/related_parties/disclosure` | Manager+ | engagement_id | Generate RP disclosure | Redirect |
| GET | `/financial_statements?...` | Manager+ | client_code, period | Financial statements | HTML |
| GET | `/financial_statements/pdf?...` | Manager+ | -- | Financial statements PDF | PDF |
| GET | `/audit/analytical?...` | Manager+ | client_code, period | Analytical procedures | HTML |
| GET | `/audit/analytical/pdf?...` | Manager+ | -- | Analytical PDF | PDF |

### Administration

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/users` | Any | -- | User management | HTML |
| POST | `/users/add` | Owner | username, password, role, display_name | Create user | Redirect |
| POST | `/users/set_password` | Owner | username_target, new_password | Reset password | Redirect |
| GET | `/portfolios` | Any | -- | Staff portfolio management | HTML |
| POST | `/portfolios/assign` | Manager+ | client_code, username_target | Assign client | Redirect |
| POST | `/portfolios/remove` | Manager+ | client_code, username_target | Remove client | Redirect |
| POST | `/portfolios/move` | Manager+ | client_code, from_user, to_user | Move client | Redirect |
| GET | `/troubleshoot` | Owner | -- | System troubleshooting | HTML |
| GET | `/troubleshoot/backup` | Owner | -- | Download DB backup | SQLite |
| POST | `/troubleshoot/restart` | Owner | -- | Restart service | Service restart |
| GET | `/admin/cache` | Owner | -- | AI cache stats | HTML |
| POST | `/admin/cache/clear` | Owner | -- | Clear AI cache | Redirect |
| GET | `/admin/vendor_aliases` | Owner | -- | Vendor alias manager | HTML |
| POST | `/admin/vendor_aliases` | Owner | action, canonical_vendor, alias_name | Manage aliases | Redirect |
| POST | `/admin/vendor_memory` | Owner | action, vendor, client_code | Manage vendor memory | Redirect |
| GET | `/analytics` | Owner | -- | Dashboard analytics | HTML |
| GET | `/license` | Owner | -- | License management | HTML |
| POST | `/license/activate` | Owner | license_key | Activate license | Redirect |

### Onboarding

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/onboarding/step1` | Owner | -- | Firm setup | HTML |
| GET | `/onboarding/step2` | Owner | -- | Staff setup | HTML |
| GET | `/onboarding/step3` | Owner | -- | Client setup | HTML |
| POST | `/onboarding/staff/add` | Owner | display_name, username, role, password | Add staff | Redirect |
| POST | `/onboarding/client/add` | Owner | client_code, client_name, province, entity_type | Add client | Redirect |
| POST | `/onboarding/complete` | Owner | -- | Complete onboarding | Redirect |

### Other

| Method | Path | Role | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/qr?client_code={cc}` | Manager+ | client_code | QR code generator | HTML |
| GET | `/qr/download?client_code={cc}` | Manager+ | -- | Download QR PNG | PNG |
| GET | `/qr/pdf` | Manager+ | -- | All QR codes PDF | PDF |
| GET | `/communications?client_code={cc}` | Manager+ | -- | Client communications | HTML |
| POST | `/communications/draft` | Manager+ | document_id, lang | AI-draft client message | Redirect |
| POST | `/communications/send` | Manager+ | document_id, comm_id, to_email, subject, message | Send via SMTP | Redirect |
| GET | `/journal_entries` | Manager+ | -- | Manual journal entries | HTML |
| POST | `/journal_entries` | Manager+ | action, client_code, period, accounts, amount | Create/manage entries | Redirect |
| POST | `/ingest/openclaw` | **None** | JSON payload | OpenClaw document ingestion (no auth) | JSON |

## Client Portal (Port 8788)

| Method | Path | Auth | Parameters | Description | Returns |
|--------|------|------|------------|-------------|---------|
| GET | `/` or `/portal` or `/login` | No | -- | Login / portal page | HTML |
| POST | `/login` | No | username, password, lang | Client login | Redirect |
| POST | `/logout` | Yes | -- | Client logout | Redirect |
| POST | `/set_language` | Yes | lang | Change language | Redirect |
| POST | `/upload` | Yes | file (multipart), note | Upload document (100MB max, 20/day) | Redirect |

---

# Section 6 -- Adding New Features

## Adding a New Fraud Detection Rule

1. Open `src/engines/fraud_engine.py`.
2. Add a new private function following the naming convention `_rule_your_rule_name(conn, document_id, ...)`. It should return `dict` (with keys: `rule`, `severity`, `description`, `risk_level`, `evidence`) or `None` if the rule does not trigger.
3. Wire the new rule into `run_fraud_detection()` by calling your function and appending the result to the `flags` list.
4. Add a corresponding test in `tests/test_fraud_engine.py` with at least:
   - A test that triggers the rule
   - A test that does NOT trigger it (negative case)
5. Add a red-team test in `tests/red_team/test_fraud_and_review_destruction.py` that tries to evade the rule.
6. The review policy (`src/agents/tools/review_policy.py`) already reads `fraud_flags` by severity -- CRITICAL and HIGH block auto-approval automatically.

## Adding a New Tax Engine Function

1. Open `src/engines/tax_engine.py`.
2. Use `Decimal` arithmetic for all monetary calculations (never `float`).
3. Register any new tax code in the `_TAX_CODE_REGISTRY` dictionary with its rate and province.
4. Add tests in `tests/test_tax_engine.py` covering:
   - Basic calculation
   - Edge cases (zero amount, one-cent rounding, very large amounts)
   - Province-specific behavior
5. Add red-team tests in `tests/red_team/test_tax_torture.py`.

## Adding a New Dashboard Route

1. Open `scripts/review_dashboard.py`.
2. Add a handler in the `do_GET()` or `do_POST()` method of the request handler class, matching on the URL path.
3. Check authentication: call `self._get_session()` to get the current user. Check `role` for authorization.
4. For Manager+ routes: `if role not in ('owner', 'manager'): return self._send_403()`
5. For Owner-only routes: `if role != 'owner': return self._send_403()`
6. Use `t(key, lang)` for all user-visible strings (bilingual support).
7. Add any new i18n keys to both `src/i18n/en.json` and `src/i18n/fr.json`.
8. Add a test in `tests/` that verifies the route exists and enforces the correct role.

## Adding a New AI Prompt Template

1. Create or edit a file in `src/agents/prompts/`.
2. Use clear system + user message separation.
3. Always request JSON output from the LLM.
4. Wire the prompt through `src/agents/tools/openrouter_client.py` or `src/agents/tools/doc_ai.py`.
5. Validate AI output with the hallucination guard before writing to the database.
6. Log the call to `audit_log` with provider, task_type, and latency.

## Adding New Bilingual Strings

1. Open `src/i18n/en.json` and add your key-value pair.
2. Open `src/i18n/fr.json` and add the French translation with the same key.
3. Use `{placeholder}` syntax for dynamic values: `"greeting": "Hello {name}"`.
4. In code, call `t("greeting", lang, name=username)`.
5. Add a test in `tests/test_i18n.py` in `TestJsonFiles.test_required_keys` to ensure your key exists in both files.
6. Never leave a value blank -- the test suite enforces non-empty values.

## Writing Tests for New Features

1. Create a test file in `tests/` named `test_your_feature.py`.
2. Use pytest fixtures for database setup:
   ```python
   @pytest.fixture
   def conn():
       conn = sqlite3.connect(":memory:")
       conn.row_factory = sqlite3.Row
       # Bootstrap schema
       return conn
   ```
3. Use `@pytest.mark.parametrize` for multiple input scenarios.
4. Use `@pytest.mark.xfail(reason="...")` only for known limitations with a clear reason.
5. For adversarial tests, add to `tests/red_team/` following the destruction test pattern.

## Running the Test Suite

```bash
# Run all tests
python -m pytest tests/ -v

# Run fast tests only (skip slow integration tests)
python -m pytest tests/ -v -m "not slow"

# Run a specific test file
python -m pytest tests/test_fraud_engine.py -v

# Run red team tests
python -m pytest tests/red_team/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

---

# Section 7 -- Test Suite Guide

## Test Organization

| Directory | Count | Type | Description |
|-----------|-------|------|-------------|
| `tests/` | 45+ | Unit/Integration | Feature tests, engine tests, pipeline wiring tests |
| `tests/red_team/` | 20 | Adversarial | Destruction tests, attack vectors, edge-case exploitation |
| `tests/documents/` | -- | Fixtures | Test document files (PDFs, images) |
| `tests/documents_real/` | -- | Fixtures | Real-world document samples |

## Running Fast Tests

```bash
# Skip slow integration tests and tests that require external services
python -m pytest tests/ -v -m "not slow" --ignore=tests/red_team/

# Run only unit tests (fastest)
python -m pytest tests/test_tax_engine.py tests/test_fraud_engine.py tests/test_i18n.py tests/test_posting_builder_sync.py -v
```

## Running Red Team Tests

```bash
# All adversarial tests
python -m pytest tests/red_team/ -v

# Specific attack category
python -m pytest tests/red_team/test_tax_torture.py -v
python -m pytest tests/red_team/test_fraud_and_review_destruction.py -v
python -m pytest tests/red_team/test_hallucination_guard_attacks.py -v
```

## What Each Test File Covers

### Core Engine Tests
- **`test_tax_engine.py`** -- 42 tests: GST/QST calculation, tax extraction, validation, ITC/ITR, filing summaries
- **`test_fraud_engine.py`** -- All 13 fraud rules: weekend/holiday, amount anomaly, duplicates, bank account changes, new vendor, timing
- **`test_uncertainty_engine.py`** -- 12-part uncertainty specification: SAFE_TO_POST, PARTIAL_POST_WITH_FLAGS, BLOCK_PENDING_REVIEW
- **`test_reconciliation_engine.py`** -- Bank reconciliation lifecycle: create, populate, calculate, finalize, PDF
- **`test_ocr_engine.py`** -- Format detection (magic bytes), image normalization, text extraction
- **`test_bank_parser.py`** -- Amount parsing, date parsing, bank detection (Desjardins, BMO, TD, RBC, National)

### Feature Tests
- **`test_i18n.py`** -- 40+ tests: JSON validity, required keys, `t()` function, template substitution, DB schema language columns
- **`test_period_close.py`** -- 60+ tests: checklist lifecycle, period locking, PDF generation, i18n keys
- **`test_time_tracking.py`** -- 50+ tests: time entries, invoicing, PDF generation, GST/QST math
- **`test_revenu_quebec.py`** -- 50+ tests: quick method rates, GST/QST prefill, RQ PDF, filing config
- **`test_cas_modules.py`** -- Management rep letters (FR/EN), materiality, risk assessment
- **`test_posting_builder_sync.py`** -- Posting payload sync between document and posting_jobs rows

### Pipeline & Wiring Tests
- **`test_pipeline_wiring.py`** -- Fraud flags flow through pipeline, Block 1-7 wiring
- **`test_independent_regression.py`** -- 8 regression areas: sign-aware matching, tax context, fraud blocking
- **`test_feature_blocks.py`** -- Substance classification, mixed tax resolution, review policy
- **`test_hallucination_guard.py`** -- Numeric total verification, vendor validation, DB write tests

### Integration Tests
- **`test_openclaw_bridge.py`** -- OpenClaw ingestion, sender mapping, messaging events
- **`test_document_pipeline.py`** -- End-to-end document processing on test PDFs
- **`test_ai_router.py`** -- SIN redaction, HTTP response mocking, router configuration
- **`test_security_hardening.py`** -- Rate limiting, login attempts table, filename sanitization

### Red Team Test Categories

| File | Attack Surface |
|------|---------------|
| `test_championship_redteam.py` | 100+ attacks across 10 categories: tax, OCR, reconciliation, fraud evasion, audit trail, bilingual, multi-currency, inventory, CAS, metamorphic |
| `test_fraud_and_review_destruction.py` | Invoice splitting, round-dollar bursts, confidence boosting, borderline signals |
| `test_tax_torture.py` | Penny rounding accumulation, negative amounts, province mismatch, extract-from-total drift |
| `test_hallucination_guard_attacks.py` | 1-cent tolerance, non-numeric subtotals, vendor validation bypass |
| `test_wave2_amount_parsing.py` | French comma-decimal, OCR noise (O vs 0), ambiguous separators |
| `test_audit_evidence_destruction.py` | Evidence chain sufficiency, three-way matching, CAS compliance |
| `test_state_drift_destruction.py` | Persistence cycles, retry/reprocessing, multi-actor races |
| `test_wave2_posting_builder.py` | SQL injection, concurrent upsert races, JSON round-trip fidelity |
| `test_ocr_and_injection_destruction.py` | OCR injection, prompt injection via document content |
| `test_security_and_chaos_destruction.py` | Auth bypass attempts, session manipulation, chaos scenarios |

## Understanding xfails

Tests marked with `@pytest.mark.xfail(reason="...")` represent known limitations that are **accepted by design**. They document behavior that is either:
- Not yet implemented (future feature)
- A deliberate boundary (requires human judgment)
- An edge case where the current behavior is acceptable given the tradeoff

See [Section 9](#section-9--known-limitations) for the specific xfails and their rationale.

## Adding New Adversarial Tests

1. Create a file in `tests/red_team/` following the pattern `test_*_destruction.py` or `test_*_attacks.py`.
2. Structure tests by attack category using pytest classes.
3. Document the attack vector in the test docstring.
4. Target specific engine functions directly -- do not use the HTTP routes.
5. Use in-memory SQLite for isolation.
6. Include both "attack succeeds" (the system catches it) and "attack fails" (the system is robust) tests.

---

# Section 8 -- Deployment Guide

## Windows Installer Build Process

### Prerequisites
- Python 3.11 installed
- PyInstaller installed (`pip install pyinstaller`)
- Tesseract OCR installed at `C:\Program Files\Tesseract-OCR\`
- Poppler installed at `C:\poppler\Library\bin\`

### Build Steps

1. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

2. Build the executable:
   ```powershell
   pyinstaller otocpa-setup.spec
   ```
   The `.spec` file targets `src/agents/tools/setup_wizard.py` as the entry point.

3. The installer is output to `dist/`.

### PowerShell Installer (`install_client.ps1`)

The PowerShell script automates client installation:
- Detects Python 3.11/3.12
- Creates install directory at `C:\OtoCPA` (configurable)
- Sets up virtual environment
- Installs pip dependencies
- Creates desktop shortcuts
- Configures the folder watcher service

## Cloudflare Tunnel Setup

Run the interactive setup wizard:
```bash
python scripts/setup_cloudflare.py
```

This walks through:
1. **Download** cloudflared.exe
2. **Login** to Cloudflare (browser-based)
3. **Create tunnel** (or reuse existing)
4. **Write config** (tunnel config.yml)
5. **DNS route** (set up hostname)
6. **Windows service** (register as background service)
7. **Save URL** to `otocpa.config.json`

The tunnel exposes the client portal (port 8788) as an HTTPS public URL.

## License Key Generation

```bash
python scripts/generate_license.py \
  --tier professionnel \
  --firm "Cabinet ABC" \
  --months 12 \
  --secret YOUR_HMAC_SECRET
```

**License tiers:**

| Tier | Max Clients | Max Users | Features |
|------|-------------|-----------|----------|
| essentiel | 10 | 3 | Basic review, basic posting |
| professionnel | 30 | 5 | + AI router, bank parser, fraud detection, Revenu Quebec, time tracking |
| cabinet | 75 | 15 | + Analytics, Microsoft 365, filing calendar, client communications |
| entreprise | Unlimited | Unlimited | + Audit module, financial statements, sampling, API access |

License keys are HMAC-SHA256 signed. The signing secret must match between generation and validation. Store the secret in `.env` as `OTOCPA_SIGNING_SECRET`.

## Demo Mode Setup

1. Load demo data:
   ```bash
   python scripts/load_demo_data.py
   ```
   This selects 50 curated documents (10 per client) showcasing fraud flags, AI warnings, meal receipts, and bank matches.

2. Enable demo mode in `otocpa.config.json`:
   ```json
   { "demo_mode": true }
   ```

3. Start the dashboard:
   ```bash
   python scripts/review_dashboard.py
   ```

## Production Checklist

Before deploying to a client:

- [ ] Run `scripts/migrate_db.py` to ensure schema is current
- [ ] Run `scripts/manage_dashboard_users.py init` to create the owner account
- [ ] Set strong passwords for all accounts
- [ ] Configure `otocpa.config.json` with correct AI API keys
- [ ] Configure `.env` with QBO credentials (access token, realm ID)
- [ ] Set `QBO_ENVIRONMENT=production` (not sandbox)
- [ ] Generate and activate a license key for the correct tier
- [ ] Set up Cloudflare Tunnel if the client portal needs public access
- [ ] Configure SMTP for daily digest emails
- [ ] Test the folder watcher with a sample document
- [ ] Verify QBO connectivity: `python src/agents/tools/qbo_list_refs.py vendors`
- [ ] Run the full test suite: `python -m pytest tests/ -v`
- [ ] Back up the empty database before the first real document
- [ ] Set `demo_mode: false` in config

---

# Section 9 -- Known Limitations

## Remaining xfails

The test suite contains a small number of expected failures that represent accepted design boundaries:

1. **OCR noise in French comma-decimal amounts** -- Ambiguous separators like `1,234,56` cannot be deterministically parsed when both thousands-separator and decimal-separator are commas. The system flags these for human review rather than guessing.

2. **DD/MM vs MM/DD date ambiguity** -- Dates like `03/04/2026` are ambiguous without language context. The uncertainty engine flags `DATE_AMBIGUOUS` and the review policy blocks auto-approval. This is intentional -- guessing wrong would corrupt the ledger.

3. **Rounding drift in multi-line tax extraction** -- When extracting taxes from a total with many line items, accumulated Decimal rounding can differ from the vendor's calculation by 1-2 cents. The reconciliation validator classifies this gap rather than forcing an exact match.

4. **Mixed tax invoices with no line items** -- When an invoice contains both taxable and exempt items but no line-item breakdown, the system cannot determine the tax split. It blocks auto-approval and requires human classification.

## Features Not Yet Built

- **TaxLink** -- Direct electronic filing with CRA and Revenu Quebec (currently generates PDF forms for manual filing)
- **Microsoft 365 OAuth** -- The current Graph API integration uses MSAL device flow. A full OAuth 2.0 authorization code flow with refresh tokens is not yet implemented.
- **PostgreSQL** -- The database is SQLite only. Migration to PostgreSQL for multi-user concurrent access is planned but not built.
- **Multi-currency real-time FX** -- Bank of Canada FX rates are cached in `boc_fx_rates` but automatic daily fetching is not yet implemented.
- **Xero integration** -- The posting queue supports `--target-system xero` as a parameter but the Xero adapter is not implemented.

## Design Boundaries Requiring Human Judgment

These are not bugs or missing features -- they are intentional limits where the system defers to human expertise:

- **Substance classification ambiguity** -- When a transaction could be either CapEx or repair, the system flags it for review rather than guessing. A $4,000 HVAC invoice could be a new unit (CapEx) or a repair (expense).
- **Credit memo decomposition** -- When a credit memo lacks a tax breakdown, the system cannot determine the pre-tax vs. tax split. It flags `CREDIT_MEMO_TAX_SPLIT_UNPROVEN` rather than assuming proportional allocation.
- **Cross-vendor work overlap** -- When two vendors invoice for similar work in the same period, the system flags `SUBCONTRACTOR_WORK_SCOPE_OVERLAP` but cannot determine if the work is actually duplicated.
- **Period recognition timing** -- Deferred revenue/expense recognition requires business judgment about when to activate. The system tracks `activation_date` and `recognition_period` but does not auto-determine them.

---

# Section 10 -- Security Model

## Authentication System

### Password Hashing
- All passwords are hashed with **bcrypt** (12 rounds) before storage.
- Legacy SHA-256 hashes are detected on login; the user is forced to reset their password (`must_reset_password = 1`).
- Passwords are never stored in plaintext or logged.

### Session Management
- Sessions are stored in `dashboard_sessions` with a cryptographically random token.
- Session cookies are set with `HttpOnly`, `Secure`, and `SameSite` flags.
- Sessions expire after 12 hours.
- Expired sessions are cleaned up on access.

### Rate Limiting
- Login attempts are tracked by IP address.
- 5 failed attempts in 15 minutes triggers HTTP 429 (Too Many Requests).
- Rate limiting applies to both the dashboard (port 8787) and client portal (port 8788).

## Role-Based Access Control Matrix

| Feature | Owner | Manager | Employee | Client |
|---------|-------|---------|----------|--------|
| View review queue | Y | Y | Y | -- |
| Edit document fields | Y | Y | Y | -- |
| Approve for QBO posting | Y | Y | Y | -- |
| Post to QBO | Y | Y | Y | -- |
| Bank reconciliation | Y | Y | -- | -- |
| Period close | Y | Y | -- | -- |
| Tax filing (RQ) | Y | -- | -- | -- |
| Filing calendar | Y | Y | -- | -- |
| Time tracking | Y | Y | Y | -- |
| Invoice generation | Y | Y | -- | -- |
| Working papers & audit | Y | Y | -- | -- |
| Analytics | Y | -- | -- | -- |
| User management | Y | -- | -- | -- |
| License management | Y | -- | -- | -- |
| System troubleshooting | Y | -- | -- | -- |
| AI cache management | Y | -- | -- | -- |
| Vendor alias management | Y | -- | -- | -- |
| Onboarding wizard | Y | -- | -- | -- |
| Upload documents | -- | -- | -- | Y |
| View own submissions | -- | -- | -- | Y |

## DB Trigger Enforcement

Two triggers enforce optimistic locking on the most critical tables:

- `trg_document_version_increment` on `documents` -- Prevents lost updates when two users edit the same document simultaneously. The concurrency engine reads the version, performs work, then updates with `WHERE version = expected`. If another process modified the row, the version mismatch causes the update to fail safely.
- `trg_posting_version_increment` on `posting_jobs` -- Same protection for posting operations.

Additionally, `period_close_locks` enforces period immutability -- once a period is locked, no documents in that period can be modified through the dashboard.

## Audit Trail Immutability

- Every document field change is appended to `review_history` (JSON array) with timestamp and username.
- The `audit_log` table records all AI API calls, login events, and administrative actions.
- Correction chains maintain full provenance: every credit memo, correction, and rollback is linked to its source with timestamps and reasons.
- Document snapshots (`document_snapshots`) and posting snapshots (`posting_snapshots`) preserve point-in-time state for audit lineage.
- The rollback log (`rollback_log`) stores before/after state for every rollback operation.

## Cloudflare Security Layer

When the client portal is exposed via Cloudflare Tunnel:

- All traffic is encrypted with TLS (HTTPS only).
- Cloudflare provides DDoS protection and bot filtering.
- The tunnel connects outbound from the client machine -- no inbound ports need to be opened on the firewall.
- The dashboard (port 8787) is NOT exposed through the tunnel -- it is accessible only on the local network.

## Data Residency

The following data **never leaves the client premises**:

- The SQLite database (`data/otocpa_agent.db`) and all its contents
- Original document files (PDFs, images)
- Extracted text and OCR results
- QBO credentials (access tokens, realm IDs)
- User passwords (stored as bcrypt hashes)
- Session tokens
- Client configuration

The following data **does leave** the network (encrypted in transit):

- Document text sent to AI providers (OpenRouter/DeepSeek) for extraction and classification. SIN numbers are redacted before sending.
- QBO API calls to post transactions to QuickBooks Online.
- Microsoft Graph API calls for email and SharePoint access (when configured).
- Cloudflare Tunnel traffic for the client portal (when configured).
- SMTP connections for daily digest emails (when configured).

---

*This guide was generated from the OtoCPA codebase as of 2026-03-25.*
