# OtoCPA Migration Gaps — 2026-04-20

A CPA onboarding OtoCPA likely has prior data in QBO, Sage 50,
Caseware, or Xero. Here's what we can and can't import today.

## Current import capabilities

| Source | Format | Status |
| --- | --- | --- |
| Bank statements | CSV | **Supported** via ``src/engines/bank_parser.py`` |
| OCR ingest | PDF / JPEG / PNG / HEIC | **Supported** via ``src/engines/ocr_engine.py`` |
| OpenClaw bridge | WhatsApp / Telegram attachments | **Supported** via ``/ingest/openclaw`` (R4 API-key gated) |
| Chart of accounts | built-in seed | **Auto-seeded** (FR/EN Quebec chart via ``seed_chart_of_accounts``) |

## Gaps — no direct import

| Source | Format | What's missing |
| --- | --- | --- |
| QuickBooks Online | API, IIF, QBO export | No API connector **inbound**; only outbound posting. Clients must re-enter or CSV-bridge. |
| Sage 50 Canada | Sage backup (.SAI / .PTB) | Proprietary format; no parser. |
| Caseware | CL files | Proprietary; no parser. |
| Xero | CSV / API | No import tool. |
| Clients list (bulk) | CSV | No ``/clients/import`` endpoint. Must add one per CPA. |
| Trial balance / opening balances | CSV | ``opening_balances`` table exists, but no bulk-upload UI. |
| Journal entries (historical) | CSV / IIF | No import — must re-enter each JE. |

## Recommended minimum for beta

For a CPA to realistically migrate 5 clients off QBO, the following
would dramatically reduce onboarding friction. Ranked by effort:

1. **CSV clients import** (~2 days): ``POST /clients/import`` accepts
   a CSV with headers (client_code, client_name, contact_email,
   language, whatsapp_number). Upsert behavior. Dry-run mode first.
2. **CSV opening balances import** (~1 day): upload trial-balance
   CSV, writes to ``opening_balances`` for the period. Flowed into
   financial statements via the R2 fix.
3. **Sage50 CSV round-trip** (~3 days): the export engine already
   produces Sage50-compatible CSV; an import would read the same
   shape for initial historical load.
4. **QBO Journal Entry CSV** (~3 days): QBO lets users export JEs
   to CSV; a matching import parses vendor/debit/credit/memo.
5. **Caseware / Sage50 proprietary formats**: defer — requires
   reverse-engineered parsers; not on the Tier-1 path.

## What exists that might help

- ``src/engines/bank_parser.py::_parse_csv`` — robust CSV reader with
  encoding detection; can be a template for client / JE importers.
- ``src/engines/export_engine.py::generate_csv`` — emits CSV with
  known schema; pair-importer reversibility possible.
- ``scripts/setup_wizard.py`` — already handles initial owner user +
  firm + client-code seeding; could grow a CSV upload step.

## Current-codebase tests

See ``tests/adversarial/test_migration_paths.py`` for the feasibility
checks wrapped around this doc.
