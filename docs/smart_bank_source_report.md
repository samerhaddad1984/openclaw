# Smart Bank Source Selection — Report

Six phases. Bank-transaction data now flows from whichever source is
already available to the client, with automatic deduplication when
both are.

## What changed

- New ``clients.bank_source`` column drives per-client routing
  (`qbo` / `plaid` / `both` / `none`).
- New columns on ``bank_transactions``: ``firm_code``, ``source``,
  ``external_id``, ``qbo_account_id``, ``qbo_sync_token``,
  ``hidden_duplicate``. Partial UNIQUE index on
  ``(firm_code, client_code, source, external_id)`` so QBO rows and
  Plaid rows co-exist without collision.
- New audit table ``bank_tx_dedup`` records every match the engine
  detects (with confidence score) even when it doesn't auto-hide.
- Ten new modules:
  - ``src/integrations/bank_source_schema.py`` — idempotent DDL.
  - ``src/integrations/qbo_bank_pull.py`` — Purchase / Deposit /
    Transfer / Check puller.
  - ``src/engines/bank_tx_dedup.py`` — cross-source dedup with
    weighted confidence.
  - ``src/integrations/bank_source_setup.py`` — decision engine +
    HTML renders + HTTP handlers (EN + FR).
  - ``src/integrations/bank_source_recon.py`` — multi-source
    reconciliation query helpers + source badge.
  - Extension to ``src/integrations/qbo_sync.py`` that hooks bank
    pull + dedup into ``incremental_sync``.

## Flow summary

At ``/clients/{code}/bank/setup`` the dashboard probes for:

1. QBO active connection (`qbo_connections.status='active'`)?
2. QBO has at least one Bank account with at least one Purchase?
3. Plaid connection active (`bank_connections.active=1`)?

Five UI states (see ``docs/bank_data_sources.md`` for the full
decision tree).

## Dedup behavior

- Weighted confidence: amount 0.6, date proximity 0.2,
  description fuzz 0.2. Auto-hide threshold 0.75.
- QBO wins over Plaid when both present.
- Manual override via ``/clients/{code}/bank/dedup`` page —
  Un-dedup restores the row but preserves the audit trail.

## Test results

| Suite | Tests | Status |
| --- | --- | --- |
| `tests/bank_source/test_schema.py` | 10 | pass |
| `tests/qbo/test_qbo_bank_pull.py` | 13 | pass |
| `tests/bank_source/test_dedup.py` | 17 | pass |
| `tests/bank_source/test_smart_setup.py` | 17 | pass |
| `tests/bank_source/test_recon_multi_source.py` | 9 | pass |
| `tests/bank_source/test_orchestrator_bank_sync.py` | 5 | pass |
| **Total** | **71** | **71 pass** |

QBO-only client: reconciliation works without a Plaid connection
(exercised by the smart-setup + orchestrator tests with
`bank_source='qbo'`). Plaid-only client: unchanged
(`bank_source='plaid'` pre-existing behaviour). Both-source: dedup
hides the Plaid duplicate, reconciliation query returns QBO-only
unmatched rows.

Schema drift guard: clean across every commit.

## Commit trail

| Phase | Commit | Scope |
| --- | --- | --- |
| 1 | `f61d12c59` | schema (bank_source + 6 cols + dedup table) |
| 2 | `4336b89cf` | QBOBankPull (Purchase / Deposit / Transfer / Check) |
| 3 | `479294b45` | BankTransactionDeduplicator with confidence scoring |
| 4 | `9fae79697` | setup UI + 4 routes + French copy |
| 5 | `e73ac9019` | reconciliation helpers + source badge |
| 6 | *this commit* | orchestrator hook + docs |

## Known limitations

- **Transfer legs look like two entries** (one `out` on the source,
  one `in` on the destination). That matches the bank-ledger
  representation — each side appears on its own statement — and is
  NOT treated as a duplicate because the two legs are on different
  accounts. The dedup engine's SQL filters
  `bt1.source != bt2.source`, so same-source pairs (like the two
  legs of a single transfer) are never considered.
- **Dedup is fuzzy.** A low-confidence match (< 0.75) stays in
  `bank_tx_dedup` as a breadcrumb but does not auto-hide. The CPA
  reviews manually. This is deliberately conservative — hidden
  rows never surface to reconciliation.
- **Check numbers format differently** between QBO (just the
  DocNumber) and Plaid (banks embed `CHECK #1041` or similar in
  the description). The description-similarity signal is only
  0.2 of the weight, so matches lean on amount+date. Edge cases
  with very different descriptions fall to manual review.
- **QBO doesn't push bank transactions via webhook.** Dedup/sync
  lean on the 15-min cron cadence for QBO. Plaid pushes are
  real-time as before.
- **`clients.bank_source` is set at sync-from-QBO time**, not at
  OAuth connect time. A client with QBO connected but no bank sync
  yet stays at `bank_source='none'` until the first sync-from-QBO
  click or incremental-sync run.
