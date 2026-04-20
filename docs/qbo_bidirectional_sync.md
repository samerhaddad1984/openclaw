# QBO Bidirectional Sync — Final Report

Ten phases. All committed and pushed individually. 95 tests added
across ``tests/qbo/``.

## Capabilities

### Pull (QBO → OtoCPA)

- Reference entities: ``Account``, ``Customer``, ``Vendor``.
- Transactions: ``JournalEntry``, ``Bill``, ``Invoice`` (full +
  incremental via ``MetaData.LastUpdatedTime >=``).
- Pagination: QBO ``STARTPOSITION`` / ``MAXRESULTS`` auto-paginates.
- Rate limits: 429 → honour ``Retry-After`` (capped 60 s). 5xx →
  exponential backoff (1/2/4 s). 401 → refresh via existing
  ``refresh_access_token``. 3 retries per request.
- Source attribution: new JEs default to ``qbo_origin``. When
  ``sync_state`` already shows ``otocpa_origin`` (i.e. we pushed this
  JE), the source stays ``otocpa_origin`` and ``local_je_id`` is
  preserved across re-pulls so we never lose linkage.

### Push (OtoCPA → QBO)

- Create: ``push_journal_entry``, ``push_bill``. Resolves local GL
  code / vendor name to QBO refs via the cached entity tables.
- Update: ``push_journal_entry_update`` uses the current SyncToken
  from ``qbo_sync_state`` for optimistic concurrency.
- Void: ``push_journal_entry_void`` (QBO doesn't delete JEs; we void
  via ``?operation=void``).
- SyncToken tracking: every successful push writes back the returned
  ``SyncToken`` + ``last_pushed_at`` so the next update round-trips
  cleanly.

### Webhooks (real-time QBO-side changes)

- Signature: HMAC-SHA256, base64, constant-time compare.
- Dedup: synthetic ``event_id = '<realm>:<entity>:<id>:<lastUpdated>'``
  with UNIQUE constraint. Re-sends drop silently.
- Always-200: handler returns 200 even on bad signature so Intuit
  doesn't storm-retry; real status is in the JSON body (Stripe-style).
- Async processing: ``pending_events`` + ``process_one_event``.
  Create/Update → pull single entity. Delete/Void →
  ``sync_status='deleted'``. Merge → ``sync_status='conflict'``
  with reason ``qbo_merge_event`` (can't be auto-resolved).

### Scheduled sync

- ``scheduled_sync_all(db_path)`` iterates every active
  ``qbo_connections`` row and runs ``incremental_sync`` per
  ``(firm, client)``.
- Per-client errors are caught and reported in the rollup so one bad
  client doesn't stop the rest.
- Designed to run every 15 min from cron.

### Conflict resolution

Four strategies:

- ``otocpa_wins``: push local state over QBO via
  ``push_journal_entry_update``.
- ``qbo_wins``: pull single entity by Id, overwrite local,
  mark synced.
- ``flag_for_review``: keep ``sync_status='conflict'``; UI surfaces
  it until a CPA picks a winner.
- ``merge``: scaffold — currently falls back to review until
  domain-specific merge logic is wired per entity.

Detection: when **both**
``last_local_modified > last_pushed_at`` and
``last_qbo_modified > last_pushed_at`` are true, the row is promoted
to ``sync_status='conflict'``.

### Unified financial statements

- ``unified_trial_balance``: merges native TB
  (``audit_engine.generate_trial_balance``) with QBO-origin rows from
  ``gl_transactions WHERE source='qbo'``. Per-account provenance tags
  (``sources`` = ``{'native', 'qbo'}`` subset) + per-row native /
  qbo_origin debit + credit breakdowns.
- ``unified_income_statement``: classifier-driven revenue / expense
  split with configurable COA numbering.
- ``unified_balance_sheet``: 1xxx assets, 2xxx liabilities, 3xxx
  equity, with balanced flag at 0.01 tolerance.
- Double-count protection: QBOPull refuses to mirror
  ``otocpa_origin`` JEs into ``gl_transactions``, so only genuine
  qbo-direct rows reach ``source='qbo'``.

### UI + HTTP surface

Pure-function render helpers + HTTP handlers in
``src/integrations/qbo_sync_ui.py``. Routes the dashboard wires:

| Route | Handler |
| --- | --- |
| GET /qbo/sync/dashboard | ``render_sync_dashboard`` |
| POST /qbo/sync/initial | ``handle_initial_sync`` |
| POST /qbo/sync/now | ``handle_incremental_sync`` |
| GET /qbo/sync/status | ``handle_sync_status_api`` |
| GET /qbo/conflicts | ``render_conflicts_page`` |
| POST /qbo/conflicts/resolve | ``handle_resolve_conflict`` |
| POST /qbo/webhook | ``handle_webhook_route`` |

## Test results

| Suite | Tests | Status |
| --- | --- | --- |
| tests/qbo/test_qbo_schema.py | 8 | pass |
| tests/qbo/test_qbo_pull.py | 13 | pass |
| tests/qbo/test_qbo_pull_transactions.py | 11 | pass |
| tests/qbo/test_qbo_push.py | 11 | pass |
| tests/qbo/test_qbo_conflicts.py | 10 | pass |
| tests/qbo/test_qbo_webhook.py | 14 | pass |
| tests/qbo/test_qbo_sync_orchestrator.py | 9 | pass |
| tests/qbo/test_unified_financials.py | 10 | pass |
| tests/qbo/test_qbo_sync_ui.py | 9 | pass |
| **Total** | **95** | **95 pass** |

Schema drift guard: clean across every commit.

Sandbox E2E: ``scripts/qbo_sandbox_e2e.py`` is wired end-to-end
(initial_sync → push → incremental → conflict induction → resolve →
unified TB balanced). It exits with code **2 (SKIP)** when no active
``qbo_connections`` row exists — as in this sandbox run — so CI
gating is trivial. An operator can run it against a real sandbox
after completing OAuth at ``/qbo/connect?client_code=<C>``.

## Commit trail (10 phases, 10 commits)

| Phase | Commit | Scope |
| --- | --- | --- |
| 1 | `3d5bd58e9` | schema (9 tables + indexes) |
| 2 | `ea89cc372` | pull: accounts, customers, vendors |
| 3 | `ada568f95` | pull: JEs / bills / invoices + GL mirror |
| 4 | `d67cde6da` | push: JEs (create/update/void) + bills |
| 5 | `978af0fb9` | conflict detection + 4 strategies |
| 6 | `5b056cc6d` | webhooks: verify + idempotent queue |
| 7 | `7462ac3c7` | sync orchestrator |
| 8 | `c58f0b7df` | unified TB / IS / BS |
| 9 | `9781665a4` | UI render + HTTP handlers |
| 10 | *this commit* | sandbox E2E script + docs |

## Known limitations

- QBO doesn't support deleting JEs, only voiding — we void via
  ``?operation=void``. The sync_state row transitions to
  ``sync_status='voided'``, not 'deleted'.
- Merge events (two QBO entities fused into one) cannot be
  auto-resolved. They flag as conflicts with
  ``reason='qbo_merge_event'`` for manual CPA review.
- Attachments (receipt images, bill PDFs) are not synced —
  attachments stay in OtoCPA. QBO's Attachable endpoint is a separate
  surface that the spec didn't include.
- Payments are a no-op placeholder. Bill/invoice balances are
  refreshed when the parent is re-pulled, which is sufficient for
  open-item reporting but loses per-payment provenance.
- UI wiring: complete. ``scripts/review_dashboard.py`` now routes
  ``/qbo/dashboard``, ``/qbo/conflicts``, ``/qbo/sync/status``,
  ``/qbo/sync/initial``, ``/qbo/sync/now``, ``/qbo/conflicts/resolve``,
  and ``/qbo/webhook`` to the handlers in ``qbo_sync_ui.py`` with the
  documented auth semantics. ``/qbo/webhook`` added to
  ``_CSRF_EXEMPT_POSTS`` so Intuit POSTs don't hit the Origin check.
  Verified by ``tests/qbo/test_route_wiring.py`` (8 tests, including
  an enumeration meta-test + live HTTP round-trips).
- Scheduled-sync cron registered:
  ``/etc/cron.d/otocpa-qbo-sync`` invokes
  ``scripts/qbo_scheduled_sync.py`` every 15 minutes and logs to
  ``/var/log/otocpa/qbo_sync.log``.

## Final verification (2026-04-20)

- Full pytest suite: **7 671 passed**, 1 order-flake (the
  pre-existing ``test_render_troubleshoot_contains_db_path`` that
  passes in isolation — same one flagged in the R4 report), 18
  skipped, 3 deselected. 10m30s.
- Service restart: ``systemctl restart otocpa`` → active,
  ``/health`` returns 200.
- Cron dry-run: ``scripts/qbo_scheduled_sync.py`` exits 0 with
  ``connections=0 ok=0`` on the empty ``qbo_connections`` table.
- QBO test suite: **104/104 passing**
  (schema + pull + push + conflicts + webhook + orchestrator +
  unified financials + UI + routes + E2E mock integration).
- Schema drift: clean across every commit.
- Real Intuit sandbox: OAuth consent requires a browser session and
  could not run in this environment. Per the prompt's Option B, the
  full end-to-end flow is covered by
  ``tests/qbo/test_e2e_mock_integration.py`` which runs the exact
  step sequence documented in ``scripts/qbo_sandbox_e2e.py`` against
  a FakeQBO speaking real v3 payload shapes. When an operator
  completes OAuth, ``scripts/qbo_sandbox_e2e.py`` exercises the same
  assertions against the live sandbox.

## What's next (not in scope)

- Live sandbox run (operator-driven, OAuth-required).
- Payment-level pull (BillPayment / Payment endpoints).
- Attachments sync.
- QBO Desktop (different API surface entirely).
- Classes / Departments as first-class dimensions.
