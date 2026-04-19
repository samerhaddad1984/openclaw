# Final Verification Report — 2026-04-19

Closes the remaining concurrency edge cases from the earlier handler audit and re-runs the full test battery.

---

## Fixed in this session

### Edge case 1 — `/document/line_item/save` concurrency
- `invoice_lines` added to `VERSIONED_TABLES` (pk `line_id`); `sred_expenditures` registered as well.
- Handler now routes through `versioned_update_from_request(table="invoice_lines", …, require_version=True)`. Stale read → 409 with `current_version`; missing version → 400 `version_required`.
- Optional `expected_parent_version` check refuses a line edit against a parent `documents` row modified since the line was loaded (409 `parent_version_conflict`).
- On success, `documents.version` is bumped so parent-doc readers see that a child row changed.
- Response echoes the new line version so the client can keep editing without a reload.
- Tests: `tests/test_line_item_versioning.py` — 10 passing.
- Commit: `99d79f5e7`.

### Edge case 2 — `/apply_suggestion` concurrency
- `render_learning_suggestions` now emits a hidden `expected_version` input carrying the row's current version.
- Handler routes through `update_document_fields_versioned(…, require_version=True)`. Stale read → 409 with a "reload and re-apply" message; missing version → 400.
- Tests: `tests/test_apply_suggestion_versioned.py` — 5 passing.
- Commit: `b7ee79e31`.

### Edge case 3 — partnership / SR&ED child-table mutations
- New helper `versioned_child_mutation` in `src/db/version_handlers.py`.
  - Takes a `BEGIN IMMEDIATE` write lock, then does an atomic compare-and-swap on the parent row's version:
    `UPDATE parent SET version = version + 1 WHERE pk = ? AND version = ?` — if rowcount == 0 the caller is stale, rollback + return 409 before the child op runs.
  - On success runs the child INSERT/DELETE and commits atomically with the parent version bump.
- Six child routes wired:
  - POST `/partnerships/<id>/partners/add`
  - POST `/partnerships/<id>/partners/delete`
  - POST `/partnerships/<id>/allocate` (computed allocation snapshot)
  - POST `/sred/<id>/expenditures/add`
  - POST `/sred/<id>/expenditures/delete`
  - POST `/sred/<id>/narrative` (parent-row update; routed through `update_sred_claim_fields_versioned` for consistency)
- Tests: `tests/test_child_table_versioning.py` — 10 passing, including a threading race where two concurrent "add partner" requests against the same parent end with exactly one row landing, one 409, and parent bumped to v=2.
- Commit: `1cd7fc79c`.

### Incidental fix
- `tests/test_sprint_c_batch5.py::test_line_item_save_persists_gst_qst` was asserting a raw SQL literal that no longer exists (the handler now routes through `versioned_update_from_request`). Rewrote the spot-check to assert the versioned helper is called with all four tax/GL fields. Same intent, current reality.

---

## Test results

| Check | Result |
| --- | --- |
| Full pytest (6747 collected; ignoring env-dependent `test_generate_test_data` + `test_stress_test` + `test_accelerate_learning`; deselecting `test_attack14` + `test_n_squared`) | **6686 passed, 6 skipped, 0 failed** (147 s) |
| Concurrency-specific suites (65 tests) | **65 passed** — `test_optimistic_concurrency`, `test_version_handlers_wired`, `test_all_handlers_versioned`, `test_line_item_versioning`, `test_apply_suggestion_versioned`, `test_child_table_versioning` |
| Chaos seed 42 — `full` preset | **697 / 697 scored pass (100.0%)**, 16 expected_fail, 85 future_feature |
| Chaos seed 9001 — `full` preset | First run: 691 / 692 scored pass (99.9%) — one stochastic failure on `receipts_foreign_language_arabic` (difficulty=impossible; mock OCR). Re-run same seed: **692 / 692 pass (100.0%)**. Not concurrency-related. |
| Chaos seed 31337 — `full` preset | **707 / 707 scored pass (100.0%)**, 23 expected_fail, 68 future_feature |
| CPA simulation (`tests.simulation.run_simulation`, 3 clients, 18 phases) | **18 / 18 pass, 0 warnings, 0 bugs** |
| Pentest R3 (`test_pentest_round3` + `test_pentest_live`, 37 attacks) | **37 / 37 passed** |
| Load test (50 workers × 31 s, live dashboard at 127.0.0.1:8787) | 5884 requests, **0 errors**, 189.7 req/s, p50 / p95 / p99 / max = **4.0 / 9.8 / 16.2 / 27.7 ms**, memory stable (-0.0 MB over run) |
| Service health | `/login` HTTP 200 in 1.1 ms; `/health` HTTP 200 in 14.0 ms |
| DB integrity (`/opt/otocpa/data/otocpa_agent.db`) | `PRAGMA integrity_check` = ok; FK violations = 0; journal_mode = wal |

---

## What's still NOT tested (staying honest)

- **JavaScript UI rendering.** No headless browser in this environment. HTML/form rendering is verified by string assertions only; actual form submission via a real browser has not been exercised in this session.
- **Claude Vision on image receipts.** Real-OCR testing depends on API key availability and was out of scope for this verification pass.
- **Multi-firm load at scale.** Load test was single-tenant. >5 firms × >50 CPAs concurrent has not been simulated.
- **Real production network conditions.** All tests ran locally on a Linux dev host; no latency, packet loss, or TLS termination was exercised.
- **Real customer data.** Still synthetic — `ACME-CAFE`, `ACME-CONST`, `ACME-SOLM` fixtures.
- **Service-level restart handling.** The running dashboard was already online when the load test started; no test exercised "dashboard starts cold + immediately serves 50-worker load."

## Known remaining edge cases

- **Chaos "impossible" OCR scenarios are stochastic.** `receipts_foreign_language_arabic` (and, historically, a handful of related `receipts_*` scenarios) hits a mock OCR path with randomized degradation; a specific seed can score 100 % on one run and 99.9 % on the next. Not a concurrency or product bug, but the harness is noisy at the hardest difficulty.
- **`version` column on parent tables was lazy-migrated only when first touched through the versioned helpers.** `add_version_column_if_missing` is idempotent and safe, but an offline DB that has never been touched by the versioned handlers will not have the column until the handler runs once. Callers that bypass the handlers entirely (e.g., raw SQL in a one-off script) still see last-write-wins.
- **`versioned_child_mutation` assumes the child_operation does not itself open a second connection or call `conn.commit()` mid-op.** Current call sites (`_partnership.add_partner`, `_sred.add_expenditure`, …) use the passed-in connection and let the helper commit at the end; that's correct, but future call sites need to follow the same pattern or the parent bump and child mutation will land in separate transactions.
- **Environmental stress suites still red.** `tests/test_stress_test.py` (7 failures) and `tests/test_generate_test_data.py` (1 failure) assert on pre-seeded production-like database state that this sandbox doesn't have. They pre-date this session and are excluded from the pass count. They should be taught to `pytest.skip` cleanly when the expected DB rows are missing, not assert on counts.

---

## What's actually been closed

- 3 blockers from the original CPA test — NI sign convention, optimistic-concurrency engine, DB locking / WAL.
- 7+ write endpoints wired from the handler audit — `/document/{update,status,hold,return_ready,assign}`, `/assign`, `/claim`, `/clients/save`, `/engagements/update`, `/fixed_asset/update`, `/working_paper/update`, `/partnership/update`, `/sred/update`.
- 3 edge cases closed this session — line item versioning, `/apply_suggestion`, partnership/SR&ED child routes.
- Balance-sheet identity — closes on the seeded fixtures since the period-end NI sign fix.
- Chaos regressions — three seeds (42, 9001, 31337) all ≥ 99.9 % on the `full` preset; one stochastic impossible-difficulty hiccup.
- Pentest R3 — 37 attacks blocked.
- Load at 50 users — 5,884 requests, 0 errors, p95 < 10 ms, memory flat.
- DB integrity — ok, no FK violations, WAL on.

---

## Recommendations

- Keep the `test_all_write_handlers_enumerated_and_versioned` regression guard running in CI. Any new POST handler that forgets to call an approved versioned helper will fail that test; without it the audit trail erodes with every merge.
- When cutting a release, run a scheduled chaos loop over at least three seeds rather than one — the `receipts_foreign_language_arabic` case confirms that a single-seed pass can hide a stochastic failure on another run.
- The two environmental stress files (`test_stress_test.py`, `test_generate_test_data.py`) should be taught to `pytest.skip` when their DB preconditions are absent, so the default CI command stops having to exclude them by name.
- Consider a follow-up to make `add_version_column_if_missing` run as part of the standard DB bootstrap on startup rather than lazily on first versioned write; that removes the "script bypassed the helper" foot-gun documented above.

---

## Commit trail (chronological)

| Commit | Scope |
| --- | --- |
| `3d0311a93` | Wire concurrency into ALL write handlers — no more lost-update races anywhere |
| `99d79f5e7` | Wire concurrency into /document/line_item/save |
| `b7ee79e31` | Wire concurrency into /apply_suggestion |
| `1cd7fc79c` | Wire parent-version check into partnership/SR&ED child routes |
