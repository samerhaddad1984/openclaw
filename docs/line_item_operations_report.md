# Line Item Operations — Split / Merge / Allocate

Post-OCR corrections for CPA review: split one line into many, merge
many lines into one, allocate a single line across several GL
accounts. All three operations are versioned, idempotent, and leave a
full audit trail.

## Scope delivered

### Phase 5 — UI modals + HTTP routes

- Backend module `src/engines/line_item_operations.py` with
  `split_line`, `merge_lines`, `allocate_line`, plus
  `get_active_lines`, `get_audit_trail`, `has_cpa_modifications`
  query helpers.
- Additive schema migration on `invoice_lines`:
  - `modification_type` (null / `split` / `merged` / `allocated`)
  - `parent_line_ids` (JSON array of source line_ids)
  - `deleted_at` (soft-delete timestamp)
- New `invoice_line_audit` table with one row per operation:
  `document_id`, `operation`, `performed_by`, `performed_at`,
  `reason`, `before_json`, `after_json`, `source_line_ids`,
  `result_line_ids`, `client_request_id` (unique per `document_id`
  for idempotent replays).
- HTTP routes wired into `scripts/review_dashboard.py`:
  - `POST /document/<doc_id>/line/<line_id>/split`
  - `POST /document/<doc_id>/lines/merge`
  - `POST /document/<doc_id>/line/<line_id>/allocate`
  - All require edit permission on the document (routed through
    `_require_document_in_firm`), enforce period-locks, and
    honour `expected_version` on the source line(s) + parent
    document. Stale reads return 409 with
    `reload_required: true`.
- UI changes in `render_line_items_card`:
  - Checkbox column on every row for merge selection.
  - `Split` / `Allocate` buttons on every row.
  - `Merge selected` toolbar that appears once ≥ 2 lines are
    ticked.
  - Single modal reused for split/merge/allocate; live sum vs
    target indicator turns green when the sum matches the
    original pretax (allocate percentage mode targets 100).
  - CSS class `.li-mod-badge` with per-type accent
    (`.li-mod-split`, `.li-mod-merged`, `.li-mod-allocated`) on
    every CPA-modified row.
- Bilingual FR/EN translation keys for all modal labels, button
  text, error strings, toast messages, and audit-trail headings
  added to `src/i18n/fr.json` and `src/i18n/en.json`.

### Phase 6 — Audit trail view

- `render_line_history_card(document_id, lang)` renders a
  collapsible card showing every operation newest-first, with
  before / after line snapshots, performed_by, timestamp, and
  reason.
- Hidden entirely when `has_cpa_modifications` returns false.
- Nested `<details>` toggle for *Show original OCR extraction*.

### Phase 7 — Integration tests

`tests/integration/test_line_item_workflow.py`:
- `test_ocr_to_split_to_qbo_ready_lines` — OCR → split leaves two
  active lines that sum to the original, with distinct GL accounts.
- `test_merge_then_audit_preserves_original` — the before snapshot
  retains the two original descriptions.
- `test_split_then_merge_roundtrip` — split followed by merge
  restores the total, and the audit trail carries both operations.
- `test_export_respects_modifications` —
  `export_engine.fetch_posted_document_lines` skips soft-deleted
  sources and returns the CPA's split lines instead.
- `test_concurrent_splits_one_wins` — two threads racing the same
  line produce exactly one winner and one loser (either
  `OptimisticConcurrencyError` or `LineItemOperationError(
  line_already_deleted)` depending on which check fires first).

### Phase 8 — Training + report

- `docs/training/cpa_owner_guide_fr.md` — new section *Corriger les
  lignes d'un document* explaining split / merge / allocate, where
  the buttons live, and how the audit trail works.
- `docs/training/cpa_owner_guide_en.md` — same content in English
  (*Correcting document lines*).
- This report.

## Downstream systems updated

Both the QBO push and the multi-format export pipeline now filter
out soft-deleted lines, so pushing a split bill to QuickBooks or
exporting a CSV reflects the CPA's final line layout — never the
raw OCR extraction.

- `src/integrations/qbo_push.py::_fetch_invoice_lines` filters
  `deleted_at IS NULL OR deleted_at = ''` (with a fallback to the
  unfiltered query on pre-migration DBs).
- `src/engines/export_engine.py::fetch_posted_document_lines` does
  the same, with the same fallback.

## Tests added

```
tests/ui/test_line_item_ui.py                               20 tests
tests/integration/test_line_item_workflow.py                 5 tests
```

All 25 pass locally:

```
python3 -m pytest tests/ui/test_line_item_ui.py \
                  tests/integration/test_line_item_workflow.py -q
=========================== 25 passed in 1.72s ===========================
```

## Evidence

- Split modal renders FR and EN: `test_split_modal_renders_fr`,
  `test_split_modal_renders_en`.
- Sum-mismatch rejected: `test_split_modal_validation_sum_mismatch`
  and `test_allocate_rejects_percentage_sum_not_100`.
- Allocate amount and percentage modes both round to the cent:
  `test_allocate_modal_amount_mode`, `test_allocate_modal_percentage_mode`.
- CPA-modified badge renders on active rows:
  `test_cpa_modified_badge_shown`.
- Audit trail hidden when empty, visible + bilingual otherwise:
  `test_audit_trail_hidden_when_no_modifications`,
  `test_audit_trail_bilingual`.
- Concurrent splits: exactly one winner:
  `test_concurrent_splits_one_wins`.
- Idempotent replay: `test_client_request_id_makes_split_idempotent`.

## Known limitations

- **No cross-document operations.** Split / merge / allocate only
  work within a single document (bill). Moving line items between
  documents is out of scope — a different invariant (document
  totals) applies.
- **Allocation requires exact sum.** Rounding is absorbed by the
  final allocation so the sum matches the original pretax to the
  cent. No "round-to-nearest-cent forgiveness" loop; a user
  submitting amounts that sum to $99.99 against a $100.00 original
  gets a `sum_mismatch` error.
- **Multi-leg JE splits not built.** Journal entries can also carry
  per-leg detail, but the posting-builder path for JEs is a
  separate module (`src/agents/tools/posting_builder.py`) with its
  own concurrency model. A follow-up ticket can mirror this work
  there; out of scope for the current line-items effort.
- **Tax amounts on merge are summed, not re-derived.** If the
  merged sources have inconsistent per-line tax treatments (e.g.
  one has GST, another doesn't), the merged line keeps the sum
  rather than re-calculating. Use allocate + re-split if the CPA
  needs to recompute tax.
- **Idempotency uses a single key column.** Two different clients
  submitting the same `client_request_id` on the same document
  would collide. In practice the UI generates a fresh id per
  click; the API contract documents that the key must be unique
  per caller.

## Files touched

```
src/engines/line_item_operations.py             NEW
scripts/review_dashboard.py                     UPDATED  (render + routes)
src/engines/export_engine.py                    UPDATED  (deleted_at filter)
src/integrations/qbo_push.py                    UPDATED  (deleted_at filter)
src/i18n/fr.json                                UPDATED  (+35 keys)
src/i18n/en.json                                UPDATED  (+35 keys)
tests/ui/__init__.py                            NEW
tests/ui/test_line_item_ui.py                   NEW
tests/integration/__init__.py                   NEW
tests/integration/test_line_item_workflow.py    NEW
docs/training/cpa_owner_guide_fr.md             UPDATED
docs/training/cpa_owner_guide_en.md             UPDATED
docs/line_item_operations_report.md             NEW
```
