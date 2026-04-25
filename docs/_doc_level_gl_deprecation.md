# Document-level Category/GL: deprecation note

## What changed (2026-04-25)

The user-facing UI no longer treats `documents.category` and
`documents.gl_account` as authoritative for documents that have line items.

- Queue list: when `documents.has_line_items = 1`, the Category and GL
  columns now show the line-derived value (single value when all lines
  share one, "Multiple" / "Plusieurs" when 2+).
- Document detail summary: doc-level Category cell shows the same
  derived value; doc-level GL cell is replaced by a "Defined by line
  items" label, and a banner shows "N lines across M GL accounts".
- Document edit form: doc-level Category, GL Account, and Tax Code
  inputs are hidden when `has_line_items = 1`.

## DB columns retained (not dropped)

`documents.category`, `documents.gl_account`, and `documents.tax_code`
are still present and still populated by the OCR pipeline (initial
single-value extraction). They are kept because:

- Existing rows would lose their data on a schema migration.
- Many backend readers depend on them: `src/engines/audit_engine.py`,
  `src/engines/tax_engine.py`, `src/engines/accrual_engine.py`,
  `src/engines/concurrency_engine.py`, `src/engines/export_engine.py`,
  `scripts/export_ready_documents.py`, `scripts/run_stress_test.py`.

## Future migration path

For multi-line documents the doc-level value can drift from the line
items' truth. When a backend report needs to be exact, it should:

1. Detect `documents.has_line_items = 1`.
2. JOIN/aggregate from `invoice_lines` (gl_account, category,
   line_total_pretax) instead of `documents`.
3. Fall back to the doc-level column only when `has_line_items = 0`.

Audit engine and tax engine are the highest-risk consumers because they
drive financial reports. They are out of scope for this UI fix and
should be migrated in a follow-up PR with their own test coverage.

## What stayed

- The OCR pipeline still writes a doc-level Category/GL on first ingest.
  This is the right default for single-line documents.
- The `documents.gl_account LIKE` filter in the queue search box still
  matches doc-level values; once the migration above lands, that filter
  should also fan out to line-level matches.
