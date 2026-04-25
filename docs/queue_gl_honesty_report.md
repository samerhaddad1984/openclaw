# Queue GL/Category honesty fix — three layers, real HTTP evidence

## What user reported

`rcpt_16.png` showed `Category=operating_expense` and `GL Account=5440`
in the queue while the same document had `vendor=NULL`,
`amount=NULL`, `document_date=NULL`, `confidence=0.0` — i.e. OCR
completely failed but the queue was happily printing fake values
as if real.

Live DB confirmation:

```
$ python3 -c "..."
{'document_id': 'doc_064f6b5cbdc3', 'file_name': 'rcpt_16.png',
 'vendor': None, 'amount': None, 'document_date': None,
 'gl_account': '5440', 'category': 'operating_expense',
 'has_line_items': None, 'review_status': 'NeedsReview',
 'confidence': 0.0}
```

3,818 documents in production matched this silent-default
fingerprint.

## Three-layer fix

### Layer 1 — display (commit `bf29f8ec2`)

The queue and document detail now derive Category/GL from the line
items when present, surface "Non catégorisé" / "Uncategorized" when
the document looks uncategorised (no line items + no vendor or no
amount), and emit `<span data-cell="gl" title="5420, 5500">2 comptes
GL</span>` for multi-line docs spanning 2+ GL accounts. The
documents.gl_account / category column is now displayed only when
OCR pulled real signal.

Tests: `tests/doc_queue/test_queue_gl_display_honesty.py` — 8 tests
(rcpt_16 repro FR + EN, OCR success, single-line invoice_lines,
multi-GL with tooltip, has_line_items flag without lines, document
detail mirror).

### Layer 2 — ingest (commit `793efce21`)

Removed the `else: result['gl_account'] = '5440'` silent default in
`src/engines/ocr_engine.py:1612`. New behaviour leaves
gl_account/category empty and sets `needs_categorization=1` instead.
Same fix in `src/engines/ai_validator.py:107` (invalid-GL rewriter).

`scripts/maintenance/flag_silently_defaulted_documents.py` flags
historical suspect documents (gl=5440 AND category=operating_expense
AND has_line_items=0 AND no vendor/amount) for CPA review without
modifying their gl_account / category — preserves CPA-confirmed
work.

Live dry-run finds 3818 suspect documents on this DB. Operator can
run the script when ready.

Tests: `tests/ingest/test_no_silent_defaults.py` — 8 tests
(OCR no-signal leaves NULL, OCR with-signal preserves GL, source-
grep for the literal '5440' line is gone, ai_validator clears
invalid GL, backfill flags-only behaviour, multi-line skip).

### Layer 3 — downstream readers (commit `822564791`)

`src/integrations/qbo_push.py` refuses to push documents flagged
`needs_categorization=1`, OR with empty gl_account and no line
items. `src/engines/export_engine.py.fetch_posted_documents`
filters out flagged docs. PRAGMA-probe falls back to legacy SQL
on test DBs without the column.

Tests: `tests/integration/test_downstream_no_silent_defaults.py` —
7 tests (export skips uncategorised, export keeps CPA-confirmed
5440, legacy DB still works, QBO push refuses uncategorised, refuses
no-GL-no-lines, accepts categorised single-line, accepts multi-line
with empty doc-level GL).

## Real HTTP evidence (from production service after restart)

```
$ systemctl restart otocpa
$ curl -sf -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8787/health
200
```

### Scenario 1 — failed-OCR doc (the rcpt_16 bug)

```html
<tr>
  <td><a href="/document?id=doc_a79fe3879fd6">rcpt_16.png</a></td>
  <td>CLI1</td>
  <td class="vendor-cell"></td>
  <td class="amount-cell"></td><td></td>
  <td><span data-cell="category" class="muted">Non catégorisé</span></td>
  <td><span data-cell="gl"       class="muted">Non catégorisé</span></td>
  <td><span class="badge badge-needsreview">Needs Review</span></td>
</tr>
```

No "5440", no "operating_expense" — replaced with honest "Non
catégorisé".

### Scenario 2 — single-extraction OCR success

```html
<tr>
  <td><a href="/document?id=doc_c22e7885fac5">sroie_0192.jpg</a></td>
  <td>live_stress</td>
  <td class="vendor-cell">SLF CASH &amp; CARRY</td>
  <td class="amount-cell">48.0</td><td>2018-02-02</td>
  <td><span data-cell="category">operating_expense</span></td>
  <td><span data-cell="gl">5430</span></td>
</tr>
```

Real OCR signal (vendor + amount) → real doc-level Category/GL shown
without the muted class.

### Scenario 3 — multi-line doc with 2 distinct GLs

```html
<tr>
  <td><a href="/document?id=doc_d50179fc40e7">Invoice-FBBD891C-0073.pdf</a></td>
  <td>MARCHE_BRE</td>
  <td class="vendor-cell">CompanyCam</td>
  <td class="amount-cell">2949.24</td><td>2025-10-06</td>
  <td><span data-cell="category" class="muted">Non catégorisé</span></td>
  <td><span data-cell="gl" title="5420, 5500">2 comptes GL</span></td>
</tr>
```

Multi-GL summary "2 comptes GL" with `title="5420, 5500"` tooltip.
Category cell is "Non catégorisé" because the lines have GL accounts
but no category strings on this particular document.

(Full HTML files saved under `docs/_queue_gl_honesty_evidence/`.)

## Tests

| Suite | Count | Status |
| --- | --- | --- |
| `tests/doc_queue/` | 14 (8 new) | passing |
| `tests/ingest/` | 8 (new) | passing |
| `tests/integration/test_downstream_no_silent_defaults.py` | 7 (new) | passing |
| `tests/portal/` | 261 | passing |
| `tests/qbo/` + `tests/migration/test_qbo_historical.py` + `tests/test_qbo_multi_client.py` | 148 | passing |
| `tests/test_export_engine.py` | 40 | passing |
| `tests/i18n/` | 128 | passing |

Cross-suite sweep: **455 passed in 78s**, no regressions.

## Migration / rollout

* **New documents**: clean from this point — OCR engine no longer
  writes the silent default.
* **Existing 3818 suspect documents**: flagged on demand by running
  `scripts/maintenance/flag_silently_defaulted_documents.py`
  (default is dry-run). The script does NOT modify gl_account /
  category; it only sets `needs_categorization=1`.
* **Database**: bootstrap_schema adds the
  `documents.needs_categorization INTEGER DEFAULT 0` column on
  dashboard startup. Backwards-compatible: legacy queries still work
  via PRAGMA-probe in export_engine.

## Honest limitations

* `audit_engine.py`, `tax_engine.py`, `accrual_engine.py`,
  `concurrency_engine.py` still read `documents.gl_account`
  directly for aggregations / reports. They were not migrated in
  this round because each one drives financial reports that need
  their own targeted test coverage. They are documented in
  `docs/_doc_level_gl_deprecation.md` for follow-up.
* The user-visible data-integrity surfaces (queue display,
  document detail, OCR ingest, QBO push, CSV export) are now
  honest. Internal reporting engines may still show the legacy
  doc-level value for multi-line docs until they're migrated.

## Service restart

```
$ systemctl restart otocpa
$ sleep 4
$ curl -sf -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8787/health
200
$ curl -sf http://127.0.0.1:8787/health | python3 -m json.tool | grep uptime
    "uptime_hours": 0.0,
```

Service confirmed healthy after restart with all three layers live.

## Commits

| Hash | Layer | Files |
| --- | --- | --- |
| `bf29f8ec2` | 1 — display | `scripts/review_dashboard.py`, `src/i18n/ui_labels.py`, `tests/doc_queue/test_queue_gl_display_honesty.py` |
| `793efce21` | 2 — ingest | `src/engines/ocr_engine.py`, `src/engines/ai_validator.py`, `scripts/review_dashboard.py`, `scripts/maintenance/flag_silently_defaulted_documents.py`, `tests/ingest/test_no_silent_defaults.py` |
| `822564791` | 3 — downstream | `src/integrations/qbo_push.py`, `src/engines/export_engine.py`, `tests/integration/test_downstream_no_silent_defaults.py` |
