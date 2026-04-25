# UI fixes: real HTTP evidence

## Service restart confirmed

```
$ systemctl restart otocpa
$ curl -sf http://127.0.0.1:8787/health
{"status": "ok", "uptime_hours": 0.0, "install_date": "2026-04-25T13:31:07.559720+00:00", ...}
```

## Fix 1: single language per portal page

Portal client `SSQ` (`language='fr'`) and `PTA_CLIENT1` (`language='en'`).
Same code, same routes — picked by client's stored language.

```
GET /c/<FR_TOK>/upload     → fr_upload.html
GET /c/<FR_TOK>/documents  → fr_documents.html
GET /c/<FR_TOK>/bank       → fr_bank.html
GET /c/<FR_TOK>/messages   → fr_messages.html
GET /c/<EN_TOK>/upload     → en_upload.html
GET /c/<EN_TOK>/documents  → en_documents.html
GET /c/<EN_TOK>/bank       → en_bank.html
GET /c/<EN_TOK>/messages   → en_messages.html
```

### FR pages — content present

| Page | FR strings present |
| --- | --- |
| upload | Envoyer un document, Glissez vos fichiers, Note (facultatif), Téléverser |
| documents | Mes documents, Fichier, Fournisseur, Statut |
| bank | Compte bancaire, Connectez votre banque, Connecter la banque |
| messages | Aucun message, Écrivez à votre CPA, Envoyer |

### EN pages — content present

| Page | EN strings present |
| --- | --- |
| upload | Upload a document, Drop files here, Note (optional) |
| documents | My documents, File, Vendor, Status |
| bank | Bank account, Connect your bank, Connect bank |
| messages | Write to your CPA, Send |

### Cross-check: no bilingual leaks

```
$ grep -oE 'Envoyer / Upload|Upload / Téléverser|...' fr_*.html  → empty
$ grep -oE 'Téléverser|Compte bancaire|Mes documents|...' en_*.html → empty
```

Every grep returned empty — neither FR pages contain EN twin phrases
nor EN pages contain FR twin phrases.

## Fix 2: document-level Category/GL removed from line-item docs

Verified via test suite (see `tests/doc_queue/test_no_redundant_doc_level_gl.py`):

```
$ python3 -m pytest tests/doc_queue/test_no_redundant_doc_level_gl.py -v
test_queue_shows_multiple_for_multi_line_docs ........... PASSED
test_queue_shows_single_category_for_single_line ........ PASSED
test_queue_falls_back_to_doc_level_when_no_lines ........ PASSED
test_document_detail_no_doc_level_category_input_when_lines  PASSED
test_document_detail_shows_lines_count_summary .......... PASSED
test_document_detail_single_line_keeps_doc_level_inputs . PASSED

6 passed
```

These tests use real `render_home` and `render_document` against
real sqlite DBs with seeded `documents` + `invoice_lines` rows, so
they prove the contract end-to-end at the renderer boundary.

## Test suite

```
tests/portal/                                         — 261 passed
  └─ test_single_language_per_page.py                 —  18 passed (new)
tests/doc_queue/test_no_redundant_doc_level_gl.py     —   6 passed (new)
tests/i18n/                                           — 128 passed (no regressions)

Total: 267 passed across portal + doc_queue
```
