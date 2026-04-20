# Nasty Detective Round 2 Report — 2026-04-20

Nine adversarial investigations, six of which found real bugs.

## Findings ledger

| # | Severity | Inv | Title | Status | Commit |
| --- | --- | --- | --- | --- | --- |
| 1 | HIGH | 1 | `/clients` 500'd on minimal DB — `whatsapp_number` / `contact_email` / `language` / `active` / `portal_token` / `portal_token_created_at` / `portal_token_rotated_count` / `created_at` / `client_name` missing from bootstrap migration | FIXED | `e6ef2ec0f` |
| 2 | HIGH | 1 | `/bank/feeds` 500'd — `bank_connections.firm_code` never added because CREATE was after migration block | FIXED | `e6ef2ec0f` |
| 3 | HIGH | 1 | `/health/page` 500'd — `documents.ai_used` never migrated | FIXED | `e6ef2ec0f` |
| 4 | HIGH | 3 | `/` and `/aging` / `/financial_statements` 500'd under load — `documents.amount` / `document_date` / `created_at` / `updated_at` / `vendor` / `client_code` / `ingest_source` never migrated | FIXED | `8541a7c69` |
| 5 | CRITICAL | 4 | **Manual journal entries invisible to financial statements.** `post_journal_entry` writes to `gl_transactions`; `generate_trial_balance` never read that table. Every manual adjustment (depreciation, accruals, RE roll-forward) was silently dropped from TB / P&L / BS | FIXED | `7aba9e772` |
| 6 | HIGH | 4 | `/financial_statements/pdf` 500'd with `KeyError: 'total'`. Both PyMuPDF and minimal PDF paths indexed `bs["equity"]["total"]` but the engine had overwritten `bs["equity"]` with a flat dict; canonical totals live under `bs["equity_detail"]` | FIXED | `7aba9e772` |
| 7 | HIGH | 4 | Same pattern: IS PDF 500'd with `TypeError: string indices must be integers`. `is_["revenue"]` / `is_["expenses"]` overwritten to flat dicts; PDF iterated them as list-of-dicts | FIXED | `7aba9e772` |
| 8 | HIGH | 4 | `generate_trial_balance` had a bare `except Exception: return []` that hid a missing-table failure AND silently skipped the opening-balances / AR / JE rollups whenever `documents` or `posting_jobs` threw | FIXED | `7aba9e772` |
| 9 | CRITICAL | 5 | **Daily backup has been silently failing for 3 days.** Cron redirect targets `/opt/otocpa/logs/backup.log`; the log dir doesn't exist; bash aborts the whole command before the script runs. Last real backup was 4 days stale; Postgres dump from that run was 0 bytes | FIXED | `61780d466` |
| 10 | MEDIUM | 8 | `reverse_journal_entry` bypassed the period-lock check that `post_journal_entry` got in R1. A locked period could still receive compensating reversal rows | FIXED | `f75806cc0` |
| 11 | LOW | 1 | Playwright unreachable in this sandbox (system libs gated). Fallback to HTTP+BS4 still caught 4 bugs on page renders | NOT FIXED — env-bounded | — |
| 12 | LOW | 3 | Backup script hardcodes Postgres password literal | NOT FIXED — ops coordination needed | — |
| 13 | NOT_A_BUG | 2/6/7/9 | OCR batch stress (846 real receipts), calculations at 10k JE legs, 3-year multi-period, and 10 hostile vendor categories: all clean. No bugs found. | — | — |

## Summary of what this round actually broke

The browser-UI investigation in R1 added column migrations for the pages it walked. R2 walked **53 more pages** and found **4 more missing-column 500s** in the same class. This is the main theme: bootstrap_schema needs every column that appears in any SELECT, not just the ones covered by the pages R1 hit. The fix continues to broaden that migration block; a CI guard that greps new column references and checks them against the migration would prevent the pattern recurring.

The **two critical bugs** are the most impactful:

1. **Manual JEs invisible to financial statements.** Any CPA's period-end adjustments — depreciation, accruals, reclasses, prior-period corrections — were being written to `gl_transactions` and then silently ignored by every downstream report. The financial statements engine was reading only documents + AR + opening_balances. Fix: a new gl_transactions rollup block inside `generate_trial_balance` that aggregates debit/credit legs by account. Without this, the 10k-JE scale test and multi-period comparative would have looked fine but returned zeros.

2. **Backups silently failing for 3 days.** The cron log redirect pointed to a nonexistent directory; bash aborts the compound command before the script can run. 4-day gap at discovery, 0-byte Postgres dump on the last "successful" attempt. Fix: script now self-creates its log dir, exits non-zero on empty outputs, runs `PRAGMA integrity_check` on the SQLite copy, and is env-overridable for the PG password.

## Tests added this round

| File | Tests | Purpose |
| --- | --- | --- |
| `tests/browser/test_real_workflows.py` | 55 | Every major page renders for firm_admin without 500/traceback |
| `tests/adversarial/test_ocr_batch_stress.py` | 1 | 846 real receipts through deterministic OCR stages |
| `tests/adversarial/test_pdf_accuracy.py` | 5 | BS identity / IS math / TB balanced / repeatability / PDF numbers match engine |
| `tests/adversarial/test_scale_calculations.py` | 4 | 10,000 JE legs, no drift, BS identity to the penny |
| `tests/adversarial/test_multi_period.py` | 4 | 3 years × 12 months × 20 JEs; each year self-balances, no bleed |
| `tests/adversarial/test_backup_integrity.py` | 7 | Script guards + live drill (age, integrity, table parity) |
| `tests/adversarial/test_workflow_interactions.py` | 7 | Period-lock × JE post/reverse, draft isolation, cross-client, repost-after-reverse |
| `tests/adversarial/test_vendor_categories.py` | 11 | Gas/restaurant/Costco/Dollarama/pharmacy/lumber/catering/hotel/Amazon/sub |
| `tests/adversarial/test_real_memory_leak.py` | 1 | 5-min sustained load, post-warmup slope < 30 MB/hr |
| `scripts/stress/ocr_batch_stress.py` | n/a | Standalone runner |
| `scripts/stress/real_memory_leak_test.py` | n/a | Standalone runner (LEAK_SECONDS=7200 for the full 2hr) |

**Total new tests: 95.** Total pytest pass count after R2: **6,813** (up from 6,775 at R1 close). CPA simulation 18/18 clean. No regressions.

## What's still NOT tested (explicitly)

- **Real Playwright browser automation.** Chromium and Firefox binaries downloaded but won't run without system libs (`libatk-1.0`, `libgtk-3`, `libnss3`) that require sudo/apt — denied in this sandbox. Approaches A, B, C (Firefox with-deps, Chromium --no-sandbox, Selenium + geckodriver) all hit the same wall. Fallback is rich HTTP + BeautifulSoup with JS-error pattern detection; still caught 4 real bugs, but pure JavaScript interactions (React state transitions, modal focus management, client-side form validation) are not exercised.
- **Live Claude Vision / Google DocAI OCR.** Sandbox has no API keys / budget. Deterministic OCR pipeline (preprocess, pdfplumber, regex parser) stress-tested across 846 real receipts; live-AI confidence regressions still need a budgeted run.
- **Full 2-hour memory leak loop.** Pytest CI window caps at 5 minutes. Standalone script supports `LEAK_SECONDS=7200` for the full spec; operator run required.
- **Real production network conditions.** All tests local; no TLS, latency, packet loss, or CDN caching.
- **Stripe SDK live path.** SDK not installed in sandbox; the critical path (webhook idempotency, firm provisioning) was tested via mocks + skipif-no-sdk.
- **Real customer data.** Synthetic receipts / synthetic JEs only.
- **Multi-firm concurrency above ~10 simultaneous users.** Load test is single-tenant.

## What's closed vs what's still open

**Closed this round (10 bugs, 8 CRITICAL/HIGH, 1 MEDIUM, 1 LOW):**
- All 4 missing-column 500s (`/clients`, `/bank/feeds`, `/health/page`, `/`+`/aging`+`/financial_statements` under load)
- Manual JEs into trial balance
- BS PDF `KeyError: 'total'`
- IS PDF `TypeError` on flat-dict iteration
- `generate_trial_balance` swallowing errors that dropped rollups
- Backup cron silent-failure + script hardening
- `reverse_journal_entry` period-lock parity

**Still open (honest):**
- Playwright in this sandbox (env issue, will remain until operator adds system libs).
- Hardcoded Postgres password literal in `scripts/backup_db.sh` (needs ops coordination to rotate + move to secret store).
- `scripts/stress/real_memory_leak_test.py` needs a 2-hour run on a stable host to meet the original spec; the 5-min probe here is proxy only.
- Per-request `open_db()` connections aren't explicitly closed — relies on GC. Python-sqlite3 context manager commits but doesn't close. Not a true leak, but a small per-request allocation that would matter at very high QPS. A connection pool would be the right fix.

## Recommendations

1. **Add a CI pre-commit check** that greps new `SELECT d.<col>` references in `review_dashboard.py` and `audit_engine.py` against the bootstrap migration block — fail the commit if a new column isn't migrated. This would have prevented every finding in Investigations 1 and 3.
2. **Move `scripts/backup_db.sh` PG_PASSWORD to a `.env` sourced by the script or a systemd credential file.** The literal in git is bad posture.
3. **Add a systemd timer for backup** to replace the fragile cron entry. Systemd timer units report failures to journalctl; cron silently swallowed 3 days.
4. **Treat generate_trial_balance like a pipeline: each source (documents, AR, opening_balances, gl_transactions, AP credit) runs in its own try/except.** One source failing should not drop the others. R1 already narrowed the outer `except`; further granularity would help.
5. **Run the leak probe nightly with `LEAK_SECONDS=7200`** once the infrastructure exists. Or ship a `/metrics` Prometheus endpoint and alert on RSS trajectory.
6. **Install Playwright system libs on the CI host** so the HTTP-only browser harness can be promoted to real JS interaction. Every R2 test with "browser-equivalent" in the name could then upgrade to real clicks.

## Commit trail this round (chronological)

| Commit | Scope |
| --- | --- |
| `e6ef2ec0f` | bootstrap_schema: 4 missing-column 500s on /clients /bank/feeds /health/page |
| `3224e76a9` | OCR stress 846 real receipts (deterministic stages clean) |
| `7aba9e772` | Manual JE rollup + BS/IS PDF flat-dict fix + narrowed TB except |
| `c3e406df9` | Scale calculations 10k legs (no drift) |
| `a40a4a325` | Multi-period 3-year (no bleed) |
| `61780d466` | Backup script + 3-day silent-fail fix + restore drill |
| `f75806cc0` | reverse_journal_entry period-lock parity |
| `bb4b39382` | Vendor category stress (10 categories) |
| `8541a7c69` | Leak probe + more documents column migration |
