# Formatting migration plan

## strftime audit — 50 call sites

### Category A (user-facing — migrate)

| File:line | Current | Fix |
|---|---|---|
| `src/engines/audit_engine.py:863` | `strftime("%Y-%m-%d")` in working-paper "Date" field | `format_date_short(..., lang)` |
| `scripts/review_dashboard.py:17045` | `last_run.strftime("%Y-%m-%d %H:%M")` cron status header | `format_date_short + format_time` (lang from user) |
| `scripts/review_dashboard.py:17910` | `generated_at.strftime("%Y-%m-%d %H:%M UTC")` report header | same |
| `scripts/daily_digest.py:271` | `date.today().strftime("%d %B %Y")` in FR body (uses SYSTEM LOCALE — bug) | `format_date(d, 'fr')` |
| `scripts/daily_digest.py:399` | `date.today().strftime("%d %B %Y")` | same |

### Category B (internal — skip)

- All `strftime("%Y%m%d_%H%M%S")` — filename/stamp generation (cas_engine, t2_engine, invoice_generator, update_otocpa, scripts/remote_management, scripts/autofix, scripts/bootstrap_install)
- `export_engine.py` (Sage/Acomba/IIF/Xero) — target format is dictated by the target accounting system, not locale
- `customs_engine.py`, `ocr_engine.py` — ISO dates as filters/query args (internal)
- `reconciliation_engine.py:599` — `%Y-%m-01` month anchor for DB filter
- `license_engine.py`, `generate_license.py`, `provision_client.py` — license file fields, fixed format by spec
- `folder_watcher.py`, `daily_digest.py:121,600`, `generate_demo_data.py` — internal file-naming

### Category C (already correct — HTML date input defaults)

- `review_dashboard.py:4412, 4504, 6215, 6419, 6422, 7254, 7255, 20594, 23485, 23592` — defaults for `<input type="date">` which the HTML spec mandates as `YYYY-MM-DD`.

### Category C (helper itself)

- `src/formatting/__init__.py:67` — lives inside `format_date_short`.

## Currency `$x,.2f` audit — 438 call sites

### Category A PDF engines (migrate; `lang` already in scope)

| File | Sites | Notes |
|---|---:|---|
| `src/engines/audit_engine.py` | 29 | Working papers, TB, P&L, BS, CF, SOCE, analytical review |
| `src/agents/core/invoice_generator.py` | 6 | GST/QST invoice |
| `src/engines/cas_engine.py` | 8 | Materiality, risk, sampling reports + PDFs |

### Category B (internal error messages, log-like strings)

- `src/agents/tools/bank_matcher.py` (12) — internal match-explanation strings stored in DB as audit/reasoning text
- `src/engines/fraud_engine.py` (19) — internal fraud detection reasoning
- `src/agents/core/hallucination_guard.py` (3) — validator error messages
- `src/engines/tax_engine.py` (19) — tax calc internals; user-visible output goes through PDF engines
- `src/agents/core/revenu_quebec.py` (1 relevant `f"${x:,.2f}"`) — part of RQ quick-method body text; bilingual already

### Category B (test/demo data generators — skip)

- `scripts/validate_demo_data.py` (57)
- `scripts/populate_all_modules.py` (5)
- `scripts/generate_canada_quebec_stress_test.py` (10)
- `scripts/generate_messy_images.py` (9)
- `scripts/analysis/gen_canadian_date_corpus.py` (5)
- `scripts/generate_test_data.py` (4)

### Category A dashboard (subset — queue row dates, report headers)

- `scripts/review_dashboard.py` (128 matches) — mix. The high-leverage subset is report/table renderings for CPAs and portal clients. For this pass, migrate only the dashboard's `last_run` + `generated_at` strftime spots (Category A list above) and audit 2-3 key PDF paths; the bulk remainder is documented as known gap in final report.

## Migration strategy

1. Add `money(amount, lang)` alias in `src/formatting/__init__.py` — short name that fits inline wrapping.
2. Per-file: import helper, replace `f"${x:,.2f}"` with `money(x, lang)`.
3. Commit each engine separately.
4. Regression tests per engine.
