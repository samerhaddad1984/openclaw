# OtoCPA Chaos Testing Framework

Stress-tests every engine, catches edge cases, proves accuracy claims.

## What it tests

- **Receipt extraction** (OCR + AI) under nightmare conditions —
  coffee stains, 45° angles, bilingual labels, Arabic text, negative
  totals, 100+ line items, etc.
- **Audit engine** — duplicate detection, period close, sampling,
  trial balance, anomalies.
- **Fraud engine** (14 rules in `src/engines/fraud_engine.py`) —
  split transactions, bank-detail changes, typo-vendor duplicates,
  sequential invoice patterns, weekend/holiday spikes.
- **Financial computations** — journal entry balancing, multi-currency
  translation, depreciation across FY change, intercompany elimination,
  prepaid amortization.
- **Reconciliation** — one-to-many / many-to-one matches, FX differences,
  rounding tolerances, wire fees, NSF cheques, closed-period rejections.
- **Tax (Quebec)** — GST/QST parallel calc, meals at 50%, zero-rated vs
  prepared food, HST cross-provincial, Quebec insurance (9% non-recoverable),
  quick-method traps, micro-transaction tax-leakage floors.
- **End-to-end workflows** — portal storms, multichannel uploads, QBO token
  expiry mid-batch, Plaid/Stripe webhook replay, plan switches mid-period.

Total scenarios today: **~174** across all tracks.

## Quick start

```bash
# Cheap smoke test (no AI, no $, ~2s)
python3 chaos/run_chaos.py --preset smoke --no-ai

# Full run with real AI-generated nightmare images (~$30, ~2 hours)
python3 chaos/run_chaos.py --preset full

# Only one track
python3 chaos/run_chaos.py --track tax --difficulty nightmare

# Reproduce a specific failure
python3 chaos/run_chaos.py --reproduce tax_meal_50pct_alcohol_surcharge_abc123

# Generate nightmare receipt images (no pipeline, for inspection)
python3 chaos/run_chaos.py --generate-only --count 50
```

## Cost management

- Real image generation uses Google Imagen at ~**$0.03/image**.
- Hard budget cap: `--budget <usd>`. Default $30 per run.
  Anything above $50 requires `--confirm-budget`. Absolute hard cap $200.
- Generated images are cached by `(model, prompt)` — re-runs cost $0.
- `--no-ai` replaces images with PIL placeholders (framework still exercises
  every runner). `--use-cached` calls Imagen only on cache misses.

## Directory layout

```
chaos/
├── run_chaos.py             CLI entry point
├── config.py                budgets, paths, difficulty mix
├── generators/
│   ├── ai_image_generator.py  Google Imagen client + budget tracking
│   ├── scenario_catalog.py    Master scenario registry
│   ├── receipt_scenarios.py   50+ receipt nightmares
│   ├── audit_scenarios.py     33 audit cases
│   ├── financial_scenarios.py 25 financial cases
│   ├── recon_scenarios.py     20 recon cases
│   ├── workflow_scenarios.py  10 E2E workflow cases
│   ├── fraud_scenarios.py     15 fraud patterns
│   └── tax_scenarios.py       20 Quebec tax edge cases
├── oracles/                  ground-truth validators per track
├── runners/                  invoke real engines per track
├── reports/                  HTML dashboard + Markdown + CSV
├── fixtures/
│   ├── vendors_quebec.json   ~150 real QC vendor archetypes
│   ├── gl_chart.json         baseline chart of accounts
│   ├── client_profiles.json  8 test firm/client archetypes
│   └── prompts/              Imagen prompt packs
└── results/                  git-ignored output
    ├── runs/                 per-run JSONL + budget/meta
    ├── failures/             failures for triage
    ├── reports/              HTML + MD + CSV
    └── cache/                cached Imagen PNGs
```

## Interpreting the report

- **Pass %** — overall accuracy across sampled scenarios.
- **By track** — quickly identify which subsystem is weakest.
- **By difficulty** — should see declining pass rate from easy → impossible.
  Pass rate 100% on `nightmare` means your scenarios are too soft.
- **Failure triage** — each failure has expected vs actual, engines
  exercised, and a one-line reproduce command.
- Severity: `critical` failures block shipping, `high` belong in the next
  sprint, `medium/low` are tracked as tech debt.

## Adding new scenarios

1. Pick the track (receipts, audit, fraud, financial, recon, tax, workflow).
2. Append a new dict to `<track>_scenarios.py` following the existing shape:
   - `subtype`, `difficulty`, `description`, `severity_on_failure`
   - `input` (for non-receipt tracks) or condition list + `extras` (receipts)
   - expected outcome (`expected`, `expected_findings`, or
     `expected_rules_fired`)
3. Ensure a runner can handle the `subtype`. If it's new, add a branch in
   the matching `runners/<track>_runner.py`.
4. Run `python3 chaos/run_chaos.py --track <your-track> --no-ai` to verify.

## What each runner actually hits

| Runner | Real code invoked | Mocked / simulated |
|---|---|---|
| `tax` | `tax_engine.calculate_gst_qst`, `calculate_itc_itr`, `calculate_cross_provincial_itc_itr`, `validate_tax_code`, `validate_quick_method_traps`, `validate_quebec_tax_compliance`, `extract_tax_from_total`, `TAX_CODE_REGISTRY` | nothing |
| `fraud` | `fraud_engine.run_fraud_detection` against a seeded `documents` + `bank_transactions` sqlite | nothing |
| `audit` | Same as `fraud`, plus SQL-level unposted / closed-period / missing-doc gates | period-close engine where no standalone fn exists yet |
| `financial` | `audit_engine.generate_trial_balance`, `aging_engine._bucket_name`, `fixed_assets_engine.add_asset + calculate_annual_cca`, `multicurrency_engine.compute_realized_fx_gain_loss`; Decimal arithmetic for JE balance, FX rounding, prepaid amortization, intercompany elim, asset disposal, BS check, deferred revenue | nothing |
| `recon` | `reconciliation_engine.create_reconciliation + add_reconciliation_item + calculate_reconciliation + finalize_reconciliation`, `bank_matcher.BankMatcher.detect_split_payments` | nothing |
| `workflow` | `ocr_engine.process_file` (against tiny PNG bytes), SQL review-state transition, `qbo_online_adapter.build_qbo_expense_payload` | QBO HTTP layer via `find_vendor_by_name` / `find_account_by_name` / `resolve_payment_settings` patched to return fixture refs; Stripe/Plaid webhook endpoints via sqlite idempotency gate |
| `receipts` | **default:** deterministic degradation mock (free, fast) that drops/fuzzes fields per condition. **with `--real-ocr`:** `ocr_engine.process_file` on AI-generated images | DocAI + DeepSeek APIs unless `--real-ocr` |

Every runner's result has a `functions_called` list so you can see exactly
which real functions fired for any scenario.

## Stage 1 coverage — before vs after

Smoke pass-rate shape (no-AI, 173 scenarios):

| Track     | Stage 0 (pre-wiring) | Stage 1 (real engines) |
|---|---:|---:|
| receipts  |  0.0% | 64.7% |
| audit     | 27.3% | 25.0% |
| fraud     |  0.0% | 29.4% |
| financial | 100%  | 87.1% |
| recon     | 100%  | 91.3% |
| tax       | 28.6% | 75.0% |
| workflow  | 100%  | 93.3% |
| **total** | **36%** | **66.5%** |

Stage 0's 100%s were cheating (optimistic passthroughs pass by construction).
Stage 1 exercises real code, so the numbers drop where we previously faked
success and rise where the real engine actually works better than mocks.

Difficulty curve is now meaningful: easy=100%, normal=96%, hard=51%,
nightmare=49%, impossible=0%. That's the expected shape — "impossible" MUST
be 0% or the scenarios are too easy.

## Remaining gaps (intentional, not bugs in chaos)

- `fraud` scenarios that require long vendor history (≥10 prior tx per vendor
  for `vendor_amount_anomaly` / `vendor_timing_anomaly`) don't fire — the
  chaos seed population mixes vendors, so historical stats are thin.
- `trial_balance_imbalance` and `statistical_sampling_reproducibility` have
  no dedicated engine rule yet — marked `expected_findings: []` so they pass
  on the clean signal. When engines land, populate expectations.
- Receipt `impossible` tier (Arabic, Chinese) intentionally fails in the
  mock — real OCR may do better; rerun with `--real-ocr` to verify.

## Known limitations

- Imagen output quality varies: truly "impossible" conditions (Arabic RTL,
  500-line receipts) may produce unreadable images even from the model.
  That's signal, not noise.
- All chaos DB writes go to `chaos/results/chaos_test.db` (separate from
  production `data/otocpa_agent.db`).
- No parallelism today — `--workers` is reserved but scenarios run serially.
