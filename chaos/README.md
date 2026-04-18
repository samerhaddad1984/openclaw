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

## Known limitations

- Many workflow/financial/recon scenarios use optimistic passthroughs
  until the engines they represent are wired up. The framework is a
  harness — running it *reveals* which engines are missing coverage.
- Imagen output quality varies: truly "impossible" conditions (Arabic RTL,
  500-line receipts) may produce unreadable images even from the model.
  That's signal, not noise.
- Fraud runner seeds patterns into a fresh `chaos_test.db`; it does NOT
  share state with the production DB.
- No parallelism today — `--workers` is reserved but scenarios run serially.
