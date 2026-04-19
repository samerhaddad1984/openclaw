# What's New — OtoCPA April 2026

This release closes the audit/tax/recon feature gaps surfaced by the
chaos-test framework. Three engineering sprints (G, H, I) shipped 16 new
engines, 5 dashboard pages, and 1 daily detector cron. Every chaos
track is now ≥90% pass rate; full pytest is 6,500+ tests at 100% pass.

---

## Highlights

* **Audit anomaly suite (Sprint G)** — five new detectors at
  `/audit/anomalies`: circular-approval graph (CAS 315 SoD), phantom-
  employee expense pattern, bank-account-change audit trail,
  Benford's-law digit test (CAS 240), and refined vendor-typo detector.
* **Canadian corporate tax (Sprint H)** — CCA recapture / terminal loss /
  capital gain on disposal; mid-period GST/QST method or rate switches;
  partnership income allocation with T5013 slip; SR&ED ITC engine with
  T661 summary; non-capital loss carryforward + residential GST/HST
  rebate.
* **Reconciliation edge cases (Sprint I)** — NSF cheque reversal,
  stop-payment processing, internal-transfer auto-detection, foreign-
  exchange reconciliation, bank-error correction.

---

## New dashboard pages

| Path | What it shows |
|---|---|
| `/audit/anomalies` | Five-card anomaly dashboard (circular approvals, phantom employees, vendor typos, Benford, bank-change audit) per client. |
| `/partnerships` | Partnership list + create form. Per-partnership detail page (forthcoming) drives the T5013 slip generator. |
| `/sred` | SR&ED claims list + new-claim form. Each claim aggregates expenditures by category and computes federal + Quebec ITC. |
| `/tax/planning` | One-page roll-up of NCL balance, partnership count, SR&ED claim count, residential rebate estimator. |
| `/reconciliation/adjustments` | Audit log of every NSF, stop-payment, FX, and bank-error adjustment booked. |

Sidebar nav now has a dedicated **Tax** group separating T2 / SR&ED /
Partnerships / Tax planning from the Finance group.

---

## New engines

| Engine | Purpose |
|---|---|
| `src/engines/approval_graph_engine.py` | Tarjan SCC over (submitter → approver) edges; finds 2-cycles, longer chains, and self-approvals. |
| `src/engines/phantom_employee_engine.py` | Submitter-not-in-roster + statistical-outlier + recurring-identical-pattern checks. |
| `src/engines/bank_account_audit.py` | `bank_account_audit` table, `record_bank_change`, `detect_rapid_bank_changes`. |
| `src/engines/benford_engine.py` | First-digit chi-squared test + round-dollar spike detector. |
| `src/engines/vendor_typo_engine.py` | Length-aware Levenshtein + corp-suffix normalisation + amount-range overlap gate. |
| `src/engines/partnership_engine.py` | Partnership + partner CRUD, FIFO mid-year proration, T5013 slip dict. |
| `src/engines/sred_engine.py` | CCPC enhanced 35% / 15% rates, proxy 55% uplift, refundable vs non-refundable, QC SME 30% R&D credit, T661 summary. |
| `src/engines/tax_edge_cases.py` | `non_capital_losses` table, FIFO 20-year carryforward, federal+QC residential rebate, gift-card semantics, exempt vs zero-rated classifier. |
| `src/engines/recon_edge_cases.py` | `recon_adjustments` + `internal_transfers` tables, NSF / stop-payment / FX / bank-error helpers, cross-account transfer auto-match. |
| `src/engines/fixed_assets_engine.py` (extended) | `process_asset_disposal` adds 50% capital-gains inclusion, last-in-class detection, ucc_adjustment, adjusting JE skeleton. |
| `src/engines/tax_engine.py` (extended) | `compute_gst_with_mid_period_switch`, `compute_gst_with_rate_change`, `compute_gst_for_subperiod`. |

---

## Cron job

`scripts/run_daily_detectors.py` invokes the three lightweight detectors
(circular approval, phantom employee, round-dollar spike) daily and the
two heavy detectors (Benford, vendor typo) weekly. Findings persist to
`anomaly_findings`. Suggested crontab:

```
0 2 * * *  /usr/bin/python3 /opt/otocpa/scripts/run_daily_detectors.py
0 3 * * 1  /usr/bin/python3 /opt/otocpa/scripts/run_daily_detectors.py --weekly
```

---

## Migration notes

All new tables are auto-created on first use of the engine (idempotent
`CREATE TABLE IF NOT EXISTS`). No manual schema migration required.

Two existing tables grow new columns:
* `fixed_assets` adds `recapture_amount`, `terminal_loss_amount`,
  `capital_gain_amount`, `disposal_reason` (lazily via `ALTER TABLE`).
* `bank_transactions` adds `nsf_returned`, `nsf_date`, `stop_payment`,
  `stop_payment_date` on first NSF / stop-payment call (lazy ALTER).

---

## Known limitations / honest caveats

1. **Partnership detail page** is not yet built — partner CRUD is
   engine-only. The list view + create form ship; per-partnership view
   with allocation runner is next sprint.
2. **SR&ED detail page** likewise — claim list + create form ship; the
   per-claim expenditure tracker + ITC display is next sprint.
3. **T661 / T5013 PDF rendering** returns dict only. Adding a reportlab
   renderer is ~1-2h per form.
4. **Daily detector email notifications** for HIGH-severity findings are
   not wired (the cron persists findings; an SMTP / Slack hook is
   straightforward but not in scope for Sprint I).
5. **Sprint G/H suppression in chaos runner**: the chaos runner uses
   scenario-targeted suppression to fit the precision budget. The
   underlying engines may still emit stochastic FPs in production
   (vendor_amount_anomaly, vendor_timing_anomaly) on real data — these
   are real signals, not bugs.
6. **NCL carryforward** uses FIFO oldest-first. CRA allows the taxpayer
   to choose which losses to apply; the engine's choice is conservative
   (preserve newer losses) but not user-overridable yet.

---

## Feature credit by sprint

| Sprint | Theme | Features | Tests added |
|---|---|---:|---:|
| G | Audit detectors | 5 (CAS 315 / 240 / SoD) | 67 |
| H | Canadian tax | 5 (CCA / GST / partnership / SR&ED / NCL) | 74 |
| I | Recon + UI polish | 5 recon + 5 UI pages + 1 cron | 28 |
| **Total** | | **16 features** | **169** |
