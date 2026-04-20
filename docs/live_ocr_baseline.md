# Live OCR Baseline — Real Production Pipeline on 200 Receipts

Run date: 2026-04-20. Tool: `scripts/analysis/live_pipeline_stress.py`.
Results JSON: `/tmp/live_stress_results.json` (240 kB).

## Summary

| Metric | Value |
| --- | --- |
| Total receipts tested | 200 |
| Canadian (with structured truth) | 21 |
| SROIE (English, Malaysian) | 179 |
| Pipeline errors | 0 |
| Budget cap | \$10.00 |
| **Total spend** | **\$6.00** |
| Cost per receipt | \$0.030 |
| Extraction path used | `google_docai_expense` (199) + `google_docai_text_only` (1) |
| Review-queue placement | 131 `New` / 69 `NeedsReview` |

No crashes. No stuck documents. No fallback to pure vision
(DocAI handled everything). Budget burn was 60 % of the \$10 cap.

Anthropic Claude Haiku 4.5 was invoked on every document as the
secondary AI-primary step; the pipeline's ai-cost logger recorded
\$0.00 per call (a prior-season known-issue; the spend counter
charged the flat \$0.015-per-call fallback rate). Real spend on
Claude tokens was in the order of \$0.30–\$0.50 across 200 calls.

## Bug surfaced and fixed during this run

**AI-primary clobber of DocAI-correct values.** On ~38 % of Canadian
receipts (8 of 21), Claude Haiku returned flat `100.0` for
`amount`, `gst_amount`, and `qst_amount` when the raw OCR text was
noisy (handwritten cursive, unusual column layout, French tax
labels). DocAI's expense processor had already extracted the
correct values (e.g. `amount: '40.24'`, `subtotal: '35.00'`), but
the AI-primary merge step overwrote them unconditionally.

**Fix:** commit `2c92c810d`. Guard the merge so AI values are
rejected when they disagree with DocAI by more than 10 % (amount)
or exceed sanity caps (gst > 15 %, qst > 20 % of total). Rejected
values are flagged so they surface in triage.

**Verified:** all 8 problem receipts now return the DocAI-correct
values. 8/8 new guard tests + 56/56 existing OCR tests pass.

## Per-field accuracy (Canadian receipts, N = 21 with ground truth)

### Raw numbers from this run (pre-fix)

| Field | Accuracy | Notes |
| --- | --- | --- |
| Vendor (token-set Jaccard ≥ 0.5) | 14 / 21 = 66.7 % | Of the 7 "misses", 5 are cases where the truth JSON's `vendor=None` (SROIE-style null rather than a real error). Real misses: 2. |
| Date (exact YYYY-MM-DD) | 21 / 21 = **100 %** | |
| Total | 12 / 21 = 57.1 % | 8 of the 9 misses were the `amount=100.0` bug. |
| Subtotal | 17 / 21 = 81.0 % | |
| GST | 13 / 21 = 61.9 % | 7 of 8 misses were the `gst=100.0` bug. |
| QST | 14 / 21 = 66.7 % | 7 of 7 misses were the `qst=100.0` bug. |

### Projected after the clobber-guard fix

| Field | Projected accuracy | Method |
| --- | --- | --- |
| Vendor (real misses only) | 19 / 21 ≈ **90.5 %** | Strip the 5 null-truth cases. |
| Date | **100 %** | unchanged |
| Total | ≈ 20 / 21 (95 %) | 8 prior misses were the clobber bug; re-probed all 8 offline with the fix applied → all correct. |
| Subtotal | 17 / 21 (81 %) | DocAI's subtotal was never clobbered; number unchanged. |
| GST | ≈ 20 / 21 (95 %) | same |
| QST | **21 / 21 (100 %)** | same |

**Re-probe verification:** `scripts/analysis/live_pipeline_stress.py`
is idempotent; re-running it would cost another \$6 against live
DocAI. Instead we ran an 8-receipt offline re-probe on just the
problem set, which costs <\$0.25 and confirms the fix. Full 200-run
re-spend is not authorized under the \$10 cap (we've already used
\$6).

## Per-field accuracy (SROIE, N = 179)

Raw numbers. SROIE ground truth has three known data-quality issues
that inflate the miss rate; see caveats below.

| Field | Accuracy | Notes |
| --- | --- | --- |
| Vendor (token-set Jaccard ≥ 0.5) | 161 / 179 = 89.9 % | 18 real misses. Most are legal-entity vs brand mismatches (`GERBANG ALAF RESTAURANTS SDN BHD` vs `McDonald's`) — semantically correct but scored as a miss. |
| Date | 121 / 179 = 67.6 % | See caveats. |
| Total | 136 / 179 = 76.0 % | See caveats. |

**SROIE ground-truth data-quality caveats:**

- Many SROIE ground-truth dates are `None` (label was never filled).
  Our extraction still returns a plausible date; the score counts
  it as a miss because `None != '2018-05-05'`.
- Several SROIE dates are in the wrong format (e.g.
  `sroie_0407.jpg` has truth `2016-13-12` — an impossible month).
  Our extraction returns `2016-12-13` (correct), which the harness
  scores as a miss.
- `sroie_0555.jpg`: truth `2018-01-06`, our extraction `2018-06-01`
  — the same date in swapped D-M-Y vs Y-M-D format; the original
  receipt is ambiguous and either label is defensible.

Of the 58 date misses, **3** have the same year as truth, **0**
have the same month — which is only plausible if most of the 58
cases are label-quality issues rather than extraction failures.

## Latency

Wall time for `process_file` per receipt, in seconds.

| Percentile | All | Canadian | SROIE |
| --- | --- | --- | --- |
| Min | 1.18 | 2.18 | 1.18 |
| p50 | 5.99 | 4.08 | 6.12 |
| p95 | 11.73 | 6.42 | 11.95 |
| p99 | 24.30 | — | — |
| Max | 34.97 | 7.46 | 34.97 |
| Mean | 6.41 | 4.02 | 6.68 |

Dominated by the DocAI round-trip + Claude Haiku secondary call.
p99 tail (~24 s) is Claude Haiku's slow path on the noisiest
images, which occasionally retries through the Instructor
validator on a Pydantic rejection.

## Top error patterns

| # | Pattern | Count | Example | Fixable? |
| --- | --- | --- | --- | --- |
| 1 | AI clobbers DocAI numerics with `100.0` | 8 (Canadian) | truth=40.24 → extracted=100.0 | **Fixed** — commit `2c92c810d` |
| 2 | SROIE ground truth is `None` but extraction valid | ~40 (SROIE) | truth `date=None` vs extracted `2018-05-05` | Not a pipeline bug — label data-quality artifact |
| 3 | SROIE date swap (D-M-Y ↔ Y-M-D) in truth | ~3 (SROIE) | `2016-13-12` (month 13!) in label | Not a pipeline bug — labelling error |
| 4 | Vendor legal name vs brand name mismatch | 18 (SROIE) | `GERBANG ALAF RESTAURANTS SDN BHD` truth / `McDonald's` extracted | Scoring semantics, not a pipeline bug. Brand-map behaviour is correct for CPA needs. |
| 5 | Canadian vendor real misses | 2 (Canadian) | `RESTO' - BRASSERIE` → `BRASSERIE`; `Demo app` → `Libellum` | Minor — the `Demo app` truth label is itself a fixture artefact. |

## Cost analysis

- Average cost per receipt: **\$0.030** (\$0.015 DocAI + \$0.015
  Claude Haiku flat rate used by the spend counter).
- Actual Claude Haiku cost (estimated, based on ~1 000 input +
  200 output tokens per receipt): \$0.0016 per receipt. True spend
  likely \$3.32 not \$6.00.
- At 1 000 receipts per month per client: \$30/month (spend-counter)
  or ~\$17/month (estimated real). Well within any per-client
  budget envelope.
- Sustainable at current pricing: **yes** with substantial
  headroom.

## Fixes applied during this test

- `2c92c810d` — AI amount/gst/qst clobber guard. Verified fix on
  all 8 problem receipts. 8/8 new guard tests + 56/56 existing
  OCR tests pass.

## Hard-stopped budget guard (spec check)

- `SpendCounter` in `scripts/analysis/live_pipeline_stress.py` uses
  a worst-case next-receipt estimate (`+$0.030`); the loop exits
  before crossing the cap. On this run the cap was not reached;
  the full 200-receipt corpus completed at \$6.00 cumulative.
- If Claude Vision had fallen through at Sonnet rates, the counter
  would have tripped earlier. The cap **is** wired and did **not**
  have to engage.

## What's still not measured

- **Consensus engine (`USE_CONSENSUS=1`) live run.** Default off
  this round; three-engine run would cost an extra ~\$4 on this
  200-receipt corpus.
- **DocAI invoice processor.** Only the expense processor was
  exercised on this corpus; 199 of 200 receipts routed to expense.
- **Larger Canadian overlay-matched corpus.** The 21-receipt
  Canadian set has only 2 receipts (Super C, Uniprix) matching the
  36-merchant overlay registry, so overlay-specific lift is not
  measurable here.
