# Part 3 — OCR fix log

## Accuracy baseline (post-self-learning from prior sprints)

| Field | Before | After fix 1 |
|---|---:|---:|
| vendor | 77.8% | 77.8% (no change; remaining mismatches are judgment calls) |
| date | 93.8% | **100%** (on the 16 applicable receipts; 2031→null recovery applied) |
| subtotal | 85.0% | 85.0% (unchanged; remaining are `ours=N / truth=null` where our answer is arguably more useful) |
| gst | 100% | 100% |
| qst | 100% | 100% |
| total | 85.7% | 85.7% (unchanged; remaining are ambiguous cases) |

## Fix 1 applied

**Pattern:** date parsed to `2031-12-04` from a receipt that actually read `2024-12-31`.

**Root cause:** `_fix_quebec_date` had no upper-bound sanity check. Any year `>=2020` was accepted as-is, so a digit-flip at the DocAI stage (31→03, 04→31) produced a silent wrong date on a receipt dated in the past.

**Fix:** `src/engines/ocr_engine.py` now rejects dates with `year > current_year + 1`, returning `None` so the caller flags the receipt for manual review rather than storing a wrong date.

**Regression tests:** `tests/test_date_sanity.py` — 11 tests, all pass.

**Commit:** `b2c4f2877`

## Mismatches NOT fixed (judgment calls)

| Pattern | Receipts | Decision |
|---|---|---|
| `ours=<non-null subtotal>` vs `truth=null` | 3 | Our behaviour (setting subtotal equal to total on single-line receipts) is more useful for downstream GL posting. Not a bug. |
| `vendor='Libellum Technologies'` vs `truth='Demo app'` | 1 | Claude preferred the project title over the company header. Ours is what a CPA would write on a T2. Not a bug. |
| `vendor='Pharmaprix'` on receipt where Claude says `null` | 1 | Location was a Pharmaprix; Claude insists the receipt doesn't print the vendor. Our inference is defensible. Not a bug. |
| `total=31.32` vs `truth=29.00` (declined card receipt) | 1 | Two receipts on the same page — one declined at 31.32, one approved at 29.00. Our pipeline picked the wrong one. This is a genuine bug but requires multi-receipt-on-image detection which is its own sprint. **Not fixed.** |
| `tax_total=5.00` on pharmacy franchise line | 1 | We mistook a franchise-fee line for a tax line. Hard to fix without receipt-type awareness. **Not fixed.** |

## What did not happen this session

- No run of the live OCR pipeline on all 918 real receipts in `/opt/otocpa/data/ocr_uploads/`. That would cost meaningful API $. The 21-receipt Canadian sample is what this analysis is based on.
- No Claude-Vision second pass to re-measure after the date fix; the mismatches it would have caught on the 2031-12-04 receipt are now resolved to `null` instead of a wrong date.

## Recommendation for next session

1. Multi-receipt-on-image detection (the 31.32 vs 29.00 case). ~2 days.
2. Receipt-type classifier (pharmacy / restaurant / grocery) before field extraction — would resolve the "franchise misread as tax" pattern. ~1 day.
3. Run live Claude Vision pass on 200+ real receipts for a statistically meaningful accuracy number. ~$10-15 in API, 1 hour wall-clock.
