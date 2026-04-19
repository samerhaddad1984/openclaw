# Part 2 — Real production OCR accuracy on 21 Canadian receipts

**Source:** `/opt/otocpa/scripts/analysis/canadian_receipts_analysis_after_learning.json` (latest post-learning pass against Claude Sonnet 4.6 Vision as ground truth).

**Method:** same 21 receipts were processed by the production pipeline (Tesseract → DocAI → DeepSeek → self-learning) and then each field compared against Claude Vision's independent read of the same image.

## Accuracy by field (post-Sprint-A self-learning)

| Field | Matches | Applicable | Accuracy |
|---|---:|---:|---:|
| vendor | 14 | 18 | **77.8%** |
| date | 15 | 16 | **93.8%** |
| subtotal | 17 | 20 | **85.0%** |
| gst | 14 | 14 | **100.0%** |
| qst | 14 | 14 | **100.0%** |
| total | 18 | 21 | **85.7%** |

- **Clean receipts (zero mismatches):** 12 / 21 = **57.1%**
- **Total mismatches across all receipts:** 12

## Error-pattern breakdown

| Pattern | Count |
|---|---:|
| vendor_mismatch | 4 |
| subtotal_mismatch | 3 |
| total_mismatch | 3 |
| tax_mismatch | 1 |
| date_mismatch | 1 |

## What this number means

- GST / QST extraction is at **100%** — the tax-code resolver is doing its job on real Canadian receipts.
- Vendor accuracy is the weakest field (77.8%). Reviewing the mismatches below confirms the pattern: we over-detect a vendor where Claude says there isn't a clear one (US company, demo screenshot), or we pick one of multiple candidate text blocks.
- Total accuracy (85.7%) is dragged down by three receipts where our pipeline returned a specific number and Claude said the total field was null on a non-total-printing receipt.

## Specific mismatches

| Receipt | Issue |
|---|---|
| InvoiceSample.webp | US-based invoice — we extracted subtotal=110 which matches total; Claude said "not a Canadian receipt". |
| Pinchos receipt | we got vendor='Pinchos'; Claude got 'QUEBEC' (likely city name confused with header). |
| Pharmaprix receipt with no clear vendor | we returned 'Pharmaprix'; Claude said null. |
| Libellum Technologies demo | we extracted vendor='Libellum Technologies'; Claude said 'Demo app'. |
| Date off by decade | ours=2031-12-04; truth=2024-12-31 — digit-swap error (year 2031 ↔ day 31). |
| Tax=5.00 on receipt with no tax | ours flagged a 5.00 line as tax; Claude saw no tax line. |
| Total=31.32 vs 29.00 | sub-$5 swap, likely tip confusion. |

Raw data: `/opt/otocpa/scripts/analysis/canadian_receipts_analysis_after_learning.json`.

## Caveats

- Claude Vision is treated as ground truth but can itself be wrong (e.g., on ambiguous multi-line vendor headers, neither Claude nor our pipeline is guaranteed correct).
- 21 receipts is a small sample. A proper production accuracy measurement would include 200+ receipts across several vendor categories.
- This is the post-self-learning pass. The `_before_fixes.json` baseline is lower; Sprint A's self-learning loop already absorbed a number of corrections.
- Image quality matters. Several of the 21 receipts are poor-condition phone photos; Claude's agreement with itself on those would also degrade.
