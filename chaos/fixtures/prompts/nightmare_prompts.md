# Nightmare Receipt Prompts

These prompts feed `chaos/generators/ai_image_generator.py` via
`receipt_scenarios.py`'s `_build_prompt()` helper. They are authored as
realistic photography directions rather than text descriptions so Imagen
produces a receipt image, not a stylized illustration.

## Base template

> A realistic photo of a Canadian retail receipt from `<VENDOR>`, dated
> `<DATE>`, total CAD `$<AMOUNT>`, showing approximately `<N>` line items.
> Conditions: `<condition phrases joined by ,>`. The receipt text must be
> legible enough that key amounts are visible.

## Sample nightmare conditions

- **coffee_stain_bottom_third**: "a large brown coffee stain covering the bottom third"
- **crumpled**: "heavily crumpled and wrinkled"
- **thermal_fade_right**: "thermal print faded on the right side"
- **4_decimal_fuel_price**: "fuel price shown with four decimal digits (e.g. $1.4799/L)"
- **torn_corner**: "torn bottom-right corner"
- **rotated_45deg**: "photographed at a 45-degree angle"
- **brightness_minus_80pct**: "in very low lighting (80% darker)"
- **flash_glare_center**: "with harsh camera flash glare obscuring the center"
- **handwritten_overlay_tip**: "with a handwritten '+$10 tip' in blue ink"
- **plastic_sleeve_reflection**: "photographed through a clear plastic sleeve"
- **bilingual_labels**: "with bilingual French/English labels on the same line"
- **split_across_photos**: "extremely long — 50+ items — cut across three photos"
- **overlapping_cc_slip**: "with a credit-card slip stapled partly over the receipt"
- **dual_currency_display**: "showing both CAD and USD totals"

## Impossible tier

- **foreign_labels_arabic** + **rtl_text**: RTL receipts are an extra stretch for OCR.
- **very_many_items**: 100+ line items — most OCR pipelines truncate.
- **negative_total**: receipt printers rarely emit negatives cleanly.

## Cost

- Imagen 3 generate is ~$0.03/image.
- 50 smoke images → ~$1.50.
- 1,000 full-run images → ~$30.
- Cache is keyed on (model, prompt) so the same scenario re-uses the first
  generation forever.
