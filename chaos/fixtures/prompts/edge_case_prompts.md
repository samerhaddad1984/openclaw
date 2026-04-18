# Edge-case Receipt Prompts

Edge cases cover values that are *valid but rare*. These push the OCR
extractor and the line-item engine to their limits without being outright
adversarial.

- **negative_total**: "a retail receipt showing a negative grand total,
  clearly marked as a RETURN / REMBOURSEMENT"
- **zero_total**: "a receipt with a $0.00 grand total because loyalty
  points covered the full amount"
- **4_digit_cent_price**: "a gas station receipt with the per-litre price
  shown to four decimal digits, e.g. $1.4799/L"
- **discount_makes_negative**: "a receipt where a large coupon discount
  makes one line go negative before the total"
- **date_in_future**: "a receipt dated two years in the future — clearly
  a device-clock error"
- **date_10_years_old**: "a receipt dated exactly ten years ago on yellowed
  thermal paper"
- **identical_line_amounts**: "a receipt with 12 line items all showing the
  same dollar amount"
- **single_line_huge_amount**: "a receipt with one line item at nearly
  one million dollars"
- **very_many_items**: "an extremely long receipt with over 100 line items"
- **foreign_labels_spanish**: "an entirely Spanish-language receipt"
- **foreign_labels_arabic**: "an entirely Arabic-language receipt with
  right-to-left text layout"
- **foreign_labels_chinese**: "an entirely simplified-Chinese receipt"
- **totals_only_no_lines**: "a parking receipt showing only the total
  and tax, no itemized lines"

All prompts are designed to force the OCR/extractor to either produce
reasonable output OR gracefully flag low confidence — never hallucinate.
