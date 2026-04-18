# Fraud-pattern Prompts

Fraud scenarios rarely need AI-generated images — the patterns are
structural (dates, amounts, vendor names). When an image IS needed (e.g.
forged-looking receipt) use these directives.

- **altered_date_handwritten**: "a receipt with the date partially crossed
  out and a different date written over it in ballpoint pen"
- **bank_detail_change**: "a vendor invoice with the bank transit/routing
  number whited out and re-typed in a slightly different font"
- **backdated_invoice**: "an invoice clearly printed today but dated three
  months ago — watermark date conflicts with invoice date"
- **duplicate_rotated**: "the same receipt image rotated 180°"

See `chaos/generators/fraud_scenarios.py` for each pattern's expected
rule name in `src/engines/fraud_engine.py`.
