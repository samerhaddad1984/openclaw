"""Quebec tax nightmare scenarios — GST/QST/HST edge cases.

Exercises src/engines/tax_engine.py directly with Decimal inputs and
expected Decimal outputs. Oracle compares field-by-field with tolerance 0.
"""
from __future__ import annotations

import random
from decimal import Decimal
from typing import Any


TAX_SPECS: list[dict[str, Any]] = [
    {
        "subtype": "meal_50pct_alcohol_surcharge",
        "difficulty": "nightmare",
        "description": "Restaurant meal — 50% deductible — with alcohol surcharge",
        "input": {"amount": "85.00", "tax_code": "M", "has_alcohol": True},
        "expected": {"itc_pct": "0.5", "itr_pct": "0.5"},
        "severity_on_failure": "high",
    },
    {
        "subtype": "zero_rated_grocery_mixed_taxable",
        "difficulty": "nightmare",
        "description": "Grocery: zero-rated bread + taxable chips — mixed line items",
        "input": {"lines": [
            {"desc": "bread", "amount": "3.50", "tax_code": "Z"},
            {"desc": "chips", "amount": "4.99", "tax_code": "T"},
        ]},
        "expected": {"gst_on_taxable_only": True, "line_level_tax_required": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "capital_asset_at_500_threshold",
        "difficulty": "hard",
        "description": "Asset at exactly $500.00 — inclusive or exclusive of threshold?",
        "input": {"amount": "500.00"},
        "expected": {"capital_threshold_policy_clear": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "gift_card_purchase_not_taxable",
        "difficulty": "hard",
        "description": "Gift card purchase — not a supply, no GST/QST collected",
        "input": {"amount": "100.00", "category": "gift_card"},
        "expected": {"gst": "0.00", "qst": "0.00", "tax_code": "E"},
        "severity_on_failure": "high",
    },
    {
        "subtype": "employee_reimbursement_no_itc",
        "difficulty": "hard",
        "description": "Employee reimbursement — no ITC because no GST invoice",
        "input": {"amount": "125.00", "category": "employee_reimbursement"},
        "expected": {"itc": "0.00", "itr": "0.00"},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "personal_portion_vehicle",
        "difficulty": "nightmare",
        "description": "Vehicle expense with 30% personal portion — apportion ITC",
        "input": {"amount": "200.00", "business_pct": 0.70, "tax_code": "T"},
        "expected": {"apportion_required": True, "itc_business_only": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "old_gst_rate_prior_period",
        "difficulty": "hard",
        "description": "Invoice dated when GST was 6% — must use historical rate",
        "input": {"amount": "1000.00", "invoice_date": "2007-06-01"},
        "expected": {"gst_rate": "0.06"},
        "severity_on_failure": "high",
    },
    {
        "subtype": "qst_compound_vs_parallel_check",
        "difficulty": "nightmare",
        "description": "Verify QST is applied in parallel, NOT compounded on GST",
        "input": {"amount": "1000.00", "tax_code": "T"},
        "expected": {"gst": "50.00", "qst": "99.75", "total": "1149.75"},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "zero_rated_basic_groceries",
        "difficulty": "normal",
        "description": "Basic groceries — zero-rated",
        "input": {"amount": "45.00", "tax_code": "Z"},
        "expected": {"gst": "0.00", "qst": "0.00", "itc_allowed": True},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "prepared_food_taxable",
        "difficulty": "normal",
        "description": "Prepared food (ready-to-eat) — taxable, not zero-rated",
        "input": {"amount": "20.00", "category": "prepared_food", "tax_code": "T"},
        "expected": {"gst": "1.00", "qst": "2.00"},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "parking_taxable",
        "difficulty": "normal",
        "description": "Parking — fully taxable",
        "input": {"amount": "15.00", "category": "parking", "tax_code": "T"},
        "expected": {"gst_rate": "0.05", "qst_rate": "0.09975"},
        "severity_on_failure": "low",
    },
    {
        "subtype": "transit_zero_rated",
        "difficulty": "normal",
        "description": "Public transit — tax-exempt",
        "input": {"amount": "15.00", "category": "transit", "tax_code": "E"},
        "expected": {"gst": "0.00", "qst": "0.00"},
        "severity_on_failure": "low",
    },
    {
        "subtype": "ontario_hst_on_quebec_firm",
        "difficulty": "hard",
        "description": "Quebec firm purchases in ON — 13% HST, cross-provincial ITC",
        "input": {"amount": "100.00", "tax_code": "HST", "province": "ON"},
        "expected": {"hst_rate": "0.13", "itc_full": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "atlantic_hst_15pct",
        "difficulty": "hard",
        "description": "Atlantic province HST at 15%",
        "input": {"amount": "100.00", "tax_code": "HST", "province": "NB"},
        "expected": {"hst_rate": "0.15"},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "insurance_quebec_9pct_no_gst",
        "difficulty": "hard",
        "description": "Quebec insurance: 9% non-recoverable, NO GST",
        "input": {"amount": "500.00", "tax_code": "I"},
        "expected": {"gst": "0.00", "provincial_charge_pct": "0.09", "recoverable": False},
        "severity_on_failure": "high",
    },
    {
        "subtype": "foreign_vat_not_recoverable",
        "difficulty": "hard",
        "description": "Foreign VAT — cannot be recovered in Canada",
        "input": {"amount": "100.00", "tax_code": "VAT", "country": "FR"},
        "expected": {"itc": "0.00", "expensed": True},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "quick_method_trap_gst_over_claim",
        "difficulty": "nightmare",
        "description": "Quick Method participant — cannot claim ITCs on operating expenses",
        "input": {"quick_method": True, "expense_amount": "500.00"},
        "expected": {"itc_disallowed": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "foreign_digital_service_registered",
        "difficulty": "hard",
        "description": "Netflix-style foreign digital service registered in QC — QST charged",
        "input": {"vendor": "Netflix", "amount": "16.00", "qst_charged": True},
        "expected": {"qst_on_foreign_digital": True, "gst": "0.00"},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "micro_transaction_tax_leakage",
        "difficulty": "hard",
        "description": "$0.10 purchase — GST rounds to 0¢? Must set minimum $0.01",
        "input": {"amount": "0.10", "tax_code": "T"},
        "expected": {"gst_min": "0.01", "qst_min": "0.01"},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "large_amount_precision",
        "difficulty": "hard",
        "description": "$999,999.99 — rounding must still hit to the cent",
        "input": {"amount": "999999.99", "tax_code": "T"},
        "expected": {"precision_to_cent": True},
        "severity_on_failure": "high",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out = []
    for spec in TAX_SPECS:
        out.append({
            "category": "tax",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec["severity_on_failure"],
            "affects_engines": ["src.engines.tax_engine", "src.engines.tax_code_resolver"],
            "oracle": "tax",
            "input_spec": {"kind": "tax", **spec["input"]},
            "ground_truth": spec["expected"],
        })
    return out
