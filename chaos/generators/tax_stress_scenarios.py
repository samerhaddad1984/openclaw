"""Tax stress scenarios (Canadian focus) — Sprint F+ Round 2.

Exercises the tax engine with real-world Canadian edge cases: method
switches, partial rate changes, exempt/zero-rated confusions, meal
50%-vs-100% rules, vehicle personal-use portions, home office,
partnership allocations, capital gains with ACB adjustments, CCA
recapture / terminal loss, loss carryforwards, SR&ED with ITC,
GST-only provinces, and residential rebates.
"""
from __future__ import annotations

import random
from typing import Any


TAX_STRESS_SCENARIOS: list[dict[str, Any]] = [
    {
        "subtype": "tax_quick_to_regular_midperiod",
        "difficulty": "nightmare",
        "description": "Client switches from Quick Method to Regular Method on 2025-06-01",
        "switch_date": "2025-06-01",
        "pre_switch_method": "quick",
        "post_switch_method": "regular",
        "severity": "high",
    },
    {
        "subtype": "tax_rate_change_midperiod",
        "difficulty": "hard",
        "description": "Hypothetical QST rate change 9.975% → 10.5% on 2025-07-01",
        "old_rate": 0.09975,
        "new_rate": 0.105,
        "switch_date": "2025-07-01",
        "severity": "high",
    },
    {
        "subtype": "tax_grocery_luxury_addon",
        "difficulty": "hard",
        "description": "QC grocery receipt with zero-rated basics + taxable luxury item",
        "zero_rated_lines": 10,
        "taxable_lines": 1,
        "severity": "medium",
    },
    {
        "subtype": "tax_meal_relocation_100pct",
        "difficulty": "hard",
        "description": "Meal during employee relocation — 100% deductible (not 50%)",
        "meal_amount": 125.00,
        "deduction_pct": 1.0,
        "severity": "medium",
    },
    {
        "subtype": "tax_vehicle_personal_use",
        "difficulty": "hard",
        "description": "Vehicle expense with 40% personal-use portion — ITC reduced",
        "total_expense": 5000.00,
        "personal_use_pct": 0.40,
        "business_use_pct": 0.60,
        "severity": "high",
    },
    {
        "subtype": "tax_home_office_multi_room",
        "difficulty": "hard",
        "description": "Home office across 2 rooms (office + storage) — combined percentage",
        "office_sqft": 150,
        "storage_sqft": 50,
        "home_sqft": 2000,
        "expected_pct": 0.10,
        "severity": "medium",
    },
    {
        "subtype": "tax_partnership_allocation",
        "difficulty": "nightmare",
        "description": "Partnership income allocated 60/30/10 across 3 partners",
        "partners": 3,
        "allocations": [0.60, 0.30, 0.10],
        "total_income": 100000.00,
        "severity": "high",
    },
    {
        "subtype": "tax_capital_gains_acb_adjustment",
        "difficulty": "nightmare",
        "description": "Capital gain on shares with ACB adjusted for reinvested dividends",
        "proceeds": 50000.00,
        "original_cost": 30000.00,
        "reinvested_dividends": 5000.00,
        "expected_gain": 15000.00,
        "severity": "high",
    },
    {
        "subtype": "tax_cca_recapture_on_disposal",
        "difficulty": "hard",
        "description": "Asset sold above UCC — recapture of CCA previously claimed",
        "ucc": 10000.00,
        "proceeds": 15000.00,
        "expected_recapture": 5000.00,
        "severity": "high",
    },
    {
        "subtype": "tax_terminal_loss",
        "difficulty": "hard",
        "description": "Last asset in class sold below UCC — terminal loss",
        "ucc": 8000.00,
        "proceeds": 3000.00,
        "expected_terminal_loss": 5000.00,
        "severity": "high",
    },
    {
        "subtype": "tax_non_capital_loss_carryforward",
        "difficulty": "hard",
        "description": "Prior-year non-capital loss applied against current income",
        "prior_year_loss": 20000.00,
        "current_year_income": 35000.00,
        "expected_taxable": 15000.00,
        "severity": "high",
    },
    {
        "subtype": "tax_sred_claim_with_itc",
        "difficulty": "nightmare",
        "description": "SR&ED eligible expenses with 35% ITC refund",
        "qualifying_expenses": 50000.00,
        "itc_rate": 0.35,
        "expected_itc": 17500.00,
        "severity": "high",
    },
    {
        "subtype": "tax_gst_only_province",
        "difficulty": "normal",
        "description": "Purchase in Alberta (GST only, no PST/QST)",
        "province": "AB",
        "subtotal": 1000.00,
        "expected_gst": 50.00,
        "expected_qst": 0.00,
        "severity": "medium",
    },
    {
        "subtype": "tax_zero_rated_vs_exempt_grocery",
        "difficulty": "hard",
        "description": "Bread = zero-rated (ITC claimable), magazine = exempt (no ITC)",
        "zero_rated_amount": 50.00,
        "exempt_amount": 20.00,
        "severity": "medium",
    },
    {
        "subtype": "tax_residential_rebate",
        "difficulty": "nightmare",
        "description": "New home GST/HST rebate for buyer under $450K",
        "home_price": 400000.00,
        "province": "QC",
        "expected_federal_rebate": 6300.00,
        "expected_provincial_rebate": 9800.00,
        "severity": "high",
    },
    {
        "subtype": "tax_mixed_gst_hst_provinces",
        "difficulty": "hard",
        "description": "Sales to customers in QC (GST+QST) + ON (HST) + AB (GST only)",
        "provinces": ["QC", "ON", "AB"],
        "severity": "medium",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in TAX_STRESS_SCENARIOS:
        out.append({
            "category": "tax",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec.get("severity", "medium"),
            "expected_fail": False,
            "future_feature": False,
            "affects_engines": [
                "src.engines.tax_engine",
                "src.engines.tax_code_resolver",
                "src.engines.fixed_assets_engine",
                "src.engines.t2_engine",
            ],
            "oracle": "tax",
            "input_spec": {"kind": "tax_synthetic", "spec": spec},
            "ground_truth": {"subtype": spec["subtype"], "expected": spec},
        })
    return out
