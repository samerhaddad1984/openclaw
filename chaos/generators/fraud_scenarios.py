"""Fraud pattern nightmare scenarios.

Note: Many overlap with audit. Fraud scenarios specifically target the fraud_engine
rules in src/engines/fraud_engine.py (14 rules as of 2026-04).
"""
from __future__ import annotations

import random
from typing import Any


FRAUD_SPECS: list[dict[str, Any]] = [
    {
        "subtype": "duplicate_altered_date",
        "difficulty": "nightmare",
        "description": "Same vendor+amount resubmitted with date shifted by 2 days",
        "input": {"pattern": "date_shift", "days_shift": 2},
        "expected_rules_fired": ["duplicate_fuzzy"],
        "severity_on_failure": "high",
    },
    {
        "subtype": "duplicate_rotated_image",
        "difficulty": "nightmare",
        "description": "Resubmission of rotated receipt — fingerprint must still match",
        "input": {"pattern": "rotated_resubmission"},
        "expected_rules_fired": ["duplicate_by_fingerprint"],
        "severity_on_failure": "high",
    },
    {
        "subtype": "sequential_invoices_different_days",
        "difficulty": "hard",
        "description": "Invoice numbers 101, 102, 103 across 3 months — phantom vendor pattern",
        "input": {"pattern": "sequential_invoice_numbers", "count": 3, "spread_months": 3},
        "expected_rules_fired": ["sequential_invoice_pattern"],
        "severity_on_failure": "medium",
    },
    {
        "subtype": "round_dollar_vendor_irregular",
        "difficulty": "hard",
        "description": "Round $1000 amounts from a vendor that normally has irregular amounts",
        "input": {"pattern": "round_dollar_from_irregular", "rounds": 3},
        "expected_rules_fired": ["round_number_flag"],
        "severity_on_failure": "medium",
    },
    {
        "subtype": "vendor_name_typos",
        "difficulty": "hard",
        "description": "IGA vs I.G.A. vs IGA Inc. — fuzzy vendor match",
        "input": {"pattern": "typo_variants", "variants": ["IGA", "I.G.A.", "IGA Inc."]},
        "expected_rules_fired": ["vendor_typo_duplicate"],
        "severity_on_failure": "medium",
    },
    {
        "subtype": "split_to_avoid_approval_limit",
        "difficulty": "nightmare",
        "description": "$9,999 bill split into $4999.50 + $4999.50 to avoid $5k approval",
        "input": {"pattern": "structured_split", "limit": 5000.0, "splits": 2},
        "expected_rules_fired": ["split_transaction"],
        "severity_on_failure": "critical",
    },
    {
        "subtype": "weekend_activity_spike",
        "difficulty": "hard",
        "description": "Sudden spike of large weekend transactions",
        "input": {"pattern": "weekend_spike", "count": 10, "min_amount": 500},
        "expected_rules_fired": ["weekend_transaction"],
        "severity_on_failure": "medium",
    },
    {
        "subtype": "amount_just_under_individual_limit",
        "difficulty": "hard",
        "description": "Transactions at $499, $499, $498 when approval limit is $500",
        "input": {"pattern": "just_under_limit", "limit": 500.0, "count": 5},
        "expected_rules_fired": ["threshold_structuring"],
        "severity_on_failure": "high",
    },
    {
        "subtype": "duplicate_amount_diff_vendor",
        "difficulty": "hard",
        "description": "Same unusual amount ($1,234.56) at 3 different vendors",
        "input": {"pattern": "cross_vendor_same_amount", "amount": 1234.56},
        "expected_rules_fired": ["duplicate_cross_vendor"],
        "severity_on_failure": "medium",
    },
    {
        "subtype": "phantom_employee_same_address",
        "difficulty": "nightmare",
        "description": "Recurring small expenses from 3 employees resolve to same address",
        "input": {"pattern": "phantom_employee", "employees": 3},
        "expected_rules_fired": ["phantom_employee"],
        "severity_on_failure": "critical",
    },
    {
        "subtype": "bank_detail_change",
        "difficulty": "nightmare",
        "description": "Vendor bank account changed between invoices — CRITICAL signal",
        "input": {"pattern": "bank_account_change"},
        "expected_rules_fired": ["bank_account_change"],
        "severity_on_failure": "critical",
    },
    {
        "subtype": "invoice_dated_after_payment",
        "difficulty": "hard",
        "description": "Invoice date is AFTER the bank payment — backdated",
        "input": {"pattern": "invoice_after_payment"},
        "expected_rules_fired": ["invoice_after_payment"],
        "severity_on_failure": "high",
    },
    {
        "subtype": "tax_reg_contradiction",
        "difficulty": "hard",
        "description": "Vendor previously unregistered now charging GST/QST",
        "input": {"pattern": "tax_registration_contradiction"},
        "expected_rules_fired": ["tax_registration_contradiction"],
        "severity_on_failure": "high",
    },
    {
        "subtype": "payee_name_diverges",
        "difficulty": "hard",
        "description": "Bank payee differs significantly from invoice vendor name",
        "input": {"pattern": "vendor_payee_mismatch"},
        "expected_rules_fired": ["vendor_payee_mismatch"],
        "severity_on_failure": "high",
    },
    {
        "subtype": "clean_no_fraud_baseline",
        "difficulty": "easy",
        "description": "Clean population — zero rules should fire",
        "input": {"pattern": "clean"},
        "expected_rules_fired": [],
        "severity_on_failure": "low",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out = []
    for spec in FRAUD_SPECS:
        out.append({
            "category": "fraud",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec["severity_on_failure"],
            "affects_engines": ["src.engines.fraud_engine"],
            "oracle": "audit",
            "input_spec": {"kind": "fraud", **spec["input"]},
            "ground_truth": {"expected_rules_fired": spec["expected_rules_fired"]},
        })
    return out
