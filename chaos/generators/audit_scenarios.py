"""Audit / anomaly detection nightmare scenarios.

Each scenario describes a synthetic transaction population that exercises
the audit, fraud, and period-close engines. Ground truth lists what the
system SHOULD find; the AuditOracle checks false positives/negatives.
"""
from __future__ import annotations

import random
from decimal import Decimal
from typing import Any

AUDIT_SPECS: list[dict[str, Any]] = [
    # ---- duplicate-needle-in-haystack ----
    {
        "subtype": "one_duplicate_in_1000",
        "difficulty": "nightmare",
        "population": 1000,
        "expected_findings": [{"type": "duplicate_exact", "count": 1}],
        "severity_on_failure": "critical",
        "description": "1000 transactions, exactly one true duplicate — must find it",
    },
    {
        "subtype": "dollar_swap_duplicate",
        "difficulty": "nightmare",
        "population": 1000,
        "expected_findings": [{"type": "dollar_swap_duplicate", "count": 1}],
        "severity_on_failure": "high",
        "description": "1000 tx, one duplicate where digits are swapped ($123.45 vs $145.23)",
    },
    {
        "subtype": "three_duplicates_200",
        "difficulty": "hard",
        "population": 200,
        "expected_findings": [{"type": "duplicate_exact", "count": 3}],
        "severity_on_failure": "high",
        "description": "200 tx, three exact duplicates",
    },
    {
        "subtype": "cross_vendor_duplicate",
        "difficulty": "hard",
        "population": 300,
        "expected_findings": [{"type": "duplicate_cross_vendor", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Same amount at two different vendors within 7 days",
    },
    # ---- period close cutoff ----
    {
        "subtype": "period_close_50_unposted",
        "difficulty": "nightmare",
        "population": 150,
        "unposted_count": 50,
        "expected_findings": [{"type": "unposted_in_period", "count": 50}],
        "severity_on_failure": "critical",
        "description": "Period close with 50 documents unposted — must block close",
    },
    {
        "subtype": "same_day_200_receipts",
        "difficulty": "hard",
        "population": 200,
        "all_same_date": True,
        "expected_findings": [{"type": "bulk_same_date", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Client submits 200 receipts all dated same day",
    },
    {
        "subtype": "receipt_for_closed_period",
        "difficulty": "hard",
        "population": 50,
        "has_closed_period_entry": True,
        "expected_findings": [{"type": "closed_period_violation", "count": 1}],
        "severity_on_failure": "critical",
        "description": "Submission for a period already locked",
    },
    {
        "subtype": "circular_approval",
        "difficulty": "nightmare",
        "population": 10,
        "expected_findings": [{"type": "circular_approval", "count": 1}],
        "severity_on_failure": "high",
        "description": "A approves B, B approves A — segregation of duties fail",
    },
    {
        "subtype": "duplicate_invoice_number",
        "difficulty": "hard",
        "population": 100,
        "expected_findings": [{"type": "duplicate_invoice_number", "count": 1}],
        "severity_on_failure": "high",
        "description": "Same vendor invoice number used twice on different dates",
    },
    {
        "subtype": "amount_just_under_threshold",
        "difficulty": "hard",
        "population": 100,
        "expected_findings": [{"type": "threshold_structuring", "count": 4}],
        "severity_on_failure": "high",
        "description": "Four transactions at $499.99 when approval threshold is $500",
    },
    {
        "subtype": "weekend_transactions_large",
        "difficulty": "hard",
        "population": 150,
        "expected_findings": [{"type": "weekend_transaction", "count": 5}],
        "severity_on_failure": "medium",
        "description": "Five large weekend transactions in normal activity",
    },
    {
        "subtype": "late_night_approvals",
        "difficulty": "hard",
        "population": 100,
        "expected_findings": [{"type": "off_hours_approval", "count": 3}],
        "severity_on_failure": "medium",
        "description": "Three approvals between 2 AM and 4 AM",
    },
    {
        "subtype": "round_dollar_spike",
        "difficulty": "hard",
        "population": 200,
        "expected_findings": [{"type": "round_dollar_cluster", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Unusual cluster of exact $100 / $500 / $1000 round amounts",
    },
    {
        "subtype": "new_vendor_large_first",
        "difficulty": "hard",
        "population": 50,
        "expected_findings": [{"type": "new_vendor_large_amount", "count": 1}],
        "severity_on_failure": "high",
        "description": "Brand new vendor, first invoice is $9,999",
    },
    {
        "subtype": "bank_account_change",
        "difficulty": "nightmare",
        "population": 20,
        "expected_findings": [{"type": "bank_account_change", "count": 1}],
        "severity_on_failure": "critical",
        "description": "Vendor changed bank details mid-period — classic fraud pattern",
    },
    {
        "subtype": "invoice_after_payment",
        "difficulty": "hard",
        "population": 40,
        "expected_findings": [{"type": "invoice_after_payment", "count": 1}],
        "severity_on_failure": "high",
        "description": "Invoice dated AFTER matching bank payment",
    },
    {
        "subtype": "tax_registration_contradiction",
        "difficulty": "hard",
        "population": 30,
        "expected_findings": [{"type": "tax_registration_contradiction", "count": 1}],
        "severity_on_failure": "high",
        "description": "Vendor charges GST/QST but was historically unregistered",
    },
    {
        "subtype": "sequential_invoice_numbers",
        "difficulty": "hard",
        "population": 60,
        "expected_findings": [{"type": "sequential_invoice_pattern", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Sequential invoice numbers across different days — phantom vendor",
    },
    {
        "subtype": "vendor_typo_variants",
        "difficulty": "hard",
        "population": 50,
        "expected_findings": [{"type": "vendor_typo_duplicate", "count": 1}],
        "severity_on_failure": "medium",
        "description": "IGA, I.G.A., IGA Inc. — same vendor under 3 spellings",
    },
    {
        "subtype": "split_to_avoid_threshold",
        "difficulty": "nightmare",
        "population": 60,
        "expected_findings": [{"type": "split_transaction", "count": 1}],
        "severity_on_failure": "high",
        "description": "Two transactions split to stay under $5000 per-tx limit",
    },
    {
        "subtype": "phantom_employee_expense",
        "difficulty": "nightmare",
        "population": 80,
        "expected_findings": [{"type": "phantom_employee", "count": 1}],
        "severity_on_failure": "critical",
        "description": "Pattern of small recurring expenses to one address across employees",
    },
    # ---- sampling ----
    {
        "subtype": "statistical_sampling_reproducibility",
        "difficulty": "normal",
        "population": 300,
        "expected_findings": [{"type": "sample_reproducible", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Statistical sample must be reproducible given same seed",
    },
    # ---- trial balance ----
    {
        "subtype": "trial_balance_imbalance",
        "difficulty": "hard",
        "population": 50,
        "expected_findings": [{"type": "trial_balance_imbalance", "count": 1}],
        "severity_on_failure": "critical",
        "description": "Injected 1-cent TB imbalance — must surface",
    },
    # ---- clean/baseline ----
    {
        "subtype": "clean_baseline_100",
        "difficulty": "easy",
        "population": 100,
        "expected_findings": [],
        "severity_on_failure": "low",
        "description": "Clean 100-tx population — should produce zero findings",
    },
    {
        "subtype": "clean_baseline_500",
        "difficulty": "normal",
        "population": 500,
        "expected_findings": [],
        "severity_on_failure": "low",
        "description": "Clean 500-tx population — zero findings expected",
    },
    # ---- more edge cases to hit 30+ ----
    {
        "subtype": "missing_document_for_tx",
        "difficulty": "hard",
        "population": 100,
        "expected_findings": [{"type": "missing_supporting_doc", "count": 5}],
        "severity_on_failure": "high",
        "description": "5 bank transactions with no matching document",
    },
    {
        "subtype": "duplicate_with_rotated_image",
        "difficulty": "nightmare",
        "population": 40,
        "expected_findings": [{"type": "duplicate_by_fingerprint", "count": 1}],
        "severity_on_failure": "high",
        "description": "Same receipt re-uploaded after rotation — fingerprint must match",
    },
    {
        "subtype": "fuzzy_amount_within_cent",
        "difficulty": "hard",
        "population": 80,
        "expected_findings": [{"type": "duplicate_fuzzy", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Near-duplicate amounts differing by 1 cent",
    },
    {
        "subtype": "holiday_large_expenses",
        "difficulty": "hard",
        "population": 120,
        "expected_findings": [{"type": "holiday_transaction", "count": 2}],
        "severity_on_failure": "medium",
        "description": "Two large expenses on Quebec statutory holidays",
    },
    {
        "subtype": "vendor_category_shift",
        "difficulty": "hard",
        "population": 60,
        "expected_findings": [{"type": "vendor_category_shift", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Vendor historically office supplies now billing travel",
    },
    {
        "subtype": "vendor_timing_anomaly",
        "difficulty": "hard",
        "population": 60,
        "expected_findings": [{"type": "vendor_timing_anomaly", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Invoice day-of-month >14 days from vendor historical norm",
    },
    {
        "subtype": "vendor_amount_anomaly",
        "difficulty": "hard",
        "population": 60,
        "expected_findings": [{"type": "vendor_amount_anomaly", "count": 1}],
        "severity_on_failure": "medium",
        "description": "Amount > 2σ from vendor historical mean",
    },
    {
        "subtype": "payee_name_mismatch",
        "difficulty": "hard",
        "population": 50,
        "expected_findings": [{"type": "vendor_payee_mismatch", "count": 1}],
        "severity_on_failure": "high",
        "description": "Bank payee differs significantly from invoice vendor name",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in AUDIT_SPECS:
        out.append({
            "category":    "audit",
            "subtype":     spec["subtype"],
            "difficulty":  spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec["severity_on_failure"],
            "affects_engines": [
                "src.engines.audit_engine",
                "src.engines.fraud_engine",
                "src.engines.aging_engine",
            ],
            "oracle": "audit",
            "input_spec": {
                "kind":                "transactions",
                "population":          spec.get("population", 100),
                "all_same_date":       spec.get("all_same_date", False),
                "unposted_count":      spec.get("unposted_count", 0),
                "has_closed_period_entry": spec.get("has_closed_period_entry", False),
            },
            "ground_truth": {"expected_findings": spec.get("expected_findings", [])},
        })
    return out
