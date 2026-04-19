"""Reconciliation stress scenarios (Sprint F+ Round 2).

Pushes the bank-reconciliation engine with pathological cases: huge
volume matching, bank-only transactions (fees, interest), duplicate bank
entries, wrong-period postings, FX mismatches, stop payments, NSF,
fraudulent withdrawals, garbage wire memos, recurring loan payments,
internal transfers, and bank-error scenarios.
"""
from __future__ import annotations

import random
from typing import Any


RECON_STRESS_SCENARIOS: list[dict[str, Any]] = [
    {
        "subtype": "recon_10000_tx_match_9800_docs",
        "difficulty": "nightmare",
        "description": "10,000 bank transactions, 9,800 documents — 200 unmatched expected",
        "bank_count": 10000,
        "doc_count": 9800,
        "severity": "high",
    },
    {
        "subtype": "recon_bank_fees_no_doc",
        "difficulty": "normal",
        "description": "Monthly bank fee — no matching document, must auto-post to 5900",
        "fee_amount": 12.50,
        "expected_gl": "5900",
        "severity": "medium",
    },
    {
        "subtype": "recon_duplicate_bank_entries",
        "difficulty": "hard",
        "description": "Bank file has same tx appearing twice (bank error) — must dedupe",
        "duplicate_count": 5,
        "severity": "high",
    },
    {
        "subtype": "recon_doc_wrong_period",
        "difficulty": "hard",
        "description": "Doc dated 2025-03-30 but posted to April; bank shows March",
        "doc_date": "2025-03-30",
        "posting_period": "2025-04",
        "bank_date": "2025-03-30",
        "severity": "high",
    },
    {
        "subtype": "recon_currency_mismatch",
        "difficulty": "hard",
        "description": "Bank shows CAD $1350, doc is USD $1000 — must apply FX to match",
        "bank_amount": 1350.00,
        "bank_currency": "CAD",
        "doc_amount": 1000.00,
        "doc_currency": "USD",
        "fx_rate": 1.35,
        "severity": "high",
    },
    {
        "subtype": "recon_stop_payment",
        "difficulty": "hard",
        "description": "Check written, stop-payment issued — bank + doc must cancel out",
        "amount": 500.00,
        "stop_payment": True,
        "severity": "medium",
    },
    {
        "subtype": "recon_nsf_returned_check",
        "difficulty": "hard",
        "description": "Deposit bounced (NSF) — must reverse prior deposit",
        "original_amount": 2500.00,
        "nsf_fee": 45.00,
        "severity": "high",
    },
    {
        "subtype": "recon_fraudulent_withdrawal",
        "difficulty": "nightmare",
        "description": "Bank shows withdrawal of $3000, no matching doc — fraud flag expected",
        "amount": 3000.00,
        "expected_fraud_flag": True,
        "severity": "high",
    },
    {
        "subtype": "recon_wire_memo_garbage",
        "difficulty": "hard",
        "description": "Wire transfer memo is 'REF//123//PMT//ABC'; must still match by amount+date",
        "amount": 5000.00,
        "memo": "REF//123//PMT//ABC//XYZ//WIRE//2025",
        "severity": "medium",
    },
    {
        "subtype": "recon_interest_credit",
        "difficulty": "normal",
        "description": "Bank interest credit — no doc, must auto-post to 4800",
        "amount": 15.23,
        "expected_gl": "4800",
        "severity": "low",
    },
    {
        "subtype": "recon_automatic_loan_payments",
        "difficulty": "normal",
        "description": "Monthly loan payment auto-debited — principal + interest split expected",
        "principal": 800.00,
        "interest": 200.00,
        "monthly_occurrences": 12,
        "severity": "medium",
    },
    {
        "subtype": "recon_internal_transfer",
        "difficulty": "normal",
        "description": "Internal transfer between two client accounts — must net to zero",
        "amount": 5000.00,
        "from_account": "1010",
        "to_account": "1020",
        "severity": "medium",
    },
    {
        "subtype": "recon_partial_payment_applied",
        "difficulty": "hard",
        "description": "Partial payment on invoice — must reduce AR balance, not close",
        "invoice_total": 1000.00,
        "payment_amount": 400.00,
        "expected_ar_balance": 600.00,
        "severity": "high",
    },
    {
        "subtype": "recon_bank_error_correction",
        "difficulty": "hard",
        "description": "Bank posts wrong amount, corrects next day — must track both",
        "wrong_amount": 1234.56,
        "corrected_amount": 1234.00,
        "severity": "medium",
    },
    {
        "subtype": "recon_weekend_batch_processing",
        "difficulty": "normal",
        "description": "Bank batches weekend tx on Monday — doc dates vs bank dates diverge",
        "doc_date": "2025-03-15",  # Saturday
        "bank_post_date": "2025-03-17",  # Monday
        "severity": "low",
    },
    {
        "subtype": "recon_credit_card_merchant_fees",
        "difficulty": "normal",
        "description": "Gross deposit split: sales + merchant fee + chargebacks",
        "gross_sales": 10000.00,
        "merchant_fee": 300.00,
        "chargebacks": 50.00,
        "net_deposit": 9650.00,
        "severity": "medium",
    },
    {
        "subtype": "recon_e_transfer_deposit",
        "difficulty": "normal",
        "description": "Interac e-Transfer deposit — memo is often empty",
        "amount": 250.00,
        "memo": "",
        "severity": "low",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in RECON_STRESS_SCENARIOS:
        out.append({
            "category": "recon",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec.get("severity", "medium"),
            "expected_fail": False,
            "future_feature": False,
            "affects_engines": [
                "src.engines.reconciliation_engine",
                "src.engines.bank_match_tolerance",
                "src.engines.bank_parser",
                "src.engines.multicurrency_engine",
            ],
            "oracle": "recon",
            "input_spec": {"kind": "recon_synthetic", "spec": spec},
            "ground_truth": {"subtype": spec["subtype"], "expected": spec},
        })
    return out
