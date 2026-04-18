"""Bank reconciliation nightmare scenarios."""
from __future__ import annotations

import random
from typing import Any


RECON_SPECS: list[dict[str, Any]] = [
    {
        "subtype": "one_to_many_split_payment",
        "difficulty": "nightmare",
        "description": "One bank transaction covers 3 invoices — must split-match",
        "input": {"bank_amount": "1500.00", "invoices": ["500.00", "500.00", "500.00"]},
        "expected": {"match_type": "one_to_many", "matched_count": 3},
        "severity_on_failure": "high",
    },
    {
        "subtype": "many_to_one_combined_deposit",
        "difficulty": "nightmare",
        "description": "3 customer payments deposited as one bank line",
        "input": {"bank_amount": "1500.00", "customer_payments": ["500.00", "500.00", "500.00"]},
        "expected": {"match_type": "many_to_one", "matched_count": 3},
        "severity_on_failure": "high",
    },
    {
        "subtype": "timing_difference_3_days",
        "difficulty": "hard",
        "description": "Invoice date vs bank clearing 3 days apart (within 7-day prod tolerance)",
        "input": {"invoice_date": "2026-03-28", "bank_date": "2026-03-31"},
        # Production BankMatcher default is 7 calendar days (≈5 business days).
        "expected": {"match_found": True, "tolerance_days": 7},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "bank_fee_not_in_gl",
        "difficulty": "hard",
        "description": "Bank service charge has no matching GL entry — must surface",
        "input": {"bank_fee": "25.00"},
        "expected": {"orphan_bank_entry": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "gl_entry_no_bank_match",
        "difficulty": "hard",
        "description": "Outstanding cheque: GL entry, no bank clearance yet",
        "input": {"cheque_amount": "850.00", "days_outstanding": 15},
        "expected": {"outstanding_cheque": True},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "reversed_transaction",
        "difficulty": "hard",
        "description": "Debit + reversal credit — should net to zero and reconcile",
        "input": {"debit": "500.00", "credit_reversal": "500.00"},
        "expected": {"net_zero": True, "matched": True},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "fx_difference_on_usd_payment",
        "difficulty": "nightmare",
        "description": "USD invoice $1000 → CAD bank deposit with FX loss",
        "input": {"usd_amount": "1000.00", "invoice_fx": "1.35", "settlement_fx": "1.37"},
        "expected": {"fx_difference_booked": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "duplicate_bank_line",
        "difficulty": "hard",
        "description": "Bank feed replayed a line — dedup on fingerprint",
        "input": {"duplicate_count": 1},
        "expected": {"duplicate_detected": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "negative_amount_refund",
        "difficulty": "normal",
        "description": "Refund from vendor appears as negative bank line",
        "input": {"refund_amount": "-150.00"},
        "expected": {"recorded_as_refund": True},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "zero_amount_memo_line",
        "difficulty": "normal",
        "description": "Zero-amount memo line (monthly statement header)",
        "input": {"amount": "0.00"},
        "expected": {"ignored": True},
        "severity_on_failure": "low",
    },
    {
        "subtype": "wire_fee_combined_with_payment",
        "difficulty": "hard",
        "description": "Wire fee bundled with principal in bank line",
        "input": {"total_debit": "10025.00", "invoice": "10000.00", "wire_fee": "25.00"},
        "expected": {"split_match": True, "fee_recognized": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "nsf_returned_cheque",
        "difficulty": "hard",
        "description": "NSF cheque — original + reversal + NSF fee",
        "input": {"cheque": "500.00", "nsf_fee": "45.00"},
        "expected": {"nsf_detected": True, "fee_recorded": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "credit_card_settlement_lag",
        "difficulty": "hard",
        "description": "Credit card batch settles 2 days after invoice, net of processing fee",
        "input": {"gross": "1000.00", "fee_pct": 0.029, "net": "971.00", "lag_days": 2},
        "expected": {"fee_recognized": True, "lag_within_tolerance": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "rounding_difference_1_cent",
        "difficulty": "normal",
        "description": "1¢ rounding difference — within tolerance, auto-adjust",
        "input": {"delta": "0.01"},
        "expected": {"auto_adjusted": True},
        "severity_on_failure": "low",
    },
    {
        "subtype": "rounding_difference_50_cent",
        "difficulty": "hard",
        "description": "50¢ discrepancy — must NOT auto-close, human review",
        "input": {"delta": "0.50"},
        "expected": {"auto_adjusted": False, "flagged": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "wrong_period_bank_line",
        "difficulty": "hard",
        "description": "Bank line dated in closed period — must not post",
        "input": {"bank_date": "2025-12-15", "close_date": "2025-12-31"},
        "expected": {"rejected_closed_period": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "100_line_statement",
        "difficulty": "hard",
        "description": "100-line bank statement — performance + correctness",
        "input": {"line_count": 100},
        "expected": {"performance_seconds_max": 10.0},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "bank_statement_pdf_parse_multi_page",
        "difficulty": "nightmare",
        "description": "Multi-page PDF bank statement — page breaks split transactions",
        "input": {"pages": 5, "break_within_tx": True},
        "expected": {"all_tx_parsed": True, "no_duplicates": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "cad_and_usd_mixed_statement",
        "difficulty": "nightmare",
        "description": "Single bank statement with CAD and USD lines interleaved",
        "input": {"cad_count": 50, "usd_count": 20},
        "expected": {"currency_assignment_correct": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "finalized_recon_mutation_blocked",
        "difficulty": "normal",
        "description": "Finalized reconciliation — further edits rejected",
        "input": {"status": "finalized"},
        "expected": {"mutation_blocked": True},
        "severity_on_failure": "critical",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out = []
    for spec in RECON_SPECS:
        out.append({
            "category": "recon",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec["severity_on_failure"],
            "affects_engines": [
                "src.engines.reconciliation_engine",
                "src.engines.reconciliation_validator",
                "src.engines.bank_parser",
            ],
            "oracle": "recon",
            "input_spec": {"kind": "recon", **spec["input"]},
            "ground_truth": spec["expected"],
        })
    return out
