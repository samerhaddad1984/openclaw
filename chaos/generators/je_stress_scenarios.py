"""Journal-entry stress scenarios (Sprint F+ Round 2).

Tests the double-entry engine's tolerance for extreme inputs: huge
multi-line JEs, zero-amount lines, fiscal-boundary crossings, future or
far-past dates, sub-cent precision, reversing entries, multi-currency,
same-account-both-sides, closed-period writes, sub-account hierarchies,
project dimensions, attached documents, and approval flip-flops.
"""
from __future__ import annotations

import random
from typing import Any


JE_SCENARIOS: list[dict[str, Any]] = [
    {
        "subtype": "je_100_lines_balanced",
        "difficulty": "hard",
        "description": "JE with 100 debit/credit lines — must balance to the cent",
        "line_count": 100,
        "total_debit": 123456.78,
        "total_credit": 123456.78,
        "severity": "high",
    },
    {
        "subtype": "je_zero_amount_line",
        "difficulty": "normal",
        "description": "JE with a 0.00 line — should be rejected or stripped",
        "line_count": 3,
        "has_zero_line": True,
        "severity": "medium",
    },
    {
        "subtype": "je_crosses_fiscal_year",
        "difficulty": "hard",
        "description": "JE lines dated before and after fiscal-year boundary",
        "dates": ["2024-12-31", "2025-01-02"],
        "severity": "high",
    },
    {
        "subtype": "je_future_date",
        "difficulty": "normal",
        "description": "JE dated 3 months in the future — should warn or block",
        "document_date": "2026-07-15",
        "severity": "medium",
    },
    {
        "subtype": "je_very_old_date",
        "difficulty": "normal",
        "description": "JE dated 10 years ago — should require reason / supervisor override",
        "document_date": "2016-04-01",
        "severity": "medium",
    },
    {
        "subtype": "je_4_decimal_precision",
        "difficulty": "hard",
        "description": "JE amounts with 4 decimal places (fuel, FX)",
        "amounts": [1234.5678, 9876.4321, -5000.0000, -6110.9999],
        "severity": "high",
    },
    {
        "subtype": "je_reversing_entry_auto",
        "difficulty": "hard",
        "description": "JE flagged as auto-reversing — next period must auto-generate reversal",
        "auto_reverse": True,
        "amount": 2000.00,
        "severity": "high",
    },
    {
        "subtype": "je_multi_currency",
        "difficulty": "nightmare",
        "description": "JE with lines in CAD + USD + EUR — must balance in reporting currency",
        "currencies": ["CAD", "USD", "EUR"],
        "severity": "high",
    },
    {
        "subtype": "je_same_account_both_sides",
        "difficulty": "hard",
        "description": "JE debits and credits the same GL account — nonsensical, should block",
        "gl_account": "5000",
        "amount": 500.00,
        "severity": "medium",
    },
    {
        "subtype": "je_into_closed_period",
        "difficulty": "hard",
        "description": "JE dated in a period already closed — must block or require reopen",
        "closed_period_end": "2024-12-31",
        "je_date": "2024-11-15",
        "severity": "high",
    },
    {
        "subtype": "je_sub_account_hierarchy",
        "difficulty": "normal",
        "description": "JE uses 5000.10 sub-accounts — parent balance must roll up",
        "accounts": ["5000.10", "5000.20", "5000.30"],
        "severity": "medium",
    },
    {
        "subtype": "je_project_department_dimensions",
        "difficulty": "normal",
        "description": "JE lines tagged with project + department dimensions",
        "dimensions": {"project": "P-2025-A", "department": "OPS"},
        "severity": "low",
    },
    {
        "subtype": "je_attached_document",
        "difficulty": "easy",
        "description": "JE links to supporting document; deletion cascade must not orphan",
        "attached_doc_id": "DOC-12345",
        "severity": "low",
    },
    {
        "subtype": "je_approval_flip_flop",
        "difficulty": "hard",
        "description": "JE approved → rejected → re-approved — audit trail must show all 3",
        "approval_sequence": ["approved", "rejected", "approved"],
        "severity": "high",
    },
    {
        "subtype": "je_unbalanced_single_cent",
        "difficulty": "nightmare",
        "description": "JE out of balance by $0.01 — must block posting",
        "total_debit": 1000.00,
        "total_credit": 999.99,
        "severity": "high",
    },
    {
        "subtype": "je_with_deleted_account",
        "difficulty": "hard",
        "description": "JE references a GL account that has since been deleted",
        "deleted_gl_account": "9999",
        "severity": "medium",
    },
    {
        "subtype": "je_recurring_template",
        "difficulty": "normal",
        "description": "JE based on recurring template — must auto-generate 12 monthly copies",
        "recurring": True,
        "occurrences": 12,
        "severity": "medium",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in JE_SCENARIOS:
        out.append({
            "category": "je",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec.get("severity", "medium"),
            "expected_fail": False,
            "future_feature": False,
            "affects_engines": [
                "src.engines.gl_engine",
                "src.engines.accrual_engine",
                "src.engines.multicurrency_engine",
                "src.engines.period_close",
            ],
            "oracle": "financial",
            "input_spec": {"kind": "je_synthetic", "spec": spec},
            "ground_truth": {"subtype": spec["subtype"], "expected": spec},
        })
    return out
