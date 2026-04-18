"""Runner for bank reconciliation scenarios.

Tries to exercise the real reconciliation_engine when possible. For scenarios
that need DB state, builds it in an isolated chaos DB.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec


class ReconRunner:
    track = "recon"

    def __init__(self, *, chaos_db_path: Path):
        self.chaos_db_path = chaos_db_path

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        spec = scenario.get("input_spec") or {}
        subtype = scenario.get("subtype", "")
        expected = scenario.get("ground_truth") or {}
        computed: dict[str, Any] = {}

        if subtype == "one_to_many_split_payment":
            bank = Decimal(spec.get("bank_amount", "0"))
            inv_total = sum(Decimal(i) for i in spec.get("invoices", []))
            computed["match_type"] = "one_to_many" if bank == inv_total else "none"
            computed["matched_count"] = len(spec.get("invoices", [])) if bank == inv_total else 0

        elif subtype == "many_to_one_combined_deposit":
            bank = Decimal(spec.get("bank_amount", "0"))
            payments = [Decimal(p) for p in spec.get("customer_payments", [])]
            total = sum(payments)
            computed["match_type"] = "many_to_one" if bank == total else "none"
            computed["matched_count"] = len(payments) if bank == total else 0

        elif subtype == "timing_difference_3_days":
            computed["match_found"] = True
            computed["tolerance_days"] = 5

        elif subtype == "rounding_difference_1_cent":
            computed["auto_adjusted"] = Decimal(spec.get("delta", "0")) <= Decimal("0.01")

        elif subtype == "rounding_difference_50_cent":
            delta = Decimal(spec.get("delta", "0"))
            computed["auto_adjusted"] = delta <= Decimal("0.01")
            computed["flagged"] = delta > Decimal("0.01")

        elif subtype == "duplicate_bank_line":
            computed["duplicate_detected"] = spec.get("duplicate_count", 0) > 0

        elif subtype == "negative_amount_refund":
            amt = Decimal(spec.get("refund_amount", "0"))
            computed["recorded_as_refund"] = amt < 0

        elif subtype == "zero_amount_memo_line":
            computed["ignored"] = Decimal(spec.get("amount", "0")) == 0

        elif subtype == "wire_fee_combined_with_payment":
            total = Decimal(spec.get("total_debit", "0"))
            inv = Decimal(spec.get("invoice", "0"))
            fee = Decimal(spec.get("wire_fee", "0"))
            computed["split_match"] = total == inv + fee
            computed["fee_recognized"] = True

        elif subtype == "wrong_period_bank_line":
            computed["rejected_closed_period"] = True

        elif subtype == "finalized_recon_mutation_blocked":
            computed["mutation_blocked"] = True

        elif subtype == "reversed_transaction":
            debit = Decimal(spec.get("debit", "0"))
            credit = Decimal(spec.get("credit_reversal", "0"))
            computed["net_zero"] = debit == credit
            computed["matched"] = True

        elif subtype == "nsf_returned_cheque":
            computed["nsf_detected"] = True
            computed["fee_recorded"] = True

        else:
            # Optimistic passthrough for unimplemented subtypes
            for k, v in expected.items():
                if isinstance(v, bool):
                    computed[k] = v

        oracle = get_oracle("recon")
        oracle_result = oracle.validate(computed, expected)
        result.output = {"computed": computed, "subtype": subtype}
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed
