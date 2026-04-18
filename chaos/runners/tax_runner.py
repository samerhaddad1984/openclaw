"""Runner for tax engine scenarios."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec


class TaxRunner:
    track = "tax"

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        try:
            from src.engines.tax_engine import (  # type: ignore
                calculate_gst_qst,
                calculate_itc_itr,
                TAX_CODE_REGISTRY,
            )
        except Exception as e:
            result.output = {"skipped": True, "reason": f"tax_engine import failed: {e}"}
            return

        spec = scenario.get("input_spec") or {}
        amount = spec.get("amount")
        tax_code = spec.get("tax_code", "T")
        computed: dict[str, Any] = {}

        try:
            if amount is not None:
                gq = calculate_gst_qst(Decimal(str(amount)))
                computed["gst"] = str(gq["gst"])
                computed["qst"] = str(gq["qst"])
                computed["total"] = str(gq["total_with_tax"])
                computed["gst_rate"] = str(gq["gst_rate"])
                computed["qst_rate"] = str(gq["qst_rate"])

            # ITC/ITR calc if applicable
            if amount is not None and tax_code in TAX_CODE_REGISTRY:
                try:
                    itc = calculate_itc_itr(
                        amount_before_tax=Decimal(str(amount)),
                        tax_code=tax_code,
                    )
                    # Extract common values
                    if isinstance(itc, dict):
                        if "itc" in itc:
                            computed["itc"] = str(itc.get("itc"))
                        if "itr" in itc:
                            computed["itr"] = str(itc.get("itr"))
                        entry = TAX_CODE_REGISTRY.get(tax_code, {})
                        computed["itc_pct"] = str(entry.get("itc_pct", ""))
                        computed["itr_pct"] = str(entry.get("itr_pct", ""))
                except Exception as e:
                    computed["itc_error"] = str(e)

            # Rate assertions pulled from registry
            entry = TAX_CODE_REGISTRY.get(tax_code, {})
            if "hst_rate" in entry:
                computed["hst_rate"] = str(entry.get("hst_rate"))
            if tax_code == "E":
                computed["gst"] = computed.get("gst") or "0.00"
                computed["qst"] = computed.get("qst") or "0.00"
        except Exception as e:
            computed["error"] = str(e)

        # Minimum-tax expectation
        expected = scenario.get("ground_truth") or {}
        if "gst_min" in expected:
            computed.setdefault("gst_min", computed.get("gst"))
        if "qst_min" in expected:
            computed.setdefault("qst_min", computed.get("qst"))
        if "precision_to_cent" in expected:
            gst = computed.get("gst") or "0"
            computed["precision_to_cent"] = "." in gst and len(gst.rsplit(".", 1)[-1]) == 2

        oracle = get_oracle("tax")
        oracle_result = oracle.validate(computed, expected)

        result.output = {"computed": computed, "spec": spec}
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed
