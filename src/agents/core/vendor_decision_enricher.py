from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_amount(value: Any) -> float | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return round(float(text.replace(",", "")), 2)
    except Exception:
        return None


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


@dataclass
class EnrichmentDecision:
    field_name: str
    old_value: str
    new_value: str
    applied: bool
    support: int
    confidence: float
    source: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EnrichmentResult:
    enriched_document: dict[str, Any]
    applied_count: int
    flagged_for_review: bool
    review_reasons: list[str]
    decisions: list[dict[str, Any]]
    amount_analysis: dict[str, Any]
    vendor_memory_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class VendorDecisionEnricher:
    """
    Conservative enrichment layer that uses vendor memory to:
    - suggest/fill stable fields
    - flag suspicious amounts
    - explain exactly what happened

    This is intentionally strict.
    It should enrich documents, not freestyle accounting.
    """

    SAFE_FIELDS = ("client_code", "gl_account", "tax_code", "category")

    def __init__(
        self,
        *,
        db_path: Path = DB_PATH,
        min_support: int = 2,
        min_confidence: float = 0.75,
        apply_only_if_blank: bool = True,
        suspicious_multiplier: float = 3.0,
    ):
        self.db_path = db_path
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.apply_only_if_blank = apply_only_if_blank
        self.suspicious_multiplier = suspicious_multiplier

    def _engine(self):
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine

        return VendorMemoryEngine(db_path=self.db_path)

    def _should_apply(
        self,
        *,
        field_name: str,
        current_value: str,
        suggested_value: str,
        support: int,
        confidence: float,
    ) -> tuple[bool, str]:
        if field_name not in self.SAFE_FIELDS:
            return False, "field_not_allowed"

        if not suggested_value:
            return False, "empty_suggestion"

        if support < self.min_support:
            return False, "support_below_threshold"

        if confidence < self.min_confidence:
            return False, "confidence_below_threshold"

        if self.apply_only_if_blank and current_value:
            return False, "current_value_already_present"

        if current_value and current_value == suggested_value:
            return False, "already_matches_suggestion"

        return True, "applied"

    def enrich_document(
        self,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        working = dict(document)

        vendor = normalize_text(working.get("vendor"))
        client_code = normalize_text(working.get("client_code"))
        doc_type = normalize_text(working.get("doc_type"))
        amount = normalize_amount(working.get("amount"))

        engine = self._engine()

        suggestions = engine.suggest_fields_for_document(
            vendor=vendor,
            client_code=client_code,
            doc_type=doc_type,
            min_support=self.min_support,
            min_confidence=self.min_confidence,
        )

        amount_analysis = engine.analyze_amount_for_document(
            vendor=vendor,
            amount=amount,
            client_code=client_code,
            doc_type=doc_type,
            suspicious_multiplier=self.suspicious_multiplier,
        )

        decisions: list[dict[str, Any]] = []
        applied_count = 0
        review_reasons: list[str] = []
        flagged_for_review = False

        for field_name in self.SAFE_FIELDS:
            current_value = normalize_text(working.get(field_name))
            suggestion = suggestions.get(field_name)

            if not suggestion:
                decisions.append(
                    EnrichmentDecision(
                        field_name=field_name,
                        old_value=current_value,
                        new_value=current_value,
                        applied=False,
                        support=0,
                        confidence=0.0,
                        source="none",
                        reason="no_suggestion",
                    ).to_dict()
                )
                continue

            suggested_value = normalize_text(suggestion.get("suggested_value"))
            support = int(suggestion.get("support", 0) or 0)
            confidence = float(suggestion.get("confidence", 0.0) or 0.0)
            source = normalize_text(suggestion.get("source"))

            apply, reason = self._should_apply(
                field_name=field_name,
                current_value=current_value,
                suggested_value=suggested_value,
                support=support,
                confidence=confidence,
            )

            if apply:
                working[field_name] = suggested_value
                applied_count += 1
                new_value = suggested_value
            else:
                new_value = current_value

            decisions.append(
                EnrichmentDecision(
                    field_name=field_name,
                    old_value=current_value,
                    new_value=new_value,
                    applied=apply,
                    support=support,
                    confidence=confidence,
                    source=source,
                    reason=reason,
                ).to_dict()
            )

        suspicious = bool(amount_analysis.get("suspicious"))
        if suspicious:
            flagged_for_review = True
            review_reasons.append(
                f"vendor_amount_anomaly:{normalize_text(amount_analysis.get('reason'))}"
            )

        if not vendor:
            flagged_for_review = True
            review_reasons.append("missing_vendor")

        missing_critical = []
        for field_name in ["document_date", "amount"]:
            if not normalize_text(working.get(field_name)):
                missing_critical.append(field_name)

        if missing_critical:
            flagged_for_review = True
            review_reasons.append("missing_critical_fields:" + ",".join(missing_critical))

        raw_result = safe_json_loads(working.get("raw_result"))
        raw_result["vendor_memory_enrichment"] = {
            "applied_count": applied_count,
            "flagged_for_review": flagged_for_review,
            "review_reasons": review_reasons,
            "decisions": decisions,
            "amount_analysis": amount_analysis,
        }
        working["raw_result"] = raw_result

        if flagged_for_review:
            if not normalize_text(working.get("review_status")) or normalize_text(working.get("review_status")) == "Ready":
                working["review_status"] = "NeedsReview"

        result = EnrichmentResult(
            enriched_document=working,
            applied_count=applied_count,
            flagged_for_review=flagged_for_review,
            review_reasons=review_reasons,
            decisions=decisions,
            amount_analysis=amount_analysis,
            vendor_memory_summary=suggestions,
        )

        return result.to_dict()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA vendor decision enricher")
    parser.add_argument("--vendor", required=True)
    parser.add_argument("--client-code", default="")
    parser.add_argument("--doc-type", default="")
    parser.add_argument("--amount", default="")
    parser.add_argument("--document-date", default="")
    parser.add_argument("--gl-account", default="")
    parser.add_argument("--tax-code", default="")
    parser.add_argument("--category", default="")
    parser.add_argument("--review-status", default="")

    args = parser.parse_args()

    document = {
        "vendor": args.vendor,
        "client_code": args.client_code,
        "doc_type": args.doc_type,
        "amount": args.amount,
        "document_date": args.document_date,
        "gl_account": args.gl_account,
        "tax_code": args.tax_code,
        "category": args.category,
        "review_status": args.review_status,
        "raw_result": {},
    }

    enricher = VendorDecisionEnricher()
    result = enricher.enrich_document(document)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())