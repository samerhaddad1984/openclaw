from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    return normalize_text(value).casefold()


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return normalize_key(value) in {"1", "true", "yes", "y", "on"}


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
class ExceptionRouteDecision:
    action: str
    reasons: list[str]
    score: float
    evaluated_at: str
    learned_pattern_used: bool = False
    learned_pattern_summary: str = ""
    vendor_memory_match_found: bool = False
    vendor_memory_confidence: float = 0.0
    duplicate_risk_level: str = "none"
    duplicate_confirmed: bool = False
    amount_suspicious: bool = False
    document_confidence_ok: bool = False
    auto_approved: bool = False
    auto_decision: str = ""
    review_pattern_hits: int = 0
    success_pattern_hits: int = 0
    posted_pattern_hits: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ExceptionRouter:
    """
    Upgraded exception router.

    Purpose:
    - convert engine outputs + duplicate signals + raw document state
      into a final route action:
        - auto_post
        - approve_but_hold
        - review
        - block_posting

    Design rule:
    - approval engine proposes
    - exception router vetoes when there is real danger
    - no dumb contradiction unless there is a hard reason
    """

    def _extract_document_fields(self, document: dict[str, Any]) -> dict[str, Any]:
        raw_result = safe_json_loads(document.get("raw_result"))
        posting_payload = safe_json_loads(document.get("payload_json"))
        posting_payload = posting_payload or safe_json_loads(raw_result.get("posting_payload"))

        confidence = safe_float(document.get("confidence"))
        if confidence is None:
            confidence = safe_float(posting_payload.get("confidence")) or 0.0

        amount = safe_float(document.get("amount"))
        if amount is None:
            amount = safe_float(posting_payload.get("amount"))

        return {
            "document_id": normalize_text(document.get("document_id"))
            or normalize_text(posting_payload.get("document_id")),
            "vendor": normalize_text(document.get("vendor"))
            or normalize_text(posting_payload.get("vendor")),
            "client_code": normalize_text(document.get("client_code"))
            or normalize_text(posting_payload.get("client_code")),
            "doc_type": normalize_text(document.get("doc_type"))
            or normalize_text(posting_payload.get("doc_type")),
            "category": normalize_text(document.get("category"))
            or normalize_text(posting_payload.get("category")),
            "gl_account": normalize_text(document.get("gl_account"))
            or normalize_text(posting_payload.get("gl_account")),
            "tax_code": normalize_text(document.get("tax_code"))
            or normalize_text(posting_payload.get("tax_code")),
            "review_status": normalize_text(document.get("review_status"))
            or normalize_text(posting_payload.get("review_status")),
            "approval_state": normalize_text(document.get("approval_state"))
            or normalize_text(posting_payload.get("approval_state")),
            "posting_status": normalize_text(document.get("posting_status"))
            or normalize_text(posting_payload.get("posting_status")),
            "currency": normalize_text(document.get("currency"))
            or normalize_text(posting_payload.get("currency")),
            "confidence": confidence,
            "amount": amount,
        }

    def _extract_duplicate_state(self, duplicate_result: dict[str, Any] | None) -> tuple[str, bool, float]:
        if not duplicate_result:
            return "none", False, 0.0

        level = normalize_key(duplicate_result.get("risk_level"))
        if level not in {"none", "low", "medium", "high"}:
            level = "none"

        confirmed = safe_bool(duplicate_result.get("duplicate_confirmed"))
        score = safe_float(duplicate_result.get("score")) or 0.0
        return level, confirmed, score

    def _extract_auto_state(self, auto_result: dict[str, Any] | None) -> dict[str, Any]:
        auto_result = auto_result or {}
        return {
            "decision": normalize_text(auto_result.get("decision")),
            "approval_score": safe_float(auto_result.get("approval_score")) or 0.0,
            "auto_approved": safe_bool(auto_result.get("auto_approved")),
            "vendor_memory_ok": safe_bool(auto_result.get("vendor_memory_ok")),
            "document_confidence_ok": safe_bool(auto_result.get("document_confidence_ok")),
            "amount_suspicious": safe_bool(auto_result.get("amount_suspicious")),
            "duplicate_risk_level": normalize_text(auto_result.get("duplicate_risk_level")) or "none",
            "reason": normalize_text(auto_result.get("reason")),
            "learned_pattern_used": safe_bool(auto_result.get("learned_pattern_used")),
            "learned_pattern_summary": normalize_text(auto_result.get("learned_pattern_summary")),
            "vendor_memory_match_found": safe_bool(auto_result.get("vendor_memory_match_found")),
            "vendor_memory_confidence": safe_float(auto_result.get("vendor_memory_confidence")) or 0.0,
            "review_pattern_hits": int(safe_float(auto_result.get("review_pattern_hits")) or 0),
            "success_pattern_hits": int(safe_float(auto_result.get("success_pattern_hits")) or 0),
            "posted_pattern_hits": int(safe_float(auto_result.get("posted_pattern_hits")) or 0),
        }

    def _missing_required_accounting_fields(self, fields: dict[str, Any]) -> list[str]:
        missing: list[str] = []

        if not fields["vendor"]:
            missing.append("missing_vendor")
        if not fields["doc_type"]:
            missing.append("missing_doc_type")
        if not fields["category"]:
            missing.append("missing_category")
        if not fields["gl_account"]:
            missing.append("missing_gl_account")
        if not fields["tax_code"]:
            missing.append("missing_tax_code")

        return missing

    def _is_posted_already(self, fields: dict[str, Any]) -> bool:
        return normalize_key(fields["posting_status"]) == "posted"

    def _is_ready_already(self, fields: dict[str, Any]) -> bool:
        posting_status = normalize_key(fields["posting_status"])
        approval_state = normalize_key(fields["approval_state"])
        return posting_status == "ready_to_post" and approval_state == "approved_for_posting"

    def _is_waiting_in_draft(self, fields: dict[str, Any]) -> bool:
        posting_status = normalize_key(fields["posting_status"])
        approval_state = normalize_key(fields["approval_state"])
        return posting_status == "draft" and approval_state == "approved_for_posting"

    def _amount_is_missing_or_zero_for_risky_doc(self, fields: dict[str, Any], auto_state: dict[str, Any]) -> bool:
        amount = fields["amount"]
        if amount is None:
            return True
        if amount == 0:
            return True
        return bool(auto_state["amount_suspicious"])

    def _route(
        self,
        *,
        document: dict[str, Any],
        auto_result: dict[str, Any] | None = None,
        duplicate_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        fields = self._extract_document_fields(document)
        auto_state = self._extract_auto_state(auto_result)
        duplicate_risk_level, duplicate_confirmed, duplicate_score = self._extract_duplicate_state(
            duplicate_result
        )

        reasons: list[str] = []
        base_score = float(auto_state["approval_score"])

        if self._is_posted_already(fields):
            reasons.append("document_already_posted")
            return ExceptionRouteDecision(
                action="auto_post",
                reasons=reasons,
                score=1.0,
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        missing_reasons = self._missing_required_accounting_fields(fields)
        if missing_reasons:
            reasons.extend(missing_reasons)
            return ExceptionRouteDecision(
                action="review",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if duplicate_confirmed:
            reasons.append("duplicate_confirmed")
            return ExceptionRouteDecision(
                action="block_posting",
                reasons=reasons,
                score=0.0,
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level="high",
                duplicate_confirmed=True,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if not auto_state["document_confidence_ok"]:
            reasons.append("low_document_confidence")
            return ExceptionRouteDecision(
                action="review",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=False,
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if self._amount_is_missing_or_zero_for_risky_doc(fields, auto_state):
            reasons.append("missing_amount")
            return ExceptionRouteDecision(
                action="review",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=True,
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if int(auto_state["review_pattern_hits"]) >= 2 and int(auto_state["success_pattern_hits"]) == 0:
            reasons.append("learned_review_only_pattern")
            return ExceptionRouteDecision(
                action="review",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if duplicate_risk_level == "medium":
            reasons.append("duplicate_medium_risk")

            if self._is_ready_already(fields):
                reasons.append("already_ready_to_post")
                return ExceptionRouteDecision(
                    action="approve_but_hold",
                    reasons=reasons,
                    score=max(0.0, min(1.0, base_score)),
                    evaluated_at=utc_now_iso(),
                    learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                    learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                    vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                    vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                    duplicate_risk_level=duplicate_risk_level,
                    duplicate_confirmed=duplicate_confirmed,
                    amount_suspicious=bool(auto_state["amount_suspicious"]),
                    document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                    auto_approved=bool(auto_state["auto_approved"]),
                    auto_decision=str(auto_state["decision"]),
                    review_pattern_hits=int(auto_state["review_pattern_hits"]),
                    success_pattern_hits=int(auto_state["success_pattern_hits"]),
                    posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
                ).to_dict()

            if self._is_waiting_in_draft(fields):
                reasons.append("already_approved_waiting_in_draft")
                return ExceptionRouteDecision(
                    action="approve_but_hold",
                    reasons=reasons,
                    score=max(0.0, min(1.0, base_score)),
                    evaluated_at=utc_now_iso(),
                    learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                    learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                    vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                    vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                    duplicate_risk_level=duplicate_risk_level,
                    duplicate_confirmed=duplicate_confirmed,
                    amount_suspicious=bool(auto_state["amount_suspicious"]),
                    document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                    auto_approved=bool(auto_state["auto_approved"]),
                    auto_decision=str(auto_state["decision"]),
                    review_pattern_hits=int(auto_state["review_pattern_hits"]),
                    success_pattern_hits=int(auto_state["success_pattern_hits"]),
                    posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
                ).to_dict()

            if bool(auto_state["auto_approved"]):
                return ExceptionRouteDecision(
                    action="approve_but_hold",
                    reasons=reasons,
                    score=max(0.0, min(1.0, base_score)),
                    evaluated_at=utc_now_iso(),
                    learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                    learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                    vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                    vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                    duplicate_risk_level=duplicate_risk_level,
                    duplicate_confirmed=duplicate_confirmed,
                    amount_suspicious=bool(auto_state["amount_suspicious"]),
                    document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                    auto_approved=bool(auto_state["auto_approved"]),
                    auto_decision=str(auto_state["decision"]),
                    review_pattern_hits=int(auto_state["review_pattern_hits"]),
                    success_pattern_hits=int(auto_state["success_pattern_hits"]),
                    posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
                ).to_dict()

            return ExceptionRouteDecision(
                action="review",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if duplicate_risk_level == "high":
            reasons.append("duplicate_high_risk")
            return ExceptionRouteDecision(
                action="review",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        auto_decision = normalize_key(auto_state["decision"])

        if auto_decision == "auto_post":
            reasons.append("auto_approval_engine_auto_post")
            return ExceptionRouteDecision(
                action="auto_post",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        if auto_decision == "approve_but_hold":
            reasons.append("auto_approval_engine_approve_but_hold")
            return ExceptionRouteDecision(
                action="approve_but_hold",
                reasons=reasons,
                score=max(0.0, min(1.0, base_score)),
                evaluated_at=utc_now_iso(),
                learned_pattern_used=bool(auto_state["learned_pattern_used"]),
                learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
                vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
                vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
                duplicate_risk_level=duplicate_risk_level,
                duplicate_confirmed=duplicate_confirmed,
                amount_suspicious=bool(auto_state["amount_suspicious"]),
                document_confidence_ok=bool(auto_state["document_confidence_ok"]),
                auto_approved=bool(auto_state["auto_approved"]),
                auto_decision=str(auto_state["decision"]),
                review_pattern_hits=int(auto_state["review_pattern_hits"]),
                success_pattern_hits=int(auto_state["success_pattern_hits"]),
                posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
            ).to_dict()

        reasons.append("auto_approval_engine_needs_review")
        return ExceptionRouteDecision(
            action="review",
            reasons=reasons,
            score=max(0.0, min(1.0, base_score)),
            evaluated_at=utc_now_iso(),
            learned_pattern_used=bool(auto_state["learned_pattern_used"]),
            learned_pattern_summary=str(auto_state["learned_pattern_summary"]),
            vendor_memory_match_found=bool(auto_state["vendor_memory_match_found"]),
            vendor_memory_confidence=float(auto_state["vendor_memory_confidence"]),
            duplicate_risk_level=duplicate_risk_level,
            duplicate_confirmed=duplicate_confirmed,
            amount_suspicious=bool(auto_state["amount_suspicious"]),
            document_confidence_ok=bool(auto_state["document_confidence_ok"]),
            auto_approved=bool(auto_state["auto_approved"]),
            auto_decision=str(auto_state["decision"]),
            review_pattern_hits=int(auto_state["review_pattern_hits"]),
            success_pattern_hits=int(auto_state["success_pattern_hits"]),
            posted_pattern_hits=int(auto_state["posted_pattern_hits"]),
        ).to_dict()

    def route_document(
        self,
        document: dict[str, Any],
        *,
        auto_result: dict[str, Any] | None = None,
        duplicate_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._route(
            document=document,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
        )

    def route(
        self,
        document: dict[str, Any],
        *,
        auto_result: dict[str, Any] | None = None,
        duplicate_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.route_document(
            document=document,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
        )

    def evaluate(
        self,
        document: dict[str, Any],
        *,
        auto_result: dict[str, Any] | None = None,
        duplicate_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.route_document(
            document=document,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
        )

    def decide(
        self,
        document: dict[str, Any],
        *,
        auto_result: dict[str, Any] | None = None,
        duplicate_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.route_document(
            document=document,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
        )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Exception router")
    parser.add_argument("--document-json", default="")
    parser.add_argument("--auto-json", default="")
    parser.add_argument("--duplicate-json", default="")
    args = parser.parse_args()

    if not args.document_json:
        print(json.dumps({"ok": False, "error": "missing --document-json"}, indent=2, ensure_ascii=False))
        return 1

    try:
        document = json.loads(args.document_json)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"invalid document json: {exc}"}, indent=2, ensure_ascii=False))
        return 1

    auto_result: dict[str, Any] | None = None
    if args.auto_json:
        try:
            auto_result = json.loads(args.auto_json)
        except Exception as exc:
            print(json.dumps({"ok": False, "error": f"invalid auto json: {exc}"}, indent=2, ensure_ascii=False))
            return 1

    duplicate_result: dict[str, Any] | None = None
    if args.duplicate_json:
        try:
            duplicate_result = json.loads(args.duplicate_json)
        except Exception as exc:
            print(
                json.dumps(
                    {"ok": False, "error": f"invalid duplicate json: {exc}"},
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return 1

    router = ExceptionRouter()
    result = router.route_document(
        document=document,
        auto_result=auto_result,
        duplicate_result=duplicate_result,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())