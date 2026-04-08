from __future__ import annotations

import argparse
import importlib
import json
import os
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Optional ai_router integration — loaded lazily so the engine keeps working
# even if the router hasn't been set up yet.
def _load_ai_router() -> Any:
    root = str(Path(__file__).resolve().parent.parent.parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        from src.agents.core import ai_router  # type: ignore[import]
        return ai_router
    except Exception:
        return None

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    return " ".join(normalize_text(value).casefold().split())


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_key(value)
    return text in {"1", "true", "yes", "y", "on"}


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def open_db_readonly(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open the database in read-only mode.  No writes are permitted."""
    uri = db_path.as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@dataclass(slots=True)
class EscalationCase:
    document_id: str
    vendor: str
    client_code: str
    amount: float | None
    currency: str
    doc_type: str
    category: str
    gl_account: str
    tax_code: str
    confidence: float
    review_status: str
    document_date: str
    duplicate_risk: str
    duplicate_confirmed: bool
    duplicate_score: float
    duplicate_reasons: list[str] = field(default_factory=list)
    duplicate_candidate_count: int = 0
    learning_applied: bool = False
    learning_reason: str = ""
    learning_support_count: int = 0
    learning_required_support_count: int = 0
    learning_candidate_new_value: str = ""
    auto_decision: str = ""
    auto_approved: bool = False
    approval_score: float = 0.0
    exception_action: str = ""
    exception_reasons: list[str] = field(default_factory=list)
    vendor_memory_flagged_for_review: bool = False
    vendor_memory_review_reasons: list[str] = field(default_factory=list)
    source_summary: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EscalationDecision:
    decision: str
    reason: str
    confidence: float
    should_escalate: bool
    escalation_reason: str
    provider: str
    escalated_at: str
    model_output: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OpenClawEscalationEngine:
    """
    Conservative AI escalation layer.

    It does NOT replace your deterministic pipeline.
    It only activates on edge cases.

    Default escalation triggers:
    - duplicate risk == medium or high
    - document confidence < 0.95
    - learning reason == insufficient_support
    - vendor memory flagged_for_review

    If no external OpenClaw callable is configured, the engine falls back
    to a deterministic escalation decision so your pipeline does not break.
    """

    def __init__(
        self,
        *,
        openclaw_callable: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        confidence_threshold: float = 0.95,
    ) -> None:
        self.confidence_threshold = confidence_threshold
        self.openclaw_callable = openclaw_callable or self._load_callable_from_env()

    def build_case(
        self,
        *,
        document: dict[str, Any],
        duplicate_result: dict[str, Any] | None = None,
        learning_result: dict[str, Any] | None = None,
        auto_result: dict[str, Any] | None = None,
        exception_result: dict[str, Any] | None = None,
    ) -> EscalationCase:
        duplicate_result = duplicate_result or {}
        learning_result = learning_result or {}
        auto_result = auto_result or {}
        exception_result = exception_result or {}

        raw_result = document.get("raw_result") or {}
        vendor_memory = raw_result.get("vendor_memory_enrichment") or {}

        duplicate_reasons = duplicate_result.get("reasons") or []
        exception_reasons = exception_result.get("reasons") or []
        vendor_review_reasons = vendor_memory.get("review_reasons") or []

        case = EscalationCase(
            document_id=normalize_text(document.get("document_id")),
            vendor=normalize_text(document.get("vendor")),
            client_code=normalize_text(document.get("client_code")),
            amount=safe_float(document.get("amount")),
            currency=normalize_text(document.get("currency")),
            doc_type=normalize_text(document.get("doc_type")),
            category=normalize_text(document.get("category")),
            gl_account=normalize_text(document.get("gl_account")),
            tax_code=normalize_text(document.get("tax_code")),
            confidence=safe_float(document.get("confidence")) or 0.0,
            review_status=normalize_text(document.get("review_status")),
            document_date=normalize_text(document.get("document_date")),
            duplicate_risk=normalize_key(duplicate_result.get("risk_level")),
            duplicate_confirmed=safe_bool(duplicate_result.get("duplicate_confirmed")),
            duplicate_score=safe_float(duplicate_result.get("score")) or 0.0,
            duplicate_reasons=[normalize_text(x) for x in duplicate_reasons if normalize_text(x)],
            duplicate_candidate_count=len(duplicate_result.get("candidates") or []),
            learning_applied=safe_bool(learning_result.get("applied")),
            learning_reason=normalize_key(learning_result.get("reason")),
            learning_support_count=safe_int(learning_result.get("support_count")),
            learning_required_support_count=safe_int(learning_result.get("required_support_count")),
            learning_candidate_new_value=normalize_text(learning_result.get("candidate_new_value")),
            auto_decision=normalize_key(auto_result.get("decision")),
            auto_approved=safe_bool(auto_result.get("auto_approved")),
            approval_score=safe_float(auto_result.get("approval_score")) or 0.0,
            exception_action=normalize_key(exception_result.get("action")),
            exception_reasons=[normalize_text(x) for x in exception_reasons if normalize_text(x)],
            vendor_memory_flagged_for_review=safe_bool(vendor_memory.get("flagged_for_review")),
            vendor_memory_review_reasons=[
                normalize_text(x) for x in vendor_review_reasons if normalize_text(x)
            ],
            source_summary={
                "raw_vendor_source": normalize_text(raw_result.get("vendor_source")),
                "document_family": normalize_text(raw_result.get("document_family")),
                "routing_method": normalize_text(raw_result.get("routing_method")),
                "routing_score": raw_result.get("routing_score"),
            },
        )
        return case

    def should_escalate(self, case: EscalationCase) -> tuple[bool, str]:
        if case.duplicate_confirmed:
            return True, "duplicate_confirmed"

        if case.duplicate_risk in {"medium", "high"}:
            return True, f"duplicate_risk_{case.duplicate_risk}"

        if case.confidence < self.confidence_threshold:
            return True, "document_confidence_below_threshold"

        if case.learning_reason == "insufficient_support":
            return True, "learning_insufficient_support"

        if case.vendor_memory_flagged_for_review:
            return True, "vendor_memory_flagged_for_review"

        return False, "no_escalation_needed"

    def build_prompt_payload(self, case: EscalationCase) -> dict[str, Any]:
        instruction = self._load_prompt_template("escalation_decision", case)
        return {
            "instruction": instruction,
            "case": {
                "document_id": case.document_id,
                "vendor": case.vendor,
                "client": case.client_code,
                "amount": case.amount,
                "currency": case.currency,
                "doc_type": case.doc_type,
                "category": case.category,
                "gl_account": case.gl_account,
                "tax_code": case.tax_code,
                "document_date": case.document_date,
                "review_status": case.review_status,
                "confidence": case.confidence,
                "duplicate_risk": case.duplicate_risk,
                "duplicate_confirmed": case.duplicate_confirmed,
                "duplicate_score": case.duplicate_score,
                "duplicate_reasons": case.duplicate_reasons,
                "duplicate_candidate_count": case.duplicate_candidate_count,
                "learning_applied": case.learning_applied,
                "learning_reason": case.learning_reason,
                "learning_support_count": case.learning_support_count,
                "learning_required_support_count": case.learning_required_support_count,
                "learning_candidate_new_value": case.learning_candidate_new_value,
                "auto_decision": case.auto_decision,
                "auto_approved": case.auto_approved,
                "approval_score": case.approval_score,
                "exception_action": case.exception_action,
                "exception_reasons": case.exception_reasons,
                "vendor_memory_flagged_for_review": case.vendor_memory_flagged_for_review,
                "vendor_memory_review_reasons": case.vendor_memory_review_reasons,
                "source_summary": case.source_summary,
            },
        }

    def _load_prompt_template(self, template_name: str, case: EscalationCase) -> str:
        """Load a locked prompt template from src/agents/prompts/ and fill placeholders.

        Falls back to a hardcoded instruction if the file cannot be read, so the
        engine continues to work in environments where the prompts directory is missing.
        """
        _FALLBACK = (
            "You are an accounting escalation decision engine. "
            "Return strict JSON only with keys: decision, reason, confidence. "
            "Allowed decision values: post, hold, reject."
        )
        prompts_dir = Path(__file__).resolve().parent.parent / "prompts"
        template_path = prompts_dir / f"{template_name}.txt"
        try:
            template = template_path.read_text(encoding="utf-8")
        except OSError:
            return _FALLBACK

        replacements: dict[str, str] = {
            "{DOCUMENT_ID}":      normalize_text(case.document_id),
            "{VENDOR}":           normalize_text(case.vendor),
            "{CLIENT}":           normalize_text(case.client_code),
            "{AMOUNT}":           str(case.amount if case.amount is not None else ""),
            "{CURRENCY}":         normalize_text(case.currency),
            "{DOC_TYPE}":         normalize_text(case.doc_type),
            "{DOCUMENT_DATE}":    normalize_text(case.document_date),
            "{GL_ACCOUNT}":       normalize_text(case.gl_account),
            "{TAX_CODE}":         normalize_text(case.tax_code),
            "{CONFIDENCE}":       str(case.confidence),
            "{DUPLICATE_RISK}":   normalize_text(case.duplicate_risk),
            "{DUPLICATE_CONFIRMED}": str(case.duplicate_confirmed).lower(),
            "{DUPLICATE_SCORE}":  str(case.duplicate_score),
            "{LEARNING_REASON}":  normalize_text(case.learning_reason),
            "{EXCEPTION_ACTION}": normalize_text(case.exception_action),
            "{VENDOR_FLAGGED}":   str(case.vendor_memory_flagged_for_review).lower(),
        }
        for placeholder, value in replacements.items():
            template = template.replace(placeholder, value)
        return template

    def decide(
        self,
        *,
        document: dict[str, Any],
        duplicate_result: dict[str, Any] | None = None,
        learning_result: dict[str, Any] | None = None,
        auto_result: dict[str, Any] | None = None,
        exception_result: dict[str, Any] | None = None,
    ) -> EscalationDecision:
        case = self.build_case(
            document=document,
            duplicate_result=duplicate_result,
            learning_result=learning_result,
            auto_result=auto_result,
            exception_result=exception_result,
        )

        should_escalate, escalation_reason = self.should_escalate(case)

        if not should_escalate:
            return EscalationDecision(
                decision=self._default_non_escalated_decision(case),
                reason="No escalation needed. Deterministic pipeline can proceed.",
                confidence=0.99,
                should_escalate=False,
                escalation_reason=escalation_reason,
                provider="deterministic",
                escalated_at=utc_now_iso(),
                model_output={},
            )

        payload = self.build_prompt_payload(case)

        # --- Prefer an explicitly configured callable (backward-compat) ------
        if self.openclaw_callable is not None:
            try:
                raw_response = self.openclaw_callable(payload)
                parsed = self._normalize_external_response(raw_response)
                return EscalationDecision(
                    decision=parsed["decision"],
                    reason=parsed["reason"],
                    confidence=parsed["confidence"],
                    should_escalate=True,
                    escalation_reason=escalation_reason,
                    provider="openclaw",
                    escalated_at=utc_now_iso(),
                    model_output={
                        "payload": payload,
                        "raw_response": raw_response,
                    },
                )
            except Exception as exc:
                fallback = self._fallback_escalation_decision(case)
                return EscalationDecision(
                    decision=fallback["decision"],
                    reason=f"{fallback['reason']} | external_call_failed: {exc}",
                    confidence=fallback["confidence"],
                    should_escalate=True,
                    escalation_reason=escalation_reason,
                    provider="deterministic_fallback_after_error",
                    escalated_at=utc_now_iso(),
                    model_output={"payload": payload, "fallback_after_error": True},
                )

        # --- No explicit callable: try ai_router -----------------------------
        ai_router = _load_ai_router()
        if ai_router is not None:
            try:
                router_result = ai_router.call(
                    "escalation_decision",
                    payload["instruction"],
                    context=payload["case"],
                    document_id=case.document_id,
                )
                if router_result.get("result") and not router_result.get("error"):
                    parsed = self._normalize_external_response(router_result["result"])
                    return EscalationDecision(
                        decision=parsed["decision"],
                        reason=parsed["reason"],
                        confidence=parsed["confidence"],
                        should_escalate=True,
                        escalation_reason=escalation_reason,
                        provider=f"ai_router/{router_result.get('provider', 'unknown')}",
                        escalated_at=utc_now_iso(),
                        model_output={
                            "payload": payload,
                            "router_result": router_result,
                        },
                    )
            except Exception:
                pass  # fall through to deterministic fallback

        # --- Final deterministic fallback ------------------------------------
        fallback = self._fallback_escalation_decision(case)
        return EscalationDecision(
            decision=fallback["decision"],
            reason=fallback["reason"],
            confidence=fallback["confidence"],
            should_escalate=True,
            escalation_reason=escalation_reason,
            provider="deterministic_fallback",
            escalated_at=utc_now_iso(),
            model_output={"payload": payload, "fallback": True},
        )

    def evaluate_document_id(self, document_id: str) -> dict[str, Any]:
        document = self._fetch_document(document_id)
        if not document:
            return {
                "status": "error",
                "document_id": document_id,
                "error": "document_not_found",
            }

        duplicate_result = self._extract_nested_dict(document, "duplicate_result")
        learning_result = self._extract_nested_dict(document, "learning_gl_account_result")
        auto_result = self._extract_nested_dict(document, "auto_result")
        exception_result = self._extract_nested_dict(document, "exception_result")

        decision = self.decide(
            document=document,
            duplicate_result=duplicate_result,
            learning_result=learning_result,
            auto_result=auto_result,
            exception_result=exception_result,
        )

        return {
            "status": "ok",
            "document_id": document_id,
            "document": {
                "vendor": normalize_text(document.get("vendor")),
                "client_code": normalize_text(document.get("client_code")),
                "doc_type": normalize_text(document.get("doc_type")),
                "amount": safe_float(document.get("amount")),
                "currency": normalize_text(document.get("currency")),
                "document_date": normalize_text(document.get("document_date")),
                "confidence": safe_float(document.get("confidence")) or 0.0,
            },
            "escalation_decision": decision.to_dict(),
        }

    def _default_non_escalated_decision(self, case: EscalationCase) -> str:
        if case.auto_decision == "auto_post":
            return "post"
        if case.auto_decision == "approve_but_hold":
            return "hold"
        if case.exception_action == "reject":
            return "reject"
        return "hold"

    def _fallback_escalation_decision(self, case: EscalationCase) -> dict[str, Any]:
        if case.duplicate_confirmed:
            return {
                "decision": "reject",
                "reason": "Duplicate confirmed. Reject posting.",
                "confidence": 0.99,
            }

        if case.duplicate_risk in {"medium", "high"}:
            return {
                "decision": "hold",
                "reason": "Duplicate risk is not low. Hold for review.",
                "confidence": 0.95,
            }

        if case.vendor_memory_flagged_for_review:
            return {
                "decision": "hold",
                "reason": "Vendor memory flagged anomaly. Hold for review.",
                "confidence": 0.92,
            }

        if case.confidence < self.confidence_threshold:
            return {
                "decision": "hold",
                "reason": "Document confidence is below threshold. Hold for review.",
                "confidence": 0.90,
            }

        if case.auto_decision == "auto_post":
            return {
                "decision": "post",
                "reason": "Escalated but deterministic outcome still supports posting.",
                "confidence": 0.85,
            }

        if case.auto_decision == "approve_but_hold":
            return {
                "decision": "hold",
                "reason": "Deterministic pipeline already prefers approve_but_hold.",
                "confidence": 0.90,
            }

        return {
            "decision": "hold",
            "reason": "Conservative fallback. Hold for review.",
            "confidence": 0.80,
        }

    def _normalize_external_response(self, response: Any) -> dict[str, Any]:
        if isinstance(response, str):
            response = json.loads(response)

        if not isinstance(response, dict):
            raise ValueError("external_response_not_dict")

        decision = normalize_key(response.get("decision"))
        reason = normalize_text(response.get("reason"))
        confidence = safe_float(response.get("confidence"))

        if decision not in {"post", "hold", "reject"}:
            raise ValueError(f"invalid_decision:{decision}")

        if not reason:
            reason = "No reason provided."

        if confidence is None:
            confidence = 0.5

        confidence = max(0.0, min(1.0, confidence))

        return {
            "decision": decision,
            "reason": reason,
            "confidence": confidence,
        }

    def _load_callable_from_env(self) -> Callable[[dict[str, Any]], dict[str, Any]] | None:
        """
        Optional external hook.

        Set env var:
            OPENCLAW_ESCALATION_CALLABLE=module.path:function_name

        Example:
            set OPENCLAW_ESCALATION_CALLABLE=src.agents.core.my_openclaw_adapter:call_openclaw
        """
        spec = normalize_text(os.getenv("OPENCLAW_ESCALATION_CALLABLE"))
        if not spec or ":" not in spec:
            return None

        module_name, function_name = spec.split(":", 1)
        module = importlib.import_module(module_name)
        func = getattr(module, function_name)

        if not callable(func):
            raise TypeError(f"Configured callable is not callable: {spec}")

        return func

    def _fetch_document(self, document_id: str) -> dict[str, Any] | None:
        with open_db_readonly() as conn:
            has_currency = bool(conn.execute(
                "SELECT 1 FROM pragma_table_info('documents') WHERE name='currency'"
            ).fetchone())
            currency_col = "currency" if has_currency else "\'' AS currency"

            row = conn.execute(
                f"""
                SELECT
                    document_id,
                    file_name,
                    file_path,
                    client_code,
                    vendor,
                    doc_type,
                    category,
                    gl_account,
                    tax_code,
                    amount,
                    document_date,
                    review_status,
                    confidence,
                    {currency_col},
                    raw_result
                FROM documents
                WHERE document_id = ?
                """,
                (document_id,),
            ).fetchone()

        if not row:
            return None

        data = dict(row)
        raw_result = data.get("raw_result")

        if isinstance(raw_result, str) and raw_result.strip():
            try:
                data["raw_result"] = json.loads(raw_result)
            except Exception:
                data["raw_result"] = {}
        elif isinstance(raw_result, dict):
            data["raw_result"] = raw_result
        else:
            data["raw_result"] = {}

        return data

    def _extract_nested_dict(self, document: dict[str, Any], key: str) -> dict[str, Any]:
        raw_result = document.get("raw_result") or {}
        value = raw_result.get(key)

        if isinstance(value, dict):
            return value

        if isinstance(value, str) and value.strip():
            try:
                loaded = json.loads(value)
                if isinstance(loaded, dict):
                    return loaded
            except Exception:
                return {}

        return {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run OpenClaw escalation engine for one document.")
    parser.add_argument("--document-id", required=True, help="Document ID from the documents table.")
    args = parser.parse_args()

    engine = OpenClawEscalationEngine()
    result = engine.evaluate_document_id(args.document_id)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()