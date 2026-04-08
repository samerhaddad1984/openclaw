from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

from src.agents.core.openclaw_otocpa_bridge import process_document as _bridge_process_document


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    return " ".join(normalize_text(value).lower().split())


class OpenClawOtoCPASkill:
    """
    Clean OpenClaw-facing wrapper around the OtoCPA bridge.

    Purpose:
    - call the bridge
    - return a compact skill-friendly response
    - optionally include the full debug payload
    """

    def __init__(self, *, db_path: Path = DB_PATH) -> None:
        self.db_path = Path(db_path)

    def _build_summary(self, bridge_result: dict[str, Any]) -> dict[str, Any]:
        original_document = bridge_result.get("original_document") or {}
        learning_result = bridge_result.get("learning_gl_account_result") or {}
        duplicate_result = bridge_result.get("duplicate_result") or {}
        auto_result = bridge_result.get("auto_result") or {}
        exception_result = bridge_result.get("exception_result") or {}
        orchestrator_result = bridge_result.get("orchestrator_result") or {}
        final_values = (
            orchestrator_result.get("final_document_values")
            or bridge_result.get("final_document_values")
            or {}
        )
        posting_before = bridge_result.get("posting_snapshot_before") or {}
        posting_after = bridge_result.get("posting_snapshot_after") or {}

        document_id = normalize_text(bridge_result.get("document_id"))
        vendor = normalize_text(final_values.get("vendor") or original_document.get("vendor"))
        client_code = normalize_text(
            final_values.get("client_code") or original_document.get("client_code")
        )
        doc_type = normalize_text(final_values.get("doc_type") or original_document.get("doc_type"))
        amount = final_values.get("amount", original_document.get("amount"))
        currency = normalize_text(original_document.get("currency"))
        confidence = final_values.get("confidence", original_document.get("confidence"))
        document_date = normalize_text(
            original_document.get("document_date") or final_values.get("document_date")
        )

        final_action = normalize_text(bridge_result.get("final_action"))
        final_reason = normalize_text(bridge_result.get("final_reason"))

        auto_decision = normalize_text(auto_result.get("decision"))
        exception_action = normalize_text(exception_result.get("action"))
        orchestrator_next_step = normalize_text(orchestrator_result.get("next_step"))
        orchestrator_status = normalize_text(orchestrator_result.get("status"))

        duplicate_risk = normalize_text(duplicate_result.get("risk_level")) or "none"
        duplicate_confirmed = bool(duplicate_result.get("duplicate_confirmed"))

        learning_applied = bool(learning_result.get("applied"))
        learned_gl_old = normalize_text(learning_result.get("old_value"))
        learned_gl_new = normalize_text(learning_result.get("new_value"))
        learned_gl_reason = normalize_text(learning_result.get("reason"))

        gl_account = normalize_text(
            final_values.get("gl_account") or original_document.get("gl_account")
        )
        tax_code = normalize_text(final_values.get("tax_code") or original_document.get("tax_code"))

        posting_status_before = normalize_text(posting_before.get("posting_status"))
        posting_status_after = normalize_text(posting_after.get("posting_status"))
        approval_state_before = normalize_text(posting_before.get("approval_state"))
        approval_state_after = normalize_text(posting_after.get("approval_state"))

        user_message = self._build_user_message(
            document_id=document_id,
            vendor=vendor,
            client_code=client_code,
            amount=amount,
            currency=currency,
            auto_decision=auto_decision,
            duplicate_risk=duplicate_risk,
            duplicate_confirmed=duplicate_confirmed,
            learning_applied=learning_applied,
            learned_gl_old=learned_gl_old,
            learned_gl_new=learned_gl_new,
            final_action=final_action,
            final_reason=final_reason,
            posting_status_after=posting_status_after,
        )

        return {
            "skill": "openclaw_otocpa_skill",
            "document_id": document_id,
            "status": "ok",
            "message": user_message,
            "document": {
                "vendor": vendor,
                "client_code": client_code,
                "doc_type": doc_type,
                "amount": amount,
                "currency": currency,
                "document_date": document_date,
                "confidence": confidence,
            },
            "accounting": {
                "gl_account": gl_account,
                "tax_code": tax_code,
                "learning_applied": learning_applied,
                "learning_reason": learned_gl_reason,
                "gl_account_before_learning": learned_gl_old or gl_account,
                "gl_account_after_learning": learned_gl_new or gl_account,
            },
            "decision": {
                "auto_decision": auto_decision,
                "auto_approved": bool(auto_result.get("auto_approved")),
                "approval_score": auto_result.get("approval_score"),
                "exception_action": exception_action,
                "orchestrator_next_step": orchestrator_next_step,
                "orchestrator_status": orchestrator_status,
                "final_action": final_action,
                "final_reason": final_reason,
            },
            "duplicate": {
                "risk_level": duplicate_risk,
                "duplicate_confirmed": duplicate_confirmed,
                "score": duplicate_result.get("score"),
                "candidate_count": len(duplicate_result.get("candidates") or []),
                "top_reasons": list((duplicate_result.get("reasons") or [])[:5]),
            },
            "posting": {
                "posting_exists": bool(posting_after.get("exists")),
                "posting_id": normalize_text(posting_after.get("posting_id")),
                "posting_status_before": posting_status_before,
                "posting_status_after": posting_status_after,
                "approval_state_before": approval_state_before,
                "approval_state_after": approval_state_after,
                "external_id": normalize_text(posting_after.get("external_id")),
            },
        }

    def _build_user_message(
        self,
        *,
        document_id: str,
        vendor: str,
        client_code: str,
        amount: Any,
        currency: str,
        auto_decision: str,
        duplicate_risk: str,
        duplicate_confirmed: bool,
        learning_applied: bool,
        learned_gl_old: str,
        learned_gl_new: str,
        final_action: str,
        final_reason: str,
        posting_status_after: str,
    ) -> str:
        parts: list[str] = []

        header = f"Document {document_id}"
        if vendor:
            header += f" from {vendor}"
        if client_code:
            header += f" for {client_code}"
        if amount not in (None, ""):
            header += f" amount {amount}"
            if currency:
                header += f" {currency}"
        parts.append(header + ".")

        if learning_applied and learned_gl_old and learned_gl_new:
            parts.append(
                f"Learned GL correction applied: '{learned_gl_old}' -> '{learned_gl_new}'."
            )

        if duplicate_confirmed:
            parts.append("Duplicate confirmed.")
        else:
            parts.append(f"Duplicate risk is {duplicate_risk or 'none'}.")

        if auto_decision:
            parts.append(f"Auto decision: {auto_decision}.")

        if final_action:
            parts.append(f"Final action: {final_action}.")
        if final_reason:
            parts.append(f"Reason: {final_reason}.")

        if posting_status_after:
            parts.append(f"Posting status is now {posting_status_after}.")

        return " ".join(parts)

    def analyze_document(
        self,
        *,
        document_id: str,
        execute: bool = False,
        include_debug: bool = False,
    ) -> dict[str, Any]:
        bridge_result = _bridge_process_document(
            document_id=document_id,
            execute=execute,
        )

        response = self._build_summary(bridge_result)

        response["execute"] = bool(execute)
        response["debug_included"] = bool(include_debug)

        if include_debug:
            response["debug"] = bridge_result

        return response

    def explain_document(
        self,
        *,
        document_id: str,
        include_debug: bool = False,
    ) -> dict[str, Any]:
        result = self.analyze_document(
            document_id=document_id,
            execute=False,
            include_debug=include_debug,
        )

        decision = result.get("decision") or {}
        duplicate = result.get("duplicate") or {}
        accounting = result.get("accounting") or {}
        posting = result.get("posting") or {}

        explanation = {
            "document_id": result.get("document_id"),
            "status": "ok",
            "explanation": {
                "auto_decision": decision.get("auto_decision"),
                "exception_action": decision.get("exception_action"),
                "orchestrator_next_step": decision.get("orchestrator_next_step"),
                "final_action": decision.get("final_action"),
                "final_reason": decision.get("final_reason"),
                "duplicate_risk_level": duplicate.get("risk_level"),
                "duplicate_top_reasons": duplicate.get("top_reasons"),
                "learning_applied": accounting.get("learning_applied"),
                "gl_account_after_learning": accounting.get("gl_account_after_learning"),
                "tax_code": accounting.get("tax_code"),
                "posting_status_after": posting.get("posting_status_after"),
            },
            "message": result.get("message"),
        }

        if include_debug:
            explanation["debug"] = result.get("debug")

        return explanation

    def run_action(
        self,
        *,
        document_id: str,
        action: str,
        include_debug: bool = False,
    ) -> dict[str, Any]:
        normalized_action = normalize_key(action)

        if normalized_action in {"analyze", "analyse", "plan", "preview"}:
            return self.analyze_document(
                document_id=document_id,
                execute=False,
                include_debug=include_debug,
            )

        if normalized_action in {"explain", "why", "reason"}:
            return self.explain_document(
                document_id=document_id,
                include_debug=include_debug,
            )

        if normalized_action in {"execute", "apply", "run"}:
            return self.analyze_document(
                document_id=document_id,
                execute=True,
                include_debug=include_debug,
            )

        return {
            "skill": "openclaw_otocpa_skill",
            "document_id": document_id,
            "status": "error",
            "error": f"unsupported_action: {action}",
            "supported_actions": ["analyze", "explain", "execute"],
        }


def summarize_bridge_result(
    bridge_result: dict[str, Any],
    *,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Public module-level function for use in tests and external callers.
    Builds a compact skill-friendly summary from a raw bridge result dict
    without requiring a database connection.
    """
    skill = OpenClawOtoCPASkill.__new__(OpenClawOtoCPASkill)
    response = skill._build_summary(bridge_result)
    response["execute"] = bool(bridge_result.get("execute"))
    response["debug_included"] = bool(debug)
    if debug:
        response["debug"] = bridge_result
    return response


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenClaw OtoCPA skill wrapper")
    parser.add_argument("--document-id", required=True)
    parser.add_argument(
        "--action",
        default="analyze",
        choices=["analyze", "explain", "execute"],
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Include full bridge payload in result",
    )
    args = parser.parse_args()

    skill = OpenClawOtoCPASkill(db_path=DB_PATH)
    result = skill.run_action(
        document_id=args.document_id,
        action=args.action,
        include_debug=args.debug,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())