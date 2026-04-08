from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_key(value)
    return text in {"1", "true", "yes", "y", "on"}


def deep_jsonable(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, sqlite3.Row):
        return {key: deep_jsonable(value[key]) for key in value.keys()}

    if isinstance(value, dict):
        return {str(key): deep_jsonable(val) for key, val in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [deep_jsonable(item) for item in value]

    if is_dataclass(value):
        return deep_jsonable(asdict(value))

    if hasattr(value, "__dict__"):
        try:
            return deep_jsonable(vars(value))
        except Exception:
            pass

    return str(value)


def load_json_maybe(value: Any) -> Any:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return value


def import_optional(module_path: str, attr_name: str) -> Any | None:
    try:
        module = __import__(module_path, fromlist=[attr_name])
        return getattr(module, attr_name, None)
    except Exception:
        return None


def get_document_row(document_id: str) -> sqlite3.Row | None:
    with open_db_readonly() as conn:
        return conn.execute(
            """
            SELECT *
            FROM documents
            WHERE document_id = ?
            """,
            (document_id,),
        ).fetchone()


def parse_document_row(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}

    document = dict(row)
    raw_result = load_json_maybe(document.get("raw_result"))
    if not isinstance(raw_result, dict):
        raw_result = {}
    document["raw_result"] = raw_result
    return document


def build_working_document(original_document: dict[str, Any]) -> dict[str, Any]:
    working = dict(original_document)
    raw_result = working.get("raw_result") or {}

    if isinstance(raw_result, dict):
        for key, value in raw_result.items():
            if working.get(key) in (None, ""):
                working[key] = value

        raw_rules_output = raw_result.get("raw_rules_output") or {}
        if working.get("currency") in (None, ""):
            working["currency"] = normalize_text(raw_rules_output.get("currency"))

    return working


def get_posting_snapshot(document_id: str) -> dict[str, Any]:
    with open_db_readonly() as conn:
        row = conn.execute(
            """
            SELECT
                posting_id,
                document_id,
                posting_status,
                approval_state,
                reviewer,
                target_system,
                entry_kind,
                external_id,
                payload_json
            FROM posting_jobs
            WHERE document_id = ?
            """,
            (document_id,),
        ).fetchone()

    if row is None:
        return {"exists": False}

    payload = load_json_maybe(row["payload_json"])
    return {
        "exists": True,
        "posting_id": normalize_text(row["posting_id"]),
        "posting_status": normalize_text(row["posting_status"]),
        "approval_state": normalize_text(row["approval_state"]),
        "reviewer": normalize_text(row["reviewer"]),
        "target_system": normalize_text(row["target_system"]),
        "entry_kind": normalize_text(row["entry_kind"]),
        "external_id": normalize_text(row["external_id"]),
        "payload_json": deep_jsonable(payload),
    }


def apply_learning_gl_account(document: dict[str, Any]) -> dict[str, Any]:
    engine_class = import_optional(
        "src.agents.core.gl_account_learning_engine",
        "GLAccountLearningEngine",
    )
    if engine_class is None:
        return {
            "applied": False,
            "reason": "learning_engine_unavailable",
            "score": 0.0,
            "support_count": 0,
            "required_support_count": 0,
            "candidate_new_value": "",
        }

    try:
        engine = engine_class()
        result = engine.apply_learning(document)
        if isinstance(result, dict):
            return deep_jsonable(result)
    except Exception as exc:
        return {
            "applied": False,
            "reason": f"learning_engine_error:{exc}",
            "score": 0.0,
            "support_count": 0,
            "required_support_count": 0,
            "candidate_new_value": "",
        }

    return {
        "applied": False,
        "reason": "invalid_learning_result",
        "score": 0.0,
        "support_count": 0,
        "required_support_count": 0,
        "candidate_new_value": "",
    }


def get_raw_embedded_result(document: dict[str, Any], key: str) -> dict[str, Any] | None:
    raw_result = document.get("raw_result") or {}
    value = raw_result.get(key)
    if isinstance(value, dict):
        return deep_jsonable(value)
    return None


def run_duplicate_guard(document: dict[str, Any]) -> dict[str, Any]:
    embedded = get_raw_embedded_result(document, "duplicate_guard_result")
    if embedded is not None:
        return embedded

    candidate_modules: list[tuple[str, str]] = [
        ("src.agents.core.openclaw_duplicate_guard", "OpenClawDuplicateGuard"),
        ("src.agents.core.duplicate_guard", "OpenClawDuplicateGuard"),
        ("src.agents.core.duplicate_guard", "DuplicateGuard"),
    ]

    for module_path, class_name in candidate_modules:
        guard_class = import_optional(module_path, class_name)
        if guard_class is None:
            continue
        try:
            guard = guard_class()
            for method_name in ("evaluate_document", "evaluate", "check_document", "check", "run"):
                method = getattr(guard, method_name, None)
                if callable(method):
                    result = method(document)
                    if isinstance(result, dict):
                        return deep_jsonable(result)
        except Exception:
            continue

    return {
        "duplicate_suspected": False,
        "duplicate_confirmed": False,
        "risk_level": "",
        "score": 0.0,
        "action": "",
        "reasons": [],
        "candidates": [],
    }


def run_auto_approval(document: dict[str, Any]) -> dict[str, Any]:
    embedded = get_raw_embedded_result(document, "auto_approval_result")
    if embedded is not None:
        return embedded

    candidate_modules: list[tuple[str, str]] = [
        ("src.agents.core.auto_approval_engine", "AutoApprovalEngine"),
        ("src.agents.core.openclaw_auto_approval_engine", "OpenClawAutoApprovalEngine"),
        ("src.agents.core.openclaw_auto_approval_engine", "AutoApprovalEngine"),
    ]

    for module_path, class_name in candidate_modules:
        engine_class = import_optional(module_path, class_name)
        if engine_class is None:
            continue
        try:
            engine = engine_class()
            for method_name in ("evaluate_document", "evaluate", "run", "score_document"):
                method = getattr(engine, method_name, None)
                if callable(method):
                    result = method(document)
                    if isinstance(result, dict):
                        return deep_jsonable(result)
        except Exception:
            continue

    return {
        "document_id": normalize_text(document.get("document_id")),
        "decision": "",
        "approval_score": 0.0,
        "auto_approved": False,
        "vendor_memory_ok": False,
        "document_confidence_ok": False,
        "amount_suspicious": False,
        "duplicate_risk_level": "",
        "reason": "",
        "evaluated_at": utc_now_iso(),
    }


def run_exception_router(document: dict[str, Any]) -> dict[str, Any]:
    embedded = get_raw_embedded_result(document, "exception_router_result")
    if embedded is not None:
        return embedded

    candidate_modules: list[tuple[str, str]] = [
        ("src.agents.core.exception_router", "ExceptionRouter"),
        ("src.agents.core.openclaw_exception_router", "OpenClawExceptionRouter"),
        ("src.agents.core.openclaw_exception_router", "ExceptionRouter"),
    ]

    for module_path, class_name in candidate_modules:
        router_class = import_optional(module_path, class_name)
        if router_class is None:
            continue
        try:
            router = router_class()
            for method_name in ("route_document", "route", "evaluate", "run"):
                method = getattr(router, method_name, None)
                if callable(method):
                    result = method(document)
                    if isinstance(result, dict):
                        return deep_jsonable(result)
        except Exception:
            continue

    return {
        "action": "",
        "reasons": [],
    }


def build_escalation_input(
    *,
    working_document: dict[str, Any],
    learning_result: dict[str, Any],
    duplicate_result: dict[str, Any],
    auto_result: dict[str, Any],
    exception_result: dict[str, Any],
) -> dict[str, Any]:
    raw_result = working_document.get("raw_result") or {}
    vendor_memory = raw_result.get("vendor_memory_enrichment") or {}

    return {
        "document_id": normalize_text(working_document.get("document_id")),
        "vendor": normalize_text(working_document.get("vendor")),
        "client_code": normalize_text(working_document.get("client_code")),
        "doc_type": normalize_text(working_document.get("doc_type")),
        "category": normalize_text(working_document.get("category")),
        "gl_account": normalize_text(working_document.get("gl_account")),
        "tax_code": normalize_text(working_document.get("tax_code")),
        "amount": safe_float(working_document.get("amount")) or 0.0,
        "currency": normalize_text(
            working_document.get("currency")
            or raw_result.get("currency")
            or (raw_result.get("raw_rules_output") or {}).get("currency")
        ),
        "document_date": normalize_text(working_document.get("document_date")),
        "review_status": normalize_text(working_document.get("review_status")),
        "confidence": safe_float(working_document.get("confidence")) or 0.0,
        "duplicate_risk": normalize_text(duplicate_result.get("risk_level")),
        "duplicate_confirmed": safe_bool(duplicate_result.get("duplicate_confirmed")),
        "duplicate_score": safe_float(duplicate_result.get("score")) or 0.0,
        "duplicate_reasons": deep_jsonable(duplicate_result.get("reasons") or []),
        "duplicate_candidate_count": len(duplicate_result.get("candidates") or []),
        "learning_applied": safe_bool(learning_result.get("applied")),
        "learning_reason": normalize_text(learning_result.get("reason")),
        "learning_support_count": safe_int(learning_result.get("support_count")),
        "learning_required_support_count": safe_int(
            learning_result.get("required_support_count")
        ),
        "learning_candidate_new_value": normalize_text(
            learning_result.get("candidate_new_value")
        ),
        "auto_decision": normalize_text(auto_result.get("decision")),
        "auto_approved": safe_bool(auto_result.get("auto_approved")),
        "approval_score": safe_float(auto_result.get("approval_score")) or 0.0,
        "exception_action": normalize_text(exception_result.get("action")),
        "exception_reasons": deep_jsonable(exception_result.get("reasons") or []),
        "vendor_memory_flagged_for_review": safe_bool(
            vendor_memory.get("flagged_for_review")
        ),
        "vendor_memory_review_reasons": deep_jsonable(
            vendor_memory.get("review_reasons") or []
        ),
        "source_summary": {
            "raw_vendor_source": normalize_text(raw_result.get("vendor_source")),
            "document_family": normalize_text(raw_result.get("document_family")),
            "routing_method": normalize_text(raw_result.get("routing_method")),
            "routing_score": safe_int(raw_result.get("routing_score")),
        },
    }


def run_escalation_engine(
    *,
    document_id: str,
    escalation_input: dict[str, Any],
) -> dict[str, Any]:
    engine_class = import_optional(
        "src.agents.core.openclaw_escalation_engine",
        "OpenClawEscalationEngine",
    )

    if engine_class is not None:
        try:
            engine = engine_class()
            for method_name in ("evaluate_case", "evaluate_document", "evaluate", "run_case", "run"):
                method = getattr(engine, method_name, None)
                if callable(method):
                    try:
                        result = method(escalation_input)
                    except TypeError:
                        result = method(document_id=document_id, case=escalation_input)
                    if isinstance(result, dict):
                        return deep_jsonable(result)
        except Exception:
            pass

    should_escalate = (
        safe_bool(escalation_input.get("vendor_memory_flagged_for_review"))
        or normalize_key(escalation_input.get("duplicate_risk")) in {"medium", "high"}
        or safe_bool(escalation_input.get("duplicate_confirmed"))
    )

    reason = "No escalation needed. Deterministic pipeline can proceed."
    escalation_reason = "no_escalation_needed"
    confidence = 0.99

    if safe_bool(escalation_input.get("vendor_memory_flagged_for_review")):
        reason = "Vendor memory flagged anomaly. Hold for review."
        escalation_reason = "vendor_memory_flagged_for_review"
        confidence = 0.92
    elif safe_bool(escalation_input.get("duplicate_confirmed")):
        reason = "Duplicate confirmed. Hold for review."
        escalation_reason = "duplicate_confirmed"
        confidence = 0.97
    elif normalize_key(escalation_input.get("duplicate_risk")) == "high":
        reason = "High duplicate risk. Hold for review."
        escalation_reason = "duplicate_risk_high"
        confidence = 0.90
    elif normalize_key(escalation_input.get("duplicate_risk")) == "medium":
        reason = "Medium duplicate risk. Hold for review."
        escalation_reason = "duplicate_risk_medium"
        confidence = 0.84

    return {
        "decision": "hold",
        "reason": reason,
        "confidence": confidence,
        "should_escalate": should_escalate,
        "escalation_reason": escalation_reason,
        "provider": "deterministic_fallback",
        "escalated_at": utc_now_iso(),
        "model_output": {
            "payload": {
                "instruction": (
                    "You are an accounting escalation decision engine. "
                    "Return strict JSON only with keys: decision, reason, confidence. "
                    "Allowed decision values: post, hold, reject."
                ),
                "case": deep_jsonable(escalation_input),
            },
            "fallback": True,
        },
    }


def run_orchestrator_fallback(
    *,
    document: dict[str, Any],
    posting_snapshot: dict[str, Any],
    duplicate_result: dict[str, Any],
    auto_result: dict[str, Any],
    exception_result: dict[str, Any],
    learning_result: dict[str, Any],
    escalation_result: dict[str, Any],
    execute: bool,
) -> dict[str, Any]:
    posting_status = normalize_key(posting_snapshot.get("posting_status"))
    approval_state = normalize_key(posting_snapshot.get("approval_state"))
    should_escalate = safe_bool(escalation_result.get("should_escalate"))
    exception_action = normalize_key(exception_result.get("action"))
    auto_decision = normalize_key(auto_result.get("decision"))

    next_step = "do_nothing"
    status = "planned"
    reason = "no_action"

    if posting_status == "posted":
        reason = "document already posted"
    elif should_escalate:
        reason = "escalated_for_review"
    elif exception_action == "approve_but_hold" or auto_decision == "approve_but_hold":
        if posting_status == "ready_to_post":
            reason = "approve_but_hold document already ready"
        else:
            reason = "approve_but_hold"
    elif posting_status == "ready_to_post":
        reason = "already_ready_to_post"
    elif (
        execute
        and auto_decision == "auto_post"
        and approval_state == "approved_for_posting"
        and posting_status in {"draft", "queued", "pending"}
    ):
        next_step = "post_now"
        status = "planned"
        reason = "ready to post"
    else:
        reason = "no_action"

    actions_attempted: list[str] = []
    action_results: list[dict[str, Any]] = []

    if execute:
        actions_attempted.append("do_nothing")
        action_results.append(
            {
                "action": "do_nothing",
                "status": "success",
                "reason": reason,
            }
        )

    return {
        "document_id": normalize_text(document.get("document_id")),
        "next_step": next_step,
        "status": status,
        "reason": reason,
        "signals": {
            "review_status": normalize_text(document.get("review_status")),
            "has_posting_job": safe_bool(posting_snapshot.get("exists")),
            "posting_status": normalize_text(posting_snapshot.get("posting_status")),
            "approval_state": normalize_text(posting_snapshot.get("approval_state")),
            "duplicate_risk_level": normalize_text(duplicate_result.get("risk_level")),
            "duplicate_confirmed": safe_bool(duplicate_result.get("duplicate_confirmed")),
            "auto_approved": safe_bool(auto_result.get("auto_approved")),
            "decision": normalize_text(auto_result.get("decision")),
            "route_action": normalize_text(exception_result.get("action")),
            "should_escalate": safe_bool(escalation_result.get("should_escalate")),
            "escalation_reason": normalize_text(escalation_result.get("escalation_reason")),
        },
        "duplicate_result": deep_jsonable(duplicate_result),
        "auto_result": deep_jsonable(auto_result),
        "exception_result": deep_jsonable(exception_result),
        "learning_gl_account_result": deep_jsonable(learning_result),
        "escalation_result": deep_jsonable(escalation_result),
        "final_document_values": {
            "client_code": normalize_text(document.get("client_code")),
            "vendor": normalize_text(document.get("vendor")),
            "doc_type": normalize_text(document.get("doc_type")),
            "category": normalize_text(document.get("category")),
            "gl_account": normalize_text(document.get("gl_account")),
            "tax_code": normalize_text(document.get("tax_code")),
            "amount": safe_float(document.get("amount")) or 0.0,
            "confidence": safe_float(document.get("confidence")) or 0.0,
        },
        "actions_attempted": actions_attempted,
        "action_results": action_results,
        "execute": execute,
    }


def run_orchestrator(
    *,
    document: dict[str, Any],
    posting_snapshot: dict[str, Any],
    duplicate_result: dict[str, Any],
    auto_result: dict[str, Any],
    exception_result: dict[str, Any],
    learning_result: dict[str, Any],
    escalation_result: dict[str, Any],
    execute: bool,
) -> dict[str, Any]:
    orchestrator_class = import_optional(
        "src.agents.core.openclaw_case_orchestrator",
        "OpenClawCaseOrchestrator",
    )

    if orchestrator_class is not None:
        try:
            orchestrator = orchestrator_class()
            context = {
                "document": document,
                "duplicate_result": duplicate_result,
                "auto_result": auto_result,
                "exception_result": exception_result,
                "learning_gl_account_result": learning_result,
                "escalation_result": escalation_result,
                "execute": execute,
            }

            for method_name in ("run_case", "run_document", "run", "orchestrate"):
                method = getattr(orchestrator, method_name, None)
                if not callable(method):
                    continue
                try:
                    result = method(**context)
                except TypeError:
                    try:
                        result = method(context)
                    except TypeError:
                        continue
                if isinstance(result, dict):
                    return deep_jsonable(result)
        except Exception:
            pass

    return run_orchestrator_fallback(
        document=document,
        posting_snapshot=posting_snapshot,
        duplicate_result=duplicate_result,
        auto_result=auto_result,
        exception_result=exception_result,
        learning_result=learning_result,
        escalation_result=escalation_result,
        execute=execute,
    )


def maybe_post_ready_job(
    *,
    orchestrator_result: dict[str, Any],
    document_id: str,
    execute: bool,
) -> list[dict[str, Any]]:
    # OpenClaw analysis code must never trigger a QBO posting directly.
    # All posting must be initiated by the dashboard / posting_builder pipeline.
    raise PermissionError(
        "OpenClaw code paths are not permitted to call the QBO adapter directly. "
        "Use the posting_builder pipeline from the review dashboard instead."
    )

    # --- unreachable; kept for IDE reference only ---
    if not execute:  # pragma: no cover
        return []

    if normalize_key(orchestrator_result.get("next_step")) != "post_now":  # pragma: no cover
        return []

    post_now_callable = import_optional(  # pragma: no cover
        "src.agents.tools.qbo_online_adapter",
        "post_one_ready_job",
    )
    if not callable(post_now_callable):
        return [
            {
                "action": "post_now",
                "status": "error",
                "error": "qbo_post_adapter_unavailable",
            }
        ]

    try:
        result = post_now_callable(document_id)
        if isinstance(result, dict):
            return [deep_jsonable(result)]
        return [{"action": "post_now", "status": "unknown"}]
    except Exception as exc:
        return [
            {
                "action": "post_now",
                "status": "error",
                "error": str(exc),
            }
        ]


def process_document(document_id: str, execute: bool = False) -> dict[str, Any]:
    original_row = get_document_row(document_id)
    if original_row is None:
        return {
            "bridge": "openclaw_otocpa_bridge",
            "document_id": document_id,
            "processed_at": utc_now_iso(),
            "execute": execute,
            "status": "error",
            "error": f"document_not_found:{document_id}",
        }

    original_document = parse_document_row(original_row)
    working_document = build_working_document(original_document)

    learning_result = apply_learning_gl_account(working_document)
    if safe_bool(learning_result.get("applied")):
        candidate_value = normalize_text(learning_result.get("candidate_new_value"))
        if candidate_value:
            working_document["gl_account"] = candidate_value

    duplicate_result = run_duplicate_guard(working_document)
    auto_result = run_auto_approval(working_document)
    exception_result = run_exception_router(working_document)

    escalation_input = build_escalation_input(
        working_document=working_document,
        learning_result=learning_result,
        duplicate_result=duplicate_result,
        auto_result=auto_result,
        exception_result=exception_result,
    )
    escalation_result = run_escalation_engine(
        document_id=document_id,
        escalation_input=escalation_input,
    )

    posting_snapshot_before = get_posting_snapshot(document_id)

    orchestrator_result = run_orchestrator(
        document=working_document,
        posting_snapshot=posting_snapshot_before,
        duplicate_result=duplicate_result,
        auto_result=auto_result,
        exception_result=exception_result,
        learning_result=learning_result,
        escalation_result=escalation_result,
        execute=execute,
    )

    posting_action_results = maybe_post_ready_job(
        orchestrator_result=orchestrator_result,
        document_id=document_id,
        execute=execute,
    )

    posting_snapshot_after = get_posting_snapshot(document_id)

    return {
        "bridge": "openclaw_otocpa_bridge",
        "document_id": document_id,
        "processed_at": utc_now_iso(),
        "execute": execute,
        "original_document": deep_jsonable(original_document),
        "working_document": deep_jsonable(working_document),
        "learning_gl_account_result": deep_jsonable(learning_result),
        "duplicate_result": deep_jsonable(duplicate_result),
        "auto_result": deep_jsonable(auto_result),
        "exception_result": deep_jsonable(exception_result),
        "escalation_input": deep_jsonable(escalation_input),
        "escalation_result": deep_jsonable(escalation_result),
        "orchestrator_result": deep_jsonable(orchestrator_result),
        "posting_snapshot_before": deep_jsonable(posting_snapshot_before),
        "posting_snapshot_after": deep_jsonable(posting_snapshot_after),
        "posting_action_results": deep_jsonable(posting_action_results),
        "final_action": normalize_text(orchestrator_result.get("next_step")),
        "final_reason": normalize_text(orchestrator_result.get("reason")),
        "summary": {
            "learning_applied": safe_bool(learning_result.get("applied")),
            "duplicate_risk_level": normalize_text(duplicate_result.get("risk_level")),
            "duplicate_confirmed": safe_bool(duplicate_result.get("duplicate_confirmed")),
            "auto_decision": normalize_text(auto_result.get("decision")),
            "auto_approved": safe_bool(auto_result.get("auto_approved")),
            "exception_action": normalize_text(exception_result.get("action")),
            "should_escalate": safe_bool(escalation_result.get("should_escalate")),
            "escalation_reason": normalize_text(escalation_result.get("escalation_reason")),
            "orchestrator_next_step": normalize_text(orchestrator_result.get("next_step")),
            "orchestrator_status": normalize_text(orchestrator_result.get("status")),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    result = process_document(
        document_id=args.document_id,
        execute=args.execute,
    )
    print(json.dumps(deep_jsonable(result), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()