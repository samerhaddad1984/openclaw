from __future__ import annotations

import importlib
import inspect
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.agents.core.auto_approval_engine import AutoApprovalEngine
from src.agents.core.duplicate_guard import DuplicateGuard
from src.agents.core.exception_router import ExceptionRouter

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
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
    return " ".join(normalize_text(value).lower().split())


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


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not table_exists(conn, table_name):
        return set()

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: set[str] = set()

    for row in rows:
        try:
            columns.add(str(row["name"]))
        except Exception:
            try:
                columns.add(str(row[1]))
            except Exception:
                pass

    return columns


def pick(row: sqlite3.Row | dict[str, Any], key: str, default: Any = "") -> Any:
    try:
        if isinstance(row, sqlite3.Row):
            return row[key]
        return row.get(key, default)
    except Exception:
        return default


def instantiate_compat(cls: type[Any], *, db_path: Path) -> Any:
    try:
        return cls(db_path=db_path)
    except TypeError:
        return cls()


def call_compat(func: Any, /, **kwargs: Any) -> Any:
    try:
        sig = inspect.signature(func)
        accepted: dict[str, Any] = {}

        for name, param in sig.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ) and name in kwargs:
                accepted[name] = kwargs[name]

        return func(**accepted)
    except Exception:
        return func()


class OpenClawCaseOrchestrator:
    def __init__(self, *, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.auto_approval_engine = instantiate_compat(AutoApprovalEngine, db_path=db_path)
        self.duplicate_guard = instantiate_compat(DuplicateGuard, db_path=db_path)
        self.exception_router = instantiate_compat(ExceptionRouter, db_path=db_path)
        self.learning_engine = self._load_optional_learning_engine()

    def _open(self) -> sqlite3.Connection:
        return open_db_readonly(self.db_path)

    def _open_rw(self) -> sqlite3.Connection:
        return open_db(self.db_path)

    def _load_optional_learning_engine(self) -> Any | None:
        """
        Optional loader so the orchestrator does NOT crash if the learning engine
        file does not exist yet.
        """
        try:
            module = importlib.import_module("src.agents.core.gl_account_learning_engine")
            cls = getattr(module, "GLAccountLearningEngine", None)
            if cls is None:
                return None

            try:
                return cls(db_path=self.db_path)
            except TypeError:
                return cls()
        except Exception:
            return None

    def _get_document_row(self, document_id: str) -> sqlite3.Row:
        with self._open() as conn:
            has_posting_jobs = table_exists(conn, "posting_jobs")

            if has_posting_jobs:
                row = conn.execute(
                    """
                    SELECT
                        d.*,
                        COALESCE(p.posting_id, '') AS posting_id,
                        COALESCE(p.posting_status, '') AS posting_status,
                        COALESCE(p.approval_state, '') AS approval_state
                    FROM documents d
                    LEFT JOIN posting_jobs p
                        ON p.document_id = d.document_id
                    WHERE d.document_id = ?
                    LIMIT 1
                    """,
                    (document_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT
                        d.*,
                        '' AS posting_id,
                        '' AS posting_status,
                        '' AS approval_state
                    FROM documents d
                    WHERE d.document_id = ?
                    LIMIT 1
                    """,
                    (document_id,),
                ).fetchone()

        if row is None:
            raise ValueError(f"Document not found: {document_id}")

        return row

    def _apply_optional_learning(self, document_payload: dict[str, Any]) -> dict[str, Any]:
        """
        Applies GL learning if the engine exists.
        This is compatibility-safe and never hard-fails the orchestrator.
        """
        result_payload = dict(document_payload)
        learning_result: dict[str, Any] = {
            "applied": False,
            "engine_available": self.learning_engine is not None,
            "status": "not_run",
        }

        if self.learning_engine is None:
            result_payload["learning_gl_account_result"] = learning_result
            return result_payload

        try:
            if hasattr(self.learning_engine, "apply_learning"):
                learning_result = call_compat(
                    self.learning_engine.apply_learning,
                    document=result_payload,
                    document_payload=result_payload,
                )
            elif hasattr(self.learning_engine, "evaluate_document"):
                learning_result = call_compat(
                    self.learning_engine.evaluate_document,
                    document=result_payload,
                    document_payload=result_payload,
                )
            else:
                learning_result = {
                    "applied": False,
                    "engine_available": True,
                    "status": "no_supported_method",
                }

            if not isinstance(learning_result, dict):
                learning_result = {
                    "applied": False,
                    "engine_available": True,
                    "status": "invalid_learning_result",
                }

            # Support engines that either mutate the document in place or return fields.
            suggested_gl = normalize_text(
                learning_result.get("gl_account")
                or learning_result.get("new_value")
                or learning_result.get("suggested_value")
            )
            suggested_tax = normalize_text(
                learning_result.get("tax_code")
                or learning_result.get("suggested_tax_code")
            )
            suggested_category = normalize_text(
                learning_result.get("category")
                or learning_result.get("suggested_category")
            )

            if suggested_gl:
                result_payload["gl_account"] = suggested_gl
            if suggested_tax:
                result_payload["tax_code"] = suggested_tax
            if suggested_category:
                result_payload["category"] = suggested_category

            result_payload["learning_gl_account_result"] = learning_result
            return result_payload

        except Exception as exc:
            result_payload["learning_gl_account_result"] = {
                "applied": False,
                "engine_available": True,
                "status": "failed",
                "error": normalize_text(exc),
            }
            return result_payload

    def _normalize_duplicate_result(self, document_id: str) -> dict[str, Any]:
        try:
            if hasattr(self.duplicate_guard, "evaluate_document"):
                result = call_compat(
                    self.duplicate_guard.evaluate_document,
                    document_id=document_id,
                    limit=10,
                )
            elif hasattr(self.duplicate_guard, "detect_duplicates"):
                result = call_compat(
                    self.duplicate_guard.detect_duplicates,
                    document_id=document_id,
                    limit=10,
                )
            elif hasattr(self.duplicate_guard, "run_duplicate_check"):
                result = call_compat(
                    self.duplicate_guard.run_duplicate_check,
                    document_id=document_id,
                    limit=10,
                )
            elif hasattr(self.duplicate_guard, "check_duplicates"):
                result = call_compat(
                    self.duplicate_guard.check_duplicates,
                    document_id=document_id,
                    limit=10,
                )
            else:
                result = {}

            return result if isinstance(result, dict) else {}
        except Exception as exc:
            return {
                "duplicate_suspected": False,
                "duplicate_confirmed": False,
                "risk_level": "none",
                "score": 0.0,
                "action": "allow",
                "reasons": [f"duplicate_guard_failed: {normalize_text(exc)}"],
                "candidates": [],
            }

    def _normalize_auto_result(
        self,
        *,
        document_id: str,
        document_payload: dict[str, Any],
        duplicate_result: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            if hasattr(self.auto_approval_engine, "evaluate_document"):
                result = call_compat(
                    self.auto_approval_engine.evaluate_document,
                    document=document_payload,
                    document_id=document_id,
                    duplicate_result=duplicate_result,
                )
            elif hasattr(self.auto_approval_engine, "evaluate"):
                result = call_compat(
                    self.auto_approval_engine.evaluate,
                    document=document_payload,
                    document_id=document_id,
                    duplicate_result=duplicate_result,
                )
            elif hasattr(self.auto_approval_engine, "run"):
                result = call_compat(
                    self.auto_approval_engine.run,
                    document=document_payload,
                    document_id=document_id,
                    duplicate_result=duplicate_result,
                )
            else:
                result = {}

            return result if isinstance(result, dict) else {}
        except Exception as exc:
            return {
                "document_id": document_id,
                "decision": "needs_review",
                "approval_score": 0.0,
                "auto_approved": False,
                "vendor_memory_ok": False,
                "document_confidence_ok": False,
                "amount_suspicious": True,
                "duplicate_risk_level": normalize_text(duplicate_result.get("risk_level")) or "none",
                "reason": f"auto_approval_engine_failed: {normalize_text(exc)}",
                "evaluated_at": utc_now_iso(),
                "vendor_memory_confidence": 0.0,
                "learned_pattern_used": False,
                "learned_pattern_confidence": 0.0,
            }

    def _normalize_exception_result(
        self,
        *,
        row: sqlite3.Row,
        document_payload: dict[str, Any],
        auto_result: dict[str, Any],
        duplicate_result: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            if hasattr(self.exception_router, "route_document"):
                result = call_compat(
                    self.exception_router.route_document,
                    document=document_payload,
                    row=row,
                    auto_result=auto_result,
                    duplicate_result=duplicate_result,
                )
            elif hasattr(self.exception_router, "evaluate_document"):
                result = call_compat(
                    self.exception_router.evaluate_document,
                    document=document_payload,
                    row=row,
                    auto_result=auto_result,
                    duplicate_result=duplicate_result,
                )
            else:
                result = {}

            if isinstance(result, dict):
                return result
        except Exception:
            pass

        return {
            "action": "review",
            "reasons": ["exception_router_failed"],
            "score": 0.0,
            "evaluated_at": utc_now_iso(),
            "learned_pattern_used": bool(auto_result.get("learned_pattern_used")),
            "learned_pattern_summary": "",
            "vendor_memory_match_found": False,
            "vendor_memory_confidence": float(auto_result.get("vendor_memory_confidence") or 0.0),
            "duplicate_risk_level": normalize_text(duplicate_result.get("risk_level")) or "none",
            "duplicate_confirmed": bool(duplicate_result.get("duplicate_confirmed")),
            "amount_suspicious": bool(auto_result.get("amount_suspicious")),
            "document_confidence_ok": bool(auto_result.get("document_confidence_ok")),
            "auto_approved": bool(auto_result.get("auto_approved")),
            "auto_decision": normalize_text(auto_result.get("decision")),
            "review_pattern_hits": 0,
            "success_pattern_hits": 0,
            "posted_pattern_hits": 0,
        }

    def _build_signals(
        self,
        *,
        row: sqlite3.Row,
        auto_result: dict[str, Any],
        duplicate_result: dict[str, Any],
        exception_result: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "review_status": normalize_text(pick(row, "review_status")),
            "has_posting_job": bool(normalize_text(pick(row, "posting_id"))),
            "posting_status": normalize_text(pick(row, "posting_status")),
            "approval_state": normalize_text(pick(row, "approval_state")),
            "duplicate_risk_level": normalize_text(duplicate_result.get("risk_level")) or "none",
            "duplicate_confirmed": bool(duplicate_result.get("duplicate_confirmed")),
            "auto_approved": bool(auto_result.get("auto_approved")),
            "decision": normalize_text(auto_result.get("decision")),
            "route_action": normalize_text(exception_result.get("action")),
        }

    def _decide_next_step(self, signals: dict[str, Any]) -> tuple[str, str]:
        review_status = normalize_key(signals.get("review_status"))
        posting_status = normalize_key(signals.get("posting_status"))
        approval_state = normalize_key(signals.get("approval_state"))
        duplicate_confirmed = bool(signals.get("duplicate_confirmed"))
        route_action = normalize_key(signals.get("route_action"))

        if duplicate_confirmed:
            return "block_as_duplicate", "confirmed duplicate"

        if route_action == "review" or review_status == "needsreview":
            return "hold_for_review", "case requires human review"

        if route_action == "approve_but_hold":
            if posting_status == "draft":
                return "approve_for_posting", "approved draft ready for posting"
            if posting_status == "ready_to_post":
                return "do_nothing", "approve_but_hold document already ready"
            if posting_status == "posted":
                return "do_nothing", "document already posted"
            return "approve_for_posting", "approved but held"

        if route_action == "auto_post":
            if posting_status == "posted":
                return "do_nothing", "document already posted"
            if posting_status == "ready_to_post" and approval_state == "approved_for_posting":
                return "post_now", "ready to post"
            if posting_status == "draft" and approval_state == "approved_for_posting":
                return "approve_for_posting", "approved draft ready for posting"
            return "create_posting_job", "auto post requested"

        if posting_status == "posted":
            return "do_nothing", "document already posted"

        return "hold_for_review", "fallback review"

    def _execute_action(self, next_step: str, reason: str, row: sqlite3.Row) -> dict[str, Any]:
        document_id = normalize_text(pick(row, "document_id"))
        posting_id = normalize_text(pick(row, "posting_id"))
        now = utc_now_iso()

        with self._open_rw() as conn:
            posting_columns = get_table_columns(conn, "posting_jobs")
            document_columns = get_table_columns(conn, "documents")

            if next_step == "hold_for_review":
                if "review_status" in document_columns:
                    conn.execute(
                        "UPDATE documents SET review_status = ?, updated_at = COALESCE(updated_at, ?) WHERE document_id = ?",
                        ("NeedsReview", now, document_id),
                    )
                conn.commit()
                return {
                    "action": "hold_for_review",
                    "status": "success",
                    "review_status": "NeedsReview",
                    "reason": reason,
                }

            if next_step == "approve_for_posting":
                if posting_id and posting_columns:
                    updates: list[str] = []
                    values: list[Any] = []

                    if "approval_state" in posting_columns:
                        updates.append("approval_state = ?")
                        values.append("approved_for_posting")

                    if "posting_status" in posting_columns:
                        updates.append("posting_status = ?")
                        values.append("ready_to_post")

                    if "reviewer" in posting_columns:
                        updates.append("reviewer = ?")
                        values.append("OpenClawCaseOrchestrator")

                    if "updated_at" in posting_columns:
                        updates.append("updated_at = ?")
                        values.append(now)

                    if updates:
                        values.append(document_id)
                        conn.execute(
                            f"UPDATE posting_jobs SET {', '.join(updates)} WHERE document_id = ?",
                            values,
                        )
                        conn.commit()

                return {
                    "action": "approve_for_posting",
                    "status": "success",
                    "posting_id": posting_id,
                    "reason": reason,
                }

            if next_step == "post_now":
                if posting_id and posting_columns:
                    updates = []
                    values = []

                    if "posting_status" in posting_columns:
                        updates.append("posting_status = ?")
                        values.append("posted")

                    if "reviewer" in posting_columns:
                        updates.append("reviewer = ?")
                        values.append("OpenClawCaseOrchestrator")

                    if "updated_at" in posting_columns:
                        updates.append("updated_at = ?")
                        values.append(now)

                    if updates:
                        values.append(document_id)
                        conn.execute(
                            f"UPDATE posting_jobs SET {', '.join(updates)} WHERE document_id = ?",
                            values,
                        )
                        conn.commit()

                return {
                    "action": "post_now",
                    "status": "posted",
                    "posting_id": posting_id,
                    "external_id": "",
                    "error": "",
                    "reason": reason,
                }

            if next_step == "block_as_duplicate":
                if "review_status" in document_columns:
                    conn.execute(
                        "UPDATE documents SET review_status = ?, updated_at = COALESCE(updated_at, ?) WHERE document_id = ?",
                        ("NeedsReview", now, document_id),
                    )
                conn.commit()
                return {
                    "action": "block_as_duplicate",
                    "status": "success",
                    "reason": reason,
                }

            if next_step == "create_posting_job":
                return {
                    "action": "create_posting_job",
                    "status": "success",
                    "reason": reason,
                }

            return {
                "action": "do_nothing",
                "status": "success",
                "reason": reason,
            }

    def _build_result_payload(
        self,
        *,
        document_id: str,
        next_step: str,
        status: str,
        reason: str,
        signals: dict[str, Any],
        duplicate_result: dict[str, Any],
        auto_result: dict[str, Any],
        exception_result: dict[str, Any],
        action_results: list[dict[str, Any]],
        execute: bool,
        learned_document: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "document_id": document_id,
            "next_step": next_step,
            "status": status,
            "reason": reason,
            "signals": signals,
            "duplicate_result": duplicate_result,
            "auto_result": auto_result,
            "exception_result": exception_result,
            "learning_gl_account_result": learned_document.get("learning_gl_account_result", {}),
            "final_document_values": {
                "client_code": normalize_text(learned_document.get("client_code")),
                "vendor": normalize_text(learned_document.get("vendor")),
                "doc_type": normalize_text(learned_document.get("doc_type")),
                "category": normalize_text(learned_document.get("category")),
                "gl_account": normalize_text(learned_document.get("gl_account")),
                "tax_code": normalize_text(learned_document.get("tax_code")),
                "amount": learned_document.get("amount"),
                "confidence": learned_document.get("confidence"),
            },
            "actions_attempted": [next_step] if next_step else [],
            "action_results": action_results,
            "execute": execute,
        }

    def _record_orchestrator_run(self, *, document_id: str, result_payload: dict[str, Any]) -> None:
        with self._open_rw() as conn:
            columns = get_table_columns(conn, "orchestrator_runs")
            if not columns:
                return

            now = utc_now_iso()
            payload_json = json.dumps(result_payload, ensure_ascii=False)

            action_value = normalize_text(result_payload.get("next_step")) or "unknown"
            status_value = normalize_text(result_payload.get("status")) or "unknown"
            reason_value = normalize_text(result_payload.get("reason"))

            record: dict[str, Any] = {}

            if "document_id" in columns:
                record["document_id"] = document_id
            if "created_at" in columns:
                record["created_at"] = now
            if "updated_at" in columns:
                record["updated_at"] = now
            if "action" in columns:
                record["action"] = action_value
            if "next_step" in columns:
                record["next_step"] = action_value
            if "status" in columns:
                record["status"] = status_value
            if "reason" in columns:
                record["reason"] = reason_value
            if "result_json" in columns:
                record["result_json"] = payload_json
            elif "payload_json" in columns:
                record["payload_json"] = payload_json
            elif "data_json" in columns:
                record["data_json"] = payload_json

            if not record:
                return

            insert_cols = ", ".join(record.keys())
            placeholders = ", ".join(["?"] * len(record))

            conn.execute(
                f"""
                INSERT INTO orchestrator_runs ({insert_cols})
                VALUES ({placeholders})
                """,
                list(record.values()),
            )
            conn.commit()

    def evaluate_document(self, *, document_id: str, execute: bool = False) -> dict[str, Any]:
        row = self._get_document_row(document_id)

        duplicate_result = self._normalize_duplicate_result(document_id)

        raw_document_payload = dict(row)
        learned_document_payload = self._apply_optional_learning(raw_document_payload)

        auto_result = self._normalize_auto_result(
            document_id=document_id,
            document_payload=learned_document_payload,
            duplicate_result=duplicate_result,
        )

        exception_result = self._normalize_exception_result(
            row=row,
            document_payload=learned_document_payload,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
        )

        duplicate_risk_level = normalize_key(duplicate_result.get("risk_level"))

        if duplicate_risk_level == "high":
            exception_result["action"] = "review"
            reasons = exception_result.get("reasons", [])
            if not isinstance(reasons, list):
                reasons = [normalize_text(reasons)] if reasons else []
            reasons.append("duplicate_high_risk")
            exception_result["reasons"] = list(
                dict.fromkeys([normalize_text(x) for x in reasons if normalize_text(x)])
            )

        elif duplicate_risk_level == "medium":
            current_action = normalize_key(exception_result.get("action"))
            if current_action == "auto_post":
                exception_result["action"] = "approve_but_hold"
                reasons = exception_result.get("reasons", [])
                if not isinstance(reasons, list):
                    reasons = [normalize_text(reasons)] if reasons else []
                reasons.append("duplicate_medium_risk")
                if normalize_key(pick(row, "posting_status")) == "ready_to_post":
                    reasons.append("already_ready_to_post")
                exception_result["reasons"] = list(
                    dict.fromkeys([normalize_text(x) for x in reasons if normalize_text(x)])
                )

        signals = self._build_signals(
            row=row,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
            exception_result=exception_result,
        )

        next_step, reason = self._decide_next_step(signals)

        action_results: list[dict[str, Any]] = []
        final_status = "planned"

        if execute:
            action_result = self._execute_action(next_step, reason, row)
            action_results.append(action_result)
            final_status = normalize_text(action_result.get("status")) or "success"
            if normalize_text(action_result.get("reason")):
                reason = normalize_text(action_result.get("reason"))

        result = self._build_result_payload(
            document_id=document_id,
            next_step=next_step,
            status=final_status,
            reason=reason,
            signals=signals,
            duplicate_result=duplicate_result,
            auto_result=auto_result,
            exception_result=exception_result,
            action_results=action_results,
            execute=execute,
            learned_document=learned_document_payload,
        )

        self._record_orchestrator_run(document_id=document_id, result_payload=result)
        return result

    def process_document(self, *, document_id: str, execute: bool = False) -> dict[str, Any]:
        return self.evaluate_document(document_id=document_id, execute=execute)

    def run_case(
        self,
        document_id: str,
        execute: bool = False,
        execute_actions: bool | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        if execute_actions is not None:
            execute = bool(execute_actions)
        return self.evaluate_document(document_id=document_id, execute=execute)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OpenClaw case orchestrator")
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    orchestrator = OpenClawCaseOrchestrator()
    result = orchestrator.evaluate_document(
        document_id=args.document_id,
        execute=args.execute,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())