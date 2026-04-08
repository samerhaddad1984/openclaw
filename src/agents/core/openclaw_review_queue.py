from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
    return " ".join(normalize_text(value).casefold().split())


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    text = normalize_text(value)
    if not text:
        return {}
    try:
        loaded = json.loads(text)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


@dataclass(frozen=True)
class UserContext:
    username: str
    role: str
    allowed_clients: list[str]
    allowed_assigned_to: list[str]
    can_view_all_clients: bool
    can_view_all_assignments: bool


class OpenClawReviewQueue:
    """
    Review queue with:
    - user filtering
    - role filtering
    - assignment filtering
    - review / escalated views

    This is intentionally simple and hardcoded for phase 1.
    """

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    # -------------------------------------------------------------------------
    # User / role model
    # -------------------------------------------------------------------------

    def get_user_context(self, username: str = "sam") -> UserContext:
        user_key = normalize_key(username)

        user_map: dict[str, UserContext] = {
            "sam": UserContext(
                username="sam",
                role="owner",
                allowed_clients=[],
                allowed_assigned_to=[],
                can_view_all_clients=True,
                can_view_all_assignments=True,
            ),
            "owner": UserContext(
                username="owner",
                role="owner",
                allowed_clients=[],
                allowed_assigned_to=[],
                can_view_all_clients=True,
                can_view_all_assignments=True,
            ),
            "manager": UserContext(
                username="manager",
                role="manager",
                allowed_clients=[],
                allowed_assigned_to=["manager", "employee1", "employee2", "sam", ""],
                can_view_all_clients=True,
                can_view_all_assignments=True,
            ),
            "employee1": UserContext(
                username="employee1",
                role="employee",
                allowed_clients=["SOUSSOL", "SOUSSOL Quebec"],
                allowed_assigned_to=["employee1", ""],
                can_view_all_clients=False,
                can_view_all_assignments=False,
            ),
            "employee2": UserContext(
                username="employee2",
                role="employee",
                allowed_clients=["MARCHE_BRESIL", "MARCHE_BRÉSIL"],
                allowed_assigned_to=["employee2", ""],
                can_view_all_clients=False,
                can_view_all_assignments=False,
            ),
        }

        return user_map.get(
            user_key,
            UserContext(
                username=user_key or "unknown",
                role="employee",
                allowed_clients=[],
                allowed_assigned_to=[user_key, ""],
                can_view_all_clients=False,
                can_view_all_assignments=False,
            ),
        )

    # -------------------------------------------------------------------------
    # Assignment bootstrap
    # -------------------------------------------------------------------------

    def ensure_assignment_column(self) -> None:
        conn = open_db(self.db_path)
        try:
            columns = conn.execute("PRAGMA table_info(posting_jobs)").fetchall()
            column_names = {normalize_key(row["name"]) for row in columns}
            if "assigned_to" not in column_names:
                conn.execute("ALTER TABLE posting_jobs ADD COLUMN assigned_to TEXT")
                conn.commit()
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def list_queue(
        self,
        *,
        limit: int = 50,
        review_only: bool = False,
        escalated_only: bool = False,
        username: str = "sam",
        only_my_queue: bool = False,
    ) -> dict[str, Any]:
        self.ensure_assignment_column()

        user_context = self.get_user_context(username=username)
        rows = self._fetch_documents(
            limit=limit,
            review_only=review_only,
            escalated_only=escalated_only,
            user_context=user_context,
            only_my_queue=only_my_queue,
        )

        items: list[dict[str, Any]] = []
        escalated_count = 0
        needs_review_count = 0
        duplicate_medium_or_higher_count = 0
        vendor_flagged_count = 0

        for row in rows:
            item = self._build_queue_item(row=row, user_context=user_context)

            if item["needs_review"]:
                needs_review_count += 1

            if item["escalation"]["should_escalate"]:
                escalated_count += 1

            if item["duplicate"]["risk_level"] in {"medium", "high"}:
                duplicate_medium_or_higher_count += 1

            if item["vendor_memory"]["flagged_for_review"]:
                vendor_flagged_count += 1

            items.append(item)

        return {
            "status": "ok",
            "generated_at": utc_now_iso(),
            "filters": {
                "limit": limit,
                "review_only": review_only,
                "escalated_only": escalated_only,
                "username": user_context.username,
                "role": user_context.role,
                "only_my_queue": only_my_queue,
            },
            "user_context": {
                "username": user_context.username,
                "role": user_context.role,
                "allowed_clients": user_context.allowed_clients,
                "allowed_assigned_to": user_context.allowed_assigned_to,
                "can_view_all_clients": user_context.can_view_all_clients,
                "can_view_all_assignments": user_context.can_view_all_assignments,
            },
            "summary": {
                "total_items": len(items),
                "needs_review_count": needs_review_count,
                "escalated_count": escalated_count,
                "duplicate_medium_or_higher_count": duplicate_medium_or_higher_count,
                "vendor_flagged_count": vendor_flagged_count,
            },
            "items": items,
        }

    # -------------------------------------------------------------------------
    # Core query
    # -------------------------------------------------------------------------

    def _fetch_documents(
        self,
        *,
        limit: int = 50,
        review_only: bool = False,
        escalated_only: bool = False,
        user_context: UserContext,
        only_my_queue: bool = False,
    ) -> list[sqlite3.Row]:
        conn = open_db_readonly(self.db_path)
        try:
            where_clauses: list[str] = ["1=1"]
            params: list[Any] = []

            # -------------------------------------------------------------
            # Review-only filter
            # -------------------------------------------------------------
            if review_only:
                where_clauses.append(
                    "("
                    "COALESCE(LOWER(d.review_status), '') IN ("
                    "'review', 'needs review', 'needsreview', 'exception', 'escalated'"
                    ") "
                    "OR COALESCE(d.raw_result, '') LIKE '%\"action\": \"review\"%' "
                    "OR COALESCE(d.raw_result, '') LIKE '%\"flagged_for_review\": true%' "
                    "OR COALESCE(d.raw_result, '') LIKE '%\"risk_level\": \"medium\"%' "
                    "OR COALESCE(d.raw_result, '') LIKE '%\"risk_level\": \"high\"%' "
                    "OR COALESCE(pj.approval_state, '') = 'pending_human_approval' "
                    "OR COALESCE(pj.reviewer, '') = 'ExceptionRouter' "
                    ")"
                )

            # -------------------------------------------------------------
            # Escalated-only filter
            # IMPORTANT:
            # We only want truly escalated queue items, not every item that
            # happens to have a vendor flag or duplicate medium risk.
            # Exclude already-posted documents from escalation.
            # -------------------------------------------------------------
            if escalated_only:
                where_clauses.append(
                    "("
                    "COALESCE(d.raw_result, '') LIKE '%\"should_escalate\": true%' "
                    "OR COALESCE(d.raw_result, '') LIKE '%\"openclaw_escalation_result\"%' "
                    "OR COALESCE(d.raw_result, '') LIKE '%\"escalation_result\"%' "
                    "OR (COALESCE(pj.reviewer, '') = 'ExceptionRouter' AND COALESCE(pj.posting_status, '') != 'posted') "
                    "OR COALESCE(pj.approval_state, '') = 'pending_human_approval' "
                    ")"
                )

            # -------------------------------------------------------------
            # Client visibility filter
            # -------------------------------------------------------------
            if not user_context.can_view_all_clients:
                allowed_client_keys = [
                    normalize_key(value)
                    for value in user_context.allowed_clients
                    if normalize_key(value)
                ]
                if allowed_client_keys:
                    placeholders = ",".join("?" for _ in allowed_client_keys)
                    where_clauses.append(
                        f"normalize_client_code(COALESCE(d.client_code, '')) IN ({placeholders})"
                    )
                    params.extend(allowed_client_keys)
                else:
                    # No allowed clients means no rows.
                    where_clauses.append("1 = 0")

            # -------------------------------------------------------------
            # Assignment visibility filter
            # -------------------------------------------------------------
            if only_my_queue:
                where_clauses.append(
                    "normalize_assigned_to(COALESCE(pj.assigned_to, '')) = ?"
                )
                params.append(normalize_key(user_context.username))
            elif not user_context.can_view_all_assignments:
                allowed_assignment_keys = [
                    normalize_key(value)
                    for value in user_context.allowed_assigned_to
                    if normalize_key(value) or value == ""
                ]

                normalized_allowed: list[str] = []
                for value in allowed_assignment_keys:
                    if value not in normalized_allowed:
                        normalized_allowed.append(value)

                if "" not in normalized_allowed:
                    normalized_allowed.append("")

                placeholders = ",".join("?" for _ in normalized_allowed)
                where_clauses.append(
                    f"normalize_assigned_to(COALESCE(pj.assigned_to, '')) IN ({placeholders})"
                )
                params.extend(normalized_allowed)

            sql = f"""
                SELECT
                    d.document_id,
                    d.file_name,
                    d.file_path,
                    d.client_code,
                    d.vendor,
                    d.doc_type,
                    d.amount,
                    d.document_date,
                    d.gl_account,
                    d.tax_code,
                    d.category,
                    d.review_status,
                    d.confidence,
                    d.raw_result,
                    d.created_at,
                    d.updated_at,
                    pj.posting_id,
                    pj.posting_status,
                    pj.approval_state,
                    pj.reviewer,
                    pj.external_id,
                    pj.payload_json AS posting_payload_json,
                    pj.error_text AS posting_error_text,
                    pj.assigned_to
                FROM documents d
                LEFT JOIN posting_jobs pj
                    ON pj.document_id = d.document_id
                WHERE {" AND ".join(where_clauses)}
                ORDER BY
                    CASE
                        WHEN COALESCE(d.updated_at, '') != '' THEN d.updated_at
                        ELSE d.created_at
                    END DESC
                LIMIT ?
            """

            conn.create_function("normalize_client_code", 1, normalize_key)
            conn.create_function("normalize_assigned_to", 1, normalize_key)

            params.append(limit)
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # Item builders
    # -------------------------------------------------------------------------

    def _build_queue_item(
        self,
        *,
        row: sqlite3.Row,
        user_context: UserContext,
    ) -> dict[str, Any]:
        raw_result = safe_json_loads(row["raw_result"])
        duplicate_result = self._extract_duplicate_result(raw_result)
        vendor_memory_result = self._extract_vendor_memory_result(raw_result)
        auto_approval_result = self._extract_auto_approval_result(raw_result)
        exception_router_result = self._extract_exception_router_result(raw_result)
        escalation_result = self._extract_escalation_result(raw_result, row)

        needs_review = self._compute_needs_review(
            review_status=row["review_status"],
            exception_router_result=exception_router_result,
            vendor_memory_result=vendor_memory_result,
            duplicate_result=duplicate_result,
            escalation_result=escalation_result,
            posting_row=row,
        )

        review_priority = self._compute_review_priority(
            duplicate_result=duplicate_result,
            vendor_memory_result=vendor_memory_result,
            escalation_result=escalation_result,
            needs_review=needs_review,
        )

        item: dict[str, Any] = {
            "document_id": normalize_text(row["document_id"]),
            "file_name": normalize_text(row["file_name"]),
            "file_path": normalize_text(row["file_path"]),
            "client_code": normalize_text(row["client_code"]),
            "vendor": normalize_text(row["vendor"]),
            "doc_type": normalize_text(row["doc_type"]),
            "amount": row["amount"],
            "document_date": normalize_text(row["document_date"]),
            "gl_account": normalize_text(row["gl_account"]),
            "tax_code": normalize_text(row["tax_code"]),
            "category": normalize_text(row["category"]),
            "review_status": normalize_text(row["review_status"]),
            "confidence": float(row["confidence"] or 0.0),
            "created_at": normalize_text(row["created_at"]),
            "updated_at": normalize_text(row["updated_at"]),
            "assignment": {
                "assigned_to": normalize_text(row["assigned_to"]),
                "visible_to_user": self._is_visible_to_user(row=row, user_context=user_context),
                "is_mine": normalize_key(row["assigned_to"]) == normalize_key(user_context.username),
            },
            "posting": {
                "posting_id": normalize_text(row["posting_id"]),
                "posting_status": normalize_text(row["posting_status"]),
                "approval_state": normalize_text(row["approval_state"]),
                "reviewer": normalize_text(row["reviewer"]),
                "external_id": normalize_text(row["external_id"]),
                "error_text": normalize_text(row["posting_error_text"]),
                "payload_summary": self._extract_posting_payload_summary(row["posting_payload_json"]),
            },
            "duplicate": duplicate_result,
            "vendor_memory": vendor_memory_result,
            "auto_approval": auto_approval_result,
            "exception_router": exception_router_result,
            "escalation": escalation_result,
            "needs_review": needs_review,
            "review_priority": review_priority,
        }

        return self._apply_role_view(item=item, user_context=user_context)

    def _apply_role_view(
        self,
        *,
        item: dict[str, Any],
        user_context: UserContext,
    ) -> dict[str, Any]:
        role = normalize_key(user_context.role)

        # Employee view: hide some internal noise
        if role == "employee":
            item = dict(item)
            item["internal"] = {
                "role_view": "employee",
            }
            return item

        if role == "manager":
            item = dict(item)
            item["internal"] = {
                "role_view": "manager",
            }
            return item

        item = dict(item)
        item["internal"] = {
            "role_view": "owner",
        }
        return item

    # -------------------------------------------------------------------------
    # Extractors
    # -------------------------------------------------------------------------

    def _extract_posting_payload_summary(self, raw_payload: Any) -> dict[str, Any]:
        payload = safe_json_loads(raw_payload)
        return {
            "target_system": normalize_text(payload.get("target_system")),
            "entry_kind": normalize_text(payload.get("entry_kind")),
        }

    def _extract_duplicate_result(self, raw_result: dict[str, Any]) -> dict[str, Any]:
        duplicate = raw_result.get("duplicate_check_result")
        if not isinstance(duplicate, dict):
            duplicate = raw_result.get("duplicate_result")
        if not isinstance(duplicate, dict):
            duplicate = {}

        top_candidates = duplicate.get("top_candidates")
        if not isinstance(top_candidates, list):
            top_candidates = []

        reasons = duplicate.get("reasons")
        if not isinstance(reasons, list):
            reasons = []

        return {
            "risk_level": normalize_text(duplicate.get("risk_level") or "none"),
            "duplicate_confirmed": bool(duplicate.get("duplicate_confirmed", False)),
            "score": float(duplicate.get("score") or 0.0),
            "reason_count": len(reasons),
            "candidate_count": len(top_candidates),
            "reasons": reasons,
            "top_candidates": top_candidates,
        }

    def _extract_vendor_memory_result(self, raw_result: dict[str, Any]) -> dict[str, Any]:
        vendor_memory = raw_result.get("vendor_memory_enrichment")
        if not isinstance(vendor_memory, dict):
            vendor_memory = raw_result.get("vendor_memory_result")
        if not isinstance(vendor_memory, dict):
            vendor_memory = {}

        review_reasons = vendor_memory.get("review_reasons")
        if not isinstance(review_reasons, list):
            review_reasons = []

        amount_analysis = vendor_memory.get("amount_analysis")
        if not isinstance(amount_analysis, dict):
            amount_analysis = {}

        return {
            "flagged_for_review": bool(vendor_memory.get("flagged_for_review", False)),
            "review_reasons": review_reasons,
            "amount_analysis": {
                "vendor": normalize_text(amount_analysis.get("vendor")),
                "context_key": normalize_text(amount_analysis.get("context_key")),
                "sample_count": int(amount_analysis.get("sample_count") or 0),
                "median_amount": amount_analysis.get("median_amount"),
                "min_amount": amount_analysis.get("min_amount"),
                "max_amount": amount_analysis.get("max_amount"),
                "suspicious": bool(amount_analysis.get("suspicious", False)),
                "reason": normalize_text(amount_analysis.get("reason")),
            },
        }

    def _extract_auto_approval_result(self, raw_result: dict[str, Any]) -> dict[str, Any]:
        approval = raw_result.get("auto_approval_result")
        if not isinstance(approval, dict):
            approval = {}

        return {
            "decision": normalize_text(approval.get("decision")),
            "auto_approved": bool(approval.get("auto_approved", False)),
            "approval_score": float(approval.get("approval_score") or 0.0),
            "reason": normalize_text(approval.get("reason")),
        }

    def _extract_exception_router_result(self, raw_result: dict[str, Any]) -> dict[str, Any]:
        router = raw_result.get("exception_router_result")
        if not isinstance(router, dict):
            router = raw_result.get("exception_router")
        if not isinstance(router, dict):
            router = {}

        reasons = router.get("reasons")
        if not isinstance(reasons, list):
            reasons = []

        return {
            "action": normalize_text(router.get("action")),
            "reasons": reasons,
        }

    def _extract_escalation_result(
        self,
        raw_result: dict[str, Any],
        row: sqlite3.Row,
    ) -> dict[str, Any]:
        escalation = raw_result.get("openclaw_escalation_result")
        if isinstance(escalation, dict):
            return self._normalize_escalation_result(escalation)

        escalation = raw_result.get("escalation_result")
        if isinstance(escalation, dict):
            return self._normalize_escalation_result(escalation)

        # Fallback from posting job ownership/state
        posting_status = normalize_text(row["posting_status"])
        reviewer = normalize_text(row["reviewer"])
        approval_state = normalize_text(row["approval_state"])

        # Already-posted documents are not re-escalated regardless of reviewer
        if posting_status == "posted":
            return {
                "should_escalate": False,
                "decision": "",
                "escalation_reason": "",
                "reason": "",
                "provider": "",
                "confidence": 0.0,
                "escalated_at": "",
            }

        if reviewer == "ExceptionRouter":
            return {
                "should_escalate": True,
                "decision": "hold",
                "escalation_reason": "posting_job_owned_by_exception_router",
                "reason": "Posting job is currently owned by ExceptionRouter.",
                "provider": "posting_job_state",
                "confidence": 0.0,
                "escalated_at": "",
            }

        if approval_state == "pending_human_approval":
            return {
                "should_escalate": True,
                "decision": "hold",
                "escalation_reason": "posting_job_pending_human_approval",
                "reason": "Posting job is pending human approval.",
                "provider": "posting_job_state",
                "confidence": 0.0,
                "escalated_at": "",
            }

        if '"should_escalate": true' in json.dumps(raw_result, ensure_ascii=False):
            return {
                "should_escalate": True,
                "escalation_reason": "unknown_from_raw_result",
                "decision": "hold",
                "reason": "Escalation detected in raw_result.",
                "provider": "raw_result",
                "confidence": 0.0,
                "escalated_at": "",
            }

        return {
            "should_escalate": False,
            "decision": "",
            "escalation_reason": "",
            "reason": "",
            "provider": "",
            "confidence": 0.0,
            "escalated_at": "",
        }

    def _normalize_escalation_result(self, escalation: dict[str, Any]) -> dict[str, Any]:
        return {
            "should_escalate": bool(escalation.get("should_escalate", False)),
            "decision": normalize_text(escalation.get("decision")),
            "escalation_reason": normalize_text(escalation.get("escalation_reason")),
            "reason": normalize_text(escalation.get("reason")),
            "provider": normalize_text(escalation.get("provider")),
            "confidence": float(escalation.get("confidence") or 0.0),
            "escalated_at": normalize_text(escalation.get("escalated_at")),
        }

    # -------------------------------------------------------------------------
    # Logic helpers
    # -------------------------------------------------------------------------

    def _compute_needs_review(
        self,
        *,
        review_status: Any,
        exception_router_result: dict[str, Any],
        vendor_memory_result: dict[str, Any],
        duplicate_result: dict[str, Any],
        escalation_result: dict[str, Any],
        posting_row: sqlite3.Row,
    ) -> bool:
        review_status_key = normalize_key(review_status)
        if review_status_key in {"needsreview", "needs review", "exception", "review", "escalated"}:
            return True

        if escalation_result.get("should_escalate"):
            return True

        if normalize_text(posting_row["approval_state"]) == "pending_human_approval":
            return True

        if normalize_text(posting_row["reviewer"]) == "ExceptionRouter":
            return True

        if normalize_text(exception_router_result.get("action")) == "review":
            return True

        if vendor_memory_result.get("flagged_for_review"):
            return True

        if duplicate_result.get("risk_level") in {"medium", "high"}:
            return True

        return False

    def _compute_review_priority(
        self,
        *,
        duplicate_result: dict[str, Any],
        vendor_memory_result: dict[str, Any],
        escalation_result: dict[str, Any],
        needs_review: bool,
    ) -> str:
        if not needs_review:
            return "low"

        if escalation_result.get("should_escalate"):
            return "high"

        if duplicate_result.get("risk_level") in {"high", "medium"}:
            return "high"

        if vendor_memory_result.get("flagged_for_review"):
            return "high"

        return "medium"

    def _is_visible_to_user(
        self,
        *,
        row: sqlite3.Row,
        user_context: UserContext,
    ) -> bool:
        if not user_context.can_view_all_clients:
            allowed_client_keys = {normalize_key(value) for value in user_context.allowed_clients}
            if normalize_key(row["client_code"]) not in allowed_client_keys:
                return False

        if not user_context.can_view_all_assignments:
            allowed_assignment_keys = {normalize_key(value) for value in user_context.allowed_assigned_to}
            assigned_key = normalize_key(row["assigned_to"])
            if assigned_key not in allowed_assignment_keys and assigned_key != "":
                return False

        return True


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OpenClaw review queue")
    parser.add_argument("--limit", type=int, default=50, help="Maximum items to return")
    parser.add_argument("--review-only", action="store_true", help="Only review items")
    parser.add_argument("--escalated-only", action="store_true", help="Only escalated items")
    parser.add_argument("--user", default="sam", help="User context to simulate")
    parser.add_argument(
        "--only-my-queue",
        action="store_true",
        help="Only items assigned to the current user",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    queue = OpenClawReviewQueue()
    result = queue.list_queue(
        limit=args.limit,
        review_only=args.review_only,
        escalated_only=args.escalated_only,
        username=args.user,
        only_my_queue=args.only_my_queue,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())