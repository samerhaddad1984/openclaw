from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


class ReviewActions:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    def _get_document(self, conn: sqlite3.Connection, document_id: str) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT
                document_id,
                file_name,
                file_path,
                client_code,
                vendor,
                doc_type,
                amount,
                document_date,
                gl_account,
                tax_code,
                category,
                review_status,
                confidence,
                raw_result,
                created_at,
                updated_at
            FROM documents
            WHERE document_id = ?
            """,
            (document_id,),
        ).fetchone()

    def _get_posting_job(self, conn: sqlite3.Connection, document_id: str) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT
                posting_id,
                document_id,
                posting_status,
                approval_state,
                reviewer,
                external_id,
                payload_json,
                error_text
            FROM posting_jobs
            WHERE document_id = ?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()

    def _load_json(self, value: Any) -> dict[str, Any]:
        text = normalize_text(value)
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        return {}

    def _save_raw_result(
        self,
        conn: sqlite3.Connection,
        *,
        document_id: str,
        raw_result: dict[str, Any],
    ) -> None:
        conn.execute(
            """
            UPDATE documents
            SET raw_result = ?, updated_at = ?
            WHERE document_id = ?
            """,
            (
                json.dumps(raw_result, ensure_ascii=False),
                utc_now_iso(),
                document_id,
            ),
        )

    def _set_document_review_status(
        self,
        conn: sqlite3.Connection,
        *,
        document_id: str,
        review_status: str,
    ) -> None:
        conn.execute(
            """
            UPDATE documents
            SET review_status = ?, updated_at = ?
            WHERE document_id = ?
            """,
            (
                review_status,
                utc_now_iso(),
                document_id,
            ),
        )

    def _ensure_action_history(self, raw_result: dict[str, Any]) -> list[dict[str, Any]]:
        history = raw_result.get("review_action_history")
        if not isinstance(history, list):
            history = []
            raw_result["review_action_history"] = history
        return history

    def _append_action_history(
        self,
        raw_result: dict[str, Any],
        *,
        action: str,
        reviewer: str,
        note: str = "",
        extra: dict[str, Any] | None = None,
    ) -> None:
        history = self._ensure_action_history(raw_result)
        item: dict[str, Any] = {
            "action": action,
            "reviewer": reviewer,
            "note": note,
            "timestamp": utc_now_iso(),
        }
        if extra:
            item.update(extra)
        history.append(item)

    def _mark_escalation_resolved(
        self,
        raw_result: dict[str, Any],
        *,
        reviewer: str,
        resolution: str,
        note: str = "",
    ) -> None:
        escalation = raw_result.get("openclaw_escalation_result")
        if not isinstance(escalation, dict):
            escalation = raw_result.get("escalation_result")
        if not isinstance(escalation, dict):
            escalation = {}

        escalation["should_escalate"] = False
        escalation["resolved"] = True
        escalation["resolved_at"] = utc_now_iso()
        escalation["resolved_by"] = reviewer
        escalation["resolution"] = resolution
        if note:
            escalation["resolution_note"] = note

        raw_result["openclaw_escalation_result"] = escalation

    def _posting_status_after_approval(self, posting_row: sqlite3.Row | None) -> str:
        if posting_row is None:
            return ""

        current_status = normalize_text(posting_row["posting_status"]).lower()
        external_id = normalize_text(posting_row["external_id"])

        if external_id or current_status == "posted":
            return "posted"

        if current_status in {"draft", "failed", "error", "ready_to_post", ""}:
            return "ready_to_post"

        return normalize_text(posting_row["posting_status"])

    def _update_posting_job_for_approval(
        self,
        conn: sqlite3.Connection,
        *,
        document_id: str,
        reviewer: str,
    ) -> None:
        posting_row = self._get_posting_job(conn, document_id)
        if posting_row is None:
            return

        new_status = self._posting_status_after_approval(posting_row)

        conn.execute(
            """
            UPDATE posting_jobs
            SET
                approval_state = ?,
                reviewer = ?,
                posting_status = ?,
                error_text = CASE
                    WHEN COALESCE(posting_status, '') IN ('failed', 'error') THEN ''
                    ELSE error_text
                END
            WHERE document_id = ?
            """,
            (
                "approved_for_posting",
                reviewer,
                new_status,
                document_id,
            ),
        )

        # FIX 5: Keep payload_json in sync after column updates
        from src.agents.tools.posting_builder import sync_posting_payload
        sync_posting_payload(conn, document_id=document_id)

    def _update_posting_job_for_hold(
        self,
        conn: sqlite3.Connection,
        *,
        document_id: str,
        reviewer: str,
        reason: str,
    ) -> None:
        posting_row = self._get_posting_job(conn, document_id)
        if posting_row is None:
            return

        current_status = normalize_text(posting_row["posting_status"]).lower()
        external_id = normalize_text(posting_row["external_id"])

        if external_id or current_status == "posted":
            next_status = "posted"
        elif current_status in {"ready_to_post", "failed", "error", "draft", ""}:
            next_status = "draft"
        else:
            next_status = normalize_text(posting_row["posting_status"])

        conn.execute(
            """
            UPDATE posting_jobs
            SET
                approval_state = ?,
                reviewer = ?,
                posting_status = ?,
                error_text = ?
            WHERE document_id = ?
            """,
            (
                "pending_human_approval",
                reviewer,
                next_status,
                reason,
                document_id,
            ),
        )

    def get_document_status(self, document_id: str) -> dict[str, Any]:
        conn = open_db(self.db_path)
        try:
            document_row = self._get_document(conn, document_id)
            if document_row is None:
                return {
                    "status": "error",
                    "message": f"Document not found: {document_id}",
                    "document_id": document_id,
                }

            posting_row = self._get_posting_job(conn, document_id)
            raw_result = self._load_json(document_row["raw_result"])

            escalation = raw_result.get("openclaw_escalation_result")
            if not isinstance(escalation, dict):
                escalation = raw_result.get("escalation_result")
            if not isinstance(escalation, dict):
                escalation = {}

            return {
                "status": "ok",
                "document": {
                    "document_id": document_row["document_id"],
                    "file_name": document_row["file_name"],
                    "review_status": normalize_text(document_row["review_status"]),
                    "vendor": normalize_text(document_row["vendor"]),
                    "amount": document_row["amount"],
                    "document_date": normalize_text(document_row["document_date"]),
                    "updated_at": normalize_text(document_row["updated_at"]),
                },
                "posting": {
                    "posting_id": normalize_text(posting_row["posting_id"]) if posting_row else "",
                    "posting_status": normalize_text(posting_row["posting_status"]) if posting_row else "",
                    "approval_state": normalize_text(posting_row["approval_state"]) if posting_row else "",
                    "reviewer": normalize_text(posting_row["reviewer"]) if posting_row else "",
                    "external_id": normalize_text(posting_row["external_id"]) if posting_row else "",
                    "error_text": normalize_text(posting_row["error_text"]) if posting_row else "",
                },
                "escalation": {
                    "should_escalate": bool(escalation.get("should_escalate", False)),
                    "decision": normalize_text(escalation.get("decision")),
                    "escalation_reason": normalize_text(escalation.get("escalation_reason")),
                    "reason": normalize_text(escalation.get("reason")),
                    "provider": normalize_text(escalation.get("provider")),
                    "confidence": escalation.get("confidence", 0.0) or 0.0,
                    "resolved": bool(escalation.get("resolved", False)),
                    "resolved_at": normalize_text(escalation.get("resolved_at")),
                    "resolved_by": normalize_text(escalation.get("resolved_by")),
                    "resolution": normalize_text(escalation.get("resolution")),
                },
            }
        finally:
            conn.close()

    def approve_document(
        self,
        *,
        document_id: str,
        reviewer: str = "Sam",
        note: str = "",
    ) -> dict[str, Any]:
        conn = open_db(self.db_path)
        try:
            document_row = self._get_document(conn, document_id)
            if document_row is None:
                return {
                    "status": "error",
                    "message": f"Document not found: {document_id}",
                    "document_id": document_id,
                }

            raw_result = self._load_json(document_row["raw_result"])
            self._append_action_history(
                raw_result,
                action="approve_document",
                reviewer=reviewer,
                note=note,
            )
            self._mark_escalation_resolved(
                raw_result,
                reviewer=reviewer,
                resolution="approved",
                note=note,
            )

            self._set_document_review_status(
                conn,
                document_id=document_id,
                review_status="Ready",
            )
            self._update_posting_job_for_approval(
                conn,
                document_id=document_id,
                reviewer=reviewer,
            )
            self._save_raw_result(
                conn,
                document_id=document_id,
                raw_result=raw_result,
            )

            conn.commit()
            return {
                "status": "ok",
                "action": "approve_document",
                "document_id": document_id,
                "reviewer": reviewer,
                "note": note,
                "document_status": self.get_document_status(document_id),
            }
        finally:
            conn.close()

    def hold_document(
        self,
        *,
        document_id: str,
        reviewer: str = "Sam",
        reason: str = "",
    ) -> dict[str, Any]:
        conn = open_db(self.db_path)
        try:
            document_row = self._get_document(conn, document_id)
            if document_row is None:
                return {
                    "status": "error",
                    "message": f"Document not found: {document_id}",
                    "document_id": document_id,
                }

            hold_reason = reason or "Held for manual review."

            raw_result = self._load_json(document_row["raw_result"])
            self._append_action_history(
                raw_result,
                action="hold_document",
                reviewer=reviewer,
                note=hold_reason,
            )

            escalation = raw_result.get("openclaw_escalation_result")
            if not isinstance(escalation, dict):
                escalation = raw_result.get("escalation_result")
            if not isinstance(escalation, dict):
                escalation = {}

            escalation["should_escalate"] = True
            escalation["decision"] = "hold"
            escalation["escalation_reason"] = "manual_hold"
            escalation["reason"] = hold_reason
            escalation["provider"] = "manual_review"
            escalation["confidence"] = 1.0
            escalation["resolved"] = False
            escalation["updated_at"] = utc_now_iso()
            escalation["updated_by"] = reviewer
            raw_result["openclaw_escalation_result"] = escalation

            self._set_document_review_status(
                conn,
                document_id=document_id,
                review_status="NeedsReview",
            )
            self._update_posting_job_for_hold(
                conn,
                document_id=document_id,
                reviewer=reviewer,
                reason=hold_reason,
            )
            self._save_raw_result(
                conn,
                document_id=document_id,
                raw_result=raw_result,
            )

            conn.commit()
            return {
                "status": "ok",
                "action": "hold_document",
                "document_id": document_id,
                "reviewer": reviewer,
                "reason": hold_reason,
                "document_status": self.get_document_status(document_id),
            }
        finally:
            conn.close()

    def resolve_escalation(
        self,
        *,
        document_id: str,
        resolution: str,
        reviewer: str = "Sam",
        note: str = "",
    ) -> dict[str, Any]:
        normalized = normalize_text(resolution).lower()

        if normalized in {"approve", "approved", "ready", "release"}:
            return self.approve_document(
                document_id=document_id,
                reviewer=reviewer,
                note=note or "Escalation resolved by approval.",
            )

        if normalized in {"hold", "held", "review", "manual_review"}:
            return self.hold_document(
                document_id=document_id,
                reviewer=reviewer,
                reason=note or "Escalation resolved by keeping document on hold.",
            )

        return {
            "status": "error",
            "message": f"Unsupported resolution: {resolution}",
            "document_id": document_id,
            "supported_resolutions": ["approve", "hold"],
        }

    def retry_posting(
        self,
        *,
        document_id: str,
        reviewer: str = "OpenClawCaseOrchestrator",
        note: str = "",
    ) -> dict[str, Any]:
        conn = open_db(self.db_path)
        try:
            document_row = self._get_document(conn, document_id)
            if document_row is None:
                return {
                    "status": "error",
                    "message": f"Document not found: {document_id}",
                    "document_id": document_id,
                }

            posting_row = self._get_posting_job(conn, document_id)
            if posting_row is None:
                return {
                    "status": "error",
                    "message": f"No posting job found for document: {document_id}",
                    "document_id": document_id,
                }

            external_id = normalize_text(posting_row["external_id"])
            current_status = normalize_text(posting_row["posting_status"]).lower()

            if external_id or current_status == "posted":
                return {
                    "status": "error",
                    "message": "Cannot retry a posting job that is already posted.",
                    "document_id": document_id,
                    "external_id": external_id,
                    "posting_status": normalize_text(posting_row["posting_status"]),
                }

            raw_result = self._load_json(document_row["raw_result"])
            self._append_action_history(
                raw_result,
                action="retry_posting",
                reviewer=reviewer,
                note=note,
            )
            self._mark_escalation_resolved(
                raw_result,
                reviewer=reviewer,
                resolution="retry_posting",
                note=note,
            )

            conn.execute(
                """
                UPDATE posting_jobs
                SET
                    posting_status = ?,
                    approval_state = ?,
                    reviewer = ?,
                    error_text = ''
                WHERE document_id = ?
                """,
                (
                    "ready_to_post",
                    "approved_for_posting",
                    reviewer,
                    document_id,
                ),
            )

            self._set_document_review_status(
                conn,
                document_id=document_id,
                review_status="Ready",
            )
            self._save_raw_result(
                conn,
                document_id=document_id,
                raw_result=raw_result,
            )

            conn.commit()
            return {
                "status": "ok",
                "action": "retry_posting",
                "document_id": document_id,
                "reviewer": reviewer,
                "note": note,
                "document_status": self.get_document_status(document_id),
            }
        finally:
            conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.agents.core.review_actions",
        description="Manual review actions for the OpenClaw accounting review workflow.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--document-id", required=True)

    approve_parser = subparsers.add_parser("approve")
    approve_parser.add_argument("--document-id", required=True)
    approve_parser.add_argument("--reviewer", default="Sam")
    approve_parser.add_argument("--note", default="")

    hold_parser = subparsers.add_parser("hold")
    hold_parser.add_argument("--document-id", required=True)
    hold_parser.add_argument("--reviewer", default="Sam")
    hold_parser.add_argument("--reason", default="Held for manual review.")

    resolve_parser = subparsers.add_parser("resolve-escalation")
    resolve_parser.add_argument("--document-id", required=True)
    resolve_parser.add_argument("--resolution", required=True, choices=["approve", "hold"])
    resolve_parser.add_argument("--reviewer", default="Sam")
    resolve_parser.add_argument("--note", default="")

    retry_parser = subparsers.add_parser("retry-posting")
    retry_parser.add_argument("--document-id", required=True)
    retry_parser.add_argument("--reviewer", default="OpenClawCaseOrchestrator")
    retry_parser.add_argument("--note", default="")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    actions = ReviewActions()

    if args.command == "status":
        result = actions.get_document_status(
            document_id=args.document_id,
        )
    elif args.command == "approve":
        result = actions.approve_document(
            document_id=args.document_id,
            reviewer=args.reviewer,
            note=args.note,
        )
    elif args.command == "hold":
        result = actions.hold_document(
            document_id=args.document_id,
            reviewer=args.reviewer,
            reason=args.reason,
        )
    elif args.command == "resolve-escalation":
        result = actions.resolve_escalation(
            document_id=args.document_id,
            resolution=args.resolution,
            reviewer=args.reviewer,
            note=args.note,
        )
    elif args.command == "retry-posting":
        result = actions.retry_posting(
            document_id=args.document_id,
            reviewer=args.reviewer,
            note=args.note,
        )
    else:
        result = {
            "status": "error",
            "message": f"Unknown command: {args.command}",
        }

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()