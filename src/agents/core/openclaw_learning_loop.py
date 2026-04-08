from __future__ import annotations

import inspect
import json
import sqlite3
import sys
from dataclasses import dataclass, asdict
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
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_learning_events_table(db_path: Path = DB_PATH) -> None:
    with open_db(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS openclaw_learning_events (
                event_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                vendor TEXT,
                client_code TEXT,
                doc_type TEXT,
                category TEXT,
                gl_account TEXT,
                tax_code TEXT,
                amount REAL,
                confidence REAL,
                source_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_openclaw_learning_events_document_id
            ON openclaw_learning_events(document_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_openclaw_learning_events_vendor
            ON openclaw_learning_events(vendor)
            """
        )
        conn.commit()


@dataclass
class LearningEvent:
    event_id: str
    document_id: str
    event_type: str
    vendor: str
    client_code: str
    doc_type: str
    category: str
    gl_account: str
    tax_code: str
    amount: float | None
    confidence: float | None
    source_json: dict[str, Any]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OpenClawLearningLoop:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        ensure_learning_events_table(db_path)

    def _fetch_document(self, document_id: str) -> sqlite3.Row | None:
        with open_db_readonly(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM documents
                WHERE document_id = ?
                """,
                (document_id,),
            ).fetchone()
        return row

    def _fetch_posting_job(self, document_id: str) -> sqlite3.Row | None:
        with open_db_readonly(self.db_path) as conn:
            if not table_exists(conn, "posting_jobs"):
                return None

            row = conn.execute(
                """
                SELECT *
                FROM posting_jobs
                WHERE document_id = ?
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """,
                (document_id,),
            ).fetchone()
        return row

    def _fetch_latest_orchestrator_run(self, document_id: str) -> sqlite3.Row | None:
        with open_db_readonly(self.db_path) as conn:
            if not table_exists(conn, "orchestrator_runs"):
                return None

            row = conn.execute(
                """
                SELECT *
                FROM orchestrator_runs
                WHERE document_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (document_id,),
            ).fetchone()
        return row

    def _event_exists(self, event_id: str) -> bool:
        with open_db_readonly(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM openclaw_learning_events
                WHERE event_id = ?
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
        return row is not None

    def _insert_learning_event(self, event: LearningEvent) -> None:
        with open_db(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO openclaw_learning_events (
                    event_id,
                    document_id,
                    event_type,
                    vendor,
                    client_code,
                    doc_type,
                    category,
                    gl_account,
                    tax_code,
                    amount,
                    confidence,
                    source_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.document_id,
                    event.event_type,
                    event.vendor,
                    event.client_code,
                    event.doc_type,
                    event.category,
                    event.gl_account,
                    event.tax_code,
                    event.amount,
                    event.confidence,
                    json.dumps(event.source_json, ensure_ascii=False),
                    event.created_at,
                ),
            )
            conn.commit()

    def _record_vendor_memory(self, event: LearningEvent) -> dict[str, Any]:
        from src.agents.core.vendor_memory_store import VendorMemoryStore

        if not event.vendor or not event.gl_account or not event.tax_code:
            return {
                "status": "skipped",
                "reason": "missing_vendor_or_accounting_fields",
            }

        store = VendorMemoryStore(db_path=self.db_path)

        if not hasattr(store, "record_approval"):
            return {
                "status": "skipped",
                "reason": "record_approval_not_available",
            }

        method = store.record_approval
        sig = inspect.signature(method)
        accepted = set(sig.parameters.keys())

        candidates = {
            "client_code": event.client_code,
            "client": event.client_code,
            "vendor": event.vendor,
            "vendor_name": event.vendor,
            "gl_account": event.gl_account,
            "account": event.gl_account,
            "tax_code": event.tax_code,
            "tax": event.tax_code,
            "doc_type": event.doc_type,
            "document_type": event.doc_type,
            "category": event.category,
            "amount": event.amount,
            "confidence": event.confidence,
        }

        kwargs: dict[str, Any] = {}
        for key, value in candidates.items():
            if key in accepted and value not in (None, ""):
                kwargs[key] = value

        if "vendor" not in kwargs and "vendor_name" not in kwargs:
            return {
                "status": "skipped",
                "reason": "vendor_parameter_not_supported",
                "accepted_parameters": sorted(accepted),
            }

        try:
            method(**kwargs)
            return {
                "status": "recorded",
                "target": "vendor_memory",
                "used_kwargs": kwargs,
            }
        except Exception as exc:
            return {
                "status": "failed",
                "target": "vendor_memory",
                "error": str(exc),
                "used_kwargs": kwargs,
            }

    def _record_learning_memory(self, event: LearningEvent) -> dict[str, Any]:
        from src.agents.core.learning_memory_store import LearningMemoryStore

        store = LearningMemoryStore(db_path=self.db_path)

        payload = {
            "document_id": event.document_id,
            "event_type": event.event_type,
            "vendor": event.vendor,
            "client_code": event.client_code,
            "doc_type": event.doc_type,
            "category": event.category,
            "gl_account": event.gl_account,
            "tax_code": event.tax_code,
            "amount": event.amount,
            "confidence": event.confidence,
            "created_at": event.created_at,
        }

        method_names = [
            "record_feedback",
            "record_event",
            "add_feedback",
            "store_feedback",
            "remember_feedback",
        ]

        for method_name in method_names:
            if not hasattr(store, method_name):
                continue

            method = getattr(store, method_name)

            try:
                sig = inspect.signature(method)
                accepted = list(sig.parameters.keys())

                if len(accepted) == 1:
                    method(payload)
                else:
                    kwargs = {k: v for k, v in payload.items() if k in accepted}
                    method(**kwargs)

                return {
                    "status": "recorded",
                    "target": "learning_memory",
                    "method": method_name,
                }
            except Exception as exc:
                return {
                    "status": "failed",
                    "target": "learning_memory",
                    "method": method_name,
                    "error": str(exc),
                }

        return {
            "status": "skipped",
            "reason": "no_supported_learning_memory_method",
        }

    def _build_event_from_document(self, document_id: str) -> LearningEvent | None:
        document_row = self._fetch_document(document_id)
        if document_row is None:
            raise ValueError(f"Document not found: {document_id}")

        posting_row = self._fetch_posting_job(document_id)
        orchestrator_row = self._fetch_latest_orchestrator_run(document_id)

        raw_result = safe_json_loads(document_row["raw_result"])
        auto_result = safe_json_loads(raw_result.get("auto_approval_result"))
        exception_result = safe_json_loads(raw_result.get("exception_router_result"))
        duplicate_result = safe_json_loads(raw_result.get("duplicate_guard_result"))

        posting_payload = safe_json_loads(posting_row["payload_json"]) if posting_row is not None else {}
        orchestrator_result = safe_json_loads(orchestrator_row["result_json"]) if orchestrator_row is not None else {}

        review_status = normalize_text(document_row["review_status"])
        posting_status = normalize_text(posting_row["posting_status"]) if posting_row else ""
        approval_state = normalize_text(posting_row["approval_state"]) if posting_row else ""
        decision = normalize_text(auto_result.get("decision"))
        route_action = normalize_text(exception_result.get("action"))
        duplicate_risk = normalize_text(duplicate_result.get("risk_level"))

        if posting_status == "posted":
            event_type = "posted_successfully"
        elif approval_state == "approved_for_posting" and posting_status == "ready_to_post":
            event_type = "approved_ready_to_post"
        elif approval_state == "approved_for_posting" and posting_status == "draft":
            event_type = "approved_but_held"
        elif review_status == "NeedsReview":
            event_type = "held_for_review"
        elif review_status == "Exception":
            event_type = "marked_exception"
        elif decision == "auto_post":
            event_type = "auto_post_decision"
        elif decision == "approve_but_hold":
            event_type = "approve_but_hold_decision"
        else:
            event_type = "observed_case"

        vendor = normalize_text(posting_payload.get("vendor")) or normalize_text(document_row["vendor"])
        client_code = normalize_text(posting_payload.get("client_code")) or normalize_text(document_row["client_code"])
        doc_type = normalize_text(posting_payload.get("doc_type")) or normalize_text(document_row["doc_type"])
        category = normalize_text(posting_payload.get("category")) or normalize_text(document_row["category"])
        gl_account = normalize_text(posting_payload.get("gl_account")) or normalize_text(document_row["gl_account"])
        tax_code = normalize_text(posting_payload.get("tax_code")) or normalize_text(document_row["tax_code"])

        try:
            amount = float(posting_payload.get("amount")) if posting_payload.get("amount") is not None else (
                float(document_row["amount"]) if document_row["amount"] is not None else None
            )
        except Exception:
            amount = None

        try:
            confidence = float(posting_payload.get("confidence")) if posting_payload.get("confidence") is not None else (
                float(document_row["confidence"]) if document_row["confidence"] is not None else None
            )
        except Exception:
            confidence = None

        fingerprint = "|".join(
            [
                normalize_text(document_id),
                event_type,
                vendor,
                client_code,
                doc_type,
                category,
                gl_account,
                tax_code,
                posting_status,
                approval_state,
                route_action,
                duplicate_risk,
            ]
        )

        event_id = f"learn_{abs(hash(fingerprint))}"

        return LearningEvent(
            event_id=event_id,
            document_id=document_id,
            event_type=event_type,
            vendor=vendor,
            client_code=client_code,
            doc_type=doc_type,
            category=category,
            gl_account=gl_account,
            tax_code=tax_code,
            amount=amount,
            confidence=confidence,
            source_json={
                "review_status": review_status,
                "posting_status": posting_status,
                "approval_state": approval_state,
                "decision": decision,
                "route_action": route_action,
                "duplicate_risk": duplicate_risk,
                "auto_result": auto_result,
                "exception_result": exception_result,
                "duplicate_result": duplicate_result,
                "orchestrator_result": orchestrator_result,
                "posting_payload": posting_payload,
            },
            created_at=utc_now_iso(),
        )

    def learn_from_document(self, document_id: str) -> dict[str, Any]:
        event = self._build_event_from_document(document_id)
        if event is None:
            return {
                "ok": False,
                "document_id": document_id,
                "reason": "event_not_built",
            }

        if self._event_exists(event.event_id):
            return {
                "ok": True,
                "document_id": document_id,
                "status": "already_recorded",
                "event_id": event.event_id,
                "event_type": event.event_type,
            }

        self._insert_learning_event(event)

        vendor_memory_result = self._record_vendor_memory(event)
        learning_memory_result = self._record_learning_memory(event)

        ok = vendor_memory_result.get("status") != "failed" and learning_memory_result.get("status") != "failed"

        return {
            "ok": ok,
            "document_id": document_id,
            "status": "recorded",
            "event_id": event.event_id,
            "event_type": event.event_type,
            "vendor_memory_result": vendor_memory_result,
            "learning_memory_result": learning_memory_result,
        }

    def learn_from_recent_documents(self, limit: int = 20) -> dict[str, Any]:
        with open_db_readonly(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT document_id
                FROM documents
                WHERE COALESCE(document_id, '') != ''
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()

        results: list[dict[str, Any]] = []
        recorded = 0
        already_recorded = 0
        failed = 0

        for row in rows:
            document_id = normalize_text(row["document_id"])
            try:
                result = self.learn_from_document(document_id)
                results.append(result)

                status = normalize_text(result.get("status"))
                if status == "recorded":
                    recorded += 1
                elif status == "already_recorded":
                    already_recorded += 1

                if not result.get("ok", False):
                    failed += 1
            except Exception as exc:
                failed += 1
                results.append(
                    {
                        "ok": False,
                        "document_id": document_id,
                        "status": "failed",
                        "error": str(exc),
                    }
                )

        return {
            "ok": failed == 0,
            "summary": {
                "documents_seen": len(rows),
                "recorded": recorded,
                "already_recorded": already_recorded,
                "failed": failed,
            },
            "results": results,
        }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA OpenClaw learning loop")
    parser.add_argument("--document-id", default="")
    parser.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    loop = OpenClawLearningLoop()

    if normalize_text(args.document_id):
        result = loop.learn_from_document(normalize_text(args.document_id))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1

    result = loop.learn_from_recent_documents(limit=max(1, int(args.limit)))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())