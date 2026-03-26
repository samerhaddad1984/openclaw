from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.auto_approval_engine import AutoApprovalEngine
from src.agents.core.duplicate_guard import DuplicateGuard
from src.agents.core.exception_router import ExceptionRouter
from src.agents.core.vendor_decision_enricher import VendorDecisionEnricher

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def safe_json_load(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def save_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def fetch_documents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM documents
        ORDER BY created_at ASC
        """
    ).fetchall()


def fetch_document(conn: sqlite3.Connection, document_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM documents
        WHERE document_id = ?
        """,
        (document_id,),
    ).fetchone()


def fetch_posting_job(conn: sqlite3.Connection, document_id: str) -> dict[str, Any] | None:
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

    if not row:
        return None

    return dict(row)


def update_document_raw_result(
    conn: sqlite3.Connection,
    *,
    document_id: str,
    raw_result: dict[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE documents
        SET
            raw_result = ?,
            updated_at = ?
        WHERE document_id = ?
        """,
        (
            save_json(raw_result),
            utc_now_iso(),
            document_id,
        ),
    )


def update_document_review_status(
    conn: sqlite3.Connection,
    *,
    document_id: str,
    review_status: str,
    raw_result: dict[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE documents
        SET
            review_status = ?,
            raw_result = ?,
            updated_at = ?
        WHERE document_id = ?
        """,
        (
            review_status,
            save_json(raw_result),
            utc_now_iso(),
            document_id,
        ),
    )


def create_posting_job(conn: sqlite3.Connection, document: dict[str, Any], route_result: dict[str, Any]) -> None:
    now = utc_now_iso()
    posting_id = f"post_qbo_expense_{document['document_id']}"

    payload = {
        "posting_id": posting_id,
        "document_id": document["document_id"],
        "target_system": "qbo",
        "entry_kind": "expense",
        "file_name": document.get("file_name"),
        "file_path": document.get("file_path"),
        "client_code": document.get("client_code"),
        "vendor": document.get("vendor"),
        "document_date": document.get("document_date"),
        "amount": document.get("amount"),
        "currency": document.get("currency") or "CAD",
        "doc_type": document.get("doc_type"),
        "category": document.get("category"),
        "gl_account": document.get("gl_account"),
        "tax_code": document.get("tax_code"),
        "memo": document.get("memo") or normalize_text(document.get("vendor")),
        "review_status": document.get("review_status"),
        "confidence": document.get("confidence"),
        "approval_state": "approved_for_posting",
        "posting_status": "ready_to_post",
        "reviewer": "ExceptionRouter",
        "blocking_issues": [],
        "notes": [
            "auto_created_from_rebuild",
            f"route_action:{route_result.get('action', '')}",
            f"route_reason:{' | '.join(route_result.get('reasons', []) or [])}",
        ],
        "created_at": now,
        "updated_at": now,
    }

    conn.execute(
        """
        INSERT INTO posting_jobs
        (
            posting_id,
            document_id,
            target_system,
            entry_kind,
            posting_status,
            approval_state,
            reviewer,
            external_id,
            payload_json,
            error_text,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            posting_id,
            document["document_id"],
            "qbo",
            "expense",
            "ready_to_post",
            "approved_for_posting",
            "ExceptionRouter",
            None,
            json.dumps(payload, ensure_ascii=False),
            None,
            now,
            now,
        ),
    )


def update_posting_job(
    conn: sqlite3.Connection,
    *,
    posting_job: dict[str, Any],
    approval_state: str,
    posting_status: str,
    reviewer: str,
    route_result: dict[str, Any],
) -> None:
    payload = safe_json_load(posting_job.get("payload_json"))
    notes = payload.get("notes", []) or []

    notes.append(f"route_action:{route_result.get('action', '')}")
    for reason in route_result.get("reasons", []) or []:
        notes.append(f"route_reason:{reason}")

    payload["notes"] = list(dict.fromkeys(notes))
    payload["approval_state"] = approval_state
    payload["posting_status"] = posting_status
    payload["reviewer"] = reviewer
    payload["updated_at"] = utc_now_iso()

    conn.execute(
        """
        UPDATE posting_jobs
        SET
            approval_state = ?,
            posting_status = ?,
            reviewer = ?,
            payload_json = ?,
            updated_at = ?
        WHERE posting_id = ?
        """,
        (
            approval_state,
            posting_status,
            reviewer,
            json.dumps(payload, ensure_ascii=False),
            utc_now_iso(),
            posting_job["posting_id"],
        ),
    )


def make_router_compatible_auto_result(auto_result: dict[str, Any]) -> dict[str, Any]:
    decision = normalize_text(auto_result.get("decision"))

    compatible = dict(auto_result)

    if decision == "auto_post":
        compatible["auto_approved"] = True
        compatible["recommended_approval_state"] = "approved_for_posting"
        compatible["recommended_posting_status"] = "ready_to_post"

    elif decision == "approve_but_hold":
        compatible["auto_approved"] = True
        compatible["recommended_approval_state"] = "approved_for_posting"
        compatible["recommended_posting_status"] = "draft"

    elif decision == "block_posting":
        compatible["auto_approved"] = False
        compatible["recommended_approval_state"] = "pending_human_approval"
        compatible["recommended_posting_status"] = "draft"

    else:
        compatible["auto_approved"] = False
        compatible["recommended_approval_state"] = "pending_human_approval"
        compatible["recommended_posting_status"] = "draft"

    return compatible


def build_route_result(
    *,
    document: dict[str, Any],
    auto_result: dict[str, Any],
    duplicate_result: dict[str, Any],
    exception_router: ExceptionRouter,
) -> dict[str, Any]:
    decision = normalize_text(auto_result.get("decision"))

    if decision == "block_posting":
        return {
            "action": "block_posting",
            "reasons": ["auto_approval_engine_block_posting"],
        }

    return exception_router.route(
        document=document,
        auto_approval_result=make_router_compatible_auto_result(auto_result),
        duplicate_guard_result=duplicate_result,
    )


def rebuild() -> None:
    conn = open_db()

    vendor_enricher = VendorDecisionEnricher()
    approval_engine = AutoApprovalEngine()
    duplicate_guard = DuplicateGuard()
    exception_router = ExceptionRouter()

    documents = fetch_documents(conn)

    scanned = 0
    updated = 0
    skipped_missing_document_id = 0

    documents_auto_post = 0
    documents_approve_but_hold = 0
    documents_needs_review = 0
    documents_block_posting = 0

    posting_jobs_created = 0
    posting_jobs_ready = 0
    posting_jobs_held = 0
    posting_jobs_blocked = 0

    for row in documents:
        scanned += 1

        document = dict(row)
        document_id = normalize_text(document.get("document_id"))

        if not document_id:
            skipped_missing_document_id += 1
            print("[SKIPPED] missing document_id")
            continue

        document["document_id"] = document_id
        document["raw_result"] = safe_json_load(document.get("raw_result"))

        # 1. Vendor enrichment
        enriched = vendor_enricher.enrich_document(document)
        enriched_document = dict(enriched.get("enriched_document", {}))
        enriched_document["document_id"] = document_id

        enriched_raw_result = safe_json_load(enriched_document.get("raw_result"))

        # 2. Duplicate guard
        duplicate_result = duplicate_guard.evaluate_document(document_id=document_id)
        enriched_raw_result["duplicate_guard_result"] = duplicate_result

        # Save intermediate state because AutoApprovalEngine v2 reads from DB
        update_document_raw_result(
            conn,
            document_id=document_id,
            raw_result=enriched_raw_result,
        )
        conn.commit()

        # 3. Auto Approval v2
        auto_result = approval_engine.evaluate_document(document_id)
        auto_result = safe_json_load(auto_result)

        # Reload latest row because auto_approval_engine writes into raw_result itself
        refreshed_row = fetch_document(conn, document_id)
        refreshed_document = dict(refreshed_row) if refreshed_row else dict(enriched_document)
        refreshed_raw_result = safe_json_load(refreshed_document.get("raw_result"))

        # Ensure duplicate result is still present
        refreshed_raw_result["duplicate_guard_result"] = duplicate_result
        refreshed_raw_result["auto_approval_result"] = auto_result
        refreshed_document["raw_result"] = refreshed_raw_result
        refreshed_document["document_id"] = document_id

        # 4. Exception router
        route_result = build_route_result(
            document=refreshed_document,
            auto_result=auto_result,
            duplicate_result=duplicate_result,
            exception_router=exception_router,
        )
        refreshed_raw_result["exception_router_result"] = route_result

        posting_job = fetch_posting_job(conn, document_id)

        action = normalize_text(route_result.get("action"))
        decision = normalize_text(auto_result.get("decision"))

        if decision == "auto_post":
            documents_auto_post += 1
        elif decision == "approve_but_hold":
            documents_approve_but_hold += 1
        elif decision == "block_posting":
            documents_block_posting += 1
        else:
            documents_needs_review += 1

        # 5. Apply final workflow action
        if action == "auto_post":
            review_status = "Ready"

            if posting_job is None:
                create_posting_job(conn, refreshed_document, route_result)
                posting_jobs_created += 1
            else:
                update_posting_job(
                    conn,
                    posting_job=posting_job,
                    approval_state="approved_for_posting",
                    posting_status="ready_to_post",
                    reviewer="ExceptionRouter",
                    route_result=route_result,
                )
                posting_jobs_ready += 1

        elif action == "approve_but_hold":
            review_status = "Ready"

            if posting_job is not None:
                update_posting_job(
                    conn,
                    posting_job=posting_job,
                    approval_state="approved_for_posting",
                    posting_status="draft",
                    reviewer="ExceptionRouter",
                    route_result=route_result,
                )
                posting_jobs_held += 1

        elif action == "block_posting":
            review_status = "NeedsReview"

            if posting_job is not None:
                update_posting_job(
                    conn,
                    posting_job=posting_job,
                    approval_state="pending_human_approval",
                    posting_status="draft",
                    reviewer="ExceptionRouter",
                    route_result=route_result,
                )
                posting_jobs_blocked += 1

        else:
            review_status = "NeedsReview"

            if posting_job is not None:
                update_posting_job(
                    conn,
                    posting_job=posting_job,
                    approval_state="pending_human_approval",
                    posting_status="draft",
                    reviewer="ExceptionRouter",
                    route_result=route_result,
                )
                posting_jobs_held += 1

        update_document_review_status(
            conn,
            document_id=document_id,
            review_status=review_status,
            raw_result=refreshed_raw_result,
        )
        updated += 1

    conn.commit()
    conn.close()

    print(
        json.dumps(
            {
                "documents_scanned": scanned,
                "documents_updated": updated,
                "documents_skipped_missing_document_id": skipped_missing_document_id,
                "documents_decision_auto_post": documents_auto_post,
                "documents_decision_approve_but_hold": documents_approve_but_hold,
                "documents_decision_needs_review": documents_needs_review,
                "documents_decision_block_posting": documents_block_posting,
                "posting_jobs_created": posting_jobs_created,
                "posting_jobs_ready": posting_jobs_ready,
                "posting_jobs_held": posting_jobs_held,
                "posting_jobs_blocked": posting_jobs_blocked,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    rebuild()