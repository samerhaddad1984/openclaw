from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def json_loads_safe(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def json_dumps_stable(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=False)


def row_to_dict(row: sqlite3.Row | Mapping[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(row)


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def first_present(data: Mapping[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return default


def fetch_document_row(conn: sqlite3.Connection, document_id: str) -> dict[str, Any]:
    if not table_exists(conn, "documents"):
        return {}

    row = conn.execute(
        "SELECT * FROM documents WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    return row_to_dict(row)


def fetch_posting_row_by_document_id(
    conn: sqlite3.Connection,
    document_id: str,
) -> dict[str, Any]:
    if not table_exists(conn, "posting_jobs"):
        return {}

    row = conn.execute(
        "SELECT * FROM posting_jobs WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    return row_to_dict(row)


def fetch_posting_row_by_posting_id(
    conn: sqlite3.Connection,
    posting_id: str,
) -> dict[str, Any]:
    if not table_exists(conn, "posting_jobs"):
        return {}

    row = conn.execute(
        "SELECT * FROM posting_jobs WHERE posting_id = ?",
        (posting_id,),
    ).fetchone()
    return row_to_dict(row)


def normalize_string_list(value: Any) -> list[str]:
    raw = json_loads_safe(value, [])
    if not isinstance(raw, list):
        return []

    cleaned: list[str] = []
    for item in raw:
        text = normalize_text(item)
        if text:
            cleaned.append(text)
    return cleaned


def build_payload_from_sources(
    posting_row: Mapping[str, Any],
    document_row: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    posting = row_to_dict(posting_row)
    document = row_to_dict(document_row)

    blocking_issues = normalize_string_list(
        first_present(posting, ["blocking_issues"], [])
    )
    notes = normalize_string_list(first_present(posting, ["notes"], []))

    payload: dict[str, Any] = {
        "posting_id": first_present(posting, ["posting_id"]),
        "document_id": first_present(posting, ["document_id"], first_present(document, ["document_id"])),
        "target_system": first_present(posting, ["target_system"], "qbo"),
        "entry_kind": first_present(posting, ["entry_kind"], "expense"),
        "file_name": first_present(posting, ["file_name"], first_present(document, ["file_name"])),
        "file_path": first_present(posting, ["file_path"], first_present(document, ["file_path"])),
        "client_code": first_present(posting, ["client_code"], first_present(document, ["client_code"])),
        "vendor": first_present(posting, ["vendor"], first_present(document, ["vendor"])),
        "document_date": first_present(posting, ["document_date"], first_present(document, ["document_date"])),
        "amount": first_present(posting, ["amount"], first_present(document, ["amount"])),
        "currency": first_present(posting, ["currency"], first_present(document, ["currency"])),
        "doc_type": first_present(posting, ["doc_type"], first_present(document, ["doc_type"])),
        "category": first_present(posting, ["category"], first_present(document, ["category"])),
        "gl_account": first_present(posting, ["gl_account"], first_present(document, ["gl_account"])),
        "tax_code": first_present(posting, ["tax_code"], first_present(document, ["tax_code"])),
        "memo": first_present(posting, ["memo"]),
        "review_status": first_present(posting, ["review_status"], first_present(document, ["review_status"])),
        "confidence": first_present(posting, ["confidence"], first_present(document, ["confidence"])),
        "approval_state": first_present(posting, ["approval_state"]),
        "posting_status": first_present(posting, ["posting_status"]),
        "reviewer": first_present(posting, ["reviewer"]),
        "blocking_issues": blocking_issues,
        "notes": notes,
        "created_at": first_present(posting, ["created_at"]),
        "updated_at": first_present(posting, ["updated_at"]),
    }

    external_id = first_present(posting, ["external_id"])
    if external_id not in (None, ""):
        payload["external_id"] = external_id

    cleaned_payload: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        cleaned_payload[key] = value

    return cleaned_payload


def sync_posting_payload(
    conn: sqlite3.Connection,
    *,
    document_id: str | None = None,
    posting_id: str | None = None,
    refresh_updated_at: bool = True,
) -> dict[str, Any]:
    if not table_exists(conn, "posting_jobs"):
        return {}
    posting_row: dict[str, Any] = {}
    if posting_id:
        posting_row = fetch_posting_row_by_posting_id(conn, posting_id)
    elif document_id:
        posting_row = fetch_posting_row_by_document_id(conn, document_id)
    if not posting_row:
        return {}
    doc_row = fetch_document_row(conn, normalize_text(posting_row.get("document_id")))
    payload = build_payload_from_sources(posting_row, doc_row)
    now_iso = utc_now_iso()
    update_parts: list[str] = []
    params: list[Any] = []
    if refresh_updated_at:
        payload["updated_at"] = now_iso
        update_parts.append("updated_at = ?")
        params.append(now_iso)
    update_parts.insert(0, "payload_json = ?")
    params.insert(0, json_dumps_stable(payload))
    params.append(posting_row["posting_id"])
    conn.execute(
        f"""
        UPDATE posting_jobs
        SET {", ".join(update_parts)}
        WHERE posting_id = ?
        """,
        params,
    )
    conn.commit()
    refreshed = fetch_posting_row_by_posting_id(conn, str(posting_row["posting_id"]))
    return row_to_dict(refreshed)


def sync_posting_payload_for_document(document_id: str) -> dict[str, Any]:
    conn = open_db()
    try:
        return sync_posting_payload(conn, document_id=document_id)
    finally:
        conn.close()


def sync_all_posting_payloads() -> dict[str, Any]:
    conn = open_db()
    try:
        if not table_exists(conn, "posting_jobs"):
            return {
                "status": "ok",
                "updated_count": 0,
                "posting_ids": [],
            }

        rows = conn.execute(
            "SELECT posting_id FROM posting_jobs ORDER BY created_at, posting_id"
        ).fetchall()

        updated_ids: list[str] = []
        for row in rows:
            posting_id = normalize_text(row["posting_id"])
            if not posting_id:
                continue
            sync_posting_payload(conn, posting_id=posting_id)
            updated_ids.append(posting_id)

        return {
            "status": "ok",
            "updated_count": len(updated_ids),
            "posting_ids": updated_ids,
        }
    finally:
        conn.close()


def ensure_posting_job_table_minimum(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT UNIQUE,
            target_system TEXT,
            entry_kind TEXT,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            document_date TEXT,
            amount REAL,
            currency TEXT,
            doc_type TEXT,
            category TEXT,
            gl_account TEXT,
            tax_code TEXT,
            memo TEXT,
            review_status TEXT,
            confidence REAL,
            approval_state TEXT,
            posting_status TEXT,
            reviewer TEXT,
            blocking_issues TEXT,
            notes TEXT,
            external_id TEXT,
            error_text TEXT,
            payload_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()

    # Ensure error_text column exists on older installs
    cols = table_columns(conn, "posting_jobs")
    if "error_text" not in cols:
        conn.execute("ALTER TABLE posting_jobs ADD COLUMN error_text TEXT")
        conn.commit()

    # Ensure audit_log table exists for trigger-based logging
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            username TEXT,
            document_id TEXT,
            provider TEXT,
            task_type TEXT,
            prompt_snippet TEXT,
            latency_ms REAL,
            created_at TEXT
        )
    """)
    conn.commit()

    # FIX 1: DB-level triggers to enforce state machine — revert invalid
    # posting_status AND log the blocked attempt to audit_log
    conn.executescript("""
        DROP TRIGGER IF EXISTS trg_posting_status_guard;
        CREATE TRIGGER trg_posting_status_guard
        AFTER UPDATE OF posting_status ON posting_jobs
        WHEN NEW.posting_status = 'posted'
         AND COALESCE(NEW.approval_state, '') NOT LIKE '%approved%'
        BEGIN
            UPDATE posting_jobs SET posting_status = OLD.posting_status
            WHERE posting_id = NEW.posting_id;
            INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
            VALUES (
                'invalid_state_blocked',
                NEW.document_id,
                '{"attempted": "posted", "reverted_to": "' || COALESCE(OLD.posting_status, 'draft') || '", "reason": "approval_state not approved", "approval_state": "' || COALESCE(NEW.approval_state, '') || '"}',
                datetime('now')
            );
        END;
    """)

    # Only create the review guard trigger if the documents table exists
    if table_exists(conn, "documents"):
        conn.executescript("""
            DROP TRIGGER IF EXISTS trg_posting_review_guard;
            CREATE TRIGGER trg_posting_review_guard
            AFTER UPDATE OF posting_status ON posting_jobs
            WHEN NEW.posting_status = 'posted'
            BEGIN
                UPDATE posting_jobs SET posting_status = OLD.posting_status
                WHERE posting_id = NEW.posting_id
                  AND (SELECT review_status FROM documents WHERE document_id = NEW.document_id)
                      IN ('Exception', 'NeedsReview', 'OnHold', 'New');
                INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
                SELECT
                    'invalid_state_blocked',
                    NEW.document_id,
                    '{"attempted": "posted", "reverted_to": "' || COALESCE(OLD.posting_status, 'draft') || '", "reason": "review_status blocked", "review_status": "' || COALESCE(d.review_status, '') || '"}',
                    datetime('now')
                FROM documents d
                WHERE d.document_id = NEW.document_id
                  AND d.review_status IN ('Exception', 'NeedsReview', 'OnHold', 'New');
            END;
        """)

    # FIX 6: Auto-log fraud overrides to audit_log when fraud_override_reason is set
    # FIX 7: Reject whitespace-only or too-short reasons (< 10 chars after trim)
    if table_exists(conn, "documents") and table_exists(conn, "audit_log"):
        conn.executescript("""
            DROP TRIGGER IF EXISTS trg_fraud_override_audit;
            CREATE TRIGGER trg_fraud_override_audit
            AFTER UPDATE OF fraud_override_reason ON documents
            WHEN COALESCE(TRIM(NEW.fraud_override_reason), '') != ''
             AND LENGTH(TRIM(COALESCE(NEW.fraud_override_reason, ''))) >= 10
             AND COALESCE(OLD.fraud_override_reason, '') = ''
            BEGIN
                INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
                VALUES (
                    'fraud_override',
                    NEW.document_id,
                    '{"override_reason": "' || REPLACE(NEW.fraud_override_reason, '"', '\\"') || '"}',
                    datetime('now')
                );
            END;
        """)

    # FIX 3: Set fraud_override_locked=1 when override reason is first set
    doc_cols = table_columns(conn, "documents") if table_exists(conn, "documents") else set()
    if "fraud_override_locked" in doc_cols and table_exists(conn, "audit_log"):
        conn.executescript("""
            DROP TRIGGER IF EXISTS trg_fraud_override_lock;
            CREATE TRIGGER trg_fraud_override_lock
            AFTER UPDATE OF fraud_override_reason ON documents
            WHEN COALESCE(TRIM(NEW.fraud_override_reason), '') != ''
             AND LENGTH(TRIM(COALESCE(NEW.fraud_override_reason, ''))) >= 10
             AND COALESCE(OLD.fraud_override_reason, '') = ''
            BEGIN
                UPDATE documents SET fraud_override_locked = 1
                WHERE document_id = NEW.document_id;
            END;

            DROP TRIGGER IF EXISTS trg_fraud_override_immutable;
            CREATE TRIGGER trg_fraud_override_immutable
            BEFORE UPDATE OF fraud_override_reason ON documents
            WHEN OLD.fraud_override_locked = 1
             AND NEW.fraud_override_reason != OLD.fraud_override_reason
            BEGIN
                SELECT RAISE(IGNORE);
            END;

            DROP TRIGGER IF EXISTS trg_fraud_override_immutable_audit;
            CREATE TRIGGER trg_fraud_override_immutable_audit
            AFTER UPDATE OF fraud_override_reason ON documents
            WHEN OLD.fraud_override_locked = 1
             AND NEW.fraud_override_reason != OLD.fraud_override_reason
            BEGIN
                INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
                VALUES (
                    'attempted_reason_change',
                    NEW.document_id,
                    '{"original_reason": "' || REPLACE(OLD.fraud_override_reason, '"', '\\"') || '", "attempted_reason": "' || REPLACE(NEW.fraud_override_reason, '"', '\\"') || '"}',
                    datetime('now')
                );
            END;
        """)

    # FIX 7: Auto-update substance_flags when GL is manually changed
    # Only if the new GL is NOT uncategorized (resetting to uncategorized is not a manual override)
    if table_exists(conn, "documents"):
        conn.executescript("""
            DROP TRIGGER IF EXISTS trg_substance_flags_on_gl_change;
            CREATE TRIGGER trg_substance_flags_on_gl_change
            AFTER UPDATE OF gl_account ON documents
            WHEN COALESCE(NEW.gl_account, '') != COALESCE(OLD.gl_account, '')
             AND COALESCE(OLD.gl_account, '') != ''
             AND LOWER(COALESCE(NEW.gl_account, '')) NOT LIKE '%uncategorized%'
             AND LOWER(COALESCE(NEW.gl_account, '')) NOT IN ('', 'expense', 'other expense')
            BEGIN
                UPDATE documents
                SET substance_flags = json_set(
                    COALESCE(substance_flags, '{}'),
                    '$.manual_override', json('true'),
                    '$.manual_gl', NEW.gl_account,
                    '$.manual_override_at', datetime('now'),
                    '$.override_applied', json('false')
                )
                WHERE document_id = NEW.document_id;
            END;
        """)

    # FIX 5: Auto-sync payload_json when approval_state or posting_status changes
    conn.executescript("""
        DROP TRIGGER IF EXISTS trg_sync_payload_on_approval;
        CREATE TRIGGER trg_sync_payload_on_approval
        AFTER UPDATE OF approval_state, posting_status ON posting_jobs
        WHEN COALESCE(NEW.payload_json, '') != ''
        BEGIN
            UPDATE posting_jobs
            SET payload_json = json_set(
                COALESCE(NEW.payload_json, '{}'),
                '$.approval_state', NEW.approval_state,
                '$.posting_status', NEW.posting_status,
                '$.reviewer', NEW.reviewer,
                '$.updated_at', datetime('now')
            ),
            updated_at = datetime('now')
            WHERE posting_id = NEW.posting_id;
        END;
    """)

    # FIX 4: BEFORE INSERT trigger — block direct SQL INSERT with posting_status='posted'
    # when approval_state is not approved or review_status is blocked
    if table_exists(conn, "documents") and table_exists(conn, "audit_log"):
        conn.executescript("""
            DROP TRIGGER IF EXISTS trg_posting_insert_guard;
            CREATE TRIGGER trg_posting_insert_guard
            AFTER INSERT ON posting_jobs
            WHEN NEW.posting_status = 'posted'
             AND (
                COALESCE(NEW.approval_state, '') NOT LIKE '%approved%'
                OR (SELECT review_status FROM documents WHERE document_id = NEW.document_id)
                   IN ('Exception', 'NeedsReview', 'OnHold', 'New')
             )
            BEGIN
                UPDATE posting_jobs SET posting_status = 'blocked'
                WHERE posting_id = NEW.posting_id;
                INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
                VALUES (
                    'invalid_state_blocked',
                    NEW.document_id,
                    '{"attempted": "posted_via_insert", "corrected_to": "blocked", "approval_state": "' || COALESCE(NEW.approval_state, '') || '"}',
                    datetime('now')
                );
            END;
        """)

    conn.commit()


CREDIT_DOC_TYPES = {"credit_note", "refund", "chargeback", "reversal"}


def infer_entry_kind(document: Mapping[str, Any], explicit_entry_kind: str = "") -> str:
    """FIX 2: Infer entry_kind from doc_type and amount.

    If doc_type is credit_note/refund/chargeback/reversal OR amount < 0,
    return 'credit'. Otherwise return the explicit value or 'expense'.
    """
    if explicit_entry_kind and explicit_entry_kind != "expense":
        return explicit_entry_kind

    doc_type = normalize_text(document.get("doc_type")).lower()
    if doc_type in CREDIT_DOC_TYPES:
        return "credit"

    amount = safe_float(document.get("amount"))
    if amount is not None and amount < 0:
        return "credit"

    return explicit_entry_kind or "expense"


def build_posting_id(document_id: str, entry_kind: str = "expense", target_system: str = "qbo") -> str:
    clean_document_id = normalize_text(document_id)
    clean_entry_kind = normalize_text(entry_kind) or "expense"
    clean_target_system = normalize_text(target_system) or "qbo"
    return f"post_{clean_target_system}_{clean_entry_kind}_{clean_document_id}"


def resolve_document_input(
    conn: sqlite3.Connection,
    document_or_id: Mapping[str, Any] | str,
) -> dict[str, Any]:
    if isinstance(document_or_id, Mapping):
        data = dict(document_or_id)
        document_id = normalize_text(data.get("document_id"))
        if document_id:
            db_row = fetch_document_row(conn, document_id)
            if db_row:
                merged = dict(db_row)
                merged.update({k: v for k, v in data.items() if v is not None})
                return merged
        return data

    document_id = normalize_text(document_or_id)
    if not document_id:
        raise ValueError("document_id is required")

    db_row = fetch_document_row(conn, document_id)
    if not db_row:
        raise ValueError(f"Document not found: {document_id}")

    return db_row


def choose_default_memo(document: Mapping[str, Any]) -> str:
    vendor = normalize_text(document.get("vendor"))
    doc_type = normalize_text(document.get("doc_type"))
    file_name = normalize_text(document.get("file_name"))

    parts = [part for part in [vendor, doc_type, file_name] if part]
    if not parts:
        return ""
    return " | ".join(parts)


def upsert_posting_job(
    conn: sqlite3.Connection,
    *,
    document: Mapping[str, Any],
    target_system: str = "qbo",
    entry_kind: str = "expense",
    reviewer: str = "ExceptionRouter",
    approval_state: str | None = None,
    posting_status: str | None = None,
    blocking_issues: list[str] | None = None,
    notes: list[str] | None = None,
    external_id: str | None = None,
    memo: str | None = None,
) -> dict[str, Any]:
    ensure_posting_job_table_minimum(conn)

    posting_columns = table_columns(conn, "posting_jobs")
    document_id = normalize_text(document.get("document_id"))
    if not document_id:
        raise ValueError("document_id is required to build a posting job")

    existing = fetch_posting_row_by_document_id(conn, document_id)
    now_iso = utc_now_iso()

    effective_target_system = normalize_text(target_system) or normalize_text(existing.get("target_system")) or "qbo"
    # FIX 2: Infer entry_kind from document when not explicitly set to non-expense
    effective_entry_kind = infer_entry_kind(document, normalize_text(entry_kind) or normalize_text(existing.get("entry_kind")) or "expense")
    effective_posting_id = normalize_text(existing.get("posting_id")) or build_posting_id(
        document_id=document_id,
        entry_kind=effective_entry_kind,
        target_system=effective_target_system,
    )

    effective_memo = normalize_text(memo)
    if not effective_memo:
        effective_memo = normalize_text(existing.get("memo"))
    if not effective_memo:
        effective_memo = choose_default_memo(document)

    effective_blocking_issues = blocking_issues
    if effective_blocking_issues is None:
        effective_blocking_issues = normalize_string_list(existing.get("blocking_issues"))
    else:
        effective_blocking_issues = normalize_string_list(effective_blocking_issues)

    effective_notes = notes
    if effective_notes is None:
        effective_notes = normalize_string_list(existing.get("notes"))
    else:
        effective_notes = normalize_string_list(effective_notes)

    # FIX 2: Add warning when entry_kind is inferred as credit
    if effective_entry_kind == "credit" and normalize_text(entry_kind) != "credit":
        credit_note_msg = "Entry kind inferred as credit from document type or negative amount"
        if credit_note_msg not in effective_notes:
            effective_notes.append(credit_note_msg)

    effective_approval_state = normalize_text(approval_state)
    if not effective_approval_state:
        effective_approval_state = normalize_text(existing.get("approval_state"))
    if not effective_approval_state:
        effective_approval_state = "pending_review"

    # FIX 6: Engine-layer fraud check when approving via any code path
    if "approved" in effective_approval_state:
        _check_fraud_flags_for_approval(conn, document, normalize_text(reviewer))

    # FIX 10: Engine-layer period lock check when approving
    if "approved" in effective_approval_state:
        _check_period_not_locked_for_doc(conn, document_id)

    # TRAP 1+4: Amendment & recognition timing checks
    try:
        from src.engines.amendment_engine import (
            get_filed_period_for_date,
            flag_amendment_needed,
            validate_recognition_timing,
        )
        doc_date = normalize_text(document.get("document_date"))
        client = normalize_text(document.get("client_code"))
        # Trap 1: Block direct posting to filed periods — must use correction entries
        filed_period = get_filed_period_for_date(conn, client, doc_date) if client and doc_date else None
        if filed_period and "approved" in effective_approval_state:
            doc_type_lower = normalize_text(document.get("doc_type")).lower()
            # Allow credit notes/corrections to target filed periods (they create amendment flags)
            if doc_type_lower not in ("credit_note", "refund", "chargeback", "reversal", "correction"):
                effective_approval_state = "pending_review"
                effective_posting_status = "blocked"
                block_msg = (
                    f"Document date {doc_date} falls in filed period {filed_period}. "
                    f"Use correction entries in current period instead."
                )
                if block_msg not in effective_blocking_issues:
                    effective_blocking_issues.append(block_msg)
        # Trap 4: Check recognition timing
        recognition_issues = validate_recognition_timing(conn, document_id)
        for issue in recognition_issues.get("issues", []):
            note = f"[RECOGNITION] {issue.get('description_en', '')}"
            if note not in effective_notes:
                effective_notes.append(note)
            if issue.get("issue") == "deferred_recognition_required":
                if "approved" in effective_approval_state:
                    effective_approval_state = "pending_review"
                    block_msg = "Recognition timing: deferred to activation date"
                    if block_msg not in effective_blocking_issues:
                        effective_blocking_issues.append(block_msg)
    except Exception:
        pass

    # TRAP 5: Duplicate cluster check — block non-head members
    try:
        from src.engines.correction_chain import is_duplicate_of_cluster_head
        if is_duplicate_of_cluster_head(conn, document_id):
            effective_approval_state = "pending_review"
            effective_posting_status = "blocked"
            block_msg = "Document is a non-head member of a duplicate cluster — only cluster head should post"
            if block_msg not in effective_blocking_issues:
                effective_blocking_issues.append(block_msg)
    except Exception:
        pass

    # TRAP 3: Overlap anomaly detection for new documents
    try:
        from src.engines.correction_chain import detect_overlap_anomaly
        client = normalize_text(document.get("client_code"))
        if client and not existing:
            overlaps = detect_overlap_anomaly(conn, new_document_id=document_id, client_code=client)
            for overlap in overlaps:
                note = f"[OVERLAP] {overlap.get('description_en', '')}"
                if note not in effective_notes:
                    effective_notes.append(note)
    except Exception:
        pass

    effective_posting_status = normalize_text(posting_status)
    if not effective_posting_status:
        effective_posting_status = normalize_text(existing.get("posting_status"))
    if not effective_posting_status:
        effective_posting_status = "draft"

    effective_external_id = normalize_text(external_id)
    if not effective_external_id:
        effective_external_id = normalize_text(existing.get("external_id"))

    effective_reviewer = normalize_text(reviewer) or normalize_text(existing.get("reviewer")) or "ExceptionRouter"

    # FIX 1+2+3+5: Run substance classifier, override GL, write back to documents
    effective_gl = first_present(document, ["gl_account"], existing.get("gl_account"))
    original_gl = effective_gl
    substance_data: dict[str, Any] = {}
    try:
        from src.engines.substance_engine import substance_classifier, PRIORITY_OVERRIDE_TYPES
        # Use the original document memo for substance detection, not the
        # generated posting memo (which is "vendor | doc_type | filename").
        doc_memo = normalize_text(document.get("memo")) or effective_memo
        substance = substance_classifier(
            vendor=normalize_text(document.get("vendor")),
            memo=doc_memo,
            doc_type=normalize_text(document.get("doc_type")),
            amount=first_present(document, ["amount"], existing.get("amount")),
        )
        substance_data = substance

        gl_lower = normalize_text(effective_gl).lower()
        is_uncategorized = (
            not effective_gl
            or "uncategorized" in gl_lower
            or gl_lower in ("", "expense", "other expense")
        )

        # FIX 2: Determine if GL is in expense range (5000-5999)
        is_expense_range = False
        try:
            gl_num = int(re.match(r"(\d+)", gl_lower).group(1)) if re.match(r"(\d+)", gl_lower) else None
            is_expense_range = gl_num is not None and 5000 <= gl_num <= 5999
        except (ValueError, AttributeError):
            pass

        # FIX 2+5: Determine override eligibility per substance type
        # Security deposit is flagged as potential_prepaid with suggested_gl=1400
        is_security_deposit = (
            substance.get("potential_prepaid")
            and substance.get("suggested_gl") == "1400"
        )
        has_priority_type = (
            substance.get("potential_loan")
            or substance.get("potential_tax_remittance")
            or is_security_deposit
        )
        has_capex_or_prepaid = substance.get("potential_capex") or substance.get("potential_prepaid")
        is_personal = substance.get("potential_personal_expense")
        doc_confidence = safe_float(first_present(document, ["confidence"], existing.get("confidence")))

        # FIX 7: Check if accountant manually overrode GL — respect it for non-PRIORITY types
        existing_substance_flags = json_loads_safe(document.get("substance_flags"), {})
        has_manual_override = isinstance(existing_substance_flags, dict) and existing_substance_flags.get("manual_override")

        should_override = False
        if substance.get("suggested_gl"):
            if has_manual_override:
                # FIX 7: Manual GL override by accountant — always respect it
                should_override = False
            elif is_uncategorized:
                # Original behavior: always override uncategorized
                should_override = True
            elif has_priority_type:
                # FIX 2+5: loan, tax_remittance, security_deposit → always override
                should_override = True
            elif has_capex_or_prepaid and not is_personal:
                # FIX 2: CapEx/prepaid override expense range
                # FIX 5: only if confidence < 0.85 (vendor memory not strong)
                if is_expense_range or is_uncategorized:
                    if doc_confidence is None or doc_confidence < 0.85:
                        should_override = True

        # FIX 2: Personal expenses — flag but never auto-override GL
        if is_personal:
            should_override = False

        if should_override:
            effective_gl = substance["suggested_gl"]

        # Always append review notes regardless of override
        for note in substance.get("review_notes", []):
            if note not in effective_notes:
                effective_notes.append(note)

        # FIX 3: Block auto-approval when substance flags it
        if substance.get("block_auto_approval"):
            effective_approval_state = "pending_review"
            effective_posting_status = "blocked"

        # FIX 1: Write substance override back to documents table
        if effective_gl != original_gl and table_exists(conn, "documents"):
            doc_cols = table_columns(conn, "documents")
            if "gl_account" in doc_cols:
                conn.execute(
                    "UPDATE documents SET gl_account = ? WHERE document_id = ?",
                    (effective_gl, document_id),
                )
            if "substance_flags" in doc_cols:
                override_reason = []
                if substance.get("potential_capex"):
                    override_reason.append("capex")
                if substance.get("potential_prepaid"):
                    override_reason.append("prepaid")
                if substance.get("potential_loan"):
                    override_reason.append("loan")
                if substance.get("potential_tax_remittance"):
                    override_reason.append("tax_remittance")
                if substance.get("potential_personal_expense"):
                    override_reason.append("personal_expense")
                substance["override_applied"] = True
                substance["original_gl"] = original_gl
                conn.execute(
                    "UPDATE documents SET substance_flags = ? WHERE document_id = ?",
                    (json_dumps_stable(substance), document_id),
                )

            # FIX 1: Log GL override to audit_log
            if table_exists(conn, "audit_log"):
                conn.execute(
                    """INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (
                        "gl_override_applied",
                        document_id,
                        json_dumps_stable({"old_value": original_gl, "new_value": effective_gl}),
                        now_iso,
                    ),
                )
    except Exception:
        pass

    base_values: dict[str, Any] = {
        "posting_id": effective_posting_id,
        "document_id": document_id,
        "target_system": effective_target_system,
        "entry_kind": effective_entry_kind,
        "file_name": first_present(document, ["file_name"], existing.get("file_name")),
        "file_path": first_present(document, ["file_path"], existing.get("file_path")),
        "client_code": first_present(document, ["client_code"], existing.get("client_code")),
        "vendor": first_present(document, ["vendor"], existing.get("vendor")),
        "document_date": first_present(document, ["document_date"], existing.get("document_date")),
        "amount": first_present(document, ["amount"], existing.get("amount")),
        "currency": first_present(document, ["currency"], existing.get("currency")),
        "doc_type": first_present(document, ["doc_type"], existing.get("doc_type")),
        "category": first_present(document, ["category"], existing.get("category")),
        "gl_account": effective_gl,
        "tax_code": first_present(document, ["tax_code"], existing.get("tax_code")),
        "memo": effective_memo,
        "review_status": first_present(document, ["review_status"], existing.get("review_status")),
        "confidence": first_present(document, ["confidence"], existing.get("confidence")),
        "approval_state": effective_approval_state,
        "posting_status": effective_posting_status,
        "reviewer": effective_reviewer,
        "blocking_issues": json_dumps_stable(effective_blocking_issues),
        "notes": json_dumps_stable(effective_notes),
        "external_id": effective_external_id or None,
        "payload_json": "{}",
        "updated_at": now_iso,
    }

    # BLOCK 1+2: Fetch fraud_flags and substance_flags, run review_policy
    try:
        from src.agents.tools.review_policy import decide_review_status as _decide_review
        doc_fraud_flags = json_loads_safe(document.get("fraud_flags"), [])
        if not isinstance(doc_fraud_flags, list):
            doc_fraud_flags = []
        doc_substance_flags = substance_data if substance_data else json_loads_safe(document.get("substance_flags"), {})
        if not isinstance(doc_substance_flags, dict):
            doc_substance_flags = {}
        _review_decision = _decide_review(
            rules_confidence=safe_float(first_present(document, ["confidence"], existing.get("confidence"))) or 0.0,
            final_method="rules",
            vendor_name=normalize_text(first_present(document, ["vendor"], existing.get("vendor"))),
            total=safe_float(first_present(document, ["amount"], existing.get("amount"))),
            document_date=normalize_text(first_present(document, ["document_date"], existing.get("document_date"))),
            client_code=normalize_text(first_present(document, ["client_code"], existing.get("client_code"))),
            fraud_flags=doc_fraud_flags,
            substance_flags=doc_substance_flags,
        )
        if _review_decision.status in ("NeedsReview", "Exception"):
            base_values["review_status"] = _review_decision.status
            if "approved" in effective_approval_state and _review_decision.status == "NeedsReview":
                pass  # Don't override explicit approval
            else:
                effective_approval_state = "pending_review"
                base_values["approval_state"] = effective_approval_state
    except Exception:
        pass

    # FIX 3: Force NeedsReview when block_auto_approval is set
    if substance_data.get("block_auto_approval"):
        base_values["review_status"] = "NeedsReview"

    if existing:
        update_fields = {k: v for k, v in base_values.items() if k in posting_columns and k != "posting_id"}
        assignments = ", ".join(f"{field} = ?" for field in update_fields.keys())
        params = list(update_fields.values()) + [effective_posting_id]
        conn.execute(
            f"""
            UPDATE posting_jobs
            SET {assignments}
            WHERE posting_id = ?
            """,
            params,
        )
    else:
        insert_values = dict(base_values)
        insert_values["created_at"] = now_iso
        insert_values = {k: v for k, v in insert_values.items() if k in posting_columns}
        fields = list(insert_values.keys())
        placeholders = ", ".join("?" for _ in fields)
        conn.execute(
            f"""
            INSERT INTO posting_jobs ({", ".join(fields)})
            VALUES ({placeholders})
            """,
            [insert_values[field] for field in fields],
        )

    conn.commit()
    return sync_posting_payload(conn, posting_id=effective_posting_id)


def build_posting_job(
    document_or_id: Mapping[str, Any] | str,
    *,
    target_system: str = "qbo",
    entry_kind: str = "expense",
    reviewer: str = "ExceptionRouter",
    approval_state: str | None = None,
    posting_status: str | None = None,
    blocking_issues: list[str] | None = None,
    notes: list[str] | None = None,
    external_id: str | None = None,
    memo: str | None = None,
) -> dict[str, Any]:
    conn = open_db()
    try:
        document = resolve_document_input(conn, document_or_id)
        return upsert_posting_job(
            conn,
            document=document,
            target_system=target_system,
            entry_kind=entry_kind,
            reviewer=reviewer,
            approval_state=approval_state,
            posting_status=posting_status,
            blocking_issues=blocking_issues,
            notes=notes,
            external_id=external_id,
            memo=memo,
        )
    finally:
        conn.close()


POSTABLE_STATUSES = {"Ready", "Posted"}
BLOCKED_REVIEW_STATUSES = {"Exception", "NeedsReview", "OnHold", "New"}
BLOCKED_APPROVAL_STATES = {"pending_review", "blocked"}


def _log_posting_blocked(
    conn: sqlite3.Connection,
    document_id: str,
    reason: str,
    username: str = "",
) -> None:
    """FIX 5: Log blocked posting attempts to audit_log."""
    if table_exists(conn, "audit_log"):
        conn.execute(
            """INSERT INTO audit_log (event_type, username, document_id, prompt_snippet, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "posting_blocked",
                username,
                document_id,
                json_dumps_stable({"reason": reason}),
                utc_now_iso(),
            ),
        )
        conn.commit()


def _check_period_not_locked_for_doc(
    conn: sqlite3.Connection,
    document_id: str,
) -> None:
    """FIX 10: Check that the document's period is not locked."""
    if not table_exists(conn, "period_locks"):
        return
    doc_row = fetch_document_row(conn, document_id)
    if not doc_row:
        return
    client_code = normalize_text(doc_row.get("client_code"))
    document_date = normalize_text(doc_row.get("document_date"))
    if not client_code or not document_date:
        return
    lock = conn.execute(
        """SELECT * FROM period_locks
           WHERE client_code = ?
             AND period_start <= ?
             AND period_end >= ?""",
        (client_code, document_date, document_date),
    ).fetchone()
    if lock:
        reason = (
            f"Period locked: {lock['period_start']} to {lock['period_end']} "
            f"by {lock['locked_by']}"
        )
        if table_exists(conn, "audit_log"):
            conn.execute(
                """INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
                   VALUES (?, ?, ?, ?)""",
                (
                    "posting_blocked_period_locked",
                    document_id,
                    json_dumps_stable({"reason": reason, "client_code": client_code, "document_date": document_date}),
                    utc_now_iso(),
                ),
            )
            conn.commit()
        raise ValueError(reason)


def _check_fraud_flags_for_approval(
    conn: sqlite3.Connection,
    document: Mapping[str, Any],
    username: str = "",
) -> None:
    """FIX 6: Engine-layer fraud flag check for approval actions."""
    fraud_flags_raw = json_loads_safe(document.get("fraud_flags"), [])
    if not isinstance(fraud_flags_raw, list):
        fraud_flags_raw = []
    blocking = [
        f for f in fraud_flags_raw
        if isinstance(f, dict) and normalize_text(f.get("severity")).upper() in ("CRITICAL", "HIGH")
    ]
    if blocking:
        document_id = normalize_text(document.get("document_id"))
        override_reason = normalize_text(document.get("fraud_override_reason"))
        if not override_reason or len(override_reason.strip()) < 10:
            reason = f"Fraud flags present: {[f.get('rule', '') for f in blocking]}"
            _log_posting_blocked(conn, document_id, reason, username)
            raise ValueError(
                f"Cannot approve document with unresolved fraud flags: "
                f"{[f.get('rule', '') for f in blocking]}. "
                f"Provide a fraud_override_reason (min 10 chars) to proceed."
            )


def enforce_posting_preconditions(
    conn: sqlite3.Connection,
    document_id: str,
    username: str = "",
) -> None:
    """DB-level enforcement: block any posting_status='posted' transition
    unless review_status is Ready/Posted and approval_state is approved.
    Must be called inside the same transaction as the status update."""
    doc_row = fetch_document_row(conn, document_id)
    review_status = normalize_text(doc_row.get("review_status"))
    if review_status in BLOCKED_REVIEW_STATUSES:
        reason = f"review_status='{review_status}' is blocked"
        _log_posting_blocked(conn, document_id, reason, username)
        raise ValueError(
            f"Cannot post document with review_status='{review_status}'. "
            f"Must be Ready or Posted."
        )
    if review_status and review_status not in POSTABLE_STATUSES:
        reason = f"review_status='{review_status}' not in postable statuses"
        _log_posting_blocked(conn, document_id, reason, username)
        raise ValueError(
            f"Cannot post document with review_status='{review_status}'. "
            f"Must be Ready or Posted."
        )

    posting_row = fetch_posting_row_by_document_id(conn, document_id)
    if posting_row:
        approval_state = normalize_text(posting_row.get("approval_state"))
        if approval_state in BLOCKED_APPROVAL_STATES:
            reason = f"approval_state='{approval_state}' is blocked"
            _log_posting_blocked(conn, document_id, reason, username)
            raise ValueError(
                f"Cannot post document with approval_state='{approval_state}'. "
                f"Must be approved."
            )


def approve_posting_job(
    document_or_id: Mapping[str, Any] | str,
    *,
    reviewer: str = "OpenClawCaseOrchestrator",
    posting_status: str = "ready_to_post",
    notes: list[str] | None = None,
    blocking_issues: list[str] | None = None,
    expected_version: int | None = None,
) -> dict[str, Any]:
    conn = open_db()
    try:
        document = resolve_document_input(conn, document_or_id)
        document_id = normalize_text(document.get("document_id"))

        # TRAP 6: Optimistic locking — reject stale approvals
        if expected_version is not None:
            try:
                from src.engines.concurrency_engine import check_version_or_raise
                check_version_or_raise(conn, "document", document_id, expected_version)
            except Exception as exc:
                if "Stale" in str(exc):
                    raise

        # FIX 5a: Block posting unless review_status is Ready or Posted
        review_status = normalize_text(document.get("review_status"))
        if review_status not in POSTABLE_STATUSES:
            raise ValueError(
                f"Document must be in Ready status before posting. "
                f"Current status: {review_status}"
            )

        # FIX 6: Engine-layer fraud flag check
        _check_fraud_flags_for_approval(conn, document, reviewer)

        # TRAP 8: Check re-import safety before approving
        try:
            from src.engines.correction_chain import check_reimport_after_rollback
            reimport_check = check_reimport_after_rollback(
                conn, document_id, normalize_text(document.get("client_code"))
            )
            if not reimport_check.get("can_reimport", True):
                reasons = reimport_check.get("reasons", [])
                reason_texts = [r.get("description_en", "") for r in reasons]
                raise ValueError(
                    f"Cannot approve: {'; '.join(reason_texts)}"
                )
        except ValueError:
            raise
        except Exception:
            pass

        return upsert_posting_job(
            conn,
            document=document,
            reviewer=reviewer,
            approval_state="approved_for_posting",
            posting_status=posting_status,
            notes=notes,
            blocking_issues=blocking_issues if blocking_issues is not None else [],
        )
    finally:
        conn.close()


def retry_posting_job(
    document_or_id: Mapping[str, Any] | str,
    *,
    reviewer: str = "OpenClawCaseOrchestrator",
    notes: list[str] | None = None,
) -> dict[str, Any]:
    conn = open_db()
    try:
        document = resolve_document_input(conn, document_or_id)
        # FIX 11: Retry is an approval action — re-check fraud flags
        _check_fraud_flags_for_approval(conn, document, reviewer)
        existing = fetch_posting_row_by_document_id(conn, normalize_text(document.get("document_id")))

        current_notes = normalize_string_list(existing.get("notes"))
        if notes:
            current_notes.extend(normalize_string_list(notes))

        return upsert_posting_job(
            conn,
            document=document,
            reviewer=reviewer,
            approval_state=normalize_text(existing.get("approval_state")) or "approved_for_posting",
            posting_status="ready_to_post",
            blocking_issues=[],
            notes=current_notes,
            external_id=normalize_text(existing.get("external_id")) or None,
            memo=normalize_text(existing.get("memo")) or None,
        )
    finally:
        conn.close()


def mark_posting_job_posted(
    document_id: str,
    *,
    external_id: str | None = None,
    reviewer: str = "OpenClawCaseOrchestrator",
    notes: list[str] | None = None,
) -> dict[str, Any]:
    conn = open_db()
    try:
        # FIX 1: Enforce state machine — block Exception→Posted bypass
        enforce_posting_preconditions(conn, document_id)
        # FIX 10: Enforce period lock before posting
        _check_period_not_locked_for_doc(conn, document_id)

        posting = fetch_posting_row_by_document_id(conn, document_id)
        if not posting:
            document = resolve_document_input(conn, document_id)
            posting = build_posting_job(
                document,
                reviewer=reviewer,
                approval_state="approved_for_posting",
                posting_status="posted",
                external_id=external_id,
                notes=notes,
                blocking_issues=[],
            )
            return posting

        current_notes = normalize_string_list(posting.get("notes"))
        if notes:
            current_notes.extend(normalize_string_list(notes))

        columns = table_columns(conn, "posting_jobs")
        now_iso = utc_now_iso()

        updates: dict[str, Any] = {
            "posting_status": "posted",
            "approval_state": normalize_text(posting.get("approval_state")) or "approved_for_posting",
            "reviewer": reviewer,
            "blocking_issues": json_dumps_stable([]),
            "notes": json_dumps_stable(current_notes),
            "updated_at": now_iso,
        }

        if external_id not in (None, ""):
            updates["external_id"] = normalize_text(external_id)

        update_fields = {k: v for k, v in updates.items() if k in columns}
        assignments = ", ".join(f"{field} = ?" for field in update_fields.keys())
        params = list(update_fields.values()) + [posting["posting_id"]]

        conn.execute(
            f"""
            UPDATE posting_jobs
            SET {assignments}
            WHERE posting_id = ?
            """,
            params,
        )
        conn.commit()

        return sync_posting_payload(conn, posting_id=str(posting["posting_id"]))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Split settlement — generate posting entries per payment method
# ---------------------------------------------------------------------------

def build_split_settlement_entries(
    document_or_id: Mapping[str, Any] | str,
    payments: list[dict[str, Any]],
    *,
    vendor_province: str = "",
    client_province: str = "",
    target_system: str = "qbo",
    reviewer: str = "ExceptionRouter",
) -> dict[str, Any]:
    """
    Build posting entries for an invoice settled via multiple payment methods.

    Each element in ``payments`` is a dict with at least ``"amount"`` and
    ``"method"`` (e.g. ``"bank_transfer"``, ``"credit_note"``).

    Uses :func:`allocate_tax_to_payments` from the tax engine to compute
    pro-rata pre-tax / tax portions for each payment, then creates
    a posting job per payment with correctly split amounts.

    Does NOT modify any existing posting jobs — only adds new ones with
    a ``_splitN`` suffix on the posting_id.

    Returns
    -------
    dict with keys:
        document_id, invoice_total, entries: list[dict], tax_allocation: dict,
        warnings: list[str]
    """
    from src.engines.tax_engine import allocate_tax_to_payments as _alloc_tax
    from decimal import Decimal

    conn = open_db()
    try:
        document = resolve_document_input(conn, document_or_id)
        document_id = normalize_text(document.get("document_id"))
        if not document_id:
            raise ValueError("document_id is required")

        invoice_total = safe_float(document.get("amount"))
        if invoice_total is None:
            raise ValueError("Document has no amount")

        tax_code = normalize_text(document.get("tax_code")) or "NONE"

        # Compute pro-rata tax allocation
        decimal_payments = []
        for p in payments:
            decimal_payments.append({
                "amount": Decimal(str(p["amount"])),
                "method": str(p.get("method", "unknown")),
            })

        tax_alloc = _alloc_tax(
            Decimal(str(invoice_total)),
            tax_code,
            decimal_payments,
            vendor_province=vendor_province,
            client_province=client_province,
        )

        entries: list[dict[str, Any]] = []
        warnings: list[str] = list(tax_alloc.get("warnings", []))

        for i, alloc in enumerate(tax_alloc["payment_allocations"]):
            method = alloc["method"]
            suffix = f"_split{i + 1}_{method}"

            # Determine entry_kind based on payment method
            entry_kind = "expense"
            if method in ("credit_note", "refund", "chargeback"):
                entry_kind = "credit"

            split_notes = [
                f"Paiement fractionné {i + 1}/{len(payments)}: "
                f"{method} — {alloc['payment_amount']}$ "
                f"(avant taxes: {alloc['pre_tax_portion']}$, "
                f"taxes: {alloc['tax_portion']}$). / "
                f"Split payment {i + 1}/{len(payments)}: "
                f"{method} — ${alloc['payment_amount']} "
                f"(pre-tax: ${alloc['pre_tax_portion']}, "
                f"tax: ${alloc['tax_portion']})."
            ]

            # Add cross-provincial advisory if applicable
            cross_prov = tax_alloc.get("cross_provincial")
            if cross_prov and cross_prov.get("cross_provincial"):
                for note in cross_prov.get("advisory_notes", []):
                    split_notes.append(note)

            # Build the split posting job
            split_doc = dict(document)
            split_doc["amount"] = float(alloc["payment_amount"])

            split_posting_id = f"post_{target_system}_{entry_kind}_{document_id}{suffix}"

            ensure_posting_job_table_minimum(conn)
            posting_columns = table_columns(conn, "posting_jobs")
            now_iso = utc_now_iso()

            base_values: dict[str, Any] = {
                "posting_id": split_posting_id,
                "document_id": document_id,
                "target_system": target_system,
                "entry_kind": entry_kind,
                "file_name": normalize_text(document.get("file_name")),
                "file_path": normalize_text(document.get("file_path")),
                "client_code": normalize_text(document.get("client_code")),
                "vendor": normalize_text(document.get("vendor")),
                "document_date": normalize_text(document.get("document_date")),
                "amount": float(alloc["payment_amount"]),
                "currency": normalize_text(document.get("currency")),
                "doc_type": normalize_text(document.get("doc_type")),
                "category": normalize_text(document.get("category")),
                "gl_account": normalize_text(document.get("gl_account")),
                "tax_code": tax_code,
                "memo": (
                    f"{normalize_text(document.get('vendor'))} | "
                    f"split {i + 1}/{len(payments)} | {method}"
                ),
                "review_status": "NeedsReview",
                "confidence": safe_float(document.get("confidence")),
                "approval_state": "pending_review",
                "posting_status": "draft",
                "reviewer": reviewer,
                "blocking_issues": json_dumps_stable([]),
                "notes": json_dumps_stable(split_notes),
                "payload_json": "{}",
                "created_at": now_iso,
                "updated_at": now_iso,
            }

            insert_values = {k: v for k, v in base_values.items() if k in posting_columns}
            fields = list(insert_values.keys())
            placeholders = ", ".join("?" for _ in fields)

            # Use INSERT OR REPLACE to allow re-running
            conn.execute(
                f"""
                INSERT OR REPLACE INTO posting_jobs ({", ".join(fields)})
                VALUES ({placeholders})
                """,
                [insert_values[f] for f in fields],
            )

            entries.append({
                "posting_id": split_posting_id,
                "entry_kind": entry_kind,
                "method": method,
                "amount": float(alloc["payment_amount"]),
                "pre_tax": float(alloc["pre_tax_portion"]),
                "tax": float(alloc["tax_portion"]),
                "ratio": alloc["ratio"],
            })

        conn.commit()

        return {
            "document_id": document_id,
            "invoice_total": invoice_total,
            "tax_code": tax_code,
            "entries": entries,
            "tax_allocation": {
                "pre_tax": float(tax_alloc["pre_tax"]),
                "total_tax": float(tax_alloc["total_tax"]),
                "cross_provincial": (
                    {
                        "qst_self_assessed": float(cross_prov["qst_self_assessed"]),
                        "qst_self_assessed_itr": float(cross_prov["qst_self_assessed_itr"]),
                    }
                    if tax_alloc.get("cross_provincial")
                    and tax_alloc["cross_provincial"].get("cross_provincial")
                    else None
                ),
            },
            "warnings": warnings,
        }
    finally:
        conn.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Repair and sync posting_jobs payload_json")
    parser.add_argument("--document-id", default="", help="Sync only one document_id")
    args = parser.parse_args()

    if normalize_text(args.document_id):
        result = sync_posting_payload_for_document(normalize_text(args.document_id))
    else:
        result = sync_all_posting_payloads()

    print(json.dumps(result, indent=2, ensure_ascii=False))


# =========================================================================
# PART 11 — Posting readiness evaluation integration
# =========================================================================

def evaluate_posting_readiness_for_document(
    document_id: str,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Evaluate whether a document is ready to post.

    Uses uncertainty_engine to determine posting readiness based on
    field confidence levels and unresolved reasons.
    """
    from src.engines.uncertainty_engine import (
        UncertaintyState,
        UncertaintyReason,
        evaluate_uncertainty,
        evaluate_posting_readiness,
        SAFE_TO_POST,
        PARTIAL_POST_WITH_FLAGS,
        BLOCK_PENDING_REVIEW,
    )

    conn = open_db(db_path)
    try:
        row = conn.execute(
            """SELECT document_id, vendor, amount, document_date, tax_code,
                      gl_account, review_status, confidence,
                      COALESCE(review_notes, '') AS review_notes,
                      COALESCE(fraud_flags, '[]') AS fraud_flags
               FROM documents
               WHERE document_id = ?""",
            (document_id,),
        ).fetchone()

        if not row:
            return {
                "outcome": BLOCK_PENDING_REVIEW,
                "can_post": False,
                "reasoning": f"Document {document_id} not found.",
            }

        doc = row_to_dict(row)
        confidence = safe_float(doc.get("confidence")) or 0.0

        # Build per-field confidence from available data
        confidence_by_field: dict[str, float] = {}
        reasons: list[UncertaintyReason] = []

        # Vendor confidence
        vendor = normalize_text(doc.get("vendor"))
        if not vendor:
            confidence_by_field["vendor"] = 0.0
            reasons.append(UncertaintyReason(
                reason_code="VENDOR_NAME_CONFLICT",
                description_fr="Nom du fournisseur manquant",
                description_en="Vendor name missing",
                evidence_available="No vendor name in document",
                evidence_needed="Verified vendor name",
            ))
        else:
            confidence_by_field["vendor"] = min(confidence, 0.95)

        # Amount confidence
        amount = safe_float(doc.get("amount"))
        if amount is None or amount <= 0:
            confidence_by_field["amount"] = 0.0
        else:
            confidence_by_field["amount"] = min(confidence, 0.95)

        # Date confidence
        doc_date = normalize_text(doc.get("document_date"))
        if not doc_date:
            confidence_by_field["date"] = 0.0
            reasons.append(UncertaintyReason(
                reason_code="DATE_AMBIGUOUS",
                description_fr="Date du document manquante",
                description_en="Document date missing",
                evidence_available="No date in document",
                evidence_needed="Verified document date",
            ))
        else:
            confidence_by_field["date"] = min(confidence, 0.95)

        # Tax code confidence
        tax_code = normalize_text(doc.get("tax_code"))
        if not tax_code:
            confidence_by_field["tax_code"] = 0.40
        else:
            confidence_by_field["tax_code"] = min(confidence, 0.90)

        # GL account confidence
        gl_account = normalize_text(doc.get("gl_account"))
        if not gl_account:
            confidence_by_field["gl_account"] = 0.30
        else:
            confidence_by_field["gl_account"] = min(confidence, 0.90)

        # Check fraud flags
        fraud_flags = json_loads_safe(doc.get("fraud_flags"), [])
        if fraud_flags:
            for ff in fraud_flags:
                if isinstance(ff, dict):
                    severity = ff.get("severity", "medium")
                    if severity == "high":
                        confidence_by_field["fraud_check"] = 0.30
                    else:
                        confidence_by_field["fraud_check"] = 0.60

        # Evaluate uncertainty state
        uncertainty_state = evaluate_uncertainty(confidence_by_field, reasons)
        decision = evaluate_posting_readiness(doc, uncertainty_state)
        return decision.to_dict()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
