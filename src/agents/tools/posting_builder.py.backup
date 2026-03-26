from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_posting_jobs_table(db_path: Path = DB_PATH) -> None:
    with open_db(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS posting_jobs (
                posting_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                target_system TEXT NOT NULL,
                entry_kind TEXT NOT NULL,
                posting_status TEXT NOT NULL,
                approval_state TEXT NOT NULL,
                reviewer TEXT,
                external_id TEXT,
                payload_json TEXT NOT NULL,
                error_text TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_document_id
            ON posting_jobs(document_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_target_system
            ON posting_jobs(target_system)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_posting_status
            ON posting_jobs(posting_status)
            """
        )
        conn.commit()


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def normalize_amount(value: Any) -> Optional[float]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = (
        text.replace(",", "")
        .replace("$", "")
        .replace("CAD", "")
        .replace("USD", "")
        .replace("EUR", "")
        .strip()
    )

    try:
        return round(float(text), 2)
    except Exception:
        return None


def normalize_date(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    candidates = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]

    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass

    trimmed = text[:10]
    try:
        return datetime.strptime(trimmed, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}

    if isinstance(value, dict):
        return value

    try:
        return json.loads(str(value))
    except Exception:
        return {}


@dataclass
class PostingPayload:
    posting_id: str
    document_id: str
    target_system: str
    entry_kind: str

    file_name: Optional[str]
    file_path: Optional[str]

    client_code: Optional[str]
    vendor: Optional[str]
    document_date: Optional[str]
    amount: Optional[float]
    currency: str

    doc_type: Optional[str]
    category: Optional[str]
    gl_account: Optional[str]
    tax_code: Optional[str]
    memo: Optional[str]

    review_status: Optional[str]
    confidence: float

    approval_state: str
    posting_status: str
    reviewer: Optional[str]

    blocking_issues: list[str]
    notes: list[str]

    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


ALLOWED_TARGET_SYSTEMS = {"qbo", "xero"}
ALLOWED_ENTRY_KINDS = {"expense", "bill"}
ALLOWED_APPROVAL_STATES = {
    "needs_review",
    "pending_human_approval",
    "approved_for_posting",
    "rejected",
}
ALLOWED_POSTING_STATUSES = {
    "draft",
    "ready_to_post",
    "posted",
    "post_failed",
}


def validate_target_system(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in ALLOWED_TARGET_SYSTEMS:
        raise ValueError(f"Invalid target_system '{value}'. Allowed: {sorted(ALLOWED_TARGET_SYSTEMS)}")
    return normalized


def validate_entry_kind(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in ALLOWED_ENTRY_KINDS:
        raise ValueError(f"Invalid entry_kind '{value}'. Allowed: {sorted(ALLOWED_ENTRY_KINDS)}")
    return normalized


def make_posting_id(document_id: str, target_system: str, entry_kind: str) -> str:
    safe_document_id = (document_id or "").strip()
    return f"post_{target_system}_{entry_kind}_{safe_document_id}"


def normalize_currency(value: Any) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None

    text = text.upper()
    allowed = {"CAD", "USD", "EUR", "GBP", "AUD", "NZD"}
    if text in allowed:
        return text

    return text


def choose_currency(raw_result: dict[str, Any]) -> str:
    candidates = [
        raw_result.get("currency"),
        raw_result.get("raw_rules_output", {}).get("currency") if isinstance(raw_result.get("raw_rules_output"), dict) else None,
        raw_result.get("raw_vendor_output", {}).get("currency") if isinstance(raw_result.get("raw_vendor_output"), dict) else None,
        raw_result.get("raw_ai_output", {}).get("currency") if isinstance(raw_result.get("raw_ai_output"), dict) else None,
        raw_result.get("metadata", {}).get("currency") if isinstance(raw_result.get("metadata"), dict) else None,
    ]

    for candidate in candidates:
        normalized = normalize_currency(candidate)
        if normalized:
            return normalized

    text_preview = normalize_text(raw_result.get("text_preview")) or ""
    preview_upper = text_preview.upper()

    if " USD" in preview_upper or ("$" in preview_upper and "USD" in preview_upper):
        return "USD"
    if " CAD" in preview_upper:
        return "CAD"
    if " EUR" in preview_upper:
        return "EUR"
    if " GBP" in preview_upper:
        return "GBP"

    return "CAD"


def build_memo(row: sqlite3.Row, raw_result: dict[str, Any]) -> Optional[str]:
    parts: list[str] = []

    vendor = normalize_text(row["vendor"])
    doc_type = normalize_text(row["doc_type"])
    file_name = normalize_text(row["file_name"])

    if vendor:
        parts.append(vendor)
    if doc_type:
        parts.append(doc_type)
    if file_name:
        parts.append(file_name)

    if parts:
        return " | ".join(parts)

    description = normalize_text(raw_result.get("description"))
    if description:
        return description

    preview = normalize_text(raw_result.get("text_preview"))
    if preview:
        return preview[:160]

    return None


def determine_blocking_issues(
    *,
    amount: Optional[float],
    document_date: Optional[str],
    vendor: Optional[str],
    client_code: Optional[str],
    gl_account: Optional[str],
    tax_code: Optional[str],
    review_status: Optional[str],
) -> list[str]:
    issues: list[str] = []

    if amount is None:
        issues.append("missing_amount")

    if not document_date:
        issues.append("missing_document_date")

    if not vendor:
        issues.append("missing_vendor")

    if not client_code:
        issues.append("missing_client_code")

    if not gl_account:
        issues.append("missing_gl_account")

    if not tax_code:
        issues.append("missing_tax_code")

    if review_status in {"NeedsReview", "Exception", "Ignored"}:
        issues.append("document_not_ready_for_posting")

    return issues


def infer_default_approval_state(review_status: Optional[str], blocking_issues: list[str]) -> str:
    if blocking_issues:
        return "needs_review"

    if review_status == "Ready":
        return "pending_human_approval"

    return "needs_review"


def infer_default_posting_status(approval_state: str) -> str:
    if approval_state == "approved_for_posting":
        return "ready_to_post"
    return "draft"


def fetch_document_row(document_id: str, db_path: Path = DB_PATH) -> sqlite3.Row | None:
    with open_db(db_path) as conn:
        row = conn.execute(
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
    return row


def get_posting_job(posting_id: str, db_path: Path = DB_PATH) -> sqlite3.Row | None:
    ensure_posting_jobs_table(db_path)
    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE posting_id = ?
            """,
            (posting_id,),
        ).fetchone()
    return row


def get_posting_job_for_document(
    document_id: str,
    target_system: str,
    entry_kind: str,
    db_path: Path = DB_PATH,
) -> sqlite3.Row | None:
    ensure_posting_jobs_table(db_path)
    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE document_id = ?
              AND target_system = ?
              AND entry_kind = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            (document_id, target_system, entry_kind),
        ).fetchone()
    return row


def build_payload_from_document_row(
    row: sqlite3.Row,
    *,
    target_system: str,
    entry_kind: str,
    existing_job: sqlite3.Row | None = None,
) -> PostingPayload:
    target_system = validate_target_system(target_system)
    entry_kind = validate_entry_kind(entry_kind)

    raw_result = safe_json_loads(row["raw_result"])

    document_id = str(row["document_id"])
    file_name = normalize_text(row["file_name"])
    file_path = normalize_text(row["file_path"])
    client_code = normalize_text(row["client_code"])
    vendor = normalize_text(row["vendor"])
    doc_type = normalize_text(row["doc_type"])
    amount = normalize_amount(row["amount"])
    document_date = normalize_date(row["document_date"])
    gl_account = normalize_text(row["gl_account"])
    tax_code = normalize_text(row["tax_code"])
    category = normalize_text(row["category"])
    review_status = normalize_text(row["review_status"])

    try:
        confidence = float(row["confidence"] or 0.0)
    except Exception:
        confidence = 0.0

    currency = choose_currency(raw_result)
    memo = build_memo(row, raw_result)

    blocking_issues = determine_blocking_issues(
        amount=amount,
        document_date=document_date,
        vendor=vendor,
        client_code=client_code,
        gl_account=gl_account,
        tax_code=tax_code,
        review_status=review_status,
    )

    if existing_job:
        approval_state = normalize_text(existing_job["approval_state"]) or "needs_review"
        posting_status = normalize_text(existing_job["posting_status"]) or "draft"
        reviewer = normalize_text(existing_job["reviewer"])
        external_id = normalize_text(existing_job["external_id"])
        created_at = normalize_text(existing_job["created_at"]) or utc_now_iso()
    else:
        approval_state = infer_default_approval_state(review_status, blocking_issues)
        posting_status = infer_default_posting_status(approval_state)
        reviewer = None
        external_id = None
        created_at = utc_now_iso()

    notes: list[str] = []

    if confidence < 0.80:
        notes.append("low_confidence_document")

    if confidence >= 0.95:
        notes.append("high_confidence_document")

    if review_status == "Ready" and approval_state not in {"approved_for_posting", "rejected"}:
        notes.append("human_approval_still_required")

    if external_id:
        notes.append(f"external_id_present:{external_id}")

    if currency:
        notes.append(f"currency:{currency}")

    posting_id = make_posting_id(document_id, target_system, entry_kind)

    return PostingPayload(
        posting_id=posting_id,
        document_id=document_id,
        target_system=target_system,
        entry_kind=entry_kind,
        file_name=file_name,
        file_path=file_path,
        client_code=client_code,
        vendor=vendor,
        document_date=document_date,
        amount=amount,
        currency=currency,
        doc_type=doc_type,
        category=category,
        gl_account=gl_account,
        tax_code=tax_code,
        memo=memo,
        review_status=review_status,
        confidence=confidence,
        approval_state=approval_state,
        posting_status=posting_status,
        reviewer=reviewer,
        blocking_issues=blocking_issues,
        notes=notes,
        created_at=created_at,
        updated_at=utc_now_iso(),
    )


def upsert_posting_job(payload: PostingPayload, db_path: Path = DB_PATH) -> None:
    ensure_posting_jobs_table(db_path)

    payload_json = json.dumps(payload.to_dict(), ensure_ascii=False)

    with open_db(db_path) as conn:
        existing = conn.execute(
            """
            SELECT posting_id
            FROM posting_jobs
            WHERE posting_id = ?
            """,
            (payload.posting_id,),
        ).fetchone()

        if existing is None:
            conn.execute(
                """
                INSERT INTO posting_jobs (
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.posting_id,
                    payload.document_id,
                    payload.target_system,
                    payload.entry_kind,
                    payload.posting_status,
                    payload.approval_state,
                    payload.reviewer,
                    None,
                    payload_json,
                    None,
                    payload.created_at,
                    payload.updated_at,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE posting_jobs
                SET
                    document_id = ?,
                    target_system = ?,
                    entry_kind = ?,
                    posting_status = ?,
                    approval_state = ?,
                    reviewer = ?,
                    payload_json = ?,
                    updated_at = ?
                WHERE posting_id = ?
                """,
                (
                    payload.document_id,
                    payload.target_system,
                    payload.entry_kind,
                    payload.posting_status,
                    payload.approval_state,
                    payload.reviewer,
                    payload_json,
                    payload.updated_at,
                    payload.posting_id,
                ),
            )

        conn.commit()


def build_posting_job(
    *,
    document_id: str,
    target_system: str,
    entry_kind: str = "expense",
    db_path: Path = DB_PATH,
) -> PostingPayload:
    target_system = validate_target_system(target_system)
    entry_kind = validate_entry_kind(entry_kind)

    row = fetch_document_row(document_id, db_path=db_path)
    if row is None:
        raise ValueError(f"Document not found: {document_id}")

    existing_job = get_posting_job_for_document(
        document_id=document_id,
        target_system=target_system,
        entry_kind=entry_kind,
        db_path=db_path,
    )

    payload = build_payload_from_document_row(
        row,
        target_system=target_system,
        entry_kind=entry_kind,
        existing_job=existing_job,
    )

    upsert_posting_job(payload, db_path=db_path)
    return payload


def approve_posting_job(
    *,
    posting_id: str,
    reviewer: str,
    db_path: Path = DB_PATH,
) -> PostingPayload:
    ensure_posting_jobs_table(db_path)

    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE posting_id = ?
            """,
            (posting_id,),
        ).fetchone()

    if row is None:
        raise ValueError(f"Posting job not found: {posting_id}")

    payload = safe_json_loads(row["payload_json"])
    blocking_issues = payload.get("blocking_issues", []) or []
    if blocking_issues:
        raise ValueError(
            f"Posting job cannot be approved because it has blocking issues: {blocking_issues}"
        )

    notes = payload.get("notes", []) or []
    notes = [n for n in notes if n != "human_approval_still_required"]

    payload["notes"] = notes
    payload["approval_state"] = "approved_for_posting"
    payload["posting_status"] = "ready_to_post"
    payload["reviewer"] = reviewer
    payload["updated_at"] = utc_now_iso()

    with open_db(db_path) as conn:
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
                "approved_for_posting",
                "ready_to_post",
                reviewer,
                json.dumps(payload, ensure_ascii=False),
                payload["updated_at"],
                posting_id,
            ),
        )
        conn.commit()

    refreshed = get_posting_job(posting_id, db_path=db_path)
    return PostingPayload(**safe_json_loads(refreshed["payload_json"]))


def reject_posting_job(
    *,
    posting_id: str,
    reviewer: str,
    note: str = "",
    db_path: Path = DB_PATH,
) -> PostingPayload:
    ensure_posting_jobs_table(db_path)

    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE posting_id = ?
            """,
            (posting_id,),
        ).fetchone()

    if row is None:
        raise ValueError(f"Posting job not found: {posting_id}")

    payload = safe_json_loads(row["payload_json"])
    notes = payload.get("notes", []) or []
    if note.strip():
        notes.append(f"rejected_note:{note.strip()}")

    payload["notes"] = notes
    payload["approval_state"] = "rejected"
    payload["posting_status"] = "draft"
    payload["reviewer"] = reviewer
    payload["updated_at"] = utc_now_iso()

    with open_db(db_path) as conn:
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
                "rejected",
                "draft",
                reviewer,
                json.dumps(payload, ensure_ascii=False),
                payload["updated_at"],
                posting_id,
            ),
        )
        conn.commit()

    refreshed = get_posting_job(posting_id, db_path=db_path)
    return PostingPayload(**safe_json_loads(refreshed["payload_json"]))


def retry_posting_job(
    *,
    posting_id: str,
    reviewer: str,
    note: str = "",
    db_path: Path = DB_PATH,
) -> PostingPayload:
    ensure_posting_jobs_table(db_path)

    with open_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE posting_id = ?
            """,
            (posting_id,),
        ).fetchone()

    if row is None:
        raise ValueError(f"Posting job not found: {posting_id}")

    current_status = normalize_text(row["posting_status"]) or ""
    current_approval = normalize_text(row["approval_state"]) or ""

    if current_status != "post_failed":
        raise ValueError(
            f"Retry is only allowed for jobs in post_failed status. Current status: {current_status}"
        )

    if current_approval != "approved_for_posting":
        raise ValueError(
            f"Retry requires approval_state=approved_for_posting. Current approval_state: {current_approval}"
        )

    payload = safe_json_loads(row["payload_json"])
    blocking_issues = payload.get("blocking_issues", []) or []
    if blocking_issues:
        raise ValueError(
            f"Posting job cannot be retried because it has blocking issues: {blocking_issues}"
        )

    notes = payload.get("notes", []) or []
    notes = [n for n in notes if not str(n).startswith("retry_note:")]
    if note.strip():
        notes.append(f"retry_note:{note.strip()}")

    payload["notes"] = notes
    payload["reviewer"] = reviewer
    payload["approval_state"] = "approved_for_posting"
    payload["posting_status"] = "ready_to_post"
    payload["updated_at"] = utc_now_iso()

    with open_db(db_path) as conn:
        conn.execute(
            """
            UPDATE posting_jobs
            SET
                approval_state = ?,
                posting_status = ?,
                reviewer = ?,
                error_text = NULL,
                payload_json = ?,
                updated_at = ?
            WHERE posting_id = ?
            """,
            (
                "approved_for_posting",
                "ready_to_post",
                reviewer,
                json.dumps(payload, ensure_ascii=False),
                payload["updated_at"],
                posting_id,
            ),
        )
        conn.commit()

    refreshed = get_posting_job(posting_id, db_path=db_path)
    return PostingPayload(**safe_json_loads(refreshed["payload_json"]))


def list_posting_jobs(
    *,
    target_system: Optional[str] = None,
    approval_state: Optional[str] = None,
    posting_status: Optional[str] = None,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    ensure_posting_jobs_table(db_path)

    where_clauses: list[str] = []
    params: list[Any] = []

    if target_system:
        where_clauses.append("target_system = ?")
        params.append(validate_target_system(target_system))

    if approval_state:
        where_clauses.append("approval_state = ?")
        params.append(approval_state)

    if posting_status:
        where_clauses.append("posting_status = ?")
        params.append(posting_status)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT *
        FROM posting_jobs
        {where_sql}
        ORDER BY updated_at DESC, created_at DESC, posting_id DESC
    """

    with open_db(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        base = dict(row)
        base["payload_json"] = safe_json_loads(base.get("payload_json"))
        results.append(base)
    return results


def export_posting_jobs_snapshot(
    *,
    target_system: Optional[str] = None,
    approval_state: Optional[str] = None,
    posting_status: Optional[str] = None,
    db_path: Path = DB_PATH,
    out_path: Optional[Path] = None,
) -> Path:
    jobs = list_posting_jobs(
        target_system=target_system,
        approval_state=approval_state,
        posting_status=posting_status,
        db_path=db_path,
    )

    snapshot = {
        "generated_at": utc_now_iso(),
        "filters": {
            "target_system": target_system,
            "approval_state": approval_state,
            "posting_status": posting_status,
        },
        "count": len(jobs),
        "jobs": jobs,
    }

    if out_path is None:
        suffix_parts = ["posting_jobs"]
        if target_system:
            suffix_parts.append(target_system)
        if approval_state:
            suffix_parts.append(approval_state)
        if posting_status:
            suffix_parts.append(posting_status)
        filename = "_".join(suffix_parts) + ".json"
        out_path = EXPORTS_DIR / filename

    out_path.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return out_path


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="LedgerLink posting builder")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build or refresh a posting job")
    build_parser.add_argument("--document-id", required=True)
    build_parser.add_argument("--target-system", required=True, choices=["qbo", "xero"])
    build_parser.add_argument("--entry-kind", default="expense", choices=["expense", "bill"])

    approve_parser = subparsers.add_parser("approve", help="Approve a posting job for posting")
    approve_parser.add_argument("--posting-id", required=True)
    approve_parser.add_argument("--reviewer", required=True)

    reject_parser = subparsers.add_parser("reject", help="Reject a posting job")
    reject_parser.add_argument("--posting-id", required=True)
    reject_parser.add_argument("--reviewer", required=True)
    reject_parser.add_argument("--note", default="")

    retry_parser = subparsers.add_parser("retry", help="Move a post_failed job back to ready_to_post")
    retry_parser.add_argument("--posting-id", required=True)
    retry_parser.add_argument("--reviewer", required=True)
    retry_parser.add_argument("--note", default="")

    list_parser = subparsers.add_parser("list", help="List posting jobs")
    list_parser.add_argument("--target-system", choices=["qbo", "xero"])
    list_parser.add_argument("--approval-state")
    list_parser.add_argument("--posting-status")

    export_parser = subparsers.add_parser("export", help="Export posting jobs snapshot")
    export_parser.add_argument("--target-system", choices=["qbo", "xero"])
    export_parser.add_argument("--approval-state")
    export_parser.add_argument("--posting-status")
    export_parser.add_argument("--out", default="")

    args = parser.parse_args()

    ensure_posting_jobs_table(DB_PATH)

    if args.command == "build":
        payload = build_posting_job(
            document_id=args.document_id,
            target_system=args.target_system,
            entry_kind=args.entry_kind,
            db_path=DB_PATH,
        )
        print(json.dumps(payload.to_dict(), indent=2, ensure_ascii=False))
        return 0

    if args.command == "approve":
        payload = approve_posting_job(
            posting_id=args.posting_id,
            reviewer=args.reviewer,
            db_path=DB_PATH,
        )
        print(json.dumps(payload.to_dict(), indent=2, ensure_ascii=False))
        return 0

    if args.command == "reject":
        payload = reject_posting_job(
            posting_id=args.posting_id,
            reviewer=args.reviewer,
            note=args.note,
            db_path=DB_PATH,
        )
        print(json.dumps(payload.to_dict(), indent=2, ensure_ascii=False))
        return 0

    if args.command == "retry":
        payload = retry_posting_job(
            posting_id=args.posting_id,
            reviewer=args.reviewer,
            note=args.note,
            db_path=DB_PATH,
        )
        print(json.dumps(payload.to_dict(), indent=2, ensure_ascii=False))
        return 0

    if args.command == "list":
        jobs = list_posting_jobs(
            target_system=args.target_system,
            approval_state=args.approval_state,
            posting_status=args.posting_status,
            db_path=DB_PATH,
        )
        print(json.dumps(jobs, indent=2, ensure_ascii=False))
        return 0

    if args.command == "export":
        out_path = Path(args.out) if args.out else None
        final_out = export_posting_jobs_snapshot(
            target_system=args.target_system,
            approval_state=args.approval_state,
            posting_status=args.posting_status,
            db_path=DB_PATH,
            out_path=out_path,
        )
        print(f"Exported posting jobs snapshot to: {final_out}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())