from __future__ import annotations

import json
import sqlite3
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
PATTERN_TABLE = "learning_memory_patterns"


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


def normalize_key(value: Any) -> str:
    text = normalize_text(value).casefold()
    # FIX: Mixed-script attack prevention
    from src.agents.tools.fingerprint_utils import detect_mixed_scripts, normalize_confusables
    if not detect_mixed_scripts(text)["is_mixed"]:
        text = normalize_confusables(text)
    # FIX 4: Strip accents so "Société" == "societe"
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")
    # FIX P1-2: Normalize hyphens to spaces for Quebec vendor name variants
    text = text.replace("-", " ")
    # FIX P1-2: Remove common Quebec/Canadian business suffixes
    import re as _re
    text = _re.sub(
        r"\b(inc|ltee|ltée|limitee|limitée|limited|ltd|enr|corp|corporation|"
        r"s\.?e\.?n\.?c\.?r?\.?l?\.?|s\.?a\.?s?\.?|s\.?e\.?c\.?)\b",
        "", text,
    )
    # Remove leftover punctuation and collapse whitespace
    text = _re.sub(r"[^a-z0-9\s]", " ", text)
    text = _re.sub(r"\s+", " ", text).strip()
    return text


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


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


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


class LearningMemoryStore:
    """
    OpenClaw / OtoCPA pattern-learning store.

    IMPORTANT:
    This store uses a dedicated table: learning_memory_patterns

    That avoids collision with the project's older learning_memory table,
    which is already being used as a correction/change log.
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        with open_db(self.db_path) as conn:
            if not table_exists(conn, PATTERN_TABLE):
                conn.execute(
                    f"""
                    CREATE TABLE {PATTERN_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        memory_key TEXT NOT NULL UNIQUE,
                        event_type TEXT NOT NULL DEFAULT '',
                        vendor TEXT,
                        vendor_key TEXT NOT NULL DEFAULT '',
                        client_code TEXT,
                        client_code_key TEXT NOT NULL DEFAULT '',
                        doc_type TEXT,
                        category TEXT,
                        gl_account TEXT,
                        tax_code TEXT,
                        outcome_count INTEGER NOT NULL DEFAULT 0,
                        success_count INTEGER NOT NULL DEFAULT 0,
                        review_count INTEGER NOT NULL DEFAULT 0,
                        posted_count INTEGER NOT NULL DEFAULT 0,
                        avg_confidence REAL NOT NULL DEFAULT 0.0,
                        avg_amount REAL,
                        last_document_id TEXT,
                        last_payload_json TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

            columns = get_table_columns(conn, PATTERN_TABLE)

            required_additions: list[tuple[str, str]] = [
                ("memory_key", "TEXT NOT NULL DEFAULT ''"),
                ("event_type", "TEXT NOT NULL DEFAULT ''"),
                ("vendor", "TEXT"),
                ("vendor_key", "TEXT NOT NULL DEFAULT ''"),
                ("client_code", "TEXT"),
                ("client_code_key", "TEXT NOT NULL DEFAULT ''"),
                ("doc_type", "TEXT"),
                ("category", "TEXT"),
                ("gl_account", "TEXT"),
                ("tax_code", "TEXT"),
                ("outcome_count", "INTEGER NOT NULL DEFAULT 0"),
                ("success_count", "INTEGER NOT NULL DEFAULT 0"),
                ("review_count", "INTEGER NOT NULL DEFAULT 0"),
                ("posted_count", "INTEGER NOT NULL DEFAULT 0"),
                ("avg_confidence", "REAL NOT NULL DEFAULT 0.0"),
                ("avg_amount", "REAL"),
                ("last_document_id", "TEXT"),
                ("last_payload_json", "TEXT"),
                ("created_at", "TEXT NOT NULL DEFAULT ''"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ]

            for column_name, column_type in required_additions:
                if column_name not in columns:
                    conn.execute(
                        f"ALTER TABLE {PATTERN_TABLE} ADD COLUMN {column_name} {column_type}"
                    )

            conn.commit()

            rows = conn.execute(
                f"""
                SELECT id, event_type, vendor, client_code, doc_type, category, gl_account, tax_code
                FROM {PATTERN_TABLE}
                WHERE COALESCE(memory_key, '') = ''
                """
            ).fetchall()

            for row in rows:
                memory_key = self._build_memory_key(
                    event_type=normalize_text(row["event_type"]),
                    vendor=normalize_text(row["vendor"]),
                    client_code=normalize_text(row["client_code"]),
                    doc_type=normalize_text(row["doc_type"]),
                    category=normalize_text(row["category"]),
                    gl_account=normalize_text(row["gl_account"]),
                    tax_code=normalize_text(row["tax_code"]),
                )
                conn.execute(
                    f"UPDATE {PATTERN_TABLE} SET memory_key = ? WHERE id = ?",
                    (memory_key, int(row["id"])),
                )

            conn.execute(
                f"""
                UPDATE {PATTERN_TABLE}
                SET vendor_key = LOWER(TRIM(COALESCE(vendor, '')))
                WHERE COALESCE(vendor_key, '') = ''
                """
            )
            conn.execute(
                f"""
                UPDATE {PATTERN_TABLE}
                SET client_code_key = LOWER(TRIM(COALESCE(client_code, '')))
                WHERE COALESCE(client_code_key, '') = ''
                """
            )

            now = utc_now_iso()
            conn.execute(
                f"""
                UPDATE {PATTERN_TABLE}
                SET created_at = CASE WHEN COALESCE(created_at, '') = '' THEN ? ELSE created_at END,
                    updated_at = CASE WHEN COALESCE(updated_at, '') = '' THEN ? ELSE updated_at END
                """,
                (now, now),
            )

            conn.commit()

            conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_{PATTERN_TABLE}_memory_key
                ON {PATTERN_TABLE}(memory_key)
                """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{PATTERN_TABLE}_vendor_key
                ON {PATTERN_TABLE}(vendor_key)
                """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{PATTERN_TABLE}_client_vendor
                ON {PATTERN_TABLE}(client_code_key, vendor_key)
                """
            )
            conn.commit()

    def _build_memory_key(
        self,
        *,
        event_type: str,
        vendor: str,
        client_code: str,
        doc_type: str,
        category: str,
        gl_account: str,
        tax_code: str,
    ) -> str:
        return "|".join(
            [
                normalize_key(event_type),
                normalize_key(vendor),
                normalize_key(client_code),
                normalize_key(doc_type),
                normalize_key(category),
                normalize_key(gl_account),
                normalize_key(tax_code),
            ]
        )

    def _normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "document_id": normalize_text(payload.get("document_id")),
            "event_type": normalize_text(payload.get("event_type")) or "observed_case",
            "vendor": normalize_text(payload.get("vendor")),
            "client_code": normalize_text(payload.get("client_code")),
            "doc_type": normalize_text(payload.get("doc_type")),
            "category": normalize_text(payload.get("category")),
            "gl_account": normalize_text(payload.get("gl_account")),
            "tax_code": normalize_text(payload.get("tax_code")),
            "amount": safe_float(payload.get("amount")),
            "confidence": safe_float(payload.get("confidence")),
            "created_at": normalize_text(payload.get("created_at")) or utc_now_iso(),
        }

        if normalized["confidence"] is None:
            normalized["confidence"] = 0.0

        return normalized

    def _classify_counters(self, event_type: str) -> tuple[int, int, int]:
        event_type = normalize_text(event_type)

        posted_count = 1 if event_type == "posted_successfully" else 0
        review_count = 1 if event_type == "held_for_review" else 0
        success_count = 1 if event_type in {
            "posted_successfully",
            "approved_ready_to_post",
            "approved_but_held",
        } else 0

        return success_count, review_count, posted_count

    def _record_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = self._normalize_payload(payload)

        memory_key = self._build_memory_key(
            event_type=data["event_type"],
            vendor=data["vendor"],
            client_code=data["client_code"],
            doc_type=data["doc_type"],
            category=data["category"],
            gl_account=data["gl_account"],
            tax_code=data["tax_code"],
        )

        vendor_key = normalize_key(data["vendor"])
        client_code_key = normalize_key(data["client_code"])
        success_inc, review_inc, posted_inc = self._classify_counters(data["event_type"])
        now = utc_now_iso()

        with open_db(self.db_path) as conn:
            existing = conn.execute(
                f"""
                SELECT *
                FROM {PATTERN_TABLE}
                WHERE memory_key = ?
                LIMIT 1
                """,
                (memory_key,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    f"""
                    INSERT INTO {PATTERN_TABLE} (
                        memory_key,
                        event_type,
                        vendor,
                        vendor_key,
                        client_code,
                        client_code_key,
                        doc_type,
                        category,
                        gl_account,
                        tax_code,
                        outcome_count,
                        success_count,
                        review_count,
                        posted_count,
                        avg_confidence,
                        avg_amount,
                        last_document_id,
                        last_payload_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        memory_key,
                        data["event_type"],
                        data["vendor"],
                        vendor_key,
                        data["client_code"],
                        client_code_key,
                        data["doc_type"],
                        data["category"],
                        data["gl_account"],
                        data["tax_code"],
                        1,
                        success_inc,
                        review_inc,
                        posted_inc,
                        float(data["confidence"] or 0.0),
                        data["amount"],
                        data["document_id"],
                        json.dumps(payload, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                conn.commit()

                return {
                    "ok": True,
                    "status": "inserted",
                    "memory_key": memory_key,
                    "event_type": data["event_type"],
                    "outcome_count": 1,
                }

            outcome_count = int(existing["outcome_count"] or 0)
            new_outcome_count = outcome_count + 1

            old_avg_confidence = safe_float(existing["avg_confidence"]) or 0.0
            new_confidence = float(data["confidence"] or 0.0)
            avg_confidence = ((old_avg_confidence * outcome_count) + new_confidence) / new_outcome_count
            # FIX 5: Cap confidence at 0.95
            avg_confidence = min(0.95, avg_confidence)

            old_avg_amount = safe_float(existing["avg_amount"])
            new_amount = data["amount"]
            if new_amount is None:
                avg_amount = old_avg_amount
            elif old_avg_amount is None or outcome_count <= 0:
                avg_amount = new_amount
            else:
                avg_amount = ((old_avg_amount * outcome_count) + new_amount) / new_outcome_count

            conn.execute(
                f"""
                UPDATE {PATTERN_TABLE}
                SET
                    event_type = ?,
                    vendor = ?,
                    vendor_key = ?,
                    client_code = ?,
                    client_code_key = ?,
                    doc_type = ?,
                    category = ?,
                    gl_account = ?,
                    tax_code = ?,
                    outcome_count = ?,
                    success_count = COALESCE(success_count, 0) + ?,
                    review_count = COALESCE(review_count, 0) + ?,
                    posted_count = COALESCE(posted_count, 0) + ?,
                    avg_confidence = ?,
                    avg_amount = ?,
                    last_document_id = ?,
                    last_payload_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    data["event_type"],
                    data["vendor"],
                    vendor_key,
                    data["client_code"],
                    client_code_key,
                    data["doc_type"],
                    data["category"],
                    data["gl_account"],
                    data["tax_code"],
                    new_outcome_count,
                    success_inc,
                    review_inc,
                    posted_inc,
                    avg_confidence,
                    avg_amount,
                    data["document_id"],
                    json.dumps(payload, ensure_ascii=False),
                    now,
                    int(existing["id"]),
                ),
            )
            conn.commit()

            return {
                "ok": True,
                "status": "updated",
                "memory_key": memory_key,
                "event_type": data["event_type"],
                "outcome_count": new_outcome_count,
            }

    def record_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_from_payload(payload)

    def record_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_from_payload(payload)

    def add_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_from_payload(payload)

    def store_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_from_payload(payload)

    def remember_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._record_from_payload(payload)

    def get_best_match(
        self,
        *,
        event_type: str = "",
        vendor: str = "",
        client_code: str = "",
        doc_type: str = "",
        category: str = "",
        gl_account: str = "",
        tax_code: str = "",
    ) -> dict[str, Any] | None:
        memory_key = self._build_memory_key(
            event_type=event_type,
            vendor=vendor,
            client_code=client_code,
            doc_type=doc_type,
            category=category,
            gl_account=gl_account,
            tax_code=tax_code,
        )

        with open_db(self.db_path) as conn:
            row = conn.execute(
                f"""
                SELECT *
                FROM {PATTERN_TABLE}
                WHERE memory_key = ?
                LIMIT 1
                """,
                (memory_key,),
            ).fetchone()

        return dict(row) if row else None

    def reset(self, vendor_key: str = "", client_code: str = "") -> dict[str, Any]:
        """Reset (delete) learning memory patterns for a given vendor+client."""
        vk = normalize_key(vendor_key)
        ck = normalize_key(client_code)
        with open_db(self.db_path) as conn:
            cursor = conn.execute(
                f"DELETE FROM {PATTERN_TABLE} WHERE vendor_key = ? AND client_code_key = ?",
                (vk, ck),
            )
            conn.commit()
        return {"ok": True, "deleted": cursor.rowcount, "vendor_key": vk, "client_code_key": ck}

    def clear(self, vendor_key: str = "", client_code: str = "") -> dict[str, Any]:
        """Alias for reset()."""
        return self.reset(vendor_key, client_code)

    def list_learning_memory(self, limit: int = 100) -> list[dict[str, Any]]:
        with open_db(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {PATTERN_TABLE}
                ORDER BY outcome_count DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(r) for r in rows]

    def summarize_patterns(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.list_learning_memory(limit=limit)
        results: list[dict[str, Any]] = []

        for row in rows:
            outcome_count = int(row.get("outcome_count") or 0)
            success_count = int(row.get("success_count") or 0)
            review_count = int(row.get("review_count") or 0)
            posted_count = int(row.get("posted_count") or 0)

            success_rate = round(success_count / outcome_count, 4) if outcome_count else 0.0
            review_rate = round(review_count / outcome_count, 4) if outcome_count else 0.0
            posted_rate = round(posted_count / outcome_count, 4) if outcome_count else 0.0

            results.append(
                {
                    "memory_key": row.get("memory_key"),
                    "event_type": row.get("event_type"),
                    "vendor": row.get("vendor"),
                    "client_code": row.get("client_code"),
                    "doc_type": row.get("doc_type"),
                    "category": row.get("category"),
                    "gl_account": row.get("gl_account"),
                    "tax_code": row.get("tax_code"),
                    "outcome_count": outcome_count,
                    "success_rate": success_rate,
                    "review_rate": review_rate,
                    "posted_rate": posted_rate,
                    "avg_confidence": safe_float(row.get("avg_confidence")) or 0.0,
                    "avg_amount": safe_float(row.get("avg_amount")),
                    "updated_at": row.get("updated_at"),
                }
            )

        return results


def reset_vendor_memory(vendor_key: str, client_code: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """Clear all learning_memory_patterns for a vendor+client."""
    vk = normalize_key(vendor_key)
    ck = normalize_key(client_code)
    cursor = conn.execute(
        f"DELETE FROM {PATTERN_TABLE} WHERE vendor_key = ? AND client_code_key = ?",
        (vk, ck),
    )
    conn.commit()
    return {"ok": True, "deleted": cursor.rowcount, "vendor_key": vk, "client_code_key": ck}


def reset_learning_corrections(vendor_key: str, client_code: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """Clear all learning_corrections for a vendor+client."""
    vk = normalize_key(vendor_key)
    ck = normalize_key(client_code)
    deleted = 0
    from src.agents.core.learning_correction_store import LearningCorrectionStore
    tbl = LearningCorrectionStore.TABLE_NAME
    try:
        cursor = conn.execute(
            f"DELETE FROM {tbl} WHERE vendor_key = ? AND client_code_key = ?",
            (vk, ck),
        )
        deleted = cursor.rowcount
        conn.commit()
    except Exception:
        pass
    return {"ok": True, "deleted": deleted, "vendor_key": vk, "client_code_key": ck}


def check_correction_integrity(
    vendor_key: str,
    client_code: str,
    field_name: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """FIX 2: Verify correction pattern integrity before applying.
    Returns flags: multi_user_verified, correction_velocity_anomaly."""
    from src.agents.core.learning_correction_store import LearningCorrectionStore, normalize_key as lc_normalize_key
    tbl = LearningCorrectionStore.TABLE_NAME
    vk = normalize_key(vendor_key)
    ck = normalize_key(client_code)
    fk = normalize_key(field_name)

    result: dict[str, Any] = {
        "multi_user_verified": False,
        "correction_velocity_anomaly": False,
        "requires_manager_review": False,
    }

    try:
        # Check 1: At least 2 different usernames (reviewers)
        reviewer_rows = conn.execute(
            f"""
            SELECT DISTINCT reviewer FROM {tbl}
            WHERE vendor_key = ? AND client_code_key = ? AND field_name_key = ?
              AND reviewer != ''
            """,
            (vk, ck, fk),
        ).fetchall()
        distinct_reviewers = len(reviewer_rows)
        result["multi_user_verified"] = distinct_reviewers >= 2
        result["distinct_reviewers"] = distinct_reviewers

        # Check 2: Correction velocity — >30% changed in last 7 days
        total_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE vendor_key = ? AND client_code_key = ? AND field_name_key = ?",
            (vk, ck, fk),
        ).fetchone()
        total = total_row["cnt"] if total_row else 0

        recent_row = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt FROM {tbl}
            WHERE vendor_key = ? AND client_code_key = ? AND field_name_key = ?
              AND updated_at > datetime('now', '-7 days')
            """,
            (vk, ck, fk),
        ).fetchone()
        recent = recent_row["cnt"] if recent_row else 0

        if total > 0 and (recent / total) > 0.30:
            result["correction_velocity_anomaly"] = True
            result["requires_manager_review"] = True
            result["velocity_ratio"] = round(recent / total, 2)
    except Exception:
        pass

    return result


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Learning memory store utility")
    subparsers = parser.add_subparsers(dest="command")

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--limit", type=int, default=50)

    pattern_parser = subparsers.add_parser("patterns")
    pattern_parser.add_argument("--limit", type=int, default=50)

    feedback_parser = subparsers.add_parser("record")
    feedback_parser.add_argument("--document-id", default="")
    feedback_parser.add_argument("--event-type", required=True)
    feedback_parser.add_argument("--vendor", default="")
    feedback_parser.add_argument("--client-code", default="")
    feedback_parser.add_argument("--doc-type", default="")
    feedback_parser.add_argument("--category", default="")
    feedback_parser.add_argument("--gl-account", default="")
    feedback_parser.add_argument("--tax-code", default="")
    feedback_parser.add_argument("--amount", default="")
    feedback_parser.add_argument("--confidence", default="")

    args = parser.parse_args()
    store = LearningMemoryStore()

    if args.command == "list":
        result = store.list_learning_memory(limit=max(1, int(args.limit)))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "patterns":
        result = store.summarize_patterns(limit=max(1, int(args.limit)))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "record":
        payload = {
            "document_id": args.document_id,
            "event_type": args.event_type,
            "vendor": args.vendor,
            "client_code": args.client_code,
            "doc_type": args.doc_type,
            "category": args.category,
            "gl_account": args.gl_account,
            "tax_code": args.tax_code,
            "amount": args.amount,
            "confidence": args.confidence,
            "created_at": utc_now_iso(),
        }
        result = store.record_feedback(payload)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())