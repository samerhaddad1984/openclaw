from __future__ import annotations

import argparse
import json
import sqlite3
import unicodedata
from dataclasses import dataclass, asdict
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


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    text = normalize_text(value).casefold()
    # FIX 4: Strip accents so "Québec" == "quebec"
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")
    for ch in [",", ".", ";", ":", "'", '"', "(", ")", "[", "]", "{", "}", "/", "\\", "-", "_", "|"]:
        text = text.replace(ch, " ")
    return " ".join(text.split())


def normalize_amount(value: Any) -> float | None:
    text = normalize_text(value)
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


@dataclass
class LearningCorrectionRecord:
    id: int | None
    document_id: str
    client_code: str
    client_code_key: str
    vendor: str
    vendor_key: str
    doc_type: str
    doc_type_key: str
    category: str
    category_key: str
    field_name: str
    field_name_key: str
    old_value: str
    old_value_key: str
    new_value: str
    new_value_key: str
    reviewer: str
    source: str
    confidence_before: float | None
    notes: str
    support_count: int
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LearningCorrectionSuggestion:
    field_name: str
    suggested_value: str
    confidence: float
    support_count: int
    exact_vendor_match: bool
    exact_client_match: bool
    exact_doc_type_match: bool
    exact_category_match: bool
    matched_records: list[dict[str, Any]]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class LearningCorrectionStore:
    TABLE_NAME = "learning_corrections"

    def __init__(self, *, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.ensure_table()

    def _open(self) -> sqlite3.Connection:
        return open_db(self.db_path)

    def ensure_table(self) -> None:
        with self._open() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id TEXT NOT NULL DEFAULT '',
                    client_code TEXT NOT NULL DEFAULT '',
                    client_code_key TEXT NOT NULL DEFAULT '',
                    vendor TEXT NOT NULL DEFAULT '',
                    vendor_key TEXT NOT NULL DEFAULT '',
                    doc_type TEXT NOT NULL DEFAULT '',
                    doc_type_key TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT '',
                    category_key TEXT NOT NULL DEFAULT '',
                    field_name TEXT NOT NULL DEFAULT '',
                    field_name_key TEXT NOT NULL DEFAULT '',
                    old_value TEXT NOT NULL DEFAULT '',
                    old_value_key TEXT NOT NULL DEFAULT '',
                    new_value TEXT NOT NULL DEFAULT '',
                    new_value_key TEXT NOT NULL DEFAULT '',
                    reviewer TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    confidence_before REAL,
                    notes TEXT NOT NULL DEFAULT '',
                    support_count INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                )
                """
            )

            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_lookup
                ON {self.TABLE_NAME} (
                    field_name_key,
                    vendor_key,
                    client_code_key,
                    doc_type_key,
                    category_key,
                    new_value_key
                )
                """
            )

            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_vendor_field
                ON {self.TABLE_NAME} (
                    vendor_key,
                    field_name_key
                )
                """
            )

            conn.commit()

    def _load_document_context(self, document_id: str) -> dict[str, Any]:
        with self._open() as conn:
            if not table_exists(conn, "documents"):
                return {}

            row = conn.execute(
                """
                SELECT *
                FROM documents
                WHERE document_id = ?
                LIMIT 1
                """,
                (document_id,),
            ).fetchone()

        if row is None:
            return {}

        raw = safe_json_loads(row["raw_result"]) if "raw_result" in row.keys() else {}

        return {
            "document_id": normalize_text(row["document_id"]) if "document_id" in row.keys() else "",
            "client_code": normalize_text(row["client_code"]) if "client_code" in row.keys() else normalize_text(raw.get("client_code")),
            "vendor": normalize_text(row["vendor"]) if "vendor" in row.keys() else normalize_text(raw.get("vendor")),
            "doc_type": normalize_text(row["doc_type"]) if "doc_type" in row.keys() else normalize_text(raw.get("doc_type")),
            "category": normalize_text(row["category"]) if "category" in row.keys() else normalize_text(raw.get("category")),
            "gl_account": normalize_text(row["gl_account"]) if "gl_account" in row.keys() else normalize_text(raw.get("gl_account")),
            "tax_code": normalize_text(row["tax_code"]) if "tax_code" in row.keys() else normalize_text(raw.get("tax_code")),
            "amount": normalize_amount(row["amount"]) if "amount" in row.keys() else normalize_amount(raw.get("amount")),
            "confidence": normalize_amount(row["confidence"]) if "confidence" in row.keys() else normalize_amount(raw.get("confidence")),
        }

    # FIX 5: Maximum corrections per vendor per day per client
    MAX_CORRECTIONS_PER_VENDOR_DAY = 10

    def _check_rate_limit(self, vendor_key: str, client_code_key: str) -> bool:
        """Return True if rate limit exceeded (10 corrections/vendor/day/client)."""
        with self._open() as conn:
            if not table_exists(conn, self.TABLE_NAME):
                return False
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM {self.TABLE_NAME}
                WHERE vendor_key = ?
                  AND client_code_key = ?
                  AND updated_at >= ?
                """,
                (vendor_key, client_code_key, today),
            ).fetchone()
            return (row["cnt"] if row else 0) >= self.MAX_CORRECTIONS_PER_VENDOR_DAY

    def _check_gl_anomaly(self, vendor_key: str, client_code_key: str, new_value: str) -> dict[str, Any] | None:
        """FIX 5: Detect dramatic GL account changes in fewer than 5 samples."""
        if normalize_key("gl_account") not in ("gl_account", "gl account"):
            return None
        with self._open() as conn:
            if not table_exists(conn, self.TABLE_NAME):
                return None
            rows = conn.execute(
                f"""
                SELECT new_value, support_count
                FROM {self.TABLE_NAME}
                WHERE vendor_key = ?
                  AND client_code_key = ?
                  AND field_name_key = ?
                ORDER BY support_count DESC
                LIMIT 5
                """,
                (vendor_key, client_code_key, normalize_key("gl_account")),
            ).fetchall()
            if not rows:
                return None
            total_support = sum(int(r["support_count"] or 0) for r in rows)
            top_value = normalize_key(rows[0]["new_value"])
            if top_value and normalize_key(new_value) != top_value and total_support < 5:
                return {
                    "anomaly": "vendor_memory_anomaly",
                    "reason": f"GL account change from '{rows[0]['new_value']}' to '{new_value}' with only {total_support} samples",
                    "requires_manager_review": True,
                }
        return None

    def record_correction(
        self,
        *,
        document_id: str = "",
        client_code: str = "",
        vendor: str = "",
        doc_type: str = "",
        category: str = "",
        field_name: str,
        old_value: str = "",
        new_value: str,
        reviewer: str = "",
        source: str = "manual",
        confidence_before: float | None = None,
        notes: str = "",
    ) -> dict[str, Any]:
        self.ensure_table()

        field_name = normalize_text(field_name)
        new_value = normalize_text(new_value)

        if not field_name:
            raise ValueError("field_name is required")

        if not new_value:
            raise ValueError("new_value is required")

        doc_context = self._load_document_context(document_id) if document_id else {}

        if not client_code:
            client_code = normalize_text(doc_context.get("client_code"))
        if not vendor:
            vendor = normalize_text(doc_context.get("vendor"))
        if not doc_type:
            doc_type = normalize_text(doc_context.get("doc_type"))
        if not category:
            category = normalize_text(doc_context.get("category"))

        if not old_value and field_name:
            old_value = normalize_text(doc_context.get(field_name))

        if confidence_before is None:
            confidence_before = doc_context.get("confidence")
            if confidence_before is not None:
                try:
                    confidence_before = float(confidence_before)
                except Exception:
                    confidence_before = None

        now = utc_now_iso()

        client_code_key = normalize_key(client_code)
        vendor_key = normalize_key(vendor)
        doc_type_key = normalize_key(doc_type)
        category_key = normalize_key(category)
        field_name_key = normalize_key(field_name)
        old_value_key = normalize_key(old_value)
        new_value_key = normalize_key(new_value)

        # FIX 5: Rate limiting — max 10 corrections per vendor per day per client
        if vendor_key and self._check_rate_limit(vendor_key, client_code_key):
            return {
                "ok": False,
                "status": "rate_limited",
                "reason": f"Maximum {self.MAX_CORRECTIONS_PER_VENDOR_DAY} corrections per vendor per day exceeded",
                "field_name": field_name,
                "new_value": new_value,
            }

        # FIX 5: GL anomaly detection
        if field_name_key == normalize_key("gl_account") and vendor_key:
            anomaly = self._check_gl_anomaly(vendor_key, client_code_key, new_value)
            if anomaly:
                notes = (notes + " | " if notes else "") + anomaly["reason"]

        with self._open() as conn:
            existing = conn.execute(
                f"""
                SELECT id, support_count
                FROM {self.TABLE_NAME}
                WHERE client_code_key = ?
                  AND vendor_key = ?
                  AND doc_type_key = ?
                  AND category_key = ?
                  AND field_name_key = ?
                  AND old_value_key = ?
                  AND new_value_key = ?
                LIMIT 1
                """,
                (
                    client_code_key,
                    vendor_key,
                    doc_type_key,
                    category_key,
                    field_name_key,
                    old_value_key,
                    new_value_key,
                ),
            ).fetchone()

            if existing is not None:
                support_count = int(existing["support_count"] or 0) + 1
                conn.execute(
                    f"""
                    UPDATE {self.TABLE_NAME}
                    SET
                        document_id = ?,
                        client_code = ?,
                        vendor = ?,
                        doc_type = ?,
                        category = ?,
                        field_name = ?,
                        old_value = ?,
                        new_value = ?,
                        reviewer = ?,
                        source = ?,
                        confidence_before = ?,
                        notes = ?,
                        support_count = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        document_id,
                        client_code,
                        vendor,
                        doc_type,
                        category,
                        field_name,
                        old_value,
                        new_value,
                        reviewer,
                        source,
                        confidence_before,
                        notes,
                        support_count,
                        now,
                        int(existing["id"]),
                    ),
                )
                conn.commit()

                return {
                    "ok": True,
                    "status": "updated",
                    "id": int(existing["id"]),
                    "support_count": support_count,
                    "field_name": field_name,
                    "new_value": new_value,
                }

            cursor = conn.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    document_id,
                    client_code,
                    client_code_key,
                    vendor,
                    vendor_key,
                    doc_type,
                    doc_type_key,
                    category,
                    category_key,
                    field_name,
                    field_name_key,
                    old_value,
                    old_value_key,
                    new_value,
                    new_value_key,
                    reviewer,
                    source,
                    confidence_before,
                    notes,
                    support_count,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document_id,
                    client_code,
                    client_code_key,
                    vendor,
                    vendor_key,
                    doc_type,
                    doc_type_key,
                    category,
                    category_key,
                    field_name,
                    field_name_key,
                    old_value,
                    old_value_key,
                    new_value,
                    new_value_key,
                    reviewer,
                    source,
                    confidence_before,
                    notes,
                    1,
                    now,
                    now,
                ),
            )
            conn.commit()

            return {
                "ok": True,
                "status": "inserted",
                "id": int(cursor.lastrowid),
                "support_count": 1,
                "field_name": field_name,
                "new_value": new_value,
            }

    def reset(self, vendor_key: str = "", client_code: str = "") -> dict[str, Any]:
        """Reset (delete) learning corrections for a given vendor+client."""
        vk = normalize_key(vendor_key)
        ck = normalize_key(client_code)
        with self._open() as conn:
            cursor = conn.execute(
                f"DELETE FROM {self.TABLE_NAME} WHERE vendor_key = ? AND client_code_key = ?",
                (vk, ck),
            )
            conn.commit()
        return {"ok": True, "deleted": cursor.rowcount, "vendor_key": vk, "client_code_key": ck}

    def clear(self, vendor_key: str = "", client_code: str = "") -> dict[str, Any]:
        """Alias for reset()."""
        return self.reset(vendor_key, client_code)

    def list_corrections(self, *, limit: int = 50) -> list[dict[str, Any]]:
        self.ensure_table()

        with self._open() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()

        return [dict(row) for row in rows]

    def suggest(
        self,
        *,
        document_id: str = "",
        client_code: str = "",
        vendor: str = "",
        doc_type: str = "",
        category: str = "",
        field_name: str,
        old_value: str = "",
        limit: int = 10,
    ) -> dict[str, Any]:
        self.ensure_table()

        field_name = normalize_text(field_name)
        if not field_name:
            raise ValueError("field_name is required")

        doc_context = self._load_document_context(document_id) if document_id else {}

        if not client_code:
            client_code = normalize_text(doc_context.get("client_code"))
        if not vendor:
            vendor = normalize_text(doc_context.get("vendor"))
        if not doc_type:
            doc_type = normalize_text(doc_context.get("doc_type"))
        if not category:
            category = normalize_text(doc_context.get("category"))
        if not old_value and field_name:
            old_value = normalize_text(doc_context.get(field_name))

        client_code_key = normalize_key(client_code)
        vendor_key = normalize_key(vendor)
        doc_type_key = normalize_key(doc_type)
        category_key = normalize_key(category)
        field_name_key = normalize_key(field_name)
        old_value_key = normalize_key(old_value)

        with self._open() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE field_name_key = ?
                ORDER BY support_count DESC, updated_at DESC, id DESC
                LIMIT 500
                """,
                (field_name_key,),
            ).fetchall()

        scored: list[tuple[float, sqlite3.Row]] = []

        for row in rows:
            score = 0.0

            row_vendor_key = normalize_key(row["vendor"])
            row_client_key = normalize_key(row["client_code"])
            row_doc_type_key = normalize_key(row["doc_type"])
            row_category_key = normalize_key(row["category"])
            row_old_value_key = normalize_key(row["old_value"])
            row_new_value_key = normalize_key(row["new_value"])

            if vendor_key and row_vendor_key:
                if vendor_key == row_vendor_key:
                    score += 0.35
                else:
                    continue
            elif not vendor_key and not row_vendor_key:
                score += 0.03

            # FIX 4: Strict client isolation — skip rows from other clients
            if client_code_key and row_client_key:
                if client_code_key == row_client_key:
                    score += 0.25
                elif row_client_key == "__global__":
                    score += 0.10
                else:
                    continue
            elif not client_code_key and not row_client_key:
                score += 0.03

            if doc_type_key and row_doc_type_key:
                if doc_type_key == row_doc_type_key:
                    score += 0.12
            elif not doc_type_key and not row_doc_type_key:
                score += 0.02

            if category_key and row_category_key:
                if category_key == row_category_key:
                    score += 0.10
            elif not category_key and not row_category_key:
                score += 0.02

            if old_value_key and row_old_value_key:
                if old_value_key == row_old_value_key:
                    score += 0.10

            support_count = int(row["support_count"] or 0)
            score += min(0.20, support_count * 0.05)

            if row_new_value_key == old_value_key and old_value_key:
                score -= 0.50

            if score <= 0:
                continue

            scored.append((round(score, 4), row))

        scored.sort(
            key=lambda item: (
                -item[0],
                -int(item[1]["support_count"] or 0),
                normalize_text(item[1]["updated_at"]),
            )
        )

        if not scored:
            return {
                "ok": True,
                "found": False,
                "field_name": field_name,
                "suggested_value": "",
                "confidence": 0.0,
                "support_count": 0,
                "matched_records": [],
                "reason": "no_correction_match",
            }

        grouped: dict[str, dict[str, Any]] = {}

        for score, row in scored:
            key = normalize_key(row["new_value"])
            if key not in grouped:
                grouped[key] = {
                    "new_value": normalize_text(row["new_value"]),
                    "best_score": score,
                    "support_count": 0,
                    "records": [],
                    "exact_vendor_match": False,
                    "exact_client_match": False,
                    "exact_doc_type_match": False,
                    "exact_category_match": False,
                }

            grouped[key]["support_count"] += int(row["support_count"] or 0)
            grouped[key]["records"].append(dict(row))

            if vendor_key and vendor_key == normalize_key(row["vendor"]):
                grouped[key]["exact_vendor_match"] = True
            if client_code_key and client_code_key == normalize_key(row["client_code"]):
                grouped[key]["exact_client_match"] = True
            if doc_type_key and doc_type_key == normalize_key(row["doc_type"]):
                grouped[key]["exact_doc_type_match"] = True
            if category_key and category_key == normalize_key(row["category"]):
                grouped[key]["exact_category_match"] = True

            grouped[key]["best_score"] = max(grouped[key]["best_score"], score)

        ranked = sorted(
            grouped.values(),
            key=lambda item: (
                -float(item["best_score"]),
                -int(item["support_count"]),
                item["new_value"],
            ),
        )

        top = ranked[0]
        # FIX 5: Cap confidence at 0.95
        confidence = min(0.95, float(top["best_score"]))

        # FIX 5: Minimum support_count >= 3 before pattern influences suggestions
        if int(top["support_count"]) < 3:
            return {
                "ok": True,
                "found": False,
                "field_name": field_name,
                "suggested_value": "",
                "confidence": 0.0,
                "support_count": int(top["support_count"]),
                "matched_records": [],
                "reason": "insufficient_support_count",
            }

        if top["exact_vendor_match"] and top["exact_client_match"] and top["exact_doc_type_match"]:
            reason = "strong_exact_context_match"
        elif top["exact_vendor_match"] and top["exact_client_match"]:
            reason = "strong_vendor_client_match"
        elif confidence >= 0.70:
            reason = "strong_partial_match"
        else:
            reason = "weak_match"

        suggestion = LearningCorrectionSuggestion(
            field_name=field_name,
            suggested_value=top["new_value"],
            confidence=round(confidence, 2),
            support_count=int(top["support_count"]),
            exact_vendor_match=bool(top["exact_vendor_match"]),
            exact_client_match=bool(top["exact_client_match"]),
            exact_doc_type_match=bool(top["exact_doc_type_match"]),
            exact_category_match=bool(top["exact_category_match"]),
            matched_records=top["records"][: max(1, int(limit))],
            reason=reason,
        )

        return {
            "ok": True,
            "found": True,
            **suggestion.to_dict(),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="OtoCPA learning correction store")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init", help="Create correction table if missing")

    parser_list = subparsers.add_parser("list", help="List correction records")
    parser_list.add_argument("--limit", type=int, default=50)

    parser_record = subparsers.add_parser("record-correction", help="Record a manual field correction")
    parser_record.add_argument("--document-id", default="")
    parser_record.add_argument("--client-code", default="")
    parser_record.add_argument("--vendor", default="")
    parser_record.add_argument("--doc-type", default="")
    parser_record.add_argument("--category", default="")
    parser_record.add_argument("--field-name", required=True)
    parser_record.add_argument("--old-value", default="")
    parser_record.add_argument("--new-value", required=True)
    parser_record.add_argument("--reviewer", default="")
    parser_record.add_argument("--source", default="manual")
    parser_record.add_argument("--confidence-before", type=float, default=None)
    parser_record.add_argument("--notes", default="")

    parser_suggest = subparsers.add_parser("suggest", help="Suggest corrected value")
    parser_suggest.add_argument("--document-id", default="")
    parser_suggest.add_argument("--client-code", default="")
    parser_suggest.add_argument("--vendor", default="")
    parser_suggest.add_argument("--doc-type", default="")
    parser_suggest.add_argument("--category", default="")
    parser_suggest.add_argument("--field-name", required=True)
    parser_suggest.add_argument("--old-value", default="")
    parser_suggest.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    store = LearningCorrectionStore()

    if args.command == "init":
        store.ensure_table()
        print(json.dumps({"ok": True, "status": "initialized"}, indent=2, ensure_ascii=False))
        return 0

    if args.command == "list":
        result = store.list_corrections(limit=max(1, int(args.limit)))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "record-correction":
        result = store.record_correction(
            document_id=args.document_id,
            client_code=args.client_code,
            vendor=args.vendor,
            doc_type=args.doc_type,
            category=args.category,
            field_name=args.field_name,
            old_value=args.old_value,
            new_value=args.new_value,
            reviewer=args.reviewer,
            source=args.source,
            confidence_before=args.confidence_before,
            notes=args.notes,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "suggest":
        result = store.suggest(
            document_id=args.document_id,
            client_code=args.client_code,
            vendor=args.vendor,
            doc_type=args.doc_type,
            category=args.category,
            field_name=args.field_name,
            old_value=args.old_value,
            limit=max(1, int(args.limit)),
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    raise SystemExit(1)


if __name__ == "__main__":
    raise SystemExit(main())