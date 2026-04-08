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
    # FIX: Mixed-script attack prevention — do NOT normalize confusables for
    # mixed-script text so Cyrillic homoglyphs produce a distinct key.
    from src.agents.tools.fingerprint_utils import detect_mixed_scripts, normalize_confusables
    if not detect_mixed_scripts(text)["is_mixed"]:
        text = normalize_confusables(text)
    # FIX 4: Strip accents so "Hydro-Québec" == "Hydro-Quebec"
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


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


class VendorMemoryStore:
    """
    Stores durable vendor accounting memory learned from approved/posted outcomes.

    Supports:
    - record_approval(document={...})
    - record_approval(vendor=..., gl_account=..., ...)
    - lookups by normalized vendor + client + doc_type
    - automatic migration from the old vendor_memory schema
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._ensure_table()

    def _ensure_table(self) -> None:
        with open_db(self.db_path) as conn:
            if not table_exists(conn, "vendor_memory"):
                conn.execute(
                    """
                    CREATE TABLE vendor_memory (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        client_code TEXT,
                        vendor TEXT NOT NULL,
                        vendor_key TEXT NOT NULL DEFAULT '',
                        client_code_key TEXT NOT NULL DEFAULT '',
                        gl_account TEXT,
                        tax_code TEXT,
                        doc_type TEXT,
                        category TEXT,
                        approval_count INTEGER NOT NULL DEFAULT 0,
                        confidence REAL NOT NULL DEFAULT 0.0,
                        last_amount REAL,
                        last_document_id TEXT,
                        last_source TEXT,
                        last_used TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

            columns = get_table_columns(conn, "vendor_memory")

            required_additions: list[tuple[str, str]] = [
                ("vendor_key", "TEXT NOT NULL DEFAULT ''"),
                ("client_code_key", "TEXT NOT NULL DEFAULT ''"),
                ("last_amount", "REAL"),
                ("last_document_id", "TEXT"),
                ("last_source", "TEXT"),
            ]

            for column_name, column_type in required_additions:
                if column_name not in columns:
                    conn.execute(
                        f"ALTER TABLE vendor_memory ADD COLUMN {column_name} {column_type}"
                    )

            conn.commit()

            columns = get_table_columns(conn, "vendor_memory")

            if "vendor_key" in columns:
                conn.execute(
                    """
                    UPDATE vendor_memory
                    SET vendor_key = LOWER(TRIM(COALESCE(vendor, '')))
                    WHERE COALESCE(vendor_key, '') = ''
                    """
                )

            if "client_code_key" in columns:
                conn.execute(
                    """
                    UPDATE vendor_memory
                    SET client_code_key = LOWER(TRIM(COALESCE(client_code, '')))
                    WHERE COALESCE(client_code_key, '') = ''
                    """
                )

            conn.commit()

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_vendor_memory_vendor_key
                ON vendor_memory(vendor_key)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_vendor_memory_client_vendor
                ON vendor_memory(client_code_key, vendor_key)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_vendor_memory_lookup
                ON vendor_memory(vendor_key, client_code_key, doc_type, updated_at)
                """
            )

            conn.commit()

    def _extract_document_fields(self, document: dict[str, Any]) -> dict[str, Any]:
        raw_result = safe_json_loads(document.get("raw_result"))
        posting_payload = safe_json_loads(document.get("payload_json"))
        posting_payload = posting_payload or safe_json_loads(raw_result.get("posting_payload"))

        extracted = {
            "document_id": normalize_text(document.get("document_id"))
            or normalize_text(posting_payload.get("document_id")),
            "client_code": normalize_text(document.get("client_code"))
            or normalize_text(posting_payload.get("client_code")),
            "vendor": normalize_text(document.get("vendor"))
            or normalize_text(posting_payload.get("vendor")),
            "gl_account": normalize_text(document.get("gl_account"))
            or normalize_text(posting_payload.get("gl_account")),
            "tax_code": normalize_text(document.get("tax_code"))
            or normalize_text(posting_payload.get("tax_code")),
            "doc_type": normalize_text(document.get("doc_type"))
            or normalize_text(posting_payload.get("doc_type")),
            "category": normalize_text(document.get("category"))
            or normalize_text(posting_payload.get("category")),
            "amount": safe_float(document.get("amount")),
            "source": normalize_text(document.get("source")) or "document",
        }

        if extracted["amount"] is None:
            extracted["amount"] = safe_float(posting_payload.get("amount"))

        return extracted

    def get_best_match(
        self,
        *,
        vendor: str,
        client_code: str = "",
        doc_type: str = "",
        min_support: int = 3,
        substance_flags: dict[str, Any] | None = None,
        substance_confidence: float = 0.0,
    ) -> dict[str, Any] | None:
        vendor_key = normalize_key(vendor)
        client_code_key = normalize_key(client_code)
        doc_type = normalize_text(doc_type)

        if not vendor_key:
            return None

        # FIX: Mixed-script attack detection — flag and cap confidence
        from src.agents.tools.fingerprint_utils import detect_mixed_scripts
        mixed_info = detect_mixed_scripts(vendor)
        if mixed_info["is_mixed"]:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "mixed_script_attack_detected vendor=%s scripts=%s",
                vendor, mixed_info["scripts_found"],
            )
            return None

        # FIX 4: Only return rows that belong to this client or __global__ seed data
        # FIX P0-2: Exclude patterns older than 24 months
        with open_db(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM vendor_memory
                WHERE vendor_key = ?
                  AND (client_code_key = ? OR client_code_key = '__global__' OR client_code_key = '')
                  AND (updated_at > datetime('now', '-24 months') OR updated_at IS NULL OR updated_at = '')
                LIMIT 50
                """,
                (vendor_key, client_code_key),
            ).fetchall()

        if not rows:
            return None

        all_dicts = [dict(r) for r in rows]

        # FIX 5: Minimum support — check total approvals across all GL accounts
        total_approvals = sum(int(d.get("approval_count") or 0) for d in all_dicts)
        if total_approvals < min_support:
            return None

        # FIX P0-1: Evidence weighting — when substance_flags conflict with
        # vendor memory and substance_confidence > 0.80, reduce memory influence.
        evidence_penalty = 1.0
        if substance_flags and substance_confidence > 0.80:
            evidence_penalty = 0.5

        # FIX 1 + P0-2: Recency-weighted scoring with anti-poisoning ordering
        # and time decay per recency tier.
        now = datetime.now(timezone.utc)
        scored = []
        for d in all_dicts:
            updated_str = d.get("updated_at", "")
            try:
                updated = datetime.fromisoformat(updated_str)
                days_old = (now - updated).days
            except Exception:
                days_old = 999

            # FIX P0-2: Tiered recency_score (decays with age)
            if days_old <= 30:
                recency_score = 1.0
            elif days_old <= 90:
                recency_score = 0.8
            elif days_old <= 180:
                recency_score = 0.6
            elif days_old <= 365:
                recency_score = 0.4
            else:
                recency_score = 0.2

            count = int(d.get("approval_count") or 0)
            # FIX P0-2: Score = count * recency_score * evidence_penalty
            score = count * recency_score * evidence_penalty

            # Client match priority
            row_client = d.get("client_code_key", "")
            if row_client == client_code_key and client_code_key:
                client_pri = 0
            elif row_client == "__global__":
                client_pri = 1
            else:
                client_pri = 2

            doc_pri = 0 if d.get("doc_type", "") == doc_type else 1
            created_at = d.get("created_at", "")
            row_id = int(d.get("id") or 999999)

            # Sort key: client match, doc match, worst recency last,
            # earliest created first (anti-poisoning), lowest id tiebreak
            scored.append((client_pri, doc_pri, -recency_score, created_at, row_id, -score, d))

        scored.sort(key=lambda x: x[:6])
        return scored[0][6]

    def list_vendor_memory(self, limit: int = 100) -> list[dict[str, Any]]:
        with open_db(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM vendor_memory
                ORDER BY approval_count DESC, confidence DESC, updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(r) for r in rows]

    def record_approval(
        self,
        document: dict[str, Any] | None = None,
        *,
        document_id: str = "",
        client_code: str = "",
        vendor: str = "",
        gl_account: str = "",
        tax_code: str = "",
        doc_type: str = "",
        category: str = "",
        amount: float | None = None,
        confidence: float | None = None,
        source: str = "approval",
    ) -> dict[str, Any]:
        if document is not None:
            extracted = self._extract_document_fields(document)
            document_id = document_id or extracted["document_id"]
            client_code = client_code or extracted["client_code"]
            vendor = vendor or extracted["vendor"]
            gl_account = gl_account or extracted["gl_account"]
            tax_code = tax_code or extracted["tax_code"]
            doc_type = doc_type or extracted["doc_type"]
            category = category or extracted["category"]
            amount = amount if amount is not None else extracted["amount"]
            source = normalize_text(source) or extracted["source"] or "document"

        document_id = normalize_text(document_id)
        client_code = normalize_text(client_code)
        vendor = normalize_text(vendor)
        gl_account = normalize_text(gl_account)
        tax_code = normalize_text(tax_code)
        doc_type = normalize_text(doc_type)
        category = normalize_text(category)
        source = normalize_text(source) or "approval"
        amount = safe_float(amount)
        confidence_value = safe_float(confidence)
        if confidence_value is None:
            confidence_value = 0.0

        if not vendor:
            return {
                "ok": False,
                "status": "skipped",
                "reason": "missing_vendor",
            }

        if not gl_account:
            return {
                "ok": False,
                "status": "skipped",
                "reason": "missing_gl_account",
            }

        if not tax_code:
            return {
                "ok": False,
                "status": "skipped",
                "reason": "missing_tax_code",
            }

        vendor_key = normalize_key(vendor)
        client_code_key = normalize_key(client_code)
        now = utc_now_iso()

        with open_db(self.db_path) as conn:
            existing = conn.execute(
                """
                SELECT *
                FROM vendor_memory
                WHERE vendor_key = ?
                  AND client_code_key = ?
                  AND COALESCE(doc_type, '') = ?
                  AND COALESCE(gl_account, '') = ?
                  AND COALESCE(tax_code, '') = ?
                  AND COALESCE(category, '') = ?
                LIMIT 1
                """,
                (
                    vendor_key,
                    client_code_key,
                    doc_type,
                    gl_account,
                    tax_code,
                    category,
                ),
            ).fetchone()

            if existing is None:
                approval_count = 1
                # FIX 5: Cap confidence at 0.95 (never 100% certain)
                stored_confidence = max(0.2, min(0.95, confidence_value if confidence_value > 0 else 0.2))

                conn.execute(
                    """
                    INSERT INTO vendor_memory (
                        client_code,
                        vendor,
                        vendor_key,
                        client_code_key,
                        gl_account,
                        tax_code,
                        doc_type,
                        category,
                        approval_count,
                        confidence,
                        last_amount,
                        last_document_id,
                        last_source,
                        last_used,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        client_code,
                        vendor,
                        vendor_key,
                        client_code_key,
                        gl_account,
                        tax_code,
                        doc_type,
                        category,
                        approval_count,
                        stored_confidence,
                        amount,
                        document_id,
                        source,
                        now,
                        now,
                        now,
                    ),
                )
                conn.commit()

                # BLOCK 3: Check if vendor had a different GL before — propagate change
                prior_gl_rows = conn.execute(
                    """SELECT gl_account FROM vendor_memory
                       WHERE vendor_key = ? AND client_code_key = ?
                         AND gl_account != ? AND gl_account != ''
                       LIMIT 1""",
                    (vendor_key, client_code_key, gl_account),
                ).fetchall()
                if prior_gl_rows and gl_account:
                    try:
                        from src.engines.substance_engine import propagate_gl_change_suggestions
                        propagate_gl_change_suggestions(
                            vendor=vendor,
                            new_gl=gl_account,
                            client_code=client_code,
                            db_path=str(self.db_path) if self.db_path else None,
                        )
                    except Exception:
                        pass

                return {
                    "ok": True,
                    "status": "inserted",
                    "vendor": vendor,
                    "client_code": client_code,
                    "gl_account": gl_account,
                    "tax_code": tax_code,
                    "doc_type": doc_type,
                    "category": category,
                    "approval_count": approval_count,
                    "confidence": stored_confidence,
                }

            approval_count = int(existing["approval_count"] or 0) + 1
            prior_confidence = safe_float(existing["confidence"]) or 0.0
            # FIX 5: Cap confidence at 0.95 (never 100% certain)
            stored_confidence = max(prior_confidence, min(0.95, prior_confidence + 0.1, confidence_value if confidence_value > 0 else 0.95))

            conn.execute(
                """
                UPDATE vendor_memory
                SET
                    client_code = ?,
                    vendor = ?,
                    vendor_key = ?,
                    client_code_key = ?,
                    gl_account = ?,
                    tax_code = ?,
                    doc_type = ?,
                    category = ?,
                    approval_count = ?,
                    confidence = ?,
                    last_amount = ?,
                    last_document_id = ?,
                    last_source = ?,
                    last_used = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    client_code,
                    vendor,
                    vendor_key,
                    client_code_key,
                    gl_account,
                    tax_code,
                    doc_type,
                    category,
                    approval_count,
                    stored_confidence,
                    amount,
                    document_id,
                    source,
                    now,
                    now,
                    int(existing["id"]),
                ),
            )
            conn.commit()

            # BLOCK 3: Propagate GL change suggestions when GL account changes
            old_gl = normalize_text(existing["gl_account"] if "gl_account" in existing.keys() else "")
            if gl_account and old_gl and gl_account != old_gl:
                try:
                    from src.engines.substance_engine import propagate_gl_change_suggestions
                    propagate_gl_change_suggestions(
                        vendor=vendor,
                        new_gl=gl_account,
                        client_code=client_code,
                        db_path=str(self.db_path) if self.db_path else None,
                    )
                except Exception:
                    pass

            return {
                "ok": True,
                "status": "updated",
                "vendor": vendor,
                "client_code": client_code,
                "gl_account": gl_account,
                "tax_code": tax_code,
                "doc_type": doc_type,
                "category": category,
                "approval_count": approval_count,
                "confidence": stored_confidence,
            }

    def learn_from_document(self, document: dict[str, Any]) -> dict[str, Any]:
        return self.record_approval(document=document, source="learn_from_document")

    def reset(self, vendor_key: str = "", client_code: str = "") -> dict[str, Any]:
        """Reset (delete) vendor memory for a given vendor+client."""
        vk = normalize_key(vendor_key)
        ck = normalize_key(client_code)
        with open_db(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM vendor_memory WHERE vendor_key = ? AND client_code_key = ?",
                (vk, ck),
            )
            conn.commit()
        return {"ok": True, "deleted": cursor.rowcount, "vendor_key": vk, "client_code_key": ck}

    def clear(self, vendor_key: str = "", client_code: str = "") -> dict[str, Any]:
        """Alias for reset()."""
        return self.reset(vendor_key, client_code)

    def record_rejection(
        self,
        *,
        vendor: str = "",
        client_code: str = "",
        doc_type: str = "",
        gl_account: str = "",
        tax_code: str = "",
        category: str = "",
    ) -> dict[str, Any]:
        vendor_key = normalize_key(vendor)
        client_code_key = normalize_key(client_code)
        doc_type = normalize_text(doc_type)
        gl_account = normalize_text(gl_account)
        tax_code = normalize_text(tax_code)
        category = normalize_text(category)

        if not vendor_key:
            return {
                "ok": False,
                "status": "skipped",
                "reason": "missing_vendor",
            }

        with open_db(self.db_path) as conn:
            existing = conn.execute(
                """
                SELECT *
                FROM vendor_memory
                WHERE vendor_key = ?
                  AND client_code_key = ?
                  AND COALESCE(doc_type, '') = ?
                  AND COALESCE(gl_account, '') = ?
                  AND COALESCE(tax_code, '') = ?
                  AND COALESCE(category, '') = ?
                LIMIT 1
                """,
                (
                    vendor_key,
                    client_code_key,
                    doc_type,
                    gl_account,
                    tax_code,
                    category,
                ),
            ).fetchone()

            if existing is None:
                return {
                    "ok": False,
                    "status": "not_found",
                    "reason": "memory_row_not_found",
                }

            prior_confidence = safe_float(existing["confidence"]) or 0.0
            lowered_confidence = max(0.0, prior_confidence - 0.2)
            now = utc_now_iso()

            conn.execute(
                """
                UPDATE vendor_memory
                SET confidence = ?, updated_at = ?, last_used = ?
                WHERE id = ?
                """,
                (
                    lowered_confidence,
                    now,
                    now,
                    int(existing["id"]),
                ),
            )
            conn.commit()

        return {
            "ok": True,
            "status": "downgraded",
            "vendor": normalize_text(vendor),
            "confidence": lowered_confidence,
        }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Vendor memory store utility")
    subparsers = parser.add_subparsers(dest="command")

    record_parser = subparsers.add_parser("record")
    record_parser.add_argument("--document-id", default="")
    record_parser.add_argument("--client-code", default="")
    record_parser.add_argument("--vendor", required=True)
    record_parser.add_argument("--gl-account", required=True)
    record_parser.add_argument("--tax-code", required=True)
    record_parser.add_argument("--doc-type", default="")
    record_parser.add_argument("--category", default="")
    record_parser.add_argument("--amount", default="")
    record_parser.add_argument("--confidence", default="")
    record_parser.add_argument("--source", default="cli")

    lookup_parser = subparsers.add_parser("lookup")
    lookup_parser.add_argument("--vendor", required=True)
    lookup_parser.add_argument("--client-code", default="")
    lookup_parser.add_argument("--doc-type", default="")

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--limit", type=int, default=50)

    args = parser.parse_args()
    store = VendorMemoryStore()

    if args.command == "record":
        result = store.record_approval(
            document_id=args.document_id,
            client_code=args.client_code,
            vendor=args.vendor,
            gl_account=args.gl_account,
            tax_code=args.tax_code,
            doc_type=args.doc_type,
            category=args.category,
            amount=args.amount,
            confidence=args.confidence,
            source=args.source,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1

    if args.command == "lookup":
        result = store.get_best_match(
            vendor=args.vendor,
            client_code=args.client_code,
            doc_type=args.doc_type,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result else 1

    if args.command == "list":
        result = store.list_vendor_memory(limit=max(1, int(args.limit)))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    parser.print_help()
    return 1


# ---------------------------------------------------------------------------
# Module-level convenience functions for pipeline integration
# ---------------------------------------------------------------------------

_default_store: VendorMemoryStore | None = None


def _get_store() -> VendorMemoryStore:
    global _default_store
    if _default_store is None:
        _default_store = VendorMemoryStore()
    return _default_store


def record_vendor_approval(
    client_code: str = "",
    vendor_name: str = "",
    gl_account: str = "",
    tax_code: str = "",
    amount: float | None = None,
    conn: Any = None,
    **kwargs,
) -> dict[str, Any]:
    """Record an approval — feeds vendor memory with the approved fields."""
    return _get_store().record_approval(
        client_code=client_code,
        vendor=vendor_name,
        gl_account=gl_account,
        tax_code=tax_code,
        amount=amount,
        source="human_approval",
    )


def record_vendor_correction(
    client_code: str = "",
    vendor_name: str = "",
    field: str = "",
    old_value: str = "",
    new_value: str = "",
    conn: Any = None,
    **kwargs,
) -> dict[str, Any]:
    """Record a field correction — lowers confidence for old pattern."""
    store = _get_store()
    # Reject the old pattern
    if old_value:
        store.record_rejection(
            vendor=vendor_name,
            client_code=client_code,
            gl_account=old_value if field == "gl_account" else "",
            tax_code=old_value if field == "tax_code" else "",
            category=old_value if field == "category" else "",
        )
    return {"ok": True, "field": field, "old": old_value, "new": new_value}


def record_posting(
    client_code: str = "",
    vendor_name: str = "",
    gl_account: str = "",
    tax_code: str = "",
    amount: float | None = None,
    conn: Any = None,
    **kwargs,
) -> dict[str, Any]:
    """Record a posting — highest confidence signal, boosts vendor memory."""
    return _get_store().record_approval(
        client_code=client_code,
        vendor=vendor_name,
        gl_account=gl_account,
        tax_code=tax_code,
        amount=amount,
        confidence=0.90,
        source="posting",
    )


if __name__ == "__main__":
    raise SystemExit(main())