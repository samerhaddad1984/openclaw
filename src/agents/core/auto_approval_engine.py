from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
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
    return " ".join(normalize_text(value).casefold().split())


def normalize_amount(value: Any) -> float | None:
    text = normalize_text(value)
    if not text:
        return None

    cleaned = (
        text.replace(",", "")
        .replace("$", "")
        .replace("CAD", "")
        .replace("USD", "")
        .replace("EUR", "")
        .strip()
    )

    try:
        return round(float(cleaned), 2)
    except Exception:
        return None


def normalize_confidence(value: Any) -> float | None:
    number = normalize_amount(value)
    if number is None:
        return None

    if number > 1.0 and number <= 100.0:
        number = number / 100.0

    if number < 0:
        number = 0.0
    if number > 1:
        number = 1.0

    return round(float(number), 4)


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
    names: set[str] = set()

    for row in rows:
        try:
            names.add(str(row["name"]))
        except Exception:
            try:
                names.add(str(row[1]))
            except Exception:
                pass

    return names


@dataclass
class AutoApprovalResult:
    document_id: str
    decision: str
    approval_score: float
    auto_approved: bool
    vendor_memory_ok: bool
    document_confidence_ok: bool
    amount_suspicious: bool
    duplicate_risk_level: str
    reason: str
    evaluated_at: str
    vendor_memory_confidence: float = 0.0
    learned_pattern_used: bool = False
    learned_pattern_confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AutoApprovalEngine:
    def __init__(
        self,
        *,
        db_path: Path = DB_PATH,
        min_document_confidence: float = 0.90,
        suspicious_zero_amount: bool = True,
    ) -> None:
        self.db_path = Path(db_path)
        self.min_document_confidence = float(min_document_confidence)
        self.suspicious_zero_amount = bool(suspicious_zero_amount)

    def _open(self) -> sqlite3.Connection:
        return open_db(self.db_path)

    def _load_document_if_needed(self, document: Any) -> dict[str, Any]:
        if isinstance(document, dict):
            return dict(document)

        if isinstance(document, sqlite3.Row):
            return dict(document)

        document_id = normalize_text(document)
        if not document_id:
            return {}

        with self._open() as conn:
            if not table_exists(conn, "documents"):
                return {}

            row = conn.execute(
                "SELECT * FROM documents WHERE document_id = ? LIMIT 1",
                (document_id,),
            ).fetchone()

        return dict(row) if row is not None else {}

    def _document_confidence(self, document: dict[str, Any]) -> tuple[bool, float]:
        confidence = normalize_confidence(document.get("confidence"))

        if confidence is None:
            raw_result = safe_json_loads(document.get("raw_result"))
            raw_rules_output = safe_json_loads(raw_result.get("raw_rules_output"))
            confidence = normalize_confidence(
                raw_result.get("effective_confidence")
                or raw_result.get("confidence")
                or raw_rules_output.get("confidence")
            )

        if confidence is None:
            confidence = 0.0

        return confidence >= self.min_document_confidence, float(confidence)

    def _amount_suspicious(self, document: dict[str, Any]) -> bool:
        amount = normalize_amount(document.get("amount"))

        if amount is None:
            raw_result = safe_json_loads(document.get("raw_result"))
            amount = normalize_amount(raw_result.get("amount"))

        if amount is None:
            return True

        if self.suspicious_zero_amount and abs(amount) < 0.00001:
            return True

        return False

    def _find_vendor_memory_match(self, document: dict[str, Any]) -> tuple[bool, float]:
        vendor = normalize_text(document.get("vendor"))
        client_code = normalize_text(document.get("client_code"))
        gl_account = normalize_text(document.get("gl_account"))
        tax_code = normalize_text(document.get("tax_code"))
        doc_type = normalize_text(document.get("doc_type"))
        category = normalize_text(document.get("category"))

        if not vendor:
            return False, 0.0

        with self._open() as conn:
            if not table_exists(conn, "vendor_memory"):
                return False, 0.0

            columns = get_table_columns(conn, "vendor_memory")

            select_cols = [
                "vendor",
                "client_code",
                "gl_account",
                "tax_code",
                "doc_type",
                "category",
            ]

            if "confidence" in columns:
                select_cols.append("confidence")
            else:
                select_cols.append("0.0 AS confidence")

            rows = conn.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM vendor_memory
                WHERE LOWER(COALESCE(vendor,'')) = LOWER(?)
                """,
                (vendor,),
            ).fetchall()

        best_score = 0.0
        doc_vendor = normalize_key(vendor)
        doc_client = normalize_key(client_code)
        doc_gl = normalize_key(gl_account)
        doc_tax = normalize_key(tax_code)
        doc_doc_type = normalize_key(doc_type)
        doc_category = normalize_key(category)

        for row in rows:
            score = 0.0

            if normalize_key(row["vendor"]) == doc_vendor and doc_vendor:
                score += 0.45

            row_client = normalize_key(row["client_code"])
            if doc_client and row_client and row_client == doc_client:
                score += 0.20
            elif not row_client or not doc_client:
                score += 0.05

            row_gl = normalize_key(row["gl_account"])
            if doc_gl and row_gl and row_gl == doc_gl:
                score += 0.15

            row_tax = normalize_key(row["tax_code"])
            if doc_tax and row_tax and row_tax == doc_tax:
                score += 0.08

            row_doc_type = normalize_key(row["doc_type"])
            if doc_doc_type and row_doc_type and row_doc_type == doc_doc_type:
                score += 0.07

            row_category = normalize_key(row["category"])
            if doc_category and row_category and row_category == doc_category:
                score += 0.05

            memory_conf = normalize_confidence(row["confidence"]) or 0.0
            score = max(score, memory_conf)

            if score > best_score:
                best_score = score

        best_score = round(best_score, 2)
        return best_score >= 0.60, best_score

    def _find_learning_pattern_signal(self, document: dict[str, Any]) -> tuple[bool, float]:
        vendor = normalize_text(document.get("vendor"))
        client_code = normalize_text(document.get("client_code"))
        doc_type = normalize_text(document.get("doc_type"))
        category = normalize_text(document.get("category"))
        gl_account = normalize_text(document.get("gl_account"))
        tax_code = normalize_text(document.get("tax_code"))

        with self._open() as conn:
            if not table_exists(conn, "learning_memory_patterns"):
                return False, 0.0

            rows = conn.execute(
                """
                SELECT
                    event_type,
                    vendor,
                    client_code,
                    doc_type,
                    category,
                    gl_account,
                    tax_code,
                    outcome_count,
                    success_count,
                    posted_count,
                    review_count,
                    avg_confidence
                FROM learning_memory_patterns
                """
            ).fetchall()

        best_score = 0.0
        used = False

        doc_vendor = normalize_key(vendor)
        doc_client = normalize_key(client_code)
        doc_doc_type = normalize_key(doc_type)
        doc_category = normalize_key(category)
        doc_gl = normalize_key(gl_account)
        doc_tax = normalize_key(tax_code)

        for row in rows:
            score = 0.0

            row_vendor = normalize_key(row["vendor"])
            row_client = normalize_key(row["client_code"])
            row_doc_type = normalize_key(row["doc_type"])
            row_category = normalize_key(row["category"])
            row_gl = normalize_key(row["gl_account"])
            row_tax = normalize_key(row["tax_code"])

            if row_vendor and doc_vendor and row_vendor == doc_vendor:
                score += 0.35
            elif not row_vendor and not doc_vendor:
                score += 0.05

            if row_client and doc_client and row_client == doc_client:
                score += 0.20
            elif not row_client or not doc_client:
                score += 0.03

            if row_doc_type and doc_doc_type and row_doc_type == doc_doc_type:
                score += 0.10

            if row_category and doc_category and row_category == doc_category:
                score += 0.10

            if row_gl and doc_gl and row_gl == doc_gl:
                score += 0.15

            if row_tax and doc_tax and row_tax == doc_tax:
                score += 0.10

            outcome_count = int(row["outcome_count"] or 0)
            success_count = int(row["success_count"] or 0)
            posted_count = int(row["posted_count"] or 0)
            review_count = int(row["review_count"] or 0)
            avg_conf = normalize_confidence(row["avg_confidence"]) or 0.0

            if outcome_count <= 0:
                continue

            success_rate = (success_count + posted_count) / max(1, outcome_count)
            review_rate = review_count / max(1, outcome_count)

            event_type = normalize_key(row["event_type"])

            if event_type in {"approved_ready_to_post", "approved_but_held", "posted_successfully"}:
                score += success_rate * 0.35
            elif event_type == "held_for_review":
                score -= review_rate * 0.60

            if outcome_count >= 3:
                score += 0.10
            elif outcome_count == 2:
                score += 0.05

            score += avg_conf * 0.10
            score = round(score, 4)

            if score > best_score:
                best_score = score
                used = score >= 0.70

        return used, round(best_score, 2)

    def evaluate_document(
        self,
        document: Any = None,
        *,
        document_id: str | None = None,
        duplicate_result: dict[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        try:
            source = document if document is not None else document_id
            document_dict = self._load_document_if_needed(source)
            resolved_document_id = normalize_text(document_dict.get("document_id")) or normalize_text(document_id)

            if not document_dict:
                return AutoApprovalResult(
                    document_id=resolved_document_id,
                    decision="needs_review",
                    approval_score=0.0,
                    auto_approved=False,
                    vendor_memory_ok=False,
                    document_confidence_ok=False,
                    amount_suspicious=True,
                    duplicate_risk_level=normalize_text((duplicate_result or {}).get("risk_level")) or "none",
                    reason="document_not_found",
                    evaluated_at=utc_now_iso(),
                    vendor_memory_confidence=0.0,
                    learned_pattern_used=False,
                    learned_pattern_confidence=0.0,
                ).to_dict()

            duplicate_result = duplicate_result or {}
            duplicate_risk_level = normalize_key(duplicate_result.get("risk_level")) or "none"

            vendor_memory_ok, vendor_memory_confidence = self._find_vendor_memory_match(document_dict)
            learned_pattern_used, learned_pattern_confidence = self._find_learning_pattern_signal(document_dict)
            document_confidence_ok, confidence_value = self._document_confidence(document_dict)
            amount_suspicious = self._amount_suspicious(document_dict)

            approval_score = 0.0

            if vendor_memory_ok:
                approval_score += 0.45
            if document_confidence_ok:
                approval_score += 0.25
            if not amount_suspicious:
                approval_score += 0.15
            if learned_pattern_used:
                approval_score += 0.15

            approval_score = round(min(approval_score, 1.0), 2)

            auto_approved = False
            decision = "needs_review"
            reason = "needs_review"

            if duplicate_risk_level == "high":
                auto_approved = False
                decision = "needs_review"
                reason = "duplicate_high_risk"

            elif duplicate_risk_level == "medium":
                if vendor_memory_ok and document_confidence_ok and not amount_suspicious and learned_pattern_used:
                    auto_approved = True
                    decision = "approve_but_hold"
                    reason = "approve_but_hold"
                else:
                    auto_approved = False
                    decision = "needs_review"
                    reason = "duplicate_medium_risk"

            else:
                if vendor_memory_ok and document_confidence_ok and not amount_suspicious:
                    auto_approved = True
                    decision = "auto_post"
                    reason = "auto_post"
                elif learned_pattern_used and document_confidence_ok and not amount_suspicious:
                    auto_approved = True
                    decision = "approve_but_hold"
                    reason = "approve_but_hold"
                else:
                    auto_approved = False
                    decision = "needs_review"
                    reason = "needs_review"

            result = AutoApprovalResult(
                document_id=resolved_document_id,
                decision=decision,
                approval_score=approval_score,
                auto_approved=auto_approved,
                vendor_memory_ok=vendor_memory_ok,
                document_confidence_ok=document_confidence_ok,
                amount_suspicious=amount_suspicious,
                duplicate_risk_level=duplicate_risk_level,
                reason=reason,
                evaluated_at=utc_now_iso(),
                vendor_memory_confidence=vendor_memory_confidence,
                learned_pattern_used=learned_pattern_used,
                learned_pattern_confidence=learned_pattern_confidence,
            ).to_dict()

            result["confidence"] = round(confidence_value, 4)
            return result

        except Exception as exc:
            fallback_document = self._load_document_if_needed(document if document is not None else document_id)
            resolved_document_id = normalize_text(fallback_document.get("document_id")) or normalize_text(document_id)

            return AutoApprovalResult(
                document_id=resolved_document_id,
                decision="needs_review",
                approval_score=0.0,
                auto_approved=False,
                vendor_memory_ok=False,
                document_confidence_ok=False,
                amount_suspicious=True,
                duplicate_risk_level=normalize_key((duplicate_result or {}).get("risk_level")) or "none",
                reason=f"auto_approval_engine_failed: {exc}",
                evaluated_at=utc_now_iso(),
                vendor_memory_confidence=0.0,
                learned_pattern_used=False,
                learned_pattern_confidence=0.0,
            ).to_dict()

    def evaluate(self, document: Any = None, duplicate_result: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.evaluate_document(document=document, duplicate_result=duplicate_result, **kwargs)

    def run(self, document: Any = None, duplicate_result: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.evaluate_document(document=document, duplicate_result=duplicate_result, **kwargs)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA auto approval engine")
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--duplicate-risk", default="none")
    args = parser.parse_args()

    engine = AutoApprovalEngine()
    result = engine.evaluate_document(
        document_id=args.document_id,
        duplicate_result={"risk_level": args.duplicate_risk},
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
