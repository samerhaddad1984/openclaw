from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, asdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


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


def ratio(a: Any, b: Any) -> float:
    left = normalize_key(a)
    right = normalize_key(b)

    if not left or not right:
        return 0.0

    if left == right:
        return 1.0

    return SequenceMatcher(None, left, right).ratio()


def amount_close(a: float | None, b: float | None, tolerance: float = 0.01) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= tolerance


def amount_near_percent(a: float | None, b: float | None, percent: float = 0.02) -> bool:
    if a is None or b is None:
        return False
    base = max(abs(a), abs(b), 1.0)
    return abs(a - b) <= base * percent


def same_date(a: Any, b: Any) -> bool:
    left = normalize_text(a)
    right = normalize_text(b)
    return bool(left and right and left == right)


def same_month(a: Any, b: Any) -> bool:
    left = normalize_text(a)
    right = normalize_text(b)

    if len(left) >= 7 and len(right) >= 7:
        return left[:7] == right[:7]

    return False


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
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {normalize_text(row["name"]) for row in rows}


@dataclass
class DuplicateCandidate:
    source: str
    match_strength: str
    document_id: str
    posting_id: str | None
    vendor: str
    client_code: str
    document_date: str
    amount: float | None
    file_name: str
    invoice_hint: str
    posting_status: str
    approval_state: str
    score: float
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DuplicateGuardResult:
    duplicate_suspected: bool
    duplicate_confirmed: bool
    risk_level: str
    score: float
    action: str
    reasons: list[str]
    candidates: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DuplicateGuard:
    def __init__(
        self,
        *,
        db_path: Path = DB_PATH,
        amount_tolerance: float = 0.01,
    ):
        self.db_path = db_path
        self.amount_tolerance = amount_tolerance

    def _open(self) -> sqlite3.Connection:
        return open_db(self.db_path)

    def _extract_invoice_hint(self, file_name: Any, raw_result: Any) -> str:
        file_name_text = normalize_key(file_name)
        raw = safe_json_loads(raw_result)

        candidates = [
            raw.get("invoice_number"),
            raw.get("invoice_no"),
            raw.get("document_number"),
            raw.get("receipt_number"),
            raw.get("invoice_id"),
        ]

        for item in candidates:
            text = normalize_key(item)
            if text:
                return text

        if file_name_text:
            base = file_name_text.rsplit(".", 1)[0]
            return base

        return ""

    def _load_document_context(self, document_id: str) -> dict[str, Any]:
        with self._open() as conn:
            doc_columns = get_table_columns(conn, "documents")

            select_parts = [
                "document_id",
                "file_name",
                "client_code",
                "vendor",
                "amount",
                "document_date",
                "raw_result",
            ]

            if "memo" in doc_columns:
                select_parts.append("memo")
            else:
                select_parts.append("'' AS memo")

            if "doc_type" in doc_columns:
                select_parts.append("doc_type")
            else:
                select_parts.append("'' AS doc_type")

            if "posting_status" in doc_columns:
                select_parts.append("posting_status")
            else:
                select_parts.append("'' AS posting_status")

            if "approval_state" in doc_columns:
                select_parts.append("approval_state")
            else:
                select_parts.append("'' AS approval_state")

            query = f"""
                SELECT
                    {", ".join(select_parts)}
                FROM documents
                WHERE document_id = ?
            """

            row = conn.execute(query, (document_id,)).fetchone()

        if row is None:
            raise ValueError(f"Document not found: {document_id}")

        return {
            "document_id": normalize_text(row["document_id"]),
            "file_name": normalize_text(row["file_name"]),
            "client_code": normalize_text(row["client_code"]),
            "vendor": normalize_text(row["vendor"]),
            "amount": normalize_amount(row["amount"]),
            "document_date": normalize_text(row["document_date"]),
            "invoice_hint": self._extract_invoice_hint(row["file_name"], row["raw_result"]),
            "memo": normalize_text(row["memo"]),
            "doc_type": normalize_text(row["doc_type"]),
            "posting_status": normalize_text(row["posting_status"]),
            "approval_state": normalize_text(row["approval_state"]),
        }

    def _candidate_rows(self, target: dict[str, Any], limit: int = 250) -> list[sqlite3.Row]:
        with self._open() as conn:
            has_posting_jobs = table_exists(conn, "posting_jobs")
            doc_columns = get_table_columns(conn, "documents")

            target_document_id = target["document_id"]
            target_client_code = target["client_code"]
            target_vendor = target["vendor"]

            where_clauses = ["COALESCE(d.document_id,'') != ?"]
            params: list[Any] = [target_document_id]

            narrowed = False

            if target_client_code:
                where_clauses.append(
                    "("
                    "LOWER(COALESCE(d.client_code,'')) = LOWER(?) "
                    "OR LOWER(COALESCE(d.client_code,'')) LIKE LOWER(?) "
                    "OR LOWER(COALESCE(d.client_code,'')) LIKE LOWER(?)"
                    ")"
                )
                params.extend([target_client_code, f"%{target_client_code}%", f"{target_client_code}%"])
                narrowed = True

            if target_vendor:
                where_clauses.append(
                    "("
                    "LOWER(COALESCE(d.vendor,'')) = LOWER(?) "
                    "OR LOWER(COALESCE(d.vendor,'')) LIKE LOWER(?) "
                    "OR LOWER(COALESCE(d.vendor,'')) LIKE LOWER(?)"
                    ")"
                )
                params.extend([target_vendor, f"%{target_vendor}%", f"{target_vendor}%"])
                narrowed = True

            if not narrowed:
                where_clauses.append("1=1")

            doc_select_parts = [
                "d.document_id",
                "d.file_name",
                "d.client_code",
                "d.vendor",
                "d.amount",
                "d.document_date",
                "d.raw_result",
            ]

            if "memo" in doc_columns:
                doc_select_parts.append("d.memo")
            else:
                doc_select_parts.append("'' AS memo")

            if "doc_type" in doc_columns:
                doc_select_parts.append("d.doc_type")
            else:
                doc_select_parts.append("'' AS doc_type")

            if "posting_status" in doc_columns:
                doc_select_parts.append("COALESCE(d.posting_status,'') AS document_posting_status")
            else:
                doc_select_parts.append("'' AS document_posting_status")

            if "approval_state" in doc_columns:
                doc_select_parts.append("COALESCE(d.approval_state,'') AS document_approval_state")
            else:
                doc_select_parts.append("'' AS document_approval_state")

            where_sql = " AND ".join(where_clauses)

            if has_posting_jobs:
                query = f"""
                    SELECT
                        {", ".join(doc_select_parts)},
                        COALESCE(p.posting_id,'') AS posting_id,
                        COALESCE(p.posting_status,'') AS posting_status,
                        COALESCE(p.approval_state,'') AS approval_state
                    FROM documents d
                    LEFT JOIN posting_jobs p
                        ON p.document_id = d.document_id
                    WHERE {where_sql}
                    ORDER BY COALESCE(d.document_date,'') DESC, COALESCE(d.document_id,'') ASC
                    LIMIT ?
                """
            else:
                query = f"""
                    SELECT
                        {", ".join(doc_select_parts)},
                        '' AS posting_id,
                        '' AS posting_status,
                        '' AS approval_state
                    FROM documents d
                    WHERE {where_sql}
                    ORDER BY COALESCE(d.document_date,'') DESC, COALESCE(d.document_id,'') ASC
                    LIMIT ?
                """

            params.append(max(1, int(limit)))
            rows = conn.execute(query, params).fetchall()

        return list(rows)

    def _exact_duplicate_evidence(
        self,
        *,
        target: dict[str, Any],
        candidate_amount: float | None,
        candidate_date: str,
        invoice_hint: str,
        file_name: str,
    ) -> tuple[bool, list[str]]:
        evidence: list[str] = []

        target_invoice_hint = normalize_key(target.get("invoice_hint"))
        candidate_invoice_hint = normalize_key(invoice_hint)
        target_file_name = normalize_key(target.get("file_name"))
        candidate_file_name = normalize_key(file_name)

        same_amount_exact = amount_close(target.get("amount"), candidate_amount, tolerance=self.amount_tolerance)
        same_doc_date_exact = same_date(target.get("document_date"), candidate_date)
        same_invoice_hint_exact = bool(target_invoice_hint and candidate_invoice_hint and target_invoice_hint == candidate_invoice_hint)
        same_file_name_exact = bool(target_file_name and candidate_file_name and target_file_name == candidate_file_name)

        if same_invoice_hint_exact:
            evidence.append("exact_invoice_hint_match")
        if same_file_name_exact:
            evidence.append("exact_file_name_match")
        if same_amount_exact:
            evidence.append("exact_amount_match")
        if same_doc_date_exact:
            evidence.append("exact_document_date_match")

        confirmed = False

        if same_invoice_hint_exact and same_amount_exact and same_doc_date_exact:
            confirmed = True
        elif same_file_name_exact and same_amount_exact and same_doc_date_exact:
            confirmed = True
        elif same_invoice_hint_exact and same_amount_exact:
            confirmed = True

        return confirmed, evidence

    def _score_candidate(self, target: dict[str, Any], row: sqlite3.Row) -> DuplicateCandidate | None:
        candidate_amount = normalize_amount(row["amount"])
        target_amount = target["amount"]

        candidate_date = normalize_text(row["document_date"])
        target_date = normalize_text(target["document_date"])

        file_name = normalize_text(row["file_name"])
        raw_result = row["raw_result"]
        doc_type = normalize_text(row["doc_type"])
        invoice_hint = self._extract_invoice_hint(row["file_name"], row["raw_result"])

        reasons: list[str] = []
        score = 0.0

        target_client_key = normalize_key(target["client_code"])
        target_vendor_key = normalize_key(target["vendor"])
        row_client_key = normalize_key(row["client_code"])
        row_vendor_key = normalize_key(row["vendor"])

        if not target_vendor_key:
            return None

        vendor_similarity = ratio(target["vendor"], row["vendor"])
        client_similarity = ratio(target["client_code"], row["client_code"])
        invoice_similarity = ratio(target["invoice_hint"], invoice_hint)
        file_similarity = ratio(target["file_name"], file_name)

        if target_client_key and row_client_key:
            if target_client_key == row_client_key:
                reasons.append("same_client_exact")
                score += 0.12
            elif client_similarity >= 0.92:
                reasons.append("same_client_fuzzy")
                score += 0.06
            else:
                return None

        if target_vendor_key == row_vendor_key:
            reasons.append("same_vendor_exact")
            score += 0.18
        elif vendor_similarity >= 0.94:
            reasons.append("same_vendor_fuzzy")
            score += 0.10
        else:
            return None

        if normalize_key(target.get("doc_type")) and normalize_key(target.get("doc_type")) == normalize_key(doc_type):
            reasons.append("same_doc_type")
            score += 0.04

        same_amount_exact = False
        if target_amount is not None and candidate_amount is not None:
            if amount_close(target_amount, candidate_amount, tolerance=self.amount_tolerance):
                same_amount_exact = True
                reasons.append("same_amount")
                score += 0.22
            elif amount_near_percent(target_amount, candidate_amount, percent=0.005):
                reasons.append("near_amount")
                score += 0.06

        same_doc_date_exact = False
        if target_date and candidate_date:
            if same_date(target_date, candidate_date):
                same_doc_date_exact = True
                reasons.append("same_document_date")
                score += 0.16
            elif same_month(target_date, candidate_date):
                reasons.append("same_month")
                score += 0.02

        same_invoice_hint_exact = False
        if target["invoice_hint"] and invoice_hint:
            if normalize_key(target["invoice_hint"]) == normalize_key(invoice_hint):
                same_invoice_hint_exact = True
                reasons.append("same_invoice_hint")
                score += 0.28
            elif invoice_similarity >= 0.97:
                reasons.append("very_close_invoice_hint")
                score += 0.08
            elif invoice_similarity >= 0.92:
                reasons.append("similar_invoice_hint")
                score += 0.03
        elif target["file_name"] and file_name:
            if file_similarity >= 0.99:
                reasons.append("same_file_name_pattern")
                score += 0.12
            elif file_similarity >= 0.96:
                reasons.append("very_close_file_name")
                score += 0.04

        posting_status = normalize_text(row["posting_status"]) or normalize_text(row["document_posting_status"])
        approval_state = normalize_text(row["approval_state"]) or normalize_text(row["document_approval_state"])

        if normalize_key(posting_status) == "posted":
            reasons.append("already_posted_candidate")
            score += 0.03
        elif normalize_key(posting_status) in {"ready_to_post", "draft"}:
            reasons.append("active_posting_candidate")
            score += 0.01

        if normalize_key(approval_state) == "approved_for_posting":
            reasons.append("already_approved_candidate")
            score += 0.01

        confirmed_evidence, evidence_reasons = self._exact_duplicate_evidence(
            target=target,
            candidate_amount=candidate_amount,
            candidate_date=candidate_date,
            invoice_hint=invoice_hint,
            file_name=file_name,
        )
        reasons.extend(evidence_reasons)

        score = round(min(score, 1.0), 2)

        if score < 0.20 and not confirmed_evidence:
            return None

        if confirmed_evidence:
            match_strength = "strong"
        elif score >= 0.70:
            match_strength = "medium"
        else:
            match_strength = "weak"

        return DuplicateCandidate(
            source="documents/posting_jobs",
            match_strength=match_strength,
            document_id=normalize_text(row["document_id"]),
            posting_id=normalize_text(row["posting_id"]) or None,
            vendor=normalize_text(row["vendor"]),
            client_code=normalize_text(row["client_code"]),
            document_date=candidate_date,
            amount=candidate_amount,
            file_name=file_name,
            invoice_hint=invoice_hint,
            posting_status=posting_status,
            approval_state=approval_state,
            score=1.0 if confirmed_evidence else score,
            reasons=list(dict.fromkeys(reasons)),
        )

    def evaluate_document(
        self,
        *,
        document_id: str,
        limit: int = 10,
    ) -> dict[str, Any]:
        target = self._load_document_context(document_id)
        rows = self._candidate_rows(target, limit=250)

        candidates: list[DuplicateCandidate] = []

        for row in rows:
            candidate = self._score_candidate(target, row)
            if candidate is not None:
                candidates.append(candidate)

        candidates.sort(key=lambda x: (-x.score, x.document_id))
        candidates = candidates[: max(1, int(limit))]

        duplicate_confirmed = False
        duplicate_suspected = False
        risk_level = "none"
        action = "allow"
        reasons: list[str] = []

        if candidates:
            top = candidates[0]

            top_reasons = set(top.reasons)

            exact_duplicate_proof = (
                "exact_invoice_hint_match" in top_reasons and "exact_amount_match" in top_reasons and "exact_document_date_match" in top_reasons
            ) or (
                "exact_file_name_match" in top_reasons and "exact_amount_match" in top_reasons and "exact_document_date_match" in top_reasons
            ) or (
                "exact_invoice_hint_match" in top_reasons and "exact_amount_match" in top_reasons
            )

            if exact_duplicate_proof:
                duplicate_confirmed = True
                duplicate_suspected = True
                risk_level = "high"
                action = "block"
                reasons.append("high_confidence_duplicate")

            elif top.score >= 0.72:
                duplicate_suspected = True
                risk_level = "medium"
                action = "review"
                reasons.append("possible_duplicate")

            elif top.score >= 0.48:
                duplicate_suspected = True
                risk_level = "low"
                action = "review"
                reasons.append("weak_duplicate_signal")

            reasons.extend(top.reasons)

        return DuplicateGuardResult(
            duplicate_suspected=duplicate_suspected,
            duplicate_confirmed=duplicate_confirmed,
            risk_level=risk_level,
            score=round(candidates[0].score, 2) if candidates else 0.0,
            action=action,
            reasons=list(dict.fromkeys(reasons)),
            candidates=[c.to_dict() for c in candidates[:10]],
        ).to_dict()

    def detect_duplicates(self, *, document_id: str, limit: int = 10) -> dict[str, Any]:
        return self.evaluate_document(document_id=document_id, limit=limit)

    def run_duplicate_check(self, *, document_id: str, limit: int = 10) -> dict[str, Any]:
        return self.evaluate_document(document_id=document_id, limit=limit)

    def check_duplicates(self, *, document_id: str, limit: int = 10) -> dict[str, Any]:
        return self.evaluate_document(document_id=document_id, limit=limit)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA duplicate guard")
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    guard = DuplicateGuard()
    result = guard.evaluate_document(
        document_id=args.document_id,
        limit=max(1, int(args.limit)),
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())