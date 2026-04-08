from __future__ import annotations

import json
import re as _re
import sqlite3
import statistics
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
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
    text = " ".join(normalize_text(value).lower().split())
    # FIX: Mixed-script attack prevention
    from src.agents.tools.fingerprint_utils import detect_mixed_scripts, normalize_confusables
    if not detect_mixed_scripts(text)["is_mixed"]:
        text = normalize_confusables(text)
    # FIX 4: Strip accents so "Hydro-Québec" == "Hydro-Quebec"
    text = unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")
    # FIX P1-2: Normalize hyphens to spaces for Quebec vendor name variants
    text = text.replace("-", " ")
    # FIX P1-2: Remove common Quebec/Canadian business suffixes
    text = _re.sub(
        r"\b(inc|ltee|ltée|limitee|limitée|limited|ltd|enr|corp|corporation|"
        r"s\.?e\.?n\.?c\.?r?\.?l?\.?|s\.?a\.?s?\.?|s\.?e\.?c\.?)\b",
        "", text,
    )
    # Remove leftover punctuation and collapse whitespace
    text = _re.sub(r"[^a-z0-9\s]", " ", text)
    text = _re.sub(r"\s+", " ", text).strip()
    return text


def normalize_amount(value: Any) -> float | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return round(float(text.replace(",", "")), 2)
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


# ---- Vendor-name ↔ category semantic cross-check ----
# Maps vendor-name keywords to incompatible category keywords.
# If the vendor name contains a key and the suggested category contains
# any of the disallowed tokens, the suggestion is rejected.
_VENDOR_CATEGORY_CONFLICTS: dict[str, set[str]] = {
    "assurance": {"office", "supplies", "software", "telecom", "travel", "meals"},
    "insurance": {"office", "supplies", "software", "telecom", "travel", "meals"},
    "telecom": {"office", "supplies", "insurance", "assurance", "meals"},
    "hydro": {"office", "supplies", "telecom", "insurance", "travel"},
    "pharmaci": {"office", "supplies", "software", "telecom"},
    "restaurant": {"office", "supplies", "software", "telecom", "insurance"},
}


def _vendor_category_conflict(vendor: str, category: str) -> bool:
    """Return True if vendor name strongly conflicts with suggested category."""
    vendor_lower = normalize_key(vendor)
    category_lower = category.lower()
    for keyword, bad_tokens in _VENDOR_CATEGORY_CONFLICTS.items():
        if keyword in vendor_lower:
            if any(tok in category_lower for tok in bad_tokens):
                return True
    return False


# 24-month cutoff for document time-decay filtering
_TIME_DECAY_MONTHS = 24


def existing_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] if isinstance(row, tuple) else row["name"] for row in cols}


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


@dataclass
class VendorFieldSuggestion:
    field_name: str
    suggested_value: str
    support: int
    confidence: float
    source: str
    context_key: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class VendorAmountInsight:
    vendor: str
    context_key: str
    sample_count: int
    median_amount: float | None
    min_amount: float | None
    max_amount: float | None
    suspicious: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class VendorMemoryEngine:
    """
    Conservative vendor memory engine.

    It learns from:
    1. learning_memory corrections
    2. approved/posted document outcomes already stored in documents/posting_jobs

    It suggests:
    - gl_account
    - tax_code
    - category
    - client_code

    It also provides simple amount anomaly detection so repeated vendors
    do not drift silently into garbage.
    """

    TRACKED_FIELDS = ("client_code", "gl_account", "tax_code", "category")
    DOC_CONTEXT_ORDER = (
        "client_vendor_doc_type",
        "vendor_doc_type",
        "vendor_only",
    )

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def _open(self) -> sqlite3.Connection:
        return open_db(self.db_path)

    def _collect_learning_rows(self) -> list[dict[str, Any]]:
        rows_out: list[dict[str, Any]] = []

        with self._open() as conn:
            if not table_exists(conn, "learning_memory"):
                return rows_out

            rows = conn.execute(
                """
                SELECT
                    vendor,
                    client_code,
                    doc_type,
                    field_name,
                    new_value
                FROM learning_memory
                WHERE field_name IN ('client_code', 'gl_account', 'tax_code', 'category')
                ORDER BY id ASC
                """
            ).fetchall()

        for row in rows:
            vendor = normalize_text(row["vendor"])
            field_name = normalize_text(row["field_name"])
            new_value = normalize_text(row["new_value"])

            if not vendor or not field_name or not new_value:
                continue

            rows_out.append(
                {
                    "vendor": vendor,
                    "client_code": normalize_text(row["client_code"]),
                    "doc_type": normalize_text(row["doc_type"]),
                    "field_name": field_name,
                    "value": new_value,
                    "source": "learning_memory",
                }
            )

        return rows_out

    def _collect_document_rows(self) -> list[dict[str, Any]]:
        rows_out: list[dict[str, Any]] = []

        with self._open() as conn:
            if not table_exists(conn, "documents"):
                return rows_out

            has_posting_jobs = table_exists(conn, "posting_jobs")

            # FIX: 24-month time-decay filter when created_at column exists
            has_created_at = "created_at" in existing_columns(conn, "documents")
            cutoff_iso = ""
            if has_created_at:
                cutoff = datetime.now(timezone.utc) - timedelta(days=_TIME_DECAY_MONTHS * 30)
                cutoff_iso = cutoff.isoformat()

            if has_posting_jobs:
                date_clause = (
                    "AND (d.created_at >= ? OR COALESCE(d.created_at, '') = '')"
                    if has_created_at else ""
                )
                params: list[str] = [cutoff_iso] if has_created_at else []
                rows = conn.execute(
                    f"""
                    SELECT
                        d.vendor,
                        d.client_code,
                        d.doc_type,
                        d.gl_account,
                        d.tax_code,
                        d.category,
                        d.amount,
                        d.review_status,
                        pj.posting_status,
                        pj.approval_state
                    FROM documents d
                    LEFT JOIN posting_jobs pj
                        ON pj.document_id = d.document_id
                    WHERE (COALESCE(d.review_status, '') IN ('Ready')
                       OR COALESCE(pj.approval_state, '') = 'approved_for_posting'
                       OR COALESCE(pj.posting_status, '') = 'posted')
                    {date_clause}
                    """,
                    params,
                ).fetchall()
            else:
                date_clause = (
                    "AND (created_at >= ? OR COALESCE(created_at, '') = '')"
                    if has_created_at else ""
                )
                params = [cutoff_iso] if has_created_at else []
                rows = conn.execute(
                    f"""
                    SELECT
                        vendor,
                        client_code,
                        doc_type,
                        gl_account,
                        tax_code,
                        category,
                        amount,
                        review_status
                    FROM documents
                    WHERE COALESCE(review_status, '') = 'Ready'
                    {date_clause}
                    """,
                    params,
                ).fetchall()

        for row in rows:
            vendor = normalize_text(row["vendor"])
            if not vendor:
                continue

            base = {
                "vendor": vendor,
                "client_code": normalize_text(row["client_code"]),
                "doc_type": normalize_text(row["doc_type"]),
                "source": "approved_documents",
            }

            for field_name in self.TRACKED_FIELDS:
                value = normalize_text(row[field_name]) if field_name in row.keys() else ""
                if value:
                    rows_out.append(
                        {
                            **base,
                            "field_name": field_name,
                            "value": value,
                        }
                    )

        return rows_out

    def _collect_amount_rows(self) -> list[dict[str, Any]]:
        rows_out: list[dict[str, Any]] = []

        with self._open() as conn:
            if not table_exists(conn, "documents"):
                return rows_out

            rows = conn.execute(
                """
                SELECT
                    vendor,
                    client_code,
                    doc_type,
                    amount,
                    review_status
                FROM documents
                WHERE COALESCE(review_status, '') = 'Ready'
                """
            ).fetchall()

        for row in rows:
            vendor = normalize_text(row["vendor"])
            amount = normalize_amount(row["amount"])
            if not vendor or amount is None:
                continue

            rows_out.append(
                {
                    "vendor": vendor,
                    "client_code": normalize_text(row["client_code"]),
                    "doc_type": normalize_text(row["doc_type"]),
                    "amount": amount,
                }
            )

        return rows_out

    def _make_context_keys(
        self,
        *,
        vendor: str,
        client_code: str,
        doc_type: str,
    ) -> dict[str, str]:
        vendor_key = normalize_key(vendor)
        client_key = normalize_key(client_code)
        doc_type_key = normalize_key(doc_type)

        return {
            "client_vendor_doc_type": f"{client_key}|{vendor_key}|{doc_type_key}",
            "vendor_doc_type": f"{vendor_key}|{doc_type_key}",
            "vendor_only": vendor_key,
        }

    def _build_field_memory(self, *, for_client_code: str = "") -> dict[str, dict[str, dict[str, int]]]:
        """
        returns:
        {
          "gl_account": {
             "vendor_only::<context>": {"Software Expense": 5, "Office": 1},
             ...
          }
        }

        FIX 4: Only include rows from the requesting client or __global__ seed data.
        """
        memory: dict[str, dict[str, dict[str, int]]] = {
            field: {} for field in self.TRACKED_FIELDS
        }

        all_rows = self._collect_learning_rows() + self._collect_document_rows()
        target_client_key = normalize_key(for_client_code)

        for row in all_rows:
            field_name = normalize_text(row["field_name"])
            if field_name not in self.TRACKED_FIELDS:
                continue

            vendor = normalize_text(row["vendor"])
            client_code = normalize_text(row["client_code"])
            doc_type = normalize_text(row["doc_type"])
            value = normalize_text(row["value"])

            if not vendor or not value:
                continue

            # FIX 4: Skip rows from other clients (unless __global__)
            row_client_key = normalize_key(client_code)
            if target_client_key and row_client_key:
                if row_client_key != target_client_key and row_client_key != "__global__":
                    continue

            context_keys = self._make_context_keys(
                vendor=vendor,
                client_code=client_code,
                doc_type=doc_type,
            )

            if client_code and doc_type:
                key = f"client_vendor_doc_type::{context_keys['client_vendor_doc_type']}"
                memory[field_name].setdefault(key, {})
                memory[field_name][key][value] = memory[field_name][key].get(value, 0) + 1

            if doc_type:
                key = f"vendor_doc_type::{context_keys['vendor_doc_type']}"
                memory[field_name].setdefault(key, {})
                memory[field_name][key][value] = memory[field_name][key].get(value, 0) + 1

            key = f"vendor_only::{context_keys['vendor_only']}"
            memory[field_name].setdefault(key, {})
            memory[field_name][key][value] = memory[field_name][key].get(value, 0) + 1

        return memory

    def _build_amount_memory(self, *, for_client_code: str = "") -> dict[str, list[float]]:
        memory: dict[str, list[float]] = {}
        rows = self._collect_amount_rows()
        target_client_key = normalize_key(for_client_code)

        for row in rows:
            vendor = normalize_text(row["vendor"])
            client_code = normalize_text(row["client_code"])
            doc_type = normalize_text(row["doc_type"])
            amount = row["amount"]

            # FIX 4: Skip rows from other clients
            row_client_key = normalize_key(client_code)
            if target_client_key and row_client_key:
                if row_client_key != target_client_key and row_client_key != "__global__":
                    continue

            context_keys = self._make_context_keys(
                vendor=vendor,
                client_code=client_code,
                doc_type=doc_type,
            )

            if client_code and doc_type:
                key = f"client_vendor_doc_type::{context_keys['client_vendor_doc_type']}"
                memory.setdefault(key, []).append(amount)

            if doc_type:
                key = f"vendor_doc_type::{context_keys['vendor_doc_type']}"
                memory.setdefault(key, []).append(amount)

            key = f"vendor_only::{context_keys['vendor_only']}"
            memory.setdefault(key, []).append(amount)

        return memory

    def suggest_fields_for_document(
        self,
        *,
        vendor: str,
        client_code: str = "",
        doc_type: str = "",
        min_support: int = 3,
        min_confidence: float = 0.75,
    ) -> dict[str, dict[str, Any]]:
        vendor = normalize_text(vendor)
        client_code = normalize_text(client_code)
        doc_type = normalize_text(doc_type)

        if not vendor:
            return {}

        # FIX 4: Pass client_code to filter cross-client leakage
        memory = self._build_field_memory(for_client_code=client_code)
        context_keys = self._make_context_keys(
            vendor=vendor,
            client_code=client_code,
            doc_type=doc_type,
        )

        suggestions: dict[str, dict[str, Any]] = {}

        for field_name in self.TRACKED_FIELDS:
            best: VendorFieldSuggestion | None = None

            for context_name in self.DOC_CONTEXT_ORDER:
                context_key = context_keys[context_name]
                bucket_key = f"{context_name}::{context_key}"

                bucket = memory.get(field_name, {}).get(bucket_key, {})
                if not bucket:
                    continue

                sorted_items = sorted(bucket.items(), key=lambda x: (-x[1], x[0]))
                top_value, top_support = sorted_items[0]
                second_support = sorted_items[1][1] if len(sorted_items) > 1 else 0

                # FIX 5: Cap confidence at 0.95 (never 100% certain)
                confidence = 0.95 if top_support == 0 else round(min(0.95, top_support / max(top_support + second_support, 1)), 2)

                candidate = VendorFieldSuggestion(
                    field_name=field_name,
                    suggested_value=top_value,
                    support=top_support,
                    confidence=confidence,
                    source=context_name,
                    context_key=context_key,
                )

                if candidate.support < min_support:
                    continue
                if candidate.confidence < min_confidence:
                    continue

                best = candidate
                break

            if best:
                # FIX: Semantic cross-check — reject category suggestions that
                # conflict with strong signals in the vendor name.
                if field_name == "category" and _vendor_category_conflict(vendor, best.suggested_value):
                    continue
                suggestions[field_name] = best.to_dict()

        return suggestions

    def analyze_amount_for_document(
        self,
        *,
        vendor: str,
        amount: Any,
        client_code: str = "",
        doc_type: str = "",
        min_support: int = 3,
        suspicious_multiplier: float = 3.0,
    ) -> dict[str, Any]:
        vendor = normalize_text(vendor)
        client_code = normalize_text(client_code)
        doc_type = normalize_text(doc_type)
        amount_value = normalize_amount(amount)

        if not vendor or amount_value is None:
            return VendorAmountInsight(
                vendor=vendor,
                context_key="",
                sample_count=0,
                median_amount=None,
                min_amount=None,
                max_amount=None,
                suspicious=False,
                reason="insufficient_input",
            ).to_dict()

        # FIX 4: Pass client_code for isolation
        amount_memory = self._build_amount_memory(for_client_code=client_code)
        context_keys = self._make_context_keys(
            vendor=vendor,
            client_code=client_code,
            doc_type=doc_type,
        )

        chosen_key = ""
        samples: list[float] = []

        for context_name in self.DOC_CONTEXT_ORDER:
            context_key = context_keys[context_name]
            bucket_key = f"{context_name}::{context_key}"
            bucket_samples = amount_memory.get(bucket_key, [])
            if len(bucket_samples) >= min_support:
                chosen_key = bucket_key
                samples = bucket_samples
                break

        if not samples:
            bucket_key = f"vendor_only::{context_keys['vendor_only']}"
            samples = amount_memory.get(bucket_key, [])
            chosen_key = bucket_key

        if len(samples) < min_support:
            return VendorAmountInsight(
                vendor=vendor,
                context_key=chosen_key,
                sample_count=len(samples),
                median_amount=None,
                min_amount=min(samples) if samples else None,
                max_amount=max(samples) if samples else None,
                suspicious=False,
                reason="not_enough_history",
            ).to_dict()

        median_amount = round(statistics.median(samples), 2)
        min_amount = round(min(samples), 2)
        max_amount = round(max(samples), 2)

        suspicious = False
        reason = "within_expected_range"

        if median_amount > 0 and amount_value > median_amount * suspicious_multiplier:
            suspicious = True
            reason = f"amount_above_{suspicious_multiplier}x_median"

        if median_amount > 0 and amount_value < median_amount / suspicious_multiplier:
            suspicious = True
            reason = f"amount_below_1_over_{suspicious_multiplier}x_median"

        return VendorAmountInsight(
            vendor=vendor,
            context_key=chosen_key,
            sample_count=len(samples),
            median_amount=median_amount,
            min_amount=min_amount,
            max_amount=max_amount,
            suspicious=suspicious,
            reason=reason,
        ).to_dict()

    def explain_document(
        self,
        *,
        vendor: str,
        amount: Any = None,
        client_code: str = "",
        doc_type: str = "",
    ) -> dict[str, Any]:
        return {
            "suggestions": self.suggest_fields_for_document(
                vendor=vendor,
                client_code=client_code,
                doc_type=doc_type,
            ),
            "amount_analysis": self.analyze_amount_for_document(
                vendor=vendor,
                amount=amount,
                client_code=client_code,
                doc_type=doc_type,
            ),
        }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA vendor memory engine")
    parser.add_argument("--vendor", required=True)
    parser.add_argument("--client-code", default="")
    parser.add_argument("--doc-type", default="")
    parser.add_argument("--amount", default="")

    args = parser.parse_args()

    engine = VendorMemoryEngine()
    result = engine.explain_document(
        vendor=args.vendor,
        client_code=args.client_code,
        doc_type=args.doc_type,
        amount=args.amount,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())