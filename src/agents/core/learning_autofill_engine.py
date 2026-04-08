from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class AutofillDecision:
    field_name: str
    old_value: Any
    new_value: Any
    support: int
    confidence: float
    source: str
    applied: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_name": self.field_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "support": self.support,
            "confidence": self.confidence,
            "source": self.source,
            "applied": self.applied,
            "reason": self.reason,
        }


class LearningAutofillEngine:

    FIELD_SUPPORT_THRESHOLDS = {
        "client_code": 2,
        "gl_account": 3,
        "tax_code": 3,
        "category": 3,
        "review_status": 5,
        "vendor": 5,
        "doc_type": 5,
    }

    FIELD_CONFIDENCE_THRESHOLDS = {
        "client_code": 0.70,
        "gl_account": 0.75,
        "tax_code": 0.75,
        "category": 0.75,
        "review_status": 0.85,
        "vendor": 0.85,
        "doc_type": 0.85,
    }

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (table_name,),
        )
        return cur.fetchone() is not None

    def _normalize_text(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _normalize_amount(self, value: Any) -> str:
        text = self._normalize_text(value)
        if not text:
            return ""
        try:
            return f"{round(float(text.replace(',', '')), 2):.2f}"
        except Exception:
            return text

    def _values_equal(self, field_name: str, left: Any, right: Any) -> bool:
        if field_name == "amount":
            return self._normalize_amount(left) == self._normalize_amount(right)
        return self._normalize_text(left) == self._normalize_text(right)

    def _candidate_variants(
        self,
        *,
        client_code: str,
        vendor: str,
        doc_type: str,
        field_name: str,
        limit: int = 5,
    ) -> list[tuple[str, tuple[Any, ...], str]]:

        variants = []

        if client_code and vendor and doc_type:
            variants.append(
                (
                    """
                    SELECT new_value, COUNT(*) AS support
                    FROM learning_memory
                    WHERE client_code = ?
                      AND vendor = ?
                      AND doc_type = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC
                    LIMIT ?
                    """,
                    (client_code, vendor, doc_type, field_name, limit),
                    "client_vendor_doc_type",
                )
            )

        if client_code and vendor:
            variants.append(
                (
                    """
                    SELECT new_value, COUNT(*) AS support
                    FROM learning_memory
                    WHERE client_code = ?
                      AND vendor = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC
                    LIMIT ?
                    """,
                    (client_code, vendor, field_name, limit),
                    "client_vendor",
                )
            )

        if vendor and doc_type:
            variants.append(
                (
                    """
                    SELECT new_value, COUNT(*) AS support
                    FROM learning_memory
                    WHERE vendor = ?
                      AND doc_type = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC
                    LIMIT ?
                    """,
                    (vendor, doc_type, field_name, limit),
                    "vendor_doc_type",
                )
            )

        if vendor:
            variants.append(
                (
                    """
                    SELECT new_value, COUNT(*) AS support
                    FROM learning_memory
                    WHERE vendor = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC
                    LIMIT ?
                    """,
                    (vendor, field_name, limit),
                    "vendor",
                )
            )

        return variants

    def _top_learning_option(
        self,
        *,
        client_code: Any,
        vendor: Any,
        doc_type: Any,
        field_name: str,
    ) -> dict[str, Any] | None:

        client_code = self._normalize_text(client_code)
        vendor = self._normalize_text(vendor)
        doc_type = self._normalize_text(doc_type)

        if not vendor:
            return None

        with self._open() as conn:

            if not self._table_exists(conn, "learning_memory"):
                return None

            for sql, params, source in self._candidate_variants(
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                field_name=field_name,
            ):

                rows = conn.execute(sql, params).fetchall()

                if not rows:
                    continue

                top = rows[0]
                support = int(top["support"])
                max_support = max(int(r["support"]) for r in rows)

                confidence = min(0.99, 0.55 + (support / max_support) * 0.35)

                return {
                    "value": self._normalize_text(top["new_value"]),
                    "support": support,
                    "confidence": round(confidence, 2),
                    "source": source,
                }

        return None

    def apply_autofill(
        self,
        extracted: dict[str, Any],
        *,
        allowed_fields: list[str] | None = None,
    ) -> dict[str, Any]:

        output = dict(extracted)
        decisions: list[AutofillDecision] = []

        client_code = output.get("client_code")
        vendor = output.get("vendor")
        doc_type = output.get("doc_type")

        fields = allowed_fields or [
            "client_code",
            "vendor",
            "doc_type",
            "gl_account",
            "tax_code",
            "category",
            "review_status",
        ]

        for field in fields:

            current_value = output.get(field)

            suggestion = self._top_learning_option(
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                field_name=field,
            )

            if not suggestion:
                continue

            suggested_value = suggestion["value"]
            support = suggestion["support"]
            confidence = suggestion["confidence"]
            source = suggestion["source"]

            if self._values_equal(field, current_value, suggested_value):
                continue

            support_threshold = self.FIELD_SUPPORT_THRESHOLDS.get(field, 5)
            confidence_threshold = self.FIELD_CONFIDENCE_THRESHOLDS.get(field, 0.85)

            if support < support_threshold:
                decisions.append(
                    AutofillDecision(
                        field,
                        current_value,
                        suggested_value,
                        support,
                        confidence,
                        source,
                        False,
                        "support_below_threshold",
                    )
                )
                continue

            if confidence < confidence_threshold:
                decisions.append(
                    AutofillDecision(
                        field,
                        current_value,
                        suggested_value,
                        support,
                        confidence,
                        source,
                        False,
                        "confidence_below_threshold",
                    )
                )
                continue

            output[field] = suggested_value

            decisions.append(
                AutofillDecision(
                    field,
                    current_value,
                    suggested_value,
                    support,
                    confidence,
                    source,
                    True,
                    "autofilled",
                )
            )

            if field == "client_code":
                client_code = suggested_value

            if field == "vendor":
                vendor = suggested_value

            if field == "doc_type":
                doc_type = suggested_value

        output["learning_autofill_decisions"] = [d.to_dict() for d in decisions]
        output["learning_autofill_applied_count"] = sum(1 for d in decisions if d.applied)

        return output