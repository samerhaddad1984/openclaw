from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class LearningSuggestionEngine:
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

    def _get_columns(self, conn: sqlite3.Connection, table_name: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cur.fetchall()]

    def _normalize(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _build_query_variants(
        self,
        *,
        client_code: str,
        vendor: str,
        doc_type: str,
        field_name: str,
        limit: int,
    ) -> list[tuple[str, tuple[Any, ...], str]]:
        variants: list[tuple[str, tuple[Any, ...], str]] = []

        if client_code and vendor and doc_type:
            variants.append(
                (
                    """
                    SELECT
                        new_value,
                        COUNT(*) AS support
                    FROM learning_memory
                    WHERE client_code = ?
                      AND vendor = ?
                      AND doc_type = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC, new_value ASC
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
                    SELECT
                        new_value,
                        COUNT(*) AS support
                    FROM learning_memory
                    WHERE client_code = ?
                      AND vendor = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC, new_value ASC
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
                    SELECT
                        new_value,
                        COUNT(*) AS support
                    FROM learning_memory
                    WHERE vendor = ?
                      AND doc_type = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC, new_value ASC
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
                    SELECT
                        new_value,
                        COUNT(*) AS support
                    FROM learning_memory
                    WHERE vendor = ?
                      AND field_name = ?
                    GROUP BY new_value
                    ORDER BY support DESC, new_value ASC
                    LIMIT ?
                    """,
                    (vendor, field_name, limit),
                    "vendor",
                )
            )

        return variants

    def suggestions_for_document(
        self,
        *,
        client_code: Any = None,
        vendor: Any = None,
        doc_type: Any = None,
        limit_per_field: int = 5,
    ) -> dict[str, Any]:
        client_code = self._normalize(client_code)
        vendor = self._normalize(vendor)
        doc_type = self._normalize(doc_type)

        if not vendor:
            return {}

        with self._open() as conn:
            if not self._table_exists(conn, "learning_memory"):
                return {}

            cols = self._get_columns(conn, "learning_memory")
            required = {"client_code", "vendor", "doc_type", "field_name", "new_value"}
            if not required.issubset(set(cols)):
                return {}

            fields_to_learn = [
                "vendor",
                "client_code",
                "doc_type",
                "gl_account",
                "tax_code",
                "category",
                "review_status",
            ]

            result: dict[str, Any] = {}

            for field_name in fields_to_learn:
                query_variants = self._build_query_variants(
                    client_code=client_code,
                    vendor=vendor,
                    doc_type=doc_type,
                    field_name=field_name,
                    limit=limit_per_field,
                )

                rows = []
                matched_source = ""

                for sql, params, source in query_variants:
                    cur = conn.cursor()
                    cur.execute(sql, params)
                    fetched = cur.fetchall()
                    if fetched:
                        rows = fetched
                        matched_source = source
                        break

                if not rows:
                    continue

                options = []
                max_support = max(int(r["support"]) for r in rows) or 1

                for row in rows:
                    support = int(row["support"])
                    confidence = min(0.99, 0.55 + (support / max_support) * 0.35)

                    options.append(
                        {
                            "value": self._normalize(row["new_value"]),
                            "support": support,
                            "confidence": round(confidence, 2),
                            "source": matched_source,
                        }
                    )

                result[field_name] = options

            return result

    def best_suggestion(
        self,
        vendor: Any,
        *,
        client_code: Any = None,
        doc_type: Any = None,
    ) -> dict[str, Any]:
        raw = self.suggestions_for_document(
            client_code=client_code,
            vendor=vendor,
            doc_type=doc_type,
            limit_per_field=5,
        )

        best: dict[str, Any] = {}

        for field_name, options in raw.items():
            if not options:
                continue
            best[field_name] = options[0]

        return best