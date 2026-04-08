from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

from src.agents.core.approval_models import MatchDecision, utc_now_iso


class ApprovalStore:

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS match_decisions (
                    decision_id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    chosen_transaction_id TEXT,
                    reviewer TEXT,
                    reason TEXT,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_match_decisions_document_id
                ON match_decisions(document_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_match_decisions_created_at
                ON match_decisions(created_at)
                """
            )
            conn.commit()

    def add_decision(self, decision: MatchDecision) -> None:
        row = decision.to_row()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO match_decisions (
                    decision_id,
                    document_id,
                    decision_type,
                    chosen_transaction_id,
                    reviewer,
                    reason,
                    notes,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["decision_id"],
                    row["document_id"],
                    row["decision_type"],
                    row["chosen_transaction_id"],
                    row["reviewer"],
                    row["reason"],
                    row["notes"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
            conn.commit()

    def update_decision(self, decision: MatchDecision) -> None:
        decision.updated_at = utc_now_iso()
        row = decision.to_row()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE match_decisions
                SET
                    document_id = ?,
                    decision_type = ?,
                    chosen_transaction_id = ?,
                    reviewer = ?,
                    reason = ?,
                    notes = ?,
                    updated_at = ?
                WHERE decision_id = ?
                """,
                (
                    row["document_id"],
                    row["decision_type"],
                    row["chosen_transaction_id"],
                    row["reviewer"],
                    row["reason"],
                    row["notes"],
                    row["updated_at"],
                    row["decision_id"],
                ),
            )
            conn.commit()

    def get_decision(self, decision_id: str) -> Optional[MatchDecision]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM match_decisions
                WHERE decision_id = ?
                """,
                (decision_id,),
            ).fetchone()

        if not row:
            return None

        return MatchDecision.from_row(dict(row))

    def list_decisions(self) -> list[MatchDecision]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM match_decisions
                ORDER BY created_at DESC, decision_id DESC
                """
            ).fetchall()

        return [MatchDecision.from_row(dict(r)) for r in rows]

    def list_decisions_for_document(self, document_id: str) -> list[MatchDecision]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM match_decisions
                WHERE document_id = ?
                ORDER BY created_at DESC, decision_id DESC
                """,
                (document_id,),
            ).fetchall()

        return [MatchDecision.from_row(dict(r)) for r in rows]

    def get_latest_decision_for_document(self, document_id: str) -> Optional[MatchDecision]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM match_decisions
                WHERE document_id = ?
                ORDER BY created_at DESC, decision_id DESC
                LIMIT 1
                """,
                (document_id,),
            ).fetchone()

        if not row:
            return None

        return MatchDecision.from_row(dict(row))

    def decision_counts_by_type(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT decision_type, COUNT(*) AS count_value
                FROM match_decisions
                GROUP BY decision_type
                ORDER BY decision_type
                """
            ).fetchall()

        return {str(r["decision_type"]): int(r["count_value"]) for r in rows}