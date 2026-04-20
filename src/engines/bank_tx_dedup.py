"""Bank-transaction dedup across QBO + Plaid sources.

When a client pulls bank data from both QBO and Plaid (clients.bank_source
= 'both'), a single real-world transaction can land in
bank_transactions twice. This engine scores potential matches and
records the winners / losers in ``bank_tx_dedup``, optionally hiding
the loser from reconciliation (``hidden_duplicate=1``).

Algorithm:

- Candidates: same firm + client, different sources, amount within
  ``amount_tolerance`` (default 1¢), date within ``tolerance_days``
  (default 2).
- Confidence score:
      amount_match      → 0.60
      date proximity    → 0.20 * (1 - |Δdays| / tolerance_days)
      description fuzz  → 0.20 * SequenceMatcher ratio
  clamped to [0, 1].
- ``auto_apply`` threshold: 0.75. Below 0.75 the match is recorded
  but not auto-hidden — CPA reviews manually.
- Source preference: QBO wins over Plaid when both present. QBO
  numbers are what the CPA's system of record looks like; Plaid is
  the raw bank feed mirror.
"""
from __future__ import annotations

import difflib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _days_between(d1: str, d2: str) -> float | None:
    try:
        dt1 = datetime.fromisoformat(d1.split('T')[0])
        dt2 = datetime.fromisoformat(d2.split('T')[0])
    except (ValueError, AttributeError):
        return None
    return abs((dt1 - dt2).total_seconds() / 86400.0)


def _desc_similarity(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.5  # neutral when either side is blank
    return difflib.SequenceMatcher(
        None, a.lower().strip(), b.lower().strip()
    ).ratio()


def _compute_confidence(*, amount_match: bool,
                          date_delta: float,
                          tolerance_days: float,
                          desc_similarity: float) -> float:
    score = 0.0
    if amount_match:
        score += 0.60
    if tolerance_days > 0:
        score += max(0.0, 0.20 * (1 - date_delta / tolerance_days))
    score += 0.20 * max(0.0, min(1.0, desc_similarity))
    return max(0.0, min(1.0, score))


class BankTransactionDeduplicator:
    """Detect + record duplicate bank_transactions rows across sources."""

    AUTO_APPLY_THRESHOLD = 0.75

    def __init__(self, firm_code: str, client_code: str,
                  db_path: Path | str) -> None:
        self.firm_code = firm_code
        self.client_code = client_code
        self.db_path = Path(db_path)

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def find_duplicates(self, *,
                          tolerance_days: float = 2.0,
                          amount_tolerance: float = 0.01,
                          ) -> list[dict[str, Any]]:
        """Return a list of duplicate-candidate records with confidence
        scores. Does NOT write to bank_tx_dedup or flip hidden_duplicate."""
        with _open(self.db_path) as conn:
            rows = conn.execute(
                "SELECT bt1.id AS id1, bt1.source AS src1, "
                "       bt1.external_id AS ext1, bt1.date AS date1, "
                "       bt1.amount AS amt1, bt1.description AS desc1, "
                "       bt2.id AS id2, bt2.source AS src2, "
                "       bt2.external_id AS ext2, bt2.date AS date2, "
                "       bt2.amount AS amt2, bt2.description AS desc2 "
                "FROM bank_transactions bt1 "
                "JOIN bank_transactions bt2 "
                "  ON bt1.firm_code = bt2.firm_code "
                " AND bt1.client_code = bt2.client_code "
                " AND bt1.source != bt2.source "
                " AND bt1.id < bt2.id "
                " AND ABS(bt1.amount - bt2.amount) <= ? "
                "WHERE bt1.firm_code = ? AND bt1.client_code = ? "
                "  AND COALESCE(bt1.hidden_duplicate, 0) = 0 "
                "  AND COALESCE(bt2.hidden_duplicate, 0) = 0",
                (amount_tolerance, self.firm_code, self.client_code),
            ).fetchall()

        candidates: list[dict[str, Any]] = []
        for r in rows:
            d_delta = _days_between(r['date1'] or '', r['date2'] or '')
            if d_delta is None or d_delta > tolerance_days:
                continue
            sim = _desc_similarity(r['desc1'], r['desc2'])
            confidence = _compute_confidence(
                amount_match=True,
                date_delta=d_delta,
                tolerance_days=tolerance_days,
                desc_similarity=sim,
            )
            # Source priority: QBO wins over Plaid. Pick the winner.
            if r['src1'] == 'qbo':
                primary_id, primary_src = r['id1'], r['src1']
                duplicate_id, duplicate_src = r['id2'], r['src2']
            elif r['src2'] == 'qbo':
                primary_id, primary_src = r['id2'], r['src2']
                duplicate_id, duplicate_src = r['id1'], r['src1']
            else:
                # Neither side is QBO — just keep the lower-id row.
                primary_id, primary_src = r['id1'], r['src1']
                duplicate_id, duplicate_src = r['id2'], r['src2']

            candidates.append({
                'primary_id': primary_id,
                'primary_source': primary_src,
                'duplicate_id': duplicate_id,
                'duplicate_source': duplicate_src,
                'confidence': round(confidence, 4),
                'date_delta_days': round(d_delta, 3),
                'description_similarity': round(sim, 4),
            })
        return candidates

    # ------------------------------------------------------------------
    # Mark / unmark
    # ------------------------------------------------------------------

    def mark_duplicates(self, *,
                          tolerance_days: float = 2.0,
                          amount_tolerance: float = 0.01,
                          auto_apply: bool = True,
                          resolved_by: str = 'auto',
                          min_auto_confidence: float | None = None,
                          ) -> int:
        """Run detection, write rows into bank_tx_dedup, and optionally
        hide the duplicate. Returns how many rows were auto-hidden."""
        if min_auto_confidence is None:
            min_auto_confidence = self.AUTO_APPLY_THRESHOLD
        dups = self.find_duplicates(
            tolerance_days=tolerance_days,
            amount_tolerance=amount_tolerance,
        )
        hidden = 0
        with _open(self.db_path) as conn:
            for d in dups:
                # Skip if we already logged this exact pair.
                existing = conn.execute(
                    "SELECT id FROM bank_tx_dedup "
                    "WHERE firm_code=? AND client_code=? "
                    "AND primary_id=? AND duplicate_id=?",
                    (self.firm_code, self.client_code,
                     d['primary_id'], d['duplicate_id']),
                ).fetchone()
                if existing:
                    continue
                conn.execute(
                    "INSERT INTO bank_tx_dedup "
                    "(firm_code, client_code, primary_source, primary_id, "
                    " duplicate_source, duplicate_id, match_confidence, "
                    " detected_at, resolved_by) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (self.firm_code, self.client_code,
                     d['primary_source'], d['primary_id'],
                     d['duplicate_source'], d['duplicate_id'],
                     d['confidence'], _iso_now(),
                     resolved_by if auto_apply else None),
                )
                if auto_apply and d['confidence'] >= min_auto_confidence:
                    conn.execute(
                        "UPDATE bank_transactions SET hidden_duplicate=1 "
                        "WHERE id=?",
                        (d['duplicate_id'],),
                    )
                    hidden += 1
            conn.commit()
        return hidden

    def unmark_duplicate(self, *, duplicate_id: str,
                            resolved_by: str = 'manual') -> bool:
        """Restore a hidden duplicate. Used by the CPA review UI when
        the engine got it wrong."""
        with _open(self.db_path) as conn:
            cur = conn.execute(
                "UPDATE bank_transactions SET hidden_duplicate=0 "
                "WHERE id=? AND firm_code=? AND client_code=?",
                (duplicate_id, self.firm_code, self.client_code),
            )
            if cur.rowcount:
                # Append an audit breadcrumb — don't delete the original
                # dedup row; it's our record that we ever flagged this.
                conn.execute(
                    "UPDATE bank_tx_dedup SET resolved_by=? "
                    "WHERE duplicate_id=? AND firm_code=? AND client_code=?",
                    (f"manual_unmark:{resolved_by}", duplicate_id,
                     self.firm_code, self.client_code),
                )
            conn.commit()
            return cur.rowcount > 0

    def list_dedup_log(self) -> list[dict[str, Any]]:
        """Return all dedup rows for this (firm, client) for the UI."""
        with _open(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM bank_tx_dedup "
                "WHERE firm_code=? AND client_code=? "
                "ORDER BY detected_at DESC, id DESC",
                (self.firm_code, self.client_code),
            ).fetchall()
        return [dict(r) for r in rows]
