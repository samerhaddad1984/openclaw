"""General-ledger engine (Sprint C).

Manual journal entries live in `manual_journal_entries` with a single
`amount` column (one debit leg + one credit leg per entry). Posting an
entry writes two rows into `gl_transactions`, one per leg, so downstream
reports (trial balance, balance sheet, P&L) can see manual activity.

The invariant is enforced both at write time (debit == credit by
construction — they come from the same `amount` field) and at read time
via `verify_gl_balanced`.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.engines.ocr_engine import DB_PATH, _utc_now_iso

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS gl_transactions (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_id      TEXT NOT NULL,          -- FK to manual_journal_entries.entry_id
        client_code   TEXT NOT NULL,
        period        TEXT NOT NULL,
        entry_date    TEXT NOT NULL,
        account_code  TEXT NOT NULL,
        side          TEXT NOT NULL CHECK (side IN ('debit','credit')),
        amount        REAL NOT NULL CHECK (amount > 0),
        description   TEXT,
        source        TEXT NOT NULL DEFAULT 'manual_je',
        document_id   TEXT,
        reversed_by   TEXT,                   -- entry_id of the reversing JE, if any
        created_at    TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_gl_client_period ON gl_transactions(client_code, period)",
    "CREATE INDEX IF NOT EXISTS idx_gl_account ON gl_transactions(account_code, client_code, period)",
    "CREATE INDEX IF NOT EXISTS idx_gl_entry ON gl_transactions(entry_id)",
]


def ensure_schema(db_path: Optional[Path] = None) -> None:
    target = str(db_path) if db_path is not None else str(DB_PATH)
    conn = sqlite3.connect(target, timeout=10)
    try:
        for stmt in _DDL:
            conn.execute(stmt)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Posting and reversal
# ---------------------------------------------------------------------------

def post_journal_entry(
    entry_id: str,
    *,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Transition a manual JE from 'draft' to 'posted' and write the two
    matching rows into gl_transactions.

    Idempotent: posting an already-posted entry returns without error but
    without re-writing the GL rows. Raises ValueError when the entry is
    missing, unbalanced (defensive), or currently in a non-postable status.
    """
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, document_id, status "
            "FROM manual_journal_entries WHERE entry_id = ?",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"journal_entry_not_found:{entry_id}")

        status = (row["status"] or "").lower()
        if status == "posted":
            return {"ok": True, "entry_id": entry_id, "idempotent": True}
        if status in ("reversed", "phantom_tax_blocked", "conflict"):
            raise ValueError(f"cannot_post_entry_status={status}")

        amount = float(row["amount"] or 0)
        if amount <= 0.005:
            raise ValueError("cannot_post_zero_or_negative_amount")
        debit_acct = (row["debit_account"] or "").strip()
        credit_acct = (row["credit_account"] or "").strip()
        if not debit_acct or not credit_acct:
            raise ValueError("missing_account_on_entry")
        if debit_acct == credit_acct:
            raise ValueError("debit_and_credit_accounts_must_differ")

        # BEGIN TRANSACTION: either both legs land or neither.
        with conn:
            # Drop any stale GL rows from a prior attempt so reposting
            # (should it be legitimate) doesn't double-count.
            conn.execute(
                "DELETE FROM gl_transactions WHERE entry_id = ? AND source = 'manual_je'",
                (entry_id,),
            )
            for side, account in (("debit", debit_acct), ("credit", credit_acct)):
                conn.execute(
                    "INSERT INTO gl_transactions "
                    "(entry_id, client_code, period, entry_date, account_code, "
                    "side, amount, description, source, document_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        entry_id, row["client_code"], row["period"],
                        row["entry_date"], account, side, amount,
                        row["description"] or "", "manual_je",
                        row["document_id"],
                    ),
                )
            conn.execute(
                "UPDATE manual_journal_entries SET status='posted', "
                "updated_at=datetime('now') WHERE entry_id=?",
                (entry_id,),
            )
        return {"ok": True, "entry_id": entry_id, "amount": amount,
                "debit": debit_acct, "credit": credit_acct}
    finally:
        conn.close()


def reverse_journal_entry(
    entry_id: str,
    *,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Flip a manual JE to status='reversed'.

    If the entry was already posted we insert two compensating GL rows
    (debit/credit sides swapped, same amount) so the ledger net-to-zero
    without destroying the audit trail. If the entry was still a draft we
    just mark it reversed — nothing to undo in the GL.
    """
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, document_id, status "
            "FROM manual_journal_entries WHERE entry_id = ?",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"journal_entry_not_found:{entry_id}")
        status = (row["status"] or "").lower()
        if status == "reversed":
            return {"ok": True, "entry_id": entry_id, "idempotent": True}
        if status not in ("draft", "posted"):
            raise ValueError(f"cannot_reverse_entry_status={status}")

        amount = float(row["amount"] or 0)
        with conn:
            if status == "posted" and amount > 0:
                # Compensating pair: debit the prior credit, credit the prior debit.
                for side, account in (("debit", row["credit_account"]),
                                       ("credit", row["debit_account"])):
                    conn.execute(
                        "INSERT INTO gl_transactions "
                        "(entry_id, client_code, period, entry_date, account_code, "
                        "side, amount, description, source, document_id, reversed_by) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            entry_id, row["client_code"], row["period"],
                            row["entry_date"], (account or "").strip(), side, amount,
                            f"REVERSAL: {row['description'] or ''}",
                            "manual_je_reversal", row["document_id"], entry_id,
                        ),
                    )
            conn.execute(
                "UPDATE manual_journal_entries SET status='reversed', "
                "updated_at=datetime('now') WHERE entry_id=?",
                (entry_id,),
            )
        return {"ok": True, "entry_id": entry_id, "prior_status": status}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GL queries used by trial balance / reports
# ---------------------------------------------------------------------------

def sum_gl_by_account(
    client_code: str,
    period: str,
    *,
    db_path: Optional[Path] = None,
) -> Dict[str, Dict[str, float]]:
    """Aggregate posted GL rows into {account_code: {debit, credit, net}}.

    period is matched as a prefix against entry_date so '2026-04' captures
    the whole month. Net is computed debit - credit regardless of account
    type; callers interpret the sign.
    """
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=10)
    try:
        rows = conn.execute(
            "SELECT account_code, side, SUM(amount) AS total FROM gl_transactions "
            "WHERE client_code = ? AND (period = ? OR entry_date LIKE ?) "
            "GROUP BY account_code, side",
            (client_code, period, f"{period}%"),
        ).fetchall()
    finally:
        conn.close()
    out: Dict[str, Dict[str, float]] = {}
    for acct, side, total in rows:
        slot = out.setdefault(acct, {"debit": 0.0, "credit": 0.0, "net": 0.0})
        slot[side] = float(total or 0.0)
    for acct, slot in out.items():
        slot["net"] = slot["debit"] - slot["credit"]
    return out


def verify_gl_balanced(
    client_code: str,
    period: str,
    *,
    db_path: Optional[Path] = None,
    tolerance: float = 0.01,
) -> Dict[str, Any]:
    """Sanity check: sum(debits) must equal sum(credits) across the period."""
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    conn = sqlite3.connect(target, timeout=10)
    try:
        row = conn.execute(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN side='debit'  THEN amount END), 0.0) AS d, "
            "  COALESCE(SUM(CASE WHEN side='credit' THEN amount END), 0.0) AS c "
            "FROM gl_transactions "
            "WHERE client_code = ? AND (period = ? OR entry_date LIKE ?)",
            (client_code, period, f"{period}%"),
        ).fetchone()
    finally:
        conn.close()
    debits = float(row[0] or 0.0)
    credits = float(row[1] or 0.0)
    diff = debits - credits
    return {
        "client_code": client_code, "period": period,
        "debit_total": debits, "credit_total": credits,
        "difference": diff, "balanced": abs(diff) <= tolerance,
    }
