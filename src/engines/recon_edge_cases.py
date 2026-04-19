"""
src/engines/recon_edge_cases.py — Sprint I Part 1.

Reconciliation edge-case handlers that the core
``reconciliation_engine`` doesn't cover natively:

  1. NSF cheque reversal (deposit + fee)
  2. Stop-payment processing (void issued cheque + fee)
  3. Internal transfers between two of the same client's bank accounts
  4. Foreign-exchange reconciliation (USD bill paid via CAD account)
  5. Bank-error correction (wrong amount posted, then reversed)

Each function returns a dict with the journal-entry skeleton(s) and an
audit summary. They write to two new tables:

  * ``recon_adjustments``   — one row per NSF/stop/FX/error event
  * ``internal_transfers``  — one row per matched transfer pair
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date as _date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
_ZERO = Decimal("0")

# Default fees the bank typically charges. Override per call when known.
DEFAULT_NSF_FEE = Decimal("45.00")
DEFAULT_STOP_PAYMENT_FEE = Decimal("30.00")

# Tolerance window for matching transfer halves.
DEFAULT_INTERNAL_TRANSFER_DAYS = 3
# Tolerance for FX rate fluctuation (2 % is the conventional CPA threshold).
FX_TOLERANCE_PCT = Decimal("0.02")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# DDL — additive tables
# ---------------------------------------------------------------------------

EDGE_DDL = """
CREATE TABLE IF NOT EXISTS recon_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT DEFAULT '',
    client_code TEXT NOT NULL,
    adjustment_type TEXT NOT NULL,
    bank_tx_id TEXT,
    document_id TEXT,
    amount REAL,
    fee_amount REAL DEFAULT 0,
    reason TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    je_payload TEXT
);

CREATE INDEX IF NOT EXISTS idx_recon_adjustments_lookup
    ON recon_adjustments(client_code, adjustment_type, created_at);

CREATE TABLE IF NOT EXISTS internal_transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT DEFAULT '',
    client_code TEXT NOT NULL,
    from_account TEXT,
    to_account TEXT,
    from_tx_id TEXT,
    to_tx_id TEXT,
    amount REAL NOT NULL,
    detected_at TEXT DEFAULT (datetime('now')),
    confidence REAL
);
"""


def ensure_edge_tables(conn: sqlite3.Connection) -> None:
    for stmt in EDGE_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()


def _record_adjustment(
    conn: sqlite3.Connection,
    *,
    firm_code: str,
    client_code: str,
    adjustment_type: str,
    bank_tx_id: str = "",
    document_id: str = "",
    amount: float | Decimal = 0,
    fee_amount: float | Decimal = 0,
    reason: str = "",
    created_by: str = "",
    je_payload: str = "",
) -> int:
    ensure_edge_tables(conn)
    cur = conn.execute(
        """INSERT INTO recon_adjustments
           (firm_code, client_code, adjustment_type, bank_tx_id, document_id,
            amount, fee_amount, reason, created_by, je_payload)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (firm_code, client_code, adjustment_type, bank_tx_id, document_id,
         float(amount), float(fee_amount), reason, created_by, je_payload),
    )
    conn.commit()
    return int(cur.lastrowid)


def _je(date_str: str, description: str, lines: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a balanced JE payload."""
    debit = sum(Decimal(str(l.get("debit") or 0)) for l in lines)
    credit = sum(Decimal(str(l.get("credit") or 0)) for l in lines)
    return {
        "date": date_str,
        "description": description,
        "lines": lines,
        "total_debit": float(debit),
        "total_credit": float(credit),
        "balanced": abs(debit - credit) <= Decimal("0.02"),
    }


# ---------------------------------------------------------------------------
# Feature 1 — NSF cheque
# ---------------------------------------------------------------------------

def handle_nsf_cheque(
    conn: sqlite3.Connection,
    *,
    firm_code: str,
    client_code: str,
    original_deposit_tx_id: str,
    nsf_date: str,
    customer_name: str = "",
    cheque_number: str = "",
    amount: float | Decimal | str = 0,
    nsf_fee: float | Decimal | str = DEFAULT_NSF_FEE,
    created_by: str = "",
) -> dict[str, Any]:
    """Record an NSF return: reverse the deposit + book the bank fee.

    Returns a dict with two JE skeletons (reversal + fee) and the audit
    row id.
    """
    if not original_deposit_tx_id:
        raise ValueError("original_deposit_tx_id is required")
    amount_d = Decimal(str(amount))
    if amount_d <= 0:
        raise ValueError("amount must be > 0")
    fee_d = Decimal(str(nsf_fee))

    reversal_je = _je(
        nsf_date,
        f"NSF return — {customer_name or 'customer'} cheque #{cheque_number or '?'}",
        [
            {"account": "1200", "debit": float(amount_d), "credit": 0,
             "memo": f"AR re-established — NSF {customer_name}"},
            {"account": "1010", "debit": 0, "credit": float(amount_d),
             "memo": "Cash reversed for NSF"},
        ],
    )
    fee_je = _je(
        nsf_date,
        "Bank NSF fee",
        [
            {"account": "5210", "debit": float(fee_d), "credit": 0,
             "memo": "Bank NSF charge"},
            {"account": "1010", "debit": 0, "credit": float(fee_d),
             "memo": "Cash for NSF fee"},
        ],
    )

    # Persist NSF flag on the bank tx if the column exists.
    try:
        conn.execute(
            "UPDATE bank_transactions SET reconciled=0 WHERE id=?",
            (original_deposit_tx_id,),
        )
        try:
            conn.execute(
                "ALTER TABLE bank_transactions ADD COLUMN nsf_returned INTEGER DEFAULT 0",
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE bank_transactions ADD COLUMN nsf_date TEXT",
            )
        except sqlite3.OperationalError:
            pass
        conn.execute(
            "UPDATE bank_transactions SET nsf_returned=1, nsf_date=? WHERE id=?",
            (nsf_date, original_deposit_tx_id),
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass

    import json as _json
    audit_id = _record_adjustment(
        conn, firm_code=firm_code, client_code=client_code,
        adjustment_type="nsf_return", bank_tx_id=original_deposit_tx_id,
        amount=amount_d, fee_amount=fee_d,
        reason=f"NSF cheque #{cheque_number} from {customer_name}",
        created_by=created_by,
        je_payload=_json.dumps({"reversal": reversal_je, "fee": fee_je}),
    )

    return {
        "audit_id": audit_id,
        "reversal_je": reversal_je,
        "fee_je": fee_je,
        "nsf_fee": float(fee_d),
        "amount": float(amount_d),
        "nsf_date": nsf_date,
    }


# ---------------------------------------------------------------------------
# Feature 2 — Stop payment
# ---------------------------------------------------------------------------

def handle_stop_payment(
    conn: sqlite3.Connection,
    *,
    firm_code: str,
    client_code: str,
    cheque_tx_id: str,
    stop_date: str,
    amount: float | Decimal | str,
    payee: str = "",
    cheque_number: str = "",
    stop_fee: float | Decimal | str = DEFAULT_STOP_PAYMENT_FEE,
    reason: str = "",
    cheque_already_cleared: bool = False,
    created_by: str = "",
) -> dict[str, Any]:
    """Process a stop-payment request.

    Two scenarios:
      * cheque_already_cleared = False → just book the stop-payment fee
      * cheque_already_cleared = True  → reverse the prior payment AND
        book the fee (the bank refunded the cheque amount)
    """
    if not cheque_tx_id:
        raise ValueError("cheque_tx_id is required")
    amount_d = Decimal(str(amount))
    if amount_d <= 0:
        raise ValueError("amount must be > 0")
    fee_d = Decimal(str(stop_fee))

    fee_je = _je(
        stop_date,
        f"Stop-payment fee — cheque #{cheque_number}",
        [
            {"account": "5210", "debit": float(fee_d), "credit": 0,
             "memo": f"Stop-payment fee {cheque_number}"},
            {"account": "1010", "debit": 0, "credit": float(fee_d),
             "memo": "Cash for stop-payment fee"},
        ],
    )

    reversal_je = None
    if cheque_already_cleared:
        # Reverse the original AP payment. Bank refunded; AP re-established.
        reversal_je = _je(
            stop_date,
            f"Stop-payment reversal — cheque #{cheque_number} to {payee}",
            [
                {"account": "1010", "debit": float(amount_d), "credit": 0,
                 "memo": "Bank refund of stopped cheque"},
                {"account": "2000", "debit": 0, "credit": float(amount_d),
                 "memo": f"AP re-established — {payee}"},
            ],
        )

    # Mark the cheque as voided/stopped.
    try:
        for col, ddl in (
            ("stop_payment", "INTEGER DEFAULT 0"),
            ("stop_payment_date", "TEXT"),
        ):
            try:
                conn.execute(
                    f"ALTER TABLE bank_transactions ADD COLUMN {col} {ddl}",
                )
            except sqlite3.OperationalError:
                pass
        conn.execute(
            "UPDATE bank_transactions SET stop_payment=1, stop_payment_date=? WHERE id=?",
            (stop_date, cheque_tx_id),
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass

    import json as _json
    audit_id = _record_adjustment(
        conn, firm_code=firm_code, client_code=client_code,
        adjustment_type="stop_payment", bank_tx_id=cheque_tx_id,
        amount=amount_d, fee_amount=fee_d,
        reason=f"Stop-payment cheque #{cheque_number}: {reason}",
        created_by=created_by,
        je_payload=_json.dumps({"reversal": reversal_je, "fee": fee_je}),
    )

    return {
        "audit_id": audit_id,
        "fee_je": fee_je,
        "reversal_je": reversal_je,
        "stop_fee": float(fee_d),
        "amount": float(amount_d),
        "stop_date": stop_date,
        "cheque_already_cleared": cheque_already_cleared,
    }


# ---------------------------------------------------------------------------
# Feature 3 — Internal transfers
# ---------------------------------------------------------------------------

def _memo_similarity(a: str, b: str) -> float:
    """Cheap memo-overlap heuristic using set-of-words."""
    sa = {w.lower() for w in (a or "").split() if len(w) > 2}
    sb = {w.lower() for w in (b or "").split() if len(w) > 2}
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(len(sa | sb), 1)


def detect_internal_transfers(
    conn: sqlite3.Connection,
    *,
    firm_code: str = "",
    client_code: str,
    tolerance_days: int = DEFAULT_INTERNAL_TRANSFER_DAYS,
    days_back: int = 730,
) -> list[dict[str, Any]]:
    """Find pairs of bank transactions that look like a transfer between two
    of the same client's accounts.

    A pair matches when:
      * one is a debit (withdrawal), the other a credit (deposit)
      * absolute amounts match within $0.01
      * dates differ by no more than tolerance_days
      * accounts are different
    """
    ensure_edge_tables(conn)
    rows = conn.execute(
        """SELECT id, account_id, amount, date,
                  COALESCE(description,'') AS description,
                  COALESCE(merchant_name,'') AS merchant_name
           FROM bank_transactions
           WHERE LOWER(COALESCE(client_code,'')) = LOWER(?)
             AND COALESCE(date,'') >= date('now', '-' || ? || ' days')
             AND amount IS NOT NULL
           ORDER BY date""",
        (client_code, int(days_back)),
    ).fetchall()
    if not rows:
        return []

    # Index by account.
    by_account: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        by_account.setdefault(r["account_id"] or "", []).append(r)

    if len(by_account) < 2:
        return []

    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for a, b in combinations(by_account.keys(), 2):
        for w in by_account[a]:
            if (w["amount"] or 0) >= 0:
                continue
            w_amt = abs(Decimal(str(w["amount"])))
            for d in by_account[b]:
                if (d["amount"] or 0) <= 0:
                    continue
                d_amt = Decimal(str(d["amount"]))
                if abs(w_amt - d_amt) > Decimal("0.01"):
                    continue
                try:
                    w_date = _date.fromisoformat(str(w["date"])[:10])
                    d_date = _date.fromisoformat(str(d["date"])[:10])
                except ValueError:
                    continue
                day_gap = abs((w_date - d_date).days)
                if day_gap > tolerance_days:
                    continue
                key = tuple(sorted([str(w["id"]), str(d["id"])]))
                if key in seen:
                    continue
                seen.add(key)
                memo_sim = _memo_similarity(w["description"], d["description"])
                confidence = 0.7 + (0.3 * memo_sim) - (0.05 * day_gap)
                matches.append({
                    "transfer_pair": (str(w["id"]), str(d["id"])),
                    "from_account": a,
                    "to_account": b,
                    "amount": float(w_amt),
                    "from_date": str(w["date"]),
                    "to_date": str(d["date"]),
                    "day_gap": day_gap,
                    "memo_similarity": round(memo_sim, 3),
                    "confidence": round(max(0.0, min(1.0, confidence)), 3),
                })

    # Persist + mark reconciled.
    for m in matches:
        conn.execute(
            """INSERT INTO internal_transfers
               (firm_code, client_code, from_account, to_account,
                from_tx_id, to_tx_id, amount, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (firm_code, client_code, m["from_account"], m["to_account"],
             m["transfer_pair"][0], m["transfer_pair"][1],
             m["amount"], m["confidence"]),
        )
        for tx_id in m["transfer_pair"]:
            conn.execute(
                "UPDATE bank_transactions SET reconciled=1 WHERE id=?",
                (tx_id,),
            )
    conn.commit()
    return matches


# ---------------------------------------------------------------------------
# Feature 4 — FX reconciliation
# ---------------------------------------------------------------------------

def reconcile_fx_transaction(
    *,
    bank_amount_cad: float | Decimal | str,
    document_amount_foreign: float | Decimal | str,
    exchange_rate: float | Decimal | str,
    foreign_currency: str = "USD",
    tolerance_pct: float | Decimal | str = FX_TOLERANCE_PCT,
    fx_date: str = "",
) -> dict[str, Any]:
    """Reconcile a foreign-currency document against a CAD bank charge.

    Returns:
      * matched         True/False
      * cad_equivalent  document × rate
      * difference      bank − cad_equivalent
      * within_tolerance bool
      * fx_gain_loss    if non-zero, what to post
      * fx_je           skeleton journal entry (only if outside tolerance)
    """
    bank = Decimal(str(bank_amount_cad))
    doc = Decimal(str(document_amount_foreign))
    rate = Decimal(str(exchange_rate))
    tol = Decimal(str(tolerance_pct))
    if bank <= 0 or doc <= 0 or rate <= 0:
        raise ValueError("amounts and rate must be > 0")
    cad_equiv = _round(doc * rate)
    diff = _round(bank - cad_equiv)
    pct = abs(diff) / cad_equiv if cad_equiv > 0 else Decimal("0")
    within_tol = pct <= tol

    fx_je = None
    if not within_tol or diff != _ZERO:
        # Always book a tiny FX gain/loss when there is *any* diff so the
        # reconciliation balances; a non-trivial diff also flips
        # within_tolerance to False.
        if diff > 0:
            # Bank charged more than the doc → FX loss.
            fx_je = _je(
                fx_date or _utc_now()[:10],
                f"FX loss on {foreign_currency} payment",
                [
                    {"account": "5300", "debit": float(abs(diff)), "credit": 0,
                     "memo": f"FX loss {foreign_currency}/CAD"},
                    {"account": "1010", "debit": 0, "credit": float(abs(diff)),
                     "memo": "Bank diff posted to FX loss"},
                ],
            )
        else:
            fx_je = _je(
                fx_date or _utc_now()[:10],
                f"FX gain on {foreign_currency} payment",
                [
                    {"account": "1010", "debit": float(abs(diff)), "credit": 0,
                     "memo": "Bank credit from FX gain"},
                    {"account": "4300", "debit": 0, "credit": float(abs(diff)),
                     "memo": f"FX gain {foreign_currency}/CAD"},
                ],
            )

    return {
        "matched": within_tol,
        "cad_equivalent": float(cad_equiv),
        "bank_amount_cad": float(bank),
        "document_amount_foreign": float(doc),
        "foreign_currency": foreign_currency,
        "exchange_rate": float(rate),
        "difference": float(diff),
        "pct_difference": float(round(pct, 4)),
        "within_tolerance": within_tol,
        "fx_je": fx_je,
    }


# ---------------------------------------------------------------------------
# Feature 5 — Bank-error correction
# ---------------------------------------------------------------------------

def handle_bank_error_correction(
    conn: sqlite3.Connection,
    *,
    firm_code: str,
    client_code: str,
    wrong_tx_id: str,
    wrong_amount: float | Decimal | str,
    correct_amount: float | Decimal | str,
    correction_date: str,
    description: str = "",
    created_by: str = "",
) -> dict[str, Any]:
    """Bank posts wrong amount, then corrects it. We track both rows and
    emit a netting JE if the net effect on the GL is zero.

    Returns the audit id and the netting JE (if any).
    """
    wrong = Decimal(str(wrong_amount))
    correct = Decimal(str(correct_amount))
    diff = _round(wrong - correct)
    if diff == 0:
        return {"audit_id": None, "net_effect": 0.0, "je": None,
                 "note": "no correction needed"}

    # Net effect: bank reversed wrong amount and posted correct, so the
    # GL needs an adjustment of (correct - wrong) = -diff.
    net_effect = -diff

    je = _je(
        correction_date,
        f"Bank error correction — {description}",
        [
            {"account": "1010",
             "debit": float(net_effect) if net_effect > 0 else 0,
             "credit": float(-net_effect) if net_effect < 0 else 0,
             "memo": f"Bank reversed {wrong} replaced with {correct}"},
            {"account": "5290",
             "debit": float(-net_effect) if net_effect < 0 else 0,
             "credit": float(net_effect) if net_effect > 0 else 0,
             "memo": "Bank-error suspense"},
        ],
    )

    import json as _json
    audit_id = _record_adjustment(
        conn, firm_code=firm_code, client_code=client_code,
        adjustment_type="bank_error_correction", bank_tx_id=wrong_tx_id,
        amount=net_effect, reason=f"Bank error: {description}",
        created_by=created_by, je_payload=_json.dumps({"correction": je}),
    )
    return {
        "audit_id": audit_id,
        "net_effect": float(net_effect),
        "je": je,
        "wrong_amount": float(wrong),
        "correct_amount": float(correct),
    }


def list_recon_adjustments(
    conn: sqlite3.Connection,
    *,
    firm_code: str = "",
    client_code: str = "",
    adjustment_type: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    ensure_edge_tables(conn)
    where: list[str] = []
    params: list[Any] = []
    if firm_code:
        where.append("LOWER(firm_code)=LOWER(?)")
        params.append(firm_code)
    if client_code:
        where.append("LOWER(client_code)=LOWER(?)")
        params.append(client_code)
    if adjustment_type:
        where.append("adjustment_type=?")
        params.append(adjustment_type)
    sql = "SELECT * FROM recon_adjustments"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
