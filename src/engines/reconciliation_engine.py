"""
src/engines/reconciliation_engine.py
=====================================
Bank reconciliation engine for LedgerLink.

Provides functions to create, populate, calculate, and finalize bank
reconciliations.  Generates professional bilingual PDF reports.

No AI calls — purely deterministic logic.
"""
from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

# Tolerance for balanced check ($0.01)
BALANCE_TOLERANCE = 0.01

# P1-1: Maximum plausible reconciliation item amount
MAX_ITEM_AMOUNT = Decimal("10000000")  # $10,000,000

CENT = Decimal("0.01")


def _D(v: Any) -> Decimal:
    """Convert any value to Decimal safely."""
    if isinstance(v, Decimal):
        return v
    if v is None or str(v).strip() == "":
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


class DuplicateItemError(Exception):
    """Raised when a duplicate reconciliation item is detected."""
    pass


class ImplausibleAmountError(Exception):
    """Raised when a reconciliation item amount is implausible."""
    pass


class NegativeAmountError(Exception):
    """Raised when a negative amount is used for an item type that requires positive."""
    pass


class FinalizedReconciliationError(Exception):
    """Raised when attempting to modify a finalized reconciliation."""
    pass


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def ensure_reconciliation_tables(conn: sqlite3.Connection) -> None:
    """Create reconciliation tables if they don't exist."""
    # P1-5: Always enforce foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bank_reconciliations (
            reconciliation_id       TEXT PRIMARY KEY,
            client_code             TEXT NOT NULL,
            account_name            TEXT NOT NULL,
            account_number          TEXT,
            period_end_date         TEXT NOT NULL,
            statement_ending_balance TEXT NOT NULL,
            gl_ending_balance       TEXT NOT NULL,
            deposits_in_transit     TEXT DEFAULT '[]',
            outstanding_cheques     TEXT DEFAULT '[]',
            bank_errors             TEXT DEFAULT '[]',
            book_errors             TEXT DEFAULT '[]',
            adjusted_bank_balance   TEXT,
            adjusted_book_balance   TEXT,
            difference              TEXT,
            status                  TEXT NOT NULL DEFAULT 'open',
            prepared_by             TEXT,
            reviewed_by             TEXT,
            prepared_at             TEXT,
            reviewed_at             TEXT,
            finalized_at            TEXT,
            notes                   TEXT
        );

        CREATE TABLE IF NOT EXISTS reconciliation_items (
            item_id             TEXT PRIMARY KEY,
            reconciliation_id   TEXT NOT NULL,
            item_type           TEXT NOT NULL,
            description         TEXT NOT NULL,
            amount              TEXT NOT NULL,
            transaction_date    TEXT,
            cleared_date        TEXT,
            document_id         TEXT,
            status              TEXT NOT NULL DEFAULT 'outstanding',
            FOREIGN KEY (reconciliation_id) REFERENCES bank_reconciliations(reconciliation_id)
        );

        CREATE INDEX IF NOT EXISTS idx_recon_items_recon
            ON reconciliation_items(reconciliation_id);

        -- P1-4: Prevent modification of finalized reconciliations
        CREATE TRIGGER IF NOT EXISTS trg_recon_finalized_insert
        BEFORE INSERT ON reconciliation_items
        WHEN (SELECT finalized_at FROM bank_reconciliations
              WHERE reconciliation_id = NEW.reconciliation_id) IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation is finalized and immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_recon_finalized_update
        BEFORE UPDATE ON reconciliation_items
        WHEN (SELECT finalized_at FROM bank_reconciliations
              WHERE reconciliation_id = OLD.reconciliation_id) IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation is finalized and immutable');
        END;
    """)
    conn.commit()
    # Ensure finalized_at column exists (migration for existing DBs)
    try:
        conn.execute("SELECT finalized_at FROM bank_reconciliations LIMIT 0")
    except sqlite3.OperationalError:
        try:
            conn.execute("ALTER TABLE bank_reconciliations ADD COLUMN finalized_at TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass


# ---------------------------------------------------------------------------
# ID helpers
# ---------------------------------------------------------------------------

def _new_reconciliation_id() -> str:
    return "recon_" + secrets.token_hex(6)


def _new_item_id() -> str:
    return "ri_" + secrets.token_hex(6)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def create_reconciliation(
    client_code: str,
    account_name: str,
    period_end_date: str,
    statement_balance: float,
    gl_balance: float,
    conn: sqlite3.Connection,
    account_number: str = "",
    prepared_by: str = "",
) -> str:
    """Create a new bank reconciliation and return its ID."""
    ensure_reconciliation_tables(conn)
    recon_id = _new_reconciliation_id()
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO bank_reconciliations (
            reconciliation_id, client_code, account_name, account_number,
            period_end_date, statement_ending_balance, gl_ending_balance,
            status, prepared_by, prepared_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
        """,
        (recon_id, client_code, account_name, account_number,
         period_end_date, statement_balance, gl_balance,
         prepared_by, now),
    )
    conn.commit()
    return recon_id


def add_reconciliation_item(
    reconciliation_id: str,
    item_type: str,
    description: str,
    amount: float | Decimal,
    transaction_date: str,
    conn: sqlite3.Connection,
    document_id: str = "",
) -> str:
    """Add a reconciliation item and return its ID.

    Validations:
    - P1-1: Reject amounts > $10,000,000 (implausible_amount)
    - P1-1: Reject negative amounts for deposit_in_transit and outstanding_cheque
    - P1-1: Allow negative amounts only for bank_error and book_error
    - P1-3: Reject duplicate items (same type, description, amount within $0.01)
    - P1-4: Reject items on finalized reconciliations
    """
    ensure_reconciliation_tables(conn)
    amt = _D(amount)

    # P1-1: Reject implausible amounts
    if abs(amt) > MAX_ITEM_AMOUNT:
        raise ImplausibleAmountError(
            f"Amount ${amt:,.2f} exceeds maximum of ${MAX_ITEM_AMOUNT:,.0f}"
        )

    # P1-1: Reject negative amounts for types that must be positive
    positive_only_types = {"deposit_in_transit", "outstanding_cheque"}
    negative_allowed_types = {"bank_error", "book_error"}
    if amt < 0 and item_type in positive_only_types:
        raise NegativeAmountError(
            f"Negative amount not allowed for {item_type}"
        )
    if amt < 0 and item_type not in negative_allowed_types:
        raise NegativeAmountError(
            f"Negative amount not allowed for {item_type}"
        )

    # P1-4: Check finalized
    recon = conn.execute(
        "SELECT finalized_at FROM bank_reconciliations WHERE reconciliation_id = ?",
        (reconciliation_id,),
    ).fetchone()
    if recon and recon["finalized_at"]:
        raise FinalizedReconciliationError(
            "reconciliation is finalized and immutable"
        )

    # P1-3: Duplicate detection
    existing = conn.execute(
        """
        SELECT item_id, amount FROM reconciliation_items
        WHERE reconciliation_id = ? AND item_type = ? AND description = ?
          AND status = 'outstanding'
        """,
        (reconciliation_id, item_type, description),
    ).fetchall()
    for row in existing:
        existing_amt = _D(row["amount"])
        if abs(existing_amt - amt) <= CENT:
            raise DuplicateItemError(
                "Élément en double détecté / Duplicate item detected"
            )

    item_id = _new_item_id()
    conn.execute(
        """
        INSERT INTO reconciliation_items (
            item_id, reconciliation_id, item_type, description,
            amount, transaction_date, document_id, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'outstanding')
        """,
        (item_id, reconciliation_id, item_type, description,
         str(amt), transaction_date, document_id or None),
    )
    conn.commit()
    # P1-2: Always recompute status after adding item
    calculate_reconciliation(reconciliation_id, conn)
    return item_id


def calculate_reconciliation(
    reconciliation_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Calculate both sides of the reconciliation and update the DB.

    Returns a dict with bank_side, book_side, difference, is_balanced.
    All arithmetic uses Decimal (P2-2).
    P1-2: Always recompute status — set to open if difference != 0.
    """
    ensure_reconciliation_tables(conn)
    recon = conn.execute(
        "SELECT * FROM bank_reconciliations WHERE reconciliation_id = ?",
        (reconciliation_id,),
    ).fetchone()
    if not recon:
        return {"error": "Reconciliation not found"}

    statement_balance = _D(recon["statement_ending_balance"])
    gl_balance = _D(recon["gl_ending_balance"])

    # Load outstanding items (only those not cleared/voided)
    items = conn.execute(
        "SELECT * FROM reconciliation_items WHERE reconciliation_id = ? AND status = 'outstanding'",
        (reconciliation_id,),
    ).fetchall()

    deposits_in_transit = Decimal("0")
    outstanding_cheques = Decimal("0")
    bank_errors = Decimal("0")
    bank_charges = Decimal("0")
    interest_earned = Decimal("0")
    book_errors = Decimal("0")

    dit_list: list[dict] = []
    oc_list: list[dict] = []
    be_list: list[dict] = []
    bke_list: list[dict] = []

    for item in items:
        it = item["item_type"]
        amt = _D(item["amount"])
        item_dict = {
            "item_id": item["item_id"],
            "description": item["description"],
            "amount": float(_round(amt)),
            "date": item["transaction_date"],
        }
        if it == "deposit_in_transit":
            deposits_in_transit += amt
            dit_list.append(item_dict)
        elif it == "outstanding_cheque":
            outstanding_cheques += amt
            oc_list.append(item_dict)
        elif it == "bank_error":
            bank_errors += amt
            be_list.append(item_dict)
        elif it == "book_error":
            book_errors += amt
            bke_list.append(item_dict)
        elif it == "bank_charge":
            bank_charges += amt
        elif it == "interest_earned":
            interest_earned += amt

    adjusted_bank = _round(
        statement_balance + deposits_in_transit - outstanding_cheques + bank_errors
    )
    adjusted_book = _round(
        gl_balance - bank_charges + interest_earned + book_errors
    )
    difference = _round(adjusted_bank - adjusted_book)
    is_balanced = abs(difference) <= BALANCE_TOLERANCE

    # P1-2: Always recompute status based on current balance
    if is_balanced:
        new_status = "balanced"
    else:
        new_status = "open"

    conn.execute(
        """
        UPDATE bank_reconciliations SET
            deposits_in_transit = ?,
            outstanding_cheques = ?,
            bank_errors = ?,
            book_errors = ?,
            adjusted_bank_balance = ?,
            adjusted_book_balance = ?,
            difference = ?,
            status = ?
        WHERE reconciliation_id = ?
        """,
        (
            json.dumps(dit_list),
            json.dumps(oc_list),
            json.dumps(be_list),
            json.dumps(bke_list),
            str(adjusted_bank),
            str(adjusted_book),
            str(difference),
            new_status,
            reconciliation_id,
        ),
    )
    conn.commit()

    return {
        "bank_side": {
            "statement_balance": float(statement_balance),
            "deposits_in_transit": float(_round(deposits_in_transit)),
            "outstanding_cheques": float(_round(outstanding_cheques)),
            "bank_errors": float(_round(bank_errors)),
            "adjusted_bank_balance": float(adjusted_bank),
        },
        "book_side": {
            "gl_balance": float(gl_balance),
            "bank_charges": float(_round(bank_charges)),
            "interest_earned": float(_round(interest_earned)),
            "book_errors": float(_round(book_errors)),
            "adjusted_book_balance": float(adjusted_book),
        },
        "difference": float(difference),
        "is_balanced": is_balanced,
    }


def auto_populate_outstanding_items(
    reconciliation_id: str,
    conn: sqlite3.Connection,
) -> int:
    """Scan bank_transactions for unmatched items and add them.

    Returns the number of items added.
    """
    ensure_reconciliation_tables(conn)
    recon = conn.execute(
        "SELECT * FROM bank_reconciliations WHERE reconciliation_id = ?",
        (reconciliation_id,),
    ).fetchone()
    if not recon:
        return 0

    client_code = recon["client_code"]
    period_end = recon["period_end_date"]

    # Find unmatched bank transactions for this client up to period end
    rows = conn.execute(
        """
        SELECT bt.* FROM bank_transactions bt
        JOIN bank_statements bs ON bs.statement_id = bt.statement_id
        WHERE bs.client_code = ?
          AND bt.txn_date <= ?
          AND (bt.matched_document_id IS NULL OR bt.matched_document_id = '')
          AND bt.match_reason = 'no_matching_invoice'
        """,
        (client_code, period_end),
    ).fetchall()

    added = 0
    for row in rows:
        desc = row["description"] or "Bank transaction"
        txn_date = row["txn_date"] or ""
        debit = row["debit"]
        credit = row["credit"]

        # Detect bank charges (frais bancaires)
        desc_lower = desc.lower()
        is_bank_charge = any(kw in desc_lower for kw in [
            "frais bancaires", "frais de service", "bank charge",
            "service charge", "bank fee", "monthly fee",
            "frais mensuels", "frais d'administration",
        ])

        if is_bank_charge and debit:
            add_reconciliation_item(
                reconciliation_id, "bank_charge", desc,
                debit, txn_date, conn, document_id=row["document_id"],
            )
            added += 1
        elif debit:
            # Outstanding cheque / payment
            add_reconciliation_item(
                reconciliation_id, "outstanding_cheque", desc,
                debit, txn_date, conn, document_id=row["document_id"],
            )
            added += 1
        elif credit:
            # Deposit in transit
            add_reconciliation_item(
                reconciliation_id, "deposit_in_transit", desc,
                credit, txn_date, conn, document_id=row["document_id"],
            )
            added += 1

    return added


def mark_item_cleared(
    item_id: str,
    cleared_date: str,
    conn: sqlite3.Connection,
) -> bool:
    """Mark a reconciliation item as cleared. Returns True on success."""
    ensure_reconciliation_tables(conn)
    row = conn.execute(
        "SELECT reconciliation_id FROM reconciliation_items WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    if not row:
        return False

    conn.execute(
        "UPDATE reconciliation_items SET status = 'cleared', cleared_date = ? WHERE item_id = ?",
        (cleared_date, item_id),
    )
    conn.commit()
    # Recalculate
    calculate_reconciliation(row["reconciliation_id"], conn)
    return True


def finalize_reconciliation(
    reconciliation_id: str,
    reviewed_by: str,
    conn: sqlite3.Connection,
) -> bool:
    """Finalize and lock a reconciliation if balanced. Returns True on success.

    P1-4: Sets finalized_at to prevent further modifications.
    """
    ensure_reconciliation_tables(conn)
    result = calculate_reconciliation(reconciliation_id, conn)
    if result.get("error") or not result.get("is_balanced"):
        return False

    now = _utc_now_iso()
    conn.execute(
        """
        UPDATE bank_reconciliations SET
            status = 'balanced',
            reviewed_by = ?,
            reviewed_at = ?,
            finalized_at = ?
        WHERE reconciliation_id = ?
        """,
        (reviewed_by, now, now, reconciliation_id),
    )
    conn.commit()
    return True


def _coerce_recon_amounts(d: dict[str, Any]) -> dict[str, Any]:
    """Convert TEXT-stored amounts back to float for API compatibility."""
    _AMOUNT_KEYS = {
        "statement_ending_balance", "gl_ending_balance",
        "adjusted_bank_balance", "adjusted_book_balance", "difference",
        "amount",
    }
    for k in _AMOUNT_KEYS:
        if k in d and isinstance(d[k], str):
            try:
                d[k] = float(Decimal(d[k]))
            except Exception:
                pass
    return d


def get_reconciliation(
    reconciliation_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any] | None:
    """Load a reconciliation by ID."""
    ensure_reconciliation_tables(conn)
    row = conn.execute(
        "SELECT * FROM bank_reconciliations WHERE reconciliation_id = ?",
        (reconciliation_id,),
    ).fetchone()
    return _coerce_recon_amounts(dict(row)) if row else None


def get_reconciliation_items(
    reconciliation_id: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Load all items for a reconciliation."""
    ensure_reconciliation_tables(conn)
    rows = conn.execute(
        "SELECT * FROM reconciliation_items WHERE reconciliation_id = ? ORDER BY item_type, transaction_date",
        (reconciliation_id,),
    ).fetchall()
    return [_coerce_recon_amounts(dict(r)) for r in rows]


def list_reconciliations(
    conn: sqlite3.Connection,
    client_code: str = "",
    period: str = "",
) -> list[dict[str, Any]]:
    """List reconciliations, optionally filtered by client and/or period."""
    ensure_reconciliation_tables(conn)
    sql = "SELECT * FROM bank_reconciliations WHERE 1=1"
    params: list[Any] = []
    if client_code:
        sql += " AND client_code = ?"
        params.append(client_code)
    if period:
        sql += " AND period_end_date LIKE ?"
        params.append(f"{period}%")
    sql += " ORDER BY period_end_date DESC, client_code"
    rows = conn.execute(sql, params).fetchall()
    return [_coerce_recon_amounts(dict(r)) for r in rows]


def get_reconciliation_summary(
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Return reconciliation status summary for analytics."""
    ensure_reconciliation_tables(conn)
    now = datetime.now(timezone.utc)
    month_start = now.strftime("%Y-%m-01")

    open_recons = conn.execute(
        "SELECT DISTINCT client_code FROM bank_reconciliations WHERE status = 'open'"
    ).fetchall()
    open_clients = [r["client_code"] for r in open_recons]

    balanced_recons = conn.execute(
        "SELECT DISTINCT client_code FROM bank_reconciliations WHERE status = 'balanced' AND prepared_at >= ?",
        (month_start,),
    ).fetchall()
    balanced_clients = [r["client_code"] for r in balanced_recons]

    # All clients from documents
    all_clients_rows = conn.execute(
        "SELECT DISTINCT client_code FROM documents WHERE client_code IS NOT NULL AND client_code != ''"
    ).fetchall()
    all_clients = {r["client_code"] for r in all_clients_rows}

    # Clients with any reconciliation this month
    recon_clients_this_month = conn.execute(
        "SELECT DISTINCT client_code FROM bank_reconciliations WHERE prepared_at >= ?",
        (month_start,),
    ).fetchall()
    recon_client_set = {r["client_code"] for r in recon_clients_this_month}
    at_risk = sorted(all_clients - recon_client_set)

    # Average days to complete
    completed = conn.execute(
        """
        SELECT prepared_at, reviewed_at FROM bank_reconciliations
        WHERE reviewed_at IS NOT NULL AND prepared_at IS NOT NULL
        """
    ).fetchall()
    avg_days = None
    if completed:
        total_days = 0.0
        count = 0
        for row in completed:
            try:
                prep = datetime.fromisoformat(row["prepared_at"])
                rev = datetime.fromisoformat(row["reviewed_at"])
                total_days += (rev - prep).total_seconds() / 86400
                count += 1
            except Exception:
                continue
        if count > 0:
            avg_days = round(total_days / count, 1)

    return {
        "open_clients": open_clients,
        "balanced_clients": balanced_clients,
        "at_risk_clients": at_risk,
        "avg_days_to_complete": avg_days,
    }


# ---------------------------------------------------------------------------
# PDF Generation
# ---------------------------------------------------------------------------

def generate_reconciliation_pdf(
    reconciliation_id: str,
    language: str,
    conn: sqlite3.Connection,
) -> bytes:
    """Generate a professional bank reconciliation PDF.

    Uses fpdf2 for PDF generation.
    """
    try:
        from fpdf import FPDF  # type: ignore
    except ImportError:
        # Fallback: return a simple text-based "PDF"
        return _generate_text_pdf(reconciliation_id, language, conn)

    recon = get_reconciliation(reconciliation_id, conn)
    if not recon:
        return b""

    items = get_reconciliation_items(reconciliation_id, conn)
    result = calculate_reconciliation(reconciliation_id, conn)

    is_fr = language == "fr"

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)

    title = "Rapprochement bancaire" if is_fr else "Bank Reconciliation"
    pdf.cell(0, 10, title, ln=True, align="C")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    client_label = "Client" if is_fr else "Client"
    account_label = "Compte" if is_fr else "Account"
    period_label = "Période se terminant le" if is_fr else "Period ending"
    status_label = "Statut" if is_fr else "Status"

    pdf.cell(0, 6, f"{client_label}: {recon['client_code']}", ln=True)
    pdf.cell(0, 6, f"{account_label}: {recon['account_name']} ({recon.get('account_number') or ''})", ln=True)
    pdf.cell(0, 6, f"{period_label}: {recon['period_end_date']}", ln=True)
    pdf.cell(0, 6, f"{status_label}: {recon['status'].upper()}", ln=True)
    pdf.ln(6)

    bank_side = result.get("bank_side", {})
    book_side = result.get("book_side", {})

    # --- Bank Side ---
    bank_title = "Côté banque" if is_fr else "Bank Side"
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, bank_title, ln=True)
    pdf.set_font("Helvetica", "", 10)

    stmt_label = "Solde du relevé" if is_fr else "Statement balance"
    pdf.cell(120, 6, stmt_label)
    pdf.cell(0, 6, f"${bank_side.get('statement_balance', 0):,.2f}", ln=True, align="R")

    # Deposits in transit
    dit_items = [i for i in items if i["item_type"] == "deposit_in_transit" and i["status"] == "outstanding"]
    if dit_items:
        dit_label = "Dépôts en circulation" if is_fr else "Deposits in transit"
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 5, f"  {dit_label}:", ln=True)
        for it in dit_items:
            pdf.cell(120, 5, f"    {it['description']} ({it.get('transaction_date', '')})")
            pdf.cell(0, 5, f"${it['amount']:,.2f}", ln=True, align="R")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(120, 6, f"  + {dit_label}")
        pdf.cell(0, 6, f"${bank_side.get('deposits_in_transit', 0):,.2f}", ln=True, align="R")

    # Outstanding cheques
    oc_items = [i for i in items if i["item_type"] == "outstanding_cheque" and i["status"] == "outstanding"]
    if oc_items:
        oc_label = "Chèques en circulation" if is_fr else "Outstanding cheques"
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 5, f"  {oc_label}:", ln=True)
        for it in oc_items:
            pdf.cell(120, 5, f"    {it['description']} ({it.get('transaction_date', '')})")
            pdf.cell(0, 5, f"${it['amount']:,.2f}", ln=True, align="R")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(120, 6, f"  - {oc_label}")
        pdf.cell(0, 6, f"${bank_side.get('outstanding_cheques', 0):,.2f}", ln=True, align="R")

    pdf.set_draw_color(0, 0, 0)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)
    adj_bank_label = "Solde bancaire ajusté" if is_fr else "Adjusted bank balance"
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(120, 6, adj_bank_label)
    pdf.cell(0, 6, f"${bank_side.get('adjusted_bank_balance', 0):,.2f}", ln=True, align="R")
    pdf.ln(6)

    # --- Book Side ---
    book_title = "Côté livres" if is_fr else "Book Side"
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, book_title, ln=True)
    pdf.set_font("Helvetica", "", 10)

    gl_label = "Solde du grand livre" if is_fr else "GL balance"
    pdf.cell(120, 6, gl_label)
    pdf.cell(0, 6, f"${book_side.get('gl_balance', 0):,.2f}", ln=True, align="R")

    bc_items = [i for i in items if i["item_type"] == "bank_charge" and i["status"] == "outstanding"]
    if bc_items:
        bc_label = "Frais bancaires" if is_fr else "Bank charges"
        pdf.set_font("Helvetica", "I", 9)
        for it in bc_items:
            pdf.cell(120, 5, f"    {it['description']}")
            pdf.cell(0, 5, f"${it['amount']:,.2f}", ln=True, align="R")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(120, 6, f"  - {bc_label}")
        pdf.cell(0, 6, f"${book_side.get('bank_charges', 0):,.2f}", ln=True, align="R")

    ie_items = [i for i in items if i["item_type"] == "interest_earned" and i["status"] == "outstanding"]
    if ie_items:
        ie_label = "Intérêts gagnés" if is_fr else "Interest earned"
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(120, 6, f"  + {ie_label}")
        pdf.cell(0, 6, f"${book_side.get('interest_earned', 0):,.2f}", ln=True, align="R")

    pdf.set_draw_color(0, 0, 0)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)
    adj_book_label = "Solde des livres ajusté" if is_fr else "Adjusted book balance"
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(120, 6, adj_book_label)
    pdf.cell(0, 6, f"${book_side.get('adjusted_book_balance', 0):,.2f}", ln=True, align="R")
    pdf.ln(8)

    # --- Difference ---
    diff = result.get("difference", 0)
    is_balanced = result.get("is_balanced", False)
    diff_label = "Différence" if is_fr else "Difference"
    pdf.set_font("Helvetica", "B", 12)
    if is_balanced:
        pdf.set_text_color(0, 128, 0)
    else:
        pdf.set_text_color(220, 0, 0)
    pdf.cell(120, 8, diff_label)
    pdf.cell(0, 8, f"${diff:,.2f}", ln=True, align="R")
    pdf.set_text_color(0, 0, 0)

    if recon.get("prepared_by"):
        pdf.ln(6)
        pdf.set_font("Helvetica", "", 9)
        prep_label = "Préparé par" if is_fr else "Prepared by"
        pdf.cell(0, 5, f"{prep_label}: {recon['prepared_by']}  ({recon.get('prepared_at', '')})", ln=True)
    if recon.get("reviewed_by"):
        rev_label = "Révisé par" if is_fr else "Reviewed by"
        pdf.cell(0, 5, f"{rev_label}: {recon['reviewed_by']}  ({recon.get('reviewed_at', '')})", ln=True)

    return pdf.output()


# =========================================================================
# FIX 8 — Deposit/credit proportional allocation and linking
# =========================================================================

def _ensure_credit_memo_invoice_link_table(conn: sqlite3.Connection) -> None:
    """Create credit_memo_invoice_link table if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS credit_memo_invoice_link (
            link_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            credit_memo_id       TEXT NOT NULL,
            original_invoice_id  TEXT NOT NULL,
            link_confidence      REAL,
            link_method          TEXT NOT NULL DEFAULT 'auto',
            invoice_number_match INTEGER NOT NULL DEFAULT 0,
            amount_match         INTEGER NOT NULL DEFAULT 0,
            created_at           TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cm_link_credit "
        "ON credit_memo_invoice_link(credit_memo_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cm_link_invoice "
        "ON credit_memo_invoice_link(original_invoice_id)"
    )


def link_deposit_to_invoice(
    deposit_document_id: str,
    invoice_document_id: str,
    allocation_method: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Link a deposit/credit to an invoice with proportional allocation.

    allocation_method:
    - PROPORTIONAL: allocate deposit across invoice lines by value
    - FULL: apply entire deposit to invoice
    - MANUAL: accountant specifies allocation (just records the link)
    """
    valid_methods = {"PROPORTIONAL", "FULL", "MANUAL"}
    if allocation_method not in valid_methods:
        return {"error": f"Invalid allocation_method. Must be one of {valid_methods}."}

    # Load deposit document
    dep_row = conn.execute(
        "SELECT amount, currency FROM documents WHERE document_id = ?",
        (deposit_document_id,),
    ).fetchone()
    if not dep_row:
        return {"error": f"Deposit document '{deposit_document_id}' not found."}

    deposit_amount = _D(dep_row["amount"])

    # Load invoice document
    inv_row = conn.execute(
        "SELECT amount, currency, COALESCE(gst_amount, 0) AS gst, "
        "COALESCE(qst_amount, 0) AS qst, COALESCE(hst_amount, 0) AS hst "
        "FROM documents WHERE document_id = ?",
        (invoice_document_id,),
    ).fetchone()
    if not inv_row:
        return {"error": f"Invoice document '{invoice_document_id}' not found."}

    invoice_amount = _D(inv_row["amount"])
    remaining_balance = _round(invoice_amount - abs(deposit_amount))

    result: dict[str, Any] = {
        "deposit_document_id": deposit_document_id,
        "invoice_document_id": invoice_document_id,
        "deposit_amount": float(abs(deposit_amount)),
        "invoice_amount": float(invoice_amount),
        "allocation_method": allocation_method,
        "remaining_balance": float(remaining_balance),
    }

    if allocation_method == "PROPORTIONAL" and invoice_amount > Decimal("0"):
        # Proportional: allocate deposit across tax components
        ratio = abs(deposit_amount) / invoice_amount
        ratio = min(ratio, Decimal("1"))  # cap at 100%

        gross_gst = _D(inv_row["gst"])
        gross_qst = _D(inv_row["qst"])
        gross_hst = _D(inv_row["hst"])

        allocated_gst = _round(gross_gst * ratio)
        allocated_qst = _round(gross_qst * ratio)
        allocated_hst = _round(gross_hst * ratio)

        remaining_gst = _round(gross_gst - allocated_gst)
        remaining_qst = _round(gross_qst - allocated_qst)
        remaining_hst = _round(gross_hst - allocated_hst)

        result["allocation_ratio"] = float(_round(ratio * Decimal("10000")) / Decimal("10000"))
        result["allocated_gst"] = float(allocated_gst)
        result["allocated_qst"] = float(allocated_qst)
        result["allocated_hst"] = float(allocated_hst)
        result["remaining_gst_claimable"] = float(remaining_gst)
        result["remaining_qst_claimable"] = float(remaining_qst)
        result["remaining_hst_claimable"] = float(remaining_hst)
        result["itc_adjustment"] = float(allocated_gst + allocated_hst)
        result["itr_adjustment"] = float(allocated_qst)

    elif allocation_method == "FULL":
        result["allocation_ratio"] = 1.0
        result["remaining_balance"] = float(max(remaining_balance, Decimal("0")))

    # Mark deposit as allocated
    try:
        conn.execute(
            "UPDATE documents SET deposit_allocated = 1 WHERE document_id = ?",
            (deposit_document_id,),
        )
        conn.commit()
    except Exception:
        pass

    result["reasoning"] = (
        f"Deposit ${abs(deposit_amount):,.2f} linked to invoice ${invoice_amount:,.2f} "
        f"({allocation_method}). Remaining balance: ${remaining_balance:,.2f}."
    )
    return result


def auto_link_credit_memos(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Auto-link credit memos to invoices using OCR-normalized invoice numbers.

    Matches when:
    - Invoice numbers match after OCR normalization
    - Credit amount < original invoice amount
    """
    _ensure_credit_memo_invoice_link_table(conn)

    # Find credit memos
    credit_memos = conn.execute(
        "SELECT document_id, invoice_number, invoice_number_normalized, amount, vendor "
        "FROM documents WHERE doc_type IN ('credit_note', 'credit note', 'refund') "
        "AND invoice_number_normalized IS NOT NULL AND invoice_number_normalized != ''"
    ).fetchall()

    links: list[dict[str, Any]] = []
    for cm in credit_memos:
        cm_norm = cm["invoice_number_normalized"]
        cm_amount = abs(float(cm["amount"] or 0))

        # Find matching invoices
        candidates = conn.execute(
            "SELECT document_id, invoice_number, amount FROM documents "
            "WHERE doc_type IN ('invoice', 'receipt', 'bill') "
            "AND invoice_number_normalized = ? "
            "AND amount IS NOT NULL AND amount > 0",
            (cm_norm,),
        ).fetchall()

        for inv in candidates:
            inv_amount = float(inv["amount"] or 0)
            if cm_amount <= 0 or cm_amount > inv_amount:
                continue

            # Check if already linked
            existing = conn.execute(
                "SELECT link_id FROM credit_memo_invoice_link "
                "WHERE credit_memo_id = ? AND original_invoice_id = ?",
                (cm["document_id"], inv["document_id"]),
            ).fetchone()
            if existing:
                continue

            confidence = 0.90 if cm_amount < inv_amount else 0.95
            conn.execute(
                "INSERT INTO credit_memo_invoice_link "
                "(credit_memo_id, original_invoice_id, link_confidence, link_method, "
                "invoice_number_match, amount_match, created_at) "
                "VALUES (?, ?, ?, 'auto', 1, ?, ?)",
                (cm["document_id"], inv["document_id"], confidence,
                 1 if abs(cm_amount - inv_amount) < 0.01 else 0,
                 _utc_now_iso()),
            )

            links.append({
                "credit_memo_id": cm["document_id"],
                "original_invoice_id": inv["document_id"],
                "invoice_number": cm["invoice_number"],
                "credit_amount": cm_amount,
                "invoice_amount": inv_amount,
                "link_confidence": confidence,
                "link_method": "auto",
            })

    if links:
        conn.commit()
    return links


def _generate_text_pdf(
    reconciliation_id: str,
    language: str,
    conn: sqlite3.Connection,
) -> bytes:
    """Fallback text-based PDF when fpdf2 is not installed."""
    recon = get_reconciliation(reconciliation_id, conn)
    if not recon:
        return b""
    result = calculate_reconciliation(reconciliation_id, conn)
    items = get_reconciliation_items(reconciliation_id, conn)

    is_fr = language == "fr"
    title = "RAPPROCHEMENT BANCAIRE" if is_fr else "BANK RECONCILIATION"

    lines = [
        title,
        "=" * 60,
        f"Client: {recon['client_code']}",
        f"Account: {recon['account_name']}",
        f"Period ending: {recon['period_end_date']}",
        f"Status: {recon['status']}",
        "",
        "--- BANK SIDE ---" if not is_fr else "--- CÔTÉ BANQUE ---",
        f"Statement balance: ${float(_D(recon['statement_ending_balance'])):,.2f}",
    ]

    bank_side = result.get("bank_side", {})
    book_side = result.get("book_side", {})

    dit = bank_side.get("deposits_in_transit", 0)
    oc = bank_side.get("outstanding_cheques", 0)
    lines.append(f"+ Deposits in transit: ${dit:,.2f}")
    lines.append(f"- Outstanding cheques: ${oc:,.2f}")
    lines.append(f"= Adjusted bank balance: ${bank_side.get('adjusted_bank_balance', 0):,.2f}")
    lines.append("")
    lines.append("--- BOOK SIDE ---" if not is_fr else "--- CÔTÉ LIVRES ---")
    lines.append(f"GL balance: ${float(_D(recon['gl_ending_balance'])):,.2f}")
    lines.append(f"- Bank charges: ${book_side.get('bank_charges', 0):,.2f}")
    lines.append(f"+ Interest earned: ${book_side.get('interest_earned', 0):,.2f}")
    lines.append(f"= Adjusted book balance: ${book_side.get('adjusted_book_balance', 0):,.2f}")
    lines.append("")
    lines.append(f"DIFFERENCE: ${result.get('difference', 0):,.2f}")
    lines.append(f"BALANCED: {'YES' if result.get('is_balanced') else 'NO'}")

    return "\n".join(lines).encode("utf-8")
