"""
src/engines/aging_engine.py — Accounts Payable & Receivable Aging Reports.

Provides AP aging (outstanding invoices not yet matched to bank transactions)
and AR aging (outgoing invoices not yet paid).

All monetary arithmetic uses Python Decimal.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
_ZERO = Decimal("0")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None or str(v).strip() == "":
        return _ZERO
    try:
        return Decimal(str(v))
    except Exception:
        return _ZERO


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _days_between(date1: str, date2: str) -> int:
    """Calculate days between two ISO date strings (YYYY-MM-DD)."""
    try:
        d1 = datetime.strptime(date1[:10], "%Y-%m-%d")
        d2 = datetime.strptime(date2[:10], "%Y-%m-%d")
        return abs((d2 - d1).days)
    except (ValueError, TypeError):
        return 0


def _bucket_name(days: int) -> str:
    """Return the aging bucket name for the given number of days."""
    if days <= 30:
        return "current"
    elif days <= 60:
        return "days_31_60"
    elif days <= 90:
        return "days_61_90"
    elif days <= 120:
        return "days_91_120"
    else:
        return "over_120"


# ---------------------------------------------------------------------------
# AR invoices table
# ---------------------------------------------------------------------------

def ensure_ar_invoices_table(conn: sqlite3.Connection) -> None:
    """Create ar_invoices table (idempotent)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ar_invoices (
            invoice_id      TEXT PRIMARY KEY,
            client_code     TEXT NOT NULL,
            customer_name   TEXT NOT NULL,
            customer_email  TEXT,
            invoice_number  TEXT,
            invoice_date    TEXT NOT NULL,
            due_date        TEXT NOT NULL,
            amount_ht       REAL NOT NULL DEFAULT 0,
            gst_amount      REAL NOT NULL DEFAULT 0,
            qst_amount      REAL NOT NULL DEFAULT 0,
            total_amount    REAL NOT NULL DEFAULT 0,
            currency        TEXT NOT NULL DEFAULT 'CAD',
            status          TEXT NOT NULL DEFAULT 'draft',
            amount_paid     REAL NOT NULL DEFAULT 0,
            payment_date    TEXT,
            description     TEXT,
            created_at      TEXT,
            created_by      TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ar_invoices_client
            ON ar_invoices(client_code)
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# AP Aging
# ---------------------------------------------------------------------------

def calculate_ap_aging(
    client_code: str,
    as_of_date: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Calculate accounts payable aging from approved but unmatched invoices.

    Queries documents with review_status in ('Ready to Post', 'Needs Review')
    that have not been matched to a bank transaction (no 'Posted' status).

    Returns list of dicts per vendor with aging bucket totals.
    """
    as_of = as_of_date[:10]

    # Query approved but unmatched invoices (AP = what we owe vendors)
    rows = conn.execute(
        """SELECT vendor, document_date, amount
           FROM documents
           WHERE client_code = ?
             AND review_status IN ('Ready to Post', 'Needs Review', 'On Hold')
             AND doc_type IN ('invoice', 'facture', 'bill', 'expense')
             AND amount IS NOT NULL
             AND document_date IS NOT NULL
           ORDER BY vendor, document_date""",
        (client_code,),
    ).fetchall()

    vendor_data: dict[str, dict[str, Any]] = {}

    for row in rows:
        r = dict(row) if not isinstance(row, dict) else row
        vendor = r.get("vendor") or "Unknown"
        doc_date = str(r.get("document_date", ""))[:10]
        amount = _to_decimal(r.get("amount"))

        if amount <= _ZERO:
            continue

        days = _days_between(doc_date, as_of)
        bucket = _bucket_name(days)

        if vendor not in vendor_data:
            vendor_data[vendor] = {
                "vendor": vendor,
                "invoice_count": 0,
                "current": _ZERO,
                "days_31_60": _ZERO,
                "days_61_90": _ZERO,
                "days_91_120": _ZERO,
                "over_120": _ZERO,
                "total": _ZERO,
            }

        entry = vendor_data[vendor]
        entry["invoice_count"] += 1
        entry[bucket] += amount
        entry["total"] += amount

    # Convert Decimals to float for JSON serialization
    result = []
    for v in sorted(vendor_data.values(), key=lambda x: x["vendor"]):
        result.append({
            "vendor": v["vendor"],
            "invoice_count": v["invoice_count"],
            "current": float(_round(v["current"])),
            "days_31_60": float(_round(v["days_31_60"])),
            "days_61_90": float(_round(v["days_61_90"])),
            "days_91_120": float(_round(v["days_91_120"])),
            "over_120": float(_round(v["over_120"])),
            "total": float(_round(v["total"])),
        })

    return result


# ---------------------------------------------------------------------------
# AR Aging
# ---------------------------------------------------------------------------

def calculate_ar_aging(
    client_code: str,
    as_of_date: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Calculate accounts receivable aging from ar_invoices table.

    Returns list of dicts per customer with aging bucket totals.
    """
    ensure_ar_invoices_table(conn)
    as_of = as_of_date[:10]

    rows = conn.execute(
        """SELECT customer_name, invoice_date, total_amount, amount_paid, status
           FROM ar_invoices
           WHERE client_code = ?
             AND status NOT IN ('paid', 'draft')
           ORDER BY customer_name, invoice_date""",
        (client_code,),
    ).fetchall()

    customer_data: dict[str, dict[str, Any]] = {}

    for row in rows:
        r = dict(row) if not isinstance(row, dict) else row
        customer = r.get("customer_name") or "Unknown"
        inv_date = str(r.get("invoice_date", ""))[:10]
        total = _to_decimal(r.get("total_amount"))
        paid = _to_decimal(r.get("amount_paid"))
        outstanding = total - paid

        if outstanding <= _ZERO:
            continue

        days = _days_between(inv_date, as_of)
        bucket = _bucket_name(days)

        if customer not in customer_data:
            customer_data[customer] = {
                "customer": customer,
                "invoice_count": 0,
                "current": _ZERO,
                "days_31_60": _ZERO,
                "days_61_90": _ZERO,
                "days_91_120": _ZERO,
                "over_120": _ZERO,
                "total": _ZERO,
            }

        entry = customer_data[customer]
        entry["invoice_count"] += 1
        entry[bucket] += outstanding
        entry["total"] += outstanding

    result = []
    for c in sorted(customer_data.values(), key=lambda x: x["customer"]):
        result.append({
            "customer": c["customer"],
            "invoice_count": c["invoice_count"],
            "current": float(_round(c["current"])),
            "days_31_60": float(_round(c["days_31_60"])),
            "days_61_90": float(_round(c["days_61_90"])),
            "days_91_120": float(_round(c["days_91_120"])),
            "over_120": float(_round(c["over_120"])),
            "total": float(_round(c["total"])),
        })

    return result


def get_aging_summary(
    client_code: str,
    as_of_date: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Return summary cards for AP and AR aging."""
    ap = calculate_ap_aging(client_code, as_of_date, conn)
    ar = calculate_ar_aging(client_code, as_of_date, conn)

    ap_total = sum(v["total"] for v in ap)
    ap_over_60 = sum(v["days_61_90"] + v["days_91_120"] + v["over_120"] for v in ap)
    ap_over_90 = sum(v["days_91_120"] + v["over_120"] for v in ap)
    ar_total = sum(v["total"] for v in ar)
    ar_over_60 = sum(v["days_61_90"] + v["days_91_120"] + v["over_120"] for v in ar)

    return {
        "ap_total": round(ap_total, 2),
        "ap_over_60": round(ap_over_60, 2),
        "ap_over_90": round(ap_over_90, 2),
        "ar_total": round(ar_total, 2),
        "ar_over_60": round(ar_over_60, 2),
    }


# ---------------------------------------------------------------------------
# AR Invoice CRUD
# ---------------------------------------------------------------------------

def create_ar_invoice(
    client_code: str,
    customer_name: str,
    invoice_date: str,
    due_date: str,
    amount_ht: float | Decimal | str,
    gst_amount: float | Decimal | str = 0,
    qst_amount: float | Decimal | str = 0,
    customer_email: str = "",
    invoice_number: str = "",
    description: str = "",
    currency: str = "CAD",
    created_by: str = "",
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Create a new AR invoice. Returns the invoice dict."""
    import secrets as _sec

    if conn is None:
        raise ValueError("Connection required")

    ensure_ar_invoices_table(conn)

    amt = _to_decimal(amount_ht)
    gst = _to_decimal(gst_amount)
    qst = _to_decimal(qst_amount)
    total = _round(amt + gst + qst)

    invoice_id = f"ARINV-{_sec.token_hex(6).upper()}"

    # Auto-generate invoice number if not provided
    if not invoice_number:
        year = invoice_date[:4] if invoice_date else "2026"
        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM ar_invoices WHERE client_code = ?",
            (client_code,),
        ).fetchone()
        cnt = (dict(count) if not isinstance(count, dict) else count).get("cnt", 0)
        invoice_number = f"INV-{year}-{cnt + 1:03d}"

    conn.execute(
        """INSERT INTO ar_invoices
           (invoice_id, client_code, customer_name, customer_email,
            invoice_number, invoice_date, due_date, amount_ht,
            gst_amount, qst_amount, total_amount, currency,
            status, amount_paid, description, created_at, created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            invoice_id, client_code, customer_name, customer_email,
            invoice_number, invoice_date, due_date,
            float(_round(amt)), float(_round(gst)), float(_round(qst)),
            float(total), currency,
            "draft", 0.0, description, _utc_now(), created_by,
        ),
    )
    conn.commit()

    return {
        "invoice_id": invoice_id,
        "client_code": client_code,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "due_date": due_date,
        "amount_ht": float(_round(amt)),
        "gst_amount": float(_round(gst)),
        "qst_amount": float(_round(qst)),
        "total_amount": float(total),
        "currency": currency,
        "status": "draft",
        "amount_paid": 0.0,
    }


def mark_ar_invoice_paid(
    invoice_id: str,
    payment_date: str,
    amount_paid: float | Decimal | str | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Mark an AR invoice as paid (full or partial)."""
    if conn is None:
        raise ValueError("Connection required")

    ensure_ar_invoices_table(conn)

    row = conn.execute(
        "SELECT * FROM ar_invoices WHERE invoice_id = ?", (invoice_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Invoice not found: {invoice_id}")

    r = dict(row) if not isinstance(row, dict) else row
    total = _to_decimal(r["total_amount"])

    if amount_paid is None:
        paid = total
    else:
        paid = _to_decimal(amount_paid)

    status = "paid" if paid >= total else "partial"

    conn.execute(
        """UPDATE ar_invoices
           SET status = ?, amount_paid = ?, payment_date = ?
           WHERE invoice_id = ?""",
        (status, float(_round(paid)), payment_date, invoice_id),
    )
    conn.commit()

    return {"invoice_id": invoice_id, "status": status, "amount_paid": float(_round(paid))}


def list_ar_invoices(
    client_code: str,
    conn: sqlite3.Connection,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """List AR invoices for a client."""
    ensure_ar_invoices_table(conn)

    if status:
        rows = conn.execute(
            """SELECT * FROM ar_invoices
               WHERE client_code = ? AND status = ?
               ORDER BY invoice_date DESC""",
            (client_code, status),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM ar_invoices
               WHERE client_code = ?
               ORDER BY invoice_date DESC""",
            (client_code,),
        ).fetchall()

    return [dict(r) if not isinstance(r, dict) else r for r in rows]


def send_ar_invoice(
    invoice_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Mark an AR invoice as sent."""
    ensure_ar_invoices_table(conn)

    row = conn.execute(
        "SELECT * FROM ar_invoices WHERE invoice_id = ?", (invoice_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Invoice not found: {invoice_id}")

    conn.execute(
        "UPDATE ar_invoices SET status = 'sent' WHERE invoice_id = ? AND status = 'draft'",
        (invoice_id,),
    )
    conn.commit()
    return {"invoice_id": invoice_id, "status": "sent"}


def update_overdue_invoices(
    client_code: str,
    as_of_date: str,
    conn: sqlite3.Connection,
) -> int:
    """Mark sent invoices past due_date as overdue. Returns count updated."""
    ensure_ar_invoices_table(conn)

    conn.execute(
        """UPDATE ar_invoices
           SET status = 'overdue'
           WHERE client_code = ? AND status = 'sent' AND due_date < ?""",
        (client_code, as_of_date[:10]),
    )
    conn.commit()
    return conn.execute("SELECT changes()").fetchone().get("changes()", 0) if hasattr(conn.execute("SELECT changes()").fetchone(), "get") else 0
