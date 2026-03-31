"""
tests/test_aging.py — AP/AR Aging Engine tests.
"""
from __future__ import annotations

import sqlite3
import pytest

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.aging_engine import (
    calculate_ap_aging,
    calculate_ar_aging,
    create_ar_invoice,
    ensure_ar_invoices_table,
    get_aging_summary,
    list_ar_invoices,
    mark_ar_invoice_paid,
    send_ar_invoice,
)


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


@pytest.fixture
def conn():
    """In-memory SQLite with documents and ar_invoices tables."""
    c = sqlite3.connect(":memory:")
    c.row_factory = _dict_factory

    # Create documents table (simplified)
    c.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            review_status TEXT,
            confidence REAL
        )
    """)

    ensure_ar_invoices_table(c)
    yield c
    c.close()


def _insert_doc(conn, doc_id, client, vendor, amount, date, status="Ready to Post", doc_type="invoice"):
    conn.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, document_date, review_status, doc_type) VALUES (?,?,?,?,?,?,?)",
        (doc_id, client, vendor, amount, date, status, doc_type),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# AP Aging
# ---------------------------------------------------------------------------

class TestAPAging:
    def test_empty(self, conn):
        result = calculate_ap_aging("ACME", "2026-03-31", conn)
        assert result == []

    def test_current_bucket(self, conn):
        _insert_doc(conn, "D1", "ACME", "Vendor A", 1000.0, "2026-03-15")
        result = calculate_ap_aging("ACME", "2026-03-31", conn)

        assert len(result) == 1
        assert result[0]["vendor"] == "Vendor A"
        assert result[0]["current"] == 1000.0
        assert result[0]["total"] == 1000.0

    def test_aging_buckets(self, conn):
        # Current (15 days old)
        _insert_doc(conn, "D1", "ACME", "Vendor A", 100.0, "2026-03-16")
        # 31-60 days
        _insert_doc(conn, "D2", "ACME", "Vendor A", 200.0, "2026-02-15")
        # 61-90 days
        _insert_doc(conn, "D3", "ACME", "Vendor A", 300.0, "2026-01-15")
        # 91-120 days
        _insert_doc(conn, "D4", "ACME", "Vendor A", 400.0, "2025-12-15")
        # 120+ days
        _insert_doc(conn, "D5", "ACME", "Vendor A", 500.0, "2025-10-01")

        result = calculate_ap_aging("ACME", "2026-03-31", conn)

        assert len(result) == 1
        r = result[0]
        assert r["current"] == 100.0
        assert r["days_31_60"] == 200.0
        assert r["days_61_90"] == 300.0
        assert r["days_91_120"] == 400.0
        assert r["over_120"] == 500.0
        assert r["total"] == 1500.0
        assert r["invoice_count"] == 5

    def test_multiple_vendors(self, conn):
        _insert_doc(conn, "D1", "ACME", "Alpha", 100.0, "2026-03-20")
        _insert_doc(conn, "D2", "ACME", "Beta", 200.0, "2026-03-20")
        _insert_doc(conn, "D3", "ACME", "Alpha", 50.0, "2026-03-20")

        result = calculate_ap_aging("ACME", "2026-03-31", conn)

        assert len(result) == 2
        alpha = [r for r in result if r["vendor"] == "Alpha"][0]
        assert alpha["total"] == 150.0
        assert alpha["invoice_count"] == 2

    def test_posted_excluded(self, conn):
        """Posted documents are not AP — they've been paid."""
        _insert_doc(conn, "D1", "ACME", "Vendor", 1000.0, "2026-03-15", status="Posted")
        result = calculate_ap_aging("ACME", "2026-03-31", conn)
        assert result == []

    def test_client_isolation(self, conn):
        _insert_doc(conn, "D1", "ACME", "Vendor", 100.0, "2026-03-15")
        _insert_doc(conn, "D2", "BETA", "Vendor", 200.0, "2026-03-15")

        acme = calculate_ap_aging("ACME", "2026-03-31", conn)
        assert len(acme) == 1
        assert acme[0]["total"] == 100.0


# ---------------------------------------------------------------------------
# AR Aging
# ---------------------------------------------------------------------------

class TestARAging:
    def test_empty(self, conn):
        result = calculate_ar_aging("ACME", "2026-03-31", conn)
        assert result == []

    def test_aging_buckets(self, conn):
        # Current
        create_ar_invoice("ACME", "Client A", "2026-03-10", "2026-04-09", 1000, conn=conn)
        # Older — mark as sent so it's included
        inv2 = create_ar_invoice("ACME", "Client A", "2026-01-15", "2026-02-14", 2000, conn=conn)
        conn.execute("UPDATE ar_invoices SET status = 'sent' WHERE invoice_id = ?", (inv2["invoice_id"],))
        # Draft invoice — should be excluded from aging
        create_ar_invoice("ACME", "Client B", "2026-03-01", "2026-03-31", 500, conn=conn)
        conn.commit()

        # Mark first as sent
        conn.execute("UPDATE ar_invoices SET status = 'sent' WHERE client_code = 'ACME' AND customer_name = 'Client A' AND total_amount = 1000")
        conn.commit()

        result = calculate_ar_aging("ACME", "2026-03-31", conn)

        # Only Client A (both invoices are sent), Client B is draft
        assert len(result) == 1
        assert result[0]["customer"] == "Client A"
        assert result[0]["total"] == 3000.0

    def test_paid_excluded(self, conn):
        inv = create_ar_invoice("ACME", "Client A", "2026-03-01", "2026-03-31", 1000, conn=conn)
        mark_ar_invoice_paid(inv["invoice_id"], "2026-03-15", conn=conn)

        result = calculate_ar_aging("ACME", "2026-03-31", conn)
        assert result == []


# ---------------------------------------------------------------------------
# AR Invoice CRUD
# ---------------------------------------------------------------------------

class TestARInvoiceCRUD:
    def test_create_invoice(self, conn):
        inv = create_ar_invoice(
            "ACME", "Client X", "2026-03-01", "2026-03-31",
            1000, gst_amount=50, qst_amount=99.75,
            customer_email="client@example.com",
            conn=conn,
        )

        assert inv["invoice_id"].startswith("ARINV-")
        assert inv["total_amount"] == 1149.75
        assert inv["status"] == "draft"
        assert inv["invoice_number"].startswith("INV-")

    def test_auto_invoice_number(self, conn):
        inv1 = create_ar_invoice("ACME", "C1", "2026-01-01", "2026-01-31", 100, conn=conn)
        inv2 = create_ar_invoice("ACME", "C2", "2026-02-01", "2026-02-28", 200, conn=conn)

        assert inv1["invoice_number"] == "INV-2026-001"
        assert inv2["invoice_number"] == "INV-2026-002"

    def test_mark_paid_full(self, conn):
        inv = create_ar_invoice("ACME", "Client", "2026-03-01", "2026-03-31", 500, conn=conn)
        result = mark_ar_invoice_paid(inv["invoice_id"], "2026-03-20", conn=conn)

        assert result["status"] == "paid"
        assert result["amount_paid"] == 500.0

    def test_mark_paid_partial(self, conn):
        inv = create_ar_invoice("ACME", "Client", "2026-03-01", "2026-03-31", 500, conn=conn)
        result = mark_ar_invoice_paid(inv["invoice_id"], "2026-03-20", amount_paid=200, conn=conn)

        assert result["status"] == "partial"
        assert result["amount_paid"] == 200.0

    def test_send_invoice(self, conn):
        inv = create_ar_invoice("ACME", "Client", "2026-03-01", "2026-03-31", 500, conn=conn)
        result = send_ar_invoice(inv["invoice_id"], conn)

        assert result["status"] == "sent"

    def test_list_invoices(self, conn):
        create_ar_invoice("ACME", "C1", "2026-01-01", "2026-01-31", 100, conn=conn)
        create_ar_invoice("ACME", "C2", "2026-02-01", "2026-02-28", 200, conn=conn)
        create_ar_invoice("BETA", "C3", "2026-03-01", "2026-03-31", 300, conn=conn)

        acme = list_ar_invoices("ACME", conn)
        assert len(acme) == 2

        beta = list_ar_invoices("BETA", conn)
        assert len(beta) == 1

    def test_not_found_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            mark_ar_invoice_paid("NONEXISTENT", "2026-03-01", conn=conn)

        with pytest.raises(ValueError, match="not found"):
            send_ar_invoice("NONEXISTENT", conn)


# ---------------------------------------------------------------------------
# Aging Summary
# ---------------------------------------------------------------------------

class TestAgingSummary:
    def test_summary(self, conn):
        _insert_doc(conn, "D1", "ACME", "Vendor", 1000.0, "2026-03-15")
        _insert_doc(conn, "D2", "ACME", "Vendor", 500.0, "2025-12-01")  # >90 days

        summary = get_aging_summary("ACME", "2026-03-31", conn)

        assert summary["ap_total"] == 1500.0
        assert summary["ap_over_90"] == 500.0
        assert summary["ar_total"] == 0.0
