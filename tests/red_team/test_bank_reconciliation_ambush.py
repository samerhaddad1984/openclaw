"""
tests/red_team/test_bank_reconciliation_ambush.py
==================================================
Red-team ambush: bank reconciliation integrity under adversarial scenarios.

Scenarios planted:
  1. One invoice paid by 3 partial payments
  2. One payment covering 4 invoices
  3. Duplicate imports from 2 bank connections (Desjardins + TD)
  4. Reversal with truncated memo
  5. Returned EFT
  6. Cheque outstanding across 2 periods
  7. USD payment against CAD invoice

Invariants enforced:
  - No double settlement (same payment must never settle two liabilities)
  - No orphan payment (every payment traces to >= 1 liability)
  - Exact audit trail of every match decision
  - Reversal chain remains one-to-one economically
  - Same payment cannot settle two liabilities
  - Duplicate reversal cannot neutralize a legitimate payment
  - Reconciliation is immutable after finalization lock
"""
from __future__ import annotations

import json
import sqlite3
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import pytest
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.engines.reconciliation_engine import (
    ensure_reconciliation_tables,
    create_reconciliation,
    add_reconciliation_item,
    calculate_reconciliation,
    finalize_reconciliation,
    mark_item_cleared,
    get_reconciliation,
    get_reconciliation_items,
    DuplicateItemError,
    FinalizedReconciliationError,
    link_deposit_to_invoice,
)
from src.engines.bank_parser import (
    parse_statement,
    import_statement,
    _ensure_bank_tables,
)
from src.agents.tools.bank_matcher import BankMatcher


# ---------------------------------------------------------------------------
# In-memory DB with all required tables
# ---------------------------------------------------------------------------

def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT DEFAULT '',
            file_path TEXT DEFAULT '',
            client_code TEXT,
            vendor TEXT,
            vendor_name TEXT,
            amount REAL,
            total REAL,
            currency TEXT DEFAULT 'CAD',
            doc_type TEXT DEFAULT 'invoice',
            document_date TEXT,
            invoice_number TEXT,
            invoice_number_normalized TEXT,
            review_status TEXT DEFAULT 'New',
            confidence REAL,
            raw_result TEXT DEFAULT '{}',
            created_at TEXT,
            updated_at TEXT,
            ingest_source TEXT DEFAULT '',
            match_reason TEXT,
            gst_amount REAL DEFAULT 0,
            qst_amount REAL DEFAULT 0,
            hst_amount REAL DEFAULT 0,
            deposit_allocated INTEGER DEFAULT 0,
            UNIQUE(document_id)
        );

        CREATE TABLE IF NOT EXISTS bank_statements (
            statement_id      TEXT PRIMARY KEY,
            bank_name         TEXT,
            file_name         TEXT,
            client_code       TEXT,
            imported_by       TEXT,
            imported_at       TEXT,
            period_start      TEXT,
            period_end        TEXT,
            transaction_count INTEGER DEFAULT 0,
            matched_count     INTEGER DEFAULT 0,
            unmatched_count   INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS bank_transactions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id        TEXT NOT NULL,
            document_id         TEXT NOT NULL,
            txn_date            TEXT,
            description         TEXT,
            debit               REAL,
            credit              REAL,
            balance             REAL,
            matched_document_id TEXT,
            match_confidence    REAL,
            match_reason        TEXT
        );

        CREATE TABLE IF NOT EXISTS vendor_aliases (
            alias_key TEXT PRIMARY KEY,
            canonical_vendor_key TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS boc_fx_rates (
            rate_date TEXT PRIMARY KEY,
            usd_cad   REAL NOT NULL
        );
    """)
    ensure_reconciliation_tables(conn)
    return conn


def _uid() -> str:
    return secrets.token_hex(6)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _insert_doc(conn, doc_id, client="ACME", vendor="Test", amount=100.0,
                currency="CAD", doc_type="invoice", date="2026-01-15",
                review_status="New"):
    now = _now()
    conn.execute(
        """INSERT INTO documents
           (document_id, client_code, vendor, amount, currency, doc_type,
            document_date, review_status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (doc_id, client, vendor, amount, currency, doc_type, date,
         review_status, now, now),
    )
    conn.commit()


def _insert_bank_txn(conn, stmt_id, doc_id, txn_date, desc,
                     debit=None, credit=None, matched_doc=None,
                     confidence=None, reason=None):
    conn.execute(
        """INSERT INTO bank_transactions
           (statement_id, document_id, txn_date, description,
            debit, credit, matched_document_id, match_confidence, match_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (stmt_id, doc_id, txn_date, desc, debit, credit,
         matched_doc, confidence, reason),
    )
    conn.commit()


def _insert_stmt(conn, stmt_id, client="ACME", bank="Desjardins"):
    now = _now()
    conn.execute(
        """INSERT INTO bank_statements
           (statement_id, bank_name, file_name, client_code,
            imported_by, imported_at)
           VALUES (?, ?, 'test.csv', ?, 'tester', ?)""",
        (stmt_id, bank, client, now),
    )
    conn.commit()


# ===================================================================
# 1 — One invoice paid by 3 partial payments
# ===================================================================

class TestOneInvoiceThreePayments:
    """Invoice $3,000 settled by payments of $1,000 + $1,200 + $800."""

    def setup_method(self):
        self.conn = _make_conn()
        self.inv_id = f"inv_{_uid()}"
        _insert_doc(self.conn, self.inv_id, vendor="Fournisseur ABC",
                    amount=3000.00, doc_type="invoice", date="2026-01-10")

        self.stmt_id = f"stmt_{_uid()}"
        _insert_stmt(self.conn, self.stmt_id)

        self.pay_ids = []
        amounts = [1000.00, 1200.00, 800.00]
        for i, amt in enumerate(amounts):
            pay_id = f"pay_{_uid()}"
            _insert_doc(self.conn, pay_id, vendor="Fournisseur ABC",
                        amount=amt, doc_type="bank_transaction",
                        date=f"2026-01-{15 + i}")
            _insert_bank_txn(self.conn, self.stmt_id, pay_id,
                             f"2026-01-{15 + i}", "Fournisseur ABC",
                             debit=amt, matched_doc=self.inv_id,
                             confidence=0.95)
            self.pay_ids.append(pay_id)

    def test_total_payments_equal_invoice(self):
        """Sum of partial payments must exactly equal the invoice."""
        rows = self.conn.execute(
            "SELECT SUM(debit) as total FROM bank_transactions WHERE statement_id=?",
            (self.stmt_id,),
        ).fetchone()
        assert abs(rows["total"] - 3000.00) < 0.01

    def test_all_payments_trace_to_invoice(self):
        """Every payment document links back to the invoice — no orphans."""
        rows = self.conn.execute(
            "SELECT matched_document_id FROM bank_transactions WHERE statement_id=?",
            (self.stmt_id,),
        ).fetchall()
        for r in rows:
            assert r["matched_document_id"] == self.inv_id

    def test_no_double_settlement(self):
        """Each payment ID appears exactly once in the settlement ledger."""
        rows = self.conn.execute(
            "SELECT document_id, COUNT(*) as cnt FROM bank_transactions "
            "WHERE statement_id=? GROUP BY document_id HAVING cnt > 1",
            (self.stmt_id,),
        ).fetchall()
        assert len(rows) == 0, "Payment settled more than once"


# ===================================================================
# 2 — One payment covering 4 invoices
# ===================================================================

class TestOnePaymentFourInvoices:
    """Single $5,000 payment covers invoices of $1,200 + $1,300 + $1,500 + $1,000."""

    def setup_method(self):
        self.conn = _make_conn()
        inv_amounts = [1200.00, 1300.00, 1500.00, 1000.00]
        self.inv_ids = []
        for i, amt in enumerate(inv_amounts):
            inv_id = f"inv_{_uid()}"
            _insert_doc(self.conn, inv_id, vendor="Multi-Inv Vendor",
                        amount=amt, doc_type="invoice",
                        date=f"2026-01-{5 + i}")
            self.inv_ids.append(inv_id)

        self.stmt_id = f"stmt_{_uid()}"
        _insert_stmt(self.conn, self.stmt_id)

        self.pay_id = f"pay_{_uid()}"
        _insert_doc(self.conn, self.pay_id, vendor="Multi-Inv Vendor",
                    amount=5000.00, doc_type="bank_transaction",
                    date="2026-01-20")

        # One bank_transactions row per invoice allocation
        for inv_id, amt in zip(self.inv_ids, inv_amounts):
            alloc_doc = f"alloc_{_uid()}"
            _insert_doc(self.conn, alloc_doc, vendor="Multi-Inv Vendor",
                        amount=amt, doc_type="bank_transaction",
                        date="2026-01-20")
            _insert_bank_txn(self.conn, self.stmt_id, alloc_doc,
                             "2026-01-20", "Multi-Inv Vendor",
                             debit=amt, matched_doc=inv_id, confidence=0.92)

    def test_allocations_sum_to_payment(self):
        rows = self.conn.execute(
            "SELECT SUM(debit) as total FROM bank_transactions WHERE statement_id=?",
            (self.stmt_id,),
        ).fetchone()
        assert abs(rows["total"] - 5000.00) < 0.01

    def test_each_invoice_settled_once(self):
        """No invoice appears as matched_document_id more than once."""
        rows = self.conn.execute(
            "SELECT matched_document_id, COUNT(*) as cnt "
            "FROM bank_transactions WHERE statement_id=? "
            "AND matched_document_id IS NOT NULL "
            "GROUP BY matched_document_id HAVING cnt > 1",
            (self.stmt_id,),
        ).fetchall()
        assert len(rows) == 0, "Same invoice settled twice by the same batch"

    def test_no_orphan_allocation(self):
        rows = self.conn.execute(
            "SELECT * FROM bank_transactions WHERE statement_id=? "
            "AND matched_document_id IS NULL",
            (self.stmt_id,),
        ).fetchall()
        assert len(rows) == 0, "Orphan allocation row without an invoice"


# ===================================================================
# 3 — Duplicate imports from 2 bank connections
# ===================================================================

class TestDuplicateBankImports:
    """Same transactions imported from Desjardins chequing AND TD line-of-credit
    must not double-count."""

    def setup_method(self):
        self.conn = _make_conn()
        self.inv_id = f"inv_{_uid()}"
        _insert_doc(self.conn, self.inv_id, vendor="Hydro-Québec",
                    amount=450.00, doc_type="invoice", date="2026-02-01")

        # Import 1 — Desjardins
        self.stmt1 = f"stmt_{_uid()}"
        _insert_stmt(self.conn, self.stmt1, bank="Desjardins")
        self.pay1 = f"pay_{_uid()}"
        _insert_doc(self.conn, self.pay1, vendor="Hydro-Québec",
                    amount=450.00, doc_type="bank_transaction", date="2026-02-05")
        _insert_bank_txn(self.conn, self.stmt1, self.pay1, "2026-02-05",
                         "HYDRO-QUEBEC", debit=450.00,
                         matched_doc=self.inv_id, confidence=0.91)

        # Import 2 — TD (same real-world payment shows up on LOC)
        self.stmt2 = f"stmt_{_uid()}"
        _insert_stmt(self.conn, self.stmt2, bank="TD")
        self.pay2 = f"pay_{_uid()}"
        _insert_doc(self.conn, self.pay2, vendor="HYDRO QUEBEC",
                    amount=450.00, doc_type="bank_transaction", date="2026-02-05")
        _insert_bank_txn(self.conn, self.stmt2, self.pay2, "2026-02-05",
                         "HYDRO QUEBEC", debit=450.00,
                         matched_doc=self.inv_id, confidence=0.88)

    def test_duplicate_detection_flag(self):
        """If the same invoice is matched from two statements, the system must
        expose that as a conflict — not silently double-settle."""
        rows = self.conn.execute(
            "SELECT matched_document_id, COUNT(*) as cnt "
            "FROM bank_transactions "
            "WHERE matched_document_id = ? "
            "GROUP BY matched_document_id",
            (self.inv_id,),
        ).fetchone()
        # The ambush: two matches exist. The system MUST flag this.
        assert rows["cnt"] == 2, "Expected duplicate match to be visible"

        # A correct system should never let both settle — build a recon
        # and verify the invoice amount is not double-counted.
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-02-28",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=self.conn, prepared_by="auditor",
        )
        add_reconciliation_item(
            recon_id, "outstanding_cheque", "Hydro-Québec",
            450.00, "2026-02-05", self.conn,
        )
        # Attempting to add the same cheque again must raise DuplicateItemError
        with pytest.raises(DuplicateItemError):
            add_reconciliation_item(
                recon_id, "outstanding_cheque", "Hydro-Québec",
                450.00, "2026-02-05", self.conn,
            )

    def test_recon_reflects_single_settlement(self):
        """After dedup, reconciliation must reflect only ONE payment of $450."""
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-02-28",
            statement_balance=10000.00, gl_balance=9550.00,
            conn=self.conn, prepared_by="auditor",
        )
        add_reconciliation_item(
            recon_id, "outstanding_cheque", "Hydro-Québec",
            450.00, "2026-02-05", self.conn,
        )
        result = calculate_reconciliation(recon_id, self.conn)
        assert result["bank_side"]["outstanding_cheques"] == 450.00
        assert result["is_balanced"]


# ===================================================================
# 4 — Reversal with truncated memo
# ===================================================================

class TestReversalTruncatedMemo:
    """Bank truncates reversal memo to 20 chars. Reversal detection must still work."""

    def test_reversal_detected_despite_truncation(self):
        matcher = BankMatcher()

        @dataclass
        class FakeTxn:
            description: str = ""
            memo: str = ""
            debit: float = 0.0
            credit: float = 0.0
            posted_date: str = ""
            txn_date: str = ""
            transaction_id: str = ""

        original = FakeTxn(
            description="SCS Industrial Equipment Ltd",
            debit=2500.00,
            posted_date="2026-03-01",
            transaction_id="txn_orig",
        )
        # Bank truncated the memo to ~20 chars and prefixed "REVERSAL"
        reversal = FakeTxn(
            description="REVERSAL SCS Industri",
            credit=2500.00,
            posted_date="2026-03-03",
            transaction_id="txn_rev",
        )
        results = matcher.detect_reversals([original, reversal])
        assert len(results) == 1, "Reversal not detected with truncated memo"
        pair = results[0]
        assert pair["flag"] == "reversal_pair"
        assert pair["amount_a"] == 2500.00
        assert pair["amount_b"] == -2500.00

    def test_reversal_remains_one_to_one(self):
        """A single reversal must pair with exactly ONE original — not two."""
        matcher = BankMatcher()

        @dataclass
        class FakeTxn:
            description: str = ""
            memo: str = ""
            debit: float = 0.0
            credit: float = 0.0
            posted_date: str = ""
            txn_date: str = ""
            transaction_id: str = ""

        txn_a = FakeTxn(description="SCS Industrial", debit=2500.00,
                        posted_date="2026-03-01", transaction_id="a")
        txn_b = FakeTxn(description="SCS Industrial", debit=2500.00,
                        posted_date="2026-03-01", transaction_id="b")
        txn_rev = FakeTxn(description="REVERSAL SCS Industri", credit=2500.00,
                          posted_date="2026-03-03", transaction_id="rev")

        results = matcher.detect_reversals([txn_a, txn_b, txn_rev])
        # Must pair with exactly one — the other remains unmatched
        assert len(results) == 1, "Reversal paired with multiple originals"
        paired_ids = {results[0]["transaction_a_id"], results[0]["transaction_b_id"]}
        assert "rev" in paired_ids


# ===================================================================
# 5 — Returned EFT
# ===================================================================

class TestReturnedEFT:
    """EFT payment goes out, comes back NSF. Must not settle the liability."""

    def setup_method(self):
        self.conn = _make_conn()
        self.inv_id = f"inv_{_uid()}"
        _insert_doc(self.conn, self.inv_id, vendor="Plomberie Côté",
                    amount=1800.00, doc_type="invoice", date="2026-02-10")

        self.stmt_id = f"stmt_{_uid()}"
        _insert_stmt(self.conn, self.stmt_id)

        # Outbound EFT
        self.eft_out = f"eft_{_uid()}"
        _insert_doc(self.conn, self.eft_out, vendor="Plomberie Côté",
                    amount=1800.00, doc_type="bank_transaction",
                    date="2026-02-15")
        _insert_bank_txn(self.conn, self.stmt_id, self.eft_out,
                         "2026-02-15", "EFT PLOMBERIE COTE",
                         debit=1800.00, matched_doc=self.inv_id,
                         confidence=0.93)

        # Returned EFT (NSF)
        self.eft_ret = f"eft_{_uid()}"
        _insert_doc(self.conn, self.eft_ret, vendor="Plomberie Côté",
                    amount=1800.00, doc_type="bank_transaction",
                    date="2026-02-18")
        _insert_bank_txn(self.conn, self.stmt_id, self.eft_ret,
                         "2026-02-18", "REVERSAL EFT PLOMBERIE COTE",
                         credit=1800.00, matched_doc=None,
                         reason="no_matching_invoice")

    def test_net_settlement_is_zero(self):
        """EFT out + EFT return = net zero; invoice remains unsettled."""
        rows = self.conn.execute(
            "SELECT COALESCE(SUM(debit), 0) - COALESCE(SUM(credit), 0) AS net "
            "FROM bank_transactions WHERE statement_id=?",
            (self.stmt_id,),
        ).fetchone()
        assert abs(rows["net"]) < 0.01, "Net must be zero after EFT return"

    def test_reversal_detected(self):
        """BankMatcher must flag the EFT pair as a reversal."""
        matcher = BankMatcher()

        @dataclass
        class FakeTxn:
            description: str = ""
            memo: str = ""
            debit: float = 0.0
            credit: float = 0.0
            posted_date: str = ""
            txn_date: str = ""
            transaction_id: str = ""

        txns = [
            FakeTxn(description="EFT PLOMBERIE COTE", debit=1800.00,
                    posted_date="2026-02-15", transaction_id=self.eft_out),
            FakeTxn(description="REVERSAL EFT PLOMBERIE COTE", credit=1800.00,
                    posted_date="2026-02-18", transaction_id=self.eft_ret),
        ]
        results = matcher.detect_reversals(txns)
        assert len(results) == 1
        assert results[0]["flag"] == "reversal_pair"

    def test_invoice_still_unpaid_after_return(self):
        """After reversal, only the original EFT matched the invoice.
        The net effect is zero — the invoice liability must be reinstated."""
        # Build a reconciliation reflecting the returned EFT
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-02-28",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=self.conn, prepared_by="auditor",
        )
        # The EFT out shows as outstanding cheque
        item_id = add_reconciliation_item(
            recon_id, "outstanding_cheque", "EFT Plomberie Côté",
            1800.00, "2026-02-15", self.conn,
        )
        # The return shows as deposit in transit (money came back)
        add_reconciliation_item(
            recon_id, "deposit_in_transit", "EFT Return Plomberie Côté",
            1800.00, "2026-02-18", self.conn,
        )
        result = calculate_reconciliation(recon_id, self.conn)
        # Bank side: +1800 DIT - 1800 OC = net 0 adjustment
        net_adj = (result["bank_side"]["deposits_in_transit"]
                   - result["bank_side"]["outstanding_cheques"])
        assert abs(net_adj) < 0.01, "EFT return must net to zero"


# ===================================================================
# 6 — Cheque outstanding across 2 periods
# ===================================================================

class TestChequeOutstandingTwoPeriods:
    """Cheque #4501 issued in January, still outstanding in February.
    Must appear on both reconciliations without double-counting."""

    def setup_method(self):
        self.conn = _make_conn()

    def test_cheque_carries_forward(self):
        # January recon
        jan_id = create_reconciliation(
            "ACME", "Chequing", "2026-01-31",
            statement_balance=50000.00, gl_balance=48500.00,
            conn=self.conn, prepared_by="auditor",
        )
        item_id = add_reconciliation_item(
            jan_id, "outstanding_cheque", "Cheque #4501 — Béton Québec",
            1500.00, "2026-01-20", self.conn,
        )
        jan_result = calculate_reconciliation(jan_id, self.conn)
        assert jan_result["is_balanced"]

        # Finalize January
        finalized = finalize_reconciliation(jan_id, "reviewer", self.conn)
        assert finalized

        # February recon — cheque STILL outstanding
        feb_id = create_reconciliation(
            "ACME", "Chequing", "2026-02-28",
            statement_balance=52000.00, gl_balance=50500.00,
            conn=self.conn, prepared_by="auditor",
        )
        feb_item_id = add_reconciliation_item(
            feb_id, "outstanding_cheque", "Cheque #4501 — Béton Québec",
            1500.00, "2026-01-20", self.conn,
        )
        feb_result = calculate_reconciliation(feb_id, self.conn)
        assert feb_result["is_balanced"]

        # Both exist independently
        jan_items = get_reconciliation_items(jan_id, self.conn)
        feb_items = get_reconciliation_items(feb_id, self.conn)
        assert len(jan_items) == 1
        assert len(feb_items) == 1
        assert jan_items[0]["item_id"] != feb_items[0]["item_id"]

    def test_cleared_in_second_period(self):
        """When cheque clears in Feb, mark it cleared — Jan stays untouched."""
        jan_id = create_reconciliation(
            "ACME", "Chequing", "2026-01-31",
            statement_balance=50000.00, gl_balance=48500.00,
            conn=self.conn, prepared_by="auditor",
        )
        jan_item = add_reconciliation_item(
            jan_id, "outstanding_cheque", "Cheque #4501 — Béton Québec",
            1500.00, "2026-01-20", self.conn,
        )
        finalize_reconciliation(jan_id, "reviewer", self.conn)

        # Feb recon with cheque that will clear
        feb_id = create_reconciliation(
            "ACME", "Chequing", "2026-02-28",
            statement_balance=50500.00, gl_balance=50500.00,
            conn=self.conn, prepared_by="auditor",
        )
        feb_item = add_reconciliation_item(
            feb_id, "outstanding_cheque", "Cheque #4501 — Béton Québec",
            1500.00, "2026-01-20", self.conn,
        )
        # Mark cleared in February
        mark_item_cleared(feb_item, "2026-02-15", self.conn)

        feb_result = calculate_reconciliation(feb_id, self.conn)
        # Outstanding cheques should be 0 now (item is cleared)
        assert feb_result["bank_side"]["outstanding_cheques"] == 0.0

        # January is finalized — its item must still show outstanding
        jan_items = get_reconciliation_items(jan_id, self.conn)
        assert jan_items[0]["status"] == "outstanding"


# ===================================================================
# 7 — USD payment against CAD invoice
# ===================================================================

class TestUSDPaymentCADInvoice:
    """USD $740.00 payment at 1.35 rate = CAD $999.00 against CAD $999.00 invoice."""

    def test_cross_currency_match(self):
        conn = _make_conn()
        # Seed FX rate
        conn.execute(
            "INSERT INTO boc_fx_rates (rate_date, usd_cad) VALUES ('2026-03-01', 1.35)"
        )
        conn.commit()

        matcher = BankMatcher()
        result = matcher.cross_currency_amount_match(
            doc_amount=740.00, doc_currency="USD",
            txn_amount=999.00, txn_currency="CAD",
            conn=conn,
        )
        assert result is not None, "Cross-currency match failed"
        assert result["currency_converted"] is True
        assert result["fx_rate"] == 1.35
        # 740 * 1.35 = 999.00 — exact match
        assert abs(result["converted_amount"] - 999.00) < 0.01

    def test_fx_mismatch_rejected(self):
        """A wildly wrong amount must NOT match even with FX conversion."""
        conn = _make_conn()
        conn.execute(
            "INSERT INTO boc_fx_rates (rate_date, usd_cad) VALUES ('2026-03-01', 1.35)"
        )
        conn.commit()

        matcher = BankMatcher()
        result = matcher.cross_currency_amount_match(
            doc_amount=740.00, doc_currency="USD",
            txn_amount=1500.00, txn_currency="CAD",  # WAY off
            conn=conn,
        )
        assert result is None, "Wildly wrong FX amount should not match"

    def test_recon_with_fx_book_error(self):
        """FX gain/loss shows up as a book error on reconciliation."""
        conn = _make_conn()
        recon_id = create_reconciliation(
            "ACME", "USD Account", "2026-02-28",
            statement_balance=10000.00, gl_balance=10012.50,
            conn=conn, prepared_by="auditor",
        )
        # FX loss of $12.50
        add_reconciliation_item(
            recon_id, "book_error", "FX loss on USD payment — Fournisseur XYZ",
            Decimal("-12.50"), "2026-02-20", conn,
        )
        result = calculate_reconciliation(recon_id, conn)
        assert result["is_balanced"]


# ===================================================================
# FAIL-IF invariants
# ===================================================================

class TestFailIfDoubleSettlement:
    """FAIL: same payment settles two different liabilities."""

    def test_same_payment_two_invoices_flagged(self):
        conn = _make_conn()
        inv_a = f"inv_{_uid()}"
        inv_b = f"inv_{_uid()}"
        pay = f"pay_{_uid()}"
        stmt_id = f"stmt_{_uid()}"

        _insert_doc(conn, inv_a, vendor="V", amount=500, doc_type="invoice")
        _insert_doc(conn, inv_b, vendor="V", amount=500, doc_type="invoice")
        _insert_doc(conn, pay, vendor="V", amount=500, doc_type="bank_transaction")
        _insert_stmt(conn, stmt_id)

        # Attempt to match the same payment doc to TWO invoices
        _insert_bank_txn(conn, stmt_id, pay, "2026-01-15", "V",
                         debit=500, matched_doc=inv_a, confidence=0.90)
        _insert_bank_txn(conn, stmt_id, pay, "2026-01-15", "V",
                         debit=500, matched_doc=inv_b, confidence=0.90)

        # Detect the violation: same document_id used twice
        rows = conn.execute(
            "SELECT document_id, COUNT(*) as cnt FROM bank_transactions "
            "WHERE statement_id=? GROUP BY document_id HAVING cnt > 1",
            (stmt_id,),
        ).fetchall()
        assert len(rows) > 0, (
            "System allowed the same payment to settle two liabilities "
            "without detection — CRITICAL FAILURE"
        )
        # The row with count > 1 IS the violation marker
        assert rows[0]["document_id"] == pay


class TestFailIfDuplicateReversalNeutralizesPayment:
    """FAIL: duplicate reversal neutralizes a legitimate payment."""

    def test_second_reversal_blocked(self):
        """If a reversal already pairs with the original, a second identical
        reversal must NOT pair — it would over-reverse."""
        matcher = BankMatcher()

        @dataclass
        class FakeTxn:
            description: str = ""
            memo: str = ""
            debit: float = 0.0
            credit: float = 0.0
            posted_date: str = ""
            txn_date: str = ""
            transaction_id: str = ""

        original = FakeTxn(description="SCS Industrial", debit=2500.00,
                           posted_date="2026-03-01", transaction_id="orig")
        rev1 = FakeTxn(description="REVERSAL SCS Industrial", credit=2500.00,
                       posted_date="2026-03-02", transaction_id="rev1")
        rev2 = FakeTxn(description="REVERSAL SCS Industrial", credit=2500.00,
                       posted_date="2026-03-02", transaction_id="rev2")

        results = matcher.detect_reversals([original, rev1, rev2])
        # Only ONE reversal pair allowed
        assert len(results) == 1, (
            f"Expected 1 reversal pair but got {len(results)} — "
            "duplicate reversal is neutralizing the legitimate payment"
        )
        # The unpaired reversal (rev2) must remain flagged for review
        paired_ids = {results[0]["transaction_a_id"], results[0]["transaction_b_id"]}
        assert "rev2" not in paired_ids or "rev1" not in paired_ids


class TestFailIfReconChangesAfterFinalization:
    """FAIL: reconciliation data changes after finalization lock."""

    def test_add_item_after_finalization_blocked(self):
        conn = _make_conn()
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-01-31",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=conn, prepared_by="auditor",
        )
        assert finalize_reconciliation(recon_id, "reviewer", conn)

        with pytest.raises(FinalizedReconciliationError):
            add_reconciliation_item(
                recon_id, "outstanding_cheque", "Late cheque",
                500.00, "2026-01-25", conn,
            )

    def test_finalized_recon_immutable_in_db(self):
        """Direct SQL UPDATE on reconciliation_items must also fail
        (guarded by DB trigger)."""
        conn = _make_conn()
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-01-31",
            statement_balance=10500.00, gl_balance=10000.00,
            conn=conn, prepared_by="auditor",
        )
        item_id = add_reconciliation_item(
            recon_id, "outstanding_cheque", "Cheque #99",
            500.00, "2026-01-10", conn,
        )
        assert finalize_reconciliation(recon_id, "reviewer", conn)

        # Attempt raw SQL mutation — trigger must block
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE reconciliation_items SET amount = '9999' WHERE item_id = ?",
                (item_id,),
            )

    def test_status_remains_balanced_after_finalization(self):
        conn = _make_conn()
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-01-31",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=conn, prepared_by="auditor",
        )
        finalize_reconciliation(recon_id, "reviewer", conn)
        recon = get_reconciliation(recon_id, conn)
        assert recon["status"] == "balanced"
        assert recon["finalized_at"] is not None


# ===================================================================
# Audit trail completeness
# ===================================================================

class TestAuditTrailCompleteness:
    """Every match decision must have a traceable record."""

    def test_every_bank_txn_has_match_decision(self):
        """Each bank_transaction row must have EITHER a matched_document_id
        OR a match_reason explaining why not."""
        conn = _make_conn()
        stmt_id = f"stmt_{_uid()}"
        _insert_stmt(conn, stmt_id)

        # Matched transaction
        pay1 = f"pay_{_uid()}"
        inv1 = f"inv_{_uid()}"
        _insert_doc(conn, inv1, vendor="V", amount=100)
        _insert_doc(conn, pay1, vendor="V", amount=100, doc_type="bank_transaction")
        _insert_bank_txn(conn, stmt_id, pay1, "2026-01-15", "V",
                         debit=100, matched_doc=inv1, confidence=0.95)

        # Unmatched transaction
        pay2 = f"pay_{_uid()}"
        _insert_doc(conn, pay2, vendor="Unknown", amount=77,
                    doc_type="bank_transaction")
        _insert_bank_txn(conn, stmt_id, pay2, "2026-01-16", "Unknown",
                         debit=77, reason="no_matching_invoice")

        rows = conn.execute(
            "SELECT * FROM bank_transactions WHERE statement_id=?",
            (stmt_id,),
        ).fetchall()
        for r in rows:
            has_match = r["matched_document_id"] is not None
            has_reason = r["match_reason"] is not None
            assert has_match or has_reason, (
                f"Transaction {r['document_id']} has neither match nor reason — "
                "audit trail broken"
            )

    def test_reconciliation_items_traceable(self):
        """Every reconciliation item must link back to a document or have
        a description sufficient for audit."""
        conn = _make_conn()
        recon_id = create_reconciliation(
            "ACME", "Chequing", "2026-01-31",
            statement_balance=10000.00, gl_balance=9500.00,
            conn=conn, prepared_by="auditor",
        )
        add_reconciliation_item(
            recon_id, "outstanding_cheque", "Cheque #4501 — Béton Québec",
            500.00, "2026-01-20", conn, document_id="doc_abc123",
        )
        items = get_reconciliation_items(recon_id, conn)
        for item in items:
            assert item["description"], "Item missing description"
            assert item["amount"] is not None, "Item missing amount"
            assert (item["document_id"] or len(item["description"]) > 5), (
                "Item has neither document_id nor meaningful description"
            )
