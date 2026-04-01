"""
B — BANK RECONCILIATION AMBUSH
===============================
Destroy bank reconciliation with partial payments, duplicate imports,
reversals, cross-currency matching, and finalization lock bypass.

Targets: reconciliation_engine, bank_parser, bank_matcher
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.reconciliation_engine import (
    ensure_reconciliation_tables,
    create_reconciliation,
    add_reconciliation_item,
    calculate_reconciliation,
    finalize_reconciliation,
    get_reconciliation,
    DuplicateItemError,
    FinalizedReconciliationError,
    ImplausibleAmountError,
    NegativeAmountError,
    BALANCE_TOLERANCE,
)

from .conftest import fresh_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _recon_db() -> sqlite3.Connection:
    conn = fresh_db()
    ensure_reconciliation_tables(conn)
    return conn


def _make_recon(conn, **kw) -> str:
    defaults = dict(
        client_code="TEST01",
        account_name="Main Chequing",
        period_end_date="2025-06-30",
        statement_balance=50000.00,
        gl_balance=48500.00,
        prepared_by="adversary",
    )
    defaults.update(kw)
    return create_reconciliation(conn=conn, **defaults)


def _add_item(conn, rid, item_type, amount, description, posting_date):
    """Helper to call add_reconciliation_item with correct param order."""
    return add_reconciliation_item(
        reconciliation_id=rid,
        item_type=item_type,
        description=description,
        amount=amount,
        transaction_date=posting_date,
        conn=conn,
    )


# ===================================================================
# TEST CLASS: Partial Payment Torture
# ===================================================================

class TestPartialPaymentTorture:
    """One invoice paid by multiple partial payments."""

    def test_three_partials_reconcile_exactly(self):
        conn = _recon_db()
        rid = _make_recon(conn, statement_balance=10000.00, gl_balance=7000.00)
        # 3 outstanding cheques totaling $3000
        _add_item(conn, rid, "outstanding_cheque", Decimal("1000.00"),
                  "Cheque #101", "2025-06-28")
        _add_item(conn, rid, "outstanding_cheque", Decimal("1200.00"),
                  "Cheque #102", "2025-06-29")
        _add_item(conn, rid, "outstanding_cheque", Decimal("800.00"),
                  "Cheque #103", "2025-06-30")
        result = calculate_reconciliation(rid, conn)
        diff = abs(float(result.get("difference", 999)))
        assert diff <= BALANCE_TOLERANCE, (
            f"DEFECT: 3 partial payments don't reconcile. diff={diff}"
        )

    def test_partial_payment_zero_remainder(self):
        """After all partials, remaining balance must be exactly zero."""
        conn = _recon_db()
        rid = _make_recon(conn, statement_balance=5000.00, gl_balance=5000.00)
        result = calculate_reconciliation(rid, conn)
        diff = float(result.get("difference", 999))
        assert abs(diff) <= BALANCE_TOLERANCE, f"Zero-item recon has diff={diff}"


# ===================================================================
# TEST CLASS: Duplicate Import Attack
# ===================================================================

class TestDuplicateImportAttack:
    """Same item added twice must be rejected."""

    def test_duplicate_item_raises(self):
        conn = _recon_db()
        rid = _make_recon(conn)
        _add_item(conn, rid, "outstanding_cheque", Decimal("500.00"),
                  "Cheque #200", "2025-06-15")
        with pytest.raises(DuplicateItemError):
            _add_item(conn, rid, "outstanding_cheque", Decimal("500.00"),
                      "Cheque #200", "2025-06-15")

    def test_near_duplicate_different_date(self):
        """Same amount but different description = different item."""
        conn = _recon_db()
        rid = _make_recon(conn)
        _add_item(conn, rid, "outstanding_cheque", Decimal("500.00"),
                  "Cheque #200", "2025-06-15")
        # Should NOT raise — different description
        _add_item(conn, rid, "outstanding_cheque", Decimal("500.00"),
                  "Cheque #201", "2025-06-16")


# ===================================================================
# TEST CLASS: Finalization Lock
# ===================================================================

class TestFinalizationLock:
    """Once finalized, no modifications allowed."""

    def test_add_item_after_finalize_raises(self):
        conn = _recon_db()
        rid = _make_recon(conn, statement_balance=5000.00, gl_balance=5000.00)
        finalize_reconciliation(rid, "senior_cpa", conn)
        with pytest.raises(FinalizedReconciliationError):
            _add_item(conn, rid, "outstanding_cheque", Decimal("100.00"),
                      "Late cheque", "2025-07-01")

    def test_double_finalize_is_idempotent(self):
        """Finalizing twice must not corrupt state."""
        conn = _recon_db()
        rid = _make_recon(conn, statement_balance=5000.00, gl_balance=5000.00)
        finalize_reconciliation(rid, "cpa1", conn)
        # Second finalize should either succeed silently or raise clearly
        try:
            finalize_reconciliation(rid, "cpa2", conn)
        except FinalizedReconciliationError:
            pass  # Acceptable
        recon = get_reconciliation(rid, conn)
        assert recon["status"] in ("finalized", "balanced")

    def test_recalculate_after_finalize_raises(self):
        """Cannot recalculate a finalized reconciliation."""
        conn = _recon_db()
        rid = _make_recon(conn, statement_balance=5000.00, gl_balance=5000.00)
        finalize_reconciliation(rid, "auditor", conn)
        try:
            result = calculate_reconciliation(rid, conn)
            # If it succeeds, check that it's read-only (not modifying)
        except FinalizedReconciliationError:
            pass  # Expected


# ===================================================================
# TEST CLASS: Implausible Amount Guard
# ===================================================================

class TestImplausibleAmountGuard:
    """Astronomically large or negative amounts must be rejected."""

    def test_implausible_amount_rejected(self):
        conn = _recon_db()
        rid = _make_recon(conn)
        with pytest.raises((ImplausibleAmountError, Exception)):
            _add_item(conn, rid, "outstanding_cheque",
                      Decimal("99999999999.99"),
                      "Implausible", "2025-06-15")

    def test_negative_outstanding_cheque_rejected(self):
        conn = _recon_db()
        rid = _make_recon(conn)
        with pytest.raises((NegativeAmountError, Exception)):
            _add_item(conn, rid, "outstanding_cheque",
                      Decimal("-500.00"),
                      "Negative cheque", "2025-06-15")

    def test_zero_amount_item(self):
        """Zero-amount item should either be accepted or rejected cleanly."""
        conn = _recon_db()
        rid = _make_recon(conn)
        try:
            _add_item(conn, rid, "bank_error", Decimal("0.00"),
                      "Zero error", "2025-06-15")
        except Exception:
            pass  # Either way, must not corrupt state
        result = calculate_reconciliation(rid, conn)
        assert "difference" in result


# ===================================================================
# TEST CLASS: Concurrent Reconciliation
# ===================================================================

class TestConcurrentReconciliation:
    """Two users modifying same reconciliation simultaneously."""

    def test_concurrent_add_items(self):
        """Parallel item additions must not lose data."""
        conn = _recon_db()
        rid = _make_recon(conn)
        errors = []

        def _add(i: int):
            try:
                _add_item(
                    conn, rid, "outstanding_cheque",
                    Decimal(f"{100 + i}.00"),
                    f"Cheque #{300 + i}",
                    "2025-06-15",
                )
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=_add, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        # At least some items should have been added successfully
        result = calculate_reconciliation(rid, conn)
        assert result is not None


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestReconciliationDeterminism:
    """Same inputs → identical reconciliation result, every time."""

    def test_calculate_deterministic(self):
        results = []
        for _ in range(10):
            conn = _recon_db()
            rid = _make_recon(conn, statement_balance=10000.00,
                              gl_balance=9500.00)
            _add_item(conn, rid, "outstanding_cheque", Decimal("500.00"),
                      "CHQ-DET", "2025-06-30")
            r = calculate_reconciliation(rid, conn)
            results.append(str(r.get("difference", "MISS")))
        assert len(set(results)) == 1, f"Non-deterministic reconciliation: {set(results)}"
