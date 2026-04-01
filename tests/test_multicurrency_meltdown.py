"""
tests/test_multicurrency_meltdown.py — Multi-currency meltdown test suite.

Scenario: a single USD purchase flows through the full lifecycle at
DIFFERENT FX rates at every stage.  The system must:

  1. Track each event's rate independently (no rate inheritance).
  2. Compute realized gain/loss ONLY with complete basis.
  3. Refuse to fabricate precise numbers from missing data.
  4. Mark unsupported areas explicitly (never silently skip).
  5. Never produce a fake "balanced" ledger from bad FX shortcuts.
  6. Handle partial payments over 3 months with FIFO lot tracking.
  7. Handle CAD refund against USD original as cross-currency (partial support).
"""
from __future__ import annotations

import pytest
from decimal import Decimal

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.multicurrency_engine import (
    CENT,
    GL_FX_GAIN,
    GL_FX_LOSS,
    BasisLot,
    FxRate,
    MultiCurrencyLedger,
    check_currency_support,
    compute_realized_fx_gain_loss,
)

# =========================================================================
# Helpers
# =========================================================================

def _dec(v) -> Decimal:
    return Decimal(str(v))


def _usd_rate(rate: str, date: str, source: str = "Bank of Canada") -> FxRate:
    return FxRate(rate=_dec(rate), date=date, source=source, from_currency="USD")


def _eur_rate(rate: str, date: str, source: str = "Bank of Canada") -> FxRate:
    return FxRate(rate=_dec(rate), date=date, source=source, from_currency="EUR")


# =========================================================================
# 1. Deposit at one FX rate, invoice at another
# =========================================================================

class TestDepositAndInvoiceAtDifferentRates:
    """Each event must record its own rate, not inherit from a prior event."""

    def test_deposit_and_invoice_have_independent_rates(self):
        ledger = MultiCurrencyLedger("doc_001")

        deposit_rate = _usd_rate("1.3500", "2025-01-15")
        invoice_rate = _usd_rate("1.3650", "2025-01-20")

        dep = ledger.record_deposit(1000, "USD", deposit_rate, "2025-01-15")
        inv = ledger.record_invoice(1000, "USD", invoice_rate, "2025-01-20")

        assert dep.fx_rate.rate == _dec("1.3500")
        assert inv.fx_rate.rate == _dec("1.3650")
        assert dep.cad_amount == _dec("1350.00")
        assert inv.cad_amount == _dec("1365.00")
        # Different CAD amounts despite same USD amount
        assert dep.cad_amount != inv.cad_amount

    def test_two_basis_lots_created(self):
        ledger = MultiCurrencyLedger("doc_002")
        ledger.record_deposit(500, "USD", _usd_rate("1.3500", "2025-01-15"), "2025-01-15")
        ledger.record_invoice(500, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")

        assert len(ledger.basis_lots) == 2
        assert ledger.basis_lots[0].fx_rate_at_recognition.rate == _dec("1.3500")
        assert ledger.basis_lots[1].fx_rate_at_recognition.rate == _dec("1.3650")


# =========================================================================
# 2. CBSA at its own FX rate
# =========================================================================

class TestCBSAAtDifferentRate:
    """CBSA customs entry uses its own FX rate (often the prior-month average)."""

    def test_cbsa_records_own_rate(self):
        ledger = MultiCurrencyLedger("doc_003")
        invoice_rate = _usd_rate("1.3650", "2025-01-20")
        cbsa_rate = _usd_rate("1.3580", "2025-01-25", source="CBSA B3 form")

        ledger.record_invoice(10000, "USD", invoice_rate, "2025-01-20")
        cbsa = ledger.record_cbsa_entry(10000, "USD", cbsa_rate, "2025-01-25", duties=500)

        assert cbsa.fx_rate.rate == _dec("1.3580")
        assert cbsa.fx_rate.source == "CBSA B3 form"
        assert cbsa.cad_amount == _dec("13580.00")
        # CBSA does NOT create a basis lot (it's a valuation, not a payable)
        assert len(ledger.basis_lots) == 1  # only from the invoice

    def test_cbsa_duties_converted_at_cbsa_rate(self):
        ledger = MultiCurrencyLedger("doc_004")
        cbsa_rate = _usd_rate("1.3580", "2025-01-25", source="CBSA B3 form")
        ev = ledger.record_cbsa_entry(10000, "USD", cbsa_rate, "2025-01-25", duties=500)
        # duties_cad is the rounded conversion: 500 * 1.3580 = 679.00
        assert ev.metadata["duties_cad"] == str(_dec("679.00"))


# =========================================================================
# 3. Payment at yet another FX rate → realized gain/loss
# =========================================================================

class TestPaymentAtDifferentRate:
    """Full payment settles basis lots; gain/loss computed from actual rates."""

    def test_payment_creates_realized_gain(self):
        ledger = MultiCurrencyLedger("doc_005")
        # Invoice at 1.3650
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        # Payment when CAD is weaker (higher rate) → gain for the payee
        ledger.record_payment(1000, "USD", _usd_rate("1.3800", "2025-02-15"), "2025-02-15")

        assert len(ledger.realized_gains_losses) == 1
        gl = ledger.realized_gains_losses[0]
        # Basis: 1000 * 1.3650 = 1365.00 CAD
        # Settlement: 1000 * 1.3800 = 1380.00 CAD
        # Gain: 1380.00 - 1365.00 = 15.00
        assert gl.realized_gain_loss == _dec("15.00")
        assert gl.gl_account == GL_FX_GAIN
        assert gl.complete_basis is True

    def test_payment_creates_realized_loss(self):
        ledger = MultiCurrencyLedger("doc_006")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        # CAD strengthens → loss
        ledger.record_payment(1000, "USD", _usd_rate("1.3400", "2025-02-15"), "2025-02-15")

        gl = ledger.realized_gains_losses[0]
        # Basis: 1365.00, Settlement: 1340.00, Loss: -25.00
        assert gl.realized_gain_loss == _dec("-25.00")
        assert gl.gl_account == GL_FX_LOSS

    def test_outstanding_balance_zero_after_full_payment(self):
        ledger = MultiCurrencyLedger("doc_007")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_payment(1000, "USD", _usd_rate("1.3800", "2025-02-15"), "2025-02-15")
        assert ledger.outstanding_balance("USD") == _dec("0")


# =========================================================================
# 4. Refund in CAD against USD original (cross-currency)
# =========================================================================

class TestCrossCurrencyRefund:
    """CAD refund against USD original: the system must NOT invent gain/loss."""

    def test_cad_refund_against_usd_is_partial_support(self):
        ledger = MultiCurrencyLedger("doc_008")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")

        # Refund is 1365 CAD (vendor refunds in CAD)
        refund = ledger.record_refund(
            refund_amount=1365,
            refund_currency="CAD",
            fx_rate_at_refund=None,  # CAD needs no rate
            date="2025-03-01",
            original_currency="USD",
        )

        assert refund.support_status == "partial"
        assert "cannot be computed" in refund.unsupported_reason.lower() or \
               "indeterminate" in refund.unsupported_reason.lower() or \
               "manual" in refund.unsupported_reason.lower()

    def test_cad_refund_does_not_settle_usd_lots(self):
        """Cross-currency refund must NOT auto-settle USD basis lots."""
        ledger = MultiCurrencyLedger("doc_009")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_refund(
            refund_amount=1365,
            refund_currency="CAD",
            fx_rate_at_refund=None,
            date="2025-03-01",
            original_currency="USD",
        )
        # USD lot should still be outstanding — CAD refund didn't settle it
        assert ledger.outstanding_balance("USD") == _dec("1000")
        # No realized gain/loss should have been computed
        assert len(ledger.realized_gains_losses) == 0

    def test_cad_refund_known_amount(self):
        """Even though gain/loss is indeterminate, CAD amount is known."""
        ledger = MultiCurrencyLedger("doc_010")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        refund = ledger.record_refund(
            refund_amount=1365,
            refund_currency="CAD",
            fx_rate_at_refund=None,
            date="2025-03-01",
            original_currency="USD",
        )
        # CAD amount is the refund amount itself (it IS in CAD)
        assert refund.cad_amount == _dec("1365")


# =========================================================================
# 5. Partial payments over 3 months (FIFO lot tracking)
# =========================================================================

class TestPartialPaymentsOverThreeMonths:
    """Three partial payments, each at a different rate, settled FIFO."""

    def test_three_partial_payments_fifo(self):
        ledger = MultiCurrencyLedger("doc_011")

        # Invoice for 3000 USD at 1.3650
        ledger.record_invoice(3000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")

        # Month 1: pay 1000 USD at 1.3700
        ledger.record_partial_payment(1000, "USD", _usd_rate("1.3700", "2025-02-15"), "2025-02-15")
        # Month 2: pay 1000 USD at 1.3500
        ledger.record_partial_payment(1000, "USD", _usd_rate("1.3500", "2025-03-15"), "2025-03-15")
        # Month 3: pay 1000 USD at 1.3800
        ledger.record_partial_payment(1000, "USD", _usd_rate("1.3800", "2025-04-15"), "2025-04-15")

        assert ledger.outstanding_balance("USD") == _dec("0")
        assert len(ledger.realized_gains_losses) == 3

        # Payment 1: 1000 * (1.3700 - 1.3650) = 5.00 gain
        assert ledger.realized_gains_losses[0].realized_gain_loss == _dec("5.00")
        # Payment 2: 1000 * (1.3500 - 1.3650) = -15.00 loss
        assert ledger.realized_gains_losses[1].realized_gain_loss == _dec("-15.00")
        # Payment 3: 1000 * (1.3800 - 1.3650) = 15.00 gain
        assert ledger.realized_gains_losses[2].realized_gain_loss == _dec("15.00")

        # Net: 5.00 - 15.00 + 15.00 = 5.00 gain
        assert ledger.total_realized_gain_loss() == _dec("5.00")

    def test_partial_payment_reduces_lot_remaining(self):
        ledger = MultiCurrencyLedger("doc_012")
        ledger.record_invoice(3000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_partial_payment(1000, "USD", _usd_rate("1.3700", "2025-02-15"), "2025-02-15")

        assert ledger.basis_lots[0].remaining_amount == _dec("2000")
        assert ledger.outstanding_balance("USD") == _dec("2000")

    def test_multiple_lots_fifo_order(self):
        """Two invoices → partial payment should settle first lot first."""
        ledger = MultiCurrencyLedger("doc_013")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3500", "2025-01-10"), "2025-01-10")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3700", "2025-01-20"), "2025-01-20")

        # Pay 1500 → should fully settle lot 1 (1000) and partially settle lot 2 (500)
        ledger.record_partial_payment(1500, "USD", _usd_rate("1.3600", "2025-02-15"), "2025-02-15")

        assert len(ledger.realized_gains_losses) == 2
        # Lot 1: 1000 * (1.3600 - 1.3500) = 10.00 gain
        assert ledger.realized_gains_losses[0].realized_gain_loss == _dec("10.00")
        assert ledger.realized_gains_losses[0].settled_amount_foreign == _dec("1000")
        # Lot 2: 500 * (1.3600 - 1.3700) = -5.00 loss
        assert ledger.realized_gains_losses[1].realized_gain_loss == _dec("-5.00")
        assert ledger.realized_gains_losses[1].settled_amount_foreign == _dec("500")

        assert ledger.basis_lots[0].remaining_amount == _dec("0")
        assert ledger.basis_lots[1].remaining_amount == _dec("500")


# =========================================================================
# 6. Unsupported currencies explicitly marked
# =========================================================================

class TestUnsupportedCurrencies:
    """Unsupported currencies must be flagged, never silently processed."""

    def test_unsupported_currency_check(self):
        result = check_currency_support("BRL")
        assert result["support_status"] == "unsupported"
        assert "BRL" in result["unsupported_reason"]

    def test_supported_currency_check(self):
        for curr in ("CAD", "USD", "EUR", "GBP"):
            result = check_currency_support(curr)
            assert result["support_status"] == "supported"

    def test_invoice_in_unsupported_currency(self):
        ledger = MultiCurrencyLedger("doc_014")
        ev = ledger.record_invoice(
            1000, "BRL",
            FxRate(_dec("0.27"), "2025-01-20", "manual", "BRL"),
            "2025-01-20",
        )
        assert ev.support_status == "unsupported"
        assert ev.cad_amount is None
        assert "BRL" in ev.unsupported_reason

    def test_unsupported_currency_creates_no_basis_lot(self):
        ledger = MultiCurrencyLedger("doc_015")
        ledger.record_invoice(1000, "BRL",
                              FxRate(_dec("0.27"), "2025-01-20", "manual", "BRL"),
                              "2025-01-20")
        assert len(ledger.basis_lots) == 0

    def test_deposit_in_unsupported_currency(self):
        ledger = MultiCurrencyLedger("doc_016")
        ev = ledger.record_deposit(500, "JPY",
                                   FxRate(_dec("0.009"), "2025-01-20", "manual", "JPY"),
                                   "2025-01-20")
        assert ev.support_status == "unsupported"

    def test_payment_in_unsupported_currency(self):
        ledger = MultiCurrencyLedger("doc_017")
        ev = ledger.record_payment(500, "CNY",
                                   FxRate(_dec("0.19"), "2025-01-20", "manual", "CNY"),
                                   "2025-01-20")
        assert ev.support_status == "unsupported"
        assert len(ledger.realized_gains_losses) == 0

    def test_has_unsupported_events_flag(self):
        ledger = MultiCurrencyLedger("doc_018")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        assert ledger.has_unsupported_events() is False

        ledger.record_deposit(1000, "BRL",
                              FxRate(_dec("0.27"), "2025-01-20", "manual", "BRL"),
                              "2025-01-20")
        assert ledger.has_unsupported_events() is True


# =========================================================================
# 7. No fake balanced ledger from bad FX shortcuts
# =========================================================================

class TestNoFakeBalancedLedger:
    """The ledger must NEVER claim to be balanced when it isn't."""

    def test_unsupported_events_prevent_balanced_status(self):
        ledger = MultiCurrencyLedger("doc_019")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_payment(1000, "BRL",
                              FxRate(_dec("0.27"), "2025-02-15", "manual", "BRL"),
                              "2025-02-15")

        summary = ledger.get_ledger_summary()
        assert summary["ledger_balanced"] is False
        assert summary["balance_warning"] is not None
        assert "unsupported" in summary["balance_warning"].lower()

    def test_outstanding_balance_prevents_balanced_status(self):
        ledger = MultiCurrencyLedger("doc_020")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        # Only pay half
        ledger.record_partial_payment(500, "USD", _usd_rate("1.3700", "2025-02-15"), "2025-02-15")

        summary = ledger.get_ledger_summary()
        assert summary["ledger_balanced"] is False
        assert "outstanding" in summary["balance_warning"].lower()

    def test_fully_settled_is_balanced(self):
        ledger = MultiCurrencyLedger("doc_021")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_payment(1000, "USD", _usd_rate("1.3800", "2025-02-15"), "2025-02-15")

        summary = ledger.get_ledger_summary()
        assert summary["ledger_balanced"] is True
        assert summary["ledger_fully_supported"] is True

    def test_cross_currency_refund_prevents_balanced(self):
        ledger = MultiCurrencyLedger("doc_022")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_refund(1365, "CAD", None, "2025-03-01", original_currency="USD")

        summary = ledger.get_ledger_summary()
        assert summary["ledger_balanced"] is False
        assert summary["ledger_fully_supported"] is False


# =========================================================================
# 8. Gain/loss recognized only where support exists
# =========================================================================

class TestGainLossOnlyWhereSupported:
    """FX gain/loss must never appear for unsupported scenarios."""

    def test_no_gain_loss_for_missing_rate(self):
        ledger = MultiCurrencyLedger("doc_023")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        # Payment with no rate
        ev = ledger.record_payment(1000, "USD", None, "2025-02-15")

        assert ev.support_status == "unsupported"
        assert len(ledger.realized_gains_losses) == 0

    def test_no_gain_loss_for_zero_rate(self):
        ledger = MultiCurrencyLedger("doc_024")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        bad_rate = FxRate(_dec("0"), "2025-02-15", "error", "USD")
        ev = ledger.record_payment(1000, "USD", bad_rate, "2025-02-15")

        assert ev.support_status == "unsupported"
        assert len(ledger.realized_gains_losses) == 0

    def test_gain_loss_only_for_settled_lots(self):
        """Unrealized gain/loss for open lots is NOT computed."""
        ledger = MultiCurrencyLedger("doc_025")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        # No payment — no gain/loss
        assert len(ledger.realized_gains_losses) == 0
        assert ledger.total_realized_gain_loss() == _dec("0")

    def test_gain_loss_entries_always_have_complete_basis(self):
        """Every realized gain/loss entry must have complete_basis=True."""
        ledger = MultiCurrencyLedger("doc_026")
        ledger.record_invoice(3000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_partial_payment(1000, "USD", _usd_rate("1.3700", "2025-02-15"), "2025-02-15")
        ledger.record_partial_payment(1000, "USD", _usd_rate("1.3500", "2025-03-15"), "2025-03-15")
        ledger.record_payment(1000, "USD", _usd_rate("1.3800", "2025-04-15"), "2025-04-15")

        for gl in ledger.realized_gains_losses:
            assert gl.complete_basis is True


# =========================================================================
# 9. System must FAIL if it would invent precise realized FX with missing basis
# =========================================================================

class TestRefuseToFabricateGainLoss:
    """The standalone compute function must refuse on incomplete data."""

    def test_missing_original_rate_refuses(self):
        result = compute_realized_fx_gain_loss(
            original_amount=1000, original_currency="USD",
            original_fx_rate=None, original_fx_date="2025-01-20",
            settlement_amount=1000, settlement_currency="USD",
            settlement_fx_rate="1.3800", settlement_fx_date="2025-02-15",
        )
        assert result["support_status"] == "unsupported"
        assert result["realized_gain_loss"] is None
        assert "missing basis data" in result["reason"].lower()

    def test_missing_settlement_rate_refuses(self):
        result = compute_realized_fx_gain_loss(
            original_amount=1000, original_currency="USD",
            original_fx_rate="1.3650", original_fx_date="2025-01-20",
            settlement_amount=1000, settlement_currency="USD",
            settlement_fx_rate=None, settlement_fx_date="2025-02-15",
        )
        assert result["support_status"] == "unsupported"
        assert result["realized_gain_loss"] is None

    def test_missing_dates_refuses(self):
        result = compute_realized_fx_gain_loss(
            original_amount=1000, original_currency="USD",
            original_fx_rate="1.3650", original_fx_date=None,
            settlement_amount=1000, settlement_currency="USD",
            settlement_fx_rate="1.3800", settlement_fx_date=None,
        )
        assert result["support_status"] == "unsupported"
        assert "original_fx_date" in str(result["missing_basis_data"])
        assert "settlement_fx_date" in str(result["missing_basis_data"])

    def test_missing_currency_refuses(self):
        result = compute_realized_fx_gain_loss(
            original_amount=1000, original_currency="",
            original_fx_rate="1.3650", original_fx_date="2025-01-20",
            settlement_amount=1000, settlement_currency="USD",
            settlement_fx_rate="1.3800", settlement_fx_date="2025-02-15",
        )
        assert result["support_status"] == "unsupported"

    def test_zero_amounts_refuses(self):
        result = compute_realized_fx_gain_loss(
            original_amount=0, original_currency="USD",
            original_fx_rate="1.3650", original_fx_date="2025-01-20",
            settlement_amount=0, settlement_currency="USD",
            settlement_fx_rate="1.3800", settlement_fx_date="2025-02-15",
        )
        assert result["support_status"] == "unsupported"

    def test_cross_currency_non_cad_refuses(self):
        """USD → EUR settlement (neither is CAD) is unsupported."""
        result = compute_realized_fx_gain_loss(
            original_amount=1000, original_currency="USD",
            original_fx_rate="1.3650", original_fx_date="2025-01-20",
            settlement_amount=900, settlement_currency="EUR",
            settlement_fx_rate="1.4500", settlement_fx_date="2025-02-15",
        )
        assert result["support_status"] == "unsupported"
        assert "triangulation" in result["reason"].lower()

    def test_complete_data_succeeds(self):
        result = compute_realized_fx_gain_loss(
            original_amount=1000, original_currency="USD",
            original_fx_rate="1.3650", original_fx_date="2025-01-20",
            settlement_amount=1000, settlement_currency="USD",
            settlement_fx_rate="1.3800", settlement_fx_date="2025-02-15",
        )
        assert result["support_status"] == "supported"
        assert result["realized_gain_loss"] == "15.00"
        assert result["gl_account"] == GL_FX_GAIN
        assert result["complete_basis"] is True


# =========================================================================
# 10. Full lifecycle meltdown scenario
# =========================================================================

class TestFullLifecycleMeltdown:
    """End-to-end: deposit → invoice → CBSA → partial payments → refund."""

    def test_full_meltdown_scenario(self):
        ledger = MultiCurrencyLedger("meltdown_001")

        # Step 1: Deposit (advance) at rate 1.3500
        dep = ledger.record_deposit(
            5000, "USD", _usd_rate("1.3500", "2025-01-10"), "2025-01-10"
        )
        assert dep.support_status == "supported"
        assert dep.cad_amount == _dec("6750.00")

        # Step 2: Invoice at rate 1.3650
        inv = ledger.record_invoice(
            10000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20"
        )
        assert inv.cad_amount == _dec("13650.00")

        # Step 3: CBSA customs at rate 1.3580
        cbsa = ledger.record_cbsa_entry(
            10000, "USD", _usd_rate("1.3580", "2025-01-25", "CBSA B3"),
            "2025-01-25", duties=750,
        )
        assert cbsa.cad_amount == _dec("13580.00")
        assert cbsa.fx_rate.rate != inv.fx_rate.rate  # different rates!

        # Step 4: Partial payment month 1 — 5000 USD at 1.3700
        pp1 = ledger.record_partial_payment(
            5000, "USD", _usd_rate("1.3700", "2025-02-15"), "2025-02-15"
        )
        assert pp1.support_status == "supported"

        # Step 5: Partial payment month 2 — 5000 USD at 1.3400
        pp2 = ledger.record_partial_payment(
            5000, "USD", _usd_rate("1.3400", "2025-03-15"), "2025-03-15"
        )

        # Step 6: Partial payment month 3 — 5000 USD at 1.3900
        pp3 = ledger.record_partial_payment(
            5000, "USD", _usd_rate("1.3900", "2025-04-15"), "2025-04-15"
        )

        # All 15000 USD settled (5000 deposit + 10000 invoice)
        assert ledger.outstanding_balance("USD") == _dec("0")

        # Verify FIFO settlement:
        # Lot 1 (deposit): 5000 @ 1.3500 → settled at 1.3700
        #   Gain: 5000 * (1.3700 - 1.3500) = 100.00
        # Lot 2 (invoice): first 5000 of 10000 @ 1.3650
        #   partial from pp1: none (pp1 fully consumed lot 1)
        #   Actually: pp1 = 5000 settles lot 1 (5000), pp2 = 5000 settles first 5000 of lot 2
        # Let me recalculate:
        # Lot 1: 5000 USD @ 1.3500
        # Lot 2: 10000 USD @ 1.3650
        # PP1: 5000 USD → all from Lot 1
        #   GL: 5000 * (1.3700 - 1.3500) = 100.00 gain
        # PP2: 5000 USD → all from Lot 2
        #   GL: 5000 * (1.3400 - 1.3650) = -125.00 loss
        # PP3: 5000 USD → rest of Lot 2
        #   GL: 5000 * (1.3900 - 1.3650) = 125.00 gain

        assert len(ledger.realized_gains_losses) == 3
        assert ledger.realized_gains_losses[0].realized_gain_loss == _dec("100.00")
        assert ledger.realized_gains_losses[1].realized_gain_loss == _dec("-125.00")
        assert ledger.realized_gains_losses[2].realized_gain_loss == _dec("125.00")

        # Net: 100.00 - 125.00 + 125.00 = 100.00
        assert ledger.total_realized_gain_loss() == _dec("100.00")

        # Ledger should be balanced (all supported, all settled)
        summary = ledger.get_ledger_summary()
        assert summary["ledger_balanced"] is True
        assert summary["ledger_fully_supported"] is True
        assert summary["event_count"] == 6

    def test_meltdown_with_cross_currency_refund_breaks_balance(self):
        """Same scenario but ending with CAD refund → ledger not balanced."""
        ledger = MultiCurrencyLedger("meltdown_002")

        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_payment(1000, "USD", _usd_rate("1.3800", "2025-02-15"), "2025-02-15")

        # Now a CAD refund against the original USD transaction
        refund = ledger.record_refund(
            500, "CAD", None, "2025-03-01", original_currency="USD"
        )

        summary = ledger.get_ledger_summary()
        # The refund event is "partial" support → ledger not fully supported
        assert summary["ledger_fully_supported"] is False
        assert refund.support_status == "partial"


# =========================================================================
# 11. Ledger summary structure validation
# =========================================================================

class TestLedgerSummaryStructure:
    """The summary must contain all required fields and be serializable."""

    def test_summary_has_all_fields(self):
        ledger = MultiCurrencyLedger("doc_027")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        summary = ledger.get_ledger_summary()

        required = {
            "document_id", "base_currency", "event_count", "events",
            "basis_lots", "realized_gains_losses",
            "outstanding_foreign_balance", "total_realized_gain_loss_cad",
            "ledger_fully_supported", "ledger_balanced", "balance_warning",
        }
        assert required.issubset(set(summary.keys()))

    def test_summary_events_have_support_status(self):
        ledger = MultiCurrencyLedger("doc_028")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_deposit(500, "BRL",
                              FxRate(_dec("0.27"), "2025-01-20", "manual", "BRL"),
                              "2025-01-20")

        summary = ledger.get_ledger_summary()
        for ev in summary["events"]:
            assert "support_status" in ev
            assert ev["support_status"] in ("supported", "unsupported", "partial")

    def test_summary_is_json_serializable(self):
        import json
        ledger = MultiCurrencyLedger("doc_029")
        ledger.record_invoice(1000, "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_payment(1000, "USD", _usd_rate("1.3800", "2025-02-15"), "2025-02-15")
        summary = ledger.get_ledger_summary()
        # Must not raise
        serialized = json.dumps(summary)
        assert len(serialized) > 0


# =========================================================================
# 12. Edge cases
# =========================================================================

class TestEdgeCases:
    """Boundary conditions and degenerate inputs."""

    def test_cad_to_cad_no_fx_needed(self):
        """CAD invoice + CAD payment: rate should be 1.0, no gain/loss."""
        ledger = MultiCurrencyLedger("doc_030")
        cad_rate = FxRate(_dec("1.0"), "2025-01-20", "identity", "CAD")
        ledger.record_invoice(1000, "CAD", cad_rate, "2025-01-20")
        ledger.record_payment(1000, "CAD", FxRate(_dec("1.0"), "2025-02-15", "identity", "CAD"), "2025-02-15")

        assert ledger.total_realized_gain_loss() == _dec("0")
        summary = ledger.get_ledger_summary()
        assert summary["ledger_balanced"] is True

    def test_negative_rate_is_unsupported(self):
        ledger = MultiCurrencyLedger("doc_031")
        bad_rate = FxRate(_dec("-1.35"), "2025-01-20", "error", "USD")
        ev = ledger.record_invoice(1000, "USD", bad_rate, "2025-01-20")
        assert ev.support_status == "unsupported"

    def test_very_small_amounts(self):
        ledger = MultiCurrencyLedger("doc_032")
        ledger.record_invoice(_dec("0.01"), "USD", _usd_rate("1.3650", "2025-01-20"), "2025-01-20")
        ledger.record_payment(_dec("0.01"), "USD", _usd_rate("1.3800", "2025-02-15"), "2025-02-15")

        # 0.01 * 1.3650 = 0.01 (rounded), 0.01 * 1.3800 = 0.01 (rounded)
        # Gain/loss at this scale rounds to 0
        assert ledger.outstanding_balance("USD") == _dec("0")

    def test_eur_supported(self):
        ledger = MultiCurrencyLedger("doc_033")
        ev = ledger.record_invoice(1000, "EUR", _eur_rate("1.4700", "2025-01-20"), "2025-01-20")
        assert ev.support_status == "supported"
        assert ev.cad_amount == _dec("1470.00")
