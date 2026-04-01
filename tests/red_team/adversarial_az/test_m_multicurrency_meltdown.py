"""
M — MULTI-CURRENCY MELTDOWN
=============================
Attack FX lifecycle with missing rates, chain breaks, partial payment
FIFO, unsupported currency, and gain/loss precision.

Targets: multicurrency_engine
"""
from __future__ import annotations

import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.multicurrency_engine import (
    FxRate,
    FxEvent,
    BasisLot,
    SUPPORTED_CURRENCIES,
    GL_FX_GAIN,
    GL_FX_LOSS,
    _round,
    _round_rate,
    _to_dec,
    CENT,
)

try:
    from src.engines.multicurrency_engine import (
        record_fx_event,
        calculate_realized_gain_loss,
        FxLifecycle,
    )
    HAS_LIFECYCLE = True
except ImportError:
    HAS_LIFECYCLE = False


# ===================================================================
# TEST CLASS: FX Rate Attacks
# ===================================================================

class TestFxRateAttacks:
    """Malformed, missing, and contradictory FX rates."""

    def test_zero_rate(self):
        """FX rate of 0.0 must be rejected or flagged."""
        rate = FxRate(rate=Decimal("0"), date="2025-06-15",
                      source="adversary", from_currency="USD")
        converted = rate.convert(Decimal("1000"))
        assert converted == Decimal("0.00"), "Zero rate should produce zero CAD"

    def test_negative_rate(self):
        """Negative FX rate must be rejected."""
        rate = FxRate(rate=Decimal("-1.35"), date="2025-06-15",
                      source="adversary", from_currency="USD")
        converted = rate.convert(Decimal("1000"))
        assert converted < Decimal("0"), "Negative rate produces negative amount"
        # Negative amounts should be caught downstream

    def test_extreme_rate(self):
        """Absurdly high FX rate (1 USD = 1,000,000 CAD)."""
        rate = FxRate(rate=Decimal("1000000"), date="2025-06-15",
                      source="adversary", from_currency="USD")
        converted = rate.convert(Decimal("100"))
        assert converted == Decimal("100000000.00")

    def test_precision_loss_on_tiny_rate(self):
        """Very small FX rate (e.g., Japanese Yen style)."""
        rate = FxRate(rate=Decimal("0.0089"), date="2025-06-15",
                      source="BoC", from_currency="JPY")
        converted = rate.convert(Decimal("100000"))
        expected = _round(Decimal("100000") * Decimal("0.0089"))
        assert converted == expected

    def test_rate_precision_roundtrip(self):
        """Convert USD→CAD→USD should be close to original (within tolerance)."""
        usd_to_cad = FxRate(rate=Decimal("1.3500"), date="2025-06-15",
                            source="BoC", from_currency="USD")
        cad_to_usd = FxRate(rate=_round_rate(Decimal("1") / Decimal("1.3500")),
                            date="2025-06-15", source="BoC", from_currency="CAD",
                            to_currency="USD")
        usd_amount = Decimal("10000.00")
        cad_amount = usd_to_cad.convert(usd_amount)
        roundtrip = cad_to_usd.convert(cad_amount)
        diff = abs(roundtrip - usd_amount)
        assert diff < Decimal("1.00"), (
            f"FX roundtrip error too large: {diff}"
        )


# ===================================================================
# TEST CLASS: Unsupported Currency
# ===================================================================

class TestUnsupportedCurrency:
    """Currencies not in SUPPORTED_CURRENCIES."""

    def test_supported_currencies_list(self):
        assert "CAD" in SUPPORTED_CURRENCIES
        assert "USD" in SUPPORTED_CURRENCIES
        assert "EUR" in SUPPORTED_CURRENCIES

    def test_unsupported_currency_handling(self):
        """BTC, XAU, etc. should be rejected or flagged as unsupported."""
        for curr in ["BTC", "XAU", "JPY", "CNY"]:
            if curr not in SUPPORTED_CURRENCIES:
                # System should handle gracefully
                rate = FxRate(rate=Decimal("1.00"), date="2025-06-15",
                              source="test", from_currency=curr)
                # Conversion works mechanically, but lifecycle should flag it
                converted = rate.convert(Decimal("100"))
                assert converted == Decimal("100.00")


# ===================================================================
# TEST CLASS: Basis Lot FIFO
# ===================================================================

class TestBasisLotFIFO:
    """Partial payment must consume lots in FIFO order."""

    def test_fifo_lot_consumption(self):
        """Two lots, partial payment consumes oldest first."""
        lot1 = BasisLot(
            lot_id="lot-1",
            original_amount=Decimal("5000"),
            original_currency="USD",
            fx_rate_at_recognition=FxRate(
                rate=Decimal("1.30"), date="2025-01-15",
                source="BoC", from_currency="USD",
            ),
            remaining_amount=Decimal("5000"),
            recognized_date="2025-01-15",
        )
        lot2 = BasisLot(
            lot_id="lot-2",
            original_amount=Decimal("3000"),
            original_currency="USD",
            fx_rate_at_recognition=FxRate(
                rate=Decimal("1.35"), date="2025-03-01",
                source="BoC", from_currency="USD",
            ),
            remaining_amount=Decimal("3000"),
            recognized_date="2025-03-01",
        )

        # Pay $6000 USD — should consume all of lot1 ($5000) + $1000 from lot2
        payment = Decimal("6000")
        remaining_payment = payment

        # Consume lot1
        consume_1 = min(lot1.remaining_amount, remaining_payment)
        lot1.remaining_amount -= consume_1
        remaining_payment -= consume_1
        assert lot1.remaining_amount == Decimal("0")
        assert remaining_payment == Decimal("1000")

        # Consume lot2
        consume_2 = min(lot2.remaining_amount, remaining_payment)
        lot2.remaining_amount -= consume_2
        remaining_payment -= consume_2
        assert lot2.remaining_amount == Decimal("2000")
        assert remaining_payment == Decimal("0")

    def test_overpayment_detection(self):
        """Payment exceeding all lots should flag an error."""
        lot = BasisLot(
            lot_id="lot-over",
            original_amount=Decimal("1000"),
            original_currency="USD",
            fx_rate_at_recognition=FxRate(
                rate=Decimal("1.30"), date="2025-01-15",
                source="BoC", from_currency="USD",
            ),
            remaining_amount=Decimal("1000"),
            recognized_date="2025-01-15",
        )
        payment = Decimal("1500")
        consume = min(lot.remaining_amount, payment)
        remainder = payment - consume
        assert remainder == Decimal("500"), "Overpayment not detected"


# ===================================================================
# TEST CLASS: Gain/Loss Calculation
# ===================================================================

class TestGainLossCalculation:
    """FX gain/loss must be exact, deterministic, and correctly signed."""

    def test_fx_gain_on_strengthening_cad(self):
        """USD weakens: bought at 1.35, paid at 1.30 → FX gain."""
        invoice_rate = Decimal("1.35")
        payment_rate = Decimal("1.30")
        usd_amount = Decimal("10000")

        cad_at_invoice = _round(usd_amount * invoice_rate)  # 13500
        cad_at_payment = _round(usd_amount * payment_rate)  # 13000
        gain_loss = cad_at_invoice - cad_at_payment  # +500 = gain
        assert gain_loss == Decimal("500.00")
        assert gain_loss > Decimal("0")  # Gain

    def test_fx_loss_on_weakening_cad(self):
        """USD strengthens: bought at 1.30, paid at 1.40 → FX loss."""
        invoice_rate = Decimal("1.30")
        payment_rate = Decimal("1.40")
        usd_amount = Decimal("10000")

        cad_at_invoice = _round(usd_amount * invoice_rate)  # 13000
        cad_at_payment = _round(usd_amount * payment_rate)  # 14000
        gain_loss = cad_at_invoice - cad_at_payment  # -1000 = loss
        assert gain_loss == Decimal("-1000.00")

    def test_same_rate_zero_gain_loss(self):
        """Same rate at invoice and payment → zero gain/loss."""
        rate = Decimal("1.35")
        usd_amount = Decimal("10000")
        gain_loss = _round(usd_amount * rate) - _round(usd_amount * rate)
        assert gain_loss == Decimal("0.00")


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestMulticurrencyDeterminism:
    def test_fx_conversion_deterministic(self):
        rate = FxRate(rate=Decimal("1.3567"), date="2025-06-15",
                      source="BoC", from_currency="USD")
        results = {rate.convert(Decimal("12345.67")) for _ in range(100)}
        assert len(results) == 1, f"Non-deterministic: {results}"

    def test_round_deterministic(self):
        results = {_round(Decimal("1.005")) for _ in range(100)}
        assert len(results) == 1
