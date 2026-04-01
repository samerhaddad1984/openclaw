"""
src/engines/multicurrency_engine.py -- Multi-currency lifecycle engine.

Tracks FX basis through the full document lifecycle:
  deposit -> invoice -> CBSA customs -> payment -> refund -> partial payment

Design principles:
  1. Each event records its OWN FX rate + date.  No rate is ever "inherited"
     from a prior event unless explicitly carried forward with provenance.
  2. Realized FX gain/loss is computed ONLY when a complete basis chain exists
     (original currency amount + original rate + settlement rate).  If any
     link is missing the system returns UNSUPPORTED with an explanation --
     it will NEVER invent a precise number.
  3. Unsupported scenarios are flagged with support_status="unsupported" and
     a human-readable reason.  The ledger is never force-balanced.
  4. Partial payments track remaining basis lots (FIFO).

All monetary arithmetic uses Python Decimal.  No AI calls.
"""
from __future__ import annotations

import json
import secrets
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ZERO = Decimal("0")
_ONE = Decimal("1")
CENT = Decimal("0.01")
FX_RATE_PRECISION = Decimal("0.000001")  # 6 decimal places for FX rates

SUPPORTED_CURRENCIES = frozenset({"CAD", "USD", "EUR", "GBP"})

# GL accounts (matches audit_engine.py chart of accounts)
GL_FX_GAIN = "4340"   # Gain de change / Foreign exchange gain
GL_FX_LOSS = "5810"   # Perte de change / Foreign exchange loss


def _round(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _round_rate(value: Decimal) -> Decimal:
    """Round an FX rate to 6 decimal places to preserve precision."""
    return value.quantize(FX_RATE_PRECISION, rounding=ROUND_HALF_UP)


def _to_dec(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        return _ZERO
    return Decimal(str(value))


def _event_id() -> str:
    return f"fxe_{secrets.token_hex(6)}"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FxRate:
    """A single FX observation: rate + date + source."""
    rate: Decimal
    date: str                # YYYY-MM-DD
    source: str              # e.g. "Bank of Canada", "CBSA B3", "invoice"
    from_currency: str       # ISO
    to_currency: str = "CAD"

    def convert(self, amount: Decimal) -> Decimal:
        return _round(amount * self.rate)


@dataclass
class BasisLot:
    """One chunk of foreign-currency cost basis (for FIFO partial payments)."""
    lot_id: str
    original_amount: Decimal          # in foreign currency
    original_currency: str
    fx_rate_at_recognition: FxRate    # rate when the obligation was booked
    remaining_amount: Decimal         # how much of this lot is still unsettled
    recognized_date: str              # YYYY-MM-DD

    def cad_basis(self) -> Decimal:
        """CAD value at original recognition."""
        return self.fx_rate_at_recognition.convert(self.remaining_amount)


@dataclass
class FxEvent:
    """A single FX-relevant event in the lifecycle."""
    event_id: str
    event_type: str           # deposit | invoice | cbsa | payment | refund | partial_payment
    date: str
    original_currency: str
    original_amount: Decimal
    fx_rate: FxRate | None    # None = rate not available
    cad_amount: Decimal | None
    support_status: str       # "supported" | "unsupported" | "partial"
    unsupported_reason: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FxGainLoss:
    """Realized FX gain or loss from a settlement event."""
    settlement_event_id: str
    basis_lot_id: str
    original_currency: str
    settled_amount_foreign: Decimal
    basis_rate: FxRate
    settlement_rate: FxRate
    cad_at_basis: Decimal
    cad_at_settlement: Decimal
    realized_gain_loss: Decimal       # positive = gain, negative = loss
    gl_account: str                   # 4340 or 5810
    complete_basis: bool              # True = computed from real data; False = NEVER happens (we refuse)


# =========================================================================
# Currency support gate
# =========================================================================

def check_currency_support(currency: str) -> dict[str, Any]:
    """Check whether a currency is supported for full lifecycle tracking."""
    curr = (currency or "").strip().upper()
    if curr in SUPPORTED_CURRENCIES:
        return {
            "currency": curr,
            "support_status": "supported",
            "unsupported_reason": None,
        }
    return {
        "currency": curr,
        "support_status": "unsupported",
        "unsupported_reason": (
            f"Currency '{curr}' is not in the supported set "
            f"({', '.join(sorted(SUPPORTED_CURRENCIES))}). "
            f"Multi-currency lifecycle tracking, FX gain/loss recognition, "
            f"and automated posting are unavailable for this currency."
        ),
    }


# =========================================================================
# Multi-currency transaction lifecycle
# =========================================================================

class MultiCurrencyLedger:
    """Tracks the full FX lifecycle for a single document/transaction chain.

    Each event is recorded with its own rate.  The ledger never
    force-balances and never fabricates gain/loss without complete basis.
    """

    def __init__(self, document_id: str, base_currency: str = "CAD"):
        self.document_id = document_id
        self.base_currency = base_currency.upper()
        self.events: list[FxEvent] = []
        self.basis_lots: list[BasisLot] = []
        self.realized_gains_losses: list[FxGainLoss] = []
        self._lot_counter = 0

    # -- helpers --

    def _next_lot_id(self) -> str:
        self._lot_counter += 1
        return f"lot_{self.document_id}_{self._lot_counter}"

    def _validate_rate(self, fx_rate: FxRate | None, event_type: str) -> tuple[str, str | None]:
        """Return (support_status, unsupported_reason)."""
        if fx_rate is None:
            return ("unsupported", f"No FX rate provided for {event_type} event. "
                    "Cannot convert to CAD or compute gain/loss.")
        if fx_rate.rate <= _ZERO:
            return ("unsupported", f"FX rate for {event_type} is <= 0 ({fx_rate.rate}). Invalid.")
        return ("supported", None)

    # =====================================================================
    # Event recording
    # =====================================================================

    def record_deposit(
        self,
        amount: Any,
        currency: str,
        fx_rate: FxRate | None,
        date: str,
        metadata: dict[str, Any] | None = None,
    ) -> FxEvent:
        """Record a deposit (e.g. advance payment received in foreign currency)."""
        curr = currency.strip().upper()
        amt = _to_dec(amount)
        support = check_currency_support(curr)

        if support["support_status"] == "unsupported":
            event = FxEvent(
                event_id=_event_id(), event_type="deposit", date=date,
                original_currency=curr, original_amount=amt,
                fx_rate=fx_rate, cad_amount=None,
                support_status="unsupported",
                unsupported_reason=support["unsupported_reason"],
                metadata=metadata or {},
            )
            self.events.append(event)
            return event

        status, reason = self._validate_rate(fx_rate, "deposit")
        cad = fx_rate.convert(amt) if status == "supported" and fx_rate else None

        event = FxEvent(
            event_id=_event_id(), event_type="deposit", date=date,
            original_currency=curr, original_amount=amt,
            fx_rate=fx_rate, cad_amount=cad,
            support_status=status, unsupported_reason=reason,
            metadata=metadata or {},
        )
        self.events.append(event)

        # Create basis lot for future settlement
        if status == "supported" and fx_rate:
            lot = BasisLot(
                lot_id=self._next_lot_id(),
                original_amount=amt,
                original_currency=curr,
                fx_rate_at_recognition=fx_rate,
                remaining_amount=amt,
                recognized_date=date,
            )
            self.basis_lots.append(lot)

        return event

    def record_invoice(
        self,
        amount: Any,
        currency: str,
        fx_rate: FxRate | None,
        date: str,
        metadata: dict[str, Any] | None = None,
    ) -> FxEvent:
        """Record an invoice (obligation) in foreign currency."""
        curr = currency.strip().upper()
        amt = _to_dec(amount)
        support = check_currency_support(curr)

        if support["support_status"] == "unsupported":
            event = FxEvent(
                event_id=_event_id(), event_type="invoice", date=date,
                original_currency=curr, original_amount=amt,
                fx_rate=fx_rate, cad_amount=None,
                support_status="unsupported",
                unsupported_reason=support["unsupported_reason"],
                metadata=metadata or {},
            )
            self.events.append(event)
            return event

        status, reason = self._validate_rate(fx_rate, "invoice")
        cad = fx_rate.convert(amt) if status == "supported" and fx_rate else None

        event = FxEvent(
            event_id=_event_id(), event_type="invoice", date=date,
            original_currency=curr, original_amount=amt,
            fx_rate=fx_rate, cad_amount=cad,
            support_status=status, unsupported_reason=reason,
            metadata=metadata or {},
        )
        self.events.append(event)

        if status == "supported" and fx_rate:
            lot = BasisLot(
                lot_id=self._next_lot_id(),
                original_amount=amt,
                original_currency=curr,
                fx_rate_at_recognition=fx_rate,
                remaining_amount=amt,
                recognized_date=date,
            )
            self.basis_lots.append(lot)

        return event

    def record_cbsa_entry(
        self,
        customs_value: Any,
        currency: str,
        fx_rate: FxRate | None,
        date: str,
        duties: Any = 0,
        metadata: dict[str, Any] | None = None,
    ) -> FxEvent:
        """Record a CBSA customs entry — always has its own FX rate."""
        curr = currency.strip().upper()
        cv = _to_dec(customs_value)
        d = _to_dec(duties)
        support = check_currency_support(curr)

        if support["support_status"] == "unsupported":
            event = FxEvent(
                event_id=_event_id(), event_type="cbsa", date=date,
                original_currency=curr, original_amount=cv,
                fx_rate=fx_rate, cad_amount=None,
                support_status="unsupported",
                unsupported_reason=support["unsupported_reason"],
                metadata={**(metadata or {}), "duties": str(d)},
            )
            self.events.append(event)
            return event

        status, reason = self._validate_rate(fx_rate, "cbsa")
        cad = fx_rate.convert(cv) if status == "supported" and fx_rate else None

        event = FxEvent(
            event_id=_event_id(), event_type="cbsa", date=date,
            original_currency=curr, original_amount=cv,
            fx_rate=fx_rate, cad_amount=cad,
            support_status=status, unsupported_reason=reason,
            metadata={
                **(metadata or {}),
                "duties": str(d),
                "duties_cad": str(fx_rate.convert(d)) if fx_rate and status == "supported" else None,
            },
        )
        self.events.append(event)
        # CBSA does NOT create a new basis lot — it's a valuation event,
        # not a new receivable/payable.
        return event

    def record_payment(
        self,
        amount: Any,
        currency: str,
        fx_rate: FxRate | None,
        date: str,
        metadata: dict[str, Any] | None = None,
    ) -> FxEvent:
        """Record a full payment settling the outstanding foreign-currency balance."""
        curr = currency.strip().upper()
        amt = _to_dec(amount)
        support = check_currency_support(curr)

        if support["support_status"] == "unsupported":
            event = FxEvent(
                event_id=_event_id(), event_type="payment", date=date,
                original_currency=curr, original_amount=amt,
                fx_rate=fx_rate, cad_amount=None,
                support_status="unsupported",
                unsupported_reason=support["unsupported_reason"],
                metadata=metadata or {},
            )
            self.events.append(event)
            return event

        status, reason = self._validate_rate(fx_rate, "payment")
        cad = fx_rate.convert(amt) if status == "supported" and fx_rate else None

        event = FxEvent(
            event_id=_event_id(), event_type="payment", date=date,
            original_currency=curr, original_amount=amt,
            fx_rate=fx_rate, cad_amount=cad,
            support_status=status, unsupported_reason=reason,
            metadata=metadata or {},
        )
        self.events.append(event)

        # Settle basis lots FIFO and compute realized gain/loss
        if status == "supported" and fx_rate:
            self._settle_lots(amt, curr, fx_rate, event.event_id)

        return event

    def record_partial_payment(
        self,
        amount: Any,
        currency: str,
        fx_rate: FxRate | None,
        date: str,
        metadata: dict[str, Any] | None = None,
    ) -> FxEvent:
        """Record a partial payment — settles basis lots FIFO."""
        curr = currency.strip().upper()
        amt = _to_dec(amount)
        support = check_currency_support(curr)

        if support["support_status"] == "unsupported":
            event = FxEvent(
                event_id=_event_id(), event_type="partial_payment", date=date,
                original_currency=curr, original_amount=amt,
                fx_rate=fx_rate, cad_amount=None,
                support_status="unsupported",
                unsupported_reason=support["unsupported_reason"],
                metadata=metadata or {},
            )
            self.events.append(event)
            return event

        status, reason = self._validate_rate(fx_rate, "partial_payment")
        cad = fx_rate.convert(amt) if status == "supported" and fx_rate else None

        event = FxEvent(
            event_id=_event_id(), event_type="partial_payment", date=date,
            original_currency=curr, original_amount=amt,
            fx_rate=fx_rate, cad_amount=cad,
            support_status=status, unsupported_reason=reason,
            metadata=metadata or {},
        )
        self.events.append(event)

        if status == "supported" and fx_rate:
            self._settle_lots(amt, curr, fx_rate, event.event_id)

        return event

    def record_refund(
        self,
        refund_amount: Any,
        refund_currency: str,
        fx_rate_at_refund: FxRate | None,
        date: str,
        original_currency: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> FxEvent:
        """Record a refund — may be in a DIFFERENT currency than the original.

        Cross-currency refunds (e.g. CAD refund against USD original) are
        marked as support_status="partial" because realized gain/loss
        computation requires knowing the original basis AND the implicit
        cross-rate, which may not be deterministic.
        """
        r_curr = refund_currency.strip().upper()
        r_amt = _to_dec(refund_amount)
        o_curr = (original_currency or "").strip().upper() if original_currency else None

        support = check_currency_support(r_curr)
        if support["support_status"] == "unsupported":
            event = FxEvent(
                event_id=_event_id(), event_type="refund", date=date,
                original_currency=r_curr, original_amount=r_amt,
                fx_rate=fx_rate_at_refund, cad_amount=None,
                support_status="unsupported",
                unsupported_reason=support["unsupported_reason"],
                metadata=metadata or {},
            )
            self.events.append(event)
            return event

        # Cross-currency refund detection
        is_cross_currency = (
            o_curr is not None
            and o_curr != r_curr
            and o_curr != self.base_currency
        )

        status, reason = self._validate_rate(fx_rate_at_refund, "refund")

        if is_cross_currency:
            # We CAN record the CAD amount of the refund itself, but we
            # CANNOT compute realized FX gain/loss against the original
            # foreign-currency basis without an explicit cross-rate.
            cad = None
            if r_curr == self.base_currency:
                # Refund is already in CAD
                cad = r_amt
                status = "partial"
                reason = (
                    f"Refund is in {r_curr} but original transaction was in "
                    f"{o_curr}. The CAD refund amount ({r_amt}) is known, but "
                    f"realized FX gain/loss against the {o_curr} basis cannot "
                    f"be computed without the original {o_curr}->{r_curr} "
                    f"settlement rate. Posting the refund at face value; "
                    f"FX gain/loss requires manual journal entry."
                )
            elif fx_rate_at_refund:
                cad = fx_rate_at_refund.convert(r_amt)
                status = "partial"
                reason = (
                    f"Cross-currency refund: refund in {r_curr} against "
                    f"{o_curr} original. CAD equivalent computed at refund "
                    f"rate, but realized FX gain/loss against {o_curr} basis "
                    f"is indeterminate — requires manual reconciliation."
                )
            else:
                status = "unsupported"
                reason = (
                    f"Cross-currency refund ({r_curr} against {o_curr} "
                    f"original) with no FX rate. Cannot convert or compute "
                    f"gain/loss."
                )
        else:
            cad = fx_rate_at_refund.convert(r_amt) if status == "supported" and fx_rate_at_refund else None

        event = FxEvent(
            event_id=_event_id(), event_type="refund", date=date,
            original_currency=r_curr, original_amount=r_amt,
            fx_rate=fx_rate_at_refund, cad_amount=cad,
            support_status=status, unsupported_reason=reason,
            metadata={
                **(metadata or {}),
                "original_currency": o_curr,
                "is_cross_currency_refund": is_cross_currency,
            },
        )
        self.events.append(event)

        # For same-currency refunds with valid rate, reverse basis lots
        if status == "supported" and fx_rate_at_refund and not is_cross_currency:
            self._settle_lots(r_amt, r_curr, fx_rate_at_refund, event.event_id)

        return event

    # =====================================================================
    # FIFO lot settlement + gain/loss
    # =====================================================================

    def _settle_lots(
        self,
        settle_amount: Decimal,
        currency: str,
        settlement_rate: FxRate,
        event_id: str,
    ) -> None:
        """Settle basis lots FIFO.  Compute realized gain/loss per lot."""
        remaining = settle_amount
        curr = currency.strip().upper()

        for lot in self.basis_lots:
            if remaining <= _ZERO:
                break
            if lot.remaining_amount <= _ZERO:
                continue
            if lot.original_currency != curr:
                continue

            settle_from_lot = min(remaining, lot.remaining_amount)
            lot.remaining_amount -= settle_from_lot
            remaining -= settle_from_lot

            cad_at_basis = lot.fx_rate_at_recognition.convert(settle_from_lot)
            cad_at_settlement = settlement_rate.convert(settle_from_lot)
            gain_loss = _round(cad_at_settlement - cad_at_basis)

            gl = GL_FX_GAIN if gain_loss >= _ZERO else GL_FX_LOSS

            self.realized_gains_losses.append(FxGainLoss(
                settlement_event_id=event_id,
                basis_lot_id=lot.lot_id,
                original_currency=curr,
                settled_amount_foreign=settle_from_lot,
                basis_rate=lot.fx_rate_at_recognition,
                settlement_rate=settlement_rate,
                cad_at_basis=cad_at_basis,
                cad_at_settlement=cad_at_settlement,
                realized_gain_loss=gain_loss,
                gl_account=gl,
                complete_basis=True,
            ))

    # =====================================================================
    # Queries
    # =====================================================================

    def outstanding_balance(self, currency: str | None = None) -> Decimal:
        """Sum of remaining amounts across all basis lots."""
        total = _ZERO
        for lot in self.basis_lots:
            if currency and lot.original_currency != currency.upper():
                continue
            total += lot.remaining_amount
        return _round(total)

    def total_realized_gain_loss(self) -> Decimal:
        """Net realized FX gain/loss across all settlements."""
        return _round(sum((gl.realized_gain_loss for gl in self.realized_gains_losses), _ZERO))

    def has_unsupported_events(self) -> bool:
        return any(e.support_status in ("unsupported", "partial") for e in self.events)

    def unsupported_events(self) -> list[FxEvent]:
        return [e for e in self.events if e.support_status in ("unsupported", "partial")]

    def get_ledger_summary(self) -> dict[str, Any]:
        """Full summary — never force-balanced.

        If there are unsupported events, the summary explicitly says
        the ledger is NOT balanced and cannot be balanced automatically.
        """
        unsupported = self.unsupported_events()
        all_supported = len(unsupported) == 0

        total_gl = self.total_realized_gain_loss()
        outstanding = self.outstanding_balance()

        summary: dict[str, Any] = {
            "document_id": self.document_id,
            "base_currency": self.base_currency,
            "event_count": len(self.events),
            "events": [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "date": e.date,
                    "original_currency": e.original_currency,
                    "original_amount": str(e.original_amount),
                    "cad_amount": str(e.cad_amount) if e.cad_amount is not None else None,
                    "support_status": e.support_status,
                    "unsupported_reason": e.unsupported_reason,
                }
                for e in self.events
            ],
            "basis_lots": [
                {
                    "lot_id": lot.lot_id,
                    "original_amount": str(lot.original_amount),
                    "remaining_amount": str(lot.remaining_amount),
                    "original_currency": lot.original_currency,
                    "rate_at_recognition": str(lot.fx_rate_at_recognition.rate),
                    "recognized_date": lot.recognized_date,
                }
                for lot in self.basis_lots
            ],
            "realized_gains_losses": [
                {
                    "settlement_event_id": gl.settlement_event_id,
                    "basis_lot_id": gl.basis_lot_id,
                    "settled_amount_foreign": str(gl.settled_amount_foreign),
                    "cad_at_basis": str(gl.cad_at_basis),
                    "cad_at_settlement": str(gl.cad_at_settlement),
                    "realized_gain_loss": str(gl.realized_gain_loss),
                    "gl_account": gl.gl_account,
                    "complete_basis": gl.complete_basis,
                }
                for gl in self.realized_gains_losses
            ],
            "outstanding_foreign_balance": str(outstanding),
            "total_realized_gain_loss_cad": str(total_gl),
            "ledger_fully_supported": all_supported,
            "ledger_balanced": False,  # computed below
            "balance_warning": None,
        }

        if not all_supported:
            summary["ledger_balanced"] = False
            summary["balance_warning"] = (
                f"Ledger has {len(unsupported)} unsupported/partial event(s). "
                "Automated balancing is impossible — manual journal entries "
                "required for the unsupported portions. The system refuses to "
                "fabricate a balanced ledger from incomplete FX data."
            )
        elif outstanding > _ZERO:
            summary["ledger_balanced"] = False
            summary["balance_warning"] = (
                f"Outstanding foreign-currency balance of {outstanding} "
                "has not been settled. Unrealized gain/loss is NOT computed "
                "(period-end revaluation is a separate process)."
            )
        else:
            summary["ledger_balanced"] = True

        return summary


# =========================================================================
# Convenience: refuse to fabricate gain/loss without basis
# =========================================================================

def compute_realized_fx_gain_loss(
    original_amount: Any,
    original_currency: str,
    original_fx_rate: Any,
    original_fx_date: str | None,
    settlement_amount: Any,
    settlement_currency: str,
    settlement_fx_rate: Any,
    settlement_fx_date: str | None,
) -> dict[str, Any]:
    """Compute realized FX gain/loss for a single settlement.

    Returns UNSUPPORTED if any basis data is missing.  Will NEVER
    fabricate a precise number from incomplete inputs.
    """
    o_curr = (original_currency or "").strip().upper()
    s_curr = (settlement_currency or "").strip().upper()
    o_amt = _to_dec(original_amount)
    s_amt = _to_dec(settlement_amount)
    o_rate = _to_dec(original_fx_rate) if original_fx_rate is not None else None
    s_rate = _to_dec(settlement_fx_rate) if settlement_fx_rate is not None else None

    # Gate: all basis data must be present
    missing: list[str] = []
    if o_amt <= _ZERO:
        missing.append("original_amount (must be positive)")
    if not o_curr:
        missing.append("original_currency")
    if o_rate is None or o_rate <= _ZERO:
        missing.append("original_fx_rate (must be positive)")
    if not original_fx_date:
        missing.append("original_fx_date")
    if s_amt <= _ZERO:
        missing.append("settlement_amount (must be positive)")
    if not s_curr:
        missing.append("settlement_currency")
    if s_rate is None or s_rate <= _ZERO:
        missing.append("settlement_fx_rate (must be positive)")
    if not settlement_fx_date:
        missing.append("settlement_fx_date")

    if missing:
        return {
            "support_status": "unsupported",
            "realized_gain_loss": None,
            "missing_basis_data": missing,
            "reason": (
                "Cannot compute realized FX gain/loss — missing basis data: "
                + ", ".join(missing) + ". "
                "The system refuses to fabricate a precise number from "
                "incomplete inputs."
            ),
        }

    # Cross-currency check
    if o_curr != s_curr and s_curr != "CAD" and o_curr != "CAD":
        return {
            "support_status": "unsupported",
            "realized_gain_loss": None,
            "reason": (
                f"Cross-currency settlement ({o_curr} -> {s_curr}) where "
                f"neither is CAD. Triangulation through CAD requires two "
                f"rates and is not automatically supported. Manual journal "
                f"entry required."
            ),
        }

    # Compute
    cad_at_origin = _round(o_amt * o_rate)
    cad_at_settlement = _round(s_amt * s_rate)

    # For same-currency settlements, gain/loss is the difference
    gain_loss = _round(cad_at_settlement - cad_at_origin)

    return {
        "support_status": "supported",
        "original_currency": o_curr,
        "original_amount": str(o_amt),
        "original_fx_rate": str(o_rate),
        "original_fx_date": original_fx_date,
        "original_cad": str(cad_at_origin),
        "settlement_currency": s_curr,
        "settlement_amount": str(s_amt),
        "settlement_fx_rate": str(s_rate),
        "settlement_fx_date": settlement_fx_date,
        "settlement_cad": str(cad_at_settlement),
        "realized_gain_loss": str(gain_loss),
        "gl_account": GL_FX_GAIN if gain_loss >= _ZERO else GL_FX_LOSS,
        "complete_basis": True,
    }
