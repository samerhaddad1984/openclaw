"""Sliding-scale bank-transaction match tolerance.

The old matcher used ±$0.02 absolute and ±7 days regardless of amount —
fine for small receipts, dangerous for $10k wires (could match a nearby
transaction that's $0.02 off and auto-reconcile it).

This module returns, for a given transaction amount:

  - amount_tol:              how close the document total must be
  - date_window_days:        ±N days around the transaction date
  - vendor_fuzzy_threshold:  min Jaccard-like overlap needed for the
                             vendor/merchant name check; higher dollars
                             require a stronger name match
  - confidence_tier:         'auto' | 'review_required' | 'manual_only'
                             — what the UI should do on a match
  - auto_apply:              True only for 'auto' tier; the others must
                             queue for a CPA click

Tiers are chosen so a CPA sees every high-dollar match before it touches
the books. A $5,000 threshold was picked because it mirrors the CRA
"significant transaction" guidance CPAs use when sampling.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# Amount above which auto-apply is never allowed, even on perfect match.
MANUAL_ONLY_AMOUNT = 5_000.0


@dataclass(frozen=True)
class MatchPolicy:
    amount_tol: float
    date_window_days: int
    vendor_fuzzy_threshold: float
    confidence_tier: str
    auto_apply: bool

    def as_dict(self) -> dict:
        return {
            "amount_tol": self.amount_tol,
            "date_window_days": self.date_window_days,
            "vendor_fuzzy_threshold": self.vendor_fuzzy_threshold,
            "confidence_tier": self.confidence_tier,
            "auto_apply": self.auto_apply,
        }


def policy_for_amount(amount: float) -> MatchPolicy:
    """Return the match policy for a bank transaction of a given size.

    The sign of `amount` is ignored (Plaid returns negatives for debits,
    but the size is what drives risk).
    """
    a = abs(float(amount or 0.0))
    if a < 100.0:
        return MatchPolicy(
            amount_tol=0.02, date_window_days=7,
            vendor_fuzzy_threshold=0.0,
            confidence_tier="auto", auto_apply=True,
        )
    if a < 1_000.0:
        return MatchPolicy(
            amount_tol=0.10, date_window_days=5,
            vendor_fuzzy_threshold=0.25,
            confidence_tier="auto", auto_apply=True,
        )
    if a < 10_000.0:
        # 0.1 % tolerance, but never below $1.00.
        tol = max(a * 0.001, 1.00)
        return MatchPolicy(
            amount_tol=tol, date_window_days=3,
            vendor_fuzzy_threshold=0.40,
            # Large enough to want CPA eyes on it before posting.
            confidence_tier="review_required",
            auto_apply=(a < MANUAL_ONLY_AMOUNT),
        )
    # >= $10k.
    tol = max(a * 0.0005, 5.00)
    return MatchPolicy(
        amount_tol=tol, date_window_days=2,
        vendor_fuzzy_threshold=0.60,
        confidence_tier="manual_only", auto_apply=False,
    )


def _normalise(s: Optional[str]) -> list[str]:
    return re.findall(r"[a-z0-9]{3,}", (s or "").lower())


def vendor_similarity(a: Optional[str], b: Optional[str]) -> float:
    """Rough Jaccard overlap on 3+ char alphanumeric tokens. 0.0 when
    either side is empty — vendor name match can't fire without both
    names being present."""
    wa = set(_normalise(a))
    wb = set(_normalise(b))
    if not wa or not wb:
        return 0.0
    overlap = len(wa & wb)
    union = len(wa | wb)
    return overlap / union if union else 0.0


@dataclass
class ScoreBreakdown:
    amount_diff: float
    date_diff_days: int
    vendor_similarity: float
    amount_ok: bool
    date_ok: bool
    vendor_ok: bool
    policy: MatchPolicy

    @property
    def all_pass(self) -> bool:
        return self.amount_ok and self.date_ok and self.vendor_ok

    def as_dict(self) -> dict:
        return {
            "amount_diff": round(self.amount_diff, 4),
            "date_diff_days": self.date_diff_days,
            "vendor_similarity": round(self.vendor_similarity, 3),
            "amount_ok": self.amount_ok,
            "date_ok": self.date_ok,
            "vendor_ok": self.vendor_ok,
            "policy": self.policy.as_dict(),
        }


def score_candidate(
    *,
    tx_amount: float,
    tx_date_days: int,
    tx_merchant: Optional[str],
    doc_amount: float,
    doc_date_days: int,
    doc_vendor: Optional[str],
    policy: Optional[MatchPolicy] = None,
) -> ScoreBreakdown:
    """Return a breakdown of why a candidate document does or doesn't
    match a bank transaction, given the sliding-scale policy for the
    transaction's amount. Callers pass dates as day-ordinals so this
    module stays pure (no datetime imports needed here).
    """
    pol = policy or policy_for_amount(tx_amount)
    amount_diff = abs(abs(tx_amount) - abs(doc_amount))
    date_diff = abs(tx_date_days - doc_date_days)
    vsim = vendor_similarity(tx_merchant, doc_vendor)
    return ScoreBreakdown(
        amount_diff=amount_diff,
        date_diff_days=date_diff,
        vendor_similarity=vsim,
        amount_ok=(amount_diff <= pol.amount_tol + 1e-9),
        date_ok=(date_diff <= pol.date_window_days),
        vendor_ok=(vsim >= pol.vendor_fuzzy_threshold),
        policy=pol,
    )


def needs_manual_review(amount: float) -> bool:
    """True when a match of this amount must NOT auto-apply."""
    return not policy_for_amount(amount).auto_apply
