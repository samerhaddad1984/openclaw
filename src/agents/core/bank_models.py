from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BankTransaction:
    transaction_id: str
    client_code: Optional[str]
    account_id: Optional[str]
    posted_date: Optional[str]
    description: Optional[str]
    memo: Optional[str]
    amount: Optional[float]
    currency: Optional[str]
    source: Optional[str] = None
    raw_data: Optional[dict] = None


@dataclass
class MatchCandidate:
    document_id: str
    transaction_id: str
    score: float
    status: str
    reasons: list[str] = field(default_factory=list)
    amount_diff: Optional[float] = None
    date_delta_days: Optional[int] = None
    vendor_similarity: Optional[float] = None


@dataclass
class MatchResult:
    document_id: str
    transaction_id: Optional[str]
    status: str
    score: float
    reasons: list[str] = field(default_factory=list)
    amount_diff: Optional[float] = None
    date_delta_days: Optional[int] = None
    vendor_similarity: Optional[float] = None