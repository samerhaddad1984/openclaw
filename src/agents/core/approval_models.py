from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from uuid import uuid4
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_decision_id() -> str:
    return f"decision_{uuid4().hex[:12]}"


@dataclass
class MatchDecision:
    decision_id: str
    document_id: str
    decision_type: str
    chosen_transaction_id: Optional[str]
    reviewer: Optional[str]
    reason: Optional[str]
    notes: Optional[str]
    created_at: str
    updated_at: str

    def to_row(self) -> dict:
        return {
            "decision_id": self.decision_id,
            "document_id": self.document_id,
            "decision_type": self.decision_type,
            "chosen_transaction_id": self.chosen_transaction_id,
            "reviewer": self.reviewer,
            "reason": self.reason,
            "notes": self.notes,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: dict) -> "MatchDecision":
        return cls(
            decision_id=row["decision_id"],
            document_id=row["document_id"],
            decision_type=row["decision_type"],
            chosen_transaction_id=row.get("chosen_transaction_id"),
            reviewer=row.get("reviewer"),
            reason=row.get("reason"),
            notes=row.get("notes"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass
class MatchReviewItem:
    document_id: str
    file_name: Optional[str]
    vendor: Optional[str]
    client_code: Optional[str]
    amount: Optional[float]
    document_date: Optional[str]
    suggested_transaction_id: Optional[str]
    suggested_status: Optional[str]
    suggested_score: Optional[float]
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "document_id": self.document_id,
            "file_name": self.file_name,
            "vendor": self.vendor,
            "client_code": self.client_code,
            "amount": self.amount,
            "document_date": self.document_date,
            "suggested_transaction_id": self.suggested_transaction_id,
            "suggested_status": self.suggested_status,
            "suggested_score": self.suggested_score,
            "reasons": self.reasons,
        }


ALLOWED_DECISION_TYPES = {
    "approve_match",
    "reject_match",
    "reassign_match",
    "mark_unmatched",
    "ignore_document",
    "mark_personal",
}


def validate_decision_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in ALLOWED_DECISION_TYPES:
        raise ValueError(
            f"Invalid decision_type '{value}'. Allowed values: {sorted(ALLOWED_DECISION_TYPES)}"
        )
    return normalized


def make_match_decision(
    *,
    document_id: str,
    decision_type: str,
    chosen_transaction_id: Optional[str] = None,
    reviewer: Optional[str] = None,
    reason: Optional[str] = None,
    notes: Optional[str] = None,
) -> MatchDecision:
    now = utc_now_iso()
    return MatchDecision(
        decision_id=new_decision_id(),
        document_id=document_id,
        decision_type=validate_decision_type(decision_type),
        chosen_transaction_id=chosen_transaction_id,
        reviewer=reviewer,
        reason=reason,
        notes=notes,
        created_at=now,
        updated_at=now,
    )