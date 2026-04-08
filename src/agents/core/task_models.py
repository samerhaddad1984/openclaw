from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional
import json
import uuid


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


@dataclass
class Task:
    task_id: str
    task_type: str
    status: str
    priority: int
    client_code: Optional[str]
    file_path: Optional[str]
    payload: dict[str, Any]
    result: Optional[dict[str, Any]]
    error: Optional[str]
    created_at: str
    updated_at: str

    @staticmethod
    def create(
        *,
        task_type: str,
        priority: int = 100,
        client_code: Optional[str] = None,
        file_path: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> "Task":
        now = utc_now_iso()
        return Task(
            task_id=new_id("task"),
            task_type=task_type,
            status="pending",
            priority=priority,
            client_code=client_code,
            file_path=file_path,
            payload=payload or {},
            result=None,
            error=None,
            created_at=now,
            updated_at=now,
        )

    def to_row(self) -> dict[str, Any]:
        data = asdict(self)
        data["payload"] = json.dumps(self.payload, ensure_ascii=False)
        data["result"] = json.dumps(self.result, ensure_ascii=False) if self.result is not None else None
        return data

    @staticmethod
    def from_row(row: dict[str, Any]) -> "Task":
        return Task(
            task_id=row["task_id"],
            task_type=row["task_type"],
            status=row["status"],
            priority=int(row["priority"]),
            client_code=row["client_code"],
            file_path=row["file_path"],
            payload=json.loads(row["payload"]) if row["payload"] else {},
            result=json.loads(row["result"]) if row["result"] else None,
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass
class DocumentRecord:
    document_id: str
    file_name: str
    file_path: str
    client_code: Optional[str]
    vendor: Optional[str]
    doc_type: Optional[str]
    amount: Optional[float]
    document_date: Optional[str]
    gl_account: Optional[str]
    tax_code: Optional[str]
    category: Optional[str]
    review_status: str
    confidence: float
    raw_result: dict[str, Any]
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    @staticmethod
    def from_pipeline_result(result: dict[str, Any]) -> "DocumentRecord":
        return DocumentRecord(
            document_id=new_id("doc"),
            file_name=result.get("file_name") or "",
            file_path=result.get("file_path") or "",
            client_code=result.get("client_code"),
            vendor=result.get("vendor"),
            doc_type=result.get("doc_type"),
            amount=result.get("amount"),
            document_date=result.get("document_date"),
            gl_account=result.get("gl_account"),
            tax_code=result.get("tax_code"),
            category=result.get("category"),
            review_status=result.get("review_status") or "NeedsReview",
            confidence=float(result.get("effective_confidence") or 0.0),
            raw_result=result,
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "file_name": self.file_name,
            "file_path": self.file_path,
            "client_code": self.client_code,
            "vendor": self.vendor,
            "doc_type": self.doc_type,
            "amount": self.amount,
            "document_date": self.document_date,
            "gl_account": self.gl_account,
            "tax_code": self.tax_code,
            "category": self.category,
            "review_status": self.review_status,
            "confidence": self.confidence,
            "raw_result": json.dumps(self.raw_result, ensure_ascii=False),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @staticmethod
    def from_row(row: dict[str, Any]) -> "DocumentRecord":
        return DocumentRecord(
            document_id=row["document_id"],
            file_name=row["file_name"],
            file_path=row["file_path"],
            client_code=row["client_code"],
            vendor=row["vendor"],
            doc_type=row["doc_type"],
            amount=row["amount"],
            document_date=row["document_date"],
            gl_account=row["gl_account"],
            tax_code=row["tax_code"],
            category=row["category"],
            review_status=row["review_status"],
            confidence=float(row["confidence"] or 0.0),
            raw_result=json.loads(row["raw_result"]) if row["raw_result"] else {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )