from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Optional
import json
import sys

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore  # noqa: E402
from src.agents.tools.duplicate_detector import find_duplicate_candidates_from_store  # noqa: E402


@dataclass
class ExceptionWorkItem:
    document_id: str
    file_name: str
    review_status: Optional[str]
    client_code: Optional[str]
    vendor: Optional[str]
    doc_type: Optional[str]
    amount: Optional[float]
    document_date: Optional[str]
    bucket: str
    severity: str
    recommended_action: str
    details: dict


def _contains_ocr_failure(errors: list[str]) -> bool:
    joined = " ".join(errors).lower()
    return "ocr failed" in joined or "text extraction failed" in joined or "poppler" in joined


def _normalize_errors(errors: Any) -> list[str]:
    if not errors:
        return []
    if isinstance(errors, list):
        return [str(x) for x in errors]
    return [str(errors)]


def build_exception_items_for_document(doc: Any, duplicate_map: dict[str, list[dict]]) -> list[ExceptionWorkItem]:
    items: list[ExceptionWorkItem] = []

    document_id = getattr(doc, "document_id")
    file_name = getattr(doc, "file_name", "")
    review_status = getattr(doc, "review_status", None)
    client_code = getattr(doc, "client_code", None)
    vendor = getattr(doc, "vendor", None)
    doc_type = getattr(doc, "doc_type", None)
    amount = getattr(doc, "amount", None)
    document_date = getattr(doc, "document_date", None)
    errors = _normalize_errors(getattr(doc, "errors", []))
    notes = getattr(doc, "notes", []) or []

    if _contains_ocr_failure(errors):
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="ocr_failed",
                severity="high",
                recommended_action="Reprocess document with OCR support or request a better PDF/image.",
                details={
                    "errors": errors,
                },
            )
        )

    if review_status == "Exception" and not _contains_ocr_failure(errors):
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="processing_exception",
                severity="high",
                recommended_action="Open document and manually review the extraction failure.",
                details={
                    "errors": errors,
                },
            )
        )

    if review_status == "NeedsReview":
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="needs_human_review",
                severity="medium",
                recommended_action="Review and approve, edit, or ignore this transaction.",
                details={
                    "notes": notes,
                    "errors": errors,
                },
            )
        )

    if client_code is None and review_status not in {"Ignored"}:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="missing_client",
                severity="high",
                recommended_action="Assign the correct client before posting.",
                details={},
            )
        )

    if vendor is None and review_status not in {"Ignored"}:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="missing_vendor",
                severity="high",
                recommended_action="Identify the vendor and update rules if this is a repeat vendor.",
                details={},
            )
        )

    if doc_type is None and review_status not in {"Ignored"}:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="missing_doc_type",
                severity="medium",
                recommended_action="Classify the document type before posting.",
                details={},
            )
        )

    if amount is None and review_status not in {"Ignored"}:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="missing_amount",
                severity="high",
                recommended_action="Confirm the total amount manually or improve extraction rules.",
                details={},
            )
        )

    if amount == 0 and review_status not in {"Ignored"}:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="zero_amount",
                severity="medium",
                recommended_action="Confirm whether this is a valid zero-dollar invoice, credit, or extraction problem.",
                details={},
            )
        )

    if document_date is None and review_status not in {"Ignored"}:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="missing_document_date",
                severity="medium",
                recommended_action="Add the transaction date or enable vendor-specific date fallback safely.",
                details={},
            )
        )

    dup_hits = duplicate_map.get(document_id, [])
    if dup_hits:
        items.append(
            ExceptionWorkItem(
                document_id=document_id,
                file_name=file_name,
                review_status=review_status,
                client_code=client_code,
                vendor=vendor,
                doc_type=doc_type,
                amount=amount,
                document_date=document_date,
                bucket="possible_duplicate",
                severity="high",
                recommended_action="Review duplicate candidates before posting or exporting.",
                details={
                    "matches": dup_hits,
                },
            )
        )

    return items


def build_exception_queue(store: TaskStore) -> list[ExceptionWorkItem]:
    docs = store.list_documents()
    duplicate_candidates = find_duplicate_candidates_from_store(store, min_score=0.85)

    duplicate_map: dict[str, list[dict]] = {}

    for c in duplicate_candidates:
        left_entry = {
            "other_document_id": c.right_document_id,
            "other_file_name": c.right_file_name,
            "score": c.score,
            "reasons": c.reasons,
        }
        right_entry = {
            "other_document_id": c.left_document_id,
            "other_file_name": c.left_file_name,
            "score": c.score,
            "reasons": c.reasons,
        }

        duplicate_map.setdefault(c.left_document_id, []).append(left_entry)
        duplicate_map.setdefault(c.right_document_id, []).append(right_entry)

    work_items: list[ExceptionWorkItem] = []
    for doc in docs:
        work_items.extend(build_exception_items_for_document(doc, duplicate_map))

    work_items.sort(
        key=lambda x: (
            {"high": 0, "medium": 1, "low": 2}.get(x.severity, 9),
            x.bucket,
            x.file_name.lower(),
        )
    )
    return work_items


def exception_queue_to_json(items: list[ExceptionWorkItem]) -> str:
    return json.dumps([asdict(i) for i in items], indent=2, ensure_ascii=False)