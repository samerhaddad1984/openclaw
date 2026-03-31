from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore  # noqa: E402


DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORT_DIR = ROOT_DIR / "exports"
OUTPUT_FILE = EXPORT_DIR / "exception_queue.json"


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def load_raw_result(document) -> dict[str, Any]:
    raw = getattr(document, "raw_result", None)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def has_processing_exception(raw_result: dict[str, Any]) -> bool:
    errors = raw_result.get("errors", [])
    if not isinstance(errors, list):
        return False
    return len(errors) > 0


def get_first_error(raw_result: dict[str, Any]) -> str | None:
    errors = raw_result.get("errors", [])
    if isinstance(errors, list) and errors:
        return clean_text(errors[0])
    return None


def has_non_accounting_ignore_note(raw_result: dict[str, Any]) -> tuple[bool, str | None]:
    notes = raw_result.get("notes", [])
    if not isinstance(notes, list):
        return False, None

    for note in notes:
        note_text = clean_text(note)
        if not note_text:
            continue
        if note_text.startswith("non_accounting:"):
            return True, note_text

    return False, None


def build_work_item(
    *,
    severity: str,
    bucket: str,
    action: str,
    document,
    raw_result: dict[str, Any],
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "bucket": bucket,
        "action": action,
        "document_id": clean_text(getattr(document, "document_id", None)),
        "file_name": clean_text(getattr(document, "file_name", None)),
        "file_path": clean_text(getattr(document, "file_path", None)),
        "review_status": clean_text(getattr(document, "review_status", None)),
        "vendor": clean_text(getattr(document, "vendor", None)),
        "client_code": clean_text(getattr(document, "client_code", None)),
        "doc_type": clean_text(getattr(document, "doc_type", None)),
        "amount": safe_float(getattr(document, "amount", None)),
        "document_date": clean_text(getattr(document, "document_date", None)),
        "gl_account": clean_text(getattr(document, "gl_account", None)),
        "confidence": safe_float(getattr(document, "confidence", None)),
        "reason": clean_text(reason),
        "raw_error": get_first_error(raw_result),
    }


def sort_key(item: dict[str, Any]) -> tuple:
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    file_name = item.get("file_name") or ""
    bucket = item.get("bucket") or ""
    return (severity_rank.get(item.get("severity"), 9), bucket, file_name.lower())


def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    store = TaskStore(DB_PATH)
    documents = store.list_documents()

    work_items: list[dict[str, Any]] = []

    for document in documents:
        raw_result = load_raw_result(document)

        review_status = clean_text(getattr(document, "review_status", None)) or "NeedsReview"
        vendor = clean_text(getattr(document, "vendor", None))
        client_code = clean_text(getattr(document, "client_code", None))
        doc_type = clean_text(getattr(document, "doc_type", None))
        document_date = clean_text(getattr(document, "document_date", None))
        amount = safe_float(getattr(document, "amount", None))

        ignored, ignore_note = has_non_accounting_ignore_note(raw_result)
        if review_status == "Ignored" and ignored:
            continue

        if has_processing_exception(raw_result):
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="processing_exception",
                    action="Reprocess document with working OCR/PDF extraction or request a cleaner source file.",
                    document=document,
                    raw_result=raw_result,
                    reason=get_first_error(raw_result),
                )
            )

        if vendor is None and review_status != "Ignored":
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="missing_vendor",
                    action="Identify the vendor and add or improve vendor rules if this is a repeat document.",
                    document=document,
                    raw_result=raw_result,
                    reason="vendor is blank",
                )
            )

        if client_code is None and review_status != "Ignored":
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="missing_client",
                    action="Assign the correct client before any bookkeeping export or posting.",
                    document=document,
                    raw_result=raw_result,
                    reason="client_code is blank",
                )
            )

        if doc_type is None and review_status != "Ignored":
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="missing_doc_type",
                    action="Classify the document type so posting logic can treat it correctly.",
                    document=document,
                    raw_result=raw_result,
                    reason="doc_type is blank",
                )
            )

        if amount is None and review_status != "Ignored":
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="missing_amount",
                    action="Extract or confirm the total amount before booking.",
                    document=document,
                    raw_result=raw_result,
                    reason="amount is blank",
                )
            )

        if amount == 0.0 and review_status != "Ignored":
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="zero_amount",
                    action="Confirm whether this is a true zero-dollar document or a failed extraction.",
                    document=document,
                    raw_result=raw_result,
                    reason="amount is 0.0",
                )
            )

        if review_status != "Ignored" and document_date is None:
            work_items.append(
                build_work_item(
                    severity="medium",
                    bucket="missing_document_date",
                    action="Add or safely infer the document date before final posting.",
                    document=document,
                    raw_result=raw_result,
                    reason="document_date is blank",
                )
            )

        if review_status == "NeedsReview":
            work_items.append(
                build_work_item(
                    severity="medium",
                    bucket="needs_human_review",
                    action="Human should review, correct if needed, and approve or ignore.",
                    document=document,
                    raw_result=raw_result,
                    reason="review_status is NeedsReview",
                )
            )

        if review_status == "Exception" and not has_processing_exception(raw_result):
            work_items.append(
                build_work_item(
                    severity="high",
                    bucket="exception_status",
                    action="Investigate why this document was marked Exception and either fix extraction or reclassify it.",
                    document=document,
                    raw_result=raw_result,
                    reason="review_status is Exception",
                )
            )

    work_items = sorted(work_items, key=sort_key)

    status_counter: Counter[str] = Counter()
    bucket_counter: Counter[str] = Counter()

    for item in work_items:
        status_counter[item["severity"]] += 1
        bucket_counter[item["bucket"]] += 1

    payload = {
        "summary": {
            "database": str(DB_PATH),
            "total_documents": len(documents),
            "work_item_count": len(work_items),
            "severity_counts": dict(status_counter),
            "bucket_counts": dict(bucket_counter),
        },
        "work_items": work_items,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print("")
    print("EXCEPTION QUEUE BUILD COMPLETED")
    print("=" * 80)
    print(f"Work items: {len(work_items)}")
    print("")
    print("BY SEVERITY")
    print("-" * 80)
    if status_counter:
        for severity, count in status_counter.most_common():
            print(f"{severity:<10} {count}")
    else:
        print("No work items.")

    print("")
    print("BY BUCKET")
    print("-" * 80)
    if bucket_counter:
        for bucket, count in bucket_counter.most_common():
            print(f"{bucket:<25} {count}")
    else:
        print("No buckets.")

    print("")
    print("TOP ITEMS")
    print("-" * 80)
    if work_items:
        for item in work_items[:25]:
            print(
                f"{item['severity']:<6} | "
                f"{item['bucket']:<25} | "
                f"{(item.get('file_name') or ''):<40} | "
                f"vendor={item.get('vendor')} | "
                f"client={item.get('client_code')} | "
                f"amount={item.get('amount')}"
            )
    else:
        print("No work items.")

    print("")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()