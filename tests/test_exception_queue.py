from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.exception_queue import build_exception_items_for_document  # noqa: E402


@dataclass
class FakeDocument:
    document_id: str
    file_name: str
    review_status: str | None
    client_code: str | None
    vendor: str | None
    doc_type: str | None
    amount: float | None
    document_date: str | None
    errors: list[str]
    notes: list[str]


def main():
    duplicate_map = {
        "doc-1": [
            {
                "other_document_id": "doc-2",
                "other_file_name": "amazon_copy.pdf",
                "score": 0.97,
                "reasons": ["same_vendor_exact", "same_amount", "same_date"],
            }
        ]
    }

    doc = FakeDocument(
        document_id="doc-1",
        file_name="amazon_original.pdf",
        review_status="NeedsReview",
        client_code=None,
        vendor=None,
        doc_type=None,
        amount=None,
        document_date=None,
        errors=["PDF text extraction failed: OCR failed: Unable to get page count."],
        notes=[],
    )

    items = build_exception_items_for_document(doc, duplicate_map)

    print("")
    print("EXCEPTION QUEUE TEST")
    print("=" * 80)
    print(f"Items found: {len(items)}")
    print("")

    for item in items:
        print(
            f"{item.severity:<6} | "
            f"{item.bucket:<24} | "
            f"{item.file_name} | "
            f"action={item.recommended_action}"
        )

    print("")

    buckets = {item.bucket for item in items}
    expected = {
        "ocr_failed",
        "needs_human_review",
        "missing_client",
        "missing_vendor",
        "missing_doc_type",
        "missing_amount",
        "missing_document_date",
        "possible_duplicate",
    }

    missing = expected - buckets
    if missing:
        print(f"FAIL: missing expected buckets: {sorted(missing)}")
    else:
        print("PASS: expected buckets found")


if __name__ == "__main__":
    main()