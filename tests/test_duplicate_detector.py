from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.duplicate_detector import find_duplicate_candidates  # noqa: E402


@dataclass
class FakeDocument:
    document_id: str
    file_name: str
    vendor: str | None
    amount: float | None
    document_date: str | None
    client_code: str | None
    review_status: str


def main():
    docs = [
        FakeDocument(
            document_id="1",
            file_name="amazon_1.pdf",
            vendor="Amazon.com.ca ULC",
            amount=106.77,
            document_date="2026-02-04",
            client_code="SOUSSOL",
            review_status="Ready",
        ),
        FakeDocument(
            document_id="2",
            file_name="amazon_2.pdf",
            vendor="Amazon.com.ca ULC",
            amount=106.77,
            document_date="2026-02-04",
            client_code="SOUSSOL",
            review_status="Ready",
        ),
        FakeDocument(
            document_id="3",
            file_name="microsoft_1.pdf",
            vendor="Microsoft Canada Inc.",
            amount=28.14,
            document_date="2026-02-20",
            client_code="SOUSSOL",
            review_status="Ready",
        ),
        FakeDocument(
            document_id="4",
            file_name="companycam_1.pdf",
            vendor="CompanyCam",
            amount=49.00,
            document_date="2026-03-06",
            client_code="SOUSSOL",
            review_status="Ready",
        ),
    ]

    candidates = find_duplicate_candidates(docs, min_score=0.85)

    print("")
    print("DUPLICATE DETECTOR TEST")
    print("=" * 80)
    print(f"Candidates found: {len(candidates)}")
    print("")

    for c in candidates:
        print(
            f"{c.score} | "
            f"{c.left_file_name} <-> {c.right_file_name} | "
            f"{c.left_vendor} | {c.left_amount} | {c.left_date} | "
            f"reasons={c.reasons}"
        )

    print("")

    strong = [
        c for c in candidates
        if c.left_document_id == "1" and c.right_document_id == "2"
    ]

    if strong:
        print("PASS: expected duplicate pair found")
    else:
        print("FAIL: expected duplicate pair not found")


if __name__ == "__main__":
    main()