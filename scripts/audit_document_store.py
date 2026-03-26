from __future__ import annotations

from collections import Counter
from pathlib import Path
import json
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore  # noqa: E402


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
EXPORT_DIR = ROOT_DIR / "exports"


def is_blank(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def main():
    EXPORT_DIR.mkdir(exist_ok=True)

    store = TaskStore(DB_PATH)
    docs = store.list_documents()

    issues = []
    issue_counter = Counter()

    for d in docs:
        doc_issues = []

        if is_blank(getattr(d, "document_id", None)):
            doc_issues.append("missing_document_id")

        if is_blank(getattr(d, "file_name", None)):
            doc_issues.append("missing_file_name")

        if is_blank(getattr(d, "review_status", None)):
            doc_issues.append("missing_review_status")

        if getattr(d, "review_status", None) in {"Ready", "NeedsReview", "Exception"}:
            if is_blank(getattr(d, "vendor", None)):
                doc_issues.append("missing_vendor")

            if is_blank(getattr(d, "doc_type", None)):
                doc_issues.append("missing_doc_type")

        if getattr(d, "review_status", None) == "Ready":
            if is_blank(getattr(d, "client_code", None)):
                doc_issues.append("missing_client_code_on_ready")

            if getattr(d, "amount", None) is None:
                doc_issues.append("missing_amount_on_ready")

            if is_blank(getattr(d, "gl_account", None)):
                doc_issues.append("missing_gl_account_on_ready")

        if getattr(d, "amount", None) == 0:
            doc_issues.append("zero_amount")

        if doc_issues:
            for issue in doc_issues:
                issue_counter[issue] += 1

            issues.append(
                {
                    "document_id": getattr(d, "document_id", None),
                    "file_name": getattr(d, "file_name", None),
                    "review_status": getattr(d, "review_status", None),
                    "vendor": getattr(d, "vendor", None),
                    "client_code": getattr(d, "client_code", None),
                    "doc_type": getattr(d, "doc_type", None),
                    "amount": getattr(d, "amount", None),
                    "document_date": getattr(d, "document_date", None),
                    "gl_account": getattr(d, "gl_account", None),
                    "issues": doc_issues,
                }
            )

    export_file = EXPORT_DIR / "document_store_audit.json"
    with open(export_file, "w", encoding="utf-8") as f:
        json.dump(issues, f, indent=2, ensure_ascii=False)

    print("")
    print("DOCUMENT STORE AUDIT")
    print("=" * 80)
    print(f"Total documents checked: {len(docs)}")
    print(f"Documents with issues: {len(issues)}")
    print("")

    print("ISSUE COUNTS")
    print("-" * 80)
    for issue, count in issue_counter.most_common():
        print(f"{issue:<30} {count}")

    print("")
    print("TOP BAD RECORDS")
    print("-" * 80)
    for item in issues[:25]:
        print(
            f"{str(item['file_name']):<40} | "
            f"status={item['review_status']} | "
            f"vendor={item['vendor']} | "
            f"client={item['client_code']} | "
            f"amount={item['amount']} | "
            f"issues={','.join(item['issues'])}"
        )

    print("")
    print(f"Saved to: {export_file}")


if __name__ == "__main__":
    main()