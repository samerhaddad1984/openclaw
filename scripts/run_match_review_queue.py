from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore
from src.agents.core.approval_store import ApprovalStore


DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
BANK_MATCHES_PATH = EXPORTS_DIR / "bank_matches.json"
OUTPUT_QUEUE_PATH = EXPORTS_DIR / "match_review_queue.json"


def load_bank_matches():
    if not BANK_MATCHES_PATH.exists():
        raise FileNotFoundError(
            f"bank_matches.json not found. Run bank matcher first: {BANK_MATCHES_PATH}"
        )

    data = json.loads(BANK_MATCHES_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("bank_matches.json must contain a JSON array")

    return data


def build_review_queue(store: TaskStore, approval_store: ApprovalStore, matches: list):
    documents = store.list_documents()
    doc_map = {d.document_id: d for d in documents}

    queue = []

    for m in matches:

        document_id = m.get("document_id")
        doc = doc_map.get(document_id)

        if not doc:
            continue

        latest_decision = approval_store.get_latest_decision_for_document(document_id)

        if latest_decision:
            decision_status = latest_decision.decision_type
        else:
            decision_status = "pending"

        queue_item = {
            "document_id": document_id,
            "file_name": doc.file_name,
            "vendor": doc.vendor,
            "client_code": doc.client_code,
            "amount": doc.amount,
            "document_date": doc.document_date,
            "match_status": m.get("status"),
            "match_score": m.get("score"),
            "transaction_id": m.get("transaction_id"),
            "transaction_description": m.get("transaction_description"),
            "transaction_amount": m.get("transaction_amount"),
            "transaction_date": m.get("transaction_date"),
            "reasons": m.get("reasons"),
            "decision_status": decision_status,
        }

        queue.append(queue_item)

    return queue


def main():

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    store = TaskStore(DB_PATH)
    approval_store = ApprovalStore(DB_PATH)

    matches = load_bank_matches()

    queue = build_review_queue(store, approval_store, matches)

    OUTPUT_QUEUE_PATH.write_text(
        json.dumps(queue, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    pending = 0
    approved = 0
    rejected = 0

    for q in queue:
        status = q["decision_status"]

        if status == "pending":
            pending += 1
        elif status == "approve_match":
            approved += 1
        elif status == "reject_match":
            rejected += 1

    print()
    print("MATCH REVIEW QUEUE")
    print("=" * 80)
    print(f"Total items      : {len(queue)}")
    print(f"Pending review   : {pending}")
    print(f"Approved matches : {approved}")
    print(f"Rejected matches : {rejected}")
    print()

    for item in queue[:15]:
        print(
            f"{item['file_name']} | "
            f"{item['match_status']} | "
            f"{item['transaction_id']} | "
            f"decision={item['decision_status']}"
        )

    print()
    print(f"Saved queue to: {OUTPUT_QUEUE_PATH}")


if __name__ == "__main__":
    main()