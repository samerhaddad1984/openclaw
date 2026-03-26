from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.approval_models import make_match_decision
from src.agents.core.approval_store import ApprovalStore
from src.agents.core.task_store import TaskStore


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
MATCH_REVIEW_QUEUE_PATH = EXPORTS_DIR / "match_review_queue.json"
DECISION_EXPORT_PATH = EXPORTS_DIR / "match_decisions.json"


DECISION_TO_DOCUMENT_STATUS = {
    "approve_match": "Approved",
    "reject_match": "NeedsReview",
    "reassign_match": "Approved",
    "mark_unmatched": "NeedsReview",
    "ignore_document": "Ignored",
    "mark_personal": "Ignored",
}


def load_review_queue() -> list[dict]:
    if not MATCH_REVIEW_QUEUE_PATH.exists():
        return []

    data = json.loads(MATCH_REVIEW_QUEUE_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("match_review_queue.json must contain a JSON array")

    return data


def save_review_queue(queue: list[dict]) -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MATCH_REVIEW_QUEUE_PATH.write_text(
        json.dumps(queue, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def export_all_decisions(approval_store: ApprovalStore) -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    decisions = [d.to_row() for d in approval_store.list_decisions()]
    DECISION_EXPORT_PATH.write_text(
        json.dumps(decisions, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def find_queue_item(queue: list[dict], document_id: str) -> dict | None:
    for item in queue:
        if item.get("document_id") == document_id:
            return item
    return None


def update_queue_item(
    queue: list[dict],
    *,
    document_id: str,
    decision_type: str,
    chosen_transaction_id: str | None,
) -> None:
    item = find_queue_item(queue, document_id)
    if not item:
        return

    item["decision_status"] = decision_type

    if decision_type in {"approve_match", "reassign_match"}:
        item["final_transaction_id"] = chosen_transaction_id
    elif decision_type in {"reject_match", "mark_unmatched", "ignore_document", "mark_personal"}:
        item["final_transaction_id"] = None


def ensure_document_exists(store: TaskStore, document_id: str):
    doc = store.get_document(document_id)
    if not doc:
        raise ValueError(f"Document not found: {document_id}")
    return doc


def apply_document_status(store: TaskStore, document_id: str, decision_type: str) -> None:
    new_status = DECISION_TO_DOCUMENT_STATUS[decision_type]
    store.update_document_status(document_id, new_status)


def validate_reassign_inputs(decision_type: str, chosen_transaction_id: str | None) -> None:
    if decision_type == "reassign_match" and not chosen_transaction_id:
        raise ValueError("reassign_match requires --transaction-id")

    if decision_type == "approve_match" and chosen_transaction_id is None:
        # approve_match can keep existing suggested transaction from queue
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply a reviewer decision to a matched document."
    )
    parser.add_argument(
        "--document-id",
        required=True,
        help="Target document_id",
    )
    parser.add_argument(
        "--decision",
        required=True,
        choices=[
            "approve_match",
            "reject_match",
            "reassign_match",
            "mark_unmatched",
            "ignore_document",
            "mark_personal",
        ],
        help="Decision to apply",
    )
    parser.add_argument(
        "--transaction-id",
        default=None,
        help="Chosen transaction_id for approve_match or reassign_match",
    )
    parser.add_argument(
        "--reviewer",
        default="human_reviewer",
        help="Reviewer name or identifier",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="Short reason for the decision",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional free-text notes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    store = TaskStore(DB_PATH)
    approval_store = ApprovalStore(DB_PATH)
    queue = load_review_queue()

    doc = ensure_document_exists(store, args.document_id)

    queue_item = find_queue_item(queue, args.document_id)

    chosen_transaction_id = args.transaction_id

    if args.decision == "approve_match" and not chosen_transaction_id and queue_item:
        chosen_transaction_id = queue_item.get("transaction_id")

    validate_reassign_inputs(args.decision, chosen_transaction_id)

    decision = make_match_decision(
        document_id=doc.document_id,
        decision_type=args.decision,
        chosen_transaction_id=chosen_transaction_id,
        reviewer=args.reviewer,
        reason=args.reason,
        notes=args.notes,
    )
    approval_store.add_decision(decision)

    apply_document_status(store, doc.document_id, args.decision)

    update_queue_item(
        queue,
        document_id=doc.document_id,
        decision_type=args.decision,
        chosen_transaction_id=chosen_transaction_id,
    )
    save_review_queue(queue)
    export_all_decisions(approval_store)

    print()
    print("MATCH DECISION APPLIED")
    print("=" * 80)
    print(f"Document ID       : {doc.document_id}")
    print(f"File name         : {doc.file_name}")
    print(f"Decision          : {decision.decision_type}")
    print(f"Transaction ID    : {decision.chosen_transaction_id}")
    print(f"Reviewer          : {decision.reviewer}")
    print(f"Reason            : {decision.reason}")
    print(f"Notes             : {decision.notes}")
    print(f"Document status   : {DECISION_TO_DOCUMENT_STATUS[decision.decision_type]}")
    print()
    print(f"Updated queue     : {MATCH_REVIEW_QUEUE_PATH}")
    print(f"Decision export   : {DECISION_EXPORT_PATH}")


if __name__ == "__main__":
    main()