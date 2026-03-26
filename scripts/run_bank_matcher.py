from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.bank_models import BankTransaction
from src.agents.core.task_store import TaskStore
from src.agents.tools.bank_matcher import BankMatcher


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
SAMPLE_TXNS_PATH = ROOT_DIR / "tests" / "sample_bank_transactions.json"


def load_transactions(path: Path) -> list[BankTransaction]:
    if not path.exists():
        raise FileNotFoundError(f"Sample bank transactions file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("sample_bank_transactions.json must contain a JSON array")

    txns: list[BankTransaction] = []
    for row in data:
        txns.append(
            BankTransaction(
                transaction_id=str(row.get("transaction_id")),
                client_code=row.get("client_code"),
                account_id=row.get("account_id"),
                posted_date=row.get("posted_date"),
                description=row.get("description"),
                memo=row.get("memo"),
                amount=float(row["amount"]) if row.get("amount") is not None else None,
                currency=row.get("currency"),
                source=row.get("source"),
                raw_data=row,
            )
        )
    return txns


def main():
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    store = TaskStore(DB_PATH)
    matcher = BankMatcher()

    documents = [
        d for d in store.list_documents()
        if (d.review_status or "").strip().lower() == "ready"
    ]

    transactions = load_transactions(SAMPLE_TXNS_PATH)
    results = matcher.match_documents(documents, transactions)

    output = []
    matched = 0
    suggested = 0
    unmatched = 0

    doc_map = {d.document_id: d for d in documents}
    txn_map = {t.transaction_id: t for t in transactions}

    for r in results:
        doc = doc_map.get(r.document_id)
        txn = txn_map.get(r.transaction_id) if r.transaction_id else None

        output.append(
            {
                "document_id": r.document_id,
                "file_name": doc.file_name if doc else None,
                "vendor": doc.vendor if doc else None,
                "client_code": doc.client_code if doc else None,
                "document_amount": doc.amount if doc else None,
                "document_date": doc.document_date if doc else None,
                "transaction_id": r.transaction_id,
                "transaction_description": txn.description if txn else None,
                "transaction_amount": txn.amount if txn else None,
                "transaction_date": txn.posted_date if txn else None,
                "status": r.status,
                "score": r.score,
                "reasons": r.reasons,
                "amount_diff": r.amount_diff,
                "date_delta_days": r.date_delta_days,
                "vendor_similarity": r.vendor_similarity,
            }
        )

        if r.status == "matched":
            matched += 1
        elif r.status == "suggested":
            suggested += 1
        else:
            unmatched += 1

    out_path = EXPORTS_DIR / "bank_matches.json"
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    print()
    print("BANK MATCH RUN")
    print("=" * 80)
    print(f"Documents loaded     : {len(documents)}")
    print(f"Transactions loaded  : {len(transactions)}")
    print(f"Matched              : {matched}")
    print(f"Suggested            : {suggested}")
    print(f"Unmatched            : {unmatched}")
    print()

    for item in output[:15]:
        print(
            f"{item['file_name']} | {item['transaction_id']} | {item['status']} | "
            f"score={item['score']}"
        )

    print()
    print(f"Saved to: {out_path}")


if __name__ == "__main__":
    main()