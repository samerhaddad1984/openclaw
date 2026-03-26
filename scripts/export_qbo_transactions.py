from pathlib import Path
import json
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
EXPORT_DIR = ROOT_DIR / "exports"


def build_qbo_transaction(doc):
    """
    Convert internal document record to
    a QuickBooks-style expense transaction structure.
    """

    return {
        "TxnDate": doc.document_date,
        "Vendor": doc.vendor,
        "ClientCode": doc.client_code,
        "Amount": doc.amount,
        "Account": doc.gl_account,
        "TaxCode": doc.tax_code,
        "Description": f"{doc.vendor} expense",
        "SourceDocument": doc.file_name,
    }


def main():

    EXPORT_DIR.mkdir(exist_ok=True)

    store = TaskStore(DB_PATH)

    documents = store.list_documents("Ready")

    if not documents:
        print("No Ready documents found.")
        return

    transactions = []

    for doc in documents:

        txn = build_qbo_transaction(doc)

        transactions.append(txn)

    export_file = EXPORT_DIR / "qbo_transactions.json"

    with open(export_file, "w", encoding="utf-8") as f:
        json.dump(transactions, f, indent=2)

    print("")
    print("QBO transaction export completed")
    print(f"Transactions exported: {len(transactions)}")
    print(f"File: {export_file}")


if __name__ == "__main__":
    main()