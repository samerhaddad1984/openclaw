from pathlib import Path
import csv
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore


DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORT_PATH = ROOT_DIR / "exports"


def main():

    EXPORT_PATH.mkdir(exist_ok=True)

    store = TaskStore(DB_PATH)

    documents = store.list_documents("Ready")

    if not documents:
        print("No Ready documents to export.")
        return

    export_file = EXPORT_PATH / "quickbooks_export.csv"

    with open(export_file, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)

        writer.writerow(
            [
                "Date",
                "Vendor",
                "Account",
                "Amount",
                "TaxCode",
                "Description",
                "Client",
            ]
        )

        for d in documents:

            description = f"{d.vendor} document"

            writer.writerow(
                [
                    d.document_date,
                    d.vendor,
                    d.gl_account,
                    d.amount,
                    d.tax_code,
                    description,
                    d.client_code,
                ]
            )

    print("")
    print("Export completed.")
    print(f"File: {export_file}")
    print(f"Documents exported: {len(documents)}")


if __name__ == "__main__":
    main()