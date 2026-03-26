import sys
import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
AGENTS_DIR = SRC_DIR / "agents"
TOOLS_DIR = AGENTS_DIR / "tools"

for path in [ROOT_DIR, SRC_DIR, AGENTS_DIR, TOOLS_DIR]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from src.agents.tools.local_document_processor import process_local_document
from src.agents.tools.sharepoint_processor import extract_pdf_text


TEST_DOC_FOLDER = ROOT_DIR / "tests" / "documents"


def main():
    pdf_files = sorted(TEST_DOC_FOLDER.glob("*.pdf")) + sorted(TEST_DOC_FOLDER.glob("*.PDF"))
    results = []

    for pdf_file in pdf_files:
        result = process_local_document(pdf_file)
        if result.get("review_status") == "Ready":
            continue

        text = extract_pdf_text(pdf_file)
        record = {
            "file_name": pdf_file.name,
            "review_status": result.get("review_status"),
            "vendor": result.get("vendor"),
            "client_code": result.get("client_code"),
            "amount": result.get("amount"),
            "doc_type": result.get("doc_type"),
            "text_preview": (text or "")[:3000],
        }
        results.append(record)

        print("\n" + "=" * 100)
        print(pdf_file.name)
        print("=" * 100)
        print(json.dumps(record, indent=2, ensure_ascii=False, default=str))

    output_file = ROOT_DIR / "tests" / "debug_non_accounting_docs.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()