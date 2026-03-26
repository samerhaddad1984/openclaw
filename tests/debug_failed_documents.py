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
OUTPUT_FILE = ROOT_DIR / "tests" / "debug_failed_documents.json"


def main():
    if not TEST_DOC_FOLDER.exists():
        print(f"Missing folder: {TEST_DOC_FOLDER}")
        return

    pdf_files = sorted(TEST_DOC_FOLDER.glob("*.pdf")) + sorted(TEST_DOC_FOLDER.glob("*.PDF"))

    if not pdf_files:
        print("No PDF files found.")
        return

    results = []

    for pdf_file in pdf_files:
        print(f"\n{'=' * 120}")
        print(f"PROCESSING: {pdf_file.name}")
        print(f"{'=' * 120}")

        result = process_local_document(pdf_file)

        text_preview = None
        try:
            text = extract_pdf_text(pdf_file)
            text_preview = (text or "")[:2000]
        except Exception as e:
            text_preview = f"TEXT EXTRACTION FAILED: {e}"

        combined = {
            "file_name": pdf_file.name,
            "review_status": result.get("review_status"),
            "vendor": result.get("vendor"),
            "client_code": result.get("client_code"),
            "amount": result.get("amount"),
            "doc_type": result.get("doc_type"),
            "gl_account": result.get("gl_account"),
            "raw_rules_output": result.get("raw_rules_output"),
            "raw_vendor_output": result.get("raw_vendor_output"),
            "raw_client_route": result.get("raw_client_route"),
            "raw_ai_client_route": result.get("raw_ai_client_route"),
            "text_preview": text_preview,
        }

        results.append(combined)

        print(f"STATUS   : {combined['review_status']}")
        print(f"VENDOR   : {combined['vendor']}")
        print(f"CLIENT   : {combined['client_code']}")
        print(f"AMOUNT   : {combined['amount']}")
        print(f"DOC TYPE : {combined['doc_type']}")
        print(f"GL       : {combined['gl_account']}")
        print("\nTEXT PREVIEW:\n")
        print(combined["text_preview"])

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\nSaved debug output to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()