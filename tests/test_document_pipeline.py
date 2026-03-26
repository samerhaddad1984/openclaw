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

from src.agents.tools.local_document_processor import process_document as process_local_document


TEST_DOC_FOLDER = ROOT_DIR / "tests" / "documents_real"
RESULTS_FILE = ROOT_DIR / "tests" / "test_results.json"


def short(value, length):
    if value is None:
        return ""
    return str(value)[:length]


def print_table(results):
    print("\n" + "=" * 150)
    print("LEDGERLINK LOCAL DOCUMENT PIPELINE TEST")
    print("=" * 150)
    print()

    header = f"{'FILE':30} {'VENDOR':24} {'CLIENT':16} {'AMOUNT':12} {'GL ACCOUNT':32} {'STATUS':12}"
    print(header)
    print("-" * len(header))

    for r in results:
        print(
            f"{short(r.get('file_name'), 30):30} "
            f"{short(r.get('vendor'), 24):24} "
            f"{short(r.get('client_code'), 16):16} "
            f"{short(r.get('amount'), 12):12} "
            f"{short(r.get('gl_account'), 32):32} "
            f"{short(r.get('review_status'), 12):12}"
        )

    print()

    failures = [r for r in results if r.get("review_status") != "Ready" or r.get("errors")]
    if failures:
        print("NON-READY / ERROR DETAILS")
        print("-" * 150)
        for r in failures:
            print(f"FILE   : {r.get('file_name')}")
            print(f"STATUS : {r.get('review_status')}")
            print(f"ERRORS : {r.get('errors')}")
            print(f"VENDOR : {r.get('vendor')}")
            print(f"CLIENT : {r.get('client_code')}")
            print(f"AMOUNT : {r.get('amount')}")
            print()


def main():
    if not TEST_DOC_FOLDER.exists():
        print(f"Test document folder not found: {TEST_DOC_FOLDER}")
        return

    pdf_files = sorted(TEST_DOC_FOLDER.glob("*.pdf"))

    if not pdf_files:
        print(f"No test PDFs found in: {TEST_DOC_FOLDER}")
        return

    print(f"Running local pipeline on {len(pdf_files)} PDF(s)...\n")

    results = []

    for pdf_file in pdf_files:
        print(f"Processing: {pdf_file.name}")
        result = process_local_document(pdf_file)
        results.append(result)

    print_table(results)

    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"Results saved to: {RESULTS_FILE}")


if __name__ == "__main__":
    main()