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

from src.agents.tools.sharepoint_processor import extract_pdf_text

TEST_DOC_FOLDER = ROOT_DIR / "tests" / "documents_real"

FILES = [
    "PayPal_ Transaction Details.pdf",
    "invoice_8922355.pdf",
    "recu_de_massotherapie (13).pdf",
    "recu_de_massotherapie (14).pdf",
    "recu_de_massotherapie (15).pdf",
    "Receipt-2167-0997-5794.pdf",
    "onlineStatement_2024-07-09.pdf",
]

def main():
    for name in FILES:
        path = TEST_DOC_FOLDER / name
        print("\n" + "=" * 100)
        print(name)
        print("=" * 100)

        if not path.exists():
            print("Missing file")
            continue

        try:
            text = extract_pdf_text(path)
            print((text or "")[:4000])
        except Exception as e:
            print(f"EXTRACTION FAILED: {e}")

if __name__ == "__main__":
    main()