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

from src.agents.tools.sharepoint_processor import RulesEngine, extract_pdf_text


RULES_DIR = ROOT_DIR / "src" / "agents" / "data" / "rules"
TEST_DOC_FOLDER = ROOT_DIR / "tests" / "documents"


FILES_TO_CHECK = [
    "Amzaon invoice-$106.77-Samer.pdf",
    "G142010201_eee5087efacc4541af1d479441e72c32.pdf",
    "Invoice-FBBD891C-0084.pdf",
    "Invoice-FBBD891C-0085.pdf",
    "Invoice-FBBD891C-0086.pdf",
    "Invoice-FBBD891C-0087.pdf",
]


def main():
    engine = RulesEngine(RULES_DIR)
    results = []

    for file_name in FILES_TO_CHECK:
        pdf_path = TEST_DOC_FOLDER / file_name

        print("\n" + "=" * 120)
        print(f"FILE: {file_name}")
        print("=" * 120)

        if not pdf_path.exists():
            print("Missing file.")
            continue

        text = extract_pdf_text(pdf_path)
        rule_result = engine.run(text)

        record = {
            "file_name": file_name,
            "rule_result": str(rule_result),
            "text_preview": text[:3000],
        }
        results.append(record)

        print("RULE RESULT:")
        print(str(rule_result))
        print("\nTEXT PREVIEW:\n")
        print(text[:3000])

    output_file = ROOT_DIR / "tests" / "debug_amount_issues.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()