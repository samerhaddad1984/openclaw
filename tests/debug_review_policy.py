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
from src.agents.tools.sharepoint_processor import decide_review_status


TEST_DOC_FOLDER = ROOT_DIR / "tests" / "documents"

FILES_TO_CHECK = [
    "Amzaon invoice-$106.77-Samer.pdf",
    "Invoice-FBBD891C-0084.pdf",
    "Invoice-FBBD891C-0085.pdf",
    "Invoice-FBBD891C-0086.pdf",
    "Invoice-FBBD891C-0087.pdf",
]


def main():
    results = []

    for file_name in FILES_TO_CHECK:
        pdf_path = TEST_DOC_FOLDER / file_name

        print("\n" + "=" * 120)
        print(f"FILE: {file_name}")
        print("=" * 120)

        if not pdf_path.exists():
            print("Missing file.")
            continue

        result = process_local_document(pdf_path)

        raw_rules = result.get("raw_rules_output") or {}
        rules_confidence = raw_rules.get("confidence", 0.0)

        vendor_name = result.get("vendor")
        total = result.get("amount")
        document_date = result.get("document_date")
        client_code = result.get("client_code")
        doc_type = result.get("doc_type")
        routing_method = result.get("routing_method")

        final_method = "rules" if rules_confidence and float(rules_confidence) > 0 else (routing_method or "unknown")

        review = decide_review_status(
            rules_confidence=rules_confidence,
            final_method=final_method,
            vendor_name=vendor_name,
            total=total,
            document_date=document_date,
            client_code=client_code,
        )

        record = {
            "file_name": file_name,
            "vendor": vendor_name,
            "client_code": client_code,
            "amount": total,
            "doc_type": doc_type,
            "document_date": document_date,
            "routing_method": routing_method,
            "rules_confidence": rules_confidence,
            "final_method_sent_to_review_policy": final_method,
            "review_output": str(review),
            "full_pipeline_status": result.get("review_status"),
            "raw_rules_output": raw_rules,
            "raw_vendor_output": result.get("raw_vendor_output"),
            "raw_client_route": result.get("raw_client_route"),
            "raw_ai_client_route": result.get("raw_ai_client_route"),
        }

        results.append(record)

        print("PIPELINE STATUS:")
        print(result.get("review_status"))
        print()
        print("INPUTS TO REVIEW POLICY:")
        print(json.dumps({
            "rules_confidence": rules_confidence,
            "final_method": final_method,
            "vendor_name": vendor_name,
            "total": total,
            "document_date": document_date,
            "client_code": client_code,
        }, indent=2, ensure_ascii=False, default=str))
        print()
        print("REVIEW OUTPUT:")
        print(str(review))

    output_file = ROOT_DIR / "tests" / "debug_review_policy.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()