import json
from pathlib import Path
from collections import Counter, defaultdict


ROOT_DIR = Path(__file__).resolve().parent.parent
RESULTS_FILE = ROOT_DIR / "tests" / "test_results.json"


def load_results():
    if not RESULTS_FILE.exists():
        print(f"Results file not found: {RESULTS_FILE}")
        return []

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_list(value):
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def classify_failure(record):
    status = record.get("review_status")
    errors = safe_list(record.get("errors"))
    vendor = record.get("vendor")
    client_code = record.get("client_code")
    amount = record.get("amount")
    doc_type = record.get("doc_type")
    notes = safe_list(record.get("notes"))

    buckets = []

    if status == "Exception":
        buckets.append("status:Exception")
    elif status == "NeedsReview":
        buckets.append("status:NeedsReview")
    elif status == "Ignored":
        buckets.append("status:Ignored")

    if not vendor:
        buckets.append("missing:vendor")
    if not client_code:
        buckets.append("missing:client")
    if amount is None:
        buckets.append("missing:amount")
    if not doc_type:
        buckets.append("missing:doc_type")

    if amount == 0 or amount == 0.0:
        buckets.append("amount:zero")

    for err in errors:
        err_lower = str(err).lower()
        if "ocr failed" in err_lower:
            buckets.append("error:ocr_failed")
        elif "pdf text extraction failed" in err_lower:
            buckets.append("error:text_extraction_failed")
        else:
            buckets.append(f"error:{str(err).strip()}")

    for note in notes:
        buckets.append(f"note:{note}")

    if not buckets:
        buckets.append("uncategorized")

    return buckets


def main():
    results = load_results()
    if not results:
        print("No results found.")
        return

    failing = [
        r for r in results
        if r.get("review_status") in {"NeedsReview", "Exception", "Ignored"}
    ]

    print("\n" + "=" * 80)
    print("FAILURE TRIAGE SUMMARY")
    print("=" * 80)
    print(f"Total documents: {len(results)}")
    print(f"Non-ready documents: {len(failing)}")

    bucket_counts = Counter()
    vendor_counts = Counter()
    status_counts = Counter()
    file_buckets = {}
    grouped_files = defaultdict(list)

    for r in failing:
        file_name = r.get("file_name", "<unknown>")
        vendor = r.get("vendor") or "<no vendor>"
        status = r.get("review_status") or "<no status>"

        status_counts[status] += 1
        vendor_counts[vendor] += 1

        buckets = classify_failure(r)
        file_buckets[file_name] = buckets

        for b in buckets:
            bucket_counts[b] += 1
            grouped_files[b].append(file_name)

    print("\n" + "=" * 80)
    print("STATUS COUNTS")
    print("=" * 80)
    for k, v in status_counts.most_common():
        print(f"{k:30} {v}")

    print("\n" + "=" * 80)
    print("TOP FAILURE BUCKETS")
    print("=" * 80)
    for k, v in bucket_counts.most_common():
        print(f"{k:40} {v}")

    print("\n" + "=" * 80)
    print("NON-READY VENDORS")
    print("=" * 80)
    for k, v in vendor_counts.most_common():
        print(f"{k:40} {v}")

    print("\n" + "=" * 80)
    print("FILES BY FAILURE BUCKET")
    print("=" * 80)
    for bucket, files in sorted(grouped_files.items(), key=lambda x: (-len(x[1]), x[0])):
        print(f"\n{bucket} ({len(files)})")
        for file_name in sorted(files):
            print(f"  - {file_name}")

    print("\n" + "=" * 80)
    print("PER-FILE TRIAGE")
    print("=" * 80)
    for file_name, buckets in sorted(file_buckets.items()):
        print(f"\n{file_name}")
        for b in buckets:
            print(f"  - {b}")

    output = {
        "total_documents": len(results),
        "non_ready_documents": len(failing),
        "status_counts": dict(status_counts),
        "bucket_counts": dict(bucket_counts),
        "vendor_counts": dict(vendor_counts),
        "grouped_files": {k: sorted(v) for k, v in grouped_files.items()},
        "file_buckets": file_buckets,
    }

    output_file = ROOT_DIR / "tests" / "failure_triage_report.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()