import json
from pathlib import Path
from collections import Counter, defaultdict


ROOT_DIR = Path(__file__).resolve().parent.parent
RESULTS_FILE = ROOT_DIR / "tests" / "test_results.json"


def load_results():
    if not RESULTS_FILE.exists():
        print("Results file not found:", RESULTS_FILE)
        return []

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def build_report(results):

    status_counts = Counter()
    vendor_counts = Counter()
    review_reasons = Counter()
    gl_counts = Counter()

    vendor_amounts = defaultdict(list)

    for r in results:

        status = r.get("review_status")
        vendor = r.get("vendor")
        amount = r.get("amount")
        gl = r.get("gl_account")

        status_counts[status] += 1

        if vendor:
            vendor_counts[vendor] += 1

        if gl:
            gl_counts[gl] += 1

        if vendor and amount:
            vendor_amounts[vendor].append(amount)

        notes = r.get("notes", [])
        for n in notes:
            review_reasons[n] += 1

    return {
        "status_counts": status_counts,
        "vendor_counts": vendor_counts,
        "review_reasons": review_reasons,
        "gl_counts": gl_counts,
        "vendor_amounts": vendor_amounts,
    }


def print_section(title, data):

    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    for k, v in sorted(data.items(), key=lambda x: x[1], reverse=True):
        print(f"{k:40} {v}")


def print_vendor_amounts(vendor_amounts):

    print("\n" + "=" * 70)
    print("VENDOR AMOUNT DISTRIBUTION")
    print("=" * 70)

    for vendor, amounts in vendor_amounts.items():

        if not amounts:
            continue

        avg = sum(amounts) / len(amounts)
        min_val = min(amounts)
        max_val = max(amounts)

        print(f"\n{vendor}")
        print(f"  docs : {len(amounts)}")
        print(f"  avg  : {round(avg,2)}")
        print(f"  min  : {min_val}")
        print(f"  max  : {max_val}")


def main():

    results = load_results()

    if not results:
        print("No results loaded.")
        return

    report = build_report(results)

    print_section("STATUS COUNTS", report["status_counts"])
    print_section("VENDOR COUNTS", report["vendor_counts"])
    print_section("GL ACCOUNT DISTRIBUTION", report["gl_counts"])
    print_section("REVIEW NOTES", report["review_reasons"])

    print_vendor_amounts(report["vendor_amounts"])


if __name__ == "__main__":
    main()