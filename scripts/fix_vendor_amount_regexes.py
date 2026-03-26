import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
VENDORS_FILE = ROOT_DIR / "src" / "agents" / "data" / "rules" / "vendors.json"


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def backup(path: Path):
    bak = path.with_suffix(path.suffix + ".regexbak")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak


def main():
    print("Backing up file...")
    print(backup(VENDORS_FILE))

    data = read_json(VENDORS_FILE)
    vendors = data.get("vendors", [])

    if not isinstance(vendors, list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    updated = 0

    for item in vendors:
        if not isinstance(item, dict):
            continue

        vendor_id = item.get("id")

        if vendor_id == "microsoft_canada_invoice":
            item["total_regex"] = r"(?is)(?:Total\s*\(including\s*Tax\)\s*\n(?:\s*\(CAD\)\s*\n)?\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})|CAD\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})\s*(?:\n|$))"
            item["date_regex"] = r"(?is)(?:Document Date|Tax Invoice Date)[^\d]{0,30}(\d{2}/\d{2}/\d{4})"
            updated += 1

        if vendor_id == "companycam_invoice":
            item["total_regex"] = r"(?is)(?:Amount due\s*\n\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})|^\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})\s*USD\s+due\b)"
            item["date_regex"] = r"(?is)(?:Date of issue)[^\w]{0,20}([A-Za-z]+\s+\d{1,2},\s+\d{4})"
            updated += 1

    write_json(VENDORS_FILE, data)

    print(f"Updated vendor rules: {updated}")
    if updated < 2:
        raise RuntimeError("Did not update both microsoft_canada_invoice and companycam_invoice.")

    print("Regex fixes written successfully.")


if __name__ == "__main__":
    main()