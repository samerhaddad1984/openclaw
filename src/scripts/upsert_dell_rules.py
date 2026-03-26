import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
RULES_DIR = ROOT_DIR / "src" / "agents" / "data" / "rules"

VENDORS_FILE = RULES_DIR / "vendors.json"
VENDOR_INTEL_FILE = RULES_DIR / "vendor_intel.json"
GL_MAP_FILE = RULES_DIR / "gl_map.json"
ACCOUNT_MAP_FILE = RULES_DIR / "account_map.json"


def read_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data) -> None:
    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    path.write_text(text, encoding="utf-8")


def upsert_vendors_json():
    data = read_json(VENDORS_FILE)

    if "vendors" not in data or not isinstance(data["vendors"], list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    dell_rule = {
        "id": "dell_canada_invoice",
        "name": "Dell Canada Inc.",
        "patterns": [
            "Dell Canada Inc.",
            "Dell Canada Inc",
            "Dell Online",
            "www.dell.ca",
            "INVOICE/FACTURE",
            "No de facture",
            "Invoice Date/Date de facture"
        ],
        "vendor_name": "Dell Canada Inc.",
        "doc_type": "invoice",
        "currency": "CAD",
        "country": "CA",
        "province": "QC",
        "min_confidence": 0.90,
        "total_regex": "(?is)(?:total(?:\\s+amount)?|montant\\s+total|total\\s+de\\s+la\\s+facture)[^\\d\\-]{0,50}(\\d{1,3}(?:[ ,]\\d{3})*[\\.,]\\d{2})\\s*\\$?",
        "date_regex": "(?is)(?:invoice\\s+date/date\\s+de\\s+facture|date\\s+de\\s+facture)[^\\d]{0,30}(\\d{2}/\\d{2}/\\d{4})"
    }

    original_count = len(data["vendors"])
    replaced = False
    new_vendors = []

    for item in data["vendors"]:
        if isinstance(item, dict) and item.get("id") == "dell_canada_invoice":
            new_vendors.append(dell_rule)
            replaced = True
        else:
            new_vendors.append(item)

    if not replaced:
        new_vendors.append(dell_rule)

    data["vendors"] = new_vendors
    write_json(VENDORS_FILE, data)

    return {
        "file": str(VENDORS_FILE),
        "before_count": original_count,
        "after_count": len(new_vendors),
        "replaced": replaced,
        "found_after_write": any(
            isinstance(x, dict) and x.get("id") == "dell_canada_invoice"
            for x in new_vendors
        ),
    }


def upsert_vendor_intel_json():
    data = read_json(VENDOR_INTEL_FILE)

    if "vendors" not in data or not isinstance(data["vendors"], dict):
        raise ValueError("vendor_intel.json must contain a top-level 'vendors' object")

    data["vendors"]["Dell Canada Inc."] = {
        "category": "Office Equipment",
        "document_family": "invoice",
        "gl_account": "Computer and Equipment Expense",
        "tax_code": "GST_QST",
        "preferred_doc_types": [
            "invoice",
            "receipt"
        ]
    }

    if "doc_type_defaults" not in data or not isinstance(data["doc_type_defaults"], dict):
        data["doc_type_defaults"] = {}

    if "invoice" not in data["doc_type_defaults"]:
        data["doc_type_defaults"]["invoice"] = {
            "category": "General Expense",
            "gl_account": "General Expense",
            "tax_code": "GST_QST"
        }

    write_json(VENDOR_INTEL_FILE, data)

    return {
        "file": str(VENDOR_INTEL_FILE),
        "found_after_write": "Dell Canada Inc." in data["vendors"],
        "value": data["vendors"].get("Dell Canada Inc."),
    }


def upsert_gl_map_json():
    data = read_json(GL_MAP_FILE)

    if "vendors" not in data or not isinstance(data["vendors"], dict):
        raise ValueError("gl_map.json must contain a top-level 'vendors' object")

    data["vendors"]["Dell Canada Inc."] = {
        "gl_account": "Computer and Equipment Expense",
        "tax_code": "GST_QST"
    }

    write_json(GL_MAP_FILE, data)

    return {
        "file": str(GL_MAP_FILE),
        "found_after_write": "Dell Canada Inc." in data["vendors"],
        "value": data["vendors"].get("Dell Canada Inc."),
    }


def upsert_account_map_json():
    data = read_json(ACCOUNT_MAP_FILE)

    if "vendors" not in data or not isinstance(data["vendors"], dict):
        raise ValueError("account_map.json must contain a top-level 'vendors' object")

    data["vendors"]["Dell Canada Inc."] = {
        "expense_account": "Computer and Equipment Expense",
        "tax_code": "GST_QST"
    }

    write_json(ACCOUNT_MAP_FILE, data)

    return {
        "file": str(ACCOUNT_MAP_FILE),
        "found_after_write": "Dell Canada Inc." in data["vendors"],
        "value": data["vendors"].get("Dell Canada Inc."),
    }


def verify_written_content():
    vendors_data = read_json(VENDORS_FILE)
    vendor_intel_data = read_json(VENDOR_INTEL_FILE)
    gl_map_data = read_json(GL_MAP_FILE)
    account_map_data = read_json(ACCOUNT_MAP_FILE)

    vendors_has_dell = any(
        isinstance(x, dict) and x.get("id") == "dell_canada_invoice"
        for x in vendors_data.get("vendors", [])
    )

    return {
        "vendors_json_has_dell_rule": vendors_has_dell,
        "vendor_intel_has_dell": "Dell Canada Inc." in vendor_intel_data.get("vendors", {}),
        "gl_map_has_dell": "Dell Canada Inc." in gl_map_data.get("vendors", {}),
        "account_map_has_dell": "Dell Canada Inc." in account_map_data.get("vendors", {}),
    }


def main():
    print(f"ROOT_DIR  = {ROOT_DIR}")
    print(f"RULES_DIR = {RULES_DIR}")
    print()

    if not RULES_DIR.exists():
        raise FileNotFoundError(f"Rules directory does not exist: {RULES_DIR}")

    results = {
        "vendors_json": upsert_vendors_json(),
        "vendor_intel_json": upsert_vendor_intel_json(),
        "gl_map_json": upsert_gl_map_json(),
        "account_map_json": upsert_account_map_json(),
        "verification": verify_written_content(),
    }

    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()