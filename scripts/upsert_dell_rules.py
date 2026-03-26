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


def backup_file(path: Path) -> Path:
    backup_path = path.with_suffix(path.suffix + ".bak")
    backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def add_or_replace_dell_vendor_rule(vendors_data: dict) -> dict:
    if "vendors" not in vendors_data or not isinstance(vendors_data["vendors"], list):
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
        "min_confidence": 0.9,
        "total_regex": "(?is)(?:total(?:\\s+amount)?|montant\\s+total|total\\s+de\\s+la\\s+facture)[^\\d\\-]{0,50}(\\d{1,3}(?:[ ,]\\d{3})*[\\.,]\\d{2})\\s*\\$?",
        "date_regex": "(?is)(?:invoice\\s+date/date\\s+de\\s+facture|date\\s+de\\s+facture)[^\\d]{0,30}(\\d{2}/\\d{2}/\\d{4})"
    }

    new_vendors = []
    replaced = False

    for item in vendors_data["vendors"]:
        if isinstance(item, dict) and item.get("id") == "dell_canada_invoice":
            new_vendors.append(dell_rule)
            replaced = True
        else:
            new_vendors.append(item)

    if not replaced:
        new_vendors.append(dell_rule)

    vendors_data["vendors"] = new_vendors
    return vendors_data


def add_vendor_mapping(data: dict, vendor_name: str, mapping: dict) -> dict:
    if "vendors" not in data or not isinstance(data["vendors"], dict):
        raise ValueError("JSON file must contain a top-level 'vendors' object")

    data["vendors"][vendor_name] = mapping
    return data


def verify_all():
    vendors_data = read_json(VENDORS_FILE)
    vendor_intel_data = read_json(VENDOR_INTEL_FILE)
    gl_map_data = read_json(GL_MAP_FILE)
    account_map_data = read_json(ACCOUNT_MAP_FILE)

    vendors_rule_found = False
    matched_vendor_rule = None

    for item in vendors_data.get("vendors", []):
        if isinstance(item, dict) and item.get("id") == "dell_canada_invoice":
            vendors_rule_found = True
            matched_vendor_rule = item
            break

    result = {
        "vendors_json_has_dell_rule": vendors_rule_found,
        "vendors_json_dell_rule": matched_vendor_rule,
        "vendor_intel_has_dell": "Dell Canada Inc." in vendor_intel_data.get("vendors", {}),
        "vendor_intel_value": vendor_intel_data.get("vendors", {}).get("Dell Canada Inc."),
        "gl_map_has_dell": "Dell Canada Inc." in gl_map_data.get("vendors", {}),
        "gl_map_value": gl_map_data.get("vendors", {}).get("Dell Canada Inc."),
        "account_map_has_dell": "Dell Canada Inc." in account_map_data.get("vendors", {}),
        "account_map_value": account_map_data.get("vendors", {}).get("Dell Canada Inc."),
    }

    return result


def main():
    print(f"ROOT_DIR  = {ROOT_DIR}")
    print(f"RULES_DIR = {RULES_DIR}")
    print()

    if not RULES_DIR.exists():
        raise FileNotFoundError(f"Rules directory does not exist: {RULES_DIR}")

    for path in [VENDORS_FILE, VENDOR_INTEL_FILE, GL_MAP_FILE, ACCOUNT_MAP_FILE]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    print("Creating backups...")
    backups = {
        str(VENDORS_FILE): str(backup_file(VENDORS_FILE)),
        str(VENDOR_INTEL_FILE): str(backup_file(VENDOR_INTEL_FILE)),
        str(GL_MAP_FILE): str(backup_file(GL_MAP_FILE)),
        str(ACCOUNT_MAP_FILE): str(backup_file(ACCOUNT_MAP_FILE)),
    }
    print(json.dumps(backups, indent=2, ensure_ascii=False))
    print()

    vendors_data = read_json(VENDORS_FILE)
    vendor_intel_data = read_json(VENDOR_INTEL_FILE)
    gl_map_data = read_json(GL_MAP_FILE)
    account_map_data = read_json(ACCOUNT_MAP_FILE)

    vendors_data = add_or_replace_dell_vendor_rule(vendors_data)

    vendor_intel_data = add_vendor_mapping(
        vendor_intel_data,
        "Dell Canada Inc.",
        {
            "category": "Office Equipment",
            "document_family": "invoice",
            "gl_account": "Computer and Equipment Expense",
            "tax_code": "GST_QST",
            "preferred_doc_types": ["invoice", "receipt"]
        }
    )

    gl_map_data = add_vendor_mapping(
        gl_map_data,
        "Dell Canada Inc.",
        {
            "gl_account": "Computer and Equipment Expense",
            "tax_code": "GST_QST"
        }
    )

    account_map_data = add_vendor_mapping(
        account_map_data,
        "Dell Canada Inc.",
        {
            "expense_account": "Computer and Equipment Expense",
            "tax_code": "GST_QST"
        }
    )

    write_json(VENDORS_FILE, vendors_data)
    write_json(VENDOR_INTEL_FILE, vendor_intel_data)
    write_json(GL_MAP_FILE, gl_map_data)
    write_json(ACCOUNT_MAP_FILE, account_map_data)

    print("Write complete.\n")

    verification = verify_all()
    print("VERIFICATION:")
    print(json.dumps(verification, indent=2, ensure_ascii=False))

    if not all([
        verification["vendors_json_has_dell_rule"],
        verification["vendor_intel_has_dell"],
        verification["gl_map_has_dell"],
        verification["account_map_has_dell"],
    ]):
        raise RuntimeError("Verification failed. Dell was not written correctly to all rule files.")

    print("\nSUCCESS: Dell rules were written correctly.")


if __name__ == "__main__":
    main()