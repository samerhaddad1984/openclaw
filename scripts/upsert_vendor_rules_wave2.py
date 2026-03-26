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
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def backup_file(path: Path) -> Path:
    backup_path = path.with_suffix(path.suffix + ".wave2.bak")
    backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def upsert_vendor_rule(vendors_data: dict, new_rule: dict) -> None:
    if "vendors" not in vendors_data or not isinstance(vendors_data["vendors"], list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    replaced = False
    updated = []

    for item in vendors_data["vendors"]:
        if isinstance(item, dict) and item.get("id") == new_rule["id"]:
            updated.append(new_rule)
            replaced = True
        else:
            updated.append(item)

    if not replaced:
        updated.append(new_rule)

    vendors_data["vendors"] = updated


def upsert_vendor_mapping_file(data: dict, vendor_name: str, mapping: dict) -> None:
    if "vendors" not in data or not isinstance(data["vendors"], dict):
        raise ValueError("JSON file must contain a top-level 'vendors' object")

    data["vendors"][vendor_name] = mapping


def verify(vendors_data, vendor_intel_data, gl_map_data, account_map_data):
    vendor_ids = {item.get("id") for item in vendors_data.get("vendors", []) if isinstance(item, dict)}

    checks = {
        "amazon_rule": "amazon_ca_invoice" in vendor_ids,
        "microsoft_rule": "microsoft_canada_invoice" in vendor_ids,
        "companycam_rule": "companycam_invoice" in vendor_ids,
        "amazon_intel": "Amazon.com.ca ULC" in vendor_intel_data.get("vendors", {}),
        "microsoft_intel": "Microsoft Canada Inc." in vendor_intel_data.get("vendors", {}),
        "companycam_intel": "CompanyCam" in vendor_intel_data.get("vendors", {}),
        "amazon_gl": "Amazon.com.ca ULC" in gl_map_data.get("vendors", {}),
        "microsoft_gl": "Microsoft Canada Inc." in gl_map_data.get("vendors", {}),
        "companycam_gl": "CompanyCam" in gl_map_data.get("vendors", {}),
        "amazon_account": "Amazon.com.ca ULC" in account_map_data.get("vendors", {}),
        "microsoft_account": "Microsoft Canada Inc." in account_map_data.get("vendors", {}),
        "companycam_account": "CompanyCam" in account_map_data.get("vendors", {}),
    }
    return checks


def main():
    print(f"ROOT_DIR  = {ROOT_DIR}")
    print(f"RULES_DIR = {RULES_DIR}")
    print()

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

    amazon_rule = {
        "id": "amazon_ca_invoice",
        "name": "Amazon.com.ca ULC",
        "patterns": [
            "Amazon.com.ca ULC",
            "www.amazon.ca/contact-us",
            "Invoice / Facture",
            "Total payable / Total à payer",
            "Order # / Commande #",
            "Shipment # / # d'expédition"
        ],
        "vendor_name": "Amazon.com.ca ULC",
        "doc_type": "invoice",
        "currency": "CAD",
        "country": "CA",
        "province": "QC",
        "min_confidence": 0.9,
        "total_regex": "(?is)(?:Total payable / Total à payer|Invoice subtotal / Total partiel de la\\s*facture)[^\\d]{0,40}\\$?(\\d{1,3}(?:[ ,]\\d{3})*[\\.,]\\d{2})",
        "date_regex": "(?is)(?:Invoice date / Date de facturation)[^\\d]{0,40}(\\d{1,2}\\s+[A-Za-zéûôîÉÛÔÎ]+\\s+\\d{4})"
    }

    microsoft_rule = {
        "id": "microsoft_canada_invoice",
        "name": "Microsoft Canada Inc.",
        "patterns": [
            "Microsoft Canada Inc.",
            "Questions on your bill? Visit https://aka.ms/invoice-billing",
            "Invoice for activity on",
            "Billing Summary",
            "Power Automate per user plan",
            "Tax Invoice Number"
        ],
        "vendor_name": "Microsoft Canada Inc.",
        "doc_type": "invoice",
        "currency": "CAD",
        "country": "CA",
        "province": "QC",
        "min_confidence": 0.9,
        "total_regex": "(?is)(?:Total \\(including Tax\\)|Total Amount)[^\\d]{0,40}(?:CAD\\s*)?(\\d{1,3}(?:[ ,]\\d{3})*[\\.,]\\d{2})",
        "date_regex": "(?is)(?:Document Date|Tax Invoice Date)[^\\d]{0,30}(\\d{2}/\\d{2}/\\d{4})"
    }

    companycam_rule = {
        "id": "companycam_invoice",
        "name": "CompanyCam",
        "patterns": [
            "CompanyCam",
            "support@companycam.com",
            "Invoice number FBBD",
            "Pay online",
            "Bill to",
            "Amount due"
        ],
        "vendor_name": "CompanyCam",
        "doc_type": "invoice",
        "currency": "USD",
        "country": "US",
        "province": None,
        "min_confidence": 0.9,
        "total_regex": "(?is)(?:Amount due|Total|\\$\\s*due)[^\\d]{0,30}\\$?(\\d{1,3}(?:[ ,]\\d{3})*[\\.,]\\d{2})",
        "date_regex": "(?is)(?:Date of issue)[^A-Za-z0-9]{0,20}([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})"
    }

    upsert_vendor_rule(vendors_data, amazon_rule)
    upsert_vendor_rule(vendors_data, microsoft_rule)
    upsert_vendor_rule(vendors_data, companycam_rule)

    upsert_vendor_mapping_file(
        vendor_intel_data,
        "Amazon.com.ca ULC",
        {
            "category": "Office Supplies",
            "document_family": "invoice",
            "gl_account": "Office Supplies",
            "tax_code": "GST_QST",
            "preferred_doc_types": ["invoice", "receipt"]
        }
    )

    upsert_vendor_mapping_file(
        vendor_intel_data,
        "Microsoft Canada Inc.",
        {
            "category": "Software",
            "document_family": "subscription",
            "gl_account": "Software Expense",
            "tax_code": "GST_QST",
            "preferred_doc_types": ["invoice", "receipt"]
        }
    )

    upsert_vendor_mapping_file(
        vendor_intel_data,
        "CompanyCam",
        {
            "category": "Software",
            "document_family": "subscription",
            "gl_account": "Software Expense",
            "tax_code": "NONE",
            "preferred_doc_types": ["invoice", "receipt"]
        }
    )

    upsert_vendor_mapping_file(
        gl_map_data,
        "Amazon.com.ca ULC",
        {
            "gl_account": "Office Supplies",
            "tax_code": "GST_QST"
        }
    )

    upsert_vendor_mapping_file(
        gl_map_data,
        "Microsoft Canada Inc.",
        {
            "gl_account": "Software Expense",
            "tax_code": "GST_QST"
        }
    )

    upsert_vendor_mapping_file(
        gl_map_data,
        "CompanyCam",
        {
            "gl_account": "Software Expense",
            "tax_code": "NONE"
        }
    )

    upsert_vendor_mapping_file(
        account_map_data,
        "Amazon.com.ca ULC",
        {
            "expense_account": "Office Supplies",
            "tax_code": "GST_QST"
        }
    )

    upsert_vendor_mapping_file(
        account_map_data,
        "Microsoft Canada Inc.",
        {
            "expense_account": "Software Expense",
            "tax_code": "GST_QST"
        }
    )

    upsert_vendor_mapping_file(
        account_map_data,
        "CompanyCam",
        {
            "expense_account": "Software Expense",
            "tax_code": "NONE"
        }
    )

    write_json(VENDORS_FILE, vendors_data)
    write_json(VENDOR_INTEL_FILE, vendor_intel_data)
    write_json(GL_MAP_FILE, gl_map_data)
    write_json(ACCOUNT_MAP_FILE, account_map_data)

    checks = verify(vendors_data, vendor_intel_data, gl_map_data, account_map_data)

    print("VERIFICATION:")
    print(json.dumps(checks, indent=2, ensure_ascii=False))

    if not all(checks.values()):
        raise RuntimeError("Wave 2 rule write failed verification.")

    print("\nSUCCESS: Amazon, Microsoft, and CompanyCam rules were written.")


if __name__ == "__main__":
    main()