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
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def backup_file(path: Path) -> Path:
    backup_path = path.with_suffix(path.suffix + ".wave5.bak")
    backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def upsert_vendor_rule(vendors_data: dict, new_rule: dict) -> None:
    if "vendors" not in vendors_data or not isinstance(vendors_data["vendors"], list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    updated = []
    replaced = False

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


def main():
    print(f"ROOT_DIR  = {ROOT_DIR}")
    print(f"RULES_DIR = {RULES_DIR}")
    print()

    for path in [VENDORS_FILE, VENDOR_INTEL_FILE, GL_MAP_FILE, ACCOUNT_MAP_FILE]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    print("Creating backups...")
    print(backup_file(VENDORS_FILE))
    print(backup_file(VENDOR_INTEL_FILE))
    print(backup_file(GL_MAP_FILE))
    print(backup_file(ACCOUNT_MAP_FILE))
    print()

    vendors_data = read_json(VENDORS_FILE)
    vendor_intel_data = read_json(VENDOR_INTEL_FILE)
    gl_map_data = read_json(GL_MAP_FILE)
    account_map_data = read_json(ACCOUNT_MAP_FILE)

    google_paypal_rule = {
        "id": "google_paypal_receipt",
        "name": "Google",
        "patterns": [
            "PayPal: Transaction Details",
            "Google",
            "Automatic Payment",
            "noreply+support@google.com",
            "100 GB (Google One)",
            "Transaction ID",
            "Invoice ID",
        ],
        "vendor_name": "Google",
        "doc_type": "receipt",
        "currency": "USD",
        "country": "US",
        "province": None,
        "min_confidence": 0.9,
        "total_regex": r"(?is)(?:Order summary.*?Total\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})|Seller info\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2}))",
        "date_regex": r"(?is)Google\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})\s*\.\s*Automatic Payment"
    }

    lastpass_rule = {
        "id": "lastpass_invoice",
        "name": "LastPass Technologies Canada ULC.",
        "patterns": [
            "LastPass Technologies Canada ULC.",
            "LastPass Premium",
            "Total facture : CAD",
            "Date de la facture",
            "REÇU",
            "Destinataire :",
        ],
        "vendor_name": "LastPass Technologies Canada ULC.",
        "doc_type": "invoice",
        "currency": "CAD",
        "country": "CA",
        "province": "QC",
        "min_confidence": 0.9,
        "total_regex": r"(?is)(?:Total facture\s*:\s*CAD\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})|TOTAL\s*Taxes et frais inclus\s*CAD\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2}))",
        "date_regex": r"(?is)Date de la facture\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})"
    }

    openai_rule = {
        "id": "openai_receipt",
        "name": "OpenAI, LLC",
        "patterns": [
            "OpenAI, LLC",
            "ChatGPT Plus Subscription (per seat)",
            "Receipt",
            "Date paid",
            "Amount paid",
            "Receipt number",
            "ar@openai.com",
        ],
        "vendor_name": "OpenAI, LLC",
        "doc_type": "receipt",
        "currency": "CAD",
        "country": "US",
        "province": None,
        "min_confidence": 0.9,
        "total_regex": r"(?is)(?:Amount paid\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})|Total\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2}))",
        "date_regex": r"(?is)Date paid\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})"
    }

    upsert_vendor_rule(vendors_data, google_paypal_rule)
    upsert_vendor_rule(vendors_data, lastpass_rule)
    upsert_vendor_rule(vendors_data, openai_rule)

    upsert_vendor_mapping_file(
        vendor_intel_data,
        "Google",
        {
            "category": "Software",
            "document_family": "subscription",
            "gl_account": "Software Expense",
            "tax_code": "NONE",
            "preferred_doc_types": ["invoice", "receipt"]
        }
    )

    upsert_vendor_mapping_file(
        vendor_intel_data,
        "LastPass Technologies Canada ULC.",
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
        "OpenAI, LLC",
        {
            "category": "Software",
            "document_family": "subscription",
            "gl_account": "Software Expense",
            "tax_code": "GST_QST",
            "preferred_doc_types": ["invoice", "receipt"]
        }
    )

    upsert_vendor_mapping_file(
        gl_map_data,
        "Google",
        {"gl_account": "Software Expense", "tax_code": "NONE"}
    )
    upsert_vendor_mapping_file(
        gl_map_data,
        "LastPass Technologies Canada ULC.",
        {"gl_account": "Software Expense", "tax_code": "GST_QST"}
    )
    upsert_vendor_mapping_file(
        gl_map_data,
        "OpenAI, LLC",
        {"gl_account": "Software Expense", "tax_code": "GST_QST"}
    )

    upsert_vendor_mapping_file(
        account_map_data,
        "Google",
        {"expense_account": "Software Expense", "tax_code": "NONE"}
    )
    upsert_vendor_mapping_file(
        account_map_data,
        "LastPass Technologies Canada ULC.",
        {"expense_account": "Software Expense", "tax_code": "GST_QST"}
    )
    upsert_vendor_mapping_file(
        account_map_data,
        "OpenAI, LLC",
        {"expense_account": "Software Expense", "tax_code": "GST_QST"}
    )

    write_json(VENDORS_FILE, vendors_data)
    write_json(VENDOR_INTEL_FILE, vendor_intel_data)
    write_json(GL_MAP_FILE, gl_map_data)
    write_json(ACCOUNT_MAP_FILE, account_map_data)

    print("SUCCESS: Wave 5 vendor rules written.")


if __name__ == "__main__":
    main()