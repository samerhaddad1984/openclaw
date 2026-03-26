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


def write_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def backup_file(path: Path) -> Path:
    backup_path = path.with_suffix(path.suffix + ".wave6.bak")
    backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def replace_vendor_rule(vendors_data: dict, rule_id: str, new_rule: dict):
    if "vendors" not in vendors_data or not isinstance(vendors_data["vendors"], list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    replaced = False
    new_vendors = []

    for item in vendors_data["vendors"]:
        if isinstance(item, dict) and item.get("id") == rule_id:
            new_vendors.append(new_rule)
            replaced = True
        else:
            new_vendors.append(item)

    if not replaced:
        new_vendors.append(new_rule)

    vendors_data["vendors"] = new_vendors


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

    # Tighten CompanyCam so it only matches real CompanyCam docs.
    companycam_rule = {
        "id": "companycam_invoice",
        "name": "CompanyCam",
        "patterns": [
            "CompanyCam",
            "support@companycam.com",
            "Invoice number FBBD891C",
            "Invoice number 6A920927",
            "Add on: Signatures",
            "Premium",
            "Checklist Templates",
            "Bill to",
            "Systemes Soussol Quebec"
        ],
        "vendor_name": "CompanyCam",
        "doc_type": "invoice",
        "currency": "USD",
        "country": "US",
        "province": None,
        "min_confidence": 0.97,
        "total_regex": (
            r"(?is)"
            r"(?:"
            r"Amount due\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
            r"|"
            r"\$?(\d{1,3}(?:,\d{3})*\.\d{2})\s*USD\s+due\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"
            r"|"
            r"Amount due\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})\s*USD"
            r")"
        ),
        "date_regex": (
            r"(?is)"
            r"(?:Date of issue\s*Date due\s*)"
            r"([A-Za-z]+\s+\d{1,2},\s*[0-9]{4})"
            r"|"
            r"(?:Date of issue[^\w]{0,20})"
            r"([A-Za-z]+\s+\d{1,2},\s*[0-9]{4})"
        )
    }

    # Strengthen OpenAI so it clearly beats generic SaaS receipt patterns.
    openai_rule = {
        "id": "openai_receipt",
        "name": "OpenAI, LLC",
        "patterns": [
            "OpenAI, LLC",
            "ChatGPT Plus Subscription (per seat)",
            "ar@openai.com",
            "Receipt number",
            "Date paid",
            "Amount paid",
            "CA GST/HST 762507606RT0001",
            "CA QST",
            "NR00037842"
        ],
        "vendor_name": "OpenAI, LLC",
        "doc_type": "receipt",
        "currency": "CAD",
        "country": "US",
        "province": None,
        "min_confidence": 0.98,
        "total_regex": (
            r"(?is)"
            r"(?:"
            r"\$?(\d{1,3}(?:,\d{3})*\.\d{2})\s+paid on\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"
            r"|"
            r"Amount paid\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
            r"|"
            r"Total\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
            r")"
        ),
        "date_regex": r"(?is)Date paid\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})"
    }

    # Improve Google/PayPal date extraction at least.
    google_rule = {
        "id": "google_paypal_receipt",
        "name": "Google",
        "patterns": [
            "PayPal: Transaction Details",
            "Google",
            "Automatic Payment",
            "noreply+support@google.com",
            "100 GB (Google One)",
            "Transaction ID",
            "Invoice ID"
        ],
        "vendor_name": "Google",
        "doc_type": "receipt",
        "currency": "USD",
        "country": "US",
        "province": None,
        "min_confidence": 0.95,
        "total_regex": (
            r"(?is)"
            r"(?:"
            r"Order summary.*?Total\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
            r"|"
            r"Seller info\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
            r"|"
            r"Total\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
            r")"
        ),
        "date_regex": (
            r"(?is)"
            r"Google\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})\s*\.\s*Automatic Payment"
        )
    }

    replace_vendor_rule(vendors_data, "companycam_invoice", companycam_rule)
    replace_vendor_rule(vendors_data, "openai_receipt", openai_rule)
    replace_vendor_rule(vendors_data, "google_paypal_receipt", google_rule)

    write_json(VENDORS_FILE, vendors_data)

    print("SUCCESS: Wave 6 rule collision fixes written.")


if __name__ == "__main__":
    main()