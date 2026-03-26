import sys
import json
from pathlib import Path


# ============================================================
# PATH SETUP
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
AGENTS_DIR = SRC_DIR / "agents"
TOOLS_DIR = AGENTS_DIR / "tools"
RULES_DIR = ROOT_DIR / "src" / "agents" / "data" / "rules"

for path in [ROOT_DIR, SRC_DIR, AGENTS_DIR, TOOLS_DIR]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


# ============================================================
# IMPORTS
# ============================================================

from src.agents.tools.sharepoint_processor import RulesEngine, extract_pdf_text


# ============================================================
# FILES
# ============================================================

VENDORS_FILE = RULES_DIR / "vendors.json"
VENDOR_INTEL_FILE = RULES_DIR / "vendor_intel.json"
GL_MAP_FILE = RULES_DIR / "gl_map.json"
ACCOUNT_MAP_FILE = RULES_DIR / "account_map.json"


# ============================================================
# HELPERS
# ============================================================

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def print_header(title: str):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)


def find_dell_entries():
    print_header("CHECKING DELL ENTRIES IN RULE FILES")

    files = [
        VENDORS_FILE,
        VENDOR_INTEL_FILE,
        GL_MAP_FILE,
        ACCOUNT_MAP_FILE,
    ]

    for file_path in files:
        print(f"\nFILE: {file_path}")
        if not file_path.exists():
            print("  MISSING")
            continue

        content = file_path.read_text(encoding="utf-8", errors="ignore")

        if "Dell Canada Inc." in content or "dell_canada_invoice" in content:
            print("  FOUND Dell-related content")
        else:
            print("  NOT FOUND")

        if file_path.name == "vendors.json":
            data = load_json(file_path)
            vendors = data.get("vendors", [])
            found = False
            for item in vendors:
                if isinstance(item, dict) and item.get("id") == "dell_canada_invoice":
                    found = True
                    print("  Dell vendor rule object:")
                    print(json.dumps(item, indent=2, ensure_ascii=False))
                    break
            if not found:
                print("  Dell vendor rule object NOT PRESENT")

        elif file_path.name in ("vendor_intel.json", "gl_map.json", "account_map.json"):
            data = load_json(file_path)
            vendors = data.get("vendors", {})
            if "Dell Canada Inc." in vendors:
                print("  Dell vendor mapping object:")
                print(json.dumps(vendors["Dell Canada Inc."], indent=2, ensure_ascii=False))
            else:
                print("  Dell vendor mapping NOT PRESENT")


def inspect_rules_engine():
    print_header("CHECKING RULES ENGINE LOADED VENDORS")

    engine = RulesEngine(RULES_DIR)

    vendors = getattr(engine, "vendors", [])
    print(f"Loaded vendor rule count: {len(vendors)}")

    found = False
    for item in vendors:
        if isinstance(item, dict) and item.get("id") == "dell_canada_invoice":
            found = True
            print("Dell rule found inside RulesEngine.vendors:")
            print(json.dumps(item, indent=2, ensure_ascii=False))
            break

    if not found:
        print("Dell rule NOT loaded into RulesEngine.vendors")


def test_pdf():
    print_header("TESTING PDF AGAINST RULES ENGINE")

    pdf_path = ROOT_DIR / "tests" / "documents" / "abcd.pdf"
    if not pdf_path.exists():
        print(f"Missing PDF: {pdf_path}")
        return

    text = extract_pdf_text(pdf_path)
    print("PDF text preview:")
    print(text[:1200])

    engine = RulesEngine(RULES_DIR)
    result = engine.run(text)

    print("\nRulesEngine.run(text) output:")
    print(str(result))

    print("\nVendor detection anchors present in text:")
    anchors = [
        "Dell Canada Inc.",
        "Dell Online",
        "www.dell.ca",
        "INVOICE/FACTURE",
        "No de facture",
        "Invoice Date/Date de facture",
        "Customer No/No de client",
        "Order No/No de commande",
    ]

    for anchor in anchors:
        print(f"- {anchor}: {anchor.lower() in text.lower()}")


def main():
    print(f"ROOT_DIR  = {ROOT_DIR}")
    print(f"RULES_DIR = {RULES_DIR}")

    find_dell_entries()
    inspect_rules_engine()
    test_pdf()


if __name__ == "__main__":
    main()