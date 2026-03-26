import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
VENDORS_FILE = ROOT_DIR / "src" / "agents" / "data" / "rules" / "vendors.json"


def read_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data):
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def backup(path: Path):
    bak = path.with_suffix(path.suffix + ".wave4.bak")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak


def main():
    print(f"ROOT_DIR = {ROOT_DIR}")
    print(f"VENDORS_FILE = {VENDORS_FILE}")
    print()

    print("Creating backup...")
    print(backup(VENDORS_FILE))
    print()

    data = read_json(VENDORS_FILE)

    if "vendors" not in data or not isinstance(data["vendors"], list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    updated = {
        "amazon_ca_invoice": False,
        "companycam_invoice": False,
    }

    for item in data["vendors"]:
        if not isinstance(item, dict):
            continue

        vendor_id = item.get("id")

        if vendor_id == "amazon_ca_invoice":
            item["date_regex"] = (
                r"(?is)"
                r"(?:Invoice date / Date de facturation:?\s*)"
                r"(\d{1,2}\s+[A-Za-zÀ-ÿ]+\s+\d{4})"
            )
            updated["amazon_ca_invoice"] = True

        elif vendor_id == "companycam_invoice":
            item["date_regex"] = (
                r"(?is)"
                r"(?:Date of issue\s*Date due\s*)"
                r"([A-Za-z]+\s+\d{1,2},\s+\d{4})"
                r"|"
                r"(?:Date of issue[^\w]{0,20})"
                r"([A-Za-z]+\s+\d{1,2},\s+\d{4})"
            )
            updated["companycam_invoice"] = True

    write_json(VENDORS_FILE, data)

    print("UPDATED:")
    print(json.dumps(updated, indent=2, ensure_ascii=False))

    if not all(updated.values()):
        raise RuntimeError("Did not update all expected vendor date regexes.")

    print("\nSUCCESS: Wave 4 date regex fixes written.")


if __name__ == "__main__":
    main()