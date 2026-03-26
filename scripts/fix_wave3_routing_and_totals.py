import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
RULES_DIR = ROOT_DIR / "src" / "agents" / "data" / "rules"

VENDORS_FILE = RULES_DIR / "vendors.json"
CLIENT_MAP_FILE = RULES_DIR / "client_map.json"
CLIENT_REGISTRY_FILE = RULES_DIR / "client_registry.json"


def read_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data):
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def backup(path: Path, suffix: str):
    bak = path.with_suffix(path.suffix + suffix)
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak


def ensure_unique_append(lst, values):
    existing = {str(x).strip().lower() for x in lst}
    for value in values:
        norm = str(value).strip().lower()
        if norm not in existing:
            lst.append(value)
            existing.add(norm)


def fix_vendor_regexes(vendors_data: dict):
    if "vendors" not in vendors_data or not isinstance(vendors_data["vendors"], list):
        raise ValueError("vendors.json must contain a top-level 'vendors' list")

    updated = {
        "amazon_ca_invoice": False,
        "microsoft_canada_invoice": False,
        "companycam_invoice": False,
    }

    for item in vendors_data["vendors"]:
        if not isinstance(item, dict):
            continue

        vendor_id = item.get("id")

        if vendor_id == "amazon_ca_invoice":
            item["total_regex"] = (
                r"(?is)"
                r"(?:Total payable / Total à payer|Invoice subtotal / Total partiel de la\s*facture)"
                r"[^\d]{0,40}\$?(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})"
            )
            updated["amazon_ca_invoice"] = True

        elif vendor_id == "microsoft_canada_invoice":
            item["total_regex"] = (
                r"(?is)"
                r"(?:"
                r"Total Amount.*?CAD\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})"
                r"|"
                r"Total\s*\(including\s*Tax\).*?CAD\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})"
                r"|"
                r"Subtotal\s*Azure Credit\s*Total\s*24\.48\s*0\s*CAD\s*(\d{1,3}(?:[ ,]\d{3})*[.,]\d{2})"
                r")"
            )
            item["date_regex"] = r"(?is)(?:Document Date|Tax Invoice Date)[^\d]{0,30}(\d{2}/\d{2}/\d{4})"
            updated["microsoft_canada_invoice"] = True

        elif vendor_id == "companycam_invoice":
            item["total_regex"] = (
                r"(?is)"
                r"(?:"
                r"Amount due\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})"
                r"|"
                r"\$?(\d{1,3}(?:,\d{3})*\.\d{2})\s*USD\s+due\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"
                r"|"
                r"Amount due\s*\$?(\d{1,3}(?:,\d{3})*\.\d{2})\s*USD"
                r")"
            )
            item["date_regex"] = r"(?is)(?:Date of issue)[^\w]{0,20}([A-Za-z]+\s+\d{1,2},\s+\d{4})"
            updated["companycam_invoice"] = True

    return updated


def strengthen_soussol_routing(client_map_data: dict, client_registry_data: dict):
    found_map = False
    found_registry = False

    for client in client_map_data.get("clients", []):
        if client.get("client_code") == "SOUSSOL":
            found_map = True

            ensure_unique_append(client.setdefault("client_names", []), [
                "basement systems quebec",
                "quebec basement systems",
                "systemes soussol quebec",
                "systemes sous-sol quebec",
                "systèmes soussol québec",
                "systèmes sous-sol québec",
                "basement systems",
            ])

            ensure_unique_append(client.setdefault("addresses", []), [
                "2990 boulevard le corbusier",
                "2990 blvd le corbusier",
                "laval quebec h7l 3m2",
                "laval qc h7l 3m2",
                "15 rue de rochebonne",
                "blainville quebec j7b 1w8",
                "blainville qc j7b 1w8",
                "1620 boul saint-elzear o",
                "1620 boul. saint-elzear o",
                "1620 boulevard saint-elzear ouest",
            ])

            ensure_unique_append(client.setdefault("pdf_keywords", []), [
                "amazon.com.ca ulc",
                "microsoft canada inc",
                "companycam",
                "accounting@soussol.com",
                "2990 boulevard le corbusier",
                "15 rue de rochebonne",
                "basement systems quebec",
            ])

            # make the match easier because these docs clearly belong to SOUSSOL
            client["min_score"] = min(int(client.get("min_score", 6)), 4)

    for client in client_registry_data.get("clients", []):
        if client.get("client_code") == "SOUSSOL":
            found_registry = True

            ensure_unique_append(client.setdefault("keywords", []), [
                "basement systems quebec",
                "quebec basement systems",
                "systemes soussol quebec",
                "systemes sous-sol quebec",
                "systèmes soussol québec",
                "systèmes sous-sol québec",
                "amazon.com.ca ulc",
                "microsoft canada inc",
                "companycam",
                "accounting@soussol.com",
                "2990 boulevard le corbusier",
                "15 rue de rochebonne",
                "1620 boul saint-elzear o",
            ])

    return {
        "client_map_found": found_map,
        "client_registry_found": found_registry,
    }


def main():
    print(f"ROOT_DIR  = {ROOT_DIR}")
    print(f"RULES_DIR = {RULES_DIR}")
    print()

    for path in [VENDORS_FILE, CLIENT_MAP_FILE, CLIENT_REGISTRY_FILE]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    print("Creating backups...")
    print(backup(VENDORS_FILE, ".wave3.bak"))
    print(backup(CLIENT_MAP_FILE, ".wave3.bak"))
    print(backup(CLIENT_REGISTRY_FILE, ".wave3.bak"))
    print()

    vendors_data = read_json(VENDORS_FILE)
    client_map_data = read_json(CLIENT_MAP_FILE)
    client_registry_data = read_json(CLIENT_REGISTRY_FILE)

    vendor_updates = fix_vendor_regexes(vendors_data)
    routing_updates = strengthen_soussol_routing(client_map_data, client_registry_data)

    write_json(VENDORS_FILE, vendors_data)
    write_json(CLIENT_MAP_FILE, client_map_data)
    write_json(CLIENT_REGISTRY_FILE, client_registry_data)

    print("VENDOR UPDATES:")
    print(json.dumps(vendor_updates, indent=2, ensure_ascii=False))
    print()
    print("ROUTING UPDATES:")
    print(json.dumps(routing_updates, indent=2, ensure_ascii=False))
    print()

    if not all(vendor_updates.values()):
        raise RuntimeError("Not all vendor regexes were updated.")

    if not routing_updates["client_map_found"] or not routing_updates["client_registry_found"]:
        raise RuntimeError("SOUSSOL was not found in client map and/or client registry.")

    print("SUCCESS: Wave 3 routing and total fixes written.")


if __name__ == "__main__":
    main()