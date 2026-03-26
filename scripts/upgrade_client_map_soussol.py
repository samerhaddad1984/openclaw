import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
CLIENT_MAP_FILE = ROOT_DIR / "src" / "agents" / "data" / "rules" / "client_map.json"
CLIENT_REGISTRY_FILE = ROOT_DIR / "src" / "agents" / "data" / "rules" / "client_registry.json"


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def backup(path: Path):
    bak = path.with_suffix(path.suffix + ".clientbak")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak


def ensure_unique_append(lst, values):
    existing = {str(x).strip().lower() for x in lst}
    for value in values:
        norm = str(value).strip().lower()
        if norm not in existing:
            lst.append(value)
            existing.add(norm)


def main():
    print("Backing up files...")
    print(backup(CLIENT_MAP_FILE))
    print(backup(CLIENT_REGISTRY_FILE))

    client_map = read_json(CLIENT_MAP_FILE)
    client_registry = read_json(CLIENT_REGISTRY_FILE)

    for client in client_map.get("clients", []):
        if client.get("client_code") == "SOUSSOL":
            ensure_unique_append(client.setdefault("client_names", []), [
                "basement systems quebec",
                "quebec basement systems",
                "systemes soussol quebec",
                "systèmes soussol québec",
                "systemes sous-sol quebec",
                "systèmes sous-sol québec",
            ])
            ensure_unique_append(client.setdefault("addresses", []), [
                "2990 boulevard le corbusier",
                "2990 blvd le corbusier",
                "laval quebec h7l 3m2",
                "laval qc h7l 3m2",
                "1620 boul saint-elzear o",
                "1620 boul. saint-elzear o",
                "15 rue de rochebonne",
                "blainville quebec j7b 1w8",
                "blainville qc j7b 1w8",
            ])
            ensure_unique_append(client.setdefault("pdf_keywords", []), [
                "basement systems quebec",
                "quebec basement systems",
                "microsoft canada inc",
                "amazon.com.ca ulc",
                "companycam",
                "accounting@soussol.com",
            ])

    for client in client_registry.get("clients", []):
        if client.get("client_code") == "SOUSSOL":
            ensure_unique_append(client.setdefault("keywords", []), [
                "basement systems quebec",
                "quebec basement systems",
                "systemes soussol quebec",
                "systemes sous-sol quebec",
                "2990 boulevard le corbusier",
                "laval qc h7l 3m2",
                "15 rue de rochebonne",
                "blainville qc j7b 1w8",
                "companycam",
                "microsoft canada inc",
                "amazon.com.ca ulc",
                "accounting@soussol.com",
            ])

    write_json(CLIENT_MAP_FILE, client_map)
    write_json(CLIENT_REGISTRY_FILE, client_registry)

    print("SOUSSOL client mapping upgraded.")


if __name__ == "__main__":
    main()