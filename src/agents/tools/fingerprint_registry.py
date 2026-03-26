from __future__ import annotations

import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
STATE_DIR = DATA_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

REGISTRY_FILE = STATE_DIR / "processed_fingerprints.json"


def load_fingerprints() -> set[str]:
    if not REGISTRY_FILE.exists():
        return set()

    try:
        obj = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
        values = obj.get("fingerprints", [])
        return set(str(x).strip() for x in values if str(x).strip())
    except Exception:
        return set()


def save_fingerprints(values: set[str]) -> None:
    REGISTRY_FILE.write_text(
        json.dumps({"fingerprints": sorted(values)}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def has_fingerprint(fp: str) -> bool:
    if not fp:
        return False
    values = load_fingerprints()
    return fp in values


def add_fingerprint(fp: str) -> None:
    if not fp:
        return
    values = load_fingerprints()
    values.add(fp)
    save_fingerprints(values)