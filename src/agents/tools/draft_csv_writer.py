from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "outputs" / "drafts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _safe(value):
    return "" if value is None else value


def _draft_file(client_code: str) -> Path:
    return OUTPUT_DIR / f"draft_posting_{client_code}.csv"


def _existing_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()

    out = set()
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fp = (row.get("Fingerprint") or "").strip()
            sid = (row.get("SourceItemId") or "").strip()

            if fp:
                out.add(f"FP::{fp}")
            if sid:
                out.add(f"ID::{sid}")
    return out


def append_draft_row(client_code: str, row: dict) -> bool:
    path = _draft_file(client_code)
    existing = _existing_keys(path)

    fingerprint = str(row.get("Fingerprint", "")).strip()
    source_id = str(row.get("SourceItemId", "")).strip()

    if fingerprint and f"FP::{fingerprint}" in existing:
        return False
    if source_id and f"ID::{source_id}" in existing:
        return False

    file_exists = path.exists()

    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "ClientCode",
                "AssignedTo",
                "Date",
                "Vendor",
                "VendorCategory",
                "DocumentType",
                "GLAccount",
                "TaxCode",
                "BookkeepingAmount",
                "AmountSource",
                "Currency",
                "SourceFile",
                "SourceItemId",
                "Fingerprint",
                "Method",
                "Confidence",
                "VendorIntelSource",
                "Notes"
            ])

        writer.writerow([
            _safe(row.get("ClientCode")),
            _safe(row.get("AssignedTo")),
            _safe(row.get("Date")),
            _safe(row.get("Vendor")),
            _safe(row.get("VendorCategory")),
            _safe(row.get("DocumentType")),
            _safe(row.get("GLAccount")),
            _safe(row.get("TaxCode")),
            _safe(row.get("BookkeepingAmount")),
            _safe(row.get("AmountSource")),
            _safe(row.get("Currency")),
            _safe(row.get("SourceFile")),
            _safe(row.get("SourceItemId")),
            _safe(row.get("Fingerprint")),
            _safe(row.get("Method")),
            _safe(row.get("Confidence")),
            _safe(row.get("VendorIntelSource")),
            _safe(row.get("Notes"))
        ])

    return True