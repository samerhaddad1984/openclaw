from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FingerprintBundle:
    physical_id: str
    logical_fingerprint: str


def compute_file_sha256(file_path: Path) -> str:

    sha = hashlib.sha256()

    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            sha.update(chunk)

    return sha.hexdigest()


def normalize_text(value: str | None) -> str:

    if not value:
        return "unknown"

    # NFKD normalize + casefold + strip accents for case/accent insensitivity
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.casefold().strip()

    value = value.replace(",", "")
    value = value.replace(".", "")

    # Normalize internal whitespace to single spaces
    import re as _re
    value = _re.sub(r"\s+", " ", value).strip()

    return value


def build_logical_key(
    vendor: str | None,
    date: str | None,
    amount: float | None,
    doc_type: str | None,
) -> str:

    vendor = normalize_text(vendor)
    date = normalize_text(date)
    doc_type = normalize_text(doc_type)

    if amount is None:
        amount_str = "0"
    else:
        amount_str = f"{amount:.2f}"

    return f"{vendor}|{date}|{amount_str}|{doc_type}"


def build_logical_fingerprint(
    vendor: str | None,
    date: str | None,
    amount: float | None,
    doc_type: str | None,
) -> str:

    key = build_logical_key(vendor, date, amount, doc_type)

    sha = hashlib.sha256(key.encode())

    return sha.hexdigest()[:24]


def build_physical_identity(
    file_name: str,
    file_hash: str,
) -> str:

    raw = f"{file_name}|{file_hash}"

    sha = hashlib.sha256(raw.encode())

    return sha.hexdigest()[:24]


def physical_fingerprint(content: bytes) -> str:
    """Compute a physical fingerprint (SHA-256) from raw file content."""
    return hashlib.sha256(content).hexdigest()[:24]


def logical_fingerprint(
    vendor: str | None,
    date: str | None,
    amount: str | float | None,
    doc_type: str | None,
) -> str:
    """Compute logical fingerprint with case/accent-insensitive vendor normalization."""
    amt = None
    if amount is not None:
        try:
            amt = float(amount)
        except (ValueError, TypeError):
            amt = None
    return build_logical_fingerprint(vendor, date, amt, doc_type)


def build_fingerprint_bundle(
    record: dict,
    file_name: str,
    file_hash: str,
) -> FingerprintBundle:

    vendor = record.get("vendor")
    date = record.get("date")
    amount = record.get("amount")
    doc_type = record.get("doc_type")

    logical = build_logical_fingerprint(vendor, date, amount, doc_type)

    physical = build_physical_identity(file_name, file_hash)

    return FingerprintBundle(
        physical_id=physical,
        logical_fingerprint=logical,
    )


# =========================================================================
# PART 9 — Source fingerprint idempotency
# =========================================================================

def _normalize_ocr_noise(value: str | None) -> str:
    """Normalize OCR noise: O→0, I→1, l→1."""
    if not value:
        return ""
    result = value.strip()
    # Common OCR confusions for invoice numbers
    result = result.replace("O", "0").replace("o", "0")
    result = result.replace("I", "1").replace("l", "1")
    return result


def source_fingerprint(document: dict) -> str:
    """Build a source fingerprint combining:
    - normalized vendor name
    - normalized invoice number (strip OCR noise)
    - amount
    - date range (to handle date ambiguity)
    - source channel

    Used for cross-channel deduplication.
    """
    vendor = normalize_text(document.get("vendor"))
    invoice_number = _normalize_ocr_noise(document.get("invoice_number") or document.get("invoice_no"))
    amount = document.get("amount")
    date = normalize_text(document.get("date") or document.get("document_date"))
    source_channel = normalize_text(document.get("source_channel") or "unknown")

    if amount is not None:
        try:
            amount_str = f"{float(amount):.2f}"
        except (ValueError, TypeError):
            amount_str = "0.00"
    else:
        amount_str = "0.00"

    key = f"{vendor}|{invoice_number}|{amount_str}|{date}|{source_channel}"
    sha = hashlib.sha256(key.encode())
    return sha.hexdigest()[:32]


def source_fingerprint_similarity(fp1: str, fp2: str) -> float:
    """Calculate similarity between two source fingerprints.

    Returns 1.0 for exact match, 0.0 for completely different.
    For partial similarity, compare the underlying document fields.
    """
    if fp1 == fp2:
        return 1.0
    return 0.0


def detect_reingest_conflict(
    new_document: dict,
    existing_document: dict,
    similarity_threshold: float = 0.85,
) -> dict:
    """Detect re-ingestion of same invoice with tiny differences.

    When a new document matches an existing document within similarity
    threshold but has differences:
    1. Do NOT silently overwrite
    2. Do NOT create a silent duplicate
    3. Create a document_conflict record
    4. Require human to choose action
    """
    # Compare key fields
    fields_to_compare = ["vendor", "invoice_number", "amount", "date", "total"]
    differences = []
    match_count = 0
    total_fields = 0

    for field in fields_to_compare:
        new_val = str(new_document.get(field, "")).strip()
        existing_val = str(existing_document.get(field, "")).strip()
        if not new_val and not existing_val:
            continue
        total_fields += 1
        if new_val == existing_val:
            match_count += 1
        else:
            differences.append({
                "field": field,
                "original_value": existing_val,
                "new_value": new_val,
            })

    if total_fields == 0:
        return {
            "is_conflict": False,
            "reasoning": "No comparable fields found.",
        }

    similarity = match_count / total_fields

    if similarity < similarity_threshold:
        return {
            "is_conflict": False,
            "similarity": round(similarity, 4),
            "reasoning": f"Similarity ({similarity:.2f}) below threshold ({similarity_threshold}).",
        }

    if not differences:
        return {
            "is_conflict": False,
            "similarity": 1.0,
            "reasoning": "Documents are identical — exact duplicate.",
            "duplicate_ingestion_candidate": True,
        }

    return {
        "is_conflict": True,
        "conflict_type": "REINGEST_WITH_VARIATION",
        "similarity": round(similarity, 4),
        "original_document": {k: str(existing_document.get(k, "")) for k in fields_to_compare},
        "new_document": {k: str(new_document.get(k, "")) for k in fields_to_compare},
        "differences": differences,
        "available_actions": [
            "UPDATE_ORIGINAL",
            "KEEP_BOTH",
            "REJECT_NEW",
        ],
        "requires_human_decision": True,
        "reasoning": (
            f"New document matches existing within {similarity:.0%} similarity "
            f"but has {len(differences)} difference(s). "
            f"Human must choose: UPDATE_ORIGINAL / KEEP_BOTH / REJECT_NEW."
        ),
    }