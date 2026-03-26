from __future__ import annotations

import re
from pathlib import Path
from typing import Any

try:
    from pdfminer.high_level import extract_text
except Exception:
    extract_text = None


AMOUNT_RE = re.compile(r"([0-9]+\.[0-9]{2})")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def read_file_text(file_path: Path) -> str:

    if file_path.suffix.lower() == ".txt":
        return file_path.read_text(errors="ignore")

    if file_path.suffix.lower() == ".pdf" and extract_text:
        try:
            return extract_text(str(file_path))
        except Exception:
            return ""

    return ""


def detect_vendor(text: str) -> str | None:

    lines = text.splitlines()

    for line in lines[:10]:

        line = line.strip()

        if len(line) > 3:
            return line

    return None


def detect_amount(text: str) -> float | None:

    match = AMOUNT_RE.search(text)

    if not match:
        return None

    try:
        return float(match.group(1))
    except Exception:
        return None


def detect_date(text: str) -> str | None:

    match = DATE_RE.search(text)

    if not match:
        return None

    return match.group(0)


def classify_doc_type(text: str) -> str:

    t = text.lower()

    if "invoice" in t:
        return "invoice"

    if "receipt" in t:
        return "receipt"

    if "statement" in t:
        return "statement"

    return "document"


def process_document(file_path: Path) -> dict[str, Any] | None:

    text = read_file_text(file_path)

    if not text:
        return None

    vendor = detect_vendor(text)

    amount = detect_amount(text)

    date = detect_date(text)

    doc_type = classify_doc_type(text)

    return {
        "vendor": vendor,
        "doc_type": doc_type,
        "amount": amount,
        "date": date,
        "client_code": None,
        "confidence": 0.5,
    }