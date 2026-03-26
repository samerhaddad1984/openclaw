from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
import json
import re
import sys

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore  # noqa: E402


# ---------------------------------------------------------------------------
# FIX 1: OCR-normalized invoice number comparison
# ---------------------------------------------------------------------------

def normalize_invoice_number(invoice_number: Optional[str]) -> str:
    """Normalize an invoice number for OCR-resilient comparison.

    - Replace O (letter) with 0 (zero)
    - Replace l (lowercase L) and I (uppercase I) with 1
    - Remove hyphens and spaces
    - Uppercase everything

    So "INV-8B1O9", "INV-88109", "INV88109", "INV-8BIO9" all become "INV88109".
    """
    if not invoice_number:
        return ""
    s = str(invoice_number).strip().upper()
    s = s.replace("O", "0")   # letter O → zero
    s = s.replace("I", "1")   # uppercase I → one
    s = s.replace("L", "1")   # uppercase L (from .upper()) → one
    s = s.replace("S", "5")   # letter S → five (OCR frequently confuses S/5)
    s = s.replace("-", "").replace(" ", "")
    return s


def _invoice_number_match(left: Any, right: Any) -> bool:
    """Check if two documents have matching invoice numbers after OCR normalization."""
    left_inv = getattr(left, "invoice_number", None) or ""
    right_inv = getattr(right, "invoice_number", None) or ""
    if not left_inv or not right_inv:
        return False
    return normalize_invoice_number(left_inv) == normalize_invoice_number(right_inv)


@dataclass
class DuplicateCandidate:
    left_document_id: str
    right_document_id: str
    left_file_name: str
    right_file_name: str
    left_vendor: Optional[str]
    right_vendor: Optional[str]
    left_amount: Optional[float]
    right_amount: Optional[float]
    left_date: Optional[str]
    right_date: Optional[str]
    left_client_code: Optional[str]
    right_client_code: Optional[str]
    score: float
    reasons: list[str]


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    s = str(value).strip().lower()
    s = s.replace("é", "e").replace("è", "e").replace("ê", "e").replace("ë", "e")
    s = s.replace("à", "a").replace("â", "a")
    s = s.replace("î", "i").replace("ï", "i")
    s = s.replace("ô", "o")
    s = s.replace("ù", "u").replace("û", "u").replace("ü", "u")
    s = s.replace("ç", "c")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _amount_equal(a: Optional[float], b: Optional[float], tolerance: float = 0.01) -> bool:
    if a is None or b is None:
        return False
    da = Decimal(str(a))
    db = Decimal(str(b))
    return abs(da - db) <= Decimal(str(tolerance))


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    text = str(value).strip()

    date_formats = [
        "%Y-%m-%d",
        "%Y-%d-%m",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass

    return None


def _date_distance_days(a: Optional[str], b: Optional[str]) -> Optional[int]:
    da = _parse_date(a)
    db = _parse_date(b)
    if not da or not db:
        return None
    return abs((da - db).days)


def _vendor_similarity(a: Optional[str], b: Optional[str]) -> float:
    na = _normalize_text(a)
    nb = _normalize_text(b)

    if not na or not nb:
        return 0.0

    if na == nb:
        return 1.0

    if na in nb or nb in na:
        return 0.92

    a_tokens = set(na.split())
    b_tokens = set(nb.split())
    if not a_tokens or not b_tokens:
        return 0.0

    overlap = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return overlap / union if union else 0.0


def score_pair(left: Any, right: Any) -> DuplicateCandidate:
    reasons: list[str] = []
    score = 0.0

    left_client = getattr(left, "client_code", None)
    right_client = getattr(right, "client_code", None)

    if left_client and right_client and left_client == right_client:
        score += 0.20
        reasons.append("same_client")

    vendor_sim = _vendor_similarity(getattr(left, "vendor", None), getattr(right, "vendor", None))
    if vendor_sim >= 1.0:
        score += 0.35
        reasons.append("same_vendor_exact")
    elif vendor_sim >= 0.90:
        score += 0.28
        reasons.append("same_vendor_close")
    elif vendor_sim >= 0.60:
        score += 0.14
        reasons.append("same_vendor_partial")

    left_amount = getattr(left, "amount", None)
    right_amount = getattr(right, "amount", None)
    if _amount_equal(left_amount, right_amount):
        score += 0.35
        reasons.append("same_amount")

    date_gap = _date_distance_days(getattr(left, "document_date", None), getattr(right, "document_date", None))
    if date_gap == 0:
        score += 0.20
        reasons.append("same_date")
    elif date_gap is not None and date_gap <= 3:
        score += 0.12
        reasons.append("date_within_3_days")
    elif date_gap is not None and date_gap <= 10:
        score += 0.05
        reasons.append("date_within_10_days")

    left_file = _normalize_text(getattr(left, "file_name", None))
    right_file = _normalize_text(getattr(right, "file_name", None))
    if left_file and right_file and left_file == right_file:
        score += 0.20
        reasons.append("same_file_name")

    # FIX 1: OCR-normalized invoice number matching
    # Guard: OCR normalization is lossy (S→5, O→0, I→1). When vendors are
    # clearly different, a post-normalization invoice match is likely a
    # coincidence of the character substitution, NOT evidence of a duplicate.
    if _invoice_number_match(left, right):
        if vendor_sim >= 0.40:
            score += 0.30
            reasons.append("same_invoice_number_ocr_normalized")
        else:
            # Vendors are clearly different — don't trust lossy OCR normalization
            score += 0.05
            reasons.append("invoice_number_ocr_match_weak_vendor")

    if "same_vendor_exact" in reasons and "same_amount" in reasons and "same_date" in reasons:
        score = max(score, 0.97)
        reasons.append("strong_duplicate_pattern")

    if "same_vendor_close" in reasons and "same_amount" in reasons and "same_date" in reasons:
        score = max(score, 0.93)
        reasons.append("strong_duplicate_pattern_close_vendor")

    if "same_vendor_exact" in reasons and "same_amount" in reasons and "date_within_3_days" in reasons:
        score = max(score, 0.88)
        reasons.append("likely_duplicate_near_date")

    return DuplicateCandidate(
        left_document_id=getattr(left, "document_id"),
        right_document_id=getattr(right, "document_id"),
        left_file_name=getattr(left, "file_name"),
        right_file_name=getattr(right, "file_name"),
        left_vendor=getattr(left, "vendor", None),
        right_vendor=getattr(right, "vendor", None),
        left_amount=getattr(left, "amount", None),
        right_amount=getattr(right, "amount", None),
        left_date=getattr(left, "document_date", None),
        right_date=getattr(right, "document_date", None),
        left_client_code=left_client,
        right_client_code=right_client,
        score=round(score, 4),
        reasons=reasons,
    )


def find_duplicate_candidates(
    documents: list[Any],
    min_score: float = 0.85,
) -> list[DuplicateCandidate]:
    candidates: list[DuplicateCandidate] = []

    total = len(documents)
    for i in range(total):
        left = documents[i]

        left_status = getattr(left, "review_status", None)
        if left_status not in {"Ready", "NeedsReview"}:
            continue

        for j in range(i + 1, total):
            right = documents[j]

            right_status = getattr(right, "review_status", None)
            if right_status not in {"Ready", "NeedsReview"}:
                continue

            # Cross-client isolation: only compare documents with same client_code
            left_cc = getattr(left, "client_code", None)
            right_cc = getattr(right, "client_code", None)
            if left_cc and right_cc and left_cc != right_cc:
                continue

            candidate = score_pair(left, right)
            if candidate.score >= min_score:
                candidates.append(candidate)

    candidates.sort(key=lambda x: (-x.score, x.left_file_name.lower(), x.right_file_name.lower()))
    return candidates


def find_duplicate_candidates_from_store(
    store: TaskStore,
    min_score: float = 0.85,
) -> list[DuplicateCandidate]:
    docs = store.list_documents()
    return find_duplicate_candidates(docs, min_score=min_score)


def candidates_to_json(candidates: list[DuplicateCandidate]) -> str:
    return json.dumps([asdict(c) for c in candidates], indent=2, ensure_ascii=False)