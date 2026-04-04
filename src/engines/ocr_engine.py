"""
src/engines/ocr_engine.py
=========================
Multi-format document ingestion pipeline for OtoCPA.

Supported formats (detected from file bytes, not extension):
    PDF, JPEG, PNG, HEIC, TIFF, WebP

Extraction strategy:
    Image formats  → Claude Vision API (OpenAI-compatible image_url)
    PDF            → pdfplumber text; if < 20 words → Vision fallback
    Handwriting    → Vision handles automatically (no special case needed)

Low-confidence threshold: 0.7 — documents below are auto-flagged NeedsReview.

Email ingestion server:
    POST /ingest/email  — accepts raw RFC 2822 / MIME email, extracts
                          all supported attachments, runs each through
                          the pipeline, returns JSON results.
    GET  /health        — liveness probe

Run standalone:
    python src/engines/ocr_engine.py [--host 127.0.0.1] [--port 8789]
"""
from __future__ import annotations

import argparse
import base64
import email
import email.policy
import io
import json
import secrets
import sqlite3
import sys
import time
import traceback
import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import requests

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
_root_str = str(ROOT_DIR)
if _root_str not in sys.path:
    sys.path.insert(0, _root_str)

CONFIG_PATH = ROOT_DIR / "otocpa.config.json"
DB_PATH     = ROOT_DIR / "data" / "otocpa_agent.db"
UPLOAD_DIR  = ROOT_DIR / "data" / "ocr_uploads"

LOW_CONFIDENCE_THRESHOLD = 0.7
PDF_TEXT_MIN_WORDS       = 20


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict[str, Any]:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _vision_provider() -> dict[str, str]:
    """Return the premium_provider config dict (used for Vision calls)."""
    return _load_config().get("ai_router", {}).get("premium_provider", {})


def _ingest_api_key() -> str:
    return _load_config().get("ingest", {}).get("api_key", "")


def _save_config(config: dict[str, Any]) -> None:
    try:
        CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Foreign-currency constants & FX rate helpers
# ---------------------------------------------------------------------------

FOREIGN_REGISTERED_DIGITAL_SERVICES = frozenset({
    "netflix", "spotify", "adobe", "amazon aws", "google cloud",
    "microsoft azure", "apple app store", "google play",
    "meta", "facebook ads", "linkedin", "twitter", "x ads",
    "amazon web services", "aws", "microsoft 365", "office 365",
    "github", "dropbox", "slack", "zoom", "canva", "shopify",
    "mailchimp", "hubspot", "salesforce", "twilio", "stripe",
    "google ads", "google workspace",
})

_DEFAULT_FALLBACK_RATES: dict[str, float] = {
    "USD": 1.38,
    "EUR": 1.50,
    "GBP": 1.75,
    "CHF": 1.55,
}

_BOC_SERIES: dict[str, str] = {
    "USD": "FXUSDCAD",
    "EUR": "FXEURCAD",
    "GBP": "FXGBPCAD",
}


def _is_foreign_registered_digital(vendor_name: str) -> bool:
    """Check if vendor is a known foreign digital service registered for GST."""
    if not vendor_name:
        return False
    v = vendor_name.lower().strip()
    for name in FOREIGN_REGISTERED_DIGITAL_SERVICES:
        if name in v or v in name:
            return True
    return False


def get_fx_rate(currency: str, invoice_date: str | None = None,
                invoice_text: str | None = None) -> Decimal:
    """Get FX rate to CAD for a given currency and date.

    Priority:
      1. Explicit rate stated on invoice text (most accurate)
      2. Bank of Canada Valet API for the date
      3. Last known rate from otocpa.config.json
      4. Hardcoded fallback
    """
    currency = currency.upper().strip()
    if currency == "CAD":
        return Decimal("1.0")

    # 1. Check if invoice states the rate explicitly
    if invoice_text:
        rate_match = re.search(
            r"(?:exchange\s*rate|taux\s*de\s*change|fx\s*rate|rate)\s*[:=]?\s*(\d+\.\d{2,6})",
            invoice_text, re.IGNORECASE,
        )
        if rate_match:
            try:
                return Decimal(rate_match.group(1))
            except Exception:
                pass

    # 2. Try Bank of Canada Valet API
    series = _BOC_SERIES.get(currency)
    if series and invoice_date:
        try:
            date_str = invoice_date[:10]  # YYYY-MM-DD
            url = (
                f"https://www.bankofcanada.ca/valet/observations/{series}/json"
                f"?start_date={date_str}&end_date={date_str}"
            )
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                obs = data.get("observations", [])
                if obs:
                    rate_str = obs[0].get(series, {}).get("v")
                    if rate_str:
                        rate = Decimal(rate_str)
                        # Save as last known rate
                        _update_last_known_rate(currency, float(rate))
                        return rate
        except Exception:
            pass

    # 3. Last known rate from config
    config = _load_config()
    fx_config = config.get("fx_rates", {})
    last_known_key = f"{currency.lower()}_cad_last_known"
    last_known = fx_config.get(last_known_key)
    if last_known:
        try:
            return Decimal(str(last_known))
        except Exception:
            pass

    # 4. Hardcoded fallback
    fallback = _DEFAULT_FALLBACK_RATES.get(currency, 1.38)
    return Decimal(str(fallback))


def get_usd_cad_rate(invoice_date: str | None = None,
                     invoice_text: str | None = None) -> Decimal:
    """Convenience wrapper for USD -> CAD rate."""
    return get_fx_rate("USD", invoice_date, invoice_text)


def _update_last_known_rate(currency: str, rate: float) -> None:
    """Persist the latest fetched rate to config for offline fallback."""
    try:
        config = _load_config()
        if "fx_rates" not in config:
            config["fx_rates"] = {}
        config["fx_rates"][f"{currency.lower()}_cad_last_known"] = round(rate, 6)
        config["fx_rates"]["last_updated"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        _save_config(config)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_doc_id() -> str:
    return "doc_" + secrets.token_hex(6)


# ---------------------------------------------------------------------------
# Magic-byte format detection
# ---------------------------------------------------------------------------

def detect_format(data: bytes) -> str:
    """
    Detect file format from magic bytes.

    Returns one of: pdf | jpeg | png | tiff | webp | heic | unknown

    Never relies on file extension.
    """
    if len(data) < 12:
        return "unknown"

    # PDF
    if data[:4] == b"%PDF":
        return "pdf"

    # PNG
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"

    # JPEG
    if data[:3] == b"\xff\xd8\xff":
        return "jpeg"

    # TIFF (little-endian II or big-endian MM)
    if data[:4] in (b"II*\x00", b"MM\x00*"):
        return "tiff"

    # WebP: bytes 0-3 = RIFF, bytes 8-11 = WEBP
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"

    # HEIC / HEIF — ISO Base Media (ftyp box)
    # Layout: [box_size:4][ftyp:4][major_brand:4]...
    if data[4:8] == b"ftyp":
        brand = data[8:12]
        _HEIC_BRANDS = {
            b"heic", b"heix", b"hevc", b"mif1",
            b"msf1", b"miaf", b"heif", b"mif2",
        }
        if brand in _HEIC_BRANDS:
            return "heic"

    return "unknown"


_SUPPORTED_FORMATS: frozenset[str] = frozenset(
    {"pdf", "jpeg", "png", "tiff", "webp", "heic"}
)


# ---------------------------------------------------------------------------
# Document type detection (bank statement, pay stub, credit memo, etc.)
# ---------------------------------------------------------------------------

_BANK_STATEMENT_PATTERNS = re.compile(
    r"("
    r"statement of account|relevé de compte|"
    r"opening balance|solde d'ouverture|"
    r"closing balance|solde de clôture|"
    r"\bdeposits\b.*\bwithdrawals\b|\bwithdrawals\b.*\bdeposits\b|"
    r"\bCIBC\b|Desjardins relevé|TD Bank statement|"
    r"RBC statement|BMO statement|Scotia statement"
    r")",
    re.IGNORECASE,
)

_CREDIT_CARD_STATEMENT_PATTERNS = re.compile(
    r"("
    r"credit card statement|relevé de carte|"
    r"minimum payment|paiement minimum|"
    r"credit limit|limite de crédit|"
    r"(?:Visa|Mastercard).*statement|statement.*(?:Visa|Mastercard)"
    r")",
    re.IGNORECASE,
)

_PAY_STUB_PATTERNS = re.compile(
    r"("
    r"pay stub|talón de paga|\bpaie\b|"
    r"\bearnings\b.*\bdéductions\b|\bdéductions\b.*\bearnings\b|"
    r"\bnet pay\b|salaire net|"
    r"\bROE\b|\bT4\b"
    r")",
    re.IGNORECASE,
)

_CREDIT_MEMO_PATTERNS = re.compile(
    r"("
    r"credit note|note de crédit|credit memo|\bavoir\b"
    r")",
    re.IGNORECASE,
)

_PURCHASE_ORDER_PATTERNS = re.compile(
    r"("
    r"purchase order|bon de commande|"
    r"\bP\.?O\.?\s*#|\bPO\s*number|numéro de commande|"
    r"order confirmation|confirmation de commande"
    r")",
    re.IGNORECASE,
)


def detect_document_type(text: str, filename: str = "") -> str:
    """Classify document type from extracted text and filename.

    Returns one of: invoice, bank_statement, credit_card_statement,
    pay_stub, receipt, credit_memo, purchase_order.
    """
    if not text:
        return "invoice"

    combined = f"{text} {filename}"

    # Credit card statement before bank statement (more specific)
    if _CREDIT_CARD_STATEMENT_PATTERNS.search(combined):
        return "credit_card_statement"

    if _BANK_STATEMENT_PATTERNS.search(combined):
        return "bank_statement"

    if _PAY_STUB_PATTERNS.search(combined):
        return "pay_stub"

    if _CREDIT_MEMO_PATTERNS.search(combined):
        return "credit_memo"

    if _PURCHASE_ORDER_PATTERNS.search(combined):
        return "purchase_order"

    # Receipt: short document with a total amount
    lines = [ln for ln in text.strip().splitlines() if ln.strip()]
    if len(lines) < 20 and re.search(r"\btotal\b", text, re.IGNORECASE):
        # Only classify as receipt if no GST/BN number (those are invoices)
        if not re.search(r"\b\d{9}\s?RT\b", text):
            return "receipt"

    return "invoice"


# ---------------------------------------------------------------------------
# Image normalisation (HEIC → JPEG when Pillow is available)
# ---------------------------------------------------------------------------

def _normalise_image(data: bytes, fmt: str) -> tuple[bytes, str]:
    """
    Return (image_bytes, mime_type) ready for the Vision API.

    HEIC is converted to JPEG via pillow-heif when available; falls
    through to raw bytes otherwise (some Vision endpoints accept it).
    """
    if fmt == "heic":
        try:
            import pillow_heif  # type: ignore[import]
            pillow_heif.register_heif_opener()
            from PIL import Image  # type: ignore[import]
            img = Image.open(io.BytesIO(data))
            buf = io.BytesIO()
            img.convert("RGB").save(buf, format="JPEG", quality=92)
            return buf.getvalue(), "image/jpeg"
        except Exception:
            return data, "image/heic"

    _MIME: dict[str, str] = {
        "jpeg": "image/jpeg",
        "png":  "image/png",
        "tiff": "image/tiff",
        "webp": "image/webp",
    }
    return data, _MIME.get(fmt, "application/octet-stream")


# ---------------------------------------------------------------------------
# Handwriting detection  (PART 1)
# ---------------------------------------------------------------------------

HANDWRITING_HIGH_THRESHOLD = 0.6
HANDWRITING_LOW_THRESHOLD  = 0.4


def detect_handwriting(image_bytes: bytes) -> float:
    """
    Return a handwriting probability score 0.0-1.0 using heuristics.

    Heuristics applied:
      1. pdfplumber text extraction — fewer than 10 words → +0.4
      2. Image pixel variance (Pillow) — high variance → +0.2
      3. Average word length — short avg word length → +0.1
    """
    score = 0.0
    text = ""

    # 1. Attempt pdfplumber text extraction
    fmt = detect_format(image_bytes)
    if fmt == "pdf":
        text = extract_pdf_text(image_bytes)
    word_count = len(text.split()) if text else 0

    # Digital PDF with substantial extractable text is never handwritten
    if fmt == "pdf" and word_count >= PDF_TEXT_MIN_WORDS:
        return 0.0

    if word_count < 10:
        score += 0.4

    # 2. Check image variance using Pillow
    try:
        from PIL import Image, ImageStat  # type: ignore[import]
        if fmt == "pdf":
            images = _pdf_to_images(image_bytes)
            if images:
                img = Image.open(io.BytesIO(images[0][0]))
            else:
                img = None
        else:
            img = Image.open(io.BytesIO(image_bytes))
        if img is not None:
            img_grey = img.convert("L")
            stat = ImageStat.Stat(img_grey)
            # Variance > 2000 suggests handwriting (irregular strokes)
            if stat.var[0] > 2000:
                score += 0.2
    except Exception:
        pass

    # 3. Check average word length
    if text:
        words = text.split()
        if words:
            avg_len = sum(len(w) for w in words) / len(words)
            if avg_len < 4.0:
                score += 0.1

    return min(1.0, score)


# ---------------------------------------------------------------------------
# Handwriting prompt (loaded from file)
# ---------------------------------------------------------------------------

_HANDWRITING_PROMPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "prompts" / "handwritten_receipt.txt"


def _load_handwriting_prompt() -> str:
    """Load the handwritten receipt prompt template."""
    try:
        return _HANDWRITING_PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        # Fallback: basic prompt if file missing
        return (
            "This is a handwritten receipt. Extract all visible fields as JSON. "
            "Return illegible for unreadable fields. Return JSON only."
        )


# ---------------------------------------------------------------------------
# Handwriting post-processing  (PART 3)
# ---------------------------------------------------------------------------

# Quebec month names → month number
_QC_MONTHS: dict[str, str] = {
    "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "décembre": "12", "decembre": "12",
    "jan": "01", "fév": "02", "fev": "02", "mar": "03", "avr": "04",
    "jui": "06", "jul": "07", "aoû": "08", "sep": "09", "oct": "10",
    "nov": "11", "déc": "12", "dec": "12",
    # English month names
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

_CENT = Decimal("0.01")


def _fix_quebec_amount(raw: Any) -> float | None:
    """
    Normalise Quebec amount formats to a Python float.

    Handles: 14,50  |  14,50$  |  $14.50  |  14.50$  |  1 234,50$
    """
    if raw is None or raw == "illegible":
        return None
    s = str(raw).strip()
    if not s or s.lower() == "illegible":
        return None
    # Remove dollar signs and spaces used as thousands separators
    s = s.replace("$", "").replace("\u00a0", "").strip()
    # Remove spaces that are thousands separators (e.g. "1 234,50" → "1234,50")
    s = re.sub(r"(\d)\s+(\d)", r"\1\2", s)
    # Convert comma decimal to dot decimal
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        d = Decimal(s).quantize(_CENT, rounding=ROUND_HALF_UP)
        return float(d)
    except (InvalidOperation, ValueError):
        return None


def _fix_quebec_date(raw: Any) -> str | None:
    """
    Normalise Quebec date formats to YYYY-MM-DD.

    Handles:
      19 mars 2026  |  mars 19 2026  |  19/03/26  |  19-03-2026  |  2026-03-19
    """
    if raw is None or raw == "illegible":
        return None
    s = str(raw).strip().lower()
    if not s or s == "illegible":
        return None

    # Already ISO: YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # DD/MM/YY or DD-MM-YYYY or DD/MM/YYYY
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})$", s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        return f"{year}-{month:02d}-{day:02d}"

    # "19 mars 2026" or "mars 19 2026" or "19 mars, 2026"
    s_clean = s.replace(",", " ").strip()
    parts = s_clean.split()
    if len(parts) >= 3:
        # Try day-month-year
        for month_name, month_num in _QC_MONTHS.items():
            if month_name in parts:
                idx = parts.index(month_name)
                others = [p for i, p in enumerate(parts) if i != idx]
                nums = [p for p in others if p.isdigit()]
                if len(nums) >= 2:
                    a, b = int(nums[0]), int(nums[1])
                    if a > 31:
                        year, day = a, b
                    elif b > 31:
                        day, year = a, b
                    else:
                        day, year = a, b
                    if year < 100:
                        year += 2000
                    return f"{year}-{month_num}-{day:02d}"

    return None


def _post_process_handwriting(result: dict[str, Any]) -> dict[str, Any]:
    """
    Post-process handwriting extraction results:
      1. Fix Quebec amount formats
      2. Fix Quebec date formats
      3. Math validation (subtotal + gst + qst ≈ total)
      4. Vendor cross-reference against learning_memory_patterns
      5. Set illegible fields to None with review notes
      6. Flag low-confidence handwriting for review
    """
    # 1. Fix Quebec amounts
    for key in ("amount", "gst_amount", "qst_amount", "total"):
        val = result.get(key)
        result[key] = _fix_quebec_amount(val)

    # Also normalise subtotal if present
    if "subtotal" in result:
        result["subtotal"] = _fix_quebec_amount(result.get("subtotal"))

    # 2. Fix Quebec date
    raw_date = result.get("date") or result.get("document_date")
    fixed_date = _fix_quebec_date(raw_date)
    result["document_date"] = fixed_date
    if "date" in result:
        result["date"] = fixed_date

    # 5. Set illegible fields to None and build review notes
    #    (do this BEFORE mapping so we can detect "illegible" strings)
    review_notes: list[str] = []
    _ILLEGIBLE_FIELDS = [
        "vendor_name", "amount", "date", "document_date",
        "gst_amount", "qst_amount", "total", "payment_method",
    ]
    for field in _ILLEGIBLE_FIELDS:
        val = result.get(field)
        if val == "illegible":
            result[field] = None
            review_notes.append(
                f"{field}: illisible / illegible"
            )

    # Map handwriting-specific fields to standard schema
    if result.get("amount") is None and result.get("total") is not None:
        result["amount"] = result["total"]

    # 3. Math validation: subtotal + gst + qst ≈ total
    confidence = float(result.get("confidence") or 0.0)
    gst = result.get("gst_amount")
    qst = result.get("qst_amount")
    total = result.get("total")
    subtotal = result.get("subtotal") or result.get("amount")

    if subtotal is not None and gst is not None and qst is not None and total is not None:
        try:
            computed = float(subtotal) + float(gst) + float(qst)
            delta = abs(computed - float(total))
            if delta <= 0.05:
                confidence = min(1.0, confidence + 0.10)
                result["math_validated"] = True
        except (TypeError, ValueError):
            pass

    # 4. Vendor cross-reference against learning_memory
    vendor = result.get("vendor_name")
    if vendor and vendor != "illegible":
        try:
            from src.agents.core.learning_memory_store import LearningMemoryStore
            store = LearningMemoryStore()
            patterns = store.search(vendor)
            if patterns:
                confidence = min(1.0, confidence + 0.15)
                result["vendor_matched"] = True
        except Exception:
            pass

    result["confidence"] = round(confidence, 4)

    # Also check field_confidence for low values
    field_conf = result.get("field_confidence", {})
    for field, conf_val in field_conf.items():
        try:
            if float(conf_val) < 0.5 and field not in [n.split(":")[0] for n in review_notes]:
                review_notes.append(
                    f"{field}: faible confiance / low confidence ({conf_val})"
                )
        except (TypeError, ValueError):
            pass

    if review_notes:
        result["review_notes"] = "; ".join(review_notes)

    # 6. Flag low-confidence handwriting for review
    if confidence < 0.65:
        result["review_status"] = "NeedsReview"
        result["handwriting_low_confidence"] = True
    else:
        result.setdefault("handwriting_low_confidence", False)

    return result


# ---------------------------------------------------------------------------
# Handwriting Vision call (uses handwriting-specific prompt)
# ---------------------------------------------------------------------------

def call_vision_handwriting(image_bytes: bytes, mime_type: str) -> dict[str, Any]:
    """
    Send one image to Claude Vision using the handwriting-specific prompt.
    Returns the parsed JSON result.
    """
    prov = _vision_provider()
    base_url = prov.get("base_url", "").rstrip("/")
    api_key  = prov.get("api_key", "")
    model    = prov.get("model", "")

    if not base_url or not api_key or not model:
        raise RuntimeError("vision_provider_not_configured")

    url    = f"{base_url}/chat/completions"
    b64    = base64.b64encode(image_bytes).decode("ascii")
    data_uri = f"data:{mime_type};base64,{b64}"

    prompt_text = _load_handwriting_prompt()

    payload = {
        "model":       model,
        "temperature": 0.0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": data_uri}},
                    {"type": "text",      "text": prompt_text},
                ],
            }
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Vision API HTTP {r.status_code}: {r.text[:400]}")

    content = r.json()["choices"][0]["message"]["content"].strip()

    # Strip markdown code fences if the model wraps its JSON
    if content.startswith("```"):
        lines = content.splitlines()
        inner = lines[1:] if len(lines) < 3 else lines[1:-1]
        content = "\n".join(inner)

    try:
        raw = json.loads(content)
    except json.JSONDecodeError:
        raise RuntimeError(f"Vision API returned non-JSON: {content[:500]}")

    # Post-process handwriting result
    raw["extraction_method"] = "vision_handwriting"
    return _post_process_handwriting(raw)


# ---------------------------------------------------------------------------
# Claude Vision extraction
# ---------------------------------------------------------------------------

_VISION_SYSTEM = (
    "You are an accounting document extractor for a Canadian bookkeeping workflow. "
    "Return STRICT JSON only. Never invent numbers not visible in the document."
)

_VISION_PROMPT = """\
Extract accounting data from this document image.

Return JSON with EXACTLY this schema (no extra keys):
{
  "doc_type":       "invoice|receipt|bank_statement|payroll|other|unknown",
  "vendor_name":    "<string or null>",
  "document_date":  "<YYYY-MM-DD or null>",
  "invoice_number": "<string or null>",
  "currency":       "CAD|USD|OTHER",
  "subtotal":       <number or null>,
  "tax_total":      <number or null>,
  "total":          <number or null>,
  "taxes":          [{"type": "GST|HST|QST|PST|OTHER", "amount": <number>}],
  "confidence":     <0.0 to 1.0>,
  "notes":          "<any relevant observations>"
}

Guidelines:
- Set confidence < 0.7 for blurry, partial, or ambiguous documents.
- Set confidence > 0.9 only when all key fields are clearly legible.
- For handwritten documents extract exactly as written; flag confidence accordingly.

CRITICAL - Find tax registration numbers anywhere in the document:
- GST/HST number format: 9 digits + RT + 4 digits (e.g. 805577574RT0001 or 764831038RT9999)
- QST number format: 10 digits (e.g. 1221825787 or 1234567890)
- They may appear in: headers, tax breakdown lines, footer, embedded in descriptions like 'GST/TPS # 805577574 RT0001 (5.00%)'
- Also look for BN# (Business Number) which has the same 9-digit root
- Return all found numbers in gst_number and qst_number fields
"""


def call_vision(image_bytes: bytes, mime_type: str) -> dict[str, Any]:
    """
    Send one image to Claude Vision via the premium provider's
    OpenAI-compatible chat/completions endpoint.

    Raises RuntimeError on configuration or HTTP errors.
    """
    prov = _vision_provider()
    base_url = prov.get("base_url", "").rstrip("/")
    api_key  = prov.get("api_key", "")
    model    = prov.get("model", "")

    if not base_url or not api_key or not model:
        raise RuntimeError("vision_provider_not_configured")

    url    = f"{base_url}/chat/completions"
    b64    = base64.b64encode(image_bytes).decode("ascii")
    data_uri = f"data:{mime_type};base64,{b64}"

    payload = {
        "model":       model,
        "temperature": 0.0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": data_uri}},
                    {"type": "text",      "text": _VISION_PROMPT},
                ],
            }
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Vision API HTTP {r.status_code}: {r.text[:400]}")

    content = r.json()["choices"][0]["message"]["content"].strip()

    # Strip markdown code fences if the model wraps its JSON
    if content.startswith("```"):
        lines = content.splitlines()
        # drop first line (```json or ```) and last line (```)
        inner = lines[1:] if len(lines) < 3 else lines[1:-1]
        content = "\n".join(inner)

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        raise RuntimeError(f"Vision API returned non-JSON: {content[:500]}")


# ---------------------------------------------------------------------------
# PDF text extraction  (pdfplumber → pdfminer fallback)
# ---------------------------------------------------------------------------

def extract_pdf_text(data: bytes) -> str:
    """
    Extract embedded text from PDF bytes.

    Tries pdfplumber first (richer table/layout support), then pdfminer.
    Returns empty string on complete failure.
    """
    # ---- pdfplumber (preferred) -----------------------------------------
    try:
        import pdfplumber  # type: ignore[import]
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        text = "\n".join(pages).strip()
        if text:
            return text
    except ImportError:
        pass
    except Exception:
        pass

    # ---- pdfminer fallback ----------------------------------------------
    try:
        from pdfminer.high_level import extract_text as _pdfminer_extract  # type: ignore[import]
        text = _pdfminer_extract(io.BytesIO(data)) or ""
        return text.strip()
    except ImportError:
        pass
    except Exception:
        pass

    return ""


# ---------------------------------------------------------------------------
# PDF → images for Vision fallback (used on scanned PDFs)
# ---------------------------------------------------------------------------

def _pdf_to_images(data: bytes) -> list[tuple[bytes, str]]:
    """
    Rasterise the first ≤3 pages of a PDF to JPEG bytes.

    Tries pdf2image first, then pypdfium2.
    Returns a list of (jpeg_bytes, "image/jpeg") tuples.
    """
    result: list[tuple[bytes, str]] = []

    # ---- pdf2image -------------------------------------------------------
    try:
        from pdf2image import convert_from_bytes  # type: ignore[import]
        images = convert_from_bytes(data, dpi=200, first_page=1, last_page=3)
        for img in images:
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=90)
            result.append((buf.getvalue(), "image/jpeg"))
        return result
    except ImportError:
        pass
    except Exception:
        pass

    # ---- pypdfium2 fallback ---------------------------------------------
    try:
        import pypdfium2 as pdfium  # type: ignore[import]
        doc = pdfium.PdfDocument(data)
        for i in range(min(3, len(doc))):
            page   = doc[i]
            bitmap = page.render(scale=2.0)
            pil_img = bitmap.to_pil()
            buf = io.BytesIO()
            pil_img.save(buf, format="JPEG", quality=90)
            result.append((buf.getvalue(), "image/jpeg"))
        return result
    except ImportError:
        pass
    except Exception:
        pass

    return []


# ---------------------------------------------------------------------------
# Text-to-extraction  (digital PDFs with good embedded text)
# ---------------------------------------------------------------------------

def _extract_from_text(text: str) -> dict[str, Any]:
    """
    Run doc_ai classify_and_extract on plain text.

    Falls back to a minimal low-confidence dict on any error.
    """
    try:
        from src.agents.tools.doc_ai import classify_and_extract  # type: ignore[import]
        return classify_and_extract(text)
    except Exception as exc:
        return {
            "doc_type":       "unknown",
            "vendor_name":    None,
            "document_date":  None,
            "invoice_number": None,
            "currency":       "CAD",
            "subtotal":       None,
            "tax_total":      None,
            "total":          None,
            "taxes":          [],
            "confidence":     0.3,
            "notes":          f"Text-extraction fallback error: {exc}",
        }


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

_NEW_COLUMNS: list[tuple[str, str]] = [
    ("currency",              "TEXT"),
    ("subtotal",              "REAL"),
    ("tax_total",             "REAL"),
    ("extraction_method",     "TEXT"),
    ("ingest_source",         "TEXT"),
    ("raw_ocr_text",          "TEXT"),
    ("hallucination_suspected",    "INTEGER"),
    ("correction_count",           "INTEGER"),
    ("handwriting_low_confidence", "INTEGER"),
    ("handwriting_sample",         "INTEGER"),
    ("gl_account",                 "TEXT"),
    ("tax_code",                   "TEXT"),
    ("category",                   "TEXT"),
]


def _ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    for col, typedef in _NEW_COLUMNS:
        if col not in existing:
            conn.execute(f"ALTER TABLE documents ADD COLUMN {col} {typedef}")
    conn.commit()


def upsert_document(record: dict[str, Any], *, db_path: Path = DB_PATH) -> None:
    """
    Insert a new document row or update an existing one by document_id.

    Columns absent from *record* are left untouched on conflict.
    """
    # Apply safe defaults for columns added after the initial schema so that
    # callers that build records without these keys still work correctly.
    record = {
        "raw_ocr_text":              None,
        "hallucination_suspected":   0,
        "handwriting_low_confidence": 0,
        "gl_account":                None,
        "tax_code":                  None,
        "category":                  None,
        **record,
    }
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_columns(conn)
        conn.execute(
            """
            INSERT INTO documents (
                document_id, file_name, file_path, client_code,
                vendor, doc_type, amount, document_date,
                gl_account, tax_code, category,
                review_status, confidence, raw_result,
                created_at, updated_at, submitted_by, client_note,
                currency, subtotal, tax_total,
                extraction_method, ingest_source,
                raw_ocr_text, hallucination_suspected,
                handwriting_low_confidence
            ) VALUES (
                :document_id, :file_name, :file_path, :client_code,
                :vendor, :doc_type, :amount, :document_date,
                :gl_account, :tax_code, :category,
                :review_status, :confidence, :raw_result,
                :created_at, :updated_at, :submitted_by, :client_note,
                :currency, :subtotal, :tax_total,
                :extraction_method, :ingest_source,
                :raw_ocr_text, :hallucination_suspected,
                :handwriting_low_confidence
            )
            ON CONFLICT(document_id) DO UPDATE SET
                vendor                      = excluded.vendor,
                doc_type                    = excluded.doc_type,
                amount                      = excluded.amount,
                document_date               = excluded.document_date,
                gl_account                  = COALESCE(excluded.gl_account, documents.gl_account),
                tax_code                    = COALESCE(excluded.tax_code, documents.tax_code),
                category                    = COALESCE(excluded.category, documents.category),
                review_status               = excluded.review_status,
                confidence                  = excluded.confidence,
                raw_result                  = excluded.raw_result,
                updated_at                  = excluded.updated_at,
                currency                    = excluded.currency,
                subtotal                    = excluded.subtotal,
                tax_total                   = excluded.tax_total,
                extraction_method           = excluded.extraction_method,
                ingest_source               = excluded.ingest_source,
                raw_ocr_text                = excluded.raw_ocr_text,
                hallucination_suspected     = excluded.hallucination_suspected,
                handwriting_low_confidence  = excluded.handwriting_low_confidence
            """,
            record,
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Post-extraction enrichment — GL account, tax code, category
# ---------------------------------------------------------------------------

def enrich_extracted_fields(result: dict[str, Any], client_code: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """Fill gl_account, tax_code, and category from learning/substance/tax engines."""
    vendor = result.get("vendor") or result.get("vendor_name") or ""
    amount = result.get("amount") or 0

    # GL account from learning engine
    if not result.get("gl_account") and vendor:
        try:
            from src.agents.core.gl_account_learning_engine import suggest_gl_account
            gl = suggest_gl_account(conn, client_code=client_code, vendor=vendor)
            if gl and gl.get("confidence", 0) > 0.50:
                result["gl_account"] = gl["gl_account"]
                result["gl_confidence"] = gl["confidence"]
        except Exception:
            pass

    # Category from substance engine
    if not result.get("category"):
        try:
            from src.engines.substance_engine import classify_substance
            substance = classify_substance(
                vendor_name=vendor,
                amount=amount,
                memo=result.get("description") or "",
            )
            result["category"] = substance.get("substance_type", "expense")
            result["is_capex"] = substance.get("potential_capex", False)
        except Exception:
            result["category"] = "expense"

    # Tax code from tax engine
    if not result.get("tax_code") and vendor:
        try:
            from src.engines.tax_engine import suggest_tax_code
            tax = suggest_tax_code(vendor, result.get("gst_number") or "", conn)
            if tax:
                result["tax_code"] = tax
        except Exception:
            pass

    # Default fallbacks if still empty
    if not result.get("category"):
        result["category"] = "expense"
    if not result.get("tax_code"):
        result["tax_code"] = "T"
    if not result.get("gl_account"):
        vendor_lower = vendor.lower()
        if any(x in vendor_lower for x in ["openai", "microsoft", "google", "adobe", "software", "subscription"]):
            result["gl_account"] = "5420"
            result["gl_account_name"] = "Logiciels et abonnements"
        elif any(x in vendor_lower for x in ["bell", "videotron", "rogers", "telus", "telecom"]):
            result["gl_account"] = "5400"
        elif any(x in vendor_lower for x in ["hydro", "gaz", "electricit"]):
            result["gl_account"] = "5410"
        else:
            result["gl_account"] = "5440"

    return result


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def process_file(
    file_bytes: bytes,
    filename:   str,
    *,
    client_code:  str        = "",
    document_id:  str | None = None,
    submitted_by: str        = "",
    client_note:  str        = "",
    ingest_source: str       = "api",
    db_path:      Path       = DB_PATH,
    upload_dir:   Path       = UPLOAD_DIR,
) -> dict[str, Any]:
    """
    Run a document through the full OCR + extraction pipeline.

    Steps
    -----
    1. Detect format from magic bytes.
    2. Save file to *upload_dir/<client_code>/<doc_id>_<filename>*.
    3. Extract: PDF → pdfplumber text (≥ 20 words) or Vision fallback;
       image → Claude Vision.
    4. Upsert into the documents table (insert or update existing row).
    5. Auto-flag confidence < 0.7 as NeedsReview.

    Returns
    -------
    dict with keys: ok, document_id, file_name, file_path, format,
    extraction_method, vendor, doc_type, amount, document_date,
    confidence, review_status, currency, low_confidence_flagged, error.
    """
    upload_dir.mkdir(parents=True, exist_ok=True)

    doc_id = document_id or _new_doc_id()
    now    = _utc_now_iso()

    # 1. Format detection
    fmt = detect_format(file_bytes)
    if fmt not in _SUPPORTED_FORMATS:
        return {
            "ok":          False,
            "document_id": doc_id,
            "error":       f"unsupported_format:{fmt}",
            "format":      fmt,
        }

    # 2. Save file
    safe_name  = "".join(c for c in filename if c.isalnum() or c in "._- ").strip() or "document"
    client_dir = upload_dir / (client_code or "unknown")
    client_dir.mkdir(parents=True, exist_ok=True)
    file_path  = client_dir / f"{doc_id}_{safe_name}"
    file_path.write_bytes(file_bytes)

    # 3. Extract (with handwriting detection)
    raw:              dict[str, Any] = {}
    extraction_method = "unknown"
    error:            str | None     = None
    raw_ocr_text:     str | None     = None
    handwriting_score = 0.0

    try:
        if fmt == "pdf":
            text       = extract_pdf_text(file_bytes)
            word_count = len(text.split()) if text else 0

            if word_count >= PDF_TEXT_MIN_WORDS:
                raw_ocr_text      = text
                raw               = _extract_from_text(text)
                extraction_method = "pdfplumber_text"
            else:
                images = _pdf_to_images(file_bytes)
                if images:
                    # Check for handwriting before choosing pipeline
                    handwriting_score = detect_handwriting(file_bytes)
                    if handwriting_score > HANDWRITING_HIGH_THRESHOLD:
                        raw               = call_vision_handwriting(*images[0])
                        extraction_method = "vision_handwriting"
                    elif handwriting_score < HANDWRITING_LOW_THRESHOLD:
                        raw               = call_vision(*images[0])
                        extraction_method = "vision_pdf_fallback"
                    else:
                        # Ambiguous: try both pipelines, use higher confidence
                        raw_std = call_vision(*images[0])
                        raw_hw  = call_vision_handwriting(*images[0])
                        std_conf = float(raw_std.get("confidence") or 0.0)
                        hw_conf  = float(raw_hw.get("confidence") or 0.0)
                        if hw_conf >= std_conf:
                            raw               = raw_hw
                            extraction_method = "vision_handwriting"
                        else:
                            raw               = raw_std
                            extraction_method = "vision_pdf_fallback"
                elif text:
                    raw_ocr_text      = text
                    raw               = _extract_from_text(text)
                    extraction_method = "pdfminer_sparse_text"
                else:
                    raw               = {"confidence": 0.1, "doc_type": "unknown",
                                        "notes": "Empty or unreadable PDF."}
                    extraction_method = "empty_pdf"
        else:
            norm_bytes, mime_type = _normalise_image(file_bytes, fmt)
            # Check for handwriting on image formats
            handwriting_score = detect_handwriting(file_bytes)
            if handwriting_score > HANDWRITING_HIGH_THRESHOLD:
                raw               = call_vision_handwriting(norm_bytes, mime_type)
                extraction_method = f"vision_handwriting_{fmt}"
            elif handwriting_score < HANDWRITING_LOW_THRESHOLD:
                raw               = call_vision(norm_bytes, mime_type)
                extraction_method = f"vision_{fmt}"
            else:
                # Ambiguous: try both pipelines, use higher confidence
                raw_std = call_vision(norm_bytes, mime_type)
                raw_hw  = call_vision_handwriting(norm_bytes, mime_type)
                std_conf = float(raw_std.get("confidence") or 0.0)
                hw_conf  = float(raw_hw.get("confidence") or 0.0)
                if hw_conf >= std_conf:
                    raw               = raw_hw
                    extraction_method = f"vision_handwriting_{fmt}"
                else:
                    raw               = raw_std
                    extraction_method = f"vision_{fmt}"

    except Exception as exc:
        error             = str(exc)
        raw               = {
            "confidence": 0.0,
            "doc_type":   "unknown",
            "notes":      f"Extraction failed: {exc}",
        }
        extraction_method = "failed"

    # 3b. Hallucination guard — validate AI output fields
    hallucination_suspected = False
    try:
        from src.agents.core.hallucination_guard import verify_ai_output
        guard = verify_ai_output(raw, client_code)
        hallucination_suspected = guard["hallucination_suspected"]
    except Exception:
        pass

    # 4. Normalise extracted fields
    confidence = float(raw.get("confidence") or 0.0)
    confidence = max(0.0, min(1.0, confidence))

    vendor         = raw.get("vendor_name") or None
    doc_type       = raw.get("doc_type") or "unknown"
    amount_raw     = raw.get("total") or raw.get("subtotal")
    amount         = float(amount_raw) if amount_raw is not None else None
    document_date  = raw.get("document_date") or None
    currency       = raw.get("currency") or "CAD"
    subtotal_raw   = raw.get("subtotal")
    tax_total_raw  = raw.get("tax_total")
    subtotal       = float(subtotal_raw)  if subtotal_raw  is not None else None
    tax_total      = float(tax_total_raw) if tax_total_raw is not None else None

    # FIX 1: Extract GST/QST from taxes array or line items
    _gst_amount = raw.get("gst_amount")
    _qst_amount = raw.get("qst_amount")
    taxes_list = raw.get("taxes") or []
    if isinstance(taxes_list, list) and (_gst_amount is None or _qst_amount is None):
        for tax_entry in taxes_list:
            if not isinstance(tax_entry, dict):
                continue
            tax_type = (tax_entry.get("type") or "").upper()
            tax_amt = tax_entry.get("amount")
            if tax_amt is None:
                continue
            try:
                tax_amt = float(tax_amt)
            except (TypeError, ValueError):
                continue
            if tax_type in ("GST", "TPS") and _gst_amount is None:
                _gst_amount = tax_amt
            elif tax_type in ("QST", "TVQ") and _qst_amount is None:
                _qst_amount = tax_amt
    # Also scan raw_ocr_text for GST/QST lines, BN#, NEQ if still missing
    _parsed_fields: dict[str, Any] = {}
    if raw_ocr_text:
        _parsed_fields = parse_invoice_fields(raw_ocr_text)
        if _gst_amount is None and _parsed_fields.get("gst_amount") is not None:
            _gst_amount = _parsed_fields["gst_amount"]
        if _qst_amount is None and _parsed_fields.get("qst_amount") is not None:
            _qst_amount = _parsed_fields["qst_amount"]
    # Store GST/QST in raw result for downstream use
    if _gst_amount is not None:
        raw["gst_amount"] = _gst_amount
    if _qst_amount is not None:
        raw["qst_amount"] = _qst_amount
    # Propagate BN/NEQ/GST number from parse_invoice_fields if not already set
    for _pf_key in ("gst_number", "bn_root", "bn_full", "neq"):
        if not raw.get(_pf_key) and _parsed_fields.get(_pf_key):
            raw[_pf_key] = _parsed_fields[_pf_key]
    # FIX 3: Detect foreign-currency invoices and convert to CAD
    _text_for_currency = raw_ocr_text or json.dumps(raw)
    for _fx_key in ("currency", "currency_converted", "currency_note", "foreign_amount",
                     "fx_rate", "cad_amount", "tax_code", "tax_note"):
        if _parsed_fields.get(_fx_key) is not None:
            raw[_fx_key] = _parsed_fields[_fx_key]
    if _parsed_fields.get("currency_converted"):
        currency = _parsed_fields.get("currency", currency)
        if _parsed_fields.get("cad_amount") is not None:
            amount = _parsed_fields["cad_amount"]
    elif re.search(r"\bUSD\b", _text_for_currency, re.IGNORECASE):
        currency = "USD"
        fx_rate = get_fx_rate("USD", document_date, _text_for_currency)
        raw["currency"] = "USD"
        raw["fx_rate"] = float(fx_rate)
        if amount is not None:
            raw["foreign_amount"] = amount
            cad_amount = round(float(Decimal(str(amount)) * fx_rate), 2)
            raw["cad_amount"] = cad_amount
            raw["currency_converted"] = True
            raw["currency_note"] = f"USD {amount:.2f} converted at {fx_rate} = CAD {cad_amount:.2f}"
            amount = cad_amount

    # 5. Auto-flag low confidence, hallucination, or handwriting low confidence
    handwriting_low_conf = bool(raw.get("handwriting_low_confidence", False))
    review_status = raw.get("review_status") if raw.get("review_status") == "NeedsReview" else None
    if review_status is None:
        review_status = (
            "NeedsReview"
            if confidence < LOW_CONFIDENCE_THRESHOLD or hallucination_suspected or handwriting_low_conf
            else "New"
        )

    # 5b. Enrich: GL account, tax code, category
    _enrich_data: dict[str, Any] = {
        "vendor": vendor, "vendor_name": vendor, "amount": amount,
        "description": raw.get("description", ""),
        "gst_number": raw.get("gst_number", ""),
    }
    try:
        _enrich_conn = sqlite3.connect(str(db_path))
        _enrich_conn.row_factory = sqlite3.Row
        try:
            _enrich_data = enrich_extracted_fields(_enrich_data, client_code or "", _enrich_conn)
        finally:
            _enrich_conn.close()
    except Exception:
        pass
    gl_account = _enrich_data.get("gl_account") or None
    tax_code   = _enrich_data.get("tax_code") or None
    category   = _enrich_data.get("category") or None

    # 5c. Document type detection — route to correct processing module
    _detect_text = raw_ocr_text or json.dumps(raw)
    detected_doc_type = detect_document_type(_detect_text, filename)
    if detected_doc_type == "bank_statement":
        category = "bank_statement"
        gl_account = "1010"
        doc_type = "bank_statement"
        review_status = "NeedsReview"
        raw["review_note"] = (
            "Relevé bancaire — traiter via Rapprochement bancaire / "
            "Bank statement — process via Bank Reconciliation"
        )
        raw["document_type_detected"] = "bank_statement"
        raw["routing_target"] = "reconciliation"
    elif detected_doc_type == "credit_card_statement":
        category = "credit_card_statement"
        gl_account = "2150"
        doc_type = "credit_card_statement"
        review_status = "NeedsReview"
        raw["review_note"] = (
            "Relevé de carte de crédit — traiter via Rapprochement / "
            "Credit card statement — process via Reconciliation"
        )
        raw["document_type_detected"] = "credit_card_statement"
        raw["routing_target"] = "reconciliation"
    elif detected_doc_type == "pay_stub":
        category = "payroll"
        gl_account = "5400"
        doc_type = "pay_stub"
        review_status = "NeedsReview"
        raw["review_note"] = (
            "Talon de paie — traiter via module Paie / "
            "Pay stub — process via Payroll module"
        )
        raw["document_type_detected"] = "pay_stub"
        raw["routing_target"] = "payroll"
    elif detected_doc_type == "credit_memo":
        category = "credit_memo"
        doc_type = "credit_memo"
        raw["document_type_detected"] = "credit_memo"
        raw["routing_target"] = "ap_ar"
        raw["is_credit"] = True
    elif detected_doc_type == "purchase_order":
        category = "purchase_order"
        doc_type = "purchase_order"
        review_status = "NeedsReview"
        raw["review_note"] = (
            "Bon de commande — traiter via Rapprochement BC / "
            "Purchase order — process via PO Matching"
        )
        raw["document_type_detected"] = "purchase_order"
        raw["routing_target"] = "po_matching"
    elif detected_doc_type == "receipt" and doc_type not in ("invoice",):
        # Only override to receipt if AI didn't already classify as invoice
        # (receipt heuristic is weak — short text + "total" keyword)
        category = "expense"
        doc_type = "receipt"
        raw["document_type_detected"] = "receipt"
        raw["routing_target"] = "expense"
    elif detected_doc_type != "invoice":
        raw["document_type_detected"] = detected_doc_type
        raw["routing_target"] = "expense"

    # 6. DB upsert
    record: dict[str, Any] = {
        "document_id":            doc_id,
        "file_name":              safe_name,
        "file_path":              str(file_path),
        "client_code":            client_code or None,
        "vendor":                 vendor,
        "doc_type":               doc_type,
        "amount":                 amount,
        "document_date":          document_date,
        "gl_account":             gl_account,
        "tax_code":               tax_code,
        "category":               category,
        "review_status":          review_status,
        "confidence":             confidence,
        "raw_result":             json.dumps(raw, ensure_ascii=False),
        "created_at":             now,
        "updated_at":             now,
        "submitted_by":           submitted_by or None,
        "client_note":            client_note or None,
        "currency":               currency,
        "subtotal":               subtotal,
        "tax_total":              tax_total,
        "extraction_method":      extraction_method,
        "ingest_source":          ingest_source,
        "raw_ocr_text":           raw_ocr_text,
        "hallucination_suspected": 1 if hallucination_suspected else 0,
        "handwriting_low_confidence": 1 if handwriting_low_conf else 0,
    }
    upsert_document(record, db_path=db_path)

    # Line-item extraction for multi-line invoices.
    # Failures are non-fatal: the document is already saved.
    try:
        from src.engines.line_item_engine import looks_like_multiline_invoice, process_line_items
        _ocr_text = raw_ocr_text or ""
        if _ocr_text and looks_like_multiline_invoice(_ocr_text):
            process_line_items(doc_id, _ocr_text, db_path=db_path)
    except Exception:  # pragma: no cover
        pass

    # Run fraud detection (Layer 1 — deterministic, no AI).
    # Failures are non-fatal: the document is already saved.
    fraud_flags_result: list[dict] = []
    try:
        from src.engines.fraud_engine import run_fraud_detection
        fraud_flags_result = run_fraud_detection(doc_id, db_path=db_path) or []
    except Exception:  # pragma: no cover
        pass

    # FIX 3: Run substance classifier after extraction
    substance_flags_result: dict = {}
    try:
        from src.engines.substance_engine import run_substance_classifier
        substance_flags_result = run_substance_classifier(record)
        _db = sqlite3.connect(str(db_path))
        _db.row_factory = sqlite3.Row
        try:
            cols = {r["name"] for r in _db.execute("PRAGMA table_info(documents)").fetchall()}
            if "substance_flags" in cols:
                _db.execute(
                    "UPDATE documents SET substance_flags = ? WHERE document_id = ?",
                    (json.dumps(substance_flags_result, ensure_ascii=False), doc_id),
                )
                _db.commit()
        finally:
            _db.close()
    except Exception:  # pragma: no cover
        pass

    # BLOCK 1+2: Re-run review_policy with fraud_flags and substance_flags
    # Only re-evaluate when fraud or substance flags are actually present,
    # to avoid downgrading review_status due to missing fields alone.
    _has_actionable_flags = bool(fraud_flags_result) or any(
        substance_flags_result.get(k) for k in (
            "potential_capex", "potential_prepaid", "potential_loan",
            "potential_tax_remittance", "potential_personal_expense",
            "potential_customer_deposit", "potential_intercompany",
            "mixed_tax_invoice", "block_auto_approval",
        )
    ) if substance_flags_result else False
    if _has_actionable_flags:
        try:
            from src.agents.tools.review_policy import decide_review_status
            decision = decide_review_status(
                rules_confidence=confidence,
                final_method=extraction_method,
                vendor_name=vendor,
                total=amount,
                document_date=document_date,
                client_code=client_code,
                fraud_flags=fraud_flags_result,
                substance_flags=substance_flags_result,
            )
            new_status = decision.status
            if new_status and new_status != review_status:
                review_status = new_status
                _db2 = sqlite3.connect(str(db_path))
                try:
                    _db2.execute(
                        "UPDATE documents SET review_status = ? WHERE document_id = ?",
                        (review_status, doc_id),
                    )
                    _db2.commit()
                finally:
                    _db2.close()
        except Exception:  # pragma: no cover
            pass

    return {
        "ok":                      True,
        "document_id":             doc_id,
        "file_name":               safe_name,
        "file_path":               str(file_path),
        "format":                  fmt,
        "extraction_method":       extraction_method,
        "vendor":                  vendor,
        "doc_type":                doc_type,
        "amount":                  amount,
        "document_date":           document_date,
        "gl_account":              gl_account,
        "tax_code":                tax_code,
        "category":                category,
        "confidence":              confidence,
        "review_status":           review_status,
        "currency":                currency,
        "low_confidence_flagged":      confidence < LOW_CONFIDENCE_THRESHOLD,
        "hallucination_suspected":     hallucination_suspected,
        "handwriting_low_confidence":  handwriting_low_conf,
        "handwriting_score":           handwriting_score,
        "error":                       error,
    }


# ---------------------------------------------------------------------------
# AI Usage Log — cost tracking per document
# ---------------------------------------------------------------------------

_AI_USAGE_LOG_CREATE = """
CREATE TABLE IF NOT EXISTS ai_usage_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id    TEXT,
    client_code    TEXT,
    source         TEXT    NOT NULL,
    model_used     TEXT,
    cost_usd       REAL    NOT NULL DEFAULT 0.0,
    tokens_used    INTEGER NOT NULL DEFAULT 0,
    confidence     REAL,
    created_at     TEXT    NOT NULL
)
"""

# Approximate per-1K-token costs (input+output blended) for cost estimation
_MODEL_COST_PER_1K: dict[str, float] = {
    "deepseek/deepseek-chat":        0.0002,
    "google/gemini-2.0-flash-001":       0.0004,
    "anthropic/claude-haiku-4-5":    0.001,
    "anthropic/claude-sonnet-4-6":   0.003,
}


def _ensure_usage_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(_AI_USAGE_LOG_CREATE)
    conn.commit()


def _estimate_cost(model: str, tokens: int) -> float:
    """Estimate USD cost for a given model and token count."""
    per_1k = _MODEL_COST_PER_1K.get(model, 0.001)
    return round(per_1k * tokens / 1000, 6)


def log_ai_usage(
    *,
    document_id: str | None = None,
    client_code: str = "",
    source: str,
    model_used: str = "",
    cost_usd: float = 0.0,
    tokens_used: int = 0,
    confidence: float | None = None,
    db_path: Path = DB_PATH,
) -> None:
    """Log a document processing event with cost tracking. Fire-and-forget."""
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            _ensure_usage_log_table(conn)
            conn.execute(
                """INSERT INTO ai_usage_log
                   (document_id, client_code, source, model_used,
                    cost_usd, tokens_used, confidence, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    document_id or "",
                    client_code or "",
                    source,
                    model_used or "",
                    cost_usd,
                    tokens_used,
                    confidence,
                    _utc_now_iso(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def get_ai_cost_summary(
    *,
    period: str = "month",
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """
    Return AI cost breakdown for the current period.

    Returns dict with keys:
        total_documents, cache_count, text_extraction_count,
        ai_simple_count, ai_medium_count, ai_complex_count,
        total_cost_usd, estimated_without_optimization,
        savings_usd, savings_pct, breakdown (list of dicts)
    """
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            _ensure_usage_log_table(conn)

            if period == "month":
                date_filter = datetime.now(timezone.utc).strftime("%Y-%m")
                where = "WHERE created_at LIKE ? || '%'"
                params: tuple = (date_filter,)
            else:
                where = ""
                params = ()

            rows = conn.execute(
                f"""SELECT source, COUNT(*) as cnt,
                           SUM(cost_usd) as total_cost,
                           SUM(tokens_used) as total_tokens
                    FROM ai_usage_log {where}
                    GROUP BY source
                    ORDER BY cnt DESC""",
                params,
            ).fetchall()

            total_docs = 0
            total_cost = 0.0
            breakdown: dict[str, dict[str, Any]] = {}

            for row in rows:
                src, cnt, cost, tokens = row
                cnt = int(cnt or 0)
                cost = float(cost or 0.0)
                tokens = int(tokens or 0)
                total_docs += cnt
                total_cost += cost
                breakdown[src] = {
                    "count": cnt,
                    "cost_usd": round(cost, 4),
                    "tokens": tokens,
                }

            cache_count = breakdown.get("cache", {}).get("count", 0)
            text_count = breakdown.get("text_extraction", {}).get("count", 0)
            ai_simple = breakdown.get("ai_simple", {}).get("count", 0)
            ai_medium = breakdown.get("ai_medium", {}).get("count", 0)
            ai_complex = breakdown.get("ai_complex", {}).get("count", 0)

            # Estimate what it would cost without optimization
            # (assume all docs would use medium model at ~$0.002/doc)
            estimated_full = total_docs * 0.002
            savings = max(0.0, estimated_full - total_cost)
            savings_pct = round((savings / estimated_full * 100), 1) if estimated_full > 0 else 0.0

            return {
                "total_documents": total_docs,
                "cache_count": cache_count,
                "text_extraction_count": text_count,
                "ai_simple_count": ai_simple,
                "ai_medium_count": ai_medium,
                "ai_complex_count": ai_complex,
                "total_cost_usd": round(total_cost, 4),
                "estimated_without_optimization": round(estimated_full, 4),
                "savings_usd": round(savings, 4),
                "savings_pct": savings_pct,
                "breakdown": breakdown,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Cost-optimized AI pipeline
# ---------------------------------------------------------------------------

def check_vendor_cache(
    client_code: str,
    file_path: str,
    conn: sqlite3.Connection,
) -> dict[str, Any] | None:
    """
    Check learning_memory_patterns for a known vendor match.
    Returns dict with vendor/amount/gl_account/confidence or None.
    """
    try:
        # Extract filename hints for vendor matching (normalize separators)
        fname = Path(file_path).stem.lower().replace("_", " ").replace("-", " ")
        # Try to find a vendor pattern that matches the filename
        rows = conn.execute(
            """SELECT vendor_key, gl_account, tax_code, category,
                      avg_confidence, outcome_count, success_count
               FROM learning_memory_patterns
               WHERE client_code_key = ? OR client_code_key = ''
               ORDER BY outcome_count DESC
               LIMIT 50""",
            (client_code.strip().casefold(),),
        ).fetchall()

        for row in rows:
            vendor_key = str(row[0] or "")
            if vendor_key and vendor_key in fname:
                outcome_count = int(row[5] or 0)
                success_count = int(row[6] or 0)
                if outcome_count >= 5:
                    success_rate = success_count / outcome_count
                    confidence = float(row[4] or 0.0) * success_rate
                    return {
                        "vendor": vendor_key,
                        "gl_account": row[1] or "",
                        "tax_code": row[2] or "",
                        "category": row[3] or "",
                        "confidence": round(confidence, 4),
                    }
    except Exception:
        pass
    return None


def extract_with_pdfplumber(file_path: str) -> dict[str, Any]:
    """
    Try to extract text from a PDF using pdfplumber.
    Returns dict with 'text' and 'confidence'.
    """
    try:
        import pdfplumber  # type: ignore[import]
        with pdfplumber.open(file_path) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        text = "\n".join(pages).strip()
        word_count = len(text.split()) if text else 0
        # Confidence based on text quality
        if word_count >= 50:
            confidence = 0.95
        elif word_count >= PDF_TEXT_MIN_WORDS:
            confidence = 0.85
        elif word_count >= 10:
            confidence = 0.60
        else:
            confidence = 0.20
        return {"text": text, "confidence": confidence, "word_count": word_count}
    except Exception:
        return {"text": "", "confidence": 0.0, "word_count": 0}


def parse_invoice_fields(text: str) -> dict[str, Any]:
    """
    Parse structured invoice fields from extracted text without AI.
    Returns dict with vendor, amount, gl_account, confidence, etc.
    """
    result: dict[str, Any] = {
        "vendor": None,
        "amount": None,
        "document_date": None,
        "gl_account": None,
        "doc_type": "unknown",
        "confidence": 0.0,
    }

    if not text or not text.strip():
        return result

    lines = text.strip().splitlines()
    confidence = 0.5

    # Extract vendor (usually first non-empty line)
    for line in lines[:5]:
        line = line.strip()
        if line and len(line) > 2 and not line.startswith("$"):
            result["vendor"] = line
            result["vendor_name"] = line
            confidence += 0.1
            break

    # Extract amounts — look for total, grand total, amount due patterns
    amount_patterns = [
        re.compile(r"(?:total|montant|amount\s*due|grand\s*total|solde)\s*[:\s]*(?:\$|USD|EUR|GBP|CHF|CAD)?\s*([\d,]+\.?\d*)", re.IGNORECASE),
        re.compile(r"(?:\$|USD|EUR|GBP|CHF|CAD)\s*([\d,]+\.\d{2})\b", re.IGNORECASE),
    ]
    amounts_found: list[float] = []
    for line in lines:
        for pat in amount_patterns:
            m = pat.search(line)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    amounts_found.append(val)
                except ValueError:
                    pass
    if amounts_found:
        result["amount"] = max(amounts_found)  # Largest amount is likely total
        confidence += 0.15

    # Extract date
    date_patterns = [
        re.compile(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})"),
        re.compile(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})"),
    ]
    for line in lines:
        for pat in date_patterns:
            m = pat.search(line)
            if m:
                fixed = _fix_quebec_date(m.group(1))
                if fixed:
                    result["document_date"] = fixed
                    confidence += 0.1
                    break
        if result["document_date"]:
            break

    # Detect doc type
    text_lower = text.lower()
    if "facture" in text_lower or "invoice" in text_lower:
        result["doc_type"] = "invoice"
        confidence += 0.05
    elif "reçu" in text_lower or "receipt" in text_lower or "recu" in text_lower:
        result["doc_type"] = "receipt"
        confidence += 0.05
    elif "relevé" in text_lower or "statement" in text_lower or "releve" in text_lower:
        result["doc_type"] = "bank_statement"
        confidence += 0.05

    # ---- BN# / GST number extraction (comprehensive) ----
    # Pattern 1: Keyword-prefixed GST number (GST/TPS/HST/BN#/Registration)
    gst_num_match = re.search(
        r"(?:GST|TPS|HST|GST/HST|GST/TPS|BN#?|Registration)\s*[#:.]?\s*(\d{9})\s*(?:RT\s*(\d{4}))?",
        text, re.IGNORECASE
    )
    if gst_num_match:
        rt_suffix = gst_num_match.group(2) or "0001"
        result["gst_number"] = f"{gst_num_match.group(1)}RT{rt_suffix}"
        result["bn_root"] = gst_num_match.group(1)
        confidence += 0.1

    # Pattern 2: Standalone 9-digit + RT pattern anywhere in full text
    if not result.get("gst_number"):
        standalone_gst = re.search(r"(\d{9})\s*RT\s*(\d{4})", text)
        if standalone_gst:
            result["gst_number"] = f"{standalone_gst.group(1)}RT{standalone_gst.group(2)}"
            result["bn_root"] = standalone_gst.group(1)
            confidence += 0.1

    # ---- QST number extraction (comprehensive) ----
    # Pattern 1: Keyword-prefixed QST number (QST/TVQ/NEQ)
    qst_num_match = re.search(
        r"(?:QST|TVQ|QST/TVQ|NEQ)\s*[#:.]?\s*(\d{10})",
        text, re.IGNORECASE
    )
    if qst_num_match:
        result["qst_number"] = qst_num_match.group(1)
        confidence += 0.05

    # Pattern 2: 10-digit number near tax keywords anywhere in full text
    if not result.get("qst_number"):
        for line in lines:
            if re.search(r"\b(?:QST|TVQ|tax|taxe|québec|quebec)\b", line, re.IGNORECASE):
                ten_digit = re.search(r"\b(\d{10})\b", line)
                if ten_digit:
                    result["qst_number"] = ten_digit.group(1)
                    confidence += 0.05
                    break

    # Match BN# with any suffix (e.g. BC0001, RT0001, RP0001)
    if not result.get("gst_number"):
        bn_match = re.search(r"BN\s*#?\s*:?\s*(\d{9})\s*([A-Z]{2})\s*(\d{4})", text, re.IGNORECASE)
        if bn_match:
            bn_root = bn_match.group(1)
            suffix_letters = bn_match.group(2).upper()
            suffix_digits = bn_match.group(3)
            result["bn_root"] = bn_root
            result["bn_full"] = f"{bn_root}{suffix_letters}{suffix_digits}"
            # Derive likely GST number from the 9-digit root
            result["gst_number"] = f"{bn_root}RT0001"
            confidence += 0.1

    # ---- NEQ (Numéro d'entreprise du Québec) extraction ----
    neq_match = re.search(r"NEQ\s*:?\s*(\d{10})", text, re.IGNORECASE)
    if neq_match:
        result["neq"] = neq_match.group(1)
        # NEQ is also a valid QST number
        if not result.get("qst_number"):
            result["qst_number"] = neq_match.group(1)
        confidence += 0.05

    # ---- FIX 5: If GST or QST number found, mark as registered / taxable ----
    if result.get("gst_number") or result.get("qst_number"):
        result["is_registered"] = True
        result["tax_code"] = "T"

    # ---- FIX 1: Detect GST/QST tax lines and set document-level fields ----
    gst_pattern = re.compile(r"\b(GST|TPS)\b", re.IGNORECASE)
    qst_pattern = re.compile(r"\b(QST|TVQ)\b", re.IGNORECASE)
    line_amount_pattern = re.compile(r"\$?\s*([\d,]+\.\d{2})")
    gst_amount = None
    qst_amount = None
    subtotal = None

    for line in lines:
        if gst_pattern.search(line):
            # Use last amount on line (skip percentage values like "5.00%")
            all_amounts = line_amount_pattern.findall(line)
            if all_amounts:
                try:
                    gst_amount = float(all_amounts[-1].replace(",", ""))
                except ValueError:
                    pass
        elif qst_pattern.search(line):
            all_amounts = line_amount_pattern.findall(line)
            if all_amounts:
                try:
                    qst_amount = float(all_amounts[-1].replace(",", ""))
                except ValueError:
                    pass

    if gst_amount is not None:
        result["gst_amount"] = gst_amount
        confidence += 0.05
    if qst_amount is not None:
        result["qst_amount"] = qst_amount
        confidence += 0.05

    # Compute subtotal (pre-tax) if we have total and taxes
    total = result.get("amount")
    if total and gst_amount is not None and qst_amount is not None:
        subtotal = round(total - gst_amount - qst_amount, 2)
        if subtotal > 0:
            result["subtotal"] = subtotal
            result["pre_tax_amount"] = subtotal

    # ---- FIX 3: Detect foreign currency and convert to CAD ----
    _currency_patterns = {
        "USD": re.compile(r"\bUSD\b", re.IGNORECASE),
        "EUR": re.compile(r"\bEUR\b", re.IGNORECASE),
        "GBP": re.compile(r"\bGBP\b", re.IGNORECASE),
        "CHF": re.compile(r"\bCHF\b", re.IGNORECASE),
    }
    detected_foreign: str | None = None
    for curr_code, curr_pat in _currency_patterns.items():
        if curr_pat.search(text):
            detected_foreign = curr_code
            break

    if detected_foreign:
        result["currency"] = detected_foreign
        usd_amount = result.get("amount")

        # Get FX rate (invoice-stated > BoC API > config fallback > hardcoded)
        invoice_date = result.get("document_date")
        fx_rate = get_fx_rate(detected_foreign, invoice_date, text)
        result["fx_rate"] = float(fx_rate)

        if usd_amount is not None:
            cad_amount = float(Decimal(str(usd_amount)) * fx_rate)
            cad_amount = round(cad_amount, 2)
            result["foreign_amount"] = usd_amount
            result["cad_amount"] = cad_amount
            result["amount"] = cad_amount  # Always store in CAD
            result["currency_converted"] = True
            result["currency_note"] = (
                f"{detected_foreign} {usd_amount:.2f} converted at {fx_rate} "
                f"= CAD {cad_amount:.2f}"
            )
        else:
            result["currency_converted"] = False

        # Tax treatment for foreign vendors
        # If the invoice already lists explicit GST/QST amounts, respect them
        _has_explicit_tax = (
            result.get("gst_amount") is not None and result["gst_amount"] > 0
        )
        has_gst_number = bool(result.get("gst_number"))
        vendor_name = result.get("vendor") or ""
        is_digital_registered = _is_foreign_registered_digital(vendor_name)

        if _has_explicit_tax:
            # Invoice explicitly lists GST/QST — vendor is collecting tax
            result["tax_code"] = "T"
        elif has_gst_number or is_digital_registered:
            # Vendor registered in Canada — taxable
            result["tax_code"] = "T"
            if usd_amount is not None:
                cad_for_tax = result.get("cad_amount", 0)
                result["gst_amount"] = round(cad_for_tax * 0.05, 2)
                result["qst_amount"] = round(cad_for_tax * 0.09975, 2)
            if is_digital_registered and not has_gst_number:
                result["tax_note"] = (
                    "Fournisseur numérique étranger inscrit à la TPS (>30k$/an au Canada) "
                    "/ Foreign digital service provider registered for GST"
                )
        else:
            # Foreign vendor not registered — exempt
            result["tax_code"] = "E"
            result["gst_amount"] = 0
            result["qst_amount"] = 0
            result["tax_note"] = (
                "Fournisseur étranger non inscrit \u2014 TPS/TVQ non applicable "
                "/ Foreign vendor not registered \u2014 GST/QST not applicable"
            )
    else:
        result["currency"] = "CAD"

    result["confidence"] = min(1.0, round(confidence, 4))
    return result


def assess_image_quality(file_path: str) -> float:
    """
    Assess image quality on a 0.0-1.0 scale.
    Higher = clearer image, easier to OCR.
    """
    try:
        from PIL import Image, ImageStat  # type: ignore[import]
        img = Image.open(file_path)
        grey = img.convert("L")
        stat = ImageStat.Stat(grey)

        # Factors: resolution, contrast (stddev), not too dark/bright
        width, height = img.size
        resolution_score = min(1.0, (width * height) / (1920 * 1080))

        stddev = stat.stddev[0]
        contrast_score = min(1.0, stddev / 60.0)

        mean_val = stat.mean[0]
        brightness_score = 1.0 - abs(mean_val - 128) / 128

        quality = (resolution_score * 0.3 + contrast_score * 0.4 + brightness_score * 0.3)
        return round(min(1.0, max(0.0, quality)), 4)
    except Exception:
        return 0.5  # default: unknown quality


def classify_complexity(file_path: str, text_result: dict[str, Any] | None = None) -> str:
    """
    Classify document complexity for model routing.

    Returns: 'simple', 'medium', or 'complex'
    """
    if text_result and text_result.get("confidence", 0) > 0.70:
        return "simple"
    if file_path.lower().endswith(".pdf"):
        return "simple"

    image_quality = assess_image_quality(file_path)
    if image_quality > 0.70:
        return "medium"
    return "complex"


def get_model_for_complexity(complexity: str) -> str:
    """
    Return the AI model for a given complexity level.
    Reads from otocpa.config.json ai_complexity_models, with defaults.
    """
    defaults = {
        "simple": "deepseek/deepseek-chat",
        "medium": "google/gemini-2.0-flash-001",
        "complex": "anthropic/claude-haiku-4-5",
        "very_complex": "anthropic/claude-sonnet-4-6",
    }
    cfg = _load_config()
    models = cfg.get("ai_complexity_models", {})
    if isinstance(models, dict) and complexity in models:
        return str(models[complexity])
    return defaults.get(complexity, "deepseek/deepseek-chat")


def _build_extraction_prompt(file_path: str, text_result: dict[str, Any] | None = None) -> str:
    """Build an extraction prompt for the AI model."""
    prompt = _VISION_PROMPT
    if text_result and text_result.get("text"):
        prompt += f"\n\nExtracted text from document:\n{text_result['text'][:3000]}"
    return prompt


def _call_openrouter_for_extraction(
    model: str,
    prompt: str,
    file_path: str,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """
    Call OpenRouter with the specified model for document extraction.
    Returns dict with extracted fields, tokens_used, and cost_usd.
    """
    cfg = _load_config()
    prov = cfg.get("ai_router", {}).get("premium_provider", {})
    base_url = prov.get("base_url", "").rstrip("/")
    api_key = prov.get("api_key", "")

    if not base_url or not api_key:
        raise RuntimeError("ai_provider_not_configured")

    url = f"{base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Build message content
    messages_content: list[dict[str, Any]] = [
        {"type": "text", "text": prompt},
    ]

    # Add image if it's an image file
    fmt = "unknown"
    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        fmt = detect_format(file_bytes)
    except Exception:
        file_bytes = b""

    if fmt in ("jpeg", "png", "webp", "tiff"):
        norm_bytes, mime_type = _normalise_image(file_bytes, fmt)
        b64 = base64.b64encode(norm_bytes).decode("ascii")
        messages_content.insert(0, {
            "type": "image_url",
            "image_url": {"url": f"data:{mime_type};base64,{b64}"},
        })
    elif fmt == "pdf":
        images = _pdf_to_images(file_bytes)
        if images:
            b64 = base64.b64encode(images[0][0]).decode("ascii")
            messages_content.insert(0, {
                "type": "image_url",
                "image_url": {"url": f"data:{images[0][1]};base64,{b64}"},
            })

    payload = {
        "model": model,
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": _VISION_SYSTEM},
            {"role": "user", "content": messages_content},
        ],
    }

    r = requests.post(url, headers=headers, json=payload, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"AI API HTTP {r.status_code}: {r.text[:400]}")

    data = r.json()
    content = data["choices"][0]["message"]["content"].strip()

    # Extract token usage
    usage = data.get("usage", {})
    tokens_used = int(usage.get("total_tokens", 0))
    if tokens_used == 0:
        tokens_used = int(usage.get("prompt_tokens", 0)) + int(usage.get("completion_tokens", 0))

    # Estimate cost
    cost_usd = _estimate_cost(model, tokens_used)

    # Parse JSON response
    if content.startswith("```"):
        lines = content.splitlines()
        inner = lines[1:] if len(lines) < 3 else lines[1:-1]
        content = "\n".join(inner)

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = {
            "confidence": 0.3,
            "doc_type": "unknown",
            "notes": f"AI returned non-JSON: {content[:200]}",
        }

    parsed["tokens_used"] = tokens_used
    parsed["cost_usd"] = cost_usd
    parsed["model_used"] = model
    return parsed


def save_vendor_cache(
    client_code: str,
    result: dict[str, Any],
    conn: sqlite3.Connection,
) -> None:
    """Save extraction result to learning_memory_patterns for future cache hits."""
    vendor = str(result.get("vendor") or result.get("vendor_name") or "").strip()
    if not vendor:
        return
    try:
        vendor_key = vendor.casefold()
        client_key = (client_code or "").strip().casefold()
        gl_account = result.get("gl_account", "")
        tax_code = result.get("tax_code", "")
        category = result.get("category", "")
        doc_type = result.get("doc_type", "")
        confidence = float(result.get("confidence", 0.0))

        existing = conn.execute(
            """SELECT outcome_count, success_count, avg_confidence
               FROM learning_memory_patterns
               WHERE vendor_key = ? AND client_code_key = ?""",
            (vendor_key, client_key),
        ).fetchone()

        if existing:
            new_count = int(existing[0] or 0) + 1
            new_success = int(existing[1] or 0) + 1
            old_avg = float(existing[2] or 0.0)
            new_avg = round((old_avg * (new_count - 1) + confidence) / new_count, 4)
            conn.execute(
                """UPDATE learning_memory_patterns
                   SET outcome_count = ?, success_count = ?, avg_confidence = ?,
                       gl_account = COALESCE(NULLIF(?, ''), gl_account),
                       tax_code = COALESCE(NULLIF(?, ''), tax_code),
                       category = COALESCE(NULLIF(?, ''), category),
                       doc_type = COALESCE(NULLIF(?, ''), doc_type)
                   WHERE vendor_key = ? AND client_code_key = ?""",
                (new_count, new_success, new_avg,
                 gl_account, tax_code, category, doc_type,
                 vendor_key, client_key),
            )
        else:
            conn.execute(
                """INSERT OR IGNORE INTO learning_memory_patterns
                   (vendor_key, client_code_key, gl_account, tax_code,
                    category, doc_type, avg_confidence,
                    outcome_count, success_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)""",
                (vendor_key, client_key, gl_account, tax_code,
                 category, doc_type, confidence),
            )
        conn.commit()
    except Exception:
        pass


def process_document_optimized(
    document_id: str,
    file_path: str,
    client_code: str,
    conn: sqlite3.Connection,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """
    Cost-optimized document processing pipeline.

    Tries FREE methods first (cache, text extraction) before
    falling back to AI with complexity-based model routing.

    Steps:
        1. Check vendor cache (FREE)
        2. Try pdfplumber text extraction (FREE)
        3. Classify document complexity
        4. Call appropriate AI model via OpenRouter
        5. Cache result for future use

    Returns dict with source, cost, model_used, and extracted fields.
    """
    # STEP 1 — Check vendor cache (FREE)
    try:
        known = check_vendor_cache(client_code, file_path, conn)
        if known and known.get("confidence", 0) > 0.85:
            result = {
                "source": "cache",
                "cost": 0.0,
                "tokens_used": 0,
                "model_used": "",
                "vendor": known["vendor"],
                "amount": known.get("amount"),
                "gl_account": known.get("gl_account", ""),
                "confidence": known["confidence"],
                "document_id": document_id,
            }
            log_ai_usage(
                document_id=document_id,
                client_code=client_code,
                source="cache",
                cost_usd=0.0,
                confidence=known["confidence"],
                db_path=db_path,
            )
            return result
    except Exception:
        pass

    # STEP 2 — Try pdfplumber text extraction (FREE)
    text_result: dict[str, Any] | None = None
    if file_path.lower().endswith(".pdf"):
        text_result = extract_with_pdfplumber(file_path)
        if text_result["confidence"] > 0.85:
            parsed = parse_invoice_fields(text_result["text"])
            if parsed["confidence"] > 0.80:
                result = {
                    "source": "text_extraction",
                    "cost": 0.0,
                    "tokens_used": 0,
                    "model_used": "",
                    "document_id": document_id,
                    **parsed,
                }
                log_ai_usage(
                    document_id=document_id,
                    client_code=client_code,
                    source="text_extraction",
                    cost_usd=0.0,
                    confidence=parsed["confidence"],
                    db_path=db_path,
                )
                # Cache for future
                if parsed["confidence"] > 0.80:
                    try:
                        save_vendor_cache(client_code, parsed, conn)
                    except Exception:
                        pass
                return result

    # STEP 3 — Classify document complexity
    complexity = classify_complexity(file_path, text_result)

    # STEP 4 — Call appropriate AI model
    model = get_model_for_complexity(complexity)
    prompt = _build_extraction_prompt(file_path, text_result)

    try:
        ai_result = _call_openrouter_for_extraction(model, prompt, file_path, db_path=db_path)
    except Exception as exc:
        # If the selected model fails, try upgrading complexity
        if complexity == "simple":
            model = get_model_for_complexity("medium")
        elif complexity == "medium":
            model = get_model_for_complexity("complex")
        else:
            model = get_model_for_complexity("very_complex")
        try:
            ai_result = _call_openrouter_for_extraction(model, prompt, file_path, db_path=db_path)
        except Exception as exc2:
            return {
                "source": f"ai_{complexity}",
                "cost": 0.0,
                "tokens_used": 0,
                "model_used": model,
                "confidence": 0.0,
                "document_id": document_id,
                "error": str(exc2),
            }

    source = f"ai_{complexity}"
    cost_usd = float(ai_result.get("cost_usd", 0.0))
    tokens_used = int(ai_result.get("tokens_used", 0))

    result = {
        "source": source,
        "cost": cost_usd,
        "tokens_used": tokens_used,
        "model_used": model,
        "document_id": document_id,
        "vendor": ai_result.get("vendor_name"),
        "amount": ai_result.get("total") or ai_result.get("subtotal"),
        "gl_account": ai_result.get("gl_account", ""),
        "doc_type": ai_result.get("doc_type", "unknown"),
        "document_date": ai_result.get("document_date"),
        "confidence": float(ai_result.get("confidence", 0.0)),
        "currency": ai_result.get("currency", "CAD"),
    }

    # STEP 5 — Log usage
    log_ai_usage(
        document_id=document_id,
        client_code=client_code,
        source=source,
        model_used=model,
        cost_usd=cost_usd,
        tokens_used=tokens_used,
        confidence=result["confidence"],
        db_path=db_path,
    )

    # Cache result for future
    if result["confidence"] > 0.80:
        try:
            save_vendor_cache(client_code, {**result, "vendor_name": result.get("vendor")}, conn)
        except Exception:
            pass

    return result


# ---------------------------------------------------------------------------
# Email attachment parser
# ---------------------------------------------------------------------------

_ATTACHMENT_MIMES: frozenset[str] = frozenset({
    "application/pdf",
    "image/jpeg", "image/jpg",
    "image/png",
    "image/tiff",
    "image/webp",
    "image/heic", "image/heif",
})


def parse_email_attachments(raw_email: bytes) -> list[tuple[bytes, str]]:
    """
    Parse a raw RFC 2822 / MIME email and return all document attachments.

    Returns a list of (file_bytes, filename) tuples for parts whose MIME
    type or magic bytes indicate a supported format.
    """
    # compat32 policy gives raw byte payloads via get_payload(decode=True)
    msg = email.message_from_bytes(raw_email, policy=email.policy.compat32)

    attachments: list[tuple[bytes, str]] = []

    for part in msg.walk():
        disposition = (part.get_content_disposition() or "").lower()
        if disposition not in ("attachment", "inline"):
            continue

        payload = part.get_payload(decode=True)
        if not payload:
            continue

        filename  = part.get_filename() or f"attachment_{len(attachments) + 1}"
        mime_type = part.get_content_type().lower()

        detected = detect_format(payload)
        if mime_type in _ATTACHMENT_MIMES or detected in _SUPPORTED_FORMATS:
            attachments.append((payload, filename))

    return attachments


# ---------------------------------------------------------------------------
# Email ingest HTTP server
# ---------------------------------------------------------------------------

class EmailIngestHandler(BaseHTTPRequestHandler):
    """
    Minimal HTTP handler that exposes:

        POST /ingest/email
            Headers:
                Content-Type:   <anything> (body is raw email bytes)
                X-API-Key:      <configured ingest.api_key>   (if set)
                X-Client-Code:  <client_code>                 (optional)
            Query params (alternative to header):
                client_code=...
            Body: raw RFC 2822 email (or MIME multipart)
            Response: JSON  { ok, processed, results: [...] }

        GET /health
            Response: JSON  { status: "ok" }
    """

    # Override in tests or subclasses
    db_path:    Path = DB_PATH
    upload_dir: Path = UPLOAD_DIR

    # ------------------------------------------------------------------ #

    def log_message(self, fmt: str, *args: Any) -> None:
        return  # suppress default stderr logging

    def _send_json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type",   "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path == "/health":
            self._send_json({"status": "ok"})
        else:
            self._send_json({"error": "not_found"}, status=404)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        qs     = urllib.parse.parse_qs(parsed.query)

        if path != "/ingest/email":
            self._send_json({"error": "not_found"}, status=404)
            return

        # API key guard
        api_key = _ingest_api_key()
        if api_key:
            provided = self.headers.get("X-API-Key", "")
            if not secrets.compare_digest(provided, api_key):
                self._send_json({"error": "unauthorized"}, status=401)
                return

        # Read body (max 50 MB)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0

        if length > 50 * 1024 * 1024:
            self._send_json({"error": "payload_too_large"}, status=413)
            return

        raw_email = self.rfile.read(length) if length else self.rfile.read()

        client_code = (
            self.headers.get("X-Client-Code", "")
            or qs.get("client_code", [""])[0]
        )

        # Parse attachments
        try:
            attachments = parse_email_attachments(raw_email)
        except Exception as exc:
            self._send_json(
                {"error": f"email_parse_failed: {exc}"}, status=400
            )
            return

        if not attachments:
            self._send_json({"ok": True, "processed": 0, "results": []})
            return

        # Process each attachment
        results: list[dict[str, Any]] = []
        for file_bytes, filename in attachments:
            try:
                result = process_file(
                    file_bytes,
                    filename,
                    client_code=client_code,
                    ingest_source="email",
                    db_path=self.db_path,
                    upload_dir=self.upload_dir,
                )
            except Exception as exc:
                result = {
                    "ok":       False,
                    "file_name": filename,
                    "error":    traceback.format_exc(),
                }
            results.append(result)

        self._send_json({"ok": True, "processed": len(results), "results": results})


# ---------------------------------------------------------------------------
# Standalone server runner
# ---------------------------------------------------------------------------

def run_email_ingest_server(host: str = "127.0.0.1", port: int = 8789) -> None:
    cfg  = _load_config()
    port = int(cfg.get("ingest", {}).get("port", port))

    server = ThreadingHTTPServer((host, port), EmailIngestHandler)
    print()
    print("OTOCPA OCR / EMAIL INGEST SERVER")
    print("=" * 60)
    print(f"Endpoint : http://{host}:{port}/ingest/email")
    print(f"Health   : http://{host}:{port}/health")
    print(f"DB       : {DB_PATH}")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser(description="OtoCPA OCR / Email Ingest Server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8789)
    args = parser.parse_args()
    run_email_ingest_server(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
