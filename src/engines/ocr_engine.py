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
                review_status, confidence, raw_result,
                created_at, updated_at, submitted_by, client_note,
                currency, subtotal, tax_total,
                extraction_method, ingest_source,
                raw_ocr_text, hallucination_suspected,
                handwriting_low_confidence
            ) VALUES (
                :document_id, :file_name, :file_path, :client_code,
                :vendor, :doc_type, :amount, :document_date,
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

    # 5. Auto-flag low confidence, hallucination, or handwriting low confidence
    handwriting_low_conf = bool(raw.get("handwriting_low_confidence", False))
    review_status = raw.get("review_status") if raw.get("review_status") == "NeedsReview" else None
    if review_status is None:
        review_status = (
            "NeedsReview"
            if confidence < LOW_CONFIDENCE_THRESHOLD or hallucination_suspected or handwriting_low_conf
            else "New"
        )

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
