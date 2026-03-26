from __future__ import annotations

from pathlib import Path

import pytesseract
from pdf2image import convert_from_path
from pdfminer.high_level import extract_text

# Hardcode Tesseract path for now (reliable on your machine)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Change this only if your Poppler is installed somewhere else
POPPLER_PATH = r"C:\poppler\Library\bin"


def extract_pdf_text(pdf_path: Path) -> str:
    # 1) Try normal text extraction first
    try:
        text = extract_text(str(pdf_path))
        if text and len(text.strip()) > 50:
            return text
    except Exception:
        pass

    # 2) OCR fallback for scanned PDFs
    try:
        images = convert_from_path(
            str(pdf_path),
            dpi=300,
            poppler_path=POPPLER_PATH,
        )

        full_text: list[str] = []

        for img in images:
            txt = pytesseract.image_to_string(img, lang="eng+fra")
            if txt and txt.strip():
                full_text.append(txt)

        return "\n".join(full_text).strip()

    except Exception as e:
        raise RuntimeError(f"OCR failed: {e}")