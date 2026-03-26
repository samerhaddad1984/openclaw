from __future__ import annotations
from pathlib import Path

def extract_text_from_file(path: Path) -> str:
    """
    V1: text-only extraction.
    - If it's a .txt/.csv, read as text.
    - For PDF/images, you will add OCR later (Azure Vision or Tesseract).
    """
    ext = path.suffix.lower()

    if ext in [".txt", ".csv", ".json"]:
        return path.read_text(encoding="utf-8", errors="ignore")

    # For now, return a placeholder so you don't pretend you extracted anything
    return f"[UNSUPPORTED_FILE_TYPE:{ext}]"