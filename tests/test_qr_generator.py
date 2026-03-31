"""
tests/test_qr_generator.py
===========================
Pytest tests for src/integrations/qr_generator.py

Tests cover:
  - URL building
  - PNG generation (structure & content)
  - PDF generation (structure & content)
  - Edge cases (empty client name, special chars in client code)
"""
from __future__ import annotations

import io
import sys
import urllib.parse
from pathlib import Path
from typing import Any

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.integrations.qr_generator import (
    _build_upload_url,
    generate_client_qr_png,
    generate_all_qr_pdf,
)


# ---------------------------------------------------------------------------
# _build_upload_url
# ---------------------------------------------------------------------------

class TestBuildUploadUrl:
    def test_basic(self):
        url = _build_upload_url("http://127.0.0.1:8788", "ACME")
        assert url == "http://127.0.0.1:8788/?client_code=ACME"

    def test_trailing_slash_stripped(self):
        url = _build_upload_url("http://127.0.0.1:8788/", "ACME")
        assert url == "http://127.0.0.1:8788/?client_code=ACME"

    def test_special_chars_encoded(self):
        url = _build_upload_url("http://example.com", "Le Café & Co")
        assert "Le+Caf" not in url  # should be percent-encoded
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        assert qs["client_code"] == ["Le Café & Co"]

    def test_empty_client_code(self):
        url = _build_upload_url("http://127.0.0.1:8788", "")
        assert "client_code=" in url


# ---------------------------------------------------------------------------
# generate_client_qr_png
# ---------------------------------------------------------------------------

class TestGenerateClientQrPng:
    def test_returns_bytes(self):
        result = generate_client_qr_png("ACME", "Acme Corp", "http://example.com/?client_code=ACME")
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_valid_png_header(self):
        result = generate_client_qr_png("ACME", "Acme Corp", "http://example.com/?client_code=ACME")
        assert result[:8] == b"\x89PNG\r\n\x1a\n"

    def test_png_parseable(self):
        from PIL import Image
        result = generate_client_qr_png("ACME", "Acme Corp", "http://example.com/?client_code=ACME")
        img = Image.open(io.BytesIO(result))
        assert img.format == "PNG"
        w, h = img.size
        assert w > 0
        assert h > w * 0.9  # height includes the label area — taller than a square

    def test_empty_client_name_falls_back_to_code(self):
        """Empty name should not raise — uses client_code as label."""
        result = generate_client_qr_png("ACME", "", "http://example.com/?client_code=ACME")
        assert result[:8] == b"\x89PNG\r\n\x1a\n"

    def test_long_client_name(self):
        long_name = "A" * 80
        result = generate_client_qr_png("X", long_name, "http://x.com/")
        assert result[:8] == b"\x89PNG\r\n\x1a\n"

    def test_unicode_client_name(self):
        result = generate_client_qr_png("FR01", "Société Générale Ltée", "http://example.com/")
        assert result[:8] == b"\x89PNG\r\n\x1a\n"

    def test_different_clients_produce_different_images(self):
        png_a = generate_client_qr_png("A", "Client A", "http://example.com/?client_code=A")
        png_b = generate_client_qr_png("B", "Client B", "http://example.com/?client_code=B")
        assert png_a != png_b


# ---------------------------------------------------------------------------
# generate_all_qr_pdf
# ---------------------------------------------------------------------------

SAMPLE_CLIENTS: list[dict[str, Any]] = [
    {"client_code": "ACME",  "client_name": "Acme Corp"},
    {"client_code": "SOUSS", "client_name": "Sous-Sol Ltée"},
]


class TestGenerateAllQrPdf:
    def test_returns_bytes(self):
        result = generate_all_qr_pdf(SAMPLE_CLIENTS, "http://127.0.0.1:8788")
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_valid_pdf_header(self):
        result = generate_all_qr_pdf(SAMPLE_CLIENTS, "http://127.0.0.1:8788")
        assert result[:4] == b"%PDF"

    def test_empty_client_list(self):
        """Empty list should return a valid (empty) PDF without raising."""
        result = generate_all_qr_pdf([], "http://127.0.0.1:8788")
        assert result[:4] == b"%PDF"

    def test_single_client(self):
        clients = [{"client_code": "ONE", "client_name": "Only Client"}]
        result = generate_all_qr_pdf(clients, "http://127.0.0.1:8788")
        assert result[:4] == b"%PDF"

    def test_pdf_contains_client_name(self):
        """Client name should appear somewhere in the PDF byte stream."""
        clients = [{"client_code": "ACME", "client_name": "SpecialCorpName"}]
        result = generate_all_qr_pdf(clients, "http://127.0.0.1:8788")
        assert b"SpecialCorpName" in result

    def test_pdf_contains_bilingual_instructions(self):
        result = generate_all_qr_pdf(SAMPLE_CLIENTS, "http://127.0.0.1:8788")
        # Instructions text is embedded in the PDF
        assert b"Scannez" in result or b"Scan" in result

    def test_pdf_contains_otocpa_header(self):
        result = generate_all_qr_pdf(SAMPLE_CLIENTS, "http://127.0.0.1:8788")
        assert b"OtoCPA" in result

    def test_missing_client_name_falls_back_to_code(self):
        clients = [{"client_code": "X99", "client_name": None}]
        result = generate_all_qr_pdf(clients, "http://127.0.0.1:8788")
        assert result[:4] == b"%PDF"
        assert b"X99" in result

    def test_public_portal_url_used(self):
        clients = [{"client_code": "TST", "client_name": "Test"}]
        result = generate_all_qr_pdf(clients, "https://portal.example.com")
        assert b"portal.example.com" in result

    def test_multiple_clients_larger_than_single(self):
        single = generate_all_qr_pdf(SAMPLE_CLIENTS[:1], "http://127.0.0.1:8788")
        multi  = generate_all_qr_pdf(SAMPLE_CLIENTS,     "http://127.0.0.1:8788")
        # Multi-page PDF should be larger than single-page
        assert len(multi) > len(single)
