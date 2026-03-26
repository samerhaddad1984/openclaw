"""
tests/test_ocr_engine.py

Unit tests for src/engines/ocr_engine.py.

All external I/O (HTTP, SQLite, filesystem) is mocked so the suite runs
with no network access and leaves no persistent state.
"""
from __future__ import annotations

import base64
import email
import email.mime.application
import email.mime.image
import email.mime.multipart
import email.mime.text
import io
import json
import sqlite3
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# sys.path bootstrap
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.engines.ocr_engine as ocr


# ---------------------------------------------------------------------------
# Magic-byte samples
# ---------------------------------------------------------------------------

_PDF_BYTES  = b"%PDF-1.4 minimal"
_PNG_BYTES  = b"\x89PNG\r\n\x1a\n" + b"\x00" * 20
_JPEG_BYTES = b"\xff\xd8\xff\xe0" + b"\x00" * 20
_TIFF_LE    = b"II*\x00" + b"\x00" * 20
_TIFF_BE    = b"MM\x00*" + b"\x00" * 20
_WEBP_BYTES = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 10
_HEIC_BYTES = b"\x00\x00\x00\x18ftyp" + b"heic" + b"\x00" * 20
_JUNK_BYTES = b"\x00\x01\x02\x03" + b"\x00" * 20


# ---------------------------------------------------------------------------
# detect_format
# ---------------------------------------------------------------------------

class TestDetectFormat:
    def test_pdf(self):
        assert ocr.detect_format(_PDF_BYTES) == "pdf"

    def test_png(self):
        assert ocr.detect_format(_PNG_BYTES) == "png"

    def test_jpeg(self):
        assert ocr.detect_format(_JPEG_BYTES) == "jpeg"

    def test_tiff_little_endian(self):
        assert ocr.detect_format(_TIFF_LE) == "tiff"

    def test_tiff_big_endian(self):
        assert ocr.detect_format(_TIFF_BE) == "tiff"

    def test_webp(self):
        assert ocr.detect_format(_WEBP_BYTES) == "webp"

    def test_heic(self):
        assert ocr.detect_format(_HEIC_BYTES) == "heic"

    def test_unknown_returns_unknown(self):
        assert ocr.detect_format(_JUNK_BYTES) == "unknown"

    def test_too_short_returns_unknown(self):
        assert ocr.detect_format(b"\x89PNG") == "unknown"

    @pytest.mark.parametrize("data,expected", [
        (_PDF_BYTES,  "pdf"),
        (_PNG_BYTES,  "png"),
        (_JPEG_BYTES, "jpeg"),
        (_WEBP_BYTES, "webp"),
        (_HEIC_BYTES, "heic"),
    ])
    def test_parametrised(self, data, expected):
        assert ocr.detect_format(data) == expected


# ---------------------------------------------------------------------------
# _normalise_image
# ---------------------------------------------------------------------------

class TestNormaliseImage:
    def test_jpeg_unchanged(self):
        data, mime = ocr._normalise_image(_JPEG_BYTES, "jpeg")
        assert data == _JPEG_BYTES
        assert mime == "image/jpeg"

    def test_png_unchanged(self):
        data, mime = ocr._normalise_image(_PNG_BYTES, "png")
        assert data == _PNG_BYTES
        assert mime == "image/png"

    def test_tiff_mime(self):
        _, mime = ocr._normalise_image(_TIFF_LE, "tiff")
        assert mime == "image/tiff"

    def test_webp_mime(self):
        _, mime = ocr._normalise_image(_WEBP_BYTES, "webp")
        assert mime == "image/webp"

    def test_heic_falls_back_to_raw_when_pillow_unavailable(self):
        with patch.dict("sys.modules", {"pillow_heif": None, "PIL": None}):
            data, mime = ocr._normalise_image(_HEIC_BYTES, "heic")
        # Should return the raw bytes as heic when pillow-heif not available
        assert data == _HEIC_BYTES
        assert mime == "image/heic"


# ---------------------------------------------------------------------------
# call_vision
# ---------------------------------------------------------------------------

_VISION_RESPONSE = {
    "doc_type":       "invoice",
    "vendor_name":    "ACME Corp",
    "document_date":  "2026-01-15",
    "invoice_number": "INV-001",
    "currency":       "CAD",
    "subtotal":       1000.00,
    "tax_total":      50.00,
    "total":          1050.00,
    "taxes":          [{"type": "GST", "amount": 50.00}],
    "confidence":     0.92,
    "notes":          "Clear invoice",
}


def _mock_vision_http(content: str = None, status: int = 200) -> MagicMock:
    if content is None:
        content = json.dumps(_VISION_RESPONSE)
    resp = MagicMock()
    resp.status_code = status
    resp.text = content
    resp.json.return_value = {"choices": [{"message": {"content": content}}]}
    return resp


class TestCallVision:
    _PROVIDER = {
        "base_url": "https://openrouter.example.com/v1",
        "api_key":  "testkey",
        "model":    "anthropic/claude-test",
    }

    def _run(self, mock_resp: MagicMock) -> dict:
        with patch.object(ocr, "_vision_provider", return_value=self._PROVIDER), \
             patch("src.engines.ocr_engine.requests.post", return_value=mock_resp):
            return ocr.call_vision(_JPEG_BYTES, "image/jpeg")

    def test_returns_parsed_dict(self):
        result = self._run(_mock_vision_http())
        assert result["vendor_name"] == "ACME Corp"
        assert result["doc_type"] == "invoice"
        assert result["confidence"] == 0.92

    def test_image_sent_as_data_uri(self):
        captured = []
        def capture(*args, **kwargs):
            captured.append(kwargs.get("json") or {})
            return _mock_vision_http()

        with patch.object(ocr, "_vision_provider", return_value=self._PROVIDER), \
             patch("src.engines.ocr_engine.requests.post", side_effect=capture):
            ocr.call_vision(_JPEG_BYTES, "image/jpeg")

        assert captured, "HTTP was never called"
        msg_content = captured[0]["messages"][0]["content"]
        img_part = next(p for p in msg_content if p["type"] == "image_url")
        uri = img_part["image_url"]["url"]
        assert uri.startswith("data:image/jpeg;base64,")

    def test_strips_markdown_fences(self):
        fenced = f"```json\n{json.dumps(_VISION_RESPONSE)}\n```"
        result = self._run(_mock_vision_http(fenced))
        assert result["vendor_name"] == "ACME Corp"

    def test_raises_on_http_error(self):
        with pytest.raises(RuntimeError, match="HTTP 503"):
            self._run(_mock_vision_http("error", status=503))

    def test_raises_when_provider_not_configured(self):
        with patch.object(ocr, "_vision_provider", return_value={}):
            with pytest.raises(RuntimeError, match="not_configured"):
                ocr.call_vision(_JPEG_BYTES, "image/jpeg")

    def test_raises_on_non_json_response(self):
        with pytest.raises(RuntimeError, match="non-JSON"):
            self._run(_mock_vision_http("This is plain text, not JSON"))


# ---------------------------------------------------------------------------
# extract_pdf_text
# ---------------------------------------------------------------------------

class TestExtractPdfText:
    def test_pdfplumber_used_when_available(self):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Invoice from ACME Corp"
        mock_pdf = MagicMock()
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdf.pages = [mock_page]

        with patch.dict("sys.modules", {}):
            mock_pdfplumber = MagicMock()
            mock_pdfplumber.open.return_value = mock_pdf
            with patch.dict("sys.modules", {"pdfplumber": mock_pdfplumber}):
                result = ocr.extract_pdf_text(_PDF_BYTES)

        assert "ACME" in result

    def test_returns_empty_string_when_all_extractors_fail(self):
        with patch.dict("sys.modules", {"pdfplumber": None, "pdfminer": None,
                                        "pdfminer.high_level": None}):
            result = ocr.extract_pdf_text(_PDF_BYTES)
        assert result == ""


# ---------------------------------------------------------------------------
# DB: upsert_document
# ---------------------------------------------------------------------------

class TestUpsertDocument:
    def _sample_record(self, doc_id: str = "doc_abc123") -> dict:
        return {
            "document_id":       doc_id,
            "file_name":         "test.pdf",
            "file_path":         "/tmp/test.pdf",
            "client_code":       "CLIENT1",
            "vendor":            "ACME",
            "doc_type":          "invoice",
            "amount":            1050.0,
            "document_date":     "2026-01-15",
            "review_status":     "New",
            "confidence":        0.92,
            "raw_result":        "{}",
            "created_at":        "2026-01-15T00:00:00+00:00",
            "updated_at":        "2026-01-15T00:00:00+00:00",
            "submitted_by":      "alice",
            "client_note":       None,
            "currency":          "CAD",
            "subtotal":          1000.0,
            "tax_total":         50.0,
            "extraction_method": "pdfplumber_text",
            "ingest_source":     "portal",
        }

    def test_inserts_row(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        # Create table first
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT, file_path TEXT, client_code TEXT,
                vendor TEXT, doc_type TEXT, amount REAL, document_date TEXT,
                review_status TEXT, confidence REAL, raw_result TEXT,
                created_at TEXT, updated_at TEXT, submitted_by TEXT,
                client_note TEXT
            )
        """)
        conn.commit()
        conn.close()

        ocr.upsert_document(self._sample_record(), db_path=db)

        conn = sqlite3.connect(str(db))
        row = conn.execute("SELECT * FROM documents WHERE document_id='doc_abc123'").fetchone()
        conn.close()
        assert row is not None

    def test_update_on_conflict(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT, file_path TEXT, client_code TEXT,
                vendor TEXT, doc_type TEXT, amount REAL, document_date TEXT,
                review_status TEXT, confidence REAL, raw_result TEXT,
                created_at TEXT, updated_at TEXT, submitted_by TEXT,
                client_note TEXT
            )
        """)
        conn.commit()
        conn.close()

        rec = self._sample_record()
        ocr.upsert_document(rec, db_path=db)

        # Update vendor
        rec["vendor"] = "UpdatedVendor"
        ocr.upsert_document(rec, db_path=db)

        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM documents WHERE document_id='doc_abc123'").fetchone()
        conn.close()
        assert row["vendor"] == "UpdatedVendor"

    def test_new_columns_added_automatically(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        # Minimal table (missing new columns)
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT, file_path TEXT, client_code TEXT,
                vendor TEXT, doc_type TEXT, amount REAL, document_date TEXT,
                review_status TEXT, confidence REAL, raw_result TEXT,
                created_at TEXT, updated_at TEXT, submitted_by TEXT,
                client_note TEXT
            )
        """)
        conn.commit()
        conn.close()

        ocr.upsert_document(self._sample_record(), db_path=db)

        # New columns should now exist
        conn = sqlite3.connect(str(db))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        conn.close()
        assert "currency" in cols
        assert "extraction_method" in cols
        assert "ingest_source" in cols


# ---------------------------------------------------------------------------
# process_file
# ---------------------------------------------------------------------------

class TestProcessFile:
    """Tests for the full pipeline function (mocked Vision + DB)."""

    def _make_db(self) -> Path:
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db = Path(f.name)
        f.close()
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                file_name TEXT, file_path TEXT, client_code TEXT,
                vendor TEXT, doc_type TEXT, amount REAL, document_date TEXT,
                review_status TEXT, confidence REAL, raw_result TEXT,
                created_at TEXT, updated_at TEXT, submitted_by TEXT,
                client_note TEXT
            )
        """)
        conn.commit()
        conn.close()
        return db

    def _vision_ok(self) -> dict:
        return {**_VISION_RESPONSE}

    def test_jpeg_processed_via_vision(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "detect_handwriting", return_value=0.0), \
                 patch.object(ocr, "call_vision", return_value=self._vision_ok()):
                result = ocr.process_file(
                    _JPEG_BYTES, "invoice.jpg",
                    client_code="CLI1",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        assert result["ok"] is True
        assert result["format"] == "jpeg"
        assert result["vendor"] == "ACME Corp"
        assert result["doc_type"] == "invoice"
        assert result["confidence"] == 0.92
        assert result["low_confidence_flagged"] is False
        assert result["review_status"] == "New"

    def test_low_confidence_flagged_for_review(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            low_conf = {**_VISION_RESPONSE, "confidence": 0.55}
            with patch.object(ocr, "call_vision", return_value=low_conf):
                result = ocr.process_file(
                    _PNG_BYTES, "blurry.png",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        assert result["low_confidence_flagged"] is True
        assert result["review_status"] == "NeedsReview"

    def test_document_written_to_db(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "detect_handwriting", return_value=0.0), \
                 patch.object(ocr, "call_vision", return_value=self._vision_ok()):
                result = ocr.process_file(
                    _JPEG_BYTES, "inv.jpg",
                    client_code="CLI2",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM documents WHERE document_id=?",
            (result["document_id"],),
        ).fetchone()
        conn.close()

        assert row is not None
        assert row["vendor"] == "ACME Corp"
        assert row["confidence"] == 0.92

    def test_unsupported_format_returns_error(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            result = ocr.process_file(
                _JUNK_BYTES, "file.xyz",
                db_path=db,
                upload_dir=Path(tmp),
            )

        assert result["ok"] is False
        assert "unsupported_format" in result["error"]

    def test_pdf_uses_pdfplumber_text_path(self):
        db = self._make_db()
        long_text = "Invoice from ACME Corp " * 10  # > 20 words

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "extract_pdf_text", return_value=long_text), \
                 patch.object(ocr, "_extract_from_text", return_value=self._vision_ok()) as mock_text:
                result = ocr.process_file(
                    _PDF_BYTES, "doc.pdf",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        mock_text.assert_called_once_with(long_text)
        assert result["extraction_method"] == "pdfplumber_text"

    def test_pdf_falls_back_to_vision_for_scanned(self):
        db = self._make_db()
        short_text = "Invoice"  # < 20 words

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "extract_pdf_text", return_value=short_text), \
                 patch.object(ocr, "_pdf_to_images", return_value=[(_JPEG_BYTES, "image/jpeg")]), \
                 patch.object(ocr, "detect_handwriting", return_value=0.0), \
                 patch.object(ocr, "call_vision", return_value=self._vision_ok()) as mock_vis:
                result = ocr.process_file(
                    _PDF_BYTES, "scan.pdf",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        mock_vis.assert_called_once()
        assert result["extraction_method"] == "vision_pdf_fallback"

    def test_pdf_empty_fallback_when_no_images(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "extract_pdf_text", return_value=""), \
                 patch.object(ocr, "_pdf_to_images", return_value=[]):
                result = ocr.process_file(
                    _PDF_BYTES, "empty.pdf",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        assert result["extraction_method"] == "empty_pdf"
        assert result["low_confidence_flagged"] is True

    def test_vision_exception_recorded_not_raised(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "call_vision", side_effect=RuntimeError("API down")):
                result = ocr.process_file(
                    _JPEG_BYTES, "err.jpg",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        assert result["ok"] is True           # pipeline itself completed
        assert result["error"] == "API down"
        assert result["extraction_method"] == "failed"
        assert result["low_confidence_flagged"] is True

    def test_document_id_preserved_when_provided(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "call_vision", return_value=self._vision_ok()):
                result = ocr.process_file(
                    _JPEG_BYTES, "inv.jpg",
                    document_id="doc_FIXED001",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        assert result["document_id"] == "doc_FIXED001"

    def test_ingest_source_stored(self):
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "call_vision", return_value=self._vision_ok()):
                result = ocr.process_file(
                    _JPEG_BYTES, "inv.jpg",
                    ingest_source="email",
                    db_path=db,
                    upload_dir=Path(tmp),
                )

        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ingest_source FROM documents WHERE document_id=?",
            (result["document_id"],),
        ).fetchone()
        conn.close()
        assert row["ingest_source"] == "email"

    def test_confidence_boundary_exactly_07_not_flagged(self):
        """confidence == 0.7 should NOT be flagged (threshold is strict <)."""
        db = self._make_db()
        with tempfile.TemporaryDirectory() as tmp:
            boundary = {**_VISION_RESPONSE, "confidence": 0.7}
            with patch.object(ocr, "detect_handwriting", return_value=0.0), \
                 patch.object(ocr, "call_vision", return_value=boundary):
                result = ocr.process_file(
                    _PNG_BYTES, "ok.png",
                    db_path=db,
                    upload_dir=Path(tmp),
                )
        assert result["low_confidence_flagged"] is False
        assert result["review_status"] == "New"


# ---------------------------------------------------------------------------
# parse_email_attachments
# ---------------------------------------------------------------------------

def _build_email(attachments: list[tuple[bytes, str, str]]) -> bytes:
    """
    Build a minimal multipart email.

    attachments: list of (payload_bytes, filename, mime_type)
    """
    msg = email.mime.multipart.MIMEMultipart()
    msg["From"]    = "sender@example.com"
    msg["To"]      = "ledgerlink@firm.com"
    msg["Subject"] = "Documents"
    msg.attach(email.mime.text.MIMEText("Please find attached.", "plain"))

    for payload, filename, mime_type in attachments:
        main, sub = mime_type.split("/", 1)
        if main == "image":
            part = email.mime.image.MIMEImage(payload, _subtype=sub)
        else:
            part = email.mime.application.MIMEApplication(payload, Name=filename)
        part["Content-Disposition"] = f'attachment; filename="{filename}"'
        msg.attach(part)

    return msg.as_bytes()


class TestParseEmailAttachments:
    def test_pdf_attachment_extracted(self):
        raw = _build_email([(_PDF_BYTES, "invoice.pdf", "application/pdf")])
        result = ocr.parse_email_attachments(raw)
        assert len(result) == 1
        payload, filename = result[0]
        assert filename == "invoice.pdf"
        assert payload == _PDF_BYTES

    def test_jpeg_attachment_extracted(self):
        raw = _build_email([(_JPEG_BYTES, "receipt.jpg", "image/jpeg")])
        result = ocr.parse_email_attachments(raw)
        assert len(result) == 1
        assert result[0][1] == "receipt.jpg"

    def test_multiple_attachments(self):
        raw = _build_email([
            (_PDF_BYTES,  "inv.pdf",     "application/pdf"),
            (_JPEG_BYTES, "photo.jpg",   "image/jpeg"),
            (_PNG_BYTES,  "scan.png",    "image/png"),
        ])
        result = ocr.parse_email_attachments(raw)
        assert len(result) == 3

    def test_text_body_not_extracted(self):
        """Plain-text email body must not appear as an attachment."""
        raw = _build_email([])
        result = ocr.parse_email_attachments(raw)
        assert result == []

    def test_unknown_mime_detected_via_magic_bytes(self):
        """Attachment with wrong MIME type but valid magic bytes is still accepted."""
        raw = _build_email([(_PDF_BYTES, "file.bin", "application/octet-stream")])
        result = ocr.parse_email_attachments(raw)
        assert len(result) == 1

    def test_unsupported_mime_skipped(self):
        raw = _build_email([
            (b"<html>not a document</html>", "page.html", "text/html"),
        ])
        result = ocr.parse_email_attachments(raw)
        assert result == []


# ---------------------------------------------------------------------------
# EmailIngestHandler (HTTP)
# ---------------------------------------------------------------------------

class TestEmailIngestHandler:
    """Test the HTTP handler using direct handle_one_request invocation."""

    def _make_handler(self, db: Path, upload: Path) -> type:
        """Return a handler subclass bound to test DB/upload paths."""
        class _H(ocr.EmailIngestHandler):
            db_path    = db
            upload_dir = upload
        return _H

    def _post_email(self, raw_email: bytes, db: Path, upload: Path,
                    api_key: str = "", client_code: str = "") -> dict:
        """Fire a POST /ingest/email through a real handler instance."""
        from io import BytesIO
        from unittest.mock import MagicMock

        headers = {
            "Content-Length": str(len(raw_email)),
            "Content-Type":   "message/rfc822",
        }
        if client_code:
            headers["X-Client-Code"] = client_code

        handler_cls = self._make_handler(db, upload)

        req  = MagicMock()
        req.makefile.return_value = BytesIO(raw_email)

        responses: list[dict] = []

        def fake_init(self_h, request, addr, server):
            self_h.rfile    = BytesIO(raw_email)
            self_h.wfile    = BytesIO()
            self_h.headers  = headers
            self_h.command  = "POST"
            self_h.path     = f"/ingest/email?client_code={client_code}"
            self_h.server   = server
            self_h.request  = request
            self_h.client_address = ("127.0.0.1", 9999)

        handler_cls.__init__ = fake_init

        server_mock = MagicMock()
        h = handler_cls.__new__(handler_cls)
        fake_init(h, req, ("127.0.0.1", 9999), server_mock)
        h.db_path    = db
        h.upload_dir = upload

        captured_body = BytesIO()
        h.wfile = captured_body

        sent_status = []
        sent_headers: list[tuple] = []

        def fake_send_response(code):
            sent_status.append(code)
        def fake_send_header(k, v):
            sent_headers.append((k, v))
        def fake_end_headers():
            pass

        h.send_response  = fake_send_response
        h.send_header    = fake_send_header
        h.end_headers    = fake_end_headers

        with patch.object(ocr, "_ingest_api_key", return_value=api_key):
            h.do_POST()

        captured_body.seek(0)
        raw_resp = captured_body.read()
        return json.loads(raw_resp) if raw_resp else {}

    def test_processes_pdf_attachment(self):
        db = Path(tempfile.NamedTemporaryFile(suffix=".db", delete=False).name)
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
                client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
                document_date TEXT, review_status TEXT, confidence REAL,
                raw_result TEXT, created_at TEXT, updated_at TEXT,
                submitted_by TEXT, client_note TEXT
            )
        """)
        conn.commit()
        conn.close()

        raw_email = _build_email([(_PDF_BYTES, "inv.pdf", "application/pdf")])

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(ocr, "extract_pdf_text", return_value=""), \
                 patch.object(ocr, "_pdf_to_images", return_value=[]), \
                 patch.object(ocr, "_ingest_api_key", return_value=""):
                resp = self._post_email(raw_email, db, Path(tmp))

        assert resp.get("ok") is True
        assert resp.get("processed") == 1

    def test_empty_email_returns_zero_processed(self):
        db = Path(tempfile.NamedTemporaryFile(suffix=".db", delete=False).name)
        with tempfile.TemporaryDirectory() as tmp:
            raw_email = _build_email([])
            with patch.object(ocr, "_ingest_api_key", return_value=""):
                resp = self._post_email(raw_email, db, Path(tmp))

        assert resp.get("ok") is True
        assert resp.get("processed") == 0

    def test_api_key_rejected_when_wrong(self):
        db = Path(tempfile.NamedTemporaryFile(suffix=".db", delete=False).name)
        with tempfile.TemporaryDirectory() as tmp:
            raw_email = _build_email([])

            handler_cls = self._make_handler(db, Path(tmp))
            from io import BytesIO

            def fake_init(self_h, request, addr, server):
                self_h.rfile   = BytesIO(raw_email)
                self_h.wfile   = BytesIO()
                self_h.headers = {"Content-Length": "0", "X-API-Key": "WRONG"}
                self_h.path    = "/ingest/email"
                self_h.server  = server

            handler_cls.__init__ = fake_init
            h = handler_cls.__new__(handler_cls)
            fake_init(h, MagicMock(), ("127.0.0.1", 9999), MagicMock())
            h.db_path    = db
            h.upload_dir = Path(tmp)

            captured = BytesIO()
            h.wfile = captured
            statuses = []

            h.send_response  = lambda c: statuses.append(c)
            h.send_header    = lambda k, v: None
            h.end_headers    = lambda: None

            with patch.object(ocr, "_ingest_api_key", return_value="CORRECT"):
                h.do_POST()

            captured.seek(0)
            resp = json.loads(captured.read())
            assert statuses[0] == 401
            assert resp["error"] == "unauthorized"
