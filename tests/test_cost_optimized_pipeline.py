"""
tests/test_cost_optimized_pipeline.py

Unit tests for the cost-optimized AI pipeline added to src/engines/ocr_engine.py.

Tests cover:
  - ai_usage_log table creation and logging
  - get_ai_cost_summary
  - check_vendor_cache
  - extract_with_pdfplumber
  - parse_invoice_fields
  - assess_image_quality
  - classify_complexity
  - get_model_for_complexity
  - save_vendor_cache
  - process_document_optimized
  - _estimate_cost

All external I/O is mocked — no network, no persistent state.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.engines.ocr_engine as ocr


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """Create a temp SQLite database with required tables."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, review_status TEXT, confidence REAL,
            raw_result TEXT, created_at TEXT, updated_at TEXT,
            submitted_by TEXT, client_note TEXT,
            gl_account TEXT, tax_code TEXT, category TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS learning_memory_patterns (
            vendor_key TEXT,
            client_code_key TEXT DEFAULT '',
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            doc_type TEXT,
            avg_confidence REAL DEFAULT 0.0,
            outcome_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            PRIMARY KEY (vendor_key, client_code_key)
        )
    """)
    conn.commit()
    return db, conn


@pytest.fixture
def populated_cache(tmp_db):
    """Database with a known vendor in learning_memory_patterns."""
    db, conn = tmp_db
    conn.execute(
        """INSERT INTO learning_memory_patterns
           (vendor_key, client_code_key, gl_account, tax_code, category,
            doc_type, avg_confidence, outcome_count, success_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("bell canada", "client1", "5100", "GST+QST", "telecom",
         "invoice", 0.95, 10, 10),
    )
    conn.commit()
    return db, conn


# ---------------------------------------------------------------------------
# _estimate_cost
# ---------------------------------------------------------------------------

class TestEstimateCost:
    def test_known_model(self):
        cost = ocr._estimate_cost("deepseek/deepseek-chat", 1000)
        assert cost == pytest.approx(0.0002, abs=1e-6)

    def test_unknown_model_default(self):
        cost = ocr._estimate_cost("unknown/model", 1000)
        assert cost == pytest.approx(0.001, abs=1e-6)

    def test_zero_tokens(self):
        assert ocr._estimate_cost("deepseek/deepseek-chat", 0) == 0.0

    def test_haiku_cost(self):
        cost = ocr._estimate_cost("anthropic/claude-haiku-4-5", 2000)
        assert cost == pytest.approx(0.002, abs=1e-6)


# ---------------------------------------------------------------------------
# ai_usage_log
# ---------------------------------------------------------------------------

class TestAiUsageLog:
    def test_log_creates_table(self, tmp_path):
        db = tmp_path / "log.db"
        ocr.log_ai_usage(
            document_id="doc_test",
            client_code="c1",
            source="cache",
            db_path=db,
        )
        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM ai_usage_log").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][3] == "cache"  # source column

    def test_log_fields(self, tmp_path):
        db = tmp_path / "log.db"
        ocr.log_ai_usage(
            document_id="doc_123",
            client_code="acme",
            source="ai_simple",
            model_used="deepseek/deepseek-chat",
            cost_usd=0.0002,
            tokens_used=500,
            confidence=0.91,
            db_path=db,
        )
        conn = sqlite3.connect(str(db))
        row = conn.execute("SELECT * FROM ai_usage_log").fetchone()
        conn.close()
        assert row[1] == "doc_123"  # document_id
        assert row[2] == "acme"     # client_code
        assert row[3] == "ai_simple"
        assert row[4] == "deepseek/deepseek-chat"
        assert row[5] == pytest.approx(0.0002)
        assert row[6] == 500

    def test_log_multiple(self, tmp_path):
        db = tmp_path / "log.db"
        for source in ("cache", "text_extraction", "ai_simple"):
            ocr.log_ai_usage(source=source, db_path=db)
        conn = sqlite3.connect(str(db))
        count = conn.execute("SELECT COUNT(*) FROM ai_usage_log").fetchone()[0]
        conn.close()
        assert count == 3


# ---------------------------------------------------------------------------
# get_ai_cost_summary
# ---------------------------------------------------------------------------

class TestGetAiCostSummary:
    def test_empty_db(self, tmp_path):
        db = tmp_path / "empty.db"
        stats = ocr.get_ai_cost_summary(db_path=db)
        assert stats["total_documents"] == 0
        assert stats["total_cost_usd"] == 0.0
        assert stats["savings_pct"] == 0.0

    def test_with_data(self, tmp_path):
        db = tmp_path / "stats.db"
        now = datetime.now(timezone.utc).strftime("%Y-%m")
        conn = sqlite3.connect(str(db))
        conn.execute(ocr._AI_USAGE_LOG_CREATE)
        for source, cost in [
            ("cache", 0.0), ("cache", 0.0),
            ("text_extraction", 0.0),
            ("ai_simple", 0.0003),
            ("ai_medium", 0.001),
        ]:
            conn.execute(
                """INSERT INTO ai_usage_log
                   (document_id, client_code, source, model_used,
                    cost_usd, tokens_used, confidence, created_at)
                   VALUES ('', '', ?, '', ?, 0, 0.9, ?)""",
                (source, cost, f"{now}-15T10:00:00+00:00"),
            )
        conn.commit()
        conn.close()

        stats = ocr.get_ai_cost_summary(period="month", db_path=db)
        assert stats["total_documents"] == 5
        assert stats["cache_count"] == 2
        assert stats["text_extraction_count"] == 1
        assert stats["ai_simple_count"] == 1
        assert stats["ai_medium_count"] == 1
        assert stats["total_cost_usd"] == pytest.approx(0.0013, abs=1e-4)
        assert stats["savings_pct"] > 0


# ---------------------------------------------------------------------------
# check_vendor_cache
# ---------------------------------------------------------------------------

class TestCheckVendorCache:
    def test_cache_hit(self, populated_cache):
        _, conn = populated_cache
        result = ocr.check_vendor_cache("client1", "/uploads/bell_canada_inv.pdf", conn)
        assert result is not None
        assert result["vendor"] == "bell canada"
        assert result["gl_account"] == "5100"
        assert result["confidence"] > 0.0

    def test_cache_miss(self, tmp_db):
        _, conn = tmp_db
        result = ocr.check_vendor_cache("c1", "/uploads/unknown_vendor.pdf", conn)
        assert result is None

    def test_low_outcome_count(self, tmp_db):
        _, conn = tmp_db
        conn.execute(
            """INSERT INTO learning_memory_patterns
               (vendor_key, client_code_key, gl_account, avg_confidence,
                outcome_count, success_count)
               VALUES ('newvendor', 'c1', '5100', 0.95, 2, 2)""",
        )
        conn.commit()
        result = ocr.check_vendor_cache("c1", "/uploads/newvendor_receipt.pdf", conn)
        assert result is None  # below min outcome threshold


# ---------------------------------------------------------------------------
# extract_with_pdfplumber
# ---------------------------------------------------------------------------

class TestExtractWithPdfplumber:
    @patch("pdfplumber.open")
    def test_good_pdf(self, mock_open):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = " ".join(["word"] * 60)
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_open.return_value = mock_pdf

        result = ocr.extract_with_pdfplumber("/test.pdf")
        assert result["confidence"] == 0.95
        assert result["word_count"] == 60

    @patch("pdfplumber.open")
    def test_sparse_pdf(self, mock_open):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "hello world"
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_open.return_value = mock_pdf

        result = ocr.extract_with_pdfplumber("/test.pdf")
        assert result["confidence"] < 0.85

    def test_nonexistent_file(self):
        result = ocr.extract_with_pdfplumber("/nonexistent.pdf")
        assert result["confidence"] == 0.0
        assert result["text"] == ""


# ---------------------------------------------------------------------------
# parse_invoice_fields
# ---------------------------------------------------------------------------

class TestParseInvoiceFields:
    def test_empty_text(self):
        result = ocr.parse_invoice_fields("")
        assert result["confidence"] == 0.0
        assert result["vendor"] is None

    def test_basic_invoice(self):
        text = """Bell Canada
Invoice #12345
Date: 2026-03-15
Subtotal: $150.00
Total: $172.43
"""
        result = ocr.parse_invoice_fields(text)
        assert result["vendor"] == "Bell Canada"
        assert result["amount"] == pytest.approx(172.43)
        assert result["document_date"] == "2026-03-15"
        assert result["doc_type"] == "invoice"
        assert result["confidence"] > 0.5

    def test_receipt_detection(self):
        text = """Store Name
Reçu de paiement
$25.50
"""
        result = ocr.parse_invoice_fields(text)
        assert result["doc_type"] == "receipt"

    def test_amount_extraction(self):
        text = """Vendor Inc
Amount Due: $1,234.56
"""
        result = ocr.parse_invoice_fields(text)
        assert result["amount"] == pytest.approx(1234.56)

    def test_paypal_google_amount(self):
        """Storage sizes like '100 GB' must not be extracted as amounts."""
        text = """Google -32.18
100 GB Google One
Total 32.18
Transaction ID 0JE97521J5871591M"""
        result = ocr.parse_invoice_fields(text)
        amount = float(result.get("amount") or 0)
        assert amount == pytest.approx(32.18), (
            f"Expected 32.18 got {amount} - picked up 100 GB as amount"
        )


# ---------------------------------------------------------------------------
# assess_image_quality
# ---------------------------------------------------------------------------

class TestAssessImageQuality:
    def test_nonexistent_file(self):
        quality = ocr.assess_image_quality("/nonexistent.png")
        assert quality == 0.5  # default for unknown

    @patch("PIL.Image.open")
    @patch("PIL.ImageStat.Stat")
    def test_good_image(self, mock_stat_cls, mock_open):
        mock_img = MagicMock()
        mock_img.size = (1920, 1080)
        mock_grey = MagicMock()
        mock_open.return_value = mock_img
        mock_img.convert.return_value = mock_grey
        mock_stat = MagicMock()
        mock_stat.stddev = [55.0]
        mock_stat.mean = [128.0]
        mock_stat_cls.return_value = mock_stat

        quality = ocr.assess_image_quality("/good_image.png")
        assert 0.5 < quality <= 1.0


# ---------------------------------------------------------------------------
# classify_complexity
# ---------------------------------------------------------------------------

class TestClassifyComplexity:
    def test_good_text_result(self):
        assert ocr.classify_complexity(
            "/doc.png",
            {"confidence": 0.90, "text": "lots of text"},
        ) == "simple"

    def test_pdf_without_text(self):
        assert ocr.classify_complexity("/doc.pdf", None) == "simple"

    def test_low_text_confidence(self):
        with patch.object(ocr, "assess_image_quality", return_value=0.80):
            assert ocr.classify_complexity(
                "/doc.jpg",
                {"confidence": 0.30},
            ) == "medium"

    def test_complex_image(self):
        with patch.object(ocr, "assess_image_quality", return_value=0.40):
            assert ocr.classify_complexity(
                "/doc.jpg",
                {"confidence": 0.20},
            ) == "complex"


# ---------------------------------------------------------------------------
# get_model_for_complexity
# ---------------------------------------------------------------------------

class TestGetModelForComplexity:
    @patch.object(ocr, "_load_config", return_value={})
    def test_defaults(self, _):
        assert ocr.get_model_for_complexity("simple") == "deepseek/deepseek-chat"
        assert ocr.get_model_for_complexity("medium") == "google/gemini-2.0-flash-001"
        assert ocr.get_model_for_complexity("complex") == "anthropic/claude-haiku-4-5"
        assert ocr.get_model_for_complexity("very_complex") == "anthropic/claude-sonnet-4-6"

    @patch.object(ocr, "_load_config", return_value={
        "ai_complexity_models": {"simple": "custom/model-1"}
    })
    def test_config_override(self, _):
        assert ocr.get_model_for_complexity("simple") == "custom/model-1"
        assert ocr.get_model_for_complexity("medium") == "google/gemini-2.0-flash-001"

    @patch.object(ocr, "_load_config", return_value={})
    def test_unknown_complexity(self, _):
        assert ocr.get_model_for_complexity("unknown") == "deepseek/deepseek-chat"


# ---------------------------------------------------------------------------
# save_vendor_cache
# ---------------------------------------------------------------------------

class TestSaveVendorCache:
    def test_insert_new(self, tmp_db):
        _, conn = tmp_db
        result = {
            "vendor": "Hydro Quebec",
            "gl_account": "5200",
            "tax_code": "QST",
            "category": "utilities",
            "doc_type": "invoice",
            "confidence": 0.91,
        }
        ocr.save_vendor_cache("client1", result, conn)
        row = conn.execute(
            "SELECT * FROM learning_memory_patterns WHERE vendor_key = ?",
            ("hydro quebec",),
        ).fetchone()
        assert row is not None
        assert row[2] == "5200"  # gl_account

    def test_update_existing(self, populated_cache):
        _, conn = populated_cache
        result = {
            "vendor": "Bell Canada",
            "gl_account": "5150",
            "tax_code": "GST+QST",
            "category": "telecom",
            "doc_type": "invoice",
            "confidence": 0.95,
        }
        ocr.save_vendor_cache("client1", result, conn)
        row = conn.execute(
            "SELECT outcome_count, success_count FROM learning_memory_patterns WHERE vendor_key = ?",
            ("bell canada",),
        ).fetchone()
        assert row[0] == 11  # outcome_count incremented
        assert row[1] == 11  # success_count incremented

    def test_no_vendor_noop(self, tmp_db):
        _, conn = tmp_db
        ocr.save_vendor_cache("c1", {"confidence": 0.9}, conn)
        count = conn.execute("SELECT COUNT(*) FROM learning_memory_patterns").fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# process_document_optimized
# ---------------------------------------------------------------------------

class TestProcessDocumentOptimized:
    def test_cache_hit(self, populated_cache):
        db, conn = populated_cache
        result = ocr.process_document_optimized(
            "doc_001",
            "/uploads/client1/bell_canada_inv.pdf",
            "client1",
            conn,
            db_path=db,
        )
        assert result["source"] == "cache"
        assert result["cost"] == 0.0
        assert result["vendor"] == "bell canada"

    @patch("pdfplumber.open")
    def test_text_extraction(self, mock_open, tmp_db):
        db, conn = tmp_db
        # Create mock PDF with rich text
        text = """Bell Canada
Facture #99999
Date: 2026-03-20
Total: $250.00
Amount Due: $250.00
""" + " ".join(["additional text word"] * 20)

        mock_page = MagicMock()
        mock_page.extract_text.return_value = text
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_open.return_value = mock_pdf

        result = ocr.process_document_optimized(
            "doc_002",
            "/uploads/c1/invoice.pdf",
            "c1",
            conn,
            db_path=db,
        )
        assert result["source"] == "text_extraction"
        assert result["cost"] == 0.0
        assert result["amount"] == pytest.approx(250.0)

    @patch.object(ocr, "_call_openrouter_for_extraction")
    @patch.object(ocr, "classify_complexity", return_value="simple")
    def test_ai_fallback(self, mock_classify, mock_call, tmp_db):
        db, conn = tmp_db
        mock_call.return_value = {
            "vendor_name": "New Vendor",
            "total": 100.0,
            "doc_type": "invoice",
            "confidence": 0.88,
            "tokens_used": 500,
            "cost_usd": 0.0001,
            "model_used": "deepseek/deepseek-chat",
        }
        result = ocr.process_document_optimized(
            "doc_003",
            "/uploads/c1/new_vendor.jpg",
            "c1",
            conn,
            db_path=db,
        )
        assert result["source"] == "ai_simple"
        assert result["cost"] == pytest.approx(0.0001)
        assert result["vendor"] == "New Vendor"
        assert result["model_used"] == "deepseek/deepseek-chat"

    @patch.object(ocr, "_call_openrouter_for_extraction")
    @patch.object(ocr, "classify_complexity", return_value="medium")
    def test_ai_model_escalation_on_failure(self, mock_classify, mock_call, tmp_db):
        db, conn = tmp_db
        # First call fails, second succeeds
        mock_call.side_effect = [
            RuntimeError("model_unavailable"),
            {
                "vendor_name": "Vendor",
                "total": 50.0,
                "confidence": 0.75,
                "tokens_used": 800,
                "cost_usd": 0.0008,
                "model_used": "anthropic/claude-haiku-4-5",
            },
        ]
        result = ocr.process_document_optimized(
            "doc_004",
            "/uploads/c1/blurry.jpg",
            "c1",
            conn,
            db_path=db,
        )
        assert result["source"] == "ai_medium"
        assert result["vendor"] == "Vendor"
        assert mock_call.call_count == 2

    @patch.object(ocr, "_call_openrouter_for_extraction")
    @patch.object(ocr, "classify_complexity", return_value="complex")
    def test_complete_failure(self, mock_classify, mock_call, tmp_db):
        db, conn = tmp_db
        mock_call.side_effect = RuntimeError("all_models_down")
        result = ocr.process_document_optimized(
            "doc_005",
            "/uploads/c1/terrible.jpg",
            "c1",
            conn,
            db_path=db,
        )
        assert "error" in result
        assert result["confidence"] == 0.0


# ---------------------------------------------------------------------------
# Config integration
# ---------------------------------------------------------------------------

class TestConfigIntegration:
    def test_config_has_complexity_models(self):
        """Verify otocpa.config.json has ai_complexity_models."""
        cfg_path = ROOT / "otocpa.config.json"
        if cfg_path.exists():
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            assert "ai_complexity_models" in cfg
            models = cfg["ai_complexity_models"]
            assert "simple" in models
            assert "medium" in models
            assert "complex" in models
            assert "very_complex" in models


# ---------------------------------------------------------------------------
# _build_extraction_prompt
# ---------------------------------------------------------------------------

class TestBuildExtractionPrompt:
    def test_without_text(self):
        prompt = ocr._build_extraction_prompt("/doc.jpg")
        assert "Extract accounting data" in prompt

    def test_with_text(self):
        prompt = ocr._build_extraction_prompt(
            "/doc.pdf",
            {"text": "Invoice from Bell Canada", "confidence": 0.9},
        )
        assert "Bell Canada" in prompt
        assert "Extract accounting data" in prompt


# ---------------------------------------------------------------------------
# Dashboard render_ai_costs (smoke test)
# ---------------------------------------------------------------------------

class TestDashboardAiCosts:
    def test_render_ai_costs_import(self):
        """Verify render_ai_costs is importable from review_dashboard."""
        sys.path.insert(0, str(ROOT / "scripts"))
        try:
            from review_dashboard import render_ai_costs
            assert callable(render_ai_costs)
        except ImportError:
            pytest.skip("review_dashboard not importable in test environment")
        finally:
            sys.path.pop(0)
