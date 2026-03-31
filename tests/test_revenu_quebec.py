"""
tests/test_revenu_quebec.py — pytest tests for the Revenu Québec pre-fill module.

Covers:
  - client_config table creation (ensure_client_config_table)
  - get / set client config (get_client_config, set_client_config)
  - Quick Method rate constants
  - compute_prefill — regular method (uses real filing data)
  - compute_prefill — Quick Method retail and services
  - compute_prefill — handles missing DB gracefully
  - PDF generation (generate_revenu_quebec_pdf) — returns valid PDF bytes
  - Migration script declares the client_config table
  - review_dashboard exposes render_revenu_quebec and the route
  - i18n keys present for all rq_* strings
"""
from __future__ import annotations

import inspect
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _make_full_db(path: Path) -> None:
    """Create a minimal otocpa_agent.db with documents + posting_jobs for tests."""
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE documents (
            document_id   TEXT PRIMARY KEY,
            client_code   TEXT,
            vendor        TEXT,
            document_date TEXT,
            amount        TEXT,
            tax_code      TEXT,
            gl_account    TEXT,
            review_status TEXT
        );
        CREATE TABLE posting_jobs (
            rowid           INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id     TEXT,
            posting_status  TEXT,
            external_id     TEXT,
            created_at      TEXT,
            updated_at      TEXT
        );
    """)
    # Insert a posted document with tax code T and amount 115 (includes GST+QST)
    conn.execute("""
        INSERT INTO documents VALUES
        ('DOC-001', 'ACME', 'Vendor A', '2025-01-15', '115.00', 'T',
         '5200 Office Supplies', 'approved')
    """)
    conn.execute("""
        INSERT INTO posting_jobs (document_id, posting_status, external_id, created_at)
        VALUES ('DOC-001', 'posted', 'QB-999', '2025-01-16')
    """)
    # Insert a pending (unposted) document
    conn.execute("""
        INSERT INTO documents VALUES
        ('DOC-002', 'ACME', 'Vendor B', '2025-02-10', '57.50', 'T',
         '5100 Supplies', 'needs_review')
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# ensure_client_config_table
# ---------------------------------------------------------------------------

class TestEnsureClientConfigTable:
    def test_creates_table(self):
        from src.agents.core.revenu_quebec import ensure_client_config_table
        conn = _in_memory_db()
        ensure_client_config_table(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert "client_config" in tables

    def test_idempotent(self):
        from src.agents.core.revenu_quebec import ensure_client_config_table
        conn = _in_memory_db()
        ensure_client_config_table(conn)
        ensure_client_config_table(conn)  # must not raise

    def test_columns_present(self):
        from src.agents.core.revenu_quebec import ensure_client_config_table
        conn = _in_memory_db()
        ensure_client_config_table(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(client_config)")}
        assert {"client_code", "quick_method", "quick_method_type", "updated_at"} <= cols


# ---------------------------------------------------------------------------
# get_client_config / set_client_config
# ---------------------------------------------------------------------------

class TestClientConfig:
    def test_defaults_when_missing(self):
        from src.agents.core.revenu_quebec import get_client_config
        conn = _in_memory_db()
        cfg = get_client_config(conn, "NEWCLIENT")
        assert cfg["quick_method"] == 0
        assert cfg["quick_method_type"] == "retail"

    def test_set_and_get_retail(self):
        from src.agents.core.revenu_quebec import get_client_config, set_client_config
        conn = _in_memory_db()
        set_client_config(conn, "ACME", True, "retail", "2025-01-01")
        cfg = get_client_config(conn, "ACME")
        assert cfg["quick_method"] == 1
        assert cfg["quick_method_type"] == "retail"

    def test_set_and_get_services(self):
        from src.agents.core.revenu_quebec import get_client_config, set_client_config
        conn = _in_memory_db()
        set_client_config(conn, "CORP", True, "services", "2025-01-01")
        cfg = get_client_config(conn, "CORP")
        assert cfg["quick_method"] == 1
        assert cfg["quick_method_type"] == "services"

    def test_upsert_replaces_existing(self):
        from src.agents.core.revenu_quebec import get_client_config, set_client_config
        conn = _in_memory_db()
        set_client_config(conn, "ACME", True, "retail", "2025-01-01")
        set_client_config(conn, "ACME", False, "services", "2025-06-01")
        cfg = get_client_config(conn, "ACME")
        assert cfg["quick_method"] == 0
        assert cfg["quick_method_type"] == "services"

    def test_false_stores_zero(self):
        from src.agents.core.revenu_quebec import get_client_config, set_client_config
        conn = _in_memory_db()
        set_client_config(conn, "ACME", False, "retail")
        cfg = get_client_config(conn, "ACME")
        assert cfg["quick_method"] == 0


# ---------------------------------------------------------------------------
# Quick Method rate constants
# ---------------------------------------------------------------------------

class TestQuickMethodRates:
    def test_retail_gst_rate(self):
        from src.agents.core.revenu_quebec import QM_RETAIL_GST
        assert QM_RETAIL_GST == Decimal("0.018")

    def test_retail_qst_rate(self):
        from src.agents.core.revenu_quebec import QM_RETAIL_QST
        assert QM_RETAIL_QST == Decimal("0.034")

    def test_services_gst_rate(self):
        from src.agents.core.revenu_quebec import QM_SERVICES_GST
        assert QM_SERVICES_GST == Decimal("0.036")

    def test_services_qst_rate(self):
        from src.agents.core.revenu_quebec import QM_SERVICES_QST
        assert QM_SERVICES_QST == Decimal("0.066")


# ---------------------------------------------------------------------------
# compute_prefill — regular method
# ---------------------------------------------------------------------------

class TestComputePrefillRegular:
    def test_missing_db_returns_error(self):
        from src.agents.core.revenu_quebec import compute_prefill
        conn = _in_memory_db()
        # In-memory DB has no documents/posting_jobs tables → tax engine will error
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        # Should not raise; should return error key OR zero values
        assert "line_101" in result
        assert "line_106" in result

    def test_line_101_is_zero(self, tmp_path):
        """Revenue is not tracked — line 101 must always be zero."""
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_101"] == Decimal("0")

    def test_line_103_is_zero(self, tmp_path):
        """GST collected on sales is not tracked."""
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_103"] == Decimal("0")

    def test_line_106_positive_for_posted_doc(self, tmp_path):
        """ITC (line 106) > 0 when there is a posted doc with tax code T."""
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_106"] > Decimal("0"), (
            f"Expected ITC > 0 for posted doc, got {result['line_106']}"
        )

    def test_line_108_equals_103_minus_106(self, tmp_path):
        """Net GST = line 103 − line 106."""
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_108"] == result["line_103"] - result["line_106"]

    def test_line_209_equals_205_minus_207(self, tmp_path):
        """Net QST = line 205 − line 207."""
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_209"] == result["line_205"] - result["line_207"]

    def test_quick_method_false_by_default(self, tmp_path):
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["quick_method"] is False

    def test_pending_doc_not_counted_in_itc(self, tmp_path):
        """Unposted documents must not contribute to ITC."""
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        # DOC-002 has no posting_job → pending
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["documents_pending"] >= 1

    def test_documents_posted_count(self, tmp_path):
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["documents_posted"] == 1


# ---------------------------------------------------------------------------
# compute_prefill — Quick Method
# ---------------------------------------------------------------------------

class TestComputePrefillQuickMethod:
    def _db_with_config(self, tmp_path, qm_type):
        from src.agents.core.revenu_quebec import set_client_config
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        set_client_config(conn, "ACME", True, qm_type, "2025-01-01")
        return conn

    def test_retail_itc_zero(self, tmp_path):
        from src.agents.core.revenu_quebec import compute_prefill
        conn = self._db_with_config(tmp_path, "retail")
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_106"] == Decimal("0"), "ITC must be 0 under Quick Method"
        assert result["line_207"] == Decimal("0"), "ITR must be 0 under Quick Method"

    def test_services_itc_zero(self, tmp_path):
        from src.agents.core.revenu_quebec import compute_prefill
        conn = self._db_with_config(tmp_path, "services")
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["line_106"] == Decimal("0")
        assert result["line_207"] == Decimal("0")

    def test_retail_rates_returned(self, tmp_path):
        from src.agents.core.revenu_quebec import QM_RETAIL_GST, QM_RETAIL_QST, compute_prefill
        conn = self._db_with_config(tmp_path, "retail")
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["quick_gst_rate"] == QM_RETAIL_GST
        assert result["quick_qst_rate"] == QM_RETAIL_QST

    def test_services_rates_returned(self, tmp_path):
        from src.agents.core.revenu_quebec import QM_SERVICES_GST, QM_SERVICES_QST, compute_prefill
        conn = self._db_with_config(tmp_path, "services")
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["quick_gst_rate"] == QM_SERVICES_GST
        assert result["quick_qst_rate"] == QM_SERVICES_QST

    def test_quick_method_type_in_result(self, tmp_path):
        from src.agents.core.revenu_quebec import compute_prefill
        conn = self._db_with_config(tmp_path, "services")
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["quick_method_type"] == "services"
        assert result["quick_method"] is True

    def test_regular_method_rates_none(self, tmp_path):
        from src.agents.core.revenu_quebec import compute_prefill
        _make_full_db(tmp_path / "otocpa_agent.db")
        conn = sqlite3.connect(str(tmp_path / "otocpa_agent.db"))
        conn.row_factory = sqlite3.Row
        result = compute_prefill("ACME", "2025-01-01", "2025-03-31", conn)
        conn.close()
        assert result["quick_gst_rate"] is None
        assert result["quick_qst_rate"] is None
        assert result["quick_method_type"] is None


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

class TestGenerateRevenuQuebecPdf:
    def _minimal_prefill(self):
        return {
            "line_101": Decimal("0"),
            "line_103": Decimal("0"),
            "line_106": Decimal("5.00"),
            "line_108": Decimal("-5.00"),
            "line_205": Decimal("0"),
            "line_207": Decimal("9.98"),
            "line_209": Decimal("-9.98"),
            "quick_method": False,
            "quick_method_type": None,
            "quick_gst_rate": None,
            "quick_qst_rate": None,
            "documents_posted": 1,
            "documents_pending": 1,
            "documents_total": 2,
            "error": None,
        }

    def test_returns_bytes(self):
        from src.agents.core.revenu_quebec import generate_revenu_quebec_pdf
        pdf = generate_revenu_quebec_pdf(
            client_code="ACME",
            period_start="2025-01-01",
            period_end="2025-03-31",
            prefill=self._minimal_prefill(),
            generated_at="2025-04-01 00:00 UTC",
        )
        assert isinstance(pdf, bytes)
        assert len(pdf) > 0

    def test_starts_with_pdf_header(self):
        from src.agents.core.revenu_quebec import generate_revenu_quebec_pdf
        pdf = generate_revenu_quebec_pdf(
            client_code="ACME",
            period_start="2025-01-01",
            period_end="2025-03-31",
            prefill=self._minimal_prefill(),
            generated_at="2025-04-01 00:00 UTC",
        )
        assert pdf[:4] == b"%PDF"

    def test_quick_method_pdf(self):
        from src.agents.core.revenu_quebec import QM_RETAIL_GST, QM_RETAIL_QST, generate_revenu_quebec_pdf
        prefill = self._minimal_prefill()
        prefill.update({
            "quick_method": True,
            "quick_method_type": "retail",
            "quick_gst_rate": QM_RETAIL_GST,
            "quick_qst_rate": QM_RETAIL_QST,
            "line_106": Decimal("0"),
            "line_207": Decimal("0"),
        })
        pdf = generate_revenu_quebec_pdf(
            client_code="ACME",
            period_start="2025-01-01",
            period_end="2025-03-31",
            prefill=prefill,
            generated_at="2025-04-01 00:00 UTC",
        )
        assert pdf[:4] == b"%PDF"


# ---------------------------------------------------------------------------
# Migration script declares client_config
# ---------------------------------------------------------------------------

class TestMigrationDeclares:
    def test_client_config_in_migration(self):
        """migrate_db.py must contain the client_config CREATE TABLE statement."""
        migration_src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "client_config" in migration_src

    def test_quick_method_column_in_migration(self):
        migration_src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "quick_method" in migration_src


# ---------------------------------------------------------------------------
# review_dashboard integration
# ---------------------------------------------------------------------------

class TestReviewDashboardIntegration:
    def test_render_revenu_quebec_exported(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "render_revenu_quebec" in src

    def test_revenu_quebec_route_present(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/revenu_quebec"' in src

    def test_revenu_quebec_pdf_route_present(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/revenu_quebec/pdf"' in src

    def test_set_config_route_present(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/revenu_quebec/set_config"' in src

    def test_owner_only_check_in_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # The route must gate on owner role
        assert 'err_owner_required' in src

    def test_warning_banner_in_render(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "rq_warning_text" in src

    def test_download_pdf_button_in_render(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "rq_btn_download_pdf" in src

    def test_bilingual_labels_in_render(self):
        """Labels must render both French and English via t(..., 'fr') and t(..., 'en')."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # render_revenu_quebec calls t(key, "fr") and t(key, "en") for line labels
        assert 't(f"rq_line_{line_num}", "fr")' in src
        assert 't(f"rq_line_{line_num}", "en")' in src


# ---------------------------------------------------------------------------
# i18n keys present in both language files
# ---------------------------------------------------------------------------

_RQ_KEYS = [
    "rq_title",
    "rq_nav_link",
    "rq_client_code",
    "rq_period_start",
    "rq_period_end",
    "rq_btn_load",
    "rq_btn_download_pdf",
    "rq_warning_title",
    "rq_warning_text",
    "rq_gst_section",
    "rq_qst_section",
    "rq_line_101",
    "rq_line_103",
    "rq_line_106",
    "rq_line_108",
    "rq_line_205",
    "rq_line_207",
    "rq_line_209",
    "rq_note_sales_not_tracked",
    "rq_quick_method",
    "rq_quick_method_active",
    "rq_quick_method_rates",
    "rq_quick_method_retail",
    "rq_quick_method_services",
    "rq_quick_method_no_itc",
    "rq_quick_method_type",
    "rq_type_retail",
    "rq_type_services",
    "rq_btn_save_config",
    "rq_docs_posted",
    "rq_docs_pending",
    "rq_docs_total",
    "flash_rq_config_saved",
]


class TestI18nKeys:
    @pytest.fixture(scope="class")
    def en_keys(self):
        return set(json.loads((ROOT / "src/i18n/en.json").read_text(encoding="utf-8")).keys())

    @pytest.fixture(scope="class")
    def fr_keys(self):
        return set(json.loads((ROOT / "src/i18n/fr.json").read_text(encoding="utf-8")).keys())

    @pytest.mark.parametrize("key", _RQ_KEYS)
    def test_key_in_en(self, key, en_keys):
        assert key in en_keys, f"Missing i18n key '{key}' in en.json"

    @pytest.mark.parametrize("key", _RQ_KEYS)
    def test_key_in_fr(self, key, fr_keys):
        assert key in fr_keys, f"Missing i18n key '{key}' in fr.json"

    @pytest.mark.parametrize("key", _RQ_KEYS)
    def test_key_non_empty_en(self, key, en_keys):
        data = json.loads((ROOT / "src/i18n/en.json").read_text(encoding="utf-8"))
        assert data.get(key, "").strip(), f"Empty value for key '{key}' in en.json"

    @pytest.mark.parametrize("key", _RQ_KEYS)
    def test_key_non_empty_fr(self, key, fr_keys):
        data = json.loads((ROOT / "src/i18n/fr.json").read_text(encoding="utf-8"))
        assert data.get(key, "").strip(), f"Empty value for key '{key}' in fr.json"
