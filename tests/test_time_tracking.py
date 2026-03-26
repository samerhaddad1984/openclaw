"""
tests/test_time_tracking.py — pytest tests for time tracking and invoice generation.

Covers:
  - time_tracker: table creation, start/stop entries, summary queries
  - invoice_generator: invoice number format, PDF generation (GST/QST compliance)
  - migrate_db.py: declares time_entries and invoices tables
  - review_dashboard.py: /time route, /time/start, /time/stop, /invoice/generate present
  - i18n: all time tracking and invoice keys present in both locales
"""
from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mem() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# time_tracker: table creation
# ---------------------------------------------------------------------------

class TestEnsureTimeTables:
    def test_creates_time_entries(self):
        from src.agents.core.time_tracker import ensure_time_tables
        conn = _mem()
        ensure_time_tables(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "time_entries" in tables

    def test_creates_invoices(self):
        from src.agents.core.time_tracker import ensure_time_tables
        conn = _mem()
        ensure_time_tables(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "invoices" in tables

    def test_idempotent(self):
        from src.agents.core.time_tracker import ensure_time_tables
        conn = _mem()
        ensure_time_tables(conn)
        ensure_time_tables(conn)  # must not raise

    def test_time_entries_columns(self):
        from src.agents.core.time_tracker import ensure_time_tables
        conn = _mem()
        ensure_time_tables(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(time_entries)")}
        for col in ["entry_id", "username", "client_code", "document_id",
                    "started_at", "ended_at", "duration_minutes",
                    "description", "billable", "hourly_rate"]:
            assert col in cols, f"time_entries missing column: {col}"

    def test_invoices_columns(self):
        from src.agents.core.time_tracker import ensure_time_tables
        conn = _mem()
        ensure_time_tables(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(invoices)")}
        for col in ["invoice_id", "client_code", "period_start", "period_end",
                    "generated_by", "generated_at", "hourly_rate", "subtotal",
                    "gst_amount", "qst_amount", "total_amount", "entry_count"]:
            assert col in cols, f"invoices missing column: {col}"


# ---------------------------------------------------------------------------
# time_tracker: start_time_entry
# ---------------------------------------------------------------------------

class TestStartTimeEntry:
    def test_returns_integer(self):
        from src.agents.core.time_tracker import start_time_entry
        conn = _mem()
        eid = start_time_entry(conn, "alice", "ACME", "doc-001")
        assert isinstance(eid, int)
        assert eid > 0

    def test_increments_entry_id(self):
        from src.agents.core.time_tracker import start_time_entry
        conn = _mem()
        e1 = start_time_entry(conn, "alice", "ACME")
        e2 = start_time_entry(conn, "bob", "ACME")
        assert e2 > e1

    def test_entry_stored_in_db(self):
        from src.agents.core.time_tracker import start_time_entry
        conn = _mem()
        eid = start_time_entry(conn, "alice", "ACME", "doc-999")
        row = conn.execute(
            "SELECT * FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert row is not None
        assert row["username"] == "alice"
        assert row["client_code"] == "ACME"
        assert row["document_id"] == "doc-999"
        assert row["started_at"] is not None
        assert row["duration_minutes"] is None  # not yet stopped

    def test_billable_defaults_to_1(self):
        from src.agents.core.time_tracker import start_time_entry
        conn = _mem()
        eid = start_time_entry(conn, "alice", "ACME")
        row = conn.execute(
            "SELECT billable FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert row["billable"] == 1

    def test_document_id_optional(self):
        from src.agents.core.time_tracker import start_time_entry
        conn = _mem()
        eid = start_time_entry(conn, "alice", "ACME")
        row = conn.execute(
            "SELECT document_id FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert row["document_id"] is None


# ---------------------------------------------------------------------------
# time_tracker: stop_time_entry
# ---------------------------------------------------------------------------

class TestStopTimeEntry:
    def _setup(self) -> tuple[sqlite3.Connection, int]:
        from src.agents.core.time_tracker import start_time_entry
        conn = _mem()
        eid = start_time_entry(conn, "alice", "ACME", "doc-001")
        return conn, eid

    def test_sets_duration(self):
        from src.agents.core.time_tracker import stop_time_entry
        conn, eid = self._setup()
        stop_time_entry(conn, eid, 15.5)
        row = conn.execute(
            "SELECT duration_minutes FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert abs(row["duration_minutes"] - 15.5) < 0.001

    def test_sets_ended_at(self):
        from src.agents.core.time_tracker import stop_time_entry
        conn, eid = self._setup()
        stop_time_entry(conn, eid, 5.0)
        row = conn.execute(
            "SELECT ended_at FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert row["ended_at"] is not None

    def test_sets_description(self):
        from src.agents.core.time_tracker import stop_time_entry
        conn, eid = self._setup()
        stop_time_entry(conn, eid, 10.0, description="Reviewed invoice")
        row = conn.execute(
            "SELECT description FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert row["description"] == "Reviewed invoice"

    def test_negative_duration_clamped_to_zero(self):
        from src.agents.core.time_tracker import stop_time_entry
        conn, eid = self._setup()
        stop_time_entry(conn, eid, -5.0)
        row = conn.execute(
            "SELECT duration_minutes FROM time_entries WHERE entry_id=?", (eid,)
        ).fetchone()
        assert row["duration_minutes"] == 0.0


# ---------------------------------------------------------------------------
# time_tracker: get_entries_for_period
# ---------------------------------------------------------------------------

class TestGetEntriesForPeriod:
    def _populate(self) -> sqlite3.Connection:
        from src.agents.core.time_tracker import start_time_entry, stop_time_entry
        conn = _mem()
        # Two entries in Jan 2025
        e1 = start_time_entry(conn, "alice", "ACME", None)
        stop_time_entry(conn, e1, 60.0)
        # Manually set started_at to Jan 2025
        conn.execute(
            "UPDATE time_entries SET started_at=? WHERE entry_id=?",
            ("2025-01-10T09:00:00", e1),
        )
        e2 = start_time_entry(conn, "bob", "ACME", None)
        stop_time_entry(conn, e2, 30.0)
        conn.execute(
            "UPDATE time_entries SET started_at=? WHERE entry_id=?",
            ("2025-01-20T14:00:00", e2),
        )
        # One entry in Feb 2025
        e3 = start_time_entry(conn, "alice", "ACME", None)
        stop_time_entry(conn, e3, 45.0)
        conn.execute(
            "UPDATE time_entries SET started_at=? WHERE entry_id=?",
            ("2025-02-05T10:00:00", e3),
        )
        conn.commit()
        return conn

    def test_returns_jan_entries(self):
        from src.agents.core.time_tracker import get_entries_for_period
        conn = self._populate()
        entries = get_entries_for_period(conn, "ACME", "2025-01-01", "2025-01-31")
        assert len(entries) == 2

    def test_excludes_feb_entry_from_jan_query(self):
        from src.agents.core.time_tracker import get_entries_for_period
        conn = self._populate()
        entries = get_entries_for_period(conn, "ACME", "2025-01-01", "2025-01-31")
        for e in entries:
            assert e["started_at"].startswith("2025-01")

    def test_client_code_case_insensitive(self):
        from src.agents.core.time_tracker import get_entries_for_period
        conn = self._populate()
        entries = get_entries_for_period(conn, "acme", "2025-01-01", "2025-01-31")
        assert len(entries) == 2

    def test_different_client_returns_empty(self):
        from src.agents.core.time_tracker import get_entries_for_period
        conn = self._populate()
        entries = get_entries_for_period(conn, "OTHER", "2025-01-01", "2025-01-31")
        assert len(entries) == 0

    def test_excludes_entries_without_duration(self):
        from src.agents.core.time_tracker import get_entries_for_period, start_time_entry
        conn = self._populate()
        # Open (no duration) entry in Jan 2025
        eid = start_time_entry(conn, "charlie", "ACME")
        conn.execute(
            "UPDATE time_entries SET started_at=? WHERE entry_id=?",
            ("2025-01-15T08:00:00", eid),
        )
        conn.commit()
        entries = get_entries_for_period(conn, "ACME", "2025-01-01", "2025-01-31")
        assert len(entries) == 2  # open entry excluded


# ---------------------------------------------------------------------------
# time_tracker: get_time_summary
# ---------------------------------------------------------------------------

class TestGetTimeSummary:
    def _conn_with_entries(self) -> sqlite3.Connection:
        from src.agents.core.time_tracker import start_time_entry, stop_time_entry
        conn = _mem()
        e1 = start_time_entry(conn, "alice", "ACME")
        stop_time_entry(conn, e1, 90.0)  # 1.5 h
        conn.execute("UPDATE time_entries SET started_at=? WHERE entry_id=?",
                     ("2025-03-01T09:00:00", e1))
        e2 = start_time_entry(conn, "bob", "ACME")
        stop_time_entry(conn, e2, 30.0)  # 0.5 h
        conn.execute("UPDATE time_entries SET started_at=? WHERE entry_id=?",
                     ("2025-03-15T10:00:00", e2))
        conn.commit()
        return conn

    def test_total_hours(self):
        from src.agents.core.time_tracker import get_time_summary
        conn = self._conn_with_entries()
        s = get_time_summary(conn, "ACME", "2025-03-01", "2025-03-31")
        assert s["total_hours"] == pytest.approx(2.0, rel=1e-3)

    def test_billable_hours(self):
        from src.agents.core.time_tracker import get_time_summary
        conn = self._conn_with_entries()
        s = get_time_summary(conn, "ACME", "2025-03-01", "2025-03-31")
        assert s["billable_hours"] == pytest.approx(2.0, rel=1e-3)

    def test_entry_count(self):
        from src.agents.core.time_tracker import get_time_summary
        conn = self._conn_with_entries()
        s = get_time_summary(conn, "ACME", "2025-03-01", "2025-03-31")
        assert s["entry_count"] == 2

    def test_by_user_breakdown(self):
        from src.agents.core.time_tracker import get_time_summary
        conn = self._conn_with_entries()
        s = get_time_summary(conn, "ACME", "2025-03-01", "2025-03-31")
        assert "alice" in s["by_user"]
        assert "bob" in s["by_user"]
        assert s["by_user"]["alice"]["total_minutes"] == pytest.approx(90.0, rel=1e-3)
        assert s["by_user"]["bob"]["total_minutes"] == pytest.approx(30.0, rel=1e-3)

    def test_empty_period_returns_zeros(self):
        from src.agents.core.time_tracker import get_time_summary
        conn = self._conn_with_entries()
        s = get_time_summary(conn, "ACME", "2024-01-01", "2024-01-31")
        assert s["entry_count"] == 0
        assert s["total_hours"] == 0.0
        assert s["billable_hours"] == 0.0

    def test_returns_entries_list(self):
        from src.agents.core.time_tracker import get_time_summary
        conn = self._conn_with_entries()
        s = get_time_summary(conn, "ACME", "2025-03-01", "2025-03-31")
        assert isinstance(s["entries"], list)
        assert len(s["entries"]) == 2


# ---------------------------------------------------------------------------
# invoice_generator: invoice number
# ---------------------------------------------------------------------------

class TestGenerateInvoiceNumber:
    def test_starts_with_inv(self):
        from src.agents.core.invoice_generator import generate_invoice_number
        num = generate_invoice_number()
        assert num.startswith("INV-")

    def test_length_reasonable(self):
        from src.agents.core.invoice_generator import generate_invoice_number
        num = generate_invoice_number()
        assert 15 <= len(num) <= 25

    def test_unique_each_call(self):
        from src.agents.core.invoice_generator import generate_invoice_number
        nums = {generate_invoice_number() for _ in range(10)}
        assert len(nums) == 10


# ---------------------------------------------------------------------------
# invoice_generator: generate_invoice_pdf
# ---------------------------------------------------------------------------

class TestGenerateInvoicePdf:
    def _make_pdf(self, lang: str = "en", hours: str = "2.0") -> bytes:
        from src.agents.core.invoice_generator import generate_invoice_pdf
        return generate_invoice_pdf(
            invoice_number="INV-20250301-TEST01",
            invoice_date="2025-03-01",
            firm_name="Fiducie Comptable inc.",
            gst_number="123 456 789 RT 0001",
            qst_number="1234567890 TQ 0001",
            client_name="ACME Corp",
            client_code="ACME",
            period_start="2025-01-01",
            period_end="2025-01-31",
            hourly_rate=Decimal("150.00"),
            billable_hours=Decimal(hours),
            entries=[
                {"username": "alice", "duration_minutes": 60.0, "description": "Bank rec"},
                {"username": "bob",   "duration_minutes": 60.0, "description": "Review"},
            ],
            lang=lang,
        )

    def test_returns_bytes(self):
        pdf = self._make_pdf()
        assert isinstance(pdf, bytes)
        assert len(pdf) > 100

    def test_starts_with_pdf_header(self):
        pdf = self._make_pdf()
        assert pdf.startswith(b"%PDF")

    def test_works_in_english(self):
        pdf = self._make_pdf(lang="en")
        assert pdf.startswith(b"%PDF")

    def test_works_in_french(self):
        pdf = self._make_pdf(lang="fr")
        assert pdf.startswith(b"%PDF")

    def test_tax_math_gst_5pct(self):
        """GST must be exactly 5% of subtotal."""
        from src.engines.tax_engine import calculate_gst_qst
        subtotal = Decimal("150.00") * Decimal("2.0")
        tax = calculate_gst_qst(subtotal)
        assert tax["gst"] == Decimal("15.00")

    def test_tax_math_qst_9975pct(self):
        """QST must be exactly 9.975% of subtotal."""
        from src.engines.tax_engine import calculate_gst_qst
        subtotal = Decimal("150.00") * Decimal("2.0")
        tax = calculate_gst_qst(subtotal)
        assert tax["qst"] == Decimal("29.93")

    def test_tax_math_total(self):
        """Total = subtotal + GST + QST."""
        from src.engines.tax_engine import calculate_gst_qst
        subtotal = Decimal("300.00")
        tax = calculate_gst_qst(subtotal)
        assert tax["total_with_tax"] == subtotal + tax["gst"] + tax["qst"]

    def test_zero_hours_returns_pdf(self):
        """Even zero-hour invoices produce valid PDF bytes (edge case)."""
        pdf = self._make_pdf(hours="0.0")
        assert pdf.startswith(b"%PDF")


# ---------------------------------------------------------------------------
# migrate_db.py declares tables
# ---------------------------------------------------------------------------

class TestMigrateDbDeclaresTimeTables:
    def test_declares_time_entries(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "time_entries" in src

    def test_declares_invoices(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "invoices" in src

    def test_time_entries_columns_declared(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        for col in ["entry_id", "username", "client_code", "duration_minutes", "billable"]:
            assert col in src, f"migrate_db.py missing column reference: {col}"


# ---------------------------------------------------------------------------
# review_dashboard.py: routes present
# ---------------------------------------------------------------------------

class TestReviewDashboardTimeRoutes:
    def _src(self) -> str:
        return (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")

    def test_time_get_route(self):
        assert '"/time"' in self._src()

    def test_time_start_post_route(self):
        assert '"/time/start"' in self._src()

    def test_time_stop_post_route(self):
        assert '"/time/stop"' in self._src()

    def test_invoice_generate_post_route(self):
        assert '"/invoice/generate"' in self._src()

    def test_render_time_summary_exported(self):
        from scripts.review_dashboard import render_time_summary
        assert callable(render_time_summary)

    def test_render_time_summary_accepts_lang(self):
        import inspect
        from scripts.review_dashboard import render_time_summary
        sig = inspect.signature(render_time_summary)
        assert "lang" in sig.parameters

    def test_time_nav_btn_in_render_home(self):
        assert "time_nav_link" in self._src()

    def test_timer_js_injected_in_document(self):
        assert "time/start" in self._src()
        assert "time/stop" in self._src()
        assert "doc-timer" in self._src()

    def test_send_json_helper_present(self):
        assert "_send_json" in self._src()

    def test_calculate_gst_qst_imported(self):
        assert "calculate_gst_qst" in self._src()

    def test_manager_owner_check_for_time(self):
        src = self._src()
        # /time route should be guarded by manager/owner check
        assert 'not in ("manager", "owner")' in src


# ---------------------------------------------------------------------------
# i18n: all time tracking and invoice keys
# ---------------------------------------------------------------------------

class TestTimeTrackingI18nKeys:
    REQUIRED_KEYS = [
        "time_title", "time_nav_link", "time_client_code",
        "time_period_start", "time_period_end",
        "time_total_hours", "time_billable_hours", "time_estimated_fee",
        "time_hourly_rate", "time_user", "time_no_entries",
        "time_generate_invoice", "time_firm_name", "time_client_name",
        "time_gst_number", "time_qst_number", "flash_invoice_generated",
        "inv_title", "inv_bill_to", "inv_services", "inv_period",
        "inv_description", "inv_hours", "inv_rate", "inv_amount",
        "inv_subtotal", "inv_gst", "inv_qst", "inv_total",
        "inv_invoice_number", "inv_invoice_date", "inv_reg_gst", "inv_reg_qst",
    ]

    def _load(self, lang: str) -> dict:
        path = ROOT / "src" / "i18n" / f"{lang}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_en_has_all_time_keys(self):
        data = self._load("en")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"en.json missing time/invoice key: {key!r}"

    def test_fr_has_all_time_keys(self):
        data = self._load("fr")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"fr.json missing time/invoice key: {key!r}"

    def test_translations_are_strings(self):
        from src.i18n import reload_cache, t
        reload_cache()
        for key in self.REQUIRED_KEYS:
            en_val = t(key, "en")
            fr_val = t(key, "fr")
            assert isinstance(en_val, str) and en_val
            assert isinstance(fr_val, str) and fr_val

    def test_key_not_returned_as_translation(self):
        """Ensure translations are real strings, not fallback key echoes."""
        from src.i18n import reload_cache, t
        reload_cache()
        for key in self.REQUIRED_KEYS:
            assert t(key, "en") != key, f"en.json missing translation for {key!r}"
            assert t(key, "fr") != key, f"fr.json missing translation for {key!r}"
