"""
tests/test_filing_calendar.py — pytest tests for the GST/QST filing calendar.

Covers:
  - DB table creation (ensure_filing_tables)
  - Deadline calculation: monthly, quarterly, annual
  - get_upcoming_deadlines: filtering by window, sorting, filed flag
  - mark_as_filed: records + idempotent upsert
  - period_label_to_dates: monthly, quarterly, annual
  - migrate_db declares the new client_config columns and gst_filings table
  - review_dashboard exposes render_calendar and /calendar route
  - i18n keys present for all calendar strings
  - daily_digest get_filing_deadlines_14_days + build_plain_text + build_html_body
"""
from __future__ import annotations

import inspect
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _db_with_client(freq: str = "monthly", fy_end: str = "12-31") -> sqlite3.Connection:
    conn = _db()
    conn.execute("""
        CREATE TABLE client_config (
            client_code               TEXT PRIMARY KEY,
            quick_method              INTEGER NOT NULL DEFAULT 0,
            quick_method_type         TEXT    NOT NULL DEFAULT 'retail',
            updated_at                TEXT,
            filing_frequency          TEXT    NOT NULL DEFAULT 'monthly',
            gst_registration_number   TEXT,
            qst_registration_number   TEXT,
            fiscal_year_end           TEXT    NOT NULL DEFAULT '12-31'
        )
    """)
    conn.execute(
        "INSERT INTO client_config (client_code, filing_frequency, fiscal_year_end) VALUES (?,?,?)",
        ("TESTCO", freq, fy_end),
    )
    conn.commit()
    return conn


# ===========================================================================
# 1. Table creation
# ===========================================================================

class TestEnsureFilingTables:
    def test_creates_gst_filings_table(self):
        from src.agents.core.filing_calendar import ensure_filing_tables
        conn = _db()
        ensure_filing_tables(conn)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "gst_filings" in tables

    def test_idempotent(self):
        from src.agents.core.filing_calendar import ensure_filing_tables
        conn = _db()
        ensure_filing_tables(conn)
        ensure_filing_tables(conn)  # must not raise

    def test_columns_exist(self):
        from src.agents.core.filing_calendar import ensure_filing_tables
        conn = _db()
        ensure_filing_tables(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(gst_filings)")}
        assert {"client_code", "period_label", "deadline", "filed_at", "filed_by"} <= cols


# ===========================================================================
# 2. Deadline arithmetic
# ===========================================================================

class TestDeadlineArithmetic:
    def test_monthly_jan(self):
        from src.agents.core.filing_calendar import _monthly_deadline
        d = _monthly_deadline(2026, 1)
        assert d == date(2026, 2, 28)

    def test_monthly_dec_wraps_year(self):
        from src.agents.core.filing_calendar import _monthly_deadline
        d = _monthly_deadline(2025, 12)
        assert d == date(2026, 1, 31)

    def test_monthly_feb(self):
        from src.agents.core.filing_calendar import _monthly_deadline
        d = _monthly_deadline(2026, 2)
        assert d == date(2026, 3, 31)

    def test_quarterly_q1(self):
        from src.agents.core.filing_calendar import _quarterly_deadline
        d = _quarterly_deadline(2026, 1)
        assert d == date(2026, 4, 30)

    def test_quarterly_q2(self):
        from src.agents.core.filing_calendar import _quarterly_deadline
        d = _quarterly_deadline(2026, 2)
        assert d == date(2026, 7, 31)

    def test_quarterly_q3(self):
        from src.agents.core.filing_calendar import _quarterly_deadline
        d = _quarterly_deadline(2026, 3)
        assert d == date(2026, 10, 31)

    def test_quarterly_q4_wraps_year(self):
        from src.agents.core.filing_calendar import _quarterly_deadline
        d = _quarterly_deadline(2025, 4)
        assert d == date(2026, 1, 31)

    def test_annual_dec31_fiscal(self):
        from src.agents.core.filing_calendar import _annual_deadline
        # FY2025 ends Dec 31 2025 → deadline Mar 31 2026
        d = _annual_deadline(12, 2025)
        assert d == date(2026, 3, 31)

    def test_annual_mar31_fiscal(self):
        from src.agents.core.filing_calendar import _annual_deadline
        # FY ending Mar 31 2025 → deadline Jun 30 2025
        d = _annual_deadline(3, 2025)
        assert d == date(2025, 6, 30)

    def test_annual_sep30_wraps(self):
        from src.agents.core.filing_calendar import _annual_deadline
        # FY ending Sep 30 2025 → deadline Dec 31 2025
        d = _annual_deadline(9, 2025)
        assert d == date(2025, 12, 31)


# ===========================================================================
# 3. period_label_to_dates
# ===========================================================================

class TestPeriodLabelToDates:
    def test_monthly(self):
        from src.agents.core.filing_calendar import period_label_to_dates
        start, end = period_label_to_dates("2026-02")
        assert start == "2026-02-01"
        assert end   == "2026-02-28"

    def test_monthly_dec(self):
        from src.agents.core.filing_calendar import period_label_to_dates
        start, end = period_label_to_dates("2026-12")
        assert start == "2026-12-01"
        assert end   == "2026-12-31"

    def test_quarterly_q1(self):
        from src.agents.core.filing_calendar import period_label_to_dates
        start, end = period_label_to_dates("2026-Q1")
        assert start == "2026-01-01"
        assert end   == "2026-03-31"

    def test_quarterly_q4(self):
        from src.agents.core.filing_calendar import period_label_to_dates
        start, end = period_label_to_dates("2025-Q4")
        assert start == "2025-10-01"
        assert end   == "2025-12-31"

    def test_annual_dec31(self):
        from src.agents.core.filing_calendar import period_label_to_dates
        start, end = period_label_to_dates("FY2025", fiscal_year_end="12-31")
        assert end   == "2025-12-31"
        assert start  < end  # start should be before end

    def test_annual_mar31(self):
        from src.agents.core.filing_calendar import period_label_to_dates
        start, end = period_label_to_dates("FY2025", fiscal_year_end="03-31")
        assert end == "2025-03-31"


# ===========================================================================
# 4. mark_as_filed
# ===========================================================================

class TestMarkAsFiled:
    def test_inserts_row(self):
        from src.agents.core.filing_calendar import mark_as_filed, ensure_filing_tables
        conn = _db()
        ensure_filing_tables(conn)
        mark_as_filed(conn, "ACME", "2026-02", "2026-03-31", "alice", "2026-03-15T10:00:00")
        row = conn.execute(
            "SELECT * FROM gst_filings WHERE client_code='ACME' AND period_label='2026-02'"
        ).fetchone()
        assert row is not None
        assert row["filed_by"] == "alice"
        assert row["filed_at"] == "2026-03-15T10:00:00"

    def test_upsert_updates_filed_at(self):
        from src.agents.core.filing_calendar import mark_as_filed, ensure_filing_tables
        conn = _db()
        ensure_filing_tables(conn)
        mark_as_filed(conn, "ACME", "2026-02", "2026-03-31", "alice", "2026-03-10T10:00:00")
        mark_as_filed(conn, "ACME", "2026-02", "2026-03-31", "bob",   "2026-03-15T12:00:00")
        rows = conn.execute(
            "SELECT COUNT(*) FROM gst_filings WHERE client_code='ACME' AND period_label='2026-02'"
        ).fetchone()[0]
        assert rows == 1
        row = conn.execute(
            "SELECT filed_by FROM gst_filings WHERE client_code='ACME' AND period_label='2026-02'"
        ).fetchone()
        assert row["filed_by"] == "bob"


# ===========================================================================
# 5. get_upcoming_deadlines
# ===========================================================================

class TestGetUpcomingDeadlines:
    def test_monthly_returns_deadline_in_window(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        # Set as_of to a date where Feb's deadline (Mar 31) is within 90 days
        as_of = date(2026, 3, 1)
        conn = _db_with_client("monthly")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        labels = [r["period_label"] for r in results]
        # Period 2026-02 has deadline 2026-03-31 → within window
        assert "2026-02" in labels

    def test_quarterly_returns_q1_deadline(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        # as_of=2026-03-19, Q1 deadline=Apr 30 (42 days away)
        as_of = date(2026, 3, 19)
        conn = _db_with_client("quarterly")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        labels = [r["period_label"] for r in results]
        assert "2026-Q1" in labels

    def test_annual_dec31_fy2025(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        # FY2025 (Dec 31) → deadline Mar 31 2026
        as_of = date(2026, 3, 1)
        conn = _db_with_client("annual", "12-31")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        labels = [r["period_label"] for r in results]
        assert "FY2025" in labels

    def test_filed_period_shows_is_filed_true(self):
        from src.agents.core.filing_calendar import (
            get_upcoming_deadlines, mark_as_filed, ensure_filing_tables,
        )
        as_of = date(2026, 3, 1)
        conn = _db_with_client("monthly")
        ensure_filing_tables(conn)
        mark_as_filed(conn, "TESTCO", "2026-02", "2026-03-31", "alice", "2026-03-10T09:00:00")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        for r in results:
            if r["period_label"] == "2026-02":
                assert r["is_filed"] is True
                assert r["filed_by"] == "alice"
                break

    def test_days_until_calculated_correctly(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        as_of = date(2026, 3, 1)
        conn = _db_with_client("monthly")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        for r in results:
            if r["period_label"] == "2026-02":
                # Deadline is Mar 31; as_of is Mar 1 → 30 days
                assert r["days_until"] == 30
                break

    def test_results_sorted_by_deadline(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        as_of = date(2026, 3, 1)
        conn = _db_with_client("monthly")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        deadlines = [r["deadline"] for r in results]
        assert deadlines == sorted(deadlines)

    def test_empty_client_config_returns_empty(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        conn = _db()
        conn.execute("""
            CREATE TABLE client_config (
                client_code TEXT PRIMARY KEY,
                filing_frequency TEXT DEFAULT 'monthly',
                fiscal_year_end TEXT DEFAULT '12-31',
                gst_registration_number TEXT,
                qst_registration_number TEXT
            )
        """)
        conn.commit()
        results = get_upcoming_deadlines(conn)
        assert results == []

    def test_deadline_beyond_window_excluded(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        # Monthly client: period 2026-06 has deadline 2026-07-31
        # If as_of=2026-03-19, window_end=2026-06-17, Jul 31 is outside window
        as_of = date(2026, 3, 19)
        conn = _db_with_client("monthly")
        results = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=90)
        for r in results:
            assert r["deadline"] <= (as_of + timedelta(days=90)).isoformat(), (
                f"Deadline {r['deadline']} is outside the 90-day window"
            )

    def test_missing_client_config_table_returns_empty(self):
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        conn = _db()  # No client_config table at all
        results = get_upcoming_deadlines(conn)
        assert results == []


# ===========================================================================
# 6. migrate_db declares new columns and table
# ===========================================================================

class TestMigrateDbDeclarations:
    def test_client_config_has_filing_frequency(self):
        source = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "filing_frequency" in source

    def test_client_config_has_gst_registration_number(self):
        source = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "gst_registration_number" in source

    def test_client_config_has_qst_registration_number(self):
        source = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "qst_registration_number" in source

    def test_client_config_has_fiscal_year_end(self):
        source = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "fiscal_year_end" in source

    def test_gst_filings_table_declared(self):
        source = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "gst_filings" in source


# ===========================================================================
# 7. review_dashboard exposes /calendar route and render_calendar
# ===========================================================================

class TestReviewDashboardCalendar:
    def test_render_calendar_exists(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "render_calendar" in source

    def test_calendar_route_in_do_get(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/calendar"' in source

    def test_mark_filed_route_in_do_post(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/calendar/mark_filed"' in source

    def test_save_config_route_in_do_post(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/calendar/save_config"' in source

    def test_filing_calendar_imported(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "filing_calendar" in source

    def test_render_calendar_uses_page_layout(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # confirm render_calendar calls page_layout
        idx = source.find("def render_calendar(")
        assert idx != -1
        # The function is long — scan up to the next top-level def
        next_def = source.find("\ndef ", idx + 1)
        snippet = source[idx: next_def] if next_def != -1 else source[idx: idx + 8000]
        assert "page_layout" in snippet

    def test_render_calendar_uses_t(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        idx = source.find("def render_calendar(")
        assert idx != -1
        snippet = source[idx: idx + 3000]
        assert 't("cal_' in snippet

    def test_render_calendar_uses_generate_filing_summary(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        idx = source.find("def render_calendar(")
        assert idx != -1
        snippet = source[idx: idx + 3000]
        assert "generate_filing_summary" in snippet

    def test_manager_owner_role_check_in_calendar(self):
        source = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # The /calendar GET handler must check for manager/owner
        idx = source.find('"/calendar"')
        assert idx != -1
        snippet = source[idx: idx + 500]
        assert "manager" in snippet or "owner" in snippet


# ===========================================================================
# 8. i18n keys
# ===========================================================================

class TestCalendarI18nKeys:
    REQUIRED_KEYS = [
        "cal_title", "cal_nav_link", "cal_h1", "cal_subtitle",
        "cal_col_client", "cal_col_period", "cal_col_frequency", "cal_col_deadline",
        "cal_col_days", "cal_col_gst_amount", "cal_col_qst_amount",
        "cal_col_docs_pending", "cal_col_status", "cal_col_action",
        "cal_mark_filed", "cal_status_filed", "cal_filed_at", "cal_filed_by",
        "cal_no_deadlines",
        "cal_freq_monthly", "cal_freq_quarterly", "cal_freq_annual",
        "cal_legend_green", "cal_legend_yellow", "cal_legend_red", "cal_legend_grey",
        "cal_filing_config", "cal_save_config",
        "flash_cal_filed", "flash_cal_config_saved",
    ]

    def _load(self, lang: str) -> dict:
        path = ROOT / "src" / "i18n" / f"{lang}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    @pytest.mark.parametrize("key", REQUIRED_KEYS)
    def test_en_key_present(self, key):
        data = self._load("en")
        assert key in data, f"Missing key '{key}' in en.json"

    @pytest.mark.parametrize("key", REQUIRED_KEYS)
    def test_fr_key_present(self, key):
        data = self._load("fr")
        assert key in data, f"Missing key '{key}' in fr.json"

    def test_en_fr_key_parity(self):
        en = self._load("en")
        fr = self._load("fr")
        cal_en = {k for k in en if k.startswith("cal_") or k.startswith("flash_cal_")}
        cal_fr = {k for k in fr if k.startswith("cal_") or k.startswith("flash_cal_")}
        assert cal_en == cal_fr, f"Key mismatch: EN-only={cal_en - cal_fr}, FR-only={cal_fr - cal_en}"


# ===========================================================================
# 9. daily_digest integration
# ===========================================================================

class TestDailyDigestFiling:
    def test_get_filing_deadlines_14_days_returns_list(self):
        from scripts.daily_digest import get_filing_deadlines_14_days
        result = get_filing_deadlines_14_days()
        assert isinstance(result, list)

    def test_build_plain_text_accepts_filing_deadlines(self):
        from scripts.daily_digest import build_plain_text
        summary = {
            "needs_review": 1, "on_hold": 0, "ready_to_post": 2,
            "posted_today": 3, "stale": 0, "total_active": 3,
        }
        deadlines = [{
            "client_code": "ACME", "period_label": "2026-02",
            "deadline": "2026-03-31", "days_until": 12,
            "docs_pending": 5, "gst_amount": 123.45, "qst_amount": 245.67,
        }]
        text = build_plain_text(summary, "fr", "Alice", filing_deadlines=deadlines)
        assert "ACME" in text
        assert "2026-02" in text

    def test_build_plain_text_filing_section_en(self):
        from scripts.daily_digest import build_plain_text
        summary = {
            "needs_review": 0, "on_hold": 0, "ready_to_post": 0,
            "posted_today": 0, "stale": 0, "total_active": 0,
        }
        deadlines = [{
            "client_code": "CORP", "period_label": "2026-Q1",
            "deadline": "2026-04-30", "days_until": 5,
            "docs_pending": 2, "gst_amount": 500.0, "qst_amount": 997.5,
        }]
        text = build_plain_text(summary, "en", "Bob", filing_deadlines=deadlines)
        assert "CORP" in text
        assert "2026-Q1" in text
        assert "14 DAYS" in text or "14 Days" in text.title() or "UPCOMING" in text.upper()

    def test_build_html_body_accepts_filing_deadlines(self):
        from scripts.daily_digest import build_html_body
        summary = {
            "needs_review": 0, "on_hold": 0, "ready_to_post": 0,
            "posted_today": 1, "stale": 0, "total_active": 0,
        }
        deadlines = [{
            "client_code": "GLOBEX", "period_label": "2026-02",
            "deadline": "2026-03-31", "days_until": 12,
            "docs_pending": 3, "gst_amount": 100.0, "qst_amount": 200.0,
        }]
        html = build_html_body(summary, "fr", "Claire", filing_deadlines=deadlines)
        assert "GLOBEX" in html
        assert "2026-02" in html

    def test_build_plain_text_no_filing_deadlines_is_unchanged(self):
        from scripts.daily_digest import build_plain_text
        summary = {
            "needs_review": 1, "on_hold": 0, "ready_to_post": 0,
            "posted_today": 0, "stale": 0, "total_active": 1,
        }
        text = build_plain_text(summary, "fr", "Test")
        # No filing section when list is empty
        assert "ÉCHÉANCES" not in text

    def test_build_html_body_no_deadlines_no_filing_table(self):
        from scripts.daily_digest import build_html_body
        summary = {
            "needs_review": 0, "on_hold": 0, "ready_to_post": 0,
            "posted_today": 0, "stale": 0, "total_active": 0,
        }
        html = build_html_body(summary, "en", "Test")
        assert "Upcoming Filing Deadlines" not in html

    def test_daily_digest_imports_filing_calendar(self):
        source = (ROOT / "scripts" / "daily_digest.py").read_text(encoding="utf-8")
        assert "filing_calendar" in source

    def test_get_filing_deadlines_exists(self):
        source = (ROOT / "scripts" / "daily_digest.py").read_text(encoding="utf-8")
        assert "get_filing_deadlines_14_days" in source
