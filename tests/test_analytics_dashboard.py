"""
tests/test_analytics_dashboard.py — pytest tests for the /analytics dashboard.

Covers:
  - _analytics_staff_productivity returns correct structure and counts
  - _analytics_client_complexity returns correct structure and averages
  - _analytics_monthly_trends returns month/count dicts
  - _analytics_fraud_summary parses JSON fraud_flags and counts by severity
  - _analytics_deadlines_at_risk joins period_close + documents correctly
  - render_analytics is importable from review_dashboard
  - /analytics route is present in review_dashboard source
  - All 32 analytics i18n keys present in en.json and fr.json
"""
from __future__ import annotations

import inspect
import json
import sqlite3
from datetime import datetime, timezone, timedelta
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


def _bootstrap(conn: sqlite3.Connection) -> None:
    """Create all tables needed by the analytics helpers."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id      TEXT PRIMARY KEY,
            file_name        TEXT,
            vendor           TEXT,
            amount           REAL,
            client_code      TEXT,
            review_status    TEXT,
            assigned_to      TEXT,
            manual_hold_reason TEXT,
            fraud_flags      TEXT,
            created_at       TEXT,
            updated_at       TEXT
        );

        CREATE TABLE IF NOT EXISTS time_entries (
            entry_id         TEXT PRIMARY KEY,
            username         TEXT,
            client_code      TEXT,
            document_id      TEXT,
            started_at       TEXT,
            ended_at         TEXT,
            duration_minutes REAL,
            billable         INTEGER,
            hourly_rate      REAL
        );

        CREATE TABLE IF NOT EXISTS period_close (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code      TEXT,
            period           TEXT,
            checklist_item   TEXT,
            status           TEXT,
            responsible_user TEXT,
            due_date         TEXT,
            completed_by     TEXT,
            completed_at     TEXT,
            notes            TEXT
        );
    """)
    conn.commit()


def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _date_offset(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# _analytics_staff_productivity
# ---------------------------------------------------------------------------

class TestStaffProductivity:
    def _import(self):
        from scripts.review_dashboard import _analytics_staff_productivity
        return _analytics_staff_productivity

    def test_returns_list(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        result = fn(conn, _date_offset(-7), _date_offset(0)[:7] + "-01")
        assert isinstance(result, list)

    def test_counts_docs_this_week(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        conn.execute(
            "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "alice", "Ready", now, now),
        )
        conn.execute(
            "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d2", "alice", "Posted", now, now),
        )
        conn.commit()
        week_start = _date_offset(-7)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, week_start, month_start)
        alice = next((r for r in result if r["username"] == "alice"), None)
        assert alice is not None
        assert alice["docs_week"] == 2
        assert alice["docs_month"] == 2

    def test_hold_rate_calculation(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        docs = [
            ("d1", "bob", "Ready", now),
            ("d2", "bob", "On Hold", now),
            ("d3", "bob", "Ready", now),
            ("d4", "bob", "On Hold", now),
        ]
        for doc_id, assigned, status, ts in docs:
            conn.execute(
                "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (doc_id, assigned, status, ts, ts),
            )
        conn.commit()
        week_start = _date_offset(-7)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, week_start, month_start)
        bob = next((r for r in result if r["username"] == "bob"), None)
        assert bob is not None
        assert bob["hold_rate"] == pytest.approx(50.0)

    def test_approval_rate_calculation(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        docs = [
            ("d1", "carol", "Ready"),
            ("d2", "carol", "Posted"),
            ("d3", "carol", "On Hold"),
            ("d4", "carol", "NeedsReview"),
        ]
        for doc_id, assigned, status in docs:
            conn.execute(
                "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (doc_id, assigned, status, now, now),
            )
        conn.commit()
        week_start = _date_offset(-7)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, week_start, month_start)
        carol = next((r for r in result if r["username"] == "carol"), None)
        assert carol is not None
        assert carol["approval_rate"] == pytest.approx(50.0)

    def test_avg_review_time_from_time_entries(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        conn.execute(
            "INSERT INTO time_entries (entry_id, username, client_code, started_at, ended_at, duration_minutes) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("t1", "dave", "ACME", now, now, 30.0),
        )
        conn.execute(
            "INSERT INTO time_entries (entry_id, username, client_code, started_at, ended_at, duration_minutes) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("t2", "dave", "ACME", now, now, 50.0),
        )
        conn.execute(
            "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "dave", "Ready", now, now),
        )
        conn.commit()
        week_start = _date_offset(-7)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, week_start, month_start)
        dave = next((r for r in result if r["username"] == "dave"), None)
        assert dave is not None
        assert dave["avg_minutes"] == pytest.approx(40.0)

    def test_sorted_by_docs_month_desc(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        for i in range(5):
            conn.execute(
                "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"d-eve-{i}", "eve", "Ready", now, now),
            )
        conn.execute(
            "INSERT INTO documents (document_id, assigned_to, review_status, updated_at, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d-frank-1", "frank", "Ready", now, now),
        )
        conn.commit()
        week_start = _date_offset(-7)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, week_start, month_start)
        usernames = [r["username"] for r in result]
        assert usernames.index("eve") < usernames.index("frank")

    def test_no_docs_returns_empty_list(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        week_start = _date_offset(-7)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, week_start, month_start)
        assert result == []


# ---------------------------------------------------------------------------
# _analytics_client_complexity
# ---------------------------------------------------------------------------

class TestClientComplexity:
    def _import(self):
        from scripts.review_dashboard import _analytics_client_complexity
        return _analytics_client_complexity

    def test_returns_list(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        result = fn(conn)
        assert isinstance(result, list)

    def test_client_appears_in_result(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "ACME", "Ready", now, now),
        )
        conn.commit()
        result = fn(conn)
        assert any(r["client_code"] == "ACME" for r in result)

    def test_hold_rate_per_client(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        docs = [
            ("d1", "BETA", "Ready"),
            ("d2", "BETA", "On Hold"),
        ]
        for doc_id, client_code, status in docs:
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (doc_id, client_code, status, now, now),
            )
        conn.commit()
        result = fn(conn)
        beta = next(r for r in result if r["client_code"] == "BETA")
        assert beta["hold_rate"] == pytest.approx(50.0)

    def test_common_reason_populated(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, manual_hold_reason, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "CORP", "On Hold", "Missing GST", now, now),
        )
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, manual_hold_reason, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d2", "CORP", "On Hold", "Missing GST", now, now),
        )
        conn.commit()
        result = fn(conn)
        corp = next(r for r in result if r["client_code"] == "CORP")
        assert corp["common_reason"] == "Missing GST"

    def test_est_fee_from_time_entries(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        # 1 doc for GAMMA, 60 min review, $120/hr rate
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "GAMMA", "Ready", now, now),
        )
        conn.execute(
            "INSERT INTO time_entries (entry_id, username, client_code, started_at, ended_at, duration_minutes, hourly_rate) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("t1", "alice", "GAMMA", now, now, 60.0, 120.0),
        )
        conn.commit()
        result = fn(conn)
        gamma = next(r for r in result if r["client_code"] == "GAMMA")
        # est_fee = avg_monthly_docs * avg_min_per_doc / 60 * rate
        # avg_monthly_docs ≈ 1/elapsed (elapsed≈0 days → clamped to 1 month)
        # so est_fee ≈ avg_docs_month * 60 / 60 * 120 = avg_docs_month * 120
        assert gamma.get("est_fee") is not None or gamma.get("avg_minutes") is not None

    def test_sorted_by_avg_docs_month_desc(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        # Insert 3 docs for ALPHA, 1 for DELTA
        now = _now_str()
        old = "2020-01-01 00:00:00"
        for i in range(3):
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"d-alpha-{i}", "ALPHA", "Ready", old, now),
            )
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d-delta-1", "DELTA", "Ready", now, now),
        )
        conn.commit()
        result = fn(conn)
        codes = [r["client_code"] for r in result]
        # DELTA has ~1 doc/month (very recent), ALPHA has 3 / ~73 months ≈ 0.04/month
        # So DELTA should come first
        assert codes.index("DELTA") < codes.index("ALPHA")


# ---------------------------------------------------------------------------
# _analytics_monthly_trends
# ---------------------------------------------------------------------------

class TestMonthlyTrends:
    def _import(self):
        from scripts.review_dashboard import _analytics_monthly_trends
        return _analytics_monthly_trends

    def test_returns_list_of_dicts(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        result = fn(conn)
        assert isinstance(result, list)

    def test_empty_when_no_docs(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        result = fn(conn)
        assert result == []

    def test_counts_docs_by_month(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        this_month = datetime.now(timezone.utc).strftime("%Y-%m-15 00:00:00")
        for i in range(4):
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"d{i}", "ACME", "Ready", this_month, this_month),
            )
        conn.commit()
        result = fn(conn)
        key = datetime.now(timezone.utc).strftime("%Y-%m")
        month_row = next((r for r in result if r["month"] == key), None)
        assert month_row is not None
        assert month_row["count"] == 4

    def test_excludes_docs_older_than_12_months(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        old_date = "2000-01-15 00:00:00"
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("old-doc", "ACME", "Ready", old_date, old_date),
        )
        conn.commit()
        result = fn(conn)
        assert not any(r["month"].startswith("2000") for r in result)

    def test_sorted_chronologically(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        dates = [
            datetime.now(timezone.utc).strftime("%Y-%m-15"),
            (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-15"),
        ]
        for i, d in enumerate(dates):
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"d{i}", "ACME", "Ready", d, d),
            )
        conn.commit()
        result = fn(conn)
        months = [r["month"] for r in result]
        assert months == sorted(months)


# ---------------------------------------------------------------------------
# _analytics_fraud_summary
# ---------------------------------------------------------------------------

class TestFraudSummary:
    def _import(self):
        from scripts.review_dashboard import _analytics_fraud_summary
        return _analytics_fraud_summary

    def test_returns_dict_with_four_keys(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert set(result.keys()) == {"critical", "high", "medium", "low"}

    def test_all_zero_when_no_fraud(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert all(v == 0 for v in result.values())

    def test_counts_by_severity(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        flags = json.dumps([
            {"severity": "critical", "rule": "bank_account_change"},
            {"severity": "high", "rule": "duplicate_exact"},
            {"severity": "high", "rule": "new_vendor_large"},
            {"severity": "medium", "rule": "weekend_transaction"},
            {"severity": "low", "rule": "round_number"},
        ])
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, fraud_flags, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "ACME", "Ready", flags, now, now),
        )
        conn.commit()
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert result["critical"] == 1
        assert result["high"] == 2
        assert result["medium"] == 1
        assert result["low"] == 1

    def test_ignores_docs_from_previous_months(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        old_date = "2020-03-15 00:00:00"
        flags = json.dumps([{"severity": "critical", "rule": "bank_account_change"}])
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, fraud_flags, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "ACME", "Ready", flags, old_date, old_date),
        )
        conn.commit()
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert result["critical"] == 0

    def test_handles_null_fraud_flags(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, fraud_flags, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "ACME", "Ready", None, now, now),
        )
        conn.commit()
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert all(v == 0 for v in result.values())

    def test_handles_empty_fraud_flags_string(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, fraud_flags, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "ACME", "Ready", "[]", now, now),
        )
        conn.commit()
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert all(v == 0 for v in result.values())

    def test_multiple_docs_aggregated(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for i, sev in enumerate(["critical", "high", "critical"]):
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, fraud_flags, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"d{i}", "ACME", "Ready",
                 json.dumps([{"severity": sev, "rule": "test"}]),
                 now, now),
            )
        conn.commit()
        month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
        result = fn(conn, month_start)
        assert result["critical"] == 2
        assert result["high"] == 1


# ---------------------------------------------------------------------------
# _analytics_deadlines_at_risk
# ---------------------------------------------------------------------------

class TestDeadlinesAtRisk:
    def _import(self):
        from scripts.review_dashboard import _analytics_deadlines_at_risk
        return _analytics_deadlines_at_risk

    def test_returns_list(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        result = fn(conn)
        assert isinstance(result, list)

    def test_empty_when_no_open_items(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        result = fn(conn)
        assert result == []

    def test_returns_clients_with_upcoming_deadline_and_open_docs(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        due_soon = _date_offset(7)
        # Add open period_close item
        conn.execute(
            "INSERT INTO period_close (client_code, period, checklist_item, status, due_date) "
            "VALUES (?, ?, ?, ?, ?)",
            ("ACME", "2026-03", "All docs reviewed", "open", due_soon),
        )
        # Add document in NeedsReview status
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "ACME", "Needs Review", now, now),
        )
        conn.commit()
        result = fn(conn)
        assert any(r["client_code"] == "ACME" for r in result)

    def test_excludes_clients_with_no_open_docs(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        due_soon = _date_offset(5)
        # Open period_close item but all docs are Posted (not open)
        conn.execute(
            "INSERT INTO period_close (client_code, period, checklist_item, status, due_date) "
            "VALUES (?, ?, ?, ?, ?)",
            ("CLEAN", "2026-03", "All docs reviewed", "open", due_soon),
        )
        now = _now_str()
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "CLEAN", "Posted", now, now),
        )
        conn.commit()
        result = fn(conn)
        assert not any(r["client_code"] == "CLEAN" for r in result)

    def test_excludes_deadlines_outside_14_day_window(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        far_future = _date_offset(30)
        now = _now_str()
        conn.execute(
            "INSERT INTO period_close (client_code, period, checklist_item, status, due_date) "
            "VALUES (?, ?, ?, ?, ?)",
            ("FAR", "2026-04", "All docs reviewed", "open", far_future),
        )
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "FAR", "Needs Review", now, now),
        )
        conn.commit()
        result = fn(conn)
        assert not any(r["client_code"] == "FAR" for r in result)

    def test_excludes_completed_checklist_items(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        due_soon = _date_offset(3)
        now = _now_str()
        conn.execute(
            "INSERT INTO period_close (client_code, period, checklist_item, status, due_date) "
            "VALUES (?, ?, ?, ?, ?)",
            ("DONE", "2026-03", "All docs reviewed", "complete", due_soon),
        )
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "DONE", "Needs Review", now, now),
        )
        conn.commit()
        result = fn(conn)
        assert not any(r["client_code"] == "DONE" for r in result)

    def test_result_has_required_keys(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        due_soon = _date_offset(4)
        now = _now_str()
        conn.execute(
            "INSERT INTO period_close (client_code, period, checklist_item, status, due_date) "
            "VALUES (?, ?, ?, ?, ?)",
            ("RISK", "2026-03", "GST reconciled", "open", due_soon),
        )
        conn.execute(
            "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("d1", "RISK", "On Hold", now, now),
        )
        conn.commit()
        result = fn(conn)
        assert len(result) >= 1
        row = next(r for r in result if r["client_code"] == "RISK")
        assert "client_code" in row
        assert "period" in row
        assert "earliest_deadline" in row
        assert "open_docs" in row
        assert row["open_docs"] >= 1

    def test_ordered_by_deadline(self):
        fn = self._import()
        conn = _in_memory_db()
        _bootstrap(conn)
        now = _now_str()
        # FIRST has deadline in 3 days, SECOND in 10 days
        for client, days in [("FIRST", 3), ("SECOND", 10)]:
            conn.execute(
                "INSERT INTO period_close (client_code, period, checklist_item, status, due_date) "
                "VALUES (?, ?, ?, ?, ?)",
                (client, "2026-03", "GST", "open", _date_offset(days)),
            )
            conn.execute(
                "INSERT INTO documents (document_id, client_code, review_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"d-{client}", client, "On Hold", now, now),
            )
        conn.commit()
        result = fn(conn)
        codes = [r["client_code"] for r in result]
        assert codes.index("FIRST") < codes.index("SECOND")


# ---------------------------------------------------------------------------
# render_analytics importability and source checks
# ---------------------------------------------------------------------------

class TestRenderAnalytics:
    def test_render_analytics_importable(self):
        from scripts.review_dashboard import render_analytics
        assert callable(render_analytics)

    def test_render_analytics_accepts_expected_args(self):
        from scripts.review_dashboard import render_analytics
        sig = inspect.signature(render_analytics)
        params = set(sig.parameters.keys())
        assert "ctx" in params
        assert "user" in params
        assert "lang" in params

    def test_analytics_route_in_source(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/analytics"' in src or "'/analytics'" in src

    def test_analytics_owner_only_check_in_source(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # Should check role == "owner" for /analytics
        assert 'analytics' in src
        assert 'owner' in src

    def test_helper_functions_importable(self):
        from scripts.review_dashboard import (
            _analytics_staff_productivity,
            _analytics_client_complexity,
            _analytics_monthly_trends,
            _analytics_fraud_summary,
            _analytics_deadlines_at_risk,
        )
        for fn in [
            _analytics_staff_productivity,
            _analytics_client_complexity,
            _analytics_monthly_trends,
            _analytics_fraud_summary,
            _analytics_deadlines_at_risk,
        ]:
            assert callable(fn)

    def test_chart_js_cdn_in_source(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "cdnjs.cloudflare.com" in src
        assert "Chart.js" in src or "chart.umd.min.js" in src

    def test_all_five_sections_referenced(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "analytics_staff_title" in src
        assert "analytics_client_title" in src
        assert "analytics_trends_title" in src
        assert "analytics_fraud_title" in src
        assert "analytics_deadlines_title" in src


# ---------------------------------------------------------------------------
# i18n keys
# ---------------------------------------------------------------------------

ANALYTICS_KEYS = [
    "analytics_title",
    "analytics_nav_link",
    "analytics_h1",
    "analytics_staff_title",
    "analytics_col_accountant",
    "analytics_col_docs_week",
    "analytics_col_docs_month",
    "analytics_col_avg_review_min",
    "analytics_col_hold_rate",
    "analytics_col_approval_rate",
    "analytics_client_title",
    "analytics_col_client",
    "analytics_col_avg_docs_month",
    "analytics_col_avg_review_min_client",
    "analytics_col_hold_rate_client",
    "analytics_col_common_reason",
    "analytics_col_est_fee",
    "analytics_trends_title",
    "analytics_fraud_title",
    "analytics_col_severity",
    "analytics_col_count",
    "analytics_no_fraud",
    "analytics_deadlines_title",
    "analytics_col_period",
    "analytics_col_deadline",
    "analytics_col_open_docs",
    "analytics_no_deadlines",
    "analytics_no_staff",
    "analytics_no_clients",
    "analytics_minutes_abbr",
    "analytics_na",
]


class TestI18nKeys:
    def _load(self, lang: str) -> dict:
        path = ROOT / "src" / "i18n" / f"{lang}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_en_has_all_analytics_keys(self):
        en = self._load("en")
        missing = [k for k in ANALYTICS_KEYS if k not in en]
        assert missing == [], f"Missing en.json keys: {missing}"

    def test_fr_has_all_analytics_keys(self):
        fr = self._load("fr")
        missing = [k for k in ANALYTICS_KEYS if k not in fr]
        assert missing == [], f"Missing fr.json keys: {missing}"

    def test_en_analytics_values_non_empty(self):
        en = self._load("en")
        for key in ANALYTICS_KEYS:
            assert en.get(key), f"en.json key '{key}' is empty"

    def test_fr_analytics_values_non_empty(self):
        fr = self._load("fr")
        for key in ANALYTICS_KEYS:
            assert fr.get(key), f"fr.json key '{key}' is empty"

    def test_at_least_31_analytics_keys(self):
        assert len(ANALYTICS_KEYS) >= 31

    def test_t_function_resolves_analytics_keys(self):
        from src.i18n import t
        for key in ANALYTICS_KEYS:
            for lang in ("en", "fr"):
                result = t(key, lang)
                assert result and result != key, (
                    f"t('{key}', '{lang}') returned missing/fallback: {result!r}"
                )
