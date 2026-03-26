"""
tests/test_fraud_engine.py — pytest tests for src/engines/fraud_engine.py

Covers:
  - Quebec holiday computation (easter, fixed dates, floating dates)
  - Weekend / holiday transaction rule
  - Vendor amount anomaly rule (2σ threshold, requires ≥10 transactions)
  - Vendor timing anomaly rule (14-day threshold)
  - Duplicate detection (same-vendor 30d HIGH, cross-vendor 7d MEDIUM)
  - Round number flag rule
  - New vendor large amount rule
  - Bank account change rule (CRITICAL)
  - run_fraud_detection end-to-end with in-memory-style DB
  - migrate_db declares fraud_flags column
  - review_dashboard exposes render_fraud_flags function
  - i18n keys present in both EN and FR
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _in_memory_conn() -> sqlite3.Connection:
    """Create a minimal in-memory documents table for testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE documents (
            document_id    TEXT PRIMARY KEY,
            file_name      TEXT,
            file_path      TEXT,
            client_code    TEXT,
            vendor         TEXT,
            doc_type       TEXT,
            amount         REAL,
            document_date  TEXT,
            review_status  TEXT DEFAULT 'NeedsReview',
            confidence     REAL DEFAULT 0.5,
            raw_result     TEXT,
            created_at     TEXT,
            updated_at     TEXT,
            fraud_flags    TEXT
        )
    """)
    conn.commit()
    return conn


def _insert_doc(
    conn: sqlite3.Connection,
    doc_id: str,
    vendor: str = "ACME Corp",
    client_code: str = "CLI1",
    amount: float = 100.0,
    document_date: str = "2025-03-10",
    review_status: str = "Posted",
    raw_result: dict | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO documents
            (document_id, vendor, client_code, amount, document_date,
             review_status, raw_result, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '2025-01-01', '2025-01-01')
        """,
        (
            doc_id, vendor, client_code, amount, document_date,
            review_status,
            json.dumps(raw_result or {}),
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Quebec holiday tests
# ---------------------------------------------------------------------------

class TestQuebecHolidays:
    def _holidays(self, year: int) -> dict:
        from src.engines.fraud_engine import _quebec_holidays
        return _quebec_holidays(year)

    def test_new_years_day(self):
        h = self._holidays(2025)
        assert date(2025, 1, 1) in h

    def test_christmas(self):
        h = self._holidays(2025)
        assert date(2025, 12, 25) in h

    def test_boxing_day(self):
        h = self._holidays(2025)
        assert date(2025, 12, 26) in h

    def test_fete_nationale(self):
        h = self._holidays(2025)
        assert date(2025, 6, 24) in h

    def test_canada_day(self):
        h = self._holidays(2025)
        assert date(2025, 7, 1) in h

    def test_easter_sunday_2025(self):
        from src.engines.fraud_engine import _easter_sunday
        # Easter 2025 is April 20
        assert _easter_sunday(2025) == date(2025, 4, 20)

    def test_good_friday_2025(self):
        h = self._holidays(2025)
        assert date(2025, 4, 18) in h  # 2 days before Easter April 20

    def test_easter_monday_2025(self):
        h = self._holidays(2025)
        assert date(2025, 4, 21) in h  # day after Easter

    def test_labour_day_2025(self):
        # First Monday of September 2025 = September 1
        h = self._holidays(2025)
        assert date(2025, 9, 1) in h

    def test_thanksgiving_2025(self):
        # Second Monday of October 2025 = October 13
        h = self._holidays(2025)
        assert date(2025, 10, 13) in h

    def test_victoria_day_2025(self):
        # Monday before May 25, 2025 = May 19
        h = self._holidays(2025)
        assert date(2025, 5, 19) in h

    def test_non_holiday_is_not_in_dict(self):
        h = self._holidays(2025)
        assert date(2025, 3, 15) not in h

    def test_is_quebec_holiday_matches(self):
        from src.engines.fraud_engine import _is_quebec_holiday
        assert _is_quebec_holiday(date(2025, 1, 1)) is not None
        assert _is_quebec_holiday(date(2025, 3, 15)) is None


# ---------------------------------------------------------------------------
# Weekend / holiday transaction rule
# ---------------------------------------------------------------------------

class TestWeekendHolidayRule:
    def _run(self, amount: float, doc_date: date) -> list:
        from src.engines.fraud_engine import _rule_weekend_holiday
        return _rule_weekend_holiday(amount, doc_date)

    def test_saturday_above_threshold_flagged(self):
        # 2025-01-04 is a Saturday
        flags = self._run(600.0, date(2025, 1, 4))
        assert any(f["rule"] == "weekend_transaction" for f in flags)

    def test_sunday_above_threshold_flagged(self):
        # 2025-01-05 is a Sunday
        flags = self._run(1000.0, date(2025, 1, 5))
        assert any(f["rule"] == "weekend_transaction" for f in flags)

    def test_saturday_below_threshold_not_flagged(self):
        # FIX 9: threshold lowered to $100
        flags = self._run(99.99, date(2025, 1, 4))
        assert flags == []

    def test_weekday_no_holiday_not_flagged(self):
        # 2025-03-10 is a Monday, not a holiday
        flags = self._run(5000.0, date(2025, 3, 10))
        assert flags == []

    def test_holiday_above_threshold_flagged(self):
        # Christmas 2025
        flags = self._run(600.0, date(2025, 12, 25))
        assert any(f["rule"] == "holiday_transaction" for f in flags)

    def test_holiday_below_threshold_not_flagged(self):
        # FIX 9: threshold lowered to $100
        flags = self._run(99.99, date(2025, 12, 25))
        assert flags == []

    def test_weekend_flag_has_correct_severity(self):
        flags = self._run(700.0, date(2025, 1, 4))
        assert flags[0]["severity"] == "low"

    def test_holiday_flag_has_i18n_key(self):
        flags = self._run(600.0, date(2025, 12, 25))
        assert flags[0]["i18n_key"] == "fraud_holiday_transaction"

    def test_exactly_at_threshold_not_flagged(self):
        # FIX 9: threshold lowered to $100; $100 exactly is NOT above $100
        flags = self._run(100.0, date(2025, 1, 4))
        assert flags == []


# ---------------------------------------------------------------------------
# Vendor amount anomaly rule
# ---------------------------------------------------------------------------

class TestVendorAmountAnomalyRule:
    def _make_history(self, amounts: list[float]) -> list[dict]:
        return [{"amount": a, "document_date": "2025-01-15"} for a in amounts]

    def _run(self, amount: float, history: list[dict]) -> dict | None:
        from src.engines.fraud_engine import _rule_vendor_amount_anomaly
        return _rule_vendor_amount_anomaly(amount, history)

    def test_insufficient_history_returns_none(self):
        history = self._make_history([100.0] * 9)  # only 9, need 10
        assert self._run(500.0, history) is None

    def test_normal_amount_no_flag(self):
        history = self._make_history([100.0] * 10)
        assert self._run(100.0, history) is None

    def test_outlier_flagged(self):
        # mean=100, std≈0, any deviation at all; use varying amounts
        amounts = [100.0, 110.0, 90.0, 105.0, 95.0, 100.0, 100.0, 100.0, 100.0, 100.0]
        history = self._make_history(amounts)
        # This transaction is way above: 500 is >2σ from mean ~100
        flag = self._run(500.0, history)
        assert flag is not None
        assert flag["rule"] == "vendor_amount_anomaly"
        assert flag["severity"] == "high"

    def test_flag_has_sigma_in_params(self):
        amounts = [100.0] * 10
        # Avoid division by zero by adding slight variance
        amounts[0] = 90.0
        history = self._make_history(amounts)
        flag = self._run(300.0, history)
        assert flag is not None
        assert "sigma" in flag["params"]

    def test_zero_std_dev_returns_none(self):
        history = self._make_history([100.0] * 10)
        # std=0, skip division
        assert self._run(200.0, history) is None


# ---------------------------------------------------------------------------
# Vendor timing anomaly rule
# ---------------------------------------------------------------------------

class TestVendorTimingAnomalyRule:
    def _make_history(self, days: list[int]) -> list[dict]:
        return [{"document_date": f"2025-01-{d:02d}"} for d in days]

    def _run(self, doc_date: date, history: list[dict]) -> dict | None:
        from src.engines.fraud_engine import _rule_vendor_timing_anomaly
        return _rule_vendor_timing_anomaly(doc_date, history)

    def test_insufficient_history_returns_none(self):
        history = self._make_history([15] * 9)
        assert self._run(date(2025, 1, 15), history) is None

    def test_normal_timing_no_flag(self):
        history = self._make_history([15] * 10)
        # Same day → no anomaly
        assert self._run(date(2025, 1, 15), history) is None

    def test_timing_anomaly_flagged(self):
        # Vendor always invoices on day 15; new invoice on day 30 = 15-day diff (> 14)
        history = self._make_history([15] * 10)
        flag = self._run(date(2025, 1, 30), history)
        assert flag is not None
        assert flag["rule"] == "vendor_timing_anomaly"

    def test_timing_within_threshold_not_flagged(self):
        history = self._make_history([15] * 10)
        # Day 22 is 7 days off → within 14-day threshold → no flag
        assert self._run(date(2025, 1, 22), history) is None

    def test_flag_has_i18n_key(self):
        history = self._make_history([15] * 10)
        flag = self._run(date(2025, 1, 30), history)
        assert flag is not None
        assert flag["i18n_key"] == "fraud_vendor_timing_anomaly"


# ---------------------------------------------------------------------------
# Duplicate detection rule
# ---------------------------------------------------------------------------

class TestDuplicateRule:
    def _run(
        self,
        conn: sqlite3.Connection,
        document_id: str,
        vendor: str,
        client_code: str,
        amount: float,
        doc_date: date,
    ) -> list:
        from src.engines.fraud_engine import _rule_duplicate
        return _rule_duplicate(conn, document_id, vendor, client_code, amount, doc_date)

    def test_no_duplicates_returns_empty(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "existing", "ACME", "CLI1", 100.0, "2025-01-01")
        flags = self._run(conn, "new_doc", "ACME", "CLI1", 200.0, date(2025, 1, 15))
        assert flags == []

    def test_exact_duplicate_same_vendor_flagged_high(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "prior", "ACME", "CLI1", 500.0, "2025-01-10")
        flags = self._run(conn, "new_doc", "ACME", "CLI1", 500.0, date(2025, 1, 20))
        assert any(f["rule"] == "duplicate_exact" for f in flags)
        assert any(f["severity"] == "high" for f in flags)

    def test_same_vendor_duplicate_outside_30d_not_flagged(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "prior", "ACME", "CLI1", 500.0, "2024-12-01")
        # More than 30 days before Jan 15
        flags = self._run(conn, "new_doc", "ACME", "CLI1", 500.0, date(2025, 1, 15))
        assert not any(f["rule"] == "duplicate_exact" for f in flags)

    def test_cross_vendor_same_amount_flagged_medium(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "prior", "OTHER Vendor", "CLI1", 750.0, "2025-01-10")
        flags = self._run(conn, "new_doc", "ACME", "CLI1", 750.0, date(2025, 1, 15))
        assert any(f["rule"] == "duplicate_cross_vendor" for f in flags)
        assert any(f["severity"] == "medium" for f in flags)

    def test_cross_vendor_outside_7d_not_flagged(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "prior", "OTHER Vendor", "CLI1", 750.0, "2024-12-30")
        # More than 7 days before Jan 15
        flags = self._run(conn, "new_doc", "ACME", "CLI1", 750.0, date(2025, 1, 15))
        assert not any(f["rule"] == "duplicate_cross_vendor" for f in flags)

    def test_self_not_compared(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "same_doc", "ACME", "CLI1", 500.0, "2025-01-15")
        flags = self._run(conn, "same_doc", "ACME", "CLI1", 500.0, date(2025, 1, 15))
        assert flags == []

    def test_different_client_not_flagged(self):
        conn = _in_memory_conn()
        _insert_doc(conn, "prior", "ACME", "CLI2", 500.0, "2025-01-10")
        flags = self._run(conn, "new_doc", "ACME", "CLI1", 500.0, date(2025, 1, 15))
        # Different client — should not trigger duplicate_exact
        assert not any(f["rule"] == "duplicate_exact" for f in flags)


# ---------------------------------------------------------------------------
# Round number flag rule
# ---------------------------------------------------------------------------

class TestRoundNumberRule:
    def _make_history(self, amounts: list[float]) -> list[dict]:
        return [{"amount": a} for a in amounts]

    def _run(self, amount: float, history: list[dict]) -> dict | None:
        from src.engines.fraud_engine import _rule_round_number
        return _rule_round_number(amount, history)

    def test_irregular_vendor_round_amount_flagged(self):
        # Vendor normally invoices irregular amounts (high CV)
        amounts = [123.45, 234.56, 198.77, 87.32, 345.99]  # irregular
        flag = self._run(1000.0, self._make_history(amounts))
        assert flag is not None
        assert flag["rule"] == "round_number_flag"

    def test_not_round_amount_not_flagged(self):
        amounts = [123.45, 234.56, 198.77, 87.32, 345.99]
        assert self._run(123.45, self._make_history(amounts)) is None

    def test_regular_vendor_round_amount_not_flagged(self):
        # Vendor always invoices exactly the same amount → low CV
        amounts = [1000.0, 1000.0, 1000.0, 1000.0, 1000.0]
        assert self._run(1000.0, self._make_history(amounts)) is None

    def test_insufficient_history_not_flagged(self):
        amounts = [123.45, 234.56, 198.77, 87.32]  # only 4, need 5
        assert self._run(1000.0, self._make_history(amounts)) is None

    def test_500_is_round(self):
        from src.engines.fraud_engine import _is_round_number
        assert _is_round_number(500.0) is True

    def test_1000_is_round(self):
        from src.engines.fraud_engine import _is_round_number
        assert _is_round_number(1000.0) is True

    def test_5000_is_round(self):
        from src.engines.fraud_engine import _is_round_number
        assert _is_round_number(5000.0) is True

    def test_123_45_not_round(self):
        from src.engines.fraud_engine import _is_round_number
        assert _is_round_number(123.45) is False

    def test_250_is_round(self):
        from src.engines.fraud_engine import _is_round_number
        # FIX 8: 250 is now flagged as round (multiple of 50, >= 100)
        assert _is_round_number(250.0) is True


# ---------------------------------------------------------------------------
# New vendor large amount rule
# ---------------------------------------------------------------------------

class TestNewVendorLargeAmountRule:
    def _run(self, vendor: str, amount: float, history: list[dict]) -> dict | None:
        from src.engines.fraud_engine import _rule_new_vendor_large_amount
        return _rule_new_vendor_large_amount(vendor, amount, history)

    def test_new_vendor_above_threshold_flagged(self):
        flag = self._run("NewCo", 2500.0, [])
        assert flag is not None
        assert flag["rule"] == "new_vendor_large_amount"
        assert flag["severity"] == "high"

    def test_new_vendor_below_threshold_not_flagged(self):
        assert self._run("NewCo", 1999.99, []) is None

    def test_known_vendor_not_flagged(self):
        # P1-7: Vendor needs >= 3 approved transactions to be considered established
        history = [
            {"review_status": "Posted", "amount": 100.0, "document_date": "2025-01-01"},
            {"review_status": "Posted", "amount": 200.0, "document_date": "2025-02-01"},
            {"review_status": "Posted", "amount": 150.0, "document_date": "2025-03-01"},
        ]
        assert self._run("ACME", 5000.0, history) is None

    def test_vendor_with_only_review_status_treated_as_new(self):
        # Only NeedsReview history — not approved/posted → still "new"
        history = [{"review_status": "NeedsReview", "amount": 100.0, "document_date": "2025-01-01"}]
        flag = self._run("NewCo", 3000.0, history)
        assert flag is not None

    def test_flag_has_correct_i18n_key(self):
        flag = self._run("NewCo", 2500.0, [])
        assert flag is not None
        assert flag["i18n_key"] == "fraud_new_vendor_large"

    def test_flag_params_include_vendor_and_amount(self):
        flag = self._run("NewCo Inc", 3000.0, [])
        assert flag is not None
        assert "NewCo Inc" in flag["params"]["vendor"]


# ---------------------------------------------------------------------------
# Bank account change rule
# ---------------------------------------------------------------------------

class TestBankAccountChangeRule:
    def _run(self, raw_json: str | None, history: list[dict]) -> dict | None:
        from src.engines.fraud_engine import _rule_bank_account_change
        return _rule_bank_account_change(raw_json, history)

    def _raw(self, account_number: str) -> str:
        return json.dumps({"account_number": account_number})

    def test_no_bank_details_returns_none(self):
        assert self._run(None, []) is None
        assert self._run("{}", []) is None

    def test_no_history_returns_none(self):
        assert self._run(self._raw("12345678"), []) is None

    def test_same_bank_account_no_flag(self):
        history = [{"raw_result": self._raw("12345678")}]
        assert self._run(self._raw("12345678"), history) is None

    def test_changed_bank_account_flagged_critical(self):
        history = [{"raw_result": self._raw("12345678")}]
        flag = self._run(self._raw("99999999"), history)
        assert flag is not None
        assert flag["rule"] == "bank_account_change"
        assert flag["severity"] == "critical"

    def test_flag_has_i18n_key(self):
        history = [{"raw_result": self._raw("12345678")}]
        flag = self._run(self._raw("99999999"), history)
        assert flag is not None
        assert flag["i18n_key"] == "fraud_bank_account_change"

    def test_prior_without_bank_details_no_flag(self):
        history = [{"raw_result": "{}"}]
        # Current has bank details, prior does not — cannot compare → no flag
        assert self._run(self._raw("12345678"), history) is None


# ---------------------------------------------------------------------------
# run_fraud_detection end-to-end
# ---------------------------------------------------------------------------

class TestRunFraudDetection:
    """
    End-to-end tests using a real SQLite file-based or in-memory database.
    We pass an in-memory DB path via a temp file to avoid touching production data.
    """

    def _make_db(self, tmp_path: Path) -> Path:
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE documents (
                document_id    TEXT PRIMARY KEY,
                file_name      TEXT,
                file_path      TEXT,
                client_code    TEXT,
                vendor         TEXT,
                doc_type       TEXT,
                amount         REAL,
                document_date  TEXT,
                review_status  TEXT DEFAULT 'NeedsReview',
                confidence     REAL DEFAULT 0.5,
                raw_result     TEXT,
                created_at     TEXT DEFAULT '',
                updated_at     TEXT DEFAULT '',
                fraud_flags    TEXT
            )
        """)
        conn.commit()
        conn.close()
        return db

    def _insert(self, db: Path, **kwargs: Any) -> None:
        conn = sqlite3.connect(str(db))
        defaults = {
            "document_id": "doc_test",
            "file_name": "test.pdf",
            "file_path": "/tmp/test.pdf",
            "client_code": "CLI1",
            "vendor": "ACME Corp",
            "doc_type": "invoice",
            "amount": 150.0,
            "document_date": "2025-03-10",
            "review_status": "NeedsReview",
            "confidence": 0.9,
            "raw_result": "{}",
            "created_at": "2025-03-10T00:00:00+00:00",
            "updated_at": "2025-03-10T00:00:00+00:00",
        }
        defaults.update(kwargs)
        cols = ", ".join(defaults.keys())
        vals = ", ".join("?" for _ in defaults)
        conn.execute(f"INSERT INTO documents ({cols}) VALUES ({vals})", list(defaults.values()))
        conn.commit()
        conn.close()

    def test_no_flags_for_simple_doc(self, tmp_path):
        from src.engines.fraud_engine import run_fraud_detection
        db = self._make_db(tmp_path)
        self._insert(db, document_id="doc_simple", amount=150.0, document_date="2025-03-10")
        flags = run_fraud_detection("doc_simple", db_path=db)
        assert isinstance(flags, list)

    def test_missing_document_returns_empty(self, tmp_path):
        from src.engines.fraud_engine import run_fraud_detection
        db = self._make_db(tmp_path)
        flags = run_fraud_detection("nonexistent_doc", db_path=db)
        assert flags == []

    def test_flags_saved_to_db(self, tmp_path):
        from src.engines.fraud_engine import get_fraud_flags, run_fraud_detection
        db = self._make_db(tmp_path)
        # Saturday + high amount → weekend flag
        self._insert(db, document_id="doc_sat", amount=600.0, document_date="2025-01-04")
        run_fraud_detection("doc_sat", db_path=db)
        saved = get_fraud_flags("doc_sat", db_path=db)
        assert isinstance(saved, list)
        assert any(f["rule"] == "weekend_transaction" for f in saved)

    def test_new_vendor_large_flagged_e2e(self, tmp_path):
        from src.engines.fraud_engine import run_fraud_detection
        db = self._make_db(tmp_path)
        self._insert(db, document_id="doc_new", vendor="BrandNew LLC", amount=2500.0,
                     document_date="2025-03-10", review_status="NeedsReview")
        flags = run_fraud_detection("doc_new", db_path=db)
        assert any(f["rule"] == "new_vendor_large_amount" for f in flags)

    def test_missing_amount_returns_empty(self, tmp_path):
        from src.engines.fraud_engine import run_fraud_detection
        db = self._make_db(tmp_path)
        self._insert(db, document_id="doc_no_amt", amount=None)
        flags = run_fraud_detection("doc_no_amt", db_path=db)
        assert flags == []

    def test_missing_date_returns_empty(self, tmp_path):
        from src.engines.fraud_engine import run_fraud_detection
        db = self._make_db(tmp_path)
        self._insert(db, document_id="doc_no_date", document_date=None)
        flags = run_fraud_detection("doc_no_date", db_path=db)
        assert flags == []

    def test_get_fraud_flags_returns_list(self, tmp_path):
        from src.engines.fraud_engine import get_fraud_flags
        db = self._make_db(tmp_path)
        self._insert(db)
        result = get_fraud_flags("doc_test", db_path=db)
        assert isinstance(result, list)

    def test_get_fraud_flags_nonexistent_returns_empty(self, tmp_path):
        from src.engines.fraud_engine import get_fraud_flags
        db = self._make_db(tmp_path)
        assert get_fraud_flags("nonexistent", db_path=db) == []


# ---------------------------------------------------------------------------
# migrate_db declares fraud_flags
# ---------------------------------------------------------------------------

class TestMigrateDbFraudFlags:
    def test_migrate_db_declares_fraud_flags(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "fraud_flags" in src

    def test_migrate_db_adds_to_documents_table(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        # The fraud_flags column should be in a documents add_missing block
        assert 'fraud_flags' in src


# ---------------------------------------------------------------------------
# review_dashboard integration
# ---------------------------------------------------------------------------

class TestReviewDashboardFraudFlags:
    def test_render_fraud_flags_exported(self):
        from scripts.review_dashboard import render_fraud_flags
        assert callable(render_fraud_flags)

    def test_render_fraud_flags_returns_empty_when_no_flags(self):
        from scripts.review_dashboard import render_fraud_flags
        conn = _in_memory_conn()
        _insert_doc(conn, "doc_x", amount=100.0, document_date="2025-03-10")
        row = conn.execute("SELECT * FROM documents WHERE document_id='doc_x'").fetchone()
        result = render_fraud_flags(row, lang="en")
        assert result == ""

    def test_render_fraud_flags_shows_flags_when_present(self):
        from scripts.review_dashboard import render_fraud_flags
        conn = _in_memory_conn()
        _insert_doc(conn, "doc_y", amount=100.0, document_date="2025-03-10")
        flags = [{"rule": "weekend_transaction", "severity": "low",
                  "i18n_key": "fraud_weekend_transaction",
                  "params": {"weekday": "Saturday", "amount": "$600.00"}}]
        conn.execute("UPDATE documents SET fraud_flags=? WHERE document_id='doc_y'",
                     (json.dumps(flags),))
        conn.commit()
        row = conn.execute("SELECT * FROM documents WHERE document_id='doc_y'").fetchone()
        html = render_fraud_flags(row, lang="en")
        assert "fraud" in html.lower() or "weekend" in html.lower() or "Fraud" in html

    def test_render_fraud_flags_accepts_lang_param(self):
        import inspect
        from scripts.review_dashboard import render_fraud_flags
        sig = inspect.signature(render_fraud_flags)
        assert "lang" in sig.parameters

    def test_render_fraud_flags_called_in_document_detail(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "render_fraud_flags" in src


# ---------------------------------------------------------------------------
# i18n keys present in both EN and FR
# ---------------------------------------------------------------------------

class TestFraudI18nKeys:
    REQUIRED_KEYS = [
        "fraud_section_title",
        "fraud_no_flags",
        "fraud_severity_critical",
        "fraud_severity_high",
        "fraud_severity_medium",
        "fraud_severity_low",
        "fraud_vendor_amount_anomaly",
        "fraud_vendor_timing_anomaly",
        "fraud_duplicate_exact",
        "fraud_duplicate_cross_vendor",
        "fraud_weekend_transaction",
        "fraud_holiday_transaction",
        "fraud_round_number",
        "fraud_new_vendor_large",
        "fraud_bank_account_change",
    ]

    def _load(self, lang: str) -> dict:
        return json.loads((ROOT / "src" / "i18n" / f"{lang}.json").read_text(encoding="utf-8"))

    def test_en_has_all_fraud_keys(self):
        data = self._load("en")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"en.json missing fraud key: {key!r}"

    def test_fr_has_all_fraud_keys(self):
        data = self._load("fr")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"fr.json missing fraud key: {key!r}"

    def test_keys_translate_via_t(self):
        from src.i18n import reload_cache, t
        reload_cache()
        for key in self.REQUIRED_KEYS:
            assert t(key, "en") != key, f"en missing translation for {key!r}"
            assert t(key, "fr") != key, f"fr missing translation for {key!r}"
