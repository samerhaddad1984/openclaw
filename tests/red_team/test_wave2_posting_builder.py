"""
Second-Wave Independent Verification — Posting Builder Attacks

The posting_builder.py was HEAVILY rewritten in this diff.
These tests attack the NEW code — not the old code — from fresh angles:
- SQL injection via document_id and field values
- Concurrent updates / upsert race conditions
- table_columns PRAGMA injection
- Null/empty cascades through first_present
- JSON serialization round-trip fidelity
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.posting_builder import (
    normalize_text,
    safe_float,
    json_loads_safe,
    json_dumps_stable,
    row_to_dict,
    table_columns,
    table_exists,
    first_present,
    normalize_string_list,
    build_posting_id,
    build_payload_from_sources,
    ensure_posting_job_table_minimum,
    upsert_posting_job,
    choose_default_memo,
)


# ═════════════════════════════════════════════════════════════════════════
# 1. normalize_text edge cases
# ═════════════════════════════════════════════════════════════════════════

class TestNormalizeText:
    def test_none_returns_empty(self):
        assert normalize_text(None) == ""

    def test_int_converts(self):
        assert normalize_text(42) == "42"

    def test_float_converts(self):
        assert normalize_text(3.14) == "3.14"

    def test_whitespace_stripped(self):
        assert normalize_text("  hello  ") == "hello"

    def test_bool_converts(self):
        assert normalize_text(True) == "True"

    def test_list_converts_to_string(self):
        # Should not crash — just stringify
        result = normalize_text([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_empty_string(self):
        assert normalize_text("") == ""


# ═════════════════════════════════════════════════════════════════════════
# 2. safe_float edge cases
# ═════════════════════════════════════════════════════════════════════════

class TestSafeFloat:
    def test_none(self):
        assert safe_float(None) is None

    def test_empty_string(self):
        assert safe_float("") is None

    def test_valid_string(self):
        assert safe_float("123.45") == 123.45

    def test_garbage_string(self):
        assert safe_float("not-a-number") is None

    def test_string_with_currency(self):
        """safe_float does NOT strip currency — it's a raw converter."""
        assert safe_float("$100") is None

    def test_int(self):
        assert safe_float(42) == 42.0

    def test_zero(self):
        assert safe_float(0) == 0.0

    def test_negative(self):
        assert safe_float(-99.99) == -99.99

    def test_bool_true(self):
        """bool is not in (None, '') so it tries float(True)=1.0"""
        assert safe_float(True) == 1.0

    def test_bool_false(self):
        """float(False) = 0.0"""
        assert safe_float(False) == 0.0


# ═════════════════════════════════════════════════════════════════════════
# 3. json_loads_safe edge cases
# ═════════════════════════════════════════════════════════════════════════

class TestJsonLoadsSafe:
    def test_none_returns_default(self):
        assert json_loads_safe(None, []) == []

    def test_empty_string_returns_default(self):
        assert json_loads_safe("", {}) == {}

    def test_valid_json_string(self):
        assert json_loads_safe('{"a": 1}', {}) == {"a": 1}

    def test_invalid_json_returns_default(self):
        assert json_loads_safe("not json", "fallback") == "fallback"

    def test_dict_passthrough(self):
        d = {"key": "val"}
        assert json_loads_safe(d, None) is d

    def test_list_passthrough(self):
        lst = [1, 2, 3]
        assert json_loads_safe(lst, None) is lst

    def test_int_as_string(self):
        """json.loads("42") = 42 — valid JSON."""
        assert json_loads_safe("42", None) == 42

    def test_nested_json(self):
        s = '{"a": {"b": [1,2,3]}}'
        result = json_loads_safe(s, {})
        assert result["a"]["b"] == [1, 2, 3]


# ═════════════════════════════════════════════════════════════════════════
# 4. first_present — the new field resolver
# ═════════════════════════════════════════════════════════════════════════

class TestFirstPresent:
    def test_first_key_present(self):
        assert first_present({"a": 1, "b": 2}, ["a", "b"]) == 1

    def test_first_key_none_falls_to_second(self):
        assert first_present({"a": None, "b": 2}, ["a", "b"]) == 2

    def test_first_key_empty_string_falls_to_second(self):
        assert first_present({"a": "", "b": "hello"}, ["a", "b"]) == "hello"

    def test_no_keys_present(self):
        assert first_present({"x": 1}, ["a", "b"], default="MISSING") == "MISSING"

    def test_zero_is_not_empty(self):
        """0 should be returned — it's not None or ''."""
        assert first_present({"a": 0}, ["a"]) == 0

    def test_false_is_not_empty(self):
        """False should be returned — it's not None or ''."""
        assert first_present({"a": False}, ["a"]) is False

    def test_empty_list_is_not_empty(self):
        """[] should be returned — it's not None or ''."""
        assert first_present({"a": []}, ["a"]) == []


# ═════════════════════════════════════════════════════════════════════════
# 5. SQL injection via document_id
# ═════════════════════════════════════════════════════════════════════════

class TestSqlInjection:
    """
    posting_builder uses f-strings for some SQL (table_columns, table_exists).
    Test that malicious inputs don't break things.
    """

    def _make_db(self, tmp_path) -> sqlite3.Connection:
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        return conn

    def test_document_id_with_sql_chars(self, tmp_path):
        """Document ID with SQL special characters."""
        conn = self._make_db(tmp_path)
        ensure_posting_job_table_minimum(conn)

        hostile_doc = {
            "document_id": "'; DROP TABLE posting_jobs; --",
            "vendor": "Test Vendor",
            "file_name": "test.pdf",
        }
        # Should not crash or drop the table
        try:
            result = upsert_posting_job(conn, document=hostile_doc)
        except Exception:
            pass  # ValueError is acceptable

        # Verify table still exists
        assert table_exists(conn, "posting_jobs"), "Table was dropped by injection!"
        conn.close()

    def test_table_columns_with_hostile_table_name(self, tmp_path):
        """
        table_columns uses f-string: f"PRAGMA table_info({table_name})"
        This is a potential injection vector.
        """
        conn = self._make_db(tmp_path)
        ensure_posting_job_table_minimum(conn)

        # This shouldn't crash — just return empty or raise
        try:
            cols = table_columns(conn, "posting_jobs; DROP TABLE posting_jobs")
        except Exception:
            pass

        # Table should still exist
        assert table_exists(conn, "posting_jobs"), "PRAGMA injection dropped the table!"
        conn.close()

    def test_vendor_with_unicode_injection(self, tmp_path):
        """Unicode in vendor name — should not corrupt DB."""
        conn = self._make_db(tmp_path)
        ensure_posting_job_table_minimum(conn)

        doc = {
            "document_id": "DOC-UNICODE-001",
            "vendor": "Société de l'énergie — «Hydro-Québec»",
            "file_name": "facture_hq.pdf",
        }
        result = upsert_posting_job(conn, document=doc)
        assert result  # Should succeed
        conn.close()


# ═════════════════════════════════════════════════════════════════════════
# 6. Upsert idempotency
# ═════════════════════════════════════════════════════════════════════════

class TestUpsertIdempotency:
    """
    Calling upsert twice with the same document_id should update, not
    create a duplicate.
    """

    def _make_db(self, tmp_path) -> sqlite3.Connection:
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        return conn

    def test_double_upsert_same_doc(self, tmp_path):
        conn = self._make_db(tmp_path)
        ensure_posting_job_table_minimum(conn)

        doc = {"document_id": "DOC-001", "vendor": "Bell", "file_name": "bell.pdf"}

        r1 = upsert_posting_job(conn, document=doc, memo="First insert")
        r2 = upsert_posting_job(conn, document=doc, memo="Second update")

        # Should be exactly 1 row
        count = conn.execute(
            "SELECT COUNT(*) as c FROM posting_jobs WHERE document_id = 'DOC-001'"
        ).fetchone()["c"]
        assert count == 1, f"Expected 1 row, got {count}"
        conn.close()

    def test_upsert_preserves_created_at(self, tmp_path):
        conn = self._make_db(tmp_path)
        ensure_posting_job_table_minimum(conn)

        doc = {"document_id": "DOC-002", "vendor": "Hydro", "file_name": "hydro.pdf"}
        r1 = upsert_posting_job(conn, document=doc)

        created_at = conn.execute(
            "SELECT created_at FROM posting_jobs WHERE document_id = 'DOC-002'"
        ).fetchone()["created_at"]

        r2 = upsert_posting_job(conn, document=doc, memo="update")

        created_at_after = conn.execute(
            "SELECT created_at FROM posting_jobs WHERE document_id = 'DOC-002'"
        ).fetchone()["created_at"]

        assert created_at == created_at_after, (
            f"created_at was mutated by update: {created_at} → {created_at_after}"
        )
        conn.close()


# ═════════════════════════════════════════════════════════════════════════
# 7. build_payload_from_sources null cascade
# ═════════════════════════════════════════════════════════════════════════

class TestBuildPayloadNullCascade:
    """
    What happens when both posting_row and document_row are empty dicts?
    Or when they have conflicting values?
    """

    def test_both_empty(self):
        payload = build_payload_from_sources({}, {})
        # Should not crash, should return mostly empty dict
        assert isinstance(payload, dict)

    def test_posting_overrides_document(self):
        """Posting row values should take priority over document row."""
        posting = {"vendor": "Posting Vendor", "posting_id": "P1"}
        document = {"vendor": "Document Vendor"}
        payload = build_payload_from_sources(posting, document)
        assert payload.get("vendor") == "Posting Vendor"

    def test_document_fills_gaps(self):
        """If posting row is missing a field, document row should fill it."""
        posting = {"posting_id": "P1"}
        document = {"vendor": "Doc Vendor", "amount": 99.99}
        payload = build_payload_from_sources(posting, document)
        assert payload.get("vendor") == "Doc Vendor"
        assert payload.get("amount") == 99.99

    def test_none_values_excluded(self):
        """None values should be excluded from the cleaned payload."""
        posting = {"posting_id": "P1", "vendor": None}
        document = {"vendor": None}
        payload = build_payload_from_sources(posting, document)
        assert "vendor" not in payload

    def test_blocking_issues_json_roundtrip(self):
        """blocking_issues stored as JSON string should parse back to list."""
        posting = {
            "posting_id": "P1",
            "blocking_issues": '["issue1", "issue2"]',
        }
        payload = build_payload_from_sources(posting, {})
        assert payload.get("blocking_issues") == ["issue1", "issue2"]

    def test_malformed_blocking_issues_json(self):
        """Corrupt JSON in blocking_issues should not crash."""
        posting = {
            "posting_id": "P1",
            "blocking_issues": "not valid json {{{",
        }
        payload = build_payload_from_sources(posting, {})
        # Should get empty list from normalize_string_list fallback
        assert payload.get("blocking_issues") == [] or "blocking_issues" not in payload


# ═════════════════════════════════════════════════════════════════════════
# 8. choose_default_memo
# ═════════════════════════════════════════════════════════════════════════

class TestChooseDefaultMemo:
    def test_all_present(self):
        memo = choose_default_memo({"vendor": "Bell", "doc_type": "invoice", "file_name": "bell.pdf"})
        assert "Bell" in memo
        assert "invoice" in memo
        assert "bell.pdf" in memo

    def test_all_none(self):
        memo = choose_default_memo({"vendor": None, "doc_type": None, "file_name": None})
        assert memo == ""

    def test_empty_dict(self):
        memo = choose_default_memo({})
        assert memo == ""

    def test_partial(self):
        memo = choose_default_memo({"vendor": "Hydro", "doc_type": None, "file_name": "scan.pdf"})
        assert "Hydro" in memo
        assert "scan.pdf" in memo


# ═════════════════════════════════════════════════════════════════════════
# 9. build_posting_id determinism
# ═════════════════════════════════════════════════════════════════════════

class TestBuildPostingId:
    def test_basic(self):
        pid = build_posting_id("DOC-001")
        assert pid == "post_qbo_expense_DOC-001"

    def test_custom_params(self):
        pid = build_posting_id("D1", entry_kind="bill", target_system="xero")
        assert pid == "post_xero_bill_D1"

    def test_whitespace_in_doc_id(self):
        pid = build_posting_id("  DOC 002  ")
        assert "DOC 002" in pid

    def test_empty_doc_id(self):
        pid = build_posting_id("")
        assert pid == "post_qbo_expense_"

    def test_none_entry_kind_defaults(self):
        pid = build_posting_id("D1", entry_kind=None)
        assert "expense" in pid
