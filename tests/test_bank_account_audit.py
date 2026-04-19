"""Sprint G F3 — bank-account-change audit trail tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.bank_account_audit import (  # noqa: E402
    detect_rapid_bank_changes,
    diff_bank_connection,
    ensure_bank_account_audit_table,
    get_bank_audit_trail,
    mask_account,
    record_bank_change,
)


def test_mask_account_short():
    assert mask_account("123") == "***"


def test_mask_account_long():
    assert mask_account("987654321") == "*****4321"


def test_mask_account_empty():
    assert mask_account("") == ""


def test_record_inserts_row(tmp_path):
    db = tmp_path / "b.db"
    rid = record_bank_change(
        firm_code="F1", client_code="C1", action="added",
        account_masked="****1234", changed_by="alice",
        db_path=db,
    )
    assert rid > 0
    rows = get_bank_audit_trail(client_code="C1", db_path=db)
    assert len(rows) == 1
    assert rows[0]["account_masked"] == "****1234"


def test_record_invalid_action_raises(tmp_path):
    db = tmp_path / "b.db"
    with pytest.raises(ValueError):
        record_bank_change(
            firm_code="F1", client_code="C1", action="bogus",
            db_path=db,
        )


def test_record_missing_client_raises(tmp_path):
    db = tmp_path / "b.db"
    with pytest.raises(ValueError):
        record_bank_change(
            firm_code="F1", client_code="", action="added", db_path=db,
        )


def test_audit_trail_ordered_newest_first(tmp_path):
    db = tmp_path / "b.db"
    import time
    for i in range(3):
        record_bank_change(
            firm_code="F1", client_code="C1", action="modified",
            account_masked=f"****000{i}", changed_by=f"u{i}",
            db_path=db,
        )
        time.sleep(0.01)
    rows = get_bank_audit_trail(client_code="C1", db_path=db)
    assert len(rows) == 3
    # Newest first => last inserted comes first.
    assert rows[0]["account_masked"] == "****0002"


def test_rapid_bank_change_detection_fires(tmp_path):
    db = tmp_path / "b.db"
    for i in range(3):
        record_bank_change(
            firm_code="F1", client_code="C1", action="modified",
            account_masked=f"****000{i}", db_path=db,
        )
    findings = detect_rapid_bank_changes(client_code="C1", days_back=7,
                                          max_changes=1, db_path=db)
    assert len(findings) == 1
    f = findings[0]
    assert f["type"] == "rapid_bank_account_change"
    assert f["change_count"] == 3
    assert f["severity"] == "high"


def test_single_change_not_flagged(tmp_path):
    db = tmp_path / "b.db"
    record_bank_change(firm_code="F1", client_code="C1", action="added",
                        db_path=db)
    findings = detect_rapid_bank_changes(client_code="C1", db_path=db)
    assert findings == []


def test_client_isolation(tmp_path):
    db = tmp_path / "b.db"
    for i in range(3):
        record_bank_change(firm_code="F1", client_code="C1",
                           action="modified", db_path=db)
    record_bank_change(firm_code="F1", client_code="OTHER",
                       action="added", db_path=db)
    f_c1 = detect_rapid_bank_changes(client_code="C1", db_path=db)
    f_other = detect_rapid_bank_changes(client_code="OTHER", db_path=db)
    assert len(f_c1) == 1
    assert f_other == []


def test_diff_bank_connection_added():
    assert diff_bank_connection(None, {"institution_name": "RBC"}) == "added"


def test_diff_bank_connection_removed():
    assert diff_bank_connection({"institution_name": "RBC"}, None) == "removed"


def test_diff_bank_connection_field_change():
    summary = diff_bank_connection(
        {"institution_name": "RBC", "account_type": "checking"},
        {"institution_name": "TD", "account_type": "checking"},
    )
    assert "institution_name" in summary
    assert "RBC" in summary and "TD" in summary
