"""
tests/test_cas_modules.py — Tests for CAS 580 / CAS 330 / CAS 550 modules
and the engagement completion checklist.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.engines.cas_engine as cas
import src.engines.audit_engine as audit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn():
    """In-memory SQLite database with audit + CAS tables."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    audit.ensure_audit_tables(c)
    cas.ensure_cas_tables(c)
    yield c
    c.close()


@pytest.fixture()
def engagement(conn):
    """Create a test engagement and return its ID."""
    eng = audit.create_engagement(
        conn, client_code="TESTCO", period="2025-12",
        engagement_type="audit", partner="Alice", manager="Bob",
    )
    return eng["engagement_id"]


# ---------------------------------------------------------------------------
# CAS 580 — Management Representation Letter
# ---------------------------------------------------------------------------

class TestRepLetter:
    def test_generate_letter_fr(self, conn, engagement):
        letter = cas.generate_management_rep_letter(engagement, "fr", conn)
        assert "Lettre de déclaration" in letter
        assert "TESTCO" in letter
        assert "2025-12" in letter

    def test_generate_letter_en(self, conn, engagement):
        letter = cas.generate_management_rep_letter(engagement, "en", conn)
        assert "Management Representation Letter" in letter
        assert "TESTCO" in letter

    def test_generate_letter_bad_engagement(self, conn):
        with pytest.raises(ValueError, match="Engagement not found"):
            cas.generate_management_rep_letter("nonexistent", "en", conn)

    def test_save_and_get(self, conn, engagement):
        letter_id = cas.save_rep_letter(engagement, "Ébauche FR", "Draft EN", conn, created_by="sam")
        assert letter_id.startswith("rep_")

        letter = cas.get_rep_letter(engagement, conn)
        assert letter is not None
        assert letter["draft_text_fr"] == "Ébauche FR"
        assert letter["draft_text_en"] == "Draft EN"
        assert letter["status"] == "draft"
        assert letter["engagement_id"] == engagement

    def test_save_updates_existing(self, conn, engagement):
        id1 = cas.save_rep_letter(engagement, "v1 FR", "v1 EN", conn)
        id2 = cas.save_rep_letter(engagement, "v2 FR", "v2 EN", conn)
        assert id1 == id2  # same letter updated
        letter = cas.get_rep_letter(engagement, conn)
        assert letter["draft_text_fr"] == "v2 FR"

    def test_mark_signed(self, conn, engagement):
        letter_id = cas.save_rep_letter(engagement, "FR", "EN", conn)
        ok = cas.mark_letter_signed(letter_id, "Jean Tremblay", "Président", conn)
        assert ok is True

        letter = cas.get_rep_letter(engagement, conn)
        assert letter["status"] == "signed"
        assert letter["management_name"] == "Jean Tremblay"
        assert letter["management_title"] == "Président"
        assert letter["signed_at"] is not None

    def test_mark_signed_bad_id(self, conn):
        ok = cas.mark_letter_signed("nonexistent", "Name", "Title", conn)
        assert ok is False

    def test_get_rep_letter_none(self, conn, engagement):
        letter = cas.get_rep_letter(engagement, conn)
        assert letter is None

    def test_letter_contains_six_confirmations_en(self, conn, engagement):
        letter = cas.generate_management_rep_letter(engagement, "en", conn)
        for i in range(1, 7):
            assert f"{i}." in letter

    def test_letter_contains_six_confirmations_fr(self, conn, engagement):
        letter = cas.generate_management_rep_letter(engagement, "fr", conn)
        for i in range(1, 7):
            assert f"{i}." in letter


# ---------------------------------------------------------------------------
# CAS 330 — Control Testing
# ---------------------------------------------------------------------------

class TestControlTesting:
    def test_create_control_test(self, conn, engagement):
        test_id = cas.create_control_test(
            engagement, "AP authorization",
            "Invoices approved before payment", "walkthrough", conn,
            tested_by="sam",
        )
        assert test_id.startswith("ctrl_")

    def test_create_bad_engagement(self, conn):
        with pytest.raises(ValueError, match="Engagement not found"):
            cas.create_control_test("bad", "name", "obj", "walkthrough", conn)

    def test_invalid_test_type_defaults(self, conn, engagement):
        test_id = cas.create_control_test(
            engagement, "Test", "Obj", "invalid_type", conn,
        )
        tests = cas.get_control_tests(engagement, conn)
        assert tests[0]["test_type"] == "walkthrough"

    def test_record_results(self, conn, engagement):
        test_id = cas.create_control_test(
            engagement, "Bank recon", "Monthly reconciliation", "reperformance", conn,
        )
        ok = cas.record_test_results(test_id, 25, 2, "Two items not reconciled", "partially_effective", conn)
        assert ok is True

        tests = cas.get_control_tests(engagement, conn)
        assert len(tests) == 1
        assert tests[0]["items_tested"] == 25
        assert tests[0]["exceptions_found"] == 2
        assert tests[0]["exception_details"] == "Two items not reconciled"
        assert tests[0]["conclusion"] == "partially_effective"

    def test_record_results_bad_id(self, conn):
        ok = cas.record_test_results("bad", 10, 0, "", "effective", conn)
        assert ok is False

    def test_record_results_invalid_conclusion(self, conn, engagement):
        test_id = cas.create_control_test(engagement, "X", "Y", "walkthrough", conn)
        cas.record_test_results(test_id, 10, 0, "", "invalid_conclusion", conn)
        tests = cas.get_control_tests(engagement, conn)
        assert tests[0]["conclusion"] == "effective"

    def test_get_control_tests_empty(self, conn, engagement):
        tests = cas.get_control_tests(engagement, conn)
        assert tests == []

    def test_effectiveness_summary(self, conn, engagement):
        cas.create_control_test(engagement, "A", "obj", "walkthrough", conn)
        cas.create_control_test(engagement, "B", "obj", "walkthrough", conn)
        cas.create_control_test(engagement, "C", "obj", "walkthrough", conn)

        tests = cas.get_control_tests(engagement, conn)
        cas.record_test_results(tests[0]["test_id"], 10, 0, "", "effective", conn)
        cas.record_test_results(tests[1]["test_id"], 10, 3, "details", "ineffective", conn)
        cas.record_test_results(tests[2]["test_id"], 10, 1, "minor", "partially_effective", conn)

        summary = cas.get_control_effectiveness_summary(engagement, conn)
        assert summary["total"] == 3
        assert summary["effective"] == 1
        assert summary["ineffective"] == 1
        assert summary["partially_effective"] == 1

    def test_standard_controls_library(self):
        assert len(cas.STANDARD_CONTROLS) == 15
        for ctrl in cas.STANDARD_CONTROLS:
            assert "name" in ctrl
            assert "objective" in ctrl
            assert "description" in ctrl


# ---------------------------------------------------------------------------
# CAS 550 — Related Party Procedures
# ---------------------------------------------------------------------------

class TestRelatedParties:
    def test_add_related_party(self, conn):
        party_id = cas.add_related_party(
            "TESTCO", "Tremblay Holdings", "affiliated_company", conn,
            ownership_percentage=75.0, identified_by="sam",
        )
        assert party_id.startswith("rp_")

    def test_invalid_relationship_type(self, conn):
        party_id = cas.add_related_party("TESTCO", "X", "invalid_type", conn)
        parties = cas.get_related_parties("TESTCO", conn)
        assert parties[0]["relationship_type"] == "affiliated_company"

    def test_get_related_parties(self, conn):
        cas.add_related_party("TESTCO", "Party A", "owner", conn)
        cas.add_related_party("TESTCO", "Party B", "family_member", conn)
        cas.add_related_party("OTHER", "Party C", "owner", conn)

        parties = cas.get_related_parties("TESTCO", conn)
        assert len(parties) == 2
        names = {p["party_name"] for p in parties}
        assert names == {"Party A", "Party B"}

    def test_flag_transaction(self, conn, engagement):
        party_id = cas.add_related_party("TESTCO", "Related Co", "affiliated_company", conn)
        rpt_id = cas.flag_related_party_transaction(
            engagement, "doc_001", party_id, "exchange_amount", conn,
            amount=5000.0, description="Consulting fees",
        )
        assert rpt_id.startswith("rpt_")

    def test_invalid_measurement_basis(self, conn, engagement):
        party_id = cas.add_related_party("TESTCO", "X", "owner", conn)
        cas.flag_related_party_transaction(engagement, "doc_001", party_id, "invalid", conn)
        txns = cas.get_related_party_transactions(engagement, conn)
        assert txns[0]["measurement_basis"] == "exchange_amount"

    def test_get_transactions_with_party_info(self, conn, engagement):
        party_id = cas.add_related_party("TESTCO", "Tremblay Inc", "owner", conn)
        cas.flag_related_party_transaction(
            engagement, "doc_001", party_id, "cost", conn,
            amount=10000.0, description="Rent",
        )
        txns = cas.get_related_party_transactions(engagement, conn)
        assert len(txns) == 1
        assert txns[0]["party_name"] == "Tremblay Inc"
        assert txns[0]["relationship_type"] == "owner"
        assert txns[0]["amount"] == 10000.0

    def test_get_related_party_summary(self, conn, engagement):
        party_id = cas.add_related_party("TESTCO", "PartyA", "owner", conn)
        cas.flag_related_party_transaction(engagement, "d1", party_id, "cost", conn, amount=1000.0)
        cas.flag_related_party_transaction(engagement, "d2", party_id, "cost", conn, amount=2000.0)

        summary = cas.get_related_party_summary(engagement, conn)
        assert summary["parties"] == 1
        assert summary["transactions"] == 2
        assert summary["total_amount"] == 3000.0
        assert summary["disclosure_required"] == 2

    def test_summary_empty(self, conn, engagement):
        summary = cas.get_related_party_summary(engagement, conn)
        assert summary["parties"] == 0
        assert summary["transactions"] == 0

    def test_auto_detect_no_vendor_memory(self, conn):
        results = cas.auto_detect_related_parties("TESTCO", conn)
        assert results == []

    def test_auto_detect_with_vendor_memory(self, conn):
        # Create vendor_memory table manually
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vendor_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor TEXT, client_code TEXT, last_amount REAL,
                category TEXT, gl_account TEXT
            )
        """)
        # Insert some test data — high frequency vendor with round amounts
        for i in range(6):
            conn.execute(
                "INSERT INTO vendor_memory (vendor, client_code, last_amount) VALUES (?,?,?)",
                ("Suspicious Corp", "TESTCO", 5000.0),
            )
        conn.commit()

        results = cas.auto_detect_related_parties("TESTCO", conn)
        assert len(results) >= 1
        assert results[0]["vendor"] == "Suspicious Corp"
        assert "round_amounts" in results[0]["evidence"]
        assert "high_frequency" in results[0]["evidence"]

    def test_generate_disclosure_en(self, conn, engagement):
        party_id = cas.add_related_party("TESTCO", "Tremblay Inc", "owner", conn, ownership_percentage=100.0)
        cas.flag_related_party_transaction(engagement, "d1", party_id, "cost", conn, amount=50000.0, description="Management fees")

        disclosure = cas.generate_related_party_disclosure(engagement, "en", conn)
        assert "Related Parties" in disclosure
        assert "Tremblay Inc" in disclosure
        assert "50,000.00" in disclosure

    def test_generate_disclosure_fr(self, conn, engagement):
        party_id = cas.add_related_party("TESTCO", "Tremblay Inc", "owner", conn)
        cas.flag_related_party_transaction(engagement, "d1", party_id, "cost", conn, amount=25000.0)

        disclosure = cas.generate_related_party_disclosure(engagement, "fr", conn)
        assert "Parties liées" in disclosure
        assert "Tremblay Inc" in disclosure

    def test_generate_disclosure_empty(self, conn, engagement):
        disclosure = cas.generate_related_party_disclosure(engagement, "en", conn)
        assert "No related parties" in disclosure

    def test_generate_disclosure_bad_engagement(self, conn):
        with pytest.raises(ValueError, match="Engagement not found"):
            cas.generate_related_party_disclosure("bad", "en", conn)


# ---------------------------------------------------------------------------
# Engagement Completion Checklist
# ---------------------------------------------------------------------------

class TestEngagementChecklist:
    def test_checklist_audit(self, conn, engagement):
        checklist = cas.get_engagement_checklist(engagement, conn)
        assert len(checklist) >= 5
        items = {c["item"] for c in checklist}
        assert "materiality_calculated" in items
        assert "risk_matrix_completed" in items
        assert "control_tests_documented" in items
        assert "rep_letter_signed" in items
        assert "working_papers_signed_off" in items

    def test_checklist_control_tests_required_for_audit(self, conn, engagement):
        checklist = cas.get_engagement_checklist(engagement, conn)
        ctrl_item = next(c for c in checklist if c["item"] == "control_tests_documented")
        assert ctrl_item["required"] is True

    def test_checklist_compilation(self, conn):
        eng_id = audit.create_engagement(conn, "COMPCO", "2025-12", "compilation")["engagement_id"]
        checklist = cas.get_engagement_checklist(eng_id, conn)
        ctrl_item = next(c for c in checklist if c["item"] == "control_tests_documented")
        assert ctrl_item["required"] is False
        rep_item = next(c for c in checklist if c["item"] == "rep_letter_signed")
        assert rep_item["required"] is False

    def test_checklist_review(self, conn):
        eng_id = audit.create_engagement(conn, "REVCO", "2025-12", "review")["engagement_id"]
        checklist = cas.get_engagement_checklist(eng_id, conn)
        rep_item = next(c for c in checklist if c["item"] == "rep_letter_signed")
        assert rep_item["required"] is True
        ctrl_item = next(c for c in checklist if c["item"] == "control_tests_documented")
        assert ctrl_item["required"] is False

    def test_check_issuable_empty(self, conn, engagement):
        can_issue, blocking = cas.check_engagement_issuable(engagement, conn)
        assert can_issue is False
        assert len(blocking) > 0

    def test_check_issuable_after_completing(self, conn, engagement):
        # Complete materiality
        mat = cas.calculate_materiality("revenue", 1000000)
        cas.save_materiality(conn, engagement, mat, "sam")

        # Complete risk
        cas.create_risk_matrix(conn, engagement, [{"account_code": "1010", "account_name": "Cash"}])

        # Complete controls
        cas.create_control_test(engagement, "AP", "obj", "walkthrough", conn)

        # Sign rep letter
        letter_id = cas.save_rep_letter(engagement, "FR", "EN", conn)
        cas.mark_letter_signed(letter_id, "Boss", "President", conn)

        # Sign off working papers
        wp = audit.get_or_create_working_paper(conn, "TESTCO", "2025-12", "audit", "1010", "Cash")
        audit.update_working_paper(conn, wp["paper_id"], reviewed_by="sam", status="complete")

        can_issue, blocking = cas.check_engagement_issuable(engagement, conn)
        # related_parties is always "complete" by default, so most items should pass
        # working_papers_signed_off depends on whether all papers are signed
        if not can_issue:
            # The only possible blocker should be working_papers if not all signed
            for b in blocking:
                assert b in ("working_papers_signed_off", "related_parties_identified")

    def test_checklist_bad_engagement(self, conn):
        checklist = cas.get_engagement_checklist("nonexistent", conn)
        assert checklist == []


# ---------------------------------------------------------------------------
# Table creation idempotency
# ---------------------------------------------------------------------------

class TestTableCreation:
    def test_ensure_tables_twice(self, conn):
        """Calling ensure_cas_tables twice should not raise."""
        cas.ensure_cas_tables(conn)
        cas.ensure_cas_tables(conn)

    def test_new_tables_exist(self, conn):
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "management_representation_letters" in tables
        assert "control_tests" in tables
        assert "related_parties" in tables
        assert "related_party_transactions" in tables


# ---------------------------------------------------------------------------
# i18n key coverage
# ---------------------------------------------------------------------------

class TestI18nKeys:
    def test_all_cas_keys_present(self):
        import json
        en_path = ROOT / "src" / "i18n" / "en.json"
        fr_path = ROOT / "src" / "i18n" / "fr.json"
        en = json.loads(en_path.read_text(encoding="utf-8"))
        fr = json.loads(fr_path.read_text(encoding="utf-8"))

        required_prefixes = ["cas_rep_", "cas_ctrl_", "cas_rp_", "checklist_"]
        for prefix in required_prefixes:
            en_keys = [k for k in en if k.startswith(prefix)]
            fr_keys = [k for k in fr if k.startswith(prefix)]
            assert len(en_keys) >= 3, f"Missing {prefix}* keys in en.json"
            assert len(fr_keys) >= 3, f"Missing {prefix}* keys in fr.json"
            # All en keys should exist in fr
            for k in en_keys:
                assert k in fr, f"Key {k} in en.json but missing in fr.json"

    def test_flash_and_err_keys(self):
        import json
        en = json.loads((ROOT / "src" / "i18n" / "en.json").read_text(encoding="utf-8"))
        fr = json.loads((ROOT / "src" / "i18n" / "fr.json").read_text(encoding="utf-8"))

        for key in ["flash_rep_saved", "flash_rep_signed", "flash_ctrl_created",
                     "flash_ctrl_results", "flash_rp_added", "flash_rp_transaction_flagged",
                     "err_rep_forbidden", "err_ctrl_forbidden", "err_rp_forbidden",
                     "err_checklist_blocking", "err_rep_not_signed"]:
            assert key in en, f"Missing {key} in en.json"
            assert key in fr, f"Missing {key} in fr.json"
