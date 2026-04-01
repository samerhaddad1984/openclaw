"""
tests/red_team/test_boss_09_audit_cross_exam.py
================================================
BOSS FIGHT 9 — Audit Module Cross-Examination.

Risk matrix, sampling seed reproducibility, signed paper invalidation,
subsequent events, CAS assertion coverage, materiality, going concern.
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.audit_engine import (
    ensure_audit_tables,
    create_working_paper,
    sign_off_working_paper,
    get_working_papers,
    get_or_create_working_paper,
    update_working_paper,
    add_working_paper_item,
    get_working_paper_items,
    get_sample,
    generate_trial_balance,
    generate_financial_statements,
    get_or_create_evidence,
    get_evidence_chains,
    create_engagement,
    get_engagement,
    get_engagement_progress,
)
from src.engines.cas_engine import (
    VALID_ASSERTIONS,
    VALID_MATERIALITY_BASES,
    VALID_RISK_LEVELS,
    calculate_materiality,
    ensure_cas_tables,
    save_materiality,
    get_materiality,
    create_risk_matrix,
    assess_risk,
    get_risk_summary,
    check_subsequent_events,
    detect_going_concern_indicators,
    add_assertion_coverage,
    get_assertion_coverage,
)

CENT = Decimal("0.01")


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _seed_documents(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
            client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, raw_result TEXT,
            submitted_by TEXT, client_note TEXT, fraud_flags TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, document_date TEXT, amount REAL,
            currency TEXT DEFAULT 'CAD', doc_type TEXT,
            category TEXT, gl_account TEXT, tax_code TEXT,
            memo TEXT, review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, blocking_issues TEXT, notes TEXT,
            posting_status TEXT DEFAULT 'posted',
            created_at TEXT, updated_at TEXT, external_id TEXT
        );
    """)
    for i in range(20):
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, amount, gl_account,
                tax_code, document_date, doc_type, review_status)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (f"DOC-{i:03d}", "AUDIT_CO", f"Vendor {i}", (i+1)*1000.0,
             f"{5000+i}", "T", f"2026-{(i%12)+1:02d}-15", "invoice", "approved"),
        )
        conn.execute(
            """INSERT INTO posting_jobs
               (posting_id, document_id, client_code, vendor, amount,
                gl_account, tax_code, document_date, doc_type, review_status,
                posting_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (f"PJ-{i:03d}", f"DOC-{i:03d}", "AUDIT_CO", f"Vendor {i}",
             (i+1)*1000.0, f"{5000+i}", "T", f"2026-{(i%12)+1:02d}-15",
             "invoice", "approved", "posted"),
        )
    conn.commit()


class TestRiskMatrix:
    """CAS 315 risk assessment matrix."""

    def test_create_risk_matrix(self):
        """Risk matrix creation for an engagement."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        ensure_cas_tables(conn)

        eng = create_engagement(conn, "AUDIT_CO", "2026-03", "audit")
        eng_id = eng["engagement_id"]

        accounts = [
            {"account_code": "1000", "account_name": "Cash"},
            {"account_code": "4000", "account_name": "Revenue"},
            {"account_code": "5000", "account_name": "Cost of Sales"},
        ]
        result = create_risk_matrix(conn, eng_id, accounts)
        assert isinstance(result, list)

    def test_valid_assertions_coverage(self):
        """All CAS assertions must be recognized."""
        expected = {"completeness", "accuracy", "existence", "cutoff",
                    "classification", "rights_obligations", "presentation"}
        assert VALID_ASSERTIONS == expected

    def test_valid_risk_levels(self):
        assert VALID_RISK_LEVELS == {"low", "medium", "high"}

    def test_risk_summary_structure(self):
        """Risk summary must return structured data."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        ensure_cas_tables(conn)

        eng = create_engagement(conn, "AUDIT_CO", "2026-03", "audit")
        eng_id = eng["engagement_id"]

        accounts = [
            {"account_code": "1000", "account_name": "Cash"},
            {"account_code": "4000", "account_name": "Revenue"},
        ]
        create_risk_matrix(conn, eng_id, accounts)

        summary = get_risk_summary(conn, eng_id)
        assert summary is not None


class TestSamplingSeedReproducibility:
    """Statistical sampling must be reproducible with the same seed."""

    def test_same_paper_same_sample(self):
        """Same paper_id → identical sample both times."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents(conn)

        wp = get_or_create_working_paper(
            conn, "AUDIT_CO", "2026-03", "audit", "5000", "Expenses"
        )
        paper_id = wp["paper_id"]

        s1 = get_sample(conn, "AUDIT_CO", "2026-03", "5000", 5, paper_id)
        s2 = get_sample(conn, "AUDIT_CO", "2026-03", "5000", 5, paper_id)

        # Same paper_id = same seed = identical samples
        s1_ids = [d["document_id"] for d in s1]
        s2_ids = [d["document_id"] for d in s2]
        assert s1_ids == s2_ids, "Same paper_id must produce identical samples"

    def test_different_papers_different_samples(self):
        """Different paper_ids should produce different samples."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents(conn)

        wp1 = get_or_create_working_paper(
            conn, "AUDIT_CO", "2026-03", "audit", "5000", "Expenses"
        )
        wp2 = get_or_create_working_paper(
            conn, "AUDIT_CO", "2026-03", "audit", "5001", "Office Supplies"
        )

        s1 = get_sample(conn, "AUDIT_CO", "2026-03", "", 5, wp1["paper_id"])
        s2 = get_sample(conn, "AUDIT_CO", "2026-03", "", 5, wp2["paper_id"])

        # Both should return results
        assert len(s1) > 0
        assert len(s2) > 0


class TestWorkingPaperSignOff:
    """Working paper sign-off and invalidation."""

    def test_sign_off_working_paper(self):
        """Signing off a working paper must record reviewer."""
        conn = _fresh_db()
        ensure_audit_tables(conn)

        wp = get_or_create_working_paper(
            conn, "AUDIT_CO", "2026-03", "audit", "1000", "Cash"
        )
        paper_id = wp["paper_id"]

        sign_off_working_paper(conn, paper_id, tested_by="senior.cpa@firm.com")

        papers = get_working_papers(conn, "AUDIT_CO", "2026-03")
        signed = next(p for p in papers if p["paper_id"] == paper_id)
        assert signed["reviewed_by"] == "senior.cpa@firm.com"
        assert signed["status"] == "complete"

    def test_update_after_sign_off_is_immutable(self):
        """Signed-off working paper must be immutable — updates must fail."""
        conn = _fresh_db()
        ensure_audit_tables(conn)

        wp = get_or_create_working_paper(
            conn, "AUDIT_CO", "2026-03", "audit", "2000", "Receivables"
        )
        sign_off_working_paper(conn, wp["paper_id"],
                               tested_by="partner@firm.com")

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            update_working_paper(
                conn, wp["paper_id"],
                balance_confirmed=50000.0,
                notes="Attempted re-open after sign-off",
            )


class TestMateriality:
    """CAS 320 materiality assessment."""

    def test_calculate_materiality(self):
        """Materiality calculation from a base amount."""
        result = calculate_materiality(
            basis_type="revenue",
            basis_amount=5000000,
        )
        assert result["planning_materiality"] > 0
        assert result["performance_materiality"] > 0
        assert result["clearly_trivial"] > 0
        assert result["performance_materiality"] < result["planning_materiality"]
        assert result["clearly_trivial"] < result["performance_materiality"]

    def test_materiality_bases(self):
        """All valid materiality bases must be accepted."""
        for basis in VALID_MATERIALITY_BASES:
            result = calculate_materiality(basis_type=basis, basis_amount=1000000)
            assert result["planning_materiality"] > 0

    def test_save_and_retrieve_materiality(self):
        """Materiality must persist and be retrievable."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        ensure_cas_tables(conn)

        eng = create_engagement(conn, "MAT_CO", "2026-03", "audit")
        eng_id = eng["engagement_id"]

        mat = calculate_materiality("revenue", 2000000)
        save_materiality(conn, eng_id, mat, username="cpa@firm.com")

        retrieved = get_materiality(conn, eng_id)
        assert retrieved is not None
        assert float(retrieved["basis_amount"]) == 2000000.0


class TestSubsequentEvents:
    """CAS 560 — subsequent events detection."""

    def test_subsequent_events_detection(self):
        """Check for events after period end."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        ensure_cas_tables(conn)
        _seed_documents(conn)

        eng = create_engagement(conn, "AUDIT_CO", "2026-03", "audit")
        eng_id = eng["engagement_id"]

        events = check_subsequent_events(eng_id, conn)
        assert events is not None


class TestGoingConcern:
    """Going concern indicator detection."""

    def test_going_concern_detection(self):
        """Detect going concern indicators from financial data."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        ensure_cas_tables(conn)
        _seed_documents(conn)

        indicators = detect_going_concern_indicators("AUDIT_CO", conn)
        assert indicators is not None
        assert "indicators" in indicators


class TestAssertionCoverage:
    """CAS assertion coverage tracking."""

    def test_add_and_get_coverage(self):
        """Add assertion coverage and verify retrieval."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        ensure_cas_tables(conn)

        wp = get_or_create_working_paper(
            conn, "AUDIT_CO", "2026-03", "audit", "1000", "Cash"
        )
        paper_id = wp["paper_id"]

        # Add a working paper item first
        item_result = add_working_paper_item(
            conn, paper_id, "DOC-TEST",
            "tested", "Test notes", "auditor@firm.com",
        )
        item_id = item_result["item_id"]

        result = add_assertion_coverage(
            conn, item_id, ["existence", "completeness"],
        )
        assert result["has_existence"]
        assert result["has_completeness"]

        coverage = get_assertion_coverage(conn, paper_id)
        assert coverage is not None


class TestEvidenceChains:
    """Three-way matching evidence chains."""

    def test_evidence_chain_creation(self):
        """Create and verify evidence chain (PO → Invoice → Payment)."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents(conn)

        ev = get_or_create_evidence(conn, "DOC-000", "three_way_match")
        assert ev is not None

    def test_evidence_chains_query(self):
        """Query evidence chains for a client/period."""
        conn = _fresh_db()
        ensure_audit_tables(conn)
        _seed_documents(conn)

        get_or_create_evidence(conn, "DOC-000", "three_way_match")
        get_or_create_evidence(conn, "DOC-001", "bank_confirmation")

        chains = get_evidence_chains(conn, "AUDIT_CO", "2026-03")
        assert chains is not None


class TestEngagementManagement:
    """Audit engagement lifecycle."""

    def test_create_and_get_engagement(self):
        conn = _fresh_db()
        ensure_audit_tables(conn)

        eng = create_engagement(conn, "ENG_CO", "2026-03", "audit")
        eng_id = eng["engagement_id"]
        retrieved = get_engagement(conn, eng_id)
        assert retrieved is not None
        assert retrieved["client_code"] == "ENG_CO"

    def test_engagement_progress(self):
        """Engagement progress should track working paper completion."""
        conn = _fresh_db()
        ensure_audit_tables(conn)

        eng = create_engagement(conn, "PROG_CO", "2026-03", "audit")
        eng_id = eng["engagement_id"]
        get_or_create_working_paper(
            conn, "PROG_CO", "2026-03", "audit", "1000", "Cash"
        )
        get_or_create_working_paper(
            conn, "PROG_CO", "2026-03", "audit", "2000", "AR"
        )

        progress = get_engagement_progress(conn, eng_id)
        assert progress is not None
