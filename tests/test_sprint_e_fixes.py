"""Sprint E Phase 2 — opportunistic fixes.

Covers:
- Fix 6: materiality reassessment is allowed with an explicit reason.
- Fix 9: PyMuPDF PDF paths degrade to the minimal fallback when the
         layout/font layer raises (instead of 500-erroring the route).

Fix 7 (_get_account_risk_profile) was a false positive from the Sprint D
audit — the function exists at cas_engine.py:476. Verified by import.

Fix 10 (_mark_as_filed audit trail) was also a false positive — it is
wired at /calendar/mark_filed. Verified by grep.
"""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Fix 6 — materiality reassessment
# ---------------------------------------------------------------------------

@pytest.fixture
def cas_db(tmp_path, monkeypatch):
    from src.engines import cas_engine as ce
    from src.engines import audit_engine as ae

    path = tmp_path / "cas.db"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # ensure_audit_tables creates engagements + working_papers (the latter
    # is a prerequisite for the cascade-lock trigger created in
    # ensure_cas_tables).
    ae.ensure_audit_tables(conn)
    conn.execute(
        "INSERT INTO engagements (engagement_id, client_code, period, "
        "engagement_type, created_at) VALUES (?, ?, ?, ?, datetime('now'))",
        ("eng1", "ACME", "2026-04", "review"),
    )
    conn.commit()
    ce.ensure_cas_tables(conn)
    return conn, ce, ae


def _mat(basis="pretax_income", amt=1_000_000.0):
    return {
        "basis": basis, "basis_amount": amt,
        "planning_materiality": amt * 0.05,
        "performance_materiality": amt * 0.05 * 0.75,
        "clearly_trivial": amt * 0.05 * 0.05,
    }


def test_first_materiality_assessment_saves_without_reason(cas_db):
    conn, ce, _ = cas_db
    aid = ce.save_materiality(conn, "eng1", _mat(), username="sam")
    assert aid.startswith("mat_")
    row = conn.execute(
        "SELECT supersedes_assessment_id, reassessment_reason "
        "FROM materiality_assessments WHERE assessment_id=?",
        (aid,),
    ).fetchone()
    assert row["supersedes_assessment_id"] is None
    assert row["reassessment_reason"] is None


def test_reassessment_requires_reason(cas_db):
    conn, ce, _ = cas_db
    ce.save_materiality(conn, "eng1", _mat(), username="sam")
    with pytest.raises(ValueError, match="reassessment_reason_required"):
        ce.save_materiality(conn, "eng1", _mat(amt=2_000_000), username="sam")


def test_reassessment_with_reason_chains_previous(cas_db):
    conn, ce, _ = cas_db
    first = ce.save_materiality(conn, "eng1", _mat(), username="sam")
    second = ce.save_materiality(
        conn, "eng1", _mat(amt=2_000_000.0),
        username="sam",
        reassessment_reason="Revenue dropped 40% mid-year; lowering benchmark.",
    )
    assert first != second
    row = conn.execute(
        "SELECT supersedes_assessment_id, reassessment_reason, basis_amount "
        "FROM materiality_assessments WHERE assessment_id=?",
        (second,),
    ).fetchone()
    assert row["supersedes_assessment_id"] == first
    assert "Revenue dropped" in row["reassessment_reason"]
    assert row["basis_amount"] == pytest.approx(2_000_000.0)


def test_get_materiality_returns_most_recent(cas_db):
    conn, ce, _ = cas_db
    ce.save_materiality(conn, "eng1", _mat(amt=1_000_000), username="sam")
    ce.save_materiality(
        conn, "eng1", _mat(amt=3_000_000),
        username="sam",
        reassessment_reason="Acquisition increased revenue base.",
    )
    current = ce.get_materiality(conn, "eng1")
    assert current is not None
    assert current["basis_amount"] == pytest.approx(3_000_000.0)


def test_old_one_per_engagement_trigger_is_gone(cas_db):
    conn, _, _ = cas_db
    row = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='trigger' AND name='trg_materiality_one_per_engagement'"
    ).fetchone()
    assert row is None, "one-per-engagement trigger should have been dropped"


def test_locked_row_is_still_immutable(cas_db):
    """Reassessment is OK (new row); editing a locked row is still blocked."""
    conn, ce, _ = cas_db
    aid = ce.save_materiality(conn, "eng1", _mat(), username="sam")
    conn.execute(
        "UPDATE materiality_assessments SET materiality_locked = 1 "
        "WHERE assessment_id = ?", (aid,),
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE materiality_assessments SET basis_amount = 42 "
            "WHERE assessment_id = ?", (aid,),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Fix 9 — PDF generators degrade gracefully on PyMuPDF exceptions
# ---------------------------------------------------------------------------

def test_financial_statements_pdf_uses_minimal_fallback_on_pymupdf_error():
    from src.engines import audit_engine as ae

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Minimal schema so generate_trial_balance's LEFT JOIN works.
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, client_code TEXT,
            document_date TEXT, gl_account TEXT, amount REAL,
            review_status TEXT, vendor TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            posting_status TEXT, created_at TEXT, updated_at TEXT
        );
    """)
    conn.commit()

    calls = []

    def explode(*a, **kw):
        raise RuntimeError("pretend a font lookup failed")

    def minimal(*a, **kw):
        calls.append("minimal")
        return b"%PDF-1.4 minimal stub"

    with patch.object(ae, "_fs_pdf_pymupdf", explode), \
         patch.object(ae, "_fs_pdf_minimal", minimal):
        out = ae.generate_financial_statements_pdf(
            conn, "ACME", "2026-04", firm_name="OtoCPA", lang="en",
        )
    assert out.startswith(b"%PDF")
    assert calls == ["minimal"]


def test_analytical_pdf_falls_back_on_pymupdf_error():
    from src.engines import audit_engine as ae

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # run_analytical_procedures needs the trial_balance + chart_of_accounts.
    ae.ensure_audit_tables(conn)

    calls = []

    with patch.object(ae, "_analytical_pdf_pymupdf",
                       side_effect=RuntimeError("layout bug")), \
         patch.object(ae, "_analytical_pdf_minimal",
                       side_effect=lambda *a, **kw: calls.append("m") or b"%PDF-1.4 m"):
        out = ae.generate_analytical_report_pdf(
            conn, "ACME", "2026-04", firm_name="OtoCPA", lang="en",
        )
    assert out.startswith(b"%PDF")
    assert calls == ["m"]


# ---------------------------------------------------------------------------
# Fix 7 & 10 false-positive guards: source of truth stays wired
# ---------------------------------------------------------------------------

def test_get_account_risk_profile_is_defined():
    from src.engines.cas_engine import _get_account_risk_profile
    prof = _get_account_risk_profile("1100", "Accounts Receivable")
    assert "inherent_risk" in prof
    assert "control_risk" in prof


def test_mark_as_filed_is_imported_and_wired():
    """/calendar/mark_filed wires _mark_as_filed — the Sprint D claim
    that it's unused was a false positive. Lock the wiring here."""
    src = open("/opt/otocpa/scripts/review_dashboard.py").read()
    assert "mark_as_filed as _mark_as_filed" in src
    assert '_mark_as_filed(' in src
    assert 'path == "/calendar/mark_filed"' in src
