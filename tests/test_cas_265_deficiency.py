"""CAS 265 control deficiency + management letter tests (Sprint F Fix 6)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines import audit_engine, cas_engine  # noqa: E402


def _conn(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c


def _setup(tmp_path: Path, period: str = "2025-12-31") -> tuple[sqlite3.Connection, str]:
    db = tmp_path / "cas265.db"
    conn = _conn(db)
    audit_engine.ensure_audit_tables(conn)
    cas_engine.ensure_cas_tables(conn)
    cas_engine.ensure_control_deficiency_table(conn)
    conn.execute(
        "INSERT INTO engagements (engagement_id, client_code, period, engagement_type, status, created_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
        ("E1", "ACME", period, "audit", "planning"),
    )
    conn.commit()
    return conn, "E1"


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def test_material_weakness_when_above_materiality():
    sev = cas_engine.classify_control_deficiency({
        "likelihood_of_misstatement": "moderate",
        "magnitude_potential": "above_materiality",
        "compensating_controls_exist": False,
        "management_override_possible": False,
    })
    assert sev == cas_engine.CONTROL_SEVERITY_MATERIAL


def test_material_weakness_when_management_override_possible():
    sev = cas_engine.classify_control_deficiency({
        "likelihood_of_misstatement": "low",
        "magnitude_potential": "below_materiality",
        "management_override_possible": True,
    })
    assert sev == cas_engine.CONTROL_SEVERITY_MATERIAL


def test_significant_when_moderate_likelihood_at_materiality():
    sev = cas_engine.classify_control_deficiency({
        "likelihood_of_misstatement": "moderate",
        "magnitude_potential": "at_materiality",
    })
    assert sev == cas_engine.CONTROL_SEVERITY_SIGNIFICANT


def test_observation_when_low_risk():
    sev = cas_engine.classify_control_deficiency({
        "likelihood_of_misstatement": "low",
        "magnitude_potential": "below_materiality",
        "compensating_controls_exist": True,
    })
    assert sev == cas_engine.CONTROL_SEVERITY_OBSERVATION


def test_above_materiality_with_compensating_becomes_significant():
    sev = cas_engine.classify_control_deficiency({
        "likelihood_of_misstatement": "low",
        "magnitude_potential": "above_materiality",
        "compensating_controls_exist": True,
    })
    assert sev == cas_engine.CONTROL_SEVERITY_SIGNIFICANT


def test_deficiency_classification_logged(tmp_path):
    conn, eid = _setup(tmp_path)
    did, sev = cas_engine.record_control_deficiency(
        conn, engagement_id=eid,
        title="Revenue cutoff override", description="Controller can post without approval",
        likelihood_of_misstatement="high", magnitude_potential="above_materiality",
        management_override_possible=True,
    )
    assert sev == cas_engine.CONTROL_SEVERITY_MATERIAL
    row = conn.execute(
        "SELECT severity FROM control_deficiencies WHERE deficiency_id=?", (did,),
    ).fetchone()
    conn.close()
    assert row["severity"] == "material_weakness"


# ---------------------------------------------------------------------------
# Management letter generation
# ---------------------------------------------------------------------------

def test_management_letter_generated_for_significant(tmp_path):
    conn, eid = _setup(tmp_path)
    cas_engine.record_control_deficiency(
        conn, engagement_id=eid,
        title="Bank rec review gap", description="Rec not reviewed by second signer",
        likelihood_of_misstatement="moderate", magnitude_potential="at_materiality",
        recommendation="Implement mandatory second-signer review",
    )
    pdf, path, count = cas_engine.generate_management_letter_pdf(
        eid, conn, output_dir=tmp_path / "ml",
    )
    conn.close()
    assert pdf[:4] == b"%PDF"
    assert Path(path).exists()
    assert count == 1


def test_no_letter_when_only_observations(tmp_path):
    conn, eid = _setup(tmp_path)
    cas_engine.record_control_deficiency(
        conn, engagement_id=eid,
        title="Minor typo in policy", description="",
        likelihood_of_misstatement="low", magnitude_potential="below_materiality",
        compensating_controls_exist=True,
    )
    pdf, path, count = cas_engine.generate_management_letter_pdf(
        eid, conn, output_dir=tmp_path / "ml",
    )
    conn.close()
    # PDF is still generated (CAS 265 allows a "no findings" letter) but the
    # reportable count is zero.
    assert count == 0
    assert pdf[:4] == b"%PDF"


def test_management_response_recorded(tmp_path):
    conn, eid = _setup(tmp_path)
    did, _ = cas_engine.record_control_deficiency(
        conn, engagement_id=eid,
        title="Vendor master change control",
        description="Anyone can add new vendors",
        likelihood_of_misstatement="moderate",
        magnitude_potential="at_materiality",
    )
    assert cas_engine.update_management_response(
        conn, did, "We will implement dual approval by Q2.",
        response_due_date="2025-06-30",
    )
    row = conn.execute(
        "SELECT management_response, response_due_date FROM control_deficiencies WHERE deficiency_id=?",
        (did,),
    ).fetchone()
    conn.close()
    assert "dual approval" in row["management_response"]
    assert row["response_due_date"] == "2025-06-30"


def test_letter_marks_deficiencies_communicated(tmp_path):
    conn, eid = _setup(tmp_path)
    did, _ = cas_engine.record_control_deficiency(
        conn, engagement_id=eid,
        title="Finding A", description="",
        likelihood_of_misstatement="moderate", magnitude_potential="at_materiality",
    )
    cas_engine.generate_management_letter_pdf(
        eid, conn, output_dir=tmp_path / "ml",
    )
    row = conn.execute(
        "SELECT communicated_at FROM control_deficiencies WHERE deficiency_id=?", (did,),
    ).fetchone()
    conn.close()
    assert row["communicated_at"] is not None


def test_material_weakness_included_in_letter(tmp_path):
    conn, eid = _setup(tmp_path)
    cas_engine.record_control_deficiency(
        conn, engagement_id=eid,
        title="Segregation of duties",
        description="Same person posts AR and reconciles bank",
        likelihood_of_misstatement="high", magnitude_potential="above_materiality",
    )
    pdf, _, count = cas_engine.generate_management_letter_pdf(
        eid, conn, output_dir=tmp_path / "ml",
    )
    conn.close()
    assert count == 1
    assert len(pdf) > 1000
