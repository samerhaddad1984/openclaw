"""R5-Investigation 7 — compliance posture smoke checks.

We don't test compliance in the legal sense; we verify the *product
features* required by the compliance posture doc are actually present.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Privacy policy / data-handling signals
# ---------------------------------------------------------------------------

def test_privacy_route_exists():
    """A /privacy route exists on the dashboard — first step of PIPEDA
    openness."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    assert "/privacy" in src, (
        "No /privacy route found. PIPEDA requires published privacy "
        "practices. Document a policy; see docs/compliance_posture.md."
    )


def test_compliance_posture_doc_exists():
    """The compliance-posture report itself must exist; R5 creates it
    as the baseline documentation."""
    doc = ROOT / "docs" / "compliance_posture.md"
    assert doc.exists(), "docs/compliance_posture.md missing"
    text = doc.read_text()
    # Must mention the relevant regulations.
    for k in ("PIPEDA", "Law 25", "SOC 2"):
        assert k in text, f"compliance posture doc missing {k} section"


# ---------------------------------------------------------------------------
# Audit trail completeness (feature-level precondition for compliance)
# ---------------------------------------------------------------------------

def test_login_attempts_table_available():
    """Audit trail for auth is required by CPA standards + SOC 2."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    assert "login_attempts" in src


def test_client_portal_access_table_available():
    """Audit trail for client-side activity."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    assert "client_portal_access" in src


def test_session_cookie_has_secure_semantics():
    """HttpOnly + SameSite enforced by _session_cookie_attrs."""
    import scripts.review_dashboard as rd
    # Simulate an HTTPS handler.
    fake = type("H", (), {"headers": {"X-Forwarded-Proto": "https"}})()
    attrs = rd._session_cookie_attrs(fake)
    assert "Secure" in attrs
    assert "SameSite" in attrs


# ---------------------------------------------------------------------------
# Data export capability (PIPEDA right-of-access / Law 25 portability)
# ---------------------------------------------------------------------------

def test_csv_export_for_client_data():
    """generate_csv is the mechanism for exporting posted documents,
    fulfilling the right-of-access spirit. Full-firm JSON export is
    still a gap (documented)."""
    from src.engines.export_engine import generate_csv
    data = generate_csv([{"document_id": "A", "vendor": "X", "amount": 1.0}])
    assert data
