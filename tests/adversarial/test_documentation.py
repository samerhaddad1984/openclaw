"""R5-Investigation 9 — documentation completeness smoke checks."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Required docs present
# ---------------------------------------------------------------------------

REQUIRED_DOCS = [
    "docs/admin_runbook.md",
    "docs/compliance_posture.md",
    "docs/migration_gaps.md",
    "docs/nasty_detective_report.md",
    "docs/nasty_detective_r2_report.md",
    "docs/nasty_detective_r3_report.md",
    "docs/nasty_detective_r4_report.md",
]


@pytest.mark.parametrize("path", REQUIRED_DOCS, ids=lambda p: p)
def test_required_doc_exists_and_non_empty(path):
    p = ROOT / path
    assert p.exists(), f"missing {path}"
    text = p.read_text()
    assert len(text) > 500, f"{path} is suspiciously short ({len(text)} chars)"


# ---------------------------------------------------------------------------
# Admin runbook covers the critical topics.
# ---------------------------------------------------------------------------

def test_admin_runbook_covers_password_reset():
    text = (ROOT / "docs" / "admin_runbook.md").read_text()
    assert "reset a user's password" in text.lower()


def test_admin_runbook_covers_backup_restore():
    text = (ROOT / "docs" / "admin_runbook.md").read_text()
    assert "restore" in text.lower() and "sqlite" in text.lower()


def test_admin_runbook_covers_api_key_rotation():
    text = (ROOT / "docs" / "admin_runbook.md").read_text()
    assert "rotate" in text.lower() and "ingest" in text.lower()


def test_admin_runbook_references_schema_drift_guard():
    """The schema-drift guard is a key pre-deploy check; the runbook
    should mention it."""
    text = (ROOT / "docs" / "admin_runbook.md").read_text()
    assert "schema" in text.lower() and "drift" in text.lower()
