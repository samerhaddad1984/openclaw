"""Grep-guard: make sure the rate-limiter scaling limitation stays
documented. If someone ever strips the docstring, this goes red.

Also checks that docs/scaling_considerations.md exists and names the
two supported migration paths (Redis, PostgreSQL).
"""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
RATE_LIMITER = ROOT / 'src' / 'security' / 'rate_limiter.py'
SCALING_DOC = ROOT / 'docs' / 'scaling_considerations.md'


def test_rate_limiter_has_scale_documentation():
    text = RATE_LIMITER.read_text()
    # Concrete markers we refuse to let disappear.
    assert 'LIMITATION' in text
    assert 'multi-process' in text.lower()
    assert 'Redis' in text or 'PostgreSQL' in text
    assert 'TODO(scale)' in text


def test_scaling_doc_exists():
    assert SCALING_DOC.is_file(), (
        "docs/scaling_considerations.md was deleted or renamed; "
        "update tests/security/test_rate_limiter_docs.py if intentional."
    )
    content = SCALING_DOC.read_text()
    assert 'Redis' in content
    assert 'PostgreSQL' in content
    assert 'migration path' in content.lower()


def test_rate_limiter_module_importable():
    """The facade module must import cleanly — the grep-guard above
    only checks the file on disk, this one exercises the import."""
    from src.security import rate_limiter
    assert hasattr(rate_limiter, 'scaling_notes')
    assert 'LIMITATION' in rate_limiter.scaling_notes()
    # Re-exported helper exists and is callable
    assert callable(rate_limiter.portal_user_upload_rate_allowed)
