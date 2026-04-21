"""Regression guards for locale-aware output in the dashboard + portal.

Covers:
- Client-facing documents table (date + amount) uses client language
- Audit-anomalies page "Last run" label and value run through locale
- Revenu Québec PDF "generated_at" runs through locale
- Daily digest "today" no longer depends on system locale
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


SRC_DASHBOARD = Path(__file__).resolve().parent.parent.parent / "scripts/review_dashboard.py"
SRC_DIGEST = Path(__file__).resolve().parent.parent.parent / "scripts/daily_digest.py"


def test_portal_documents_uses_format_date_and_money() -> None:
    content = SRC_DASHBOARD.read_text(encoding="utf-8")
    # The render_portal_documents function should call the helpers.
    section = re.search(
        r"def render_portal_documents\(.*?\n(?=\ndef |\Z)",
        content,
        re.DOTALL,
    )
    assert section, "render_portal_documents not found"
    body = section.group(0)
    assert "format_date_short" in body
    assert "money(" in body
    # The old pattern must be gone.
    assert 'f"${amt:.2f}"' not in body
    assert '(r["created_at"] or "")[:10]' in body  # we still extract ISO first


def test_audit_anomalies_last_run_uses_locale_time() -> None:
    content = SRC_DASHBOARD.read_text(encoding="utf-8")
    # The last_run assignment now uses format_date_short + format_time
    idx = content.find("last_run = ")
    # Skip the sentinel "last_run = '—'" and find the computed one
    while idx != -1:
        line = content[idx:idx + 200]
        if "format_date_short" in line or "_fds(" in line:
            break
        idx = content.find("last_run = ", idx + 1)
    assert idx != -1, (
        "expected last_run to be computed via format_date_short + format_time"
    )


def test_revenu_quebec_pdf_generated_at_is_locale_aware() -> None:
    content = SRC_DASHBOARD.read_text(encoding="utf-8")
    # The legacy "%H:%M UTC" strftime in the RQ serve method is gone.
    assert 'utc_now().strftime("%Y-%m-%d %H:%M UTC")' not in content


def test_daily_digest_today_uses_format_date_per_lang() -> None:
    content = SRC_DIGEST.read_text(encoding="utf-8")
    # Old pattern that silently depended on system locale for %B
    assert 'date.today().strftime("%d %B %Y")' not in content
    # New pattern
    assert "format_date(date.today(), lang)" in content


@pytest.mark.parametrize("lang,expected_fragment", [
    ("fr", "avril"),
    ("en", "April"),
])
def test_daily_digest_renders_french_or_english_month(
    lang: str, expected_fragment: str
) -> None:
    """Light end-to-end: the digest's plain-text builder with lang=fr
    must contain a French month name (avril), and en → April."""
    from datetime import date
    from unittest.mock import patch
    import scripts.daily_digest as dd

    summary = {
        "needs_review": 0, "on_hold": 0, "ready_to_post": 0,
        "posted_today": 0, "stale": 0, "total_active": 0,
    }
    with patch.object(dd, "date") as mock_date:
        mock_date.today.return_value = date(2026, 4, 21)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        text = dd.build_plain_text(summary, lang=lang, recipient_name="Marie")
    assert expected_fragment in text, (
        f"expected {expected_fragment!r} in {lang} digest, got: {text[:200]!r}"
    )
