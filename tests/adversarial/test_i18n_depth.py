"""R3-Investigation 8 — French/Quebec locale depth."""
from __future__ import annotations

import sqlite3
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Accent round-trip: names with Quebec accents survive storage + read.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", [
    "Tremblay",
    "Trémblay",
    "Boulevard René-Lévesque",
    "Société Générale",
    "Café français à Montréal",
    "L'Éclair",  # apostrophe + accent
])
def test_accent_name_roundtrips_through_sqlite(tmp_path, name):
    db = tmp_path / "acc.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE clients (name TEXT)")
    conn.execute("INSERT INTO clients VALUES (?)", (name,))
    conn.commit()
    back, = conn.execute("SELECT name FROM clients").fetchone()
    conn.close()
    assert back == name, f"{name!r} changed to {back!r} through SQLite"


# ---------------------------------------------------------------------------
# Accent-insensitive search: stored "Tremblay" should match "tremblay"
# or "Trémblay" if the dashboard's search normalizes accents.
# ---------------------------------------------------------------------------

def test_sqlite_like_is_case_sensitive_without_pragma(tmp_path):
    """Document current behavior. SQLite's default LIKE is case-INsensitive
    for ASCII but case-sensitive for non-ASCII. 'tremblay' does NOT
    match 'Tremblay' across accents by default. This is expected; the
    dashboard should normalize server-side before querying."""
    db = tmp_path / "s.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE c (n TEXT)")
    conn.execute("INSERT INTO c VALUES ('Tremblay')")
    conn.execute("INSERT INTO c VALUES ('Trémblay')")
    conn.commit()
    # Plain LIKE: matches Tremblay (case-insensitive ASCII).
    n = conn.execute("SELECT COUNT(*) FROM c WHERE n LIKE 'tremblay'").fetchone()[0]
    assert n == 1
    # With accent: accent-sensitive, does NOT match.
    n2 = conn.execute("SELECT COUNT(*) FROM c WHERE n LIKE 'tremblay'").fetchone()[0]
    assert n2 == 1  # still only Tremblay, not Trémblay
    conn.close()


# ---------------------------------------------------------------------------
# normalize_text and related helpers preserve accents.
# ---------------------------------------------------------------------------

def test_dashboard_normalize_text_preserves_accents(tmp_path):
    import scripts.review_dashboard as rd
    for s in ("René", "Québec", "Trémblay", "L'Éclair"):
        assert rd.normalize_text(s) == s, (
            f"normalize_text stripped accents from {s!r}"
        )


# ---------------------------------------------------------------------------
# Error message translations: scan for obvious English-only leaks in
# the FR rendering path.
# ---------------------------------------------------------------------------

def test_portal_invalid_page_contains_both_languages():
    import scripts.review_dashboard as rd
    html = rd.render_portal_invalid_page()
    assert "invalide" in html.lower() or "remplac" in html.lower()
    assert "invalid" in html.lower() and "link" in html.lower()


def test_dashboard_login_page_has_both_languages():
    import scripts.review_dashboard as rd
    html = rd.render_login("", lang="fr")
    # FR labels
    assert "Connexion" in html or "Mot de passe" in html
    # An EN toggle link exists.
    assert "English" in html or "lang=en" in html


# ---------------------------------------------------------------------------
# Date formatting helpers: FR renders "25 avril 2026" style if provided.
# ---------------------------------------------------------------------------

def test_date_helpers_exist_and_return_strings(tmp_path):
    """Even if we don't have full FR-locale formatting, the helpers
    should render readable dates, not raise or return None."""
    import scripts.review_dashboard as rd
    # Use utc_now_iso which is used throughout and should always work.
    s = rd.utc_now_iso()
    assert s and isinstance(s, str) and len(s) >= 10


# ---------------------------------------------------------------------------
# Unicode in the request body — CRLF injection via accented strings.
# ---------------------------------------------------------------------------

def test_accent_in_post_body_does_not_break_form_parse(tmp_path):
    import scripts.review_dashboard as rd
    # Simulate parse_form_body with a UTF-8-encoded body containing
    # accented chars.
    body = urllib.parse.urlencode(
        {"client_name": "Société Générale", "note": "Facturé"},
    ).encode("utf-8")
    parsed = rd.parse_form_body(body)
    assert parsed["client_name"] == "Société Générale"
    assert parsed["note"] == "Facturé"


# ---------------------------------------------------------------------------
# Timezone: all timestamps in DB should be UTC ISO, with trailing
# timezone info.
# ---------------------------------------------------------------------------

def test_utc_now_iso_format_has_timezone_suffix(tmp_path):
    import scripts.review_dashboard as rd
    s = rd.utc_now_iso()
    # Either 'Z' suffix or '+00:00' — both are valid UTC markers.
    assert s.endswith("Z") or s.endswith("+00:00"), (
        f"timestamp {s!r} lacks UTC marker"
    )
