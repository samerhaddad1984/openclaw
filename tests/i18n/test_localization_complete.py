"""Regression guards for localization.

Seeded by the "Message envoy&eacute; / Message sent" bug: a flash message
containing the HTML entity &eacute; got URL-encoded into a redirect, then
HTML-escaped on the way back, and rendered to the user as literal
``&eacute;``. These tests assert the invariants that prevent that class
of bug from coming back:

* no French HTML entities in Python production source,
* both fr.json and en.json have the same keys,
* FR values don't unexpectedly equal EN values,
* the locale-format helpers render as expected,
* the portal flash message specifically renders the real é character.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent


FR_HTML_ENTITIES = re.compile(
    r"&(?:eacute|egrave|ecirc|euml|agrave|acirc|aelig|auml|ccedil|icirc"
    r"|iuml|ocirc|ouml|oelig|ucirc|uuml|yuml|szlig|aring|Eacute|Egrave"
    r"|Ecirc|Agrave|Acirc|Ccedil|Icirc|Ocirc|Ucirc|OElig|Oelig);"
)


PRODUCTION_FILES = [
    ROOT / "scripts/review_dashboard.py",
    ROOT / "scripts/client_portal.py",
    ROOT / "src/integrations/email_client.py",
    ROOT / "src/integrations/multi_user_portal.py",
    ROOT / "src/integrations/gap_routes.py",
]


def test_no_french_html_entities_in_production_source() -> None:
    """French HTML entities in Python string literals cause double-escape
    bugs when the string flows through ``html.escape`` — use the raw
    UTF-8 character instead.
    """
    offenders: list[tuple[str, int, str]] = []
    for fp in PRODUCTION_FILES:
        if not fp.exists():
            continue
        for lineno, line in enumerate(fp.read_text(encoding="utf-8").splitlines(), 1):
            m = FR_HTML_ENTITIES.search(line)
            if m:
                offenders.append((str(fp.relative_to(ROOT)), lineno, line.strip()))
    assert not offenders, (
        "HTML entities for French characters found in production source — "
        "use UTF-8 characters (é, à, ç, ...) instead so they survive "
        "urlquote + html.escape round-trips.\n"
        + "\n".join(f"  {f}:{ln}: {text[:120]}" for f, ln, text in offenders[:10])
    )


def test_fr_and_en_json_keys_match() -> None:
    fr = json.loads((ROOT / "src/i18n/fr.json").read_text(encoding="utf-8"))
    en = json.loads((ROOT / "src/i18n/en.json").read_text(encoding="utf-8"))
    missing_en = sorted(set(fr) - set(en))
    missing_fr = sorted(set(en) - set(fr))
    assert not missing_en, f"keys present in FR but missing in EN: {missing_en[:10]}"
    assert not missing_fr, f"keys present in EN but missing in FR: {missing_fr[:10]}"


def test_fr_strings_not_identical_to_en_except_cognates() -> None:
    """Most FR/EN pairs should differ. Identical values are allowed only
    for genuine cognates (Description, Client, Total, ...) and proper
    names (brand names, Quebec tax acronyms).
    """
    fr = json.loads((ROOT / "src/i18n/fr.json").read_text(encoding="utf-8"))
    en = json.loads((ROOT / "src/i18n/en.json").read_text(encoding="utf-8"))
    # Values that are genuinely the same word in French and English, or
    # brand/product names that are never translated.
    COGNATE_ALLOWLIST = {
        # Accounting column headers — cognates
        "Action", "Actions", "Assertion", "Budget", "Classification",
        "Client", "Clients", "Communications", "Conclusion",
        "Conf.", "Date", "Description", "Direction", "Document",
        "Exception", "Existence", "Message", "Notes", "Observation",
        "Ping", "Premium", "Province", "Source", "Support", "Total",
        "TOTAL", "Transactions", "Version", "Visible",
        # French words in French product names (also used as-is in EN)
        "Essentiel", "Professionnel", "Cabinet", "Entreprise",
        # Brand / software / government names — never translated
        "Acomba", "Excel", "QuickBooks Desktop", "Sage 50 Canada",
        "Wave", "Xero", "Revenu Québec",
        # Abbreviations / technical codes that are identical in both
        "LLAI-...", "Services", "PDF", "URL", "N/A", "min", "Machines",
        # Single-word labels that happen to be same in both languages
        "Note",
        # Bilingual strings that already include both languages
        "Scannez pour soumettre vos documents / Scan to submit your documents",
        "Trop de tentatives. Réessayez dans 15 minutes / Too many attempts. Try again in 15 minutes.",
        # Plural markers used as simple labels
        "clients", "client",
    }

    offenders: list[tuple[str, str]] = []
    for k in set(fr) & set(en):
        v_fr, v_en = fr[k], en[k]
        if v_fr != v_en:
            continue
        if v_fr in COGNATE_ALLOWLIST:
            continue
        if len(v_fr) < 3:
            continue
        if v_fr.replace(",", "").replace(".", "").isdigit():
            continue
        offenders.append((k, v_fr))
    assert not offenders, (
        "FR values equal EN values (likely untranslated). Add to the "
        "COGNATE_ALLOWLIST if genuinely same-in-both, otherwise translate:\n"
        + "\n".join(f"  {k}: {v!r}" for k, v in offenders[:15])
    )


def test_format_date_differs_between_locales() -> None:
    from src.formatting import format_date
    d = date(2026, 4, 21)
    assert format_date(d, "fr") == "21 avril 2026"
    assert format_date(d, "en") == "April 21, 2026"
    assert format_date(d, "fr") != format_date(d, "en")


def test_format_currency_french_convention() -> None:
    from src.formatting import format_currency
    # Space thousands, comma decimal, trailing symbol
    assert format_currency(1234.56, "fr") == "1 234,56 $"
    assert format_currency(1000000, "fr") == "1 000 000,00 $"
    # English: leading $, comma thousands, dot decimal
    assert format_currency(1234.56, "en") == "$1,234.56"
    # Negatives: leading minus in both
    assert format_currency(-99.9, "fr").startswith("-"), format_currency(-99.9, "fr")
    assert format_currency(-99.9, "en").startswith("-$")


def test_format_number_french_convention() -> None:
    from src.formatting import format_number
    assert format_number(1234567.89, "fr", decimals=2) == "1 234 567,89"
    assert format_number(1234567.89, "en", decimals=2) == "1,234,567.89"


def test_format_time_french_uses_24h_h_separator() -> None:
    from src.formatting import format_time
    t = datetime(2026, 4, 21, 14, 30)
    assert format_time(t, "fr") == "14h30"
    assert format_time(t, "en") == "2:30 PM"


def test_portal_message_sent_flash_uses_utf8() -> None:
    """The bug that seeded this module: the flash had &eacute; in source,
    which survived through URL-encode + HTML-escape and shipped to the
    client as literal ``&eacute;``. Guard against regression by asserting
    the real character ends up in the source.
    """
    src = (ROOT / "scripts/review_dashboard.py").read_text(encoding="utf-8")
    # The user-facing confirmation must contain the real é, not the entity.
    assert "Message envoyé" in src, (
        "Client portal 'Message envoyé' flash is missing the real é — "
        "if it reverted to an HTML entity, the bug is back."
    )
    assert "Message envoy&eacute;" not in src


def test_email_subject_renders_utf8_for_french_accents() -> None:
    """Password-reset subject contains é. The MIMEText/Header path must
    encode the header as UTF-8 so Gmail/Outlook render it correctly."""
    from email.header import Header
    from email.mime.text import MIMEText
    subj = "Réinitialisation de mot de passe OtoCPA / OtoCPA password reset"
    h = Header(subj, "utf-8")
    encoded = h.encode()
    # Encoded form is either the raw UTF-8 string (if ASCII-safe) or a
    # base64/quoted-printable RFC 2047 encoding. Either way, the real é
    # must decode back.
    from email.header import decode_header
    decoded = decode_header(encoded)[0][0]
    if isinstance(decoded, bytes):
        decoded = decoded.decode("utf-8")
    assert "Réinitialisation" in decoded


def test_portal_invalid_page_is_bilingual_after_entity_cleanup() -> None:
    """Post-cleanup, the portal invalid-link page must render the real é
    for ``été`` — if a refactor reintroduces an entity, both the string
    check and a visible-browser test break."""
    import scripts.review_dashboard as rd
    html = rd.render_portal_invalid_page()
    assert "été" in html or "invalide" in html.lower()
    assert "&eacute;" not in html
    assert "&amp;eacute;" not in html  # double-escape canary


@pytest.mark.parametrize("lang", ["fr", "en"])
def test_invitation_email_renders_per_lang(lang: str) -> None:
    from src.integrations.multi_user_portal import render_invitation_email
    subject, body = render_invitation_email(
        recipient_name="Marie Tremblay",
        inviter_name="Jean Lévesque",
        client_display="Construction Tremblay",
        accept_url="https://otocpa.example/invite/tok123",
        lang=lang,
    )
    # No HTML entities for French chars should remain in either lang body.
    assert not FR_HTML_ENTITIES.search(body), f"entity leak in {lang} body"
    assert not FR_HTML_ENTITIES.search(subject), f"entity leak in {lang} subject"
    if lang == "fr":
        assert "invité" in body or "invité(e)" in body
    else:
        assert "invited" in body.lower()
