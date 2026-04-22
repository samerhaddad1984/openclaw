"""Tests for :mod:`src.integrations.phone_normalizer`.

Covers the inputs that Twilio and the admin invite form realistically
send: `whatsapp:+1...` prefixes, spaces, parens, leading country
codes, typos. Rejected shapes (non-NANP, too short, structurally
invalid) must return ``None`` rather than silently normalizing to
garbage.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.phone_normalizer import normalize_phone, format_display


# ---------------------------------------------------------------------------
# Accepted inputs
# ---------------------------------------------------------------------------

def test_e164_passes_through():
    assert normalize_phone("+15145550123") == "+15145550123"


def test_whatsapp_prefix_stripped():
    assert normalize_phone("whatsapp:+15145550123") == "+15145550123"


def test_whatsapp_prefix_case_insensitive():
    assert normalize_phone("WhatsApp:+15145550123") == "+15145550123"


def test_ten_digit_assumed_plus_one():
    assert normalize_phone("5145550123") == "+15145550123"


def test_dashes_accepted():
    assert normalize_phone("514-555-0123") == "+15145550123"


def test_parens_and_spaces_accepted():
    assert normalize_phone("+1 (514) 555-0123") == "+15145550123"


def test_leading_one_no_plus():
    assert normalize_phone("1-514-555-0123") == "+15145550123"


def test_mixed_punctuation():
    assert normalize_phone("  1.514.555.0123  ") == "+15145550123"


def test_us_number_accepted():
    # NANP covers US too; any assignable NPA/NXX works.
    assert normalize_phone("+1 212 555 0123") == "+12125550123"


# ---------------------------------------------------------------------------
# Rejected inputs
# ---------------------------------------------------------------------------

def test_empty_returns_none():
    assert normalize_phone("") is None
    assert normalize_phone(None) is None
    assert normalize_phone("   ") is None


def test_too_short_rejected():
    assert normalize_phone("12345") is None


def test_foreign_country_code_rejected():
    # UK number — structurally valid E.164 but we only accept NANP.
    assert normalize_phone("+447911123456") is None


def test_french_country_code_rejected():
    assert normalize_phone("+33612345678") is None


def test_area_code_starting_zero_rejected():
    # NANP NPA (area code) cannot start with 0 or 1.
    assert normalize_phone("0145550123") is None


def test_area_code_starting_one_rejected():
    assert normalize_phone("1145550123") is None


def test_exchange_starting_zero_rejected():
    # NXX (central-office code) cannot start with 0 or 1.
    assert normalize_phone("5140550123") is None


def test_exchange_starting_one_rejected():
    assert normalize_phone("5141550123") is None


def test_garbage_rejected():
    assert normalize_phone("not a phone") is None
    assert normalize_phone("abcdefghij") is None


def test_eleven_digits_not_leading_one_rejected():
    # 11 digits that don't start with 1 are not NANP.
    assert normalize_phone("25145550123") is None


# ---------------------------------------------------------------------------
# format_display
# ---------------------------------------------------------------------------

def test_format_display_nanp():
    assert format_display("+15145550123") == "+1 (514) 555-0123"


def test_format_display_empty():
    assert format_display("") == ""
    assert format_display(None) == ""


def test_format_display_passthrough_when_not_nanp():
    # Legacy rows may have stored raw 10-digit numbers; display as-is.
    assert format_display("5145550123") == "5145550123"
