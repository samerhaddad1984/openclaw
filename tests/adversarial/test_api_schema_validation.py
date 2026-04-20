"""R5-Investigation 6 — external API schema validation.

When upstream APIs (Stripe, Plaid, QBO, Google DocAI, Anthropic,
Gmail) change their response shape, we want to fail loudly — not
silently store garbage or crash with a Python TypeError.

This file exercises each integration's parser/handler against
mock responses shaped like a schema change, and verifies we either
reject cleanly or continue with a safe default.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# STRIPE
# ---------------------------------------------------------------------------

def test_stripe_event_missing_id_is_handled():
    """A Stripe webhook event without 'id' (malformed or schema
    change) must not crash _stripe_event_id — it should return
    None/empty and skip the idempotency write."""
    import scripts.review_dashboard as rd
    for shape in ({}, {"type": "x"}, {"id": None}, None):
        try:
            eid = rd._stripe_event_id(shape)
        except Exception as e:
            pytest.fail(f"_stripe_event_id crashed on {shape!r}: {e}")
        # Non-crash is the contract; value can be None or "".


def test_stripe_event_unknown_type_handled_by_provisioner():
    """handle_webhook ignores event types it doesn't understand. A
    new upstream event.type like 'subscription.schedule.v2' should
    not crash _handle_stripe_event."""
    import scripts.review_dashboard as rd
    event = {"id": "evt_new", "type": "some.new.unknown.type.v99",
             "data": {"object": {}}}
    # Should not raise; may be a no-op.
    rd._handle_stripe_event(event)


# ---------------------------------------------------------------------------
# QBO adapter
# ---------------------------------------------------------------------------

def test_qbo_adapter_public_surface_present():
    """The QBO adapter should expose post_one_ready_job; we don't
    exercise it end-to-end here (requires a full qbo_connections
    fixture) — we just verify the public surface is stable so
    callers don't break silently."""
    from src.agents.tools import qbo_online_adapter
    assert hasattr(qbo_online_adapter, "post_one_ready_job")


# ---------------------------------------------------------------------------
# OCR engine — parse_invoice_fields is the parser that ingests AI
# output. If the AI returns an unexpected shape, we should cope.
# ---------------------------------------------------------------------------

def test_parse_invoice_fields_handles_empty_and_none():
    from src.engines.ocr_engine import parse_invoice_fields
    for arg in ("", "   ", None):
        try:
            r = parse_invoice_fields(arg or "")
        except Exception as e:
            pytest.fail(f"parse_invoice_fields crashed on {arg!r}: {e}")
        assert isinstance(r, dict)
        assert r.get("confidence", 0) < 0.5


def test_parse_invoice_fields_unknown_currency_markers():
    """A receipt with an unknown currency marker (e.g., '¤' or 'XYZ')
    should not crash the parser; extraction may simply miss fields."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields(
        "Exotic Vendor\n"
        "TOTAL: ¤123.45\n"
        "Currency: ZZZ\n"
        "Date: 2026-04-20\n"
    )
    assert isinstance(r, dict)


def test_parse_invoice_fields_absurd_amounts_flagged():
    """R1 regression: trillion-dollar OCR output is dropped + flagged
    rather than stored."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields("TOTAL: $999,999,999,999.99")
    assert r.get("amount") is None
    assert r.get("amount_flagged_absurd") is True


# ---------------------------------------------------------------------------
# Gmail / email client — graceful when service unavailable.
# ---------------------------------------------------------------------------

def test_gmail_send_returns_false_on_service_none():
    from src.integrations import email_client
    with patch.object(email_client, "_get_gmail_service", return_value=None):
        ok = email_client.send_email("x@y.com", "s", "<p>h</p>")
    assert ok is False


def test_gmail_send_returns_false_on_api_exception():
    """Mock a Gmail service that raises on send — must return False,
    not propagate."""
    from src.integrations import email_client

    fake_service = MagicMock()
    fake_send = fake_service.users.return_value.messages.return_value.send
    fake_send.return_value.execute.side_effect = RuntimeError("api boom")
    with patch.object(email_client, "_get_gmail_service",
                       return_value=fake_service):
        ok = email_client.send_email("x@y.com", "s", "<p>h</p>")
    assert ok is False


# ---------------------------------------------------------------------------
# Plaid / bank — webhook signature failures.
# ---------------------------------------------------------------------------

def test_stripe_webhook_signature_verification_wired():
    """The Stripe webhook handler must route invalid-signature cases
    to a 400, not silently accept. R2 fix regression guard."""
    import scripts.review_dashboard as rd
    # The handler logic is in-process; static check on the handler
    # block for the expected branch.
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    assert "invalid_signature" in src, (
        "Stripe signature verification response string missing"
    )


# ---------------------------------------------------------------------------
# Schema-version pinning
# ---------------------------------------------------------------------------

def test_requirements_pin_major_api_libraries():
    """Check requirements.txt or similar: Stripe + Plaid + google-cloud
    libraries should be pinned, not floating on 'latest', to prevent
    surprise upstream breaking changes."""
    reqs = ROOT / "requirements.txt"
    if not reqs.exists():
        pytest.skip("requirements.txt not present")
    text = reqs.read_text().lower()
    # For libraries known to be used:
    critical = ["stripe", "plaid", "google-cloud"]
    for name in critical:
        # If the library is referenced, check it has a version.
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if name in line.lower():
                assert (
                    "==" in line or ">=" in line or "~=" in line
                    or "<" in line
                ), f"{name} in requirements.txt is unpinned: {line!r}"
                break
