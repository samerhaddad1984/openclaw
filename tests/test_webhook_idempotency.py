"""Sprint C Batch 4 — Stripe webhook idempotency.

BUG #6: Stripe retries webhook delivery until it sees a 2xx response,
so without an event-id dedup table every retry ran _handle_stripe_event
again — creating the firm and user twice.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


def _load_rd():
    """Import review_dashboard without running main()."""
    if "rd" in sys.modules:
        return sys.modules["rd"]
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "rd", "/opt/otocpa/scripts/review_dashboard.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rd"] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


@pytest.fixture
def rd_with_tmpdb(tmp_path, monkeypatch):
    rd = _load_rd()
    db = tmp_path / "idempotent.db"
    monkeypatch.setattr(rd, "DB_PATH", db)
    # open_db reads DB_PATH via a module-level path alias; reset the
    # table on a clean file.
    conn = sqlite3.connect(db)
    rd._ensure_stripe_events_table(conn)
    conn.commit()
    conn.close()
    return rd, db


def test_webhook_table_auto_created(rd_with_tmpdb):
    rd, db = rd_with_tmpdb
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='stripe_events_processed'"
    ).fetchall()
    conn.close()
    assert rows == [("stripe_events_processed",)]


def test_duplicate_webhook_idempotent(rd_with_tmpdb):
    rd, db = rd_with_tmpdb
    # First delivery marks it processed.
    rd._stripe_event_mark_processed("evt_ABC123", "customer.subscription.created")
    # Second delivery (retry) hits the idempotency check.
    assert rd._stripe_event_already_processed("evt_ABC123") is True


def test_unknown_event_not_marked_processed(rd_with_tmpdb):
    rd, _ = rd_with_tmpdb
    assert rd._stripe_event_already_processed("evt_NOT_SEEN") is False


def test_mark_processed_is_idempotent_itself(rd_with_tmpdb):
    rd, db = rd_with_tmpdb
    for _ in range(3):
        rd._stripe_event_mark_processed("evt_SAME", "invoice.paid")
    conn = sqlite3.connect(db)
    n = conn.execute(
        "SELECT COUNT(*) FROM stripe_events_processed WHERE event_id='evt_SAME'"
    ).fetchone()[0]
    conn.close()
    assert n == 1


def test_empty_event_id_is_no_op(rd_with_tmpdb):
    rd, _ = rd_with_tmpdb
    # Defensive behaviour: a malformed event with no id must not crash and
    # must not poison the table with an empty primary key.
    rd._stripe_event_mark_processed("", "typ")
    assert rd._stripe_event_already_processed("") is False


def test_event_id_extracted_from_sdk_style_object(rd_with_tmpdb):
    rd, _ = rd_with_tmpdb

    class FakeEvent:
        id = "evt_SDK1"
        type = "customer.subscription.created"

    assert rd._stripe_event_id(FakeEvent()) == "evt_SDK1"
    assert rd._stripe_event_type(FakeEvent()) == "customer.subscription.created"


def test_event_id_extracted_from_dict_payload(rd_with_tmpdb):
    rd, _ = rd_with_tmpdb
    payload = {"id": "evt_DICT1", "type": "invoice.paid"}
    assert rd._stripe_event_id(payload) == "evt_DICT1"
    assert rd._stripe_event_type(payload) == "invoice.paid"


def test_webhook_replay_does_not_double_create_user(rd_with_tmpdb):
    """Simulate the critical path: two deliveries, one real processing.

    We don't run the full _handle_stripe_event (it depends on live Stripe
    data); instead we stand in for it with a counter to prove the second
    delivery short-circuits before any side effects.
    """
    rd, _ = rd_with_tmpdb
    calls = []

    def fake_process(event):
        calls.append(getattr(event, "id", event.get("id")))

    evt = {"id": "evt_WELCOME_1", "type": "customer.subscription.created"}
    eid = rd._stripe_event_id(evt)

    # First delivery.
    if not rd._stripe_event_already_processed(eid):
        fake_process(evt)
        rd._stripe_event_mark_processed(eid, rd._stripe_event_type(evt))
    # Second delivery (retry).
    if not rd._stripe_event_already_processed(eid):
        fake_process(evt)
        rd._stripe_event_mark_processed(eid, rd._stripe_event_type(evt))

    assert calls == ["evt_WELCOME_1"]
