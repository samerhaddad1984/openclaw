"""Regression guards for locale-aware output in cas_engine.

The FR branch of ``generate_related_party_disclosure`` previously wrote
``1,234.56 $`` — mixing English comma-thousands with trailing-dollar
French convention. Now it uses the canonical ``1 234,56 $`` form via
:func:`src.formatting.money`.
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal

from src.engines.cas_engine import (
    add_related_party,
    ensure_cas_tables,
    flag_related_party_transaction,
    generate_related_party_disclosure,
    money,
)


def _seed_engagement_with_related_parties(conn: sqlite3.Connection) -> str:
    from src.engines.audit_engine import create_engagement, ensure_audit_tables
    ensure_audit_tables(conn)
    ensure_cas_tables(conn)
    eng = create_engagement(
        conn,
        client_code="ACME",
        period="2025-12-31",
        engagement_type="review",
    )
    eng_id = eng["engagement_id"]
    party_id = add_related_party(
        client_code="ACME",
        party_name="Jean Lévesque",
        relationship_type="shareholder",
        conn=conn,
        ownership_percentage=60.0,
    )
    flag_related_party_transaction(
        engagement_id=eng_id,
        document_id="doc_1",
        party_id=party_id,
        measurement_basis="exchange_amount",
        conn=conn,
        amount=12345.67,
        description="prêt de l'actionnaire / shareholder loan",
        transaction_date="2025-06-15",
    )
    return eng_id


def test_related_party_disclosure_fr_uses_canonical_currency() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        eng_id = _seed_engagement_with_related_parties(conn)
        fr = generate_related_party_disclosure(eng_id, "fr", conn)
        # Canonical FR currency form: space thousands, comma decimal, trailing $
        assert "12 345,67 $" in fr, f"expected FR currency in disclosure: {fr!r}"
        # Must not leak the English convention on the FR branch
        assert "$12,345.67" not in fr
        assert "12,345.67 $" not in fr  # the old broken form
    finally:
        conn.close()


def test_related_party_disclosure_en_keeps_anglo_currency() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        eng_id = _seed_engagement_with_related_parties(conn)
        en = generate_related_party_disclosure(eng_id, "en", conn)
        assert "$12,345.67" in en, f"expected EN currency in disclosure: {en!r}"
    finally:
        conn.close()


def test_cas_related_party_helper_renders_via_money_alias() -> None:
    """The helper is re-exported by cas_engine; the two sanity checks
    below pin that it's the canonical one."""
    assert money(12345.67, "fr") == "12 345,67 $"
    assert money(12345.67, "en") == "$12,345.67"
