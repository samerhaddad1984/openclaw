"""Phase 1 — smart-bank-source schema."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.bank_source_schema import (  # noqa: E402
    apply_bank_source_schema, ensure_bank_source_schema,
)


def _cols(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_adds_bank_source_column_to_clients(tmp_path):
    db = tmp_path / 'x.db'
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT)"
        )
        apply_bank_source_schema(conn)
        cols = _cols(conn, 'clients')
    assert 'bank_source' in cols


def test_clients_bank_source_default_none(tmp_path):
    db = tmp_path / 'x.db'
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT)"
        )
        apply_bank_source_schema(conn)
        conn.execute("INSERT INTO clients (client_code, firm_code) VALUES ('C1','F1')")
        conn.commit()
        row = conn.execute(
            "SELECT bank_source FROM clients WHERE client_code='C1'"
        ).fetchone()
    assert row[0] == 'none'


def test_adds_bank_transaction_columns(tmp_path):
    db = tmp_path / 'x.db'
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        cols = _cols(conn, 'bank_transactions')
    for expected in ('firm_code', 'source', 'external_id',
                      'qbo_account_id', 'qbo_sync_token', 'hidden_duplicate'):
        assert expected in cols


def test_bank_transaction_source_defaults_plaid(tmp_path):
    db = tmp_path / 'x.db'
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions (id, client_code) VALUES ('bt1','C1')"
        )
        conn.commit()
        row = conn.execute(
            "SELECT source, hidden_duplicate FROM bank_transactions WHERE id='bt1'"
        ).fetchone()
    assert row == ('plaid', 0)


def test_unique_source_external_constraint(tmp_path):
    db = tmp_path / 'x.db'
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions (id, firm_code, client_code, "
            "source, external_id) VALUES ('bt1','F1','C1','qbo','Q-100')"
        )
        conn.commit()
        import pytest
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO bank_transactions (id, firm_code, client_code, "
                "source, external_id) VALUES ('bt2','F1','C1','qbo','Q-100')"
            )
            conn.commit()


def test_same_external_id_allowed_across_sources(tmp_path):
    """Plaid and QBO can both have 'ID-1' for different real-world txs —
    the UNIQUE is scoped to (source, external_id)."""
    db = tmp_path / 'x.db'
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions (id, firm_code, client_code, "
            "source, external_id) VALUES ('bt1','F1','C1','qbo','ID-1')"
        )
        conn.execute(
            "INSERT INTO bank_transactions (id, firm_code, client_code, "
            "source, external_id) VALUES ('bt2','F1','C1','plaid','ID-1')"
        )
        conn.commit()
        n = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions WHERE external_id='ID-1'"
        ).fetchone()[0]
    assert n == 2


def test_null_external_id_exempt_from_unique(tmp_path):
    """Legacy rows with external_id IS NULL must not collide."""
    db = tmp_path / 'x.db'
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions (id, firm_code, client_code, source) "
            "VALUES ('bt1','F1','C1','plaid')"
        )
        conn.execute(
            "INSERT INTO bank_transactions (id, firm_code, client_code, source) "
            "VALUES ('bt2','F1','C1','plaid')"
        )
        conn.commit()


def test_bank_tx_dedup_table_created(tmp_path):
    db = tmp_path / 'x.db'
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert 'bank_tx_dedup' in tables


def test_idempotent_across_replays(tmp_path):
    db = tmp_path / 'x.db'
    for _ in range(3):
        ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        cols = _cols(conn, 'bank_transactions')
    # No duplicate columns (ALTER TABLE ADD fails noisy without our guard)
    assert 'firm_code' in cols


def test_bootstrap_wires_the_migration():
    src = (ROOT / 'scripts' / 'review_dashboard.py').read_text()
    assert 'from src.integrations.bank_source_schema import apply_bank_source_schema' in src
    assert 'apply_bank_source_schema(conn)' in src
