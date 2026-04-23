"""Scope 2 Phase 3 — opening balances for mid-year adoption.

Invariants exercised:

  - TB must balance to within $0.01 or posting is rejected.
  - Native activity at/after the as-of date blocks posting (unless
    ``force=True`` is passed explicitly).
  - A second post attempt with a locked entry already present is
    rejected with reason ``already_posted``.
  - A reversal writes compensating rows with source=
    ``opening_balance_reversal`` and swapped sides. After reversal
    another post is allowed.
  - TB still balances after opening + reversal + repost cycle.
  - Supporting documents get linked in opening_balance_supporting_docs.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import opening_balances as ob  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'ob.db'
    # Schema ensured on-demand by ob.ensure_schema()
    return db


def _balanced_rows():
    return [
        {'account_code': '1000', 'side': 'debit',  'amount': 15000.00},  # Cash
        {'account_code': '1100', 'side': 'debit',  'amount':  2500.00},  # AR
        {'account_code': '2000', 'side': 'credit', 'amount':  3500.00},  # AP
        {'account_code': '3000', 'side': 'credit', 'amount': 14000.00},  # Equity
    ]


# ---------------------------------------------------------------------------


def test_opening_balances_page_renders(tmp_path):
    db = _mkdb(tmp_path)
    ob.ensure_schema(db)
    html = ob.render_opening_balances_page(
        firm_code='FIRM', client_code='CONS',
        client_name='Construction Tremblay',
    )
    assert "Construction Tremblay" in html
    assert "as_of_date" in html
    # The form offers at least one row input.
    assert "account_code_0" in html
    assert "side_0" in html


def test_tb_must_balance_to_post(tmp_path):
    db = _mkdb(tmp_path)
    unbalanced = [
        {'account_code': '1000', 'side': 'debit',  'amount': 1000.00},
        {'account_code': '3000', 'side': 'credit', 'amount':  900.00},
    ]
    result = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=unbalanced,
    )
    assert result['ok'] is False
    assert result['reason'] == 'unbalanced_or_invalid'
    assert round(result['validation']['diff'], 2) == 100.00


def test_opening_je_posted_correctly(tmp_path):
    db = _mkdb(tmp_path)
    result = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
        description='Migration from Sage',
        posted_by='cpa@firm.com',
    )
    assert result['ok'] is True
    assert result['rows_posted'] == 4
    assert result['debits'] == 17500.00
    assert result['credits'] == 17500.00
    # GL rows exist with the opening-balance source tag.
    with sqlite3.connect(db) as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE client_code='CONS' AND source='opening_balance'"
        ).fetchone()[0]
        assert cnt == 4
        # Header is locked.
        stat = conn.execute(
            "SELECT status FROM opening_balance_entries "
            "WHERE client_code='CONS'"
        ).fetchone()[0]
        assert stat == 'locked'


def test_bs_identity_preserved_A_equals_L_plus_E(tmp_path):
    """Interaction test: A = L + E still holds after posting."""
    db = _mkdb(tmp_path)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    with sqlite3.connect(db) as conn:
        debits, credits = conn.execute(
            "SELECT COALESCE(SUM(CASE WHEN side='debit' THEN amount END),0),"
            "       COALESCE(SUM(CASE WHEN side='credit' THEN amount END),0) "
            "FROM gl_transactions WHERE client_code='CONS'"
        ).fetchone()
    assert round(debits - credits, 2) == 0.0


def test_supporting_docs_attached(tmp_path):
    db = _mkdb(tmp_path)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
        supporting_document_ids=['DOC-BANK-MARCH', 'DOC-PRIOR-TB'],
    )
    with sqlite3.connect(db) as conn:
        rows = list(conn.execute(
            "SELECT document_id FROM opening_balance_supporting_docs "
            "ORDER BY document_id"
        ))
    assert [r[0] for r in rows] == ['DOC-BANK-MARCH', 'DOC-PRIOR-TB']


def test_adoption_date_respected(tmp_path):
    """Native activity after as-of blocks posting (without force=True)."""
    db = _mkdb(tmp_path)
    ob.ensure_schema(db)
    # Seed a native manual_je row dated after the proposed as-of.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO gl_transactions "
            "(entry_id, client_code, period, entry_date, account_code, "
            "side, amount, source) VALUES "
            "('JE-0001','CONS','2026-02','2026-02-15','1000','debit',"
            "100.0,'manual_je')"
        )
        conn.commit()
    result = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    assert result['ok'] is False
    assert result['reason'] == 'native_activity_after_as_of_date'

    # Force=True lets owner override.
    forced = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(), force=True,
    )
    assert forced['ok'] is True


def test_double_post_rejected(tmp_path):
    db = _mkdb(tmp_path)
    first = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    assert first['ok'] is True
    # Second post with same as-of but in a future period to avoid the
    # adoption-date guard, to isolate the already-posted check.
    second = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2027-01-01', rows=_balanced_rows(),
    )
    assert second['ok'] is False
    assert second['reason'] == 'already_posted'


def test_reversal_writes_compensating_rows(tmp_path):
    db = _mkdb(tmp_path)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    r = ob.reverse_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        reversed_by='cpa@firm.com',
    )
    assert r['ok'] is True
    assert r['rows_reversed'] == 4
    with sqlite3.connect(db) as conn:
        reversal_cnt = conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE source='opening_balance_reversal' AND client_code='CONS'"
        ).fetchone()[0]
        # After reversal, net balance across ALL opening + reversal rows
        # is zero (the original still exists — we just add compensating).
        debits, credits = conn.execute(
            "SELECT SUM(CASE WHEN side='debit' THEN amount END),"
            "       SUM(CASE WHEN side='credit' THEN amount END) "
            "FROM gl_transactions "
            "WHERE source IN ('opening_balance','opening_balance_reversal') "
            "AND client_code='CONS'"
        ).fetchone()
        status = conn.execute(
            "SELECT status FROM opening_balance_entries "
            "WHERE client_code='CONS'"
        ).fetchone()[0]
    assert reversal_cnt == 4
    assert round(debits - credits, 2) == 0.0
    assert status == 'reversed'


def test_repost_allowed_after_reversal(tmp_path):
    db = _mkdb(tmp_path)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    ob.reverse_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
    )
    # Re-post should succeed now. Use a slightly different as_of so
    # the entry_id doesn't collide.
    again = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-02', rows=_balanced_rows(),
    )
    assert again['ok'] is True


def test_existing_fs_unchanged_for_clients_without_opening(tmp_path):
    """A client with NO opening balances must have zero opening rows."""
    db = _mkdb(tmp_path)
    ob.ensure_schema(db)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    with sqlite3.connect(db) as conn:
        other = conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE client_code='OTHER' AND source='opening_balance'"
        ).fetchone()[0]
    assert other == 0


def test_validation_flags_duplicates_and_negatives():
    v = ob.validate_balances([
        {'account_code': '1000', 'side': 'debit',  'amount': 100},
        {'account_code': '1000', 'side': 'debit',  'amount': 200},
        {'account_code': '3000', 'side': 'credit', 'amount': 300},
        {'account_code': '', 'side': 'debit', 'amount': 10},
        {'account_code': '4000', 'side': 'sideways', 'amount': 10},
        {'account_code': '5000', 'side': 'credit', 'amount': -1},
    ])
    assert v['ok'] is False
    # At least 4 errors: duplicate, missing account, bad side, negative.
    assert len(v['errors']) >= 4
