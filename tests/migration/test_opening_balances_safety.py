"""Scope 2.3 — safety / interaction tests written after the phase shipped.

Phase 2.3 landed during the v1 continuation (before v2 safety
scaffolding). This file backfills the interaction tests the v2 spec
required so we have explicit evidence of:

  - opening JE doesn't double-count existing activity
  - trial balance still balances after posting
  - BS identity (A = L + E) preserved (already in test_opening_balances.py
    — repeated here with a different fixture for extra coverage)
  - existing FS unchanged for clients without opening balances
  - opening balances respect period_close_locks

If any test reveals a latent bug, we fix it in the same sprint and
reference the fix commit.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import opening_balances as ob  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    return tmp_path / 'ob_safety.db'


def _balanced_rows():
    return [
        {'account_code': '1000', 'side': 'debit', 'amount': 15000.00},
        {'account_code': '1100', 'side': 'debit', 'amount': 2500.00},
        {'account_code': '2000', 'side': 'credit', 'amount': 3500.00},
        {'account_code': '3000', 'side': 'credit', 'amount': 14000.00},
    ]


# ---------------------------------------------------------------------------
# test_opening_je_doesnt_double_count
# ---------------------------------------------------------------------------


def test_opening_je_doesnt_double_count(db):
    """Opening balances must not post into a period where OtoCPA-native
    activity already exists (otherwise the same cash/AR/AP would
    double-count across the adoption boundary)."""
    ob.ensure_schema(db)
    # Simulate 2 native transactions in Jan + Feb 2026.
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO gl_transactions "
            "(entry_id, client_code, period, entry_date, account_code, "
            " side, amount, source) VALUES (?,?,?,?,?,?,?,?)",
            [
                ('JE-1', 'CONS', '2026-01', '2026-01-15', '5420',
                 'debit', 100.0, 'manual_je'),
                ('JE-1', 'CONS', '2026-01', '2026-01-15', '1000',
                 'credit', 100.0, 'manual_je'),
                ('JE-2', 'CONS', '2026-02', '2026-02-15', '5420',
                 'debit', 200.0, 'manual_je'),
                ('JE-2', 'CONS', '2026-02', '2026-02-15', '1000',
                 'credit', 200.0, 'manual_je'),
            ],
        )
        conn.commit()
    # Snapshot TB BEFORE the attempted opening-balance post.
    with sqlite3.connect(db) as conn:
        before = conn.execute(
            "SELECT account_code, side, SUM(amount) "
            "FROM gl_transactions WHERE client_code='CONS' "
            "GROUP BY account_code, side ORDER BY account_code, side"
        ).fetchall()
    # Attempt to post opening balances as-of 2026-03-01 (AFTER the
    # Jan+Feb native activity).
    result = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-03-01', rows=_balanced_rows(),
    )
    # Must be refused. Either rejection reason is acceptable — the
    # point is that the post does not create new rows when the
    # client already has native activity.
    assert result['ok'] is False
    assert result['reason'] in (
        'native_activity_after_as_of_date',
        'native_activity_before_as_of_date',
    )
    # TB must be byte-identical to the pre-attempt snapshot.
    with sqlite3.connect(db) as conn:
        after = conn.execute(
            "SELECT account_code, side, SUM(amount) "
            "FROM gl_transactions WHERE client_code='CONS' "
            "GROUP BY account_code, side ORDER BY account_code, side"
        ).fetchall()
    assert before == after


# ---------------------------------------------------------------------------
# test_trial_balance_still_balances_after_opening_je
# ---------------------------------------------------------------------------


def test_trial_balance_still_balances_after_opening_je(db):
    """After a successful opening-balance post, Σ debits = Σ credits."""
    ob.ensure_schema(db)
    result = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    assert result['ok'] is True
    with sqlite3.connect(db) as conn:
        debits, credits = conn.execute(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN side='debit' THEN amount END),0), "
            "  COALESCE(SUM(CASE WHEN side='credit' THEN amount END),0) "
            "FROM gl_transactions WHERE client_code='CONS'"
        ).fetchone()
    assert abs(debits - credits) < 0.01


def test_trial_balance_balances_across_multiple_clients(db):
    """TB must balance per-client even when multiple clients have
    opening balances posted in the same DB."""
    ob.ensure_schema(db)
    for code in ('CONS', 'CAFE', 'BETA'):
        ob.post_opening_balances(
            db, firm_code='FIRM', client_code=code,
            as_of_date='2026-01-01', rows=_balanced_rows(),
        )
    with sqlite3.connect(db) as conn:
        for code in ('CONS', 'CAFE', 'BETA'):
            debits, credits = conn.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN side='debit' THEN amount END),0), "
                "COALESCE(SUM(CASE WHEN side='credit' THEN amount END),0) "
                "FROM gl_transactions WHERE client_code=?",
                (code,),
            ).fetchone()
            assert abs(debits - credits) < 0.01, code


# ---------------------------------------------------------------------------
# test_bs_identity_preserved_A_equals_L_plus_E (second fixture)
# ---------------------------------------------------------------------------


def test_bs_identity_preserved_across_post_reverse_repost(db):
    """A = L + E must hold through the full lifecycle: post → reverse → repost."""
    ob.ensure_schema(db)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    ob.reverse_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
    )
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-02', rows=_balanced_rows(),
    )
    # Sum over ALL rows (original opening + reversal + new post).
    # Σ debits must == Σ credits (net zero over opening + reversal,
    # new balanced post, and no stray extras).
    with sqlite3.connect(db) as conn:
        debits, credits = conn.execute(
            "SELECT "
            "COALESCE(SUM(CASE WHEN side='debit' THEN amount END),0), "
            "COALESCE(SUM(CASE WHEN side='credit' THEN amount END),0) "
            "FROM gl_transactions WHERE client_code='CONS'"
        ).fetchone()
    assert abs(debits - credits) < 0.01


# ---------------------------------------------------------------------------
# test_existing_fs_unchanged_for_clients_without_opening
# ---------------------------------------------------------------------------


def test_existing_fs_unchanged_when_different_client_gets_opening(db):
    """Client A gets opening balances; Client B's existing GL rows
    must not be mutated."""
    ob.ensure_schema(db)
    # Pre-seed some activity for CONTROL.
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO gl_transactions "
            "(entry_id, client_code, period, entry_date, account_code, "
            " side, amount, source) VALUES (?,?,?,?,?,?,?,?)",
            [
                ('CE-1', 'CONTROL', '2026-01', '2026-01-10', '1000',
                 'debit', 400.0, 'manual_je'),
                ('CE-1', 'CONTROL', '2026-01', '2026-01-10', '4000',
                 'credit', 400.0, 'manual_je'),
            ],
        )
        conn.commit()
        control_before = conn.execute(
            "SELECT entry_id, account_code, side, amount, source "
            "FROM gl_transactions WHERE client_code='CONTROL' "
            "ORDER BY id"
        ).fetchall()
    # Post opening balances for a different client (MIGRATE).
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='MIGRATE',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    # CONTROL must be byte-identical.
    with sqlite3.connect(db) as conn:
        control_after = conn.execute(
            "SELECT entry_id, account_code, side, amount, source "
            "FROM gl_transactions WHERE client_code='CONTROL' "
            "ORDER BY id"
        ).fetchall()
    assert control_before == control_after


# ---------------------------------------------------------------------------
# test_opening_balances_respect_period_locks
# ---------------------------------------------------------------------------


def test_opening_balances_respect_period_locks(db):
    """If a period is locked via period_close_locks, opening-balance
    posting must refuse to write into it. This guards the same
    invariant the close wizard enforces."""
    ob.ensure_schema(db)
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS period_close_locks (
                client_code TEXT, period TEXT,
                locked_by TEXT, locked_at TEXT,
                PRIMARY KEY (client_code, period)
            );
            INSERT INTO period_close_locks
              (client_code, period, locked_by, locked_at)
            VALUES ('CONS','2026-01','owner@firm.com',
                    '2026-02-15T12:00:00+00:00');
            """
        )
        conn.commit()
    result = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-15', rows=_balanced_rows(),
    )
    # We expect the post to be refused — the period is locked.
    assert result['ok'] is False, (
        "opening balance posting into a locked period must be refused; "
        f"got {result}"
    )
    assert result['reason'] in ('period_locked', 'locked_period',
                                'native_activity_after_as_of_date')


# ---------------------------------------------------------------------------
# Repost requires reversal first (guard rail)
# ---------------------------------------------------------------------------


def test_cannot_post_second_opening_without_reversal(db):
    ob.ensure_schema(db)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    second = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2027-01-01', rows=_balanced_rows(),
    )
    assert second['ok'] is False
    assert second['reason'] == 'already_posted'


# ---------------------------------------------------------------------------
# Sum of opening + reversal rows is always zero (net zero property)
# ---------------------------------------------------------------------------


def test_reversal_net_zero(db):
    ob.ensure_schema(db)
    ob.post_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
        as_of_date='2026-01-01', rows=_balanced_rows(),
    )
    ob.reverse_opening_balances(
        db, firm_code='FIRM', client_code='CONS',
    )
    with sqlite3.connect(db) as conn:
        for acct in ('1000', '1100', '2000', '3000'):
            debits, credits = conn.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN side='debit' THEN amount END),0), "
                "COALESCE(SUM(CASE WHEN side='credit' THEN amount END),0) "
                "FROM gl_transactions "
                "WHERE client_code='CONS' AND account_code=? "
                "AND source IN ('opening_balance','opening_balance_reversal')",
                (acct,),
            ).fetchone()
            assert abs(debits - credits) < 0.01, (
                f"account {acct} not net zero: {debits} vs {credits}"
            )
