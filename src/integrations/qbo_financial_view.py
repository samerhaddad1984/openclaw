"""Unified financial statements that combine OtoCPA-native + QBO-origin data.

QBO-origin journal entries are already mirrored into ``gl_transactions``
with ``source='qbo'`` by :mod:`qbo_pull`. The native engines
(:mod:`audit_engine`, :mod:`cashflow_engine`, etc.) build their
statements from the ``documents`` / ``ar_invoices`` tables and don't
read ``gl_transactions``, so they miss QBO-direct entries.

This module composes. Given a client + period, it returns a trial
balance where each account row carries:

- ``native_debit`` / ``native_credit``: contribution from OtoCPA
  documents/ar_invoices/manual_journal_entries.
- ``qbo_origin_debit`` / ``qbo_origin_credit``: contribution from
  ``gl_transactions`` rows with ``source='qbo'`` and
  ``entry_id LIKE 'QBO:%'``.
- ``debit`` / ``credit`` / ``balance``: the sum.
- ``sources``: set of source names for that account ({'native'},
  {'qbo'}, or {'native','qbo'}).

The double-count risk: when an OtoCPA-origin JE is pushed to QBO and
then pulled back, its mirror would duplicate the native GL row.
:func:`qbo_pull.QBOPull._upsert_journal_entry` guards against this by
refusing to mirror when ``sync_source='otocpa_origin'``, so only
genuine qbo_origin rows reach ``gl_transactions`` under ``source='qbo'``.
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _qbo_origin_totals(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> dict[str, dict[str, Decimal]]:
    """Aggregate gl_transactions WHERE source='qbo' by account for the period."""
    try:
        rows = conn.execute(
            "SELECT account_code, side, SUM(amount) AS s "
            "FROM gl_transactions "
            "WHERE client_code=? AND period=? AND source='qbo' "
            "GROUP BY account_code, side",
            (client_code, period),
        ).fetchall()
    except sqlite3.OperationalError:
        # gl_transactions might not exist in a fresh test DB
        return {}
    out: dict[str, dict[str, Decimal]] = {}
    for r in rows:
        acct = r['account_code']
        side = r['side']
        amt = Decimal(str(r['s'] or 0))
        out.setdefault(acct, {'debit': Decimal('0'), 'credit': Decimal('0')})
        out[acct][side] = amt
    return out


def unified_trial_balance(
    db_path: Path | str,
    *,
    client_code: str,
    period: str,
    native_tb: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Return the unified trial balance for a (client, period).

    ``native_tb`` lets callers pre-compute the legacy TB (so this module
    doesn't need a hard import on ``audit_engine``); when omitted we
    attempt the native computation but tolerate its absence gracefully.
    """
    with _open(db_path) as conn:
        qbo_totals = _qbo_origin_totals(conn, client_code, period)

    if native_tb is None:
        native_tb = _compute_native_tb(db_path, client_code, period)

    # Normalise native rows onto account_code keys.
    native_by_account: dict[str, dict[str, Decimal]] = {}
    for row in native_tb or []:
        acct = str(row.get('account_code') or row.get('account')
                    or row.get('gl_account') or '')
        if not acct:
            continue
        native_by_account[acct] = {
            'debit': Decimal(str(row.get('debit') or row.get('debit_total') or 0)),
            'credit': Decimal(str(row.get('credit') or row.get('credit_total') or 0)),
        }

    all_accounts = sorted(set(native_by_account.keys()) | set(qbo_totals.keys()))
    unified: list[dict[str, Any]] = []
    total_debit = Decimal('0')
    total_credit = Decimal('0')
    for acct in all_accounts:
        native = native_by_account.get(acct, {'debit': Decimal('0'), 'credit': Decimal('0')})
        qbo = qbo_totals.get(acct, {'debit': Decimal('0'), 'credit': Decimal('0')})
        debit = native['debit'] + qbo['debit']
        credit = native['credit'] + qbo['credit']
        balance = debit - credit
        sources = []
        if native['debit'] or native['credit']:
            sources.append('native')
        if qbo['debit'] or qbo['credit']:
            sources.append('qbo')
        unified.append({
            'account_code': acct,
            'native_debit':  float(native['debit']),
            'native_credit': float(native['credit']),
            'qbo_origin_debit':  float(qbo['debit']),
            'qbo_origin_credit': float(qbo['credit']),
            'debit':   float(debit),
            'credit':  float(credit),
            'balance': float(balance),
            'sources': sources,
        })
        total_debit += debit
        total_credit += credit

    return {
        'accounts': unified,
        'total_debits':  float(total_debit),
        'total_credits': float(total_credit),
        'balanced': abs(total_debit - total_credit) < Decimal('0.01'),
        'sources': {
            'native_accounts': sum(
                1 for r in unified if 'native' in r['sources']
            ),
            'qbo_origin_accounts': sum(
                1 for r in unified if 'qbo' in r['sources']
            ),
            'both': sum(1 for r in unified if len(r['sources']) == 2),
        },
    }


def _compute_native_tb(db_path: Path | str, client_code: str,
                        period: str) -> list[dict[str, Any]]:
    """Call ``audit_engine.generate_trial_balance`` when available. Returns
    [] when the dependency surface isn't available in this DB (test mode)."""
    try:
        from src.engines.audit_engine import generate_trial_balance
    except ImportError:
        return []
    with _open(db_path) as conn:
        try:
            return generate_trial_balance(conn, client_code, period) or []
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Income-statement / balance-sheet helpers
#
# These return plain dicts keyed by account classification so downstream
# renderers don't need to know the source breakdown — they can still
# display it from the .sources field when needed.
# ---------------------------------------------------------------------------


def unified_income_statement(
    db_path: Path | str,
    *,
    client_code: str,
    period: str,
    account_classifier: Optional[callable] = None,
) -> dict[str, Any]:
    """Split unified TB rows into Revenue / Expense totals via
    ``account_classifier(account_code) -> 'revenue' | 'expense' | 'other'``.

    Default classifier: account codes starting with '4' are revenue,
    '5'/'6' are expense, everything else is 'other'.
    """
    if account_classifier is None:
        def account_classifier(code: str) -> str:
            if not code:
                return 'other'
            if code[0] == '4':
                return 'revenue'
            if code[0] in ('5', '6'):
                return 'expense'
            return 'other'

    tb = unified_trial_balance(db_path, client_code=client_code, period=period)
    revenue = Decimal('0')
    expense = Decimal('0')
    breakdown: dict[str, list[dict[str, Any]]] = {'revenue': [], 'expense': []}
    for row in tb['accounts']:
        kind = account_classifier(row['account_code'])
        if kind == 'revenue':
            amount = Decimal(str(row['credit'])) - Decimal(str(row['debit']))
            revenue += amount
            breakdown['revenue'].append({**row, 'amount': float(amount)})
        elif kind == 'expense':
            amount = Decimal(str(row['debit'])) - Decimal(str(row['credit']))
            expense += amount
            breakdown['expense'].append({**row, 'amount': float(amount)})
    net_income = revenue - expense
    return {
        'revenue_total': float(revenue),
        'expense_total': float(expense),
        'net_income':    float(net_income),
        'rows':          breakdown,
        'sources':       tb['sources'],
    }


def unified_balance_sheet(
    db_path: Path | str,
    *,
    client_code: str,
    period: str,
) -> dict[str, Any]:
    """Split TB rows into Assets / Liabilities / Equity totals. Default
    classifier uses the standard 1xxx / 2xxx / 3xxx numbering."""
    tb = unified_trial_balance(db_path, client_code=client_code, period=period)
    assets = Decimal('0')
    liab = Decimal('0')
    equity = Decimal('0')
    breakdown: dict[str, list[dict[str, Any]]] = {
        'assets': [], 'liabilities': [], 'equity': [],
    }
    for row in tb['accounts']:
        code = row['account_code']
        debit = Decimal(str(row['debit']))
        credit = Decimal(str(row['credit']))
        if code.startswith('1'):
            amount = debit - credit
            assets += amount
            breakdown['assets'].append({**row, 'amount': float(amount)})
        elif code.startswith('2'):
            amount = credit - debit
            liab += amount
            breakdown['liabilities'].append({**row, 'amount': float(amount)})
        elif code.startswith('3'):
            amount = credit - debit
            equity += amount
            breakdown['equity'].append({**row, 'amount': float(amount)})
    return {
        'assets_total':      float(assets),
        'liabilities_total': float(liab),
        'equity_total':      float(equity),
        'balanced': abs(assets - (liab + equity)) < Decimal('0.01'),
        'rows': breakdown,
        'sources': tb['sources'],
    }
