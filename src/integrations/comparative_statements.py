"""Prior-year comparative financial statements from imported data.

After mid-year OtoCPA adoption the current-year books are native
(``audit_engine.generate_financial_statements``), but the prior-year
column still needs to come from whatever the client used before. For
clients who came through Scope 2.2 (QBO historical) or Scope 2.3
(opening balances) or Scope 2.4 (Caseware/Sage/Excel/IIF import) we
already have that data staged in ``gl_transactions`` or
``historical_imports``.

This module stitches the comparative together: it picks the right
source for the prior-year column, labels every line with its
provenance, and adds the mandatory disclosure note.

Design boundaries:

  - No new GL writes — we only read data already posted by the three
    source scopes. The native FS engine stays untouched; we wrap its
    output with a prior-year dict.
  - Source selection preference: native (if the prior period has
    native GL data), else historical_import, else qbo_historical,
    else opening_balances. First match wins.
  - A client with *no* prior-year data returns a comparative where
    prior_year is ``None`` and the disclosure is omitted — the UI
    can hide the column.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


SOURCE_NATIVE = 'native'
SOURCE_HISTORICAL = 'historical_import'
SOURCE_QBO = 'qbo_historical'
SOURCE_OPENING = 'opening_balance'


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _period_bounds(period: str) -> tuple[str, str]:
    """Return (start, end) inclusive for a YYYY or YYYY-MM period label."""
    if len(period) == 4:  # year
        return (f'{period}-01-01', f'{period}-12-31')
    # YYYY-MM
    import calendar
    year, month = period.split('-')
    last = calendar.monthrange(int(year), int(month))[1]
    return (f'{period}-01', f'{period}-{last:02d}')


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------


def get_prior_year_gl_rows(db_path: Path | str, client_code: str,
                           prior_period: str) -> list[dict]:
    """Return ``gl_transactions`` rows for the prior period, regardless
    of source. Returned rows carry the ``source`` column untouched so
    callers can bucket by provenance.
    """
    start, end = _period_bounds(prior_period)
    with _open(db_path) as conn:
        tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='gl_transactions'"
        ).fetchone()
        if not tbl:
            return []
        rows = conn.execute(
            "SELECT account_code, side, amount, source, entry_date, "
            "       description FROM gl_transactions "
            "WHERE client_code=? AND entry_date BETWEEN ? AND ?",
            (client_code, start, end),
        ).fetchall()
    return [dict(r) for r in rows]


def detect_prior_year_source(rows: list[dict]) -> str | None:
    """Given a row list, report which source provided the majority of
    data for the period (used for the disclosure note).

    Returns the source identifier or ``None`` if the list is empty.
    """
    if not rows:
        return None
    counts: dict[str, int] = {}
    for r in rows:
        src = (r.get('source') or '').strip()
        # Historical imports tag as historical_<fmt>; bucket them all.
        if src.startswith('historical_'):
            bucket = SOURCE_HISTORICAL
        elif src == 'qbo':
            bucket = SOURCE_QBO
        elif src == 'opening_balance':
            bucket = SOURCE_OPENING
        else:
            bucket = SOURCE_NATIVE
        counts[bucket] = counts.get(bucket, 0) + 1
    # Priority: native > historical > qbo > opening for tie-break.
    priority = (SOURCE_NATIVE, SOURCE_HISTORICAL, SOURCE_QBO, SOURCE_OPENING)
    ordered = sorted(counts.items(),
                     key=lambda kv: (-kv[1], priority.index(kv[0])))
    return ordered[0][0]


def get_source_label(db_path: Path | str, client_code: str,
                     source: str) -> str:
    """Produce the human-readable label for the disclosure note.

    Falls back to the bare source identifier when the module that
    owns the underlying record is unavailable.
    """
    if source == SOURCE_NATIVE:
        return 'OtoCPA native ledger'
    if source == SOURCE_OPENING:
        return 'opening balance migration'
    if source == SOURCE_QBO:
        return 'QuickBooks Online historical import'
    if source == SOURCE_HISTORICAL:
        # Look up the most recent historical_imports row for this client
        # and report the specific format.
        try:
            with _open(db_path) as conn:
                tbl = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND "
                    "name='historical_imports'"
                ).fetchone()
                if tbl:
                    row = conn.execute(
                        "SELECT source_format, posted_at FROM historical_imports "
                        "WHERE client_code=? AND status='posted' "
                        "ORDER BY posted_at DESC LIMIT 1",
                        (client_code,),
                    ).fetchone()
                    if row:
                        fmt_label = {
                            'csv': 'generic CSV',
                            'iif': 'QuickBooks Desktop IIF',
                            'excel_tb': 'Excel trial balance',
                            'sage50': 'Sage 50 export',
                            'caseware': 'Caseware trial balance',
                        }.get(row['source_format'], row['source_format'])
                        return f'imported from {fmt_label}'
        except Exception:
            pass
        return 'historical import'
    return source


def get_import_date(db_path: Path | str, client_code: str) -> str | None:
    """Return the ISO date when the client's historical data was
    imported (if any). Used for the disclosure note.
    """
    try:
        with _open(db_path) as conn:
            tbl = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND "
                "name='historical_imports'"
            ).fetchone()
            if tbl:
                row = conn.execute(
                    "SELECT posted_at FROM historical_imports "
                    "WHERE client_code=? AND status='posted' "
                    "ORDER BY posted_at DESC LIMIT 1",
                    (client_code,),
                ).fetchone()
                if row and row['posted_at']:
                    return str(row['posted_at'])[:10]
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Comparative statement assembly
# ---------------------------------------------------------------------------


def roll_up_rows(rows: list[dict]) -> dict[str, dict]:
    """Collapse per-leg rows into per-account totals.

    Returns ``{account_code: {debit, credit, net, sources}}`` where
    ``sources`` is the set of distinct ``source`` tags that
    contributed.
    """
    out: dict[str, dict] = {}
    for r in rows:
        code = r['account_code']
        bucket = out.setdefault(code, {
            'debit': Decimal('0'), 'credit': Decimal('0'),
            'sources': set(),
        })
        amt = Decimal(str(r['amount']))
        if r['side'] == 'debit':
            bucket['debit'] += amt
        else:
            bucket['credit'] += amt
        src = (r.get('source') or '').strip()
        if src.startswith('historical_'):
            bucket['sources'].add(SOURCE_HISTORICAL)
        elif src == 'qbo':
            bucket['sources'].add(SOURCE_QBO)
        elif src == 'opening_balance':
            bucket['sources'].add(SOURCE_OPENING)
        else:
            bucket['sources'].add(SOURCE_NATIVE)
    for bucket in out.values():
        bucket['net'] = bucket['debit'] - bucket['credit']
        bucket['sources'] = sorted(bucket['sources'])
    return out


def build_comparative(
    db_path: Path | str,
    client_code: str,
    current_period: str,
    prior_period: str,
    current_statements: dict,
) -> dict:
    """Return a dict shaped like ``current_statements`` with a
    ``prior_year`` subtree and disclosure metadata.

    ``current_statements`` is the output of
    ``audit_engine.generate_financial_statements``. The function does
    not touch its structure — it attaches a sibling ``prior_year``
    dict with the same shape (balance_sheet + income_statement) and
    adds ``comparative_metadata`` with source + disclosure note.
    """
    prior_rows = get_prior_year_gl_rows(db_path, client_code, prior_period)
    source = detect_prior_year_source(prior_rows)
    rolled = roll_up_rows(prior_rows)
    # Map accounts into BS/IS buckets by sign, mirroring the engine's
    # `infer_account_type` — but without re-importing the full engine
    # we use the first digit as a simple convention: 1=asset, 2=liab,
    # 3=equity, 4=revenue, 5-9=expense. When the chart is available
    # in the DB we prefer its account_type.
    chart: dict[str, str] = {}
    try:
        with _open(db_path) as conn:
            tbl = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND "
                "name='chart_of_accounts'"
            ).fetchone()
            if tbl:
                for r in conn.execute(
                    "SELECT account_code, account_type FROM chart_of_accounts"
                ).fetchall():
                    chart[r['account_code']] = (r['account_type'] or '').lower()
    except Exception:
        pass

    def _type_for(code: str) -> str:
        if code in chart and chart[code]:
            return chart[code]
        first = (code[:1] or '0')
        return {
            '1': 'asset', '2': 'liability', '3': 'equity',
            '4': 'revenue', '5': 'expense', '6': 'expense',
            '7': 'expense', '8': 'expense', '9': 'expense',
        }.get(first, 'expense')

    bs = {'assets': [], 'liabilities': [], 'equity': []}
    is_ = {'revenue': [], 'expenses': []}
    sources_seen: set[str] = set()
    for code, bucket in rolled.items():
        amt = bucket['net']
        atype = _type_for(code)
        item = {
            'account_code': code,
            'amount': float(amt),
            'sources': bucket['sources'],
        }
        sources_seen.update(bucket['sources'])
        if atype == 'asset':
            bs['assets'].append(item)
        elif atype == 'liability':
            item['amount'] = float(-amt)
            bs['liabilities'].append(item)
        elif atype == 'equity':
            item['amount'] = float(-amt)
            bs['equity'].append(item)
        elif atype == 'revenue':
            item['amount'] = float(-amt)
            is_['revenue'].append(item)
        elif atype == 'expense':
            is_['expenses'].append(item)

    prior_year = {
        'balance_sheet': bs,
        'income_statement': is_,
        'totals': {
            'assets': round(sum(i['amount'] for i in bs['assets']), 2),
            'liabilities': round(
                sum(i['amount'] for i in bs['liabilities']), 2
            ),
            'equity': round(sum(i['amount'] for i in bs['equity']), 2),
            'revenue': round(sum(i['amount'] for i in is_['revenue']), 2),
            'expenses': round(
                sum(i['amount'] for i in is_['expenses']), 2
            ),
        },
    } if prior_rows else None

    disclosure = ''
    comparative_enabled = bool(prior_rows)
    if prior_year and source and source != SOURCE_NATIVE:
        label = get_source_label(db_path, client_code, source)
        import_date = get_import_date(db_path, client_code) or 'an earlier date'
        disclosure = (
            f'Prior year figures ({prior_period}) were {label} on '
            f'{import_date}. Current year figures are generated by OtoCPA.'
        )

    return {
        'current_year': current_statements,
        'prior_year': prior_year,
        'comparative_metadata': {
            'enabled': comparative_enabled,
            'source': source,
            'source_label': (
                get_source_label(db_path, client_code, source)
                if source else None
            ),
            'current_period': current_period,
            'prior_period': prior_period,
            'disclosure_note': disclosure,
            'sources_seen': sorted(sources_seen),
        },
    }


def toggle_comparative(comparative: dict, enabled: bool) -> dict:
    """Return a copy of ``comparative`` with ``prior_year`` hidden
    when ``enabled`` is False. The metadata.enabled flag tracks the
    toggle so the UI can label accordingly.
    """
    out = dict(comparative)
    meta = dict(out.get('comparative_metadata') or {})
    meta['enabled'] = bool(enabled) and meta.get('enabled', False)
    out['comparative_metadata'] = meta
    if not enabled:
        out['prior_year'] = None
    return out
