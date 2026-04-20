"""Multi-source reconciliation helpers.

The existing ``src.engines.reconciliation_engine`` works against a
statement-backed ``bank_transactions`` schema (statement_id + debit/
credit). The smart-bank-source build extends the other
``bank_transactions`` schema (Plaid-style: id TEXT, date, amount) —
this module provides reconciliation-facing helpers that speak that
shape and honour the ``hidden_duplicate`` flag.

Public surface:

- :func:`get_unmatched_bank_transactions` — single-call fetch of
  unmatched, non-hidden rows for a (firm, client, period).
- :func:`source_badge` — tiny render helper so the dashboard can
  show ``[QBO]`` / ``[Plaid]`` per row.
- :func:`period_totals_by_source` — summary of per-source totals
  before and after dedup, used by the setup page and dedup page.

Every query tolerates the hidden_duplicate column being absent
(legacy test DBs) via a COALESCE.
"""
from __future__ import annotations

import html
import sqlite3
from pathlib import Path
from typing import Any


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def get_unmatched_bank_transactions(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    period_start: str,
    period_end: str,
    include_hidden: bool = False,
) -> list[dict[str, Any]]:
    """Return unmatched bank rows for the period, filtering out rows
    flagged as duplicates unless ``include_hidden`` is explicitly set.

    The ``source`` column is returned so the UI can render the badge.
    """
    hidden_clause = "" if include_hidden else (
        " AND COALESCE(hidden_duplicate, 0) = 0"
    )
    with _open(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT id, firm_code, client_code, source, external_id,
                   date, amount, description, merchant_name,
                   qbo_account_id, hidden_duplicate
            FROM bank_transactions
            WHERE firm_code = ?
              AND client_code = ?
              AND date BETWEEN ? AND ?
              AND (matched_document_id IS NULL OR matched_document_id = '')
              {hidden_clause}
            ORDER BY date, amount
            """,
            (firm_code, client_code, period_start, period_end),
        ).fetchall()
    return [dict(r) for r in rows]


def period_totals_by_source(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    period_start: str,
    period_end: str,
) -> dict[str, Any]:
    """Return per-source row counts + amount totals for the period.

    Structure::

        {
            'sources': {'qbo': {'count': 12, 'total': 1234.56, 'hidden': 2},
                        'plaid': {...}},
            'visible_count': 22,
            'hidden_count': 2,
        }
    """
    with _open(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(source, 'plaid') AS src,
                   COALESCE(hidden_duplicate, 0) AS hid,
                   COUNT(*) AS n,
                   COALESCE(SUM(amount), 0) AS total
            FROM bank_transactions
            WHERE firm_code = ?
              AND client_code = ?
              AND date BETWEEN ? AND ?
            GROUP BY src, hid
            """,
            (firm_code, client_code, period_start, period_end),
        ).fetchall()

    sources: dict[str, dict[str, Any]] = {}
    visible = hidden = 0
    for r in rows:
        s = sources.setdefault(r['src'], {
            'count': 0, 'hidden': 0, 'total': 0.0,
        })
        if r['hid']:
            s['hidden'] += r['n']
            hidden += r['n']
        else:
            s['count'] += r['n']
            s['total'] += float(r['total'] or 0.0)
            visible += r['n']
    return {
        'sources': sources,
        'visible_count': visible,
        'hidden_count': hidden,
    }


# ---------------------------------------------------------------------------
# Render helper
# ---------------------------------------------------------------------------


_SOURCE_STYLES = {
    'qbo':   ('QBO',   '#fef3c7', '#92400e'),
    'plaid': ('Plaid', '#dbeafe', '#1e40af'),
    'manual': ('Manual', '#e4e4e4', '#444'),
}


def source_badge(source: str | None) -> str:
    label, bg, fg = _SOURCE_STYLES.get(
        (source or 'plaid').lower(),
        ('?', '#e4e4e4', '#444'),
    )
    return (f'<span class="source-badge" style="padding:1px 6px;'
            f'border-radius:8px;background:{bg};color:{fg};'
            f'font-size:.78em;">{html.escape(label)}</span>')
