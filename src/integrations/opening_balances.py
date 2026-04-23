"""Opening balances for mid-year OtoCPA adoption.

When a firm starts using OtoCPA for a client that already has ledger
activity elsewhere (spreadsheets, Sage, Caseware), they need to
anchor the book at the adoption date with opening balances so
subsequent OtoCPA postings produce a correct trial balance and
balance sheet.

Semantics (the invariants the tests pin):

  1. Trial balance MUST sum to zero (|Σ debits − Σ credits| ≤ $0.01).
  2. Opening balances can only be posted when no native (non-QBO,
     non-opening) GL activity exists AT or AFTER the as-of date
     for this client — otherwise we'd be double-counting or
     back-dating into a period already populated.
  3. Posting is idempotent at the (firm, client) level: a client
     has exactly one open opening-balance entry. A second post is
     rejected unless the first has been reversed.
  4. Posted rows are locked (``source='opening_balance'``,
     ``status='locked'``), and can only be undone via an explicit
     reversal entry written to ``gl_transactions`` with swapped
     sides and ``source='opening_balance_reversal'``.

All GL writes go through the existing ``gl_transactions`` table
(schema owned by ``src.engines.gl_engine``), so trial balance +
financial statement generators automatically pick up opening
balances without further changes.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)


SOURCE_OPENING = 'opening_balance'
SOURCE_OPENING_REVERSAL = 'opening_balance_reversal'
BALANCE_TOLERANCE = 0.01


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def ensure_schema(db_path: Path | str) -> None:
    """Create the gl_transactions + opening-balance support tables.

    Idempotent; safe to call every request.
    """
    # Piggyback on gl_engine schema so we match its constraints exactly.
    try:
        from src.engines.gl_engine import ensure_schema as _gl_ensure
        _gl_ensure(db_path=Path(db_path))
    except Exception:
        # Fall back to the table we need if gl_engine import fails
        # (tests may skip it). The schema matches gl_engine's DDL.
        with _open(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gl_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_id TEXT NOT NULL,
                    client_code TEXT NOT NULL,
                    period TEXT NOT NULL,
                    entry_date TEXT NOT NULL,
                    account_code TEXT NOT NULL,
                    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount REAL NOT NULL CHECK (amount > 0),
                    description TEXT,
                    source TEXT NOT NULL DEFAULT 'manual_je',
                    document_id TEXT,
                    reversed_by TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
            conn.commit()

    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS opening_balance_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                entry_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'locked',
                posted_at TEXT,
                posted_by TEXT,
                description TEXT,
                reversed_at TEXT,
                reversed_by TEXT
            )
        """)
        # Partial unique index: at most one 'locked' entry per (firm,
        # client). Reversed entries are history and don't block reposts.
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "uq_opening_balance_locked "
            "ON opening_balance_entries(firm_code, client_code) "
            "WHERE status='locked'"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS opening_balance_supporting_docs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id TEXT NOT NULL,
                document_id TEXT NOT NULL,
                filename TEXT,
                attached_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_balances(
    rows: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Return {ok, debits, credits, diff, errors}. Pure function."""
    debits = 0.0
    credits = 0.0
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for i, r in enumerate(rows):
        acct = str(r.get('account_code') or '').strip()
        side = str(r.get('side') or '').strip().lower()
        try:
            amt = float(r.get('amount') or 0)
        except (TypeError, ValueError):
            errors.append(f"row {i + 1}: amount is not numeric")
            continue
        if not acct:
            errors.append(f"row {i + 1}: account_code is required")
            continue
        if side not in ('debit', 'credit'):
            errors.append(f"row {i + 1}: side must be debit or credit")
            continue
        if amt <= 0:
            errors.append(f"row {i + 1}: amount must be > 0")
            continue
        key = (acct, side)
        if key in seen:
            errors.append(
                f"row {i + 1}: duplicate ({acct}, {side}) — consolidate"
            )
            continue
        seen.add(key)
        if side == 'debit':
            debits += amt
        else:
            credits += amt
    diff = round(debits - credits, 2)
    ok = (not errors) and abs(diff) <= BALANCE_TOLERANCE
    return {
        'ok': ok,
        'debits': round(debits, 2),
        'credits': round(credits, 2),
        'diff': diff,
        'errors': errors,
    }


# ---------------------------------------------------------------------------
# Adoption-date guard
# ---------------------------------------------------------------------------


def has_native_activity_on_or_after(
    db_path: Path | str, *,
    client_code: str, as_of_date: str,
) -> bool:
    """True when non-opening GL rows exist on or after ``as_of_date``.

    Used to reject posting opening balances into a period that already
    has OtoCPA-native activity (which would double-count).
    """
    ensure_schema(db_path)
    with _open(db_path) as conn:
        try:
            row = conn.execute(
                "SELECT 1 FROM gl_transactions "
                "WHERE client_code=? "
                "  AND entry_date >= ? "
                "  AND source NOT IN (?, ?) "
                "LIMIT 1",
                (client_code, as_of_date,
                 SOURCE_OPENING, SOURCE_OPENING_REVERSAL),
            ).fetchone()
            return row is not None
        except sqlite3.OperationalError:
            return False


def get_opening_balance_state(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
) -> dict[str, Any] | None:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM opening_balance_entries "
            "WHERE firm_code=? AND client_code=? "
            "ORDER BY id DESC LIMIT 1",
            (firm_code, client_code),
        ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Posting
# ---------------------------------------------------------------------------


def post_opening_balances(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    as_of_date: str,
    rows: list[dict[str, Any]],
    description: str = '',
    posted_by: str = '',
    force: bool = False,
    supporting_document_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Post opening balances as a locked JE batch.

    ``force=True`` lets an owner post even when native activity exists
    at or after ``as_of_date`` (use with caution — requires explicit
    confirmation at the UI layer).
    """
    ensure_schema(db_path)
    # Validate.
    validation = validate_balances(rows)
    if not validation['ok']:
        return {
            'ok': False, 'reason': 'unbalanced_or_invalid',
            'validation': validation,
        }

    # Adoption-date guard.
    if not force and has_native_activity_on_or_after(
        db_path, client_code=client_code, as_of_date=as_of_date,
    ):
        return {
            'ok': False,
            'reason': 'native_activity_after_as_of_date',
            'validation': validation,
        }

    # Single-open-entry invariant: refuse to post if an active
    # (non-reversed) opening-balance entry already exists.
    existing = get_opening_balance_state(
        db_path, firm_code=firm_code, client_code=client_code,
    )
    if existing and existing.get('status') == 'locked':
        return {
            'ok': False, 'reason': 'already_posted',
            'existing_entry_id': existing.get('entry_id'),
        }

    entry_id = f"OB-{client_code}-{as_of_date.replace('-','')}"
    period = as_of_date[:7]  # YYYY-MM
    now = _iso_now()

    with _open(db_path) as conn:
        # Write the header row first so GL legs share a real entry_id.
        try:
            conn.execute(
                "INSERT INTO opening_balance_entries "
                "(firm_code, client_code, as_of_date, entry_id, status, "
                "posted_at, posted_by, description) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (firm_code, client_code, as_of_date, entry_id, 'locked',
                 now, posted_by, description or 'Opening balances migration'),
            )
        except sqlite3.IntegrityError:
            return {
                'ok': False, 'reason': 'entry_id_collision',
                'entry_id': entry_id,
            }

        # GL legs.
        for r in rows:
            conn.execute(
                "INSERT INTO gl_transactions "
                "(entry_id, client_code, period, entry_date, "
                "account_code, side, amount, description, source) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (entry_id, client_code, period, as_of_date,
                 str(r['account_code']).strip(), str(r['side']).strip().lower(),
                 float(r['amount']),
                 description or 'Opening balance',
                 SOURCE_OPENING),
            )
        # Supporting docs.
        for doc_id in (supporting_document_ids or []):
            conn.execute(
                "INSERT INTO opening_balance_supporting_docs "
                "(entry_id, document_id) VALUES (?,?)",
                (entry_id, doc_id),
            )
        conn.commit()

    return {
        'ok': True, 'entry_id': entry_id,
        'as_of_date': as_of_date,
        'debits': validation['debits'],
        'credits': validation['credits'],
        'rows_posted': len(rows),
        'supporting_docs': len(supporting_document_ids or []),
    }


def reverse_opening_balances(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    reversed_by: str = '',
) -> dict[str, Any]:
    """Reverse the locked opening-balance entry by writing flipped GL rows."""
    ensure_schema(db_path)
    existing = get_opening_balance_state(
        db_path, firm_code=firm_code, client_code=client_code,
    )
    if not existing or existing.get('status') != 'locked':
        return {'ok': False, 'reason': 'no_locked_entry_to_reverse'}
    entry_id = existing['entry_id']
    reversal_entry_id = f"{entry_id}-REV"
    now = _iso_now()
    with _open(db_path) as conn:
        # Flip sides for every original leg.
        legs = list(conn.execute(
            "SELECT client_code, period, entry_date, account_code, "
            "side, amount, description "
            "FROM gl_transactions WHERE entry_id=? AND source=?",
            (entry_id, SOURCE_OPENING),
        ))
        for leg in legs:
            flipped = 'credit' if leg['side'] == 'debit' else 'debit'
            conn.execute(
                "INSERT INTO gl_transactions "
                "(entry_id, client_code, period, entry_date, "
                "account_code, side, amount, description, source) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (reversal_entry_id, leg['client_code'],
                 leg['period'], leg['entry_date'], leg['account_code'],
                 flipped, leg['amount'],
                 'Reversal: ' + (leg['description'] or ''),
                 SOURCE_OPENING_REVERSAL),
            )
        conn.execute(
            "UPDATE opening_balance_entries SET status=?, reversed_at=?, "
            "reversed_by=? WHERE entry_id=?",
            ('reversed', now, reversed_by, entry_id),
        )
        conn.execute(
            "UPDATE gl_transactions SET reversed_by=? WHERE entry_id=?",
            (reversal_entry_id, entry_id),
        )
        conn.commit()
    return {'ok': True, 'entry_id': entry_id,
            'reversal_entry_id': reversal_entry_id,
            'rows_reversed': len(legs)}


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


def render_opening_balances_page(
    *, firm_code: str, client_code: str, client_name: str,
    state: dict[str, Any] | None = None,
    preview: dict[str, Any] | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    import html as _html
    def _esc(s: Any) -> str:
        return _html.escape(str(s or ""))
    flash_html = ''
    if flash:
        flash_html += (
            f'<div style="background:#d4edda;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash)}</div>'
        )
    if flash_error:
        flash_html += (
            f'<div style="background:#f8d7da;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash_error)}</div>'
        )

    if state and state.get('status') == 'locked':
        # Already posted — show the summary, offer reversal.
        return (
            '<!DOCTYPE html><html><head><meta charset="utf-8">'
            f'<title>Opening balances — {_esc(client_name)}</title></head>'
            f'<body><h1>{_esc(client_name)} — Opening balances</h1>'
            f'{flash_html}'
            f'<p>Entry id: <code>{_esc(state["entry_id"])}</code></p>'
            f'<p>As-of: {_esc(state["as_of_date"])} &middot; '
            f'posted {_esc(state.get("posted_at"))}</p>'
            f'<p>Status: <strong>{_esc(state["status"])}</strong></p>'
            f'<form method="POST" '
            f'action="/clients/opening_balances/reverse" '
            'style="margin-top:16px;" '
            'onsubmit="return confirm(\'Reverse opening balances? A compensating JE is posted; the original is preserved.\');">'
            f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
            '<button type="submit" style="background:#dc2626;color:white;">'
            'Reverse opening balances</button></form>'
            '</body></html>'
        )

    preview_html = ''
    if preview is not None:
        v = preview
        tone = '#166534' if v.get('ok') else '#b91c1c'
        preview_html = (
            f'<div class="card" style="border-color:{tone};">'
            f'<p>Debits: <strong>${v.get("debits")}</strong> / '
            f'Credits: <strong>${v.get("credits")}</strong></p>'
            f'<p>Diff: <strong style="color:{tone};">'
            f'${v.get("diff")}</strong></p>'
        )
        for e in v.get('errors') or []:
            preview_html += f'<div style="color:#b91c1c;">{_esc(e)}</div>'
        preview_html += '</div>'

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<title>Opening balances — {_esc(client_name)}</title>'
        '<style>body{font-family:system-ui,Arial;max-width:1000px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;margin:1rem 0;}'
        'th,td{border-bottom:1px solid #eee;padding:8px;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '</style></head><body>'
        f'<h1>{_esc(client_name)} — Opening balances / Soldes d\'ouverture</h1>'
        f'{flash_html}'
        '<form method="POST" action="/clients/opening_balances" '
        'enctype="multipart/form-data">'
        f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
        '<label>As-of date / Date d\'adoption: '
        '<input type="date" name="as_of_date" required></label>'
        '<label>Description: '
        '<input type="text" name="description" '
        'placeholder="Opening balances migration"></label>'
        '<p class="muted">Enter one line per account; side is debit or credit.</p>'
        '<table id="ob-rows"><thead><tr>'
        '<th>Account code</th><th>Side</th><th>Amount</th>'
        '</tr></thead><tbody>'
        + ''.join([
            '<tr>'
            f'<td><input name="account_code_{i}" placeholder="1000"></td>'
            f'<td><select name="side_{i}"><option>debit</option>'
            '<option>credit</option></select></td>'
            f'<td><input name="amount_{i}" type="number" step="0.01"></td>'
            '</tr>' for i in range(10)
        ])
        + '</tbody></table>'
        '<label>Supporting documents: '
        '<input type="file" name="supporting_docs" multiple></label>'
        '<button type="submit">Validate / Post</button>'
        '</form>'
        f'{preview_html}'
        '</body></html>'
    )
