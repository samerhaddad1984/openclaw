"""Queue filter by uploader (Item 1 of the polish pass).

The CPA queue ( `/` / `/queue` ) lets the operator filter documents
by who uploaded them. This module owns the pure helpers so tests
can exercise them without a live dashboard:

- uploader_filter_options()    → per-uploader counts + display name
- render_uploader_filter_dropdown() → HTML <select> with counts
- render_uploader_badge()      → small coloured chip per row
- parse_uploader_filter_qs()   → URL query-string → list of emails

Badge colour is deterministic from the email hash so a given uploader
always gets the same chip colour even across sessions.
"""
from __future__ import annotations

import hashlib
import html as _html
import sqlite3
import urllib.parse
from pathlib import Path
from typing import Any


# Stable "anonymous" key used for documents uploaded without a
# portal_user (the legacy /c/{token}/upload anonymous flow). Kept as
# a literal string rather than NULL so it serialises cleanly in the
# URL.
ANON_KEY = '__anonymous__'


def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else ''))


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def uploader_filter_options(
    db_path: Path | str, *,
    firm_code: str | None = None,
    client_code: str | None = None,
) -> list[dict[str, Any]]:
    """Return `[{'uploader_key', 'display_name', 'count'}]` one row per
    distinct uploader in the (optionally scoped) documents table.

    `uploader_key` is either the email (active identity) or
    ``ANON_KEY`` for documents with no uploader_email. Counts include
    every document in scope; callers that want to restrict by period
    should pass the scoped `client_code` and add their own date
    filter before/after (this module stays generic)."""
    with _open(db_path) as conn:
        where_parts: list[str] = []
        params: list[Any] = []
        if firm_code and firm_code != 'OWNER':
            where_parts.append(
                "client_code IN (SELECT client_code FROM clients WHERE firm_code=?)"
            )
            params.append(firm_code)
        if client_code:
            where_parts.append("client_code=?")
            params.append(client_code)
        where_sql = ('WHERE ' + ' AND '.join(where_parts)
                     ) if where_parts else ''
        sql = f"""
            SELECT COALESCE(NULLIF(uploader_email,''), '{ANON_KEY}') AS uploader_key,
                   COALESCE(NULLIF(uploader_name,''), uploader_email,
                             '{ANON_KEY}') AS display_name,
                   COUNT(*) AS cnt
            FROM documents
            {where_sql}
            GROUP BY uploader_key
            ORDER BY cnt DESC, uploader_key ASC
        """
        try:
            rows = conn.execute(sql, tuple(params)).fetchall()
        except sqlite3.OperationalError:
            # Pre-migration DBs where uploader_email column is missing.
            return []
    out: list[dict[str, Any]] = []
    for r in rows:
        key = r['uploader_key']
        label = r['display_name']
        if key == ANON_KEY:
            label = 'Anonymous'
        out.append({
            'uploader_key': key,
            'display_name': label,
            'count': int(r['cnt'] or 0),
        })
    return out


def parse_uploader_filter_qs(raw: str | None) -> list[str]:
    """Parse ?uploader=a@b.com,c@d.com into ['a@b.com','c@d.com'].

    An empty / missing value returns ``[]`` (meaning 'show all').
    Entries are trimmed and URL-decoded; duplicates preserved in
    input order."""
    if not raw:
        return []
    parts = [urllib.parse.unquote(p).strip()
             for p in raw.split(',')
             if p.strip()]
    return parts


def build_uploader_where_fragment(
    uploader_emails: list[str] | None,
) -> tuple[str, list[Any]]:
    """Build an SQL fragment + params for `d.uploader_email IN (...)`
    with the special `__anonymous__` sentinel meaning `IS NULL OR ''`.

    Returns ``('', [])`` when no filter is applied so the caller can
    just concatenate the fragment onto an existing WHERE chain."""
    if not uploader_emails:
        return '', []
    selected = set(uploader_emails)
    include_anon = ANON_KEY in selected
    concrete = [e for e in uploader_emails if e != ANON_KEY]
    clauses: list[str] = []
    params: list[Any] = []
    if include_anon:
        clauses.append(
            "(d.uploader_email IS NULL OR d.uploader_email='')"
        )
    if concrete:
        placeholders = ','.join('?' for _ in concrete)
        clauses.append(
            f"(LOWER(d.uploader_email) IN ({placeholders}))"
        )
        params.extend(e.lower() for e in concrete)
    if not clauses:
        return '', []
    return '(' + ' OR '.join(clauses) + ')', params


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------


def _badge_color(key: str) -> str:
    """Deterministic chip colour for an uploader key.

    Hashes the key into one of nine well-contrasting palette colours so
    the same uploader always gets the same chip colour. Anonymous is
    always grey so it's immediately distinguishable."""
    if key == ANON_KEY or not key:
        return '#9ca3af'  # grey
    palette = [
        '#1e40af', '#059669', '#b45309', '#7c3aed',
        '#be185d', '#0891b2', '#c2410c', '#4d7c0f', '#9333ea',
    ]
    h = hashlib.sha1(key.encode('utf-8', 'ignore')).digest()
    return palette[h[0] % len(palette)]


def render_uploader_badge(
    uploader_name: str | None,
    uploader_email: str | None = None,
) -> str:
    """Small coloured chip shown next to a queue row.

    Returns empty string for legacy anonymous uploads — callers should
    render the 'no uploader' cell themselves if they want an Anonymous
    chip (pass 'Anonymous' as `uploader_name`)."""
    name = (uploader_name or '').strip()
    email = (uploader_email or '').strip()
    if not name and not email:
        return ''
    key = email or name
    color = _badge_color(key)
    display = name or email
    return (
        f'<span class="uploader-badge" '
        f'style="display:inline-block;padding:2px 8px;border-radius:10px;'
        f'background:{color};color:white;font-size:11px;font-weight:600;'
        f'white-space:nowrap;" '
        f'title="{_esc(email or name)}">{_esc(display)}</span>'
    )


def render_uploader_filter_dropdown(
    options: list[dict[str, Any]], *,
    selected_keys: list[str] | None = None,
    form_action: str = '/',
    preserve_params: dict[str, str] | None = None,
) -> str:
    """Return a ``<form>`` with a multi-select dropdown + submit + clear.

    Multi-select (size=5) so the CPA can pick two or more uploaders
    at once; selection list pre-populates with ``selected_keys``.
    ``preserve_params`` carries over the other queue filters (status,
    q, queue_mode, page) so applying the uploader filter doesn't
    reset them."""
    selected = set(selected_keys or [])
    if not options:
        return ''
    option_html = []
    for o in options:
        key = o['uploader_key']
        label = f'{o["display_name"]} ({o["count"]})'
        sel = ' selected' if key in selected else ''
        option_html.append(
            f'<option value="{_esc(key)}"{sel}>{_esc(label)}</option>'
        )
    hidden_inputs = ''
    if preserve_params:
        for name, val in preserve_params.items():
            if name in ('uploader',):
                continue
            if val is None or val == '':
                continue
            hidden_inputs += (
                f'<input type="hidden" name="{_esc(name)}" '
                f'value="{_esc(val)}">'
            )
    return (
        '<form method="GET" '
        f'action="{_esc(form_action)}" '
        'class="uploader-filter" '
        'style="display:flex;gap:8px;align-items:center;margin:8px 0;">'
        f'{hidden_inputs}'
        '<label style="color:#6b7280;font-size:13px;">'
        'Filter by uploader'
        '</label>'
        '<select name="uploader" multiple size="5" '
        'style="min-width:220px;padding:4px;">'
        + ''.join(option_html)
        + '</select>'
        '<button type="submit" '
        'style="background:#1e40af;color:white;border:none;'
        'padding:6px 12px;border-radius:4px;">Apply</button>'
        f'<a href="{_esc(form_action)}?uploader=" '
        'style="color:#6b7280;">Clear</a>'
        '</form>'
    )
