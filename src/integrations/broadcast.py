"""Cross-firm broadcast primitive — Sam (owner role) can announce to
every firm or every firm-owner, with FR/EN per-user selection.

Uses the fan-out primitive from `notification_sender` so every
broadcast lands in the same `client_notifications` queue the 5-min
cron drains. No new delivery code; the fan-out just resolves the
recipient list via the dashboard_users + firms tables.

Broadcast types (audience strings):

- ``all_firm_owners``  — users with role='owner' in dashboard_users,
                           skipping cancelled firms
- ``all_firm_admins``  — role='firm_admin' in active firms
- ``all_users``         — every active dashboard_users row
- ``specific_firms``   — filtered by a firm_code list (``firm_codes=``)
- ``plan_tier``         — filtered by firms.plan LIKE ``tier%`` (e.g.
                            'pro' -> every pro_monthly / pro_yearly row)
"""
from __future__ import annotations

import logging
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


log = logging.getLogger(__name__)


BROADCAST_AUDIENCES = (
    'all_firm_owners', 'all_firm_admins', 'all_users',
    'specific_firms', 'plan_tier',
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_broadcast_schema(db_path: Path | str) -> None:
    """Idempotent audit table for every broadcast Sam sends."""
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_firm_broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT UNIQUE NOT NULL,
                audience TEXT NOT NULL,
                audience_filter TEXT,
                subject_en TEXT, subject_fr TEXT,
                body_en TEXT, body_fr TEXT,
                from_user TEXT NOT NULL,
                scheduled_for TEXT,
                recipient_count INTEGER,
                sent_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


def resolve_recipients(
    db_path: Path | str, *,
    audience: str,
    firm_codes: Iterable[str] | None = None,
    plan_tier: str | None = None,
) -> list[dict[str, Any]]:
    """Return the list of dashboard_users rows this broadcast targets.

    Each row has at least: username (email), display_name, language,
    firm_code. Missing `language` defaults to the user's row default
    'fr' (per the existing schema)."""
    if audience not in BROADCAST_AUDIENCES:
        raise ValueError(f'unknown audience {audience!r}')

    with _open(db_path) as conn:
        # Base join + firm-active filter. Only include firms whose
        # subscription_status isn't 'cancelled' so a cancelled firm's
        # ex-owner doesn't get announcements.
        where_parts = ["COALESCE(u.active,1)=1",
                        "COALESCE(f.subscription_status,'active') != 'cancelled'"]
        params: list[Any] = []
        if audience == 'all_firm_owners':
            where_parts.append("u.role='owner'")
        elif audience == 'all_firm_admins':
            where_parts.append("u.role='firm_admin'")
        elif audience == 'specific_firms':
            codes = [c for c in (firm_codes or []) if c]
            if not codes:
                return []
            placeholders = ','.join('?' for _ in codes)
            where_parts.append(f"u.firm_code IN ({placeholders})")
            params.extend(codes)
        elif audience == 'plan_tier':
            if not plan_tier:
                raise ValueError("audience=plan_tier requires plan_tier=")
            where_parts.append("LOWER(COALESCE(f.plan,'')) LIKE ?")
            params.append(f'{plan_tier.lower()}%')
        where_sql = ' AND '.join(where_parts)
        try:
            rows = conn.execute(
                f"SELECT u.username, "
                f"       COALESCE(u.display_name, u.username) AS display_name, "
                f"       COALESCE(NULLIF(u.language,''),'fr') AS language, "
                f"       u.firm_code "
                f"FROM dashboard_users u "
                f"LEFT JOIN firms f ON f.firm_code = u.firm_code "
                f"WHERE {where_sql}",
                params,
            ).fetchall()
        except sqlite3.OperationalError as exc:
            log.warning('broadcast recipient query failed: %s', exc)
            return []
    return [dict(r) for r in rows]


def preview_recipient_count(
    db_path: Path | str, *,
    audience: str,
    firm_codes: Iterable[str] | None = None,
    plan_tier: str | None = None,
) -> dict[str, Any]:
    """Return a dict with count + a short sample of who'd receive.

    The caller (owner UI) shows the count before Send so Sam can
    confirm the blast radius."""
    recipients = resolve_recipients(
        db_path, audience=audience,
        firm_codes=firm_codes, plan_tier=plan_tier,
    )
    sample = [
        {'email': r['username'],
         'name': r['display_name'], 'firm': r['firm_code']}
        for r in recipients[:5]
    ]
    return {
        'count': len(recipients),
        'sample': sample,
        'audience': audience,
    }


def broadcast(
    db_path: Path | str, *,
    audience: str,
    subject_en: str, subject_fr: str,
    body_en: str, body_fr: str,
    from_user: str,
    firm_codes: Iterable[str] | None = None,
    plan_tier: str | None = None,
    scheduled_for: str | None = None,
) -> dict[str, Any]:
    """Fan out a broadcast to every matching recipient in their
    preferred language. Returns {batch_id, recipient_count,
    recipients: [emails]}."""
    if audience not in BROADCAST_AUDIENCES:
        raise ValueError(f'unknown audience {audience!r}')
    if not subject_en.strip() or not subject_fr.strip():
        raise ValueError('both subject_en and subject_fr are required')
    if not body_en.strip() or not body_fr.strip():
        raise ValueError('both body_en and body_fr are required')
    if audience == 'specific_firms':
        firm_codes = list(firm_codes or [])
        if not firm_codes:
            raise ValueError('audience=specific_firms requires firm_codes')
    if audience == 'plan_tier' and not plan_tier:
        raise ValueError('audience=plan_tier requires plan_tier')

    _ensure_broadcast_schema(db_path)

    batch_id = f'bc_{secrets.token_hex(8)}'
    recipients = resolve_recipients(
        db_path, audience=audience,
        firm_codes=firm_codes, plan_tier=plan_tier,
    )

    # Per-recipient language-picked enqueue.
    from src.integrations.notification_sender import (
        enqueue_single_notification,
    )
    delivered: list[str] = []
    audience_filter_str = ''
    if audience == 'specific_firms':
        audience_filter_str = ','.join(firm_codes or [])
    elif audience == 'plan_tier':
        audience_filter_str = plan_tier or ''
    for r in recipients:
        lang = (r.get('language') or 'fr').lower()
        subject = subject_fr if lang == 'fr' else subject_en
        body = body_fr if lang == 'fr' else body_en
        # Personalise {name} placeholder per recipient.
        name = r.get('display_name') or r['username']
        body_r = body.replace('{name}', name)
        enqueue_single_notification(
            db_path,
            firm_code=r.get('firm_code') or '',
            client_code='',
            recipient_email=r['username'],
            recipient_name=name,
            subject=subject, body=body_r,
            kind='cross_firm_broadcast',
            priority=4,
            send_at=scheduled_for or _iso_now(),
            metadata={
                'type': 'cross_firm_broadcast',
                'batch_id': batch_id,
                'audience': audience,
                'from_user': from_user,
                'lang': lang,
            },
        )
        delivered.append(r['username'])

    # Audit the broadcast itself so we can diff future sends and so
    # Sam can see prior broadcasts in the UI.
    try:
        with _open(db_path) as conn:
            conn.execute(
                "INSERT INTO cross_firm_broadcasts "
                "(batch_id, audience, audience_filter, "
                " subject_en, subject_fr, body_en, body_fr, "
                " from_user, scheduled_for, recipient_count, sent_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?, ?)",
                (batch_id, audience, audience_filter_str,
                 subject_en, subject_fr, body_en, body_fr,
                 from_user, scheduled_for,
                 len(delivered), _iso_now()),
            )
            conn.commit()
    except sqlite3.OperationalError as exc:
        log.warning('broadcast audit insert failed: %s', exc)

    return {
        'batch_id': batch_id,
        'recipient_count': len(delivered),
        'recipients': delivered,
        'audience': audience,
    }


def recent_broadcasts(
    db_path: Path | str, *, limit: int = 10,
) -> list[dict[str, Any]]:
    """Return the N most-recent broadcasts for the admin UI."""
    _ensure_broadcast_schema(db_path)
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM cross_firm_broadcasts "
            "ORDER BY sent_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Render helpers (simple; the owner broadcast page is sparse by design)
# ---------------------------------------------------------------------------


import html as _html


def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else ''))


def render_broadcast_page(
    db_path: Path | str, *,
    firm_codes_available: list[dict[str, Any]] | None = None,
    preview: dict[str, Any] | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    """Compose + preview + send form for /owner/broadcast."""
    firm_options = ''
    for f in (firm_codes_available or []):
        firm_options += (
            f'<option value="{_esc(f["firm_code"])}">'
            f'{_esc(f["firm_code"])} — {_esc(f.get("name") or "")}</option>'
        )
    audience_options = ''.join(
        f'<option value="{a}">{a.replace("_", " ")}</option>'
        for a in BROADCAST_AUDIENCES
    )
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'

    preview_html = ''
    if preview:
        sample_rows = ''.join(
            f'<li>{_esc(s["email"])} ({_esc(s["name"])}) — firm {_esc(s["firm"])}</li>'
            for s in preview.get('sample') or []
        )
        preview_html = (
            '<div class="card" style="background:#eef2ff;'
            'border:1px solid #c7d2fe;padding:12px;margin:1rem 0;">'
            f'<strong>Preview:</strong> would send to '
            f'<strong>{preview["count"]}</strong> recipients '
            f'({_esc(preview["audience"])}).'
            f'<ul>{sample_rows}</ul>'
            '</div>'
        )

    history = recent_broadcasts(db_path, limit=10)
    history_rows = ''
    for b in history:
        history_rows += (
            f'<tr><td>{_esc(b["sent_at"])}</td>'
            f'<td>{_esc(b["audience"])}</td>'
            f'<td>{_esc(b.get("audience_filter") or "")}</td>'
            f'<td>{_esc(b.get("subject_en") or b.get("subject_fr") or "")}</td>'
            f'<td>{b.get("recipient_count") or 0}</td></tr>'
        )
    history_html = (
        '<h3 style="margin-top:2rem;">Recent broadcasts</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Sent</th><th>Audience</th>'
        '<th>Filter</th><th>Subject</th><th>#</th></tr></thead>'
        f'<tbody>{history_rows or "<tr><td colspan=5>None yet.</td></tr>"}</tbody>'
        '</table>'
    )

    return (
        '<div class="card" style="max-width:900px;margin:1rem auto;">'
        '<h2>Broadcast to firms</h2>'
        f'{flash_html}{preview_html}'
        '<form method="POST" action="/owner/broadcast" '
        'style="display:grid;gap:10px;">'
        '<label>Audience'
        '<select name="audience" required>'
        f'{audience_options}</select></label>'
        '<label>Specific firms (for audience=specific_firms)'
        '<select name="firm_codes" multiple size="5" '
        'style="min-width:280px;">'
        f'{firm_options}</select></label>'
        '<label>Plan tier (for audience=plan_tier, e.g. "pro" or "starter")'
        '<input type="text" name="plan_tier" placeholder="pro"></label>'
        '<label>Subject (EN)'
        '<input type="text" name="subject_en" required></label>'
        '<label>Subject (FR)'
        '<input type="text" name="subject_fr" required></label>'
        '<label>Body (EN)'
        '<textarea name="body_en" rows="4" required></textarea></label>'
        '<label>Body (FR)'
        '<textarea name="body_fr" rows="4" required></textarea></label>'
        '<label>Schedule for (leave blank to send now)'
        '<input type="text" name="scheduled_for" '
        'placeholder="YYYY-MM-DDTHH:MM:SS+00:00"></label>'
        '<div style="display:flex;gap:10px;">'
        '<button type="submit" name="action" value="preview" '
        'style="background:#6b7280;color:white;padding:10px 20px;">'
        'Preview recipient count</button>'
        '<button type="submit" name="action" value="send" '
        'style="background:#1e40af;color:white;padding:10px 20px;" '
        'onclick="return confirm(\'Send broadcast now?\');">'
        'Send broadcast</button>'
        '</div></form>'
        f'{history_html}'
        '</div>'
    )
