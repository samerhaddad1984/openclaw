"""Queued notification delivery.

`client_notifications` rows are created by review/portal/messaging flows;
`send_pending_notifications()` is the single worker that actually dispatches
them (email via `email_client.send_email`; WhatsApp via `whatsapp.send_text`)
and writes the outcome back to the row. Designed to be safe under 5-minute
cron polling (only rows whose `status='pending'` and `send_at<=now` are
claimed; successful sends flip to `sent`, failures increment `retry_count`
and bounce back to `pending` up to 3 times before `failed`).

The extra columns on `client_notifications` (status, channel, recipient_*,
subject, priority, send_at, retry_count, sent_at, last_error) are added by
`ensure_sender_schema` — it's idempotent so cron invocations stay cheap.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


MAX_RETRIES = 3
DEFAULT_BATCH = 50


def ensure_sender_schema(db_path: Path | str) -> None:
    """Add columns the sender needs; preserve existing rows.

    Safe to call every cron run — columns only get added once.
    """
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT NOT NULL,
                kind TEXT NOT NULL,
                title TEXT,
                body TEXT,
                document_id TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                read_at TEXT
            )
        """)
        have = _columns(conn, 'client_notifications')
        wanted = {
            'status':           "TEXT DEFAULT 'pending'",
            'channel':          "TEXT DEFAULT 'email'",
            'recipient_email':  'TEXT',
            'recipient_phone':  'TEXT',
            'subject':          'TEXT',
            'priority':         'INTEGER DEFAULT 5',
            'send_at':          'TEXT',
            'sent_at':          'TEXT',
            'retry_count':      'INTEGER DEFAULT 0',
            'last_error':       'TEXT',
        }
        for col, ddl in wanted.items():
            if col not in have:
                conn.execute(
                    f"ALTER TABLE client_notifications ADD COLUMN {col} {ddl}"
                )
        # Some legacy rows have status IS NULL — normalise.
        conn.execute(
            "UPDATE client_notifications SET status='pending' "
            "WHERE status IS NULL OR status=''"
        )
        conn.execute(
            "UPDATE client_notifications SET channel='email' "
            "WHERE channel IS NULL OR channel=''"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_notifications_sendable "
            "ON client_notifications(status, send_at)"
        )
        conn.commit()


def enqueue(
    db_path: Path | str, *,
    client_code: str,
    kind: str,
    title: str,
    body: str = '',
    channel: str = 'email',
    recipient_email: str | None = None,
    recipient_phone: str | None = None,
    subject: str | None = None,
    priority: int = 5,
    send_at: str | None = None,
    document_id: str | None = None,
) -> int:
    """Insert a pending notification. Idempotent only on its caller."""
    ensure_sender_schema(db_path)
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO client_notifications "
            "(client_code, kind, title, body, document_id, status, channel, "
            " recipient_email, recipient_phone, subject, priority, send_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (client_code, kind, title, body, document_id, 'pending', channel,
             recipient_email, recipient_phone, subject or title,
             priority, send_at or _iso_now()),
        )
        conn.commit()
        return int(cur.lastrowid)


def _mark_sent(conn: sqlite3.Connection, notif_id: int) -> None:
    conn.execute(
        "UPDATE client_notifications SET status='sent', sent_at=?, "
        "last_error=NULL WHERE id=?",
        (_iso_now(), notif_id),
    )


def _mark_failed(conn: sqlite3.Connection, notif_id: int, err: str,
                   *, requeue: bool) -> None:
    if requeue:
        conn.execute(
            "UPDATE client_notifications SET "
            "retry_count=retry_count+1, last_error=?, "
            "status='pending', send_at=? "
            "WHERE id=?",
            (err[:500], _iso_now(), notif_id),
        )
    else:
        conn.execute(
            "UPDATE client_notifications SET status='failed', "
            "last_error=?, retry_count=retry_count+1 WHERE id=?",
            (err[:500], notif_id),
        )


def send_pending_notifications(
    db_path: Path | str,
    *,
    limit: int = DEFAULT_BATCH,
    email_fn: Optional[Callable[[str, str, str], bool]] = None,
    whatsapp_fn: Optional[Callable[[str, str], bool]] = None,
    now_iso: str | None = None,
) -> dict[str, int]:
    """Drain up to `limit` queued notifications. Returns delivery tally.

    email_fn / whatsapp_fn are injected for testability — production uses
    `email_client.send_email` and `whatsapp.send_text`."""
    ensure_sender_schema(db_path)
    now = now_iso or _iso_now()
    sent = failed = skipped = requeued = 0
    with _open(db_path) as conn:
        pending = conn.execute(
            "SELECT * FROM client_notifications "
            "WHERE status='pending' "
            "  AND (send_at IS NULL OR send_at <= ?) "
            "ORDER BY priority DESC, created_at ASC "
            "LIMIT ?",
            (now, limit),
        ).fetchall()
        # Claim each row as 'sending' before dispatch so a concurrent cron
        # tick can't double-send.
        claimed: list[sqlite3.Row] = []
        for r in pending:
            upd = conn.execute(
                "UPDATE client_notifications SET status='sending' "
                "WHERE id=? AND status='pending'",
                (r['id'],),
            )
            if upd.rowcount:
                claimed.append(r)
        conn.commit()

    # Dispatch outside the claim transaction; each update is its own tx.
    for r in claimed:
        channel = (r['channel'] or 'email').lower()
        try:
            if channel == 'email':
                to = r['recipient_email']
                if not to:
                    raise RuntimeError('missing recipient_email')
                fn = email_fn or _default_email_fn()
                ok = bool(fn(to, r['subject'] or r['title'] or '(no subject)',
                             r['body'] or ''))
                if not ok:
                    raise RuntimeError('email_fn returned False')
            elif channel == 'whatsapp':
                to = r['recipient_phone']
                if not to:
                    raise RuntimeError('missing recipient_phone')
                fn = whatsapp_fn or _default_whatsapp_fn()
                ok = bool(fn(to, r['body'] or r['title'] or ''))
                if not ok:
                    raise RuntimeError('whatsapp_fn returned False')
            else:
                raise RuntimeError(f'unknown channel {channel!r}')
            with _open(db_path) as conn:
                _mark_sent(conn, r['id'])
                conn.commit()
            sent += 1
        except Exception as exc:
            err = f'{type(exc).__name__}: {exc}'
            requeue = (r['retry_count'] or 0) + 1 < MAX_RETRIES
            with _open(db_path) as conn:
                _mark_failed(conn, r['id'], err, requeue=requeue)
                conn.commit()
            if requeue:
                requeued += 1
            else:
                failed += 1
            log.warning('notification %s failed (%s): %s',
                        r['id'], 'requeue' if requeue else 'give-up', err)

    skipped = len(pending) - len(claimed)
    return {'sent': sent, 'failed': failed, 'requeued': requeued,
             'skipped': skipped, 'claimed': len(claimed)}


def _default_email_fn() -> Callable[[str, str, str], bool]:
    def _send(to: str, subject: str, body: str) -> bool:
        from src.integrations.email_client import send_email
        return bool(send_email(to, subject, body))
    return _send


def _default_whatsapp_fn() -> Callable[[str, str], bool]:
    def _send(to: str, body: str) -> bool:
        try:
            from src.integrations.whatsapp import send_text  # type: ignore
        except Exception:
            return False
        try:
            return bool(send_text(to, body))
        except Exception:
            return False
    return _send


# ---------------------------------------------------------------------------
# Convenience wrappers used by the route handlers
# ---------------------------------------------------------------------------


def notify_receipt_approved(
    db_path: Path | str, *, client_code: str,
    recipient_email: str, doc_count: int = 1, document_id: str | None = None,
) -> int:
    title = f'{doc_count} receipt(s) approved and recorded'
    body = (
        f'Your CPA has approved {doc_count} receipt(s). They are now posted '
        'to your books. Log in to the portal to see details.'
    )
    return enqueue(
        db_path, client_code=client_code, kind='receipt_approved',
        title=title, body=body, channel='email',
        recipient_email=recipient_email, subject=title,
        priority=5, document_id=document_id,
    )


def notify_cpa_question(
    db_path: Path | str, *, client_code: str,
    recipient_email: str, subject: str, body: str,
    document_id: str | None = None,
) -> int:
    return enqueue(
        db_path, client_code=client_code, kind='cpa_question',
        title=subject, body=body, channel='email',
        recipient_email=recipient_email, subject=subject,
        priority=6, document_id=document_id,
    )


def notify_review_assigned(
    db_path: Path | str, *, firm_code: str, assignee_email: str,
    entity_type: str, entity_id: str, priority: str = 'normal',
) -> int:
    title = f'{entity_type} {entity_id} assigned to you'
    body = (
        f'A {entity_type} ({entity_id}) was assigned to you with '
        f'priority={priority}. Open /my_tasks to review.'
    )
    return enqueue(
        db_path, client_code=firm_code, kind='review_assigned',
        title=title, body=body, channel='email',
        recipient_email=assignee_email, subject=title,
        priority=7 if priority == 'urgent' else 5,
        document_id=entity_id,
    )


def enqueue_single_notification(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    recipient_email: str,
    subject: str,
    body: str,
    kind: str = 'single',
    channel: str = 'email',
    recipient_phone: str | None = None,
    recipient_name: str | None = None,
    priority: int = 5,
    send_at: str | None = None,
    document_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    """Canonical single-recipient enqueue with metadata.

    Layers under `enqueue`; accepts a `metadata` dict that gets JSON-
    serialised into the title when non-empty so downstream can group
    related notifications (e.g. batch_id from a fanout). Keeping the
    payload in ``title`` avoids a schema migration."""
    md = ''
    if metadata:
        import json as _json
        md = f' [meta={_json.dumps(metadata, separators=(",", ":"), default=str)}]'
    display_title = f'{subject}{md}'
    return enqueue(
        db_path, client_code=client_code, kind=kind,
        title=display_title, body=body, channel=channel,
        recipient_email=recipient_email,
        recipient_phone=recipient_phone,
        subject=subject, priority=priority,
        send_at=send_at, document_id=document_id,
    )


def enqueue_notification_to_group(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    group_type: str,
    subject: str,
    body: str,
    kind: str = 'group',
    priority: int = 5,
    document_id: str | None = None,
    target_user_id: int | None = None,
) -> dict[str, Any]:
    """Fan out one notification to every recipient in a named group.

    ``group_type`` is one of:

    - ``'all_admins'``         — every active portal user with role='admin'
    - ``'all_contributors'``   — every active portal user with role='contributor'
    - ``'all_portal_users'``   — every active portal user
    - ``'specific_user'``      — single user identified by ``target_user_id``

    Returns ``{'fanout_count': N, 'batch_id': 'b_xxx',
    'recipients': [emails], 'group_type': ...}``. A warning is logged
    when the resolved group is empty (the caller may still want to
    know that nothing was dispatched, e.g. to surface "no admins
    configured" in the UI)."""
    import secrets as _secrets
    from src.integrations import multi_user_portal as _mup

    batch_id = f'b_{_secrets.token_hex(8)}'

    if group_type == 'specific_user':
        if target_user_id is None:
            raise ValueError(
                "group_type='specific_user' requires target_user_id"
            )
        user = _mup.get_user(db_path, user_id=target_user_id)
        if user is None or user.get('status') != 'active':
            log.warning(
                'fanout(specific_user=%s): user not active; skipping',
                target_user_id,
            )
            return {'fanout_count': 0, 'batch_id': batch_id,
                     'recipients': [], 'group_type': group_type}
        recipients = [user]
    elif group_type in ('all_admins', 'all_contributors', 'all_portal_users'):
        users = _mup.list_users(
            db_path, firm_code=firm_code, client_code=client_code,
        )
        # list_users filters out 'removed'; apply status + role filters.
        if group_type == 'all_admins':
            recipients = [u for u in users
                           if u.get('status') == 'active'
                           and u.get('role') == 'admin']
        elif group_type == 'all_contributors':
            recipients = [u for u in users
                           if u.get('status') == 'active'
                           and u.get('role') == 'contributor']
        else:  # all_portal_users
            recipients = [u for u in users if u.get('status') == 'active']
    else:
        raise ValueError(f'unknown group_type: {group_type!r}')

    if not recipients:
        log.warning(
            'fanout(%s) resolved to zero recipients for %s/%s',
            group_type, firm_code, client_code,
        )
        return {'fanout_count': 0, 'batch_id': batch_id,
                 'recipients': [], 'group_type': group_type}

    emails: list[str] = []
    for r in recipients:
        email = r.get('email')
        if not email:
            continue
        # Personalise body with recipient name when present.
        body_for_r = body
        name = r.get('full_name') or ''
        if name and '{name}' in body_for_r:
            body_for_r = body_for_r.replace('{name}', name)
        enqueue_single_notification(
            db_path, firm_code=firm_code, client_code=client_code,
            recipient_email=email, recipient_name=name,
            subject=subject, body=body_for_r,
            kind=kind, priority=priority,
            document_id=document_id,
            metadata={'group_type': group_type,
                        'batch_id': batch_id,
                        'portal_user_id': r.get('id')},
        )
        emails.append(email)
    return {'fanout_count': len(emails), 'batch_id': batch_id,
             'recipients': emails, 'group_type': group_type}


def notify_feedback_submitted(
    db_path: Path | str, *, feedback_id: int, owner_email: str,
    firm_code: str, subject: str, body: str,
) -> int:
    title = f'[Feedback #{feedback_id}] {subject}'
    return enqueue(
        db_path, client_code=firm_code, kind='feedback_submitted',
        title=title, body=body, channel='email',
        recipient_email=owner_email, subject=title,
        priority=6,
    )
