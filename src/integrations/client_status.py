"""Client-portal status dashboard: upload breakdown + recent activity
+ YTD summary + simple threaded messaging.

Consumed by the ``/c/{portal_token}/status`` route. Every helper is a
pure DB aggregator scoped to a single client_code so portal-token
auth is sufficient.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _iso_days_ago(d: int) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(days=d)).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def ensure_client_status_schema(db_path: Path | str) -> None:
    """Shared with bootstrap: adds client_messages + notification queue."""
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_notifications_client "
            "ON client_notifications(client_code, read_at)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_message_threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT,
                client_code TEXT,
                subject TEXT,
                document_id TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_message_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                sender_type TEXT NOT NULL,
                sender_id TEXT,
                body TEXT,
                attachment_url TEXT,
                read_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cm_posts_thread "
            "ON client_message_posts(thread_id, created_at)"
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Upload status breakdown
# ---------------------------------------------------------------------------


def upload_status(
    db_path: Path | str, *,
    client_code: str,
    period: str | None = None,
) -> dict[str, Any]:
    """Counts by review_status bucket + this-month tally.

    Buckets:
    - reviewed:  review_status in ('Posted', 'Approved')
    - processing: review_status in ('Processing', 'New')
    - needs_attention: review_status in ('NeedsReview', 'Rejected')
    """
    with _open(db_path) as conn:
        if not _table_exists(conn, 'documents'):
            return {'total': 0, 'reviewed': 0, 'processing': 0,
                     'needs_attention': 0, 'this_month': 0}
        month = period or _iso_now()[:7]
        row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN LOWER(COALESCE(review_status,''))
                        IN ('posted','approved') THEN 1 ELSE 0 END) AS reviewed,
              SUM(CASE WHEN LOWER(COALESCE(review_status,''))
                        IN ('processing','new') THEN 1 ELSE 0 END) AS processing,
              SUM(CASE WHEN LOWER(COALESCE(review_status,''))
                        IN ('needsreview','rejected') THEN 1 ELSE 0 END) AS needs_attention,
              SUM(CASE WHEN COALESCE(document_date,'') LIKE ?
                        THEN 1 ELSE 0 END) AS this_month
            FROM documents WHERE client_code=?
            """,
            (f"{month}%", client_code),
        ).fetchone()
    return {
        'total':            int(row['total'] or 0),
        'reviewed':         int(row['reviewed'] or 0),
        'processing':       int(row['processing'] or 0),
        'needs_attention':  int(row['needs_attention'] or 0),
        'this_month':       int(row['this_month'] or 0),
    }


# ---------------------------------------------------------------------------
# Recent activity feed (event stream rendered for the portal)
# ---------------------------------------------------------------------------


def recent_activity(
    db_path: Path | str, *,
    client_code: str, limit: int = 20,
) -> list[dict[str, Any]]:
    """Interleave document status changes + notifications + messages
    into a reverse-chronological feed."""
    events: list[dict[str, Any]] = []
    with _open(db_path) as conn:
        if _table_exists(conn, 'documents'):
            # Prefer explicit review_timestamp when present; fall back to
            # uploaded_at then document_date. Legacy test DBs that don't
            # have review_timestamp fall through to the two-column version.
            doc_cols = {r['name'] if hasattr(r, 'keys') else r[1]
                         for r in conn.execute(
                             "PRAGMA table_info(documents)"
                         ).fetchall()}
            if 'review_timestamp' in doc_cols:
                ts_sql = ("COALESCE(review_timestamp, uploaded_at, "
                          "document_date)")
            elif 'uploaded_at' in doc_cols:
                ts_sql = "COALESCE(uploaded_at, document_date)"
            else:
                ts_sql = "document_date"
            rows = conn.execute(
                f"SELECT document_id, vendor, amount, review_status, "
                f"  {ts_sql} AS ts "
                f"FROM documents WHERE client_code=? "
                f"ORDER BY ts DESC LIMIT ?",
                (client_code, limit),
            ).fetchall()
            for r in rows:
                events.append({
                    'kind': 'document',
                    'ts': r['ts'],
                    'summary': (f"{r['review_status']} — "
                                 f"{r['vendor'] or 'receipt'} "
                                 f"(${r['amount'] or 0:.2f})"),
                    'document_id': r['document_id'],
                })

        if _table_exists(conn, 'client_notifications'):
            rows = conn.execute(
                "SELECT kind, title, body, created_at, read_at, document_id "
                "FROM client_notifications WHERE client_code=? "
                "ORDER BY created_at DESC LIMIT ?",
                (client_code, limit),
            ).fetchall()
            for r in rows:
                events.append({
                    'kind': 'notification',
                    'ts': r['created_at'],
                    'summary': r['title'] or r['kind'],
                    'body': r['body'],
                    'unread': not r['read_at'],
                    'document_id': r['document_id'],
                })

    events.sort(key=lambda e: e.get('ts') or '', reverse=True)
    return events[:limit]


# ---------------------------------------------------------------------------
# YTD summary
# ---------------------------------------------------------------------------


def ytd_summary(
    db_path: Path | str, *,
    client_code: str, year: int | None = None,
) -> dict[str, Any]:
    year = year or datetime.now(timezone.utc).year
    y_prefix = f"{year:04d}"
    with _open(db_path) as conn:
        if not _table_exists(conn, 'documents'):
            return {'year': year, 'total_receipts': 0,
                     'total_expenses_cad': 0.0,
                     'this_month': 0, 'prior_month': 0}
        row = conn.execute(
            "SELECT COUNT(*) AS n, "
            "       COALESCE(SUM(amount), 0) AS total "
            "FROM documents WHERE client_code=? "
            "AND COALESCE(document_date,'') LIKE ?",
            (client_code, f"{y_prefix}%"),
        ).fetchone()
        this_month = int(conn.execute(
            "SELECT COUNT(*) FROM documents WHERE client_code=? "
            "AND COALESCE(document_date,'') LIKE ?",
            (client_code, f"{_iso_now()[:7]}%"),
        ).fetchone()[0] or 0)
        prior_month_prefix = _compute_prior_month_prefix()
        prior_month = int(conn.execute(
            "SELECT COUNT(*) FROM documents WHERE client_code=? "
            "AND COALESCE(document_date,'') LIKE ?",
            (client_code, f"{prior_month_prefix}%"),
        ).fetchone()[0] or 0)
    return {
        'year': year,
        'total_receipts': int(row['n'] or 0),
        'total_expenses_cad': round(float(row['total'] or 0), 2),
        'this_month': this_month,
        'prior_month': prior_month,
    }


def _compute_prior_month_prefix() -> str:
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    month -= 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}-{month:02d}"


# ---------------------------------------------------------------------------
# Notifications — create + mark read
# ---------------------------------------------------------------------------


def create_notification(
    db_path: Path | str, *,
    client_code: str, kind: str, title: str, body: str = '',
    document_id: str | None = None,
) -> int:
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO client_notifications "
            "(client_code, kind, title, body, document_id) VALUES "
            "(?,?,?,?,?)",
            (client_code, kind, title, body, document_id),
        )
        conn.commit()
        return int(cur.lastrowid)


def unread_count(db_path: Path | str, *, client_code: str) -> int:
    with _open(db_path) as conn:
        if not _table_exists(conn, 'client_notifications'):
            return 0
        row = conn.execute(
            "SELECT COUNT(*) FROM client_notifications "
            "WHERE client_code=? AND read_at IS NULL",
            (client_code,),
        ).fetchone()
    return int(row[0]) if row else 0


def mark_notifications_read(
    db_path: Path | str, *,
    client_code: str,
    notification_ids: list[int] | None = None,
) -> int:
    with _open(db_path) as conn:
        if notification_ids:
            placeholders = ','.join('?' * len(notification_ids))
            cur = conn.execute(
                f"UPDATE client_notifications SET read_at=? "
                f"WHERE client_code=? AND id IN ({placeholders}) "
                f"AND read_at IS NULL",
                (_iso_now(), client_code, *notification_ids),
            )
        else:
            cur = conn.execute(
                "UPDATE client_notifications SET read_at=? "
                "WHERE client_code=? AND read_at IS NULL",
                (_iso_now(), client_code),
            )
        conn.commit()
        return cur.rowcount


# ---------------------------------------------------------------------------
# Threaded messaging
# ---------------------------------------------------------------------------


def create_thread(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    subject: str, document_id: str | None = None,
) -> int:
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO client_message_threads "
            "(firm_code, client_code, subject, document_id) VALUES (?,?,?,?)",
            (firm_code, client_code, subject, document_id),
        )
        conn.commit()
        return int(cur.lastrowid)


def post_message(
    db_path: Path | str, *,
    thread_id: int, sender_type: str,
    sender_id: str, body: str,
    attachment_url: str | None = None,
) -> int:
    if sender_type not in ('client', 'cpa'):
        raise ValueError(f"sender_type must be 'client' or 'cpa', got {sender_type!r}")
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO client_message_posts "
            "(thread_id, sender_type, sender_id, body, attachment_url) "
            "VALUES (?,?,?,?,?)",
            (thread_id, sender_type, sender_id, body, attachment_url),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_thread(
    db_path: Path | str, *,
    thread_id: int, mark_read_as: str | None = None,
) -> dict[str, Any]:
    """Return the thread header + all posts. When ``mark_read_as`` is
    'client' or 'cpa', automatically mark every post by the OTHER
    party as read."""
    with _open(db_path) as conn:
        header = conn.execute(
            "SELECT * FROM client_message_threads WHERE id=?",
            (thread_id,),
        ).fetchone()
        if mark_read_as in ('client', 'cpa'):
            other = 'cpa' if mark_read_as == 'client' else 'client'
            conn.execute(
                "UPDATE client_message_posts SET read_at=? "
                "WHERE thread_id=? AND sender_type=? AND read_at IS NULL",
                (_iso_now(), thread_id, other),
            )
            conn.commit()
        # Re-read after mark-read so the caller sees up-to-date read_at.
        posts = conn.execute(
            "SELECT * FROM client_message_posts "
            "WHERE thread_id=? ORDER BY id",
            (thread_id,),
        ).fetchall()
    return {
        'header': dict(header) if header else None,
        'posts': [dict(p) for p in posts],
    }


def list_threads(
    db_path: Path | str, *, client_code: str,
) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT t.*, "
            "       (SELECT MAX(created_at) FROM client_message_posts "
            "         WHERE thread_id = t.id) AS last_post_at, "
            "       (SELECT COUNT(*) FROM client_message_posts "
            "         WHERE thread_id = t.id AND read_at IS NULL "
            "         AND sender_type = 'cpa') AS unread_from_cpa "
            "FROM client_message_threads t "
            "WHERE client_code=? "
            "ORDER BY last_post_at DESC, t.id DESC",
            (client_code,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Bundle — single call for the /c/{token}/status route
# ---------------------------------------------------------------------------


def build_client_status(
    db_path: Path | str, *,
    client_code: str,
) -> dict[str, Any]:
    return {
        'client_code': client_code,
        'upload_status': upload_status(db_path, client_code=client_code),
        'recent_activity': recent_activity(db_path, client_code=client_code),
        'ytd_summary': ytd_summary(db_path, client_code=client_code),
        'unread_notifications': unread_count(db_path,
                                                client_code=client_code),
        'threads': list_threads(db_path, client_code=client_code),
    }
