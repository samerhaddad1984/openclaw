"""First-login experience: getting-started checklist + welcome modal +
empty-state CTAs + quick-setup page.

The checklist is auto-computed from DB state on every render so it
stays honest — we never ask the user to manually tick boxes for
things already done. Each item reports:

  {id, label_en, label_fr, href, done, dismissable}

The widget is hidden globally when the user dismissed it (flag on
``dashboard_users``) or when every item is done.

Welcome modal: shown on first login only. Tour-start / tour-skip
both flip ``dashboard_users.welcome_seen_at`` so the modal never
reappears.

This module is pure — the dashboard wires render hooks and writes
the tick/dismiss timestamps.
"""
from __future__ import annotations

import html
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    out = set()
    for r in conn.execute(f"PRAGMA table_info({table})").fetchall():
        out.add(r['name'] if hasattr(r, 'keys') else r[1])
    return out


def ensure_onboarding_schema(db_path: Path | str) -> None:
    """Idempotent: add checklist + welcome columns on dashboard_users."""
    with _open(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dashboard_users "
            "(username TEXT PRIMARY KEY, role TEXT, firm_code TEXT)"
        )
        cols = _columns(conn, 'dashboard_users')
        wanted = {
            'getting_started_completed': 'INTEGER DEFAULT 0',
            'getting_started_dismissed_at': 'TEXT',
            'first_login_at': 'TEXT',
            'welcome_seen_at': 'TEXT',
            'tour_completed_at': 'TEXT',
        }
        for col, ddl in wanted.items():
            if col not in cols:
                conn.execute(f"ALTER TABLE dashboard_users ADD COLUMN {col} {ddl}")
        # user_events for lightweight tracking (guide view etc)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                firm_code TEXT,
                event_type TEXT,
                event_data TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_events_user "
            "ON user_events(username, event_type)"
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Checklist state
# ---------------------------------------------------------------------------


def _count(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> int:
    try:
        row = conn.execute(sql, args).fetchone()
    except sqlite3.OperationalError:
        return 0
    if not row:
        return 0
    return int(row[0] if not hasattr(row, 'keys') else row['c'] if 'c' in row.keys()
                else list(row)[0])


def compute_checklist(
    db_path: Path | str,
    *,
    firm_code: str,
    username: str | None = None,
) -> list[dict[str, Any]]:
    """Return the 6-item checklist with ``done`` computed from live DB
    state. The order is fixed — each item has a stable id so the UI
    template can render consistently."""
    with _open(db_path) as conn:
        # firms row (may not exist yet)
        firms_row = None
        try:
            firms_row = conn.execute(
                "SELECT * FROM firms WHERE firm_code=?", (firm_code,),
            ).fetchone()
        except sqlite3.OperationalError:
            pass
        firm_profile_complete = bool(
            firms_row and firms_row['name']
            and (firms_row['address'] if 'address' in firms_row.keys() else None)
            and (firms_row['phone'] if 'phone' in firms_row.keys() else None)
        )
        client_count = _count(
            conn,
            "SELECT COUNT(*) FROM clients WHERE firm_code=?",
            (firm_code,),
        )
        qbo_count = 0
        try:
            qbo_count = _count(
                conn,
                "SELECT COUNT(*) FROM qbo_connections WHERE firm_code=? "
                "AND status='active'", (firm_code,),
            )
        except sqlite3.OperationalError:
            pass
        portal_sent = _count(
            conn,
            "SELECT COUNT(*) FROM clients WHERE firm_code=? "
            "AND portal_token IS NOT NULL AND portal_token != ''",
            (firm_code,),
        )
        doc_count = _count(
            conn,
            "SELECT COUNT(*) FROM documents "
            "WHERE firm_code=? OR (firm_code IS NULL AND ? = 'OWNER')",
            (firm_code, firm_code),
        )
        guide_viewed = False
        if username:
            row = conn.execute(
                "SELECT 1 FROM user_events WHERE username=? AND "
                "event_type='viewed_guide' LIMIT 1",
                (username,),
            ).fetchone()
            guide_viewed = row is not None

    return [
        {'id': 'firm_profile',
         'label_en': 'Complete your firm profile',
         'label_fr': 'Complétez le profil de votre cabinet',
         'href': '/onboarding/quick_setup',
         'done': firm_profile_complete},
        {'id': 'first_client',
         'label_en': 'Add your first client',
         'label_fr': 'Ajoutez votre premier client',
         'href': '/clients/new',
         'done': client_count > 0},
        {'id': 'connect_qbo',
         'label_en': 'Connect QuickBooks (optional)',
         'label_fr': 'Connecter QuickBooks (optionnel)',
         'href': '/qbo/status',
         'done': qbo_count > 0,
         'dismissable': True},
        {'id': 'portal_sent',
         'label_en': 'Send portal link to first client',
         'label_fr': 'Envoyer le lien portail au premier client',
         'href': '/clients',
         'done': portal_sent > 0},
        {'id': 'first_document',
         'label_en': 'Upload a test receipt to try the system',
         'label_fr': 'Téléversez un reçu test',
         'href': '/queue',
         'done': doc_count > 0},
        {'id': 'guide_viewed',
         'label_en': 'Review the getting-started guide',
         'label_fr': 'Consultez le guide de démarrage',
         'href': '/docs/getting-started',
         'done': guide_viewed},
    ]


def all_done(items: list[dict[str, Any]]) -> bool:
    """True when every non-dismissable item is done (dismissable items
    like QBO are skippable)."""
    return all(it.get('done') or it.get('dismissable') for it in items)


def should_show(db_path: Path | str, *, username: str) -> bool:
    """False when the user dismissed or all items completed."""
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT getting_started_dismissed_at, firm_code "
            "FROM dashboard_users WHERE username=?", (username,),
        ).fetchone()
    if not row:
        return True
    if row['getting_started_dismissed_at']:
        return False
    items = compute_checklist(db_path, firm_code=row['firm_code'] or '',
                                username=username)
    return not all_done(items)


def dismiss(db_path: Path | str, *, username: str) -> None:
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE dashboard_users SET "
            "getting_started_dismissed_at=?, getting_started_completed=1 "
            "WHERE username=?",
            (_iso_now(), username),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Welcome modal state
# ---------------------------------------------------------------------------


def should_show_welcome(db_path: Path | str, *, username: str) -> bool:
    """Show on very first login only."""
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT first_login_at, welcome_seen_at "
            "FROM dashboard_users WHERE username=?", (username,),
        ).fetchone()
    if not row:
        return False
    # If they've never logged in we stamp it + show the modal.
    return not row['welcome_seen_at']


def record_first_login(db_path: Path | str, *, username: str) -> None:
    """Called at the top of every authenticated request. No-op after
    the first call."""
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE dashboard_users SET first_login_at = COALESCE(first_login_at, ?) "
            "WHERE username=?",
            (_iso_now(), username),
        )
        conn.commit()


def mark_welcome_seen(db_path: Path | str, *, username: str,
                        tour_taken: bool = False) -> None:
    now = _iso_now()
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE dashboard_users SET welcome_seen_at=?, "
            "tour_completed_at = CASE WHEN ? THEN ? ELSE tour_completed_at END "
            "WHERE username=?",
            (now, 1 if tour_taken else 0, now, username),
        )
        conn.commit()


def log_event(db_path: Path | str, *, username: str, firm_code: str,
                event_type: str, event_data: str = '') -> None:
    with _open(db_path) as conn:
        conn.execute(
            "INSERT INTO user_events (username, firm_code, event_type, event_data) "
            "VALUES (?,?,?,?)",
            (username, firm_code, event_type, event_data),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------


def _esc(v: Any) -> str:
    return html.escape(str(v if v is not None else ''))


def render_checklist_widget(items: list[dict[str, Any]], *,
                              lang: str = 'en') -> str:
    """Sidebar widget HTML. Returns empty string when every item done."""
    if all_done(items):
        return ''
    label_key = 'label_fr' if lang == 'fr' else 'label_en'
    rows = []
    for it in items:
        mark = '&#10003;' if it['done'] else '&#9744;'
        style = ('text-decoration:line-through;color:#888;'
                  if it['done'] else '')
        rows.append(
            f'<li style="{style}">{mark} '
            f'<a href="{_esc(it["href"])}">{_esc(it[label_key])}</a></li>'
        )
    return (
        '<div class="card getting-started-widget" '
        'style="border:1px solid #ddd;padding:1rem;border-radius:6px;'
        'margin-bottom:1rem;background:#fffef5;">'
        f'<h3 style="margin-top:0;">{_esc("Getting started" if lang == "en" else "Démarrage")}</h3>'
        f'<ul style="list-style:none;padding-left:0;line-height:1.8">{"".join(rows)}</ul>'
        '<form method="POST" action="/onboarding/checklist/dismiss" '
        'style="margin-top:.5rem;">'
        f'<button class="small">{_esc("Dismiss" if lang == "en" else "Masquer")}</button>'
        '</form></div>'
    )


def render_welcome_modal(lang: str = 'en') -> str:
    """Return HTML for the welcome modal. Empty string when caller
    decides not to show it."""
    title = 'Welcome to OtoCPA' if lang == 'en' else 'Bienvenue sur OtoCPA'
    bullets_en = [
        'AI-powered OCR reads your receipts automatically.',
        'Two-way QuickBooks sync — no double entry.',
        'Client portal — let clients upload directly.',
    ]
    bullets_fr = [
        "OCR assistée par IA : vos reçus sont lus automatiquement.",
        'Synchronisation QuickBooks bidirectionnelle — pas de double saisie.',
        'Portail client — vos clients téléversent directement.',
    ]
    bullets = bullets_fr if lang == 'fr' else bullets_en
    take_tour = 'Take the tour' if lang == 'en' else 'Visite guidée'
    skip = 'Skip and explore' if lang == 'en' else 'Ignorer et explorer'
    lis = ''.join(f'<li>{_esc(b)}</li>' for b in bullets)
    return (
        '<div id="welcome-modal" style="position:fixed;inset:0;'
        'background:rgba(0,0,0,.5);display:flex;align-items:center;'
        'justify-content:center;z-index:9999;">'
        '<div style="background:white;padding:2rem;border-radius:8px;'
        'max-width:500px;width:90%;">'
        f'<h2>{_esc(title)}</h2>'
        f'<ul>{lis}</ul>'
        '<form method="POST" action="/onboarding/welcome/ack" '
        'style="display:flex;gap:1rem;justify-content:flex-end;">'
        f'<button name="tour" value="0">{_esc(skip)}</button>'
        f'<button name="tour" value="1" class="primary">{_esc(take_tour)}</button>'
        '</form></div></div>'
    )


# ---------------------------------------------------------------------------
# Empty-state helpers
# ---------------------------------------------------------------------------


_EMPTY_STATES: dict[str, dict[str, dict[str, str]]] = {
    '/queue': {
        'en': {'what': 'Uploaded documents awaiting review appear here.',
                'cta_label': 'Upload receipts',
                'cta_href': '/upload'},
        'fr': {'what': 'Les documents téléversés à réviser apparaissent ici.',
                'cta_label': 'Téléverser des reçus',
                'cta_href': '/upload'},
    },
    '/clients': {
        'en': {'what': 'Clients you serve, with their portal links.',
                'cta_label': 'Add your first client',
                'cta_href': '/clients/new'},
        'fr': {'what': 'Vos clients et leurs liens vers le portail.',
                'cta_label': 'Ajouter un premier client',
                'cta_href': '/clients/new'},
    },
    '/reconciliation': {
        'en': {'what': 'Bank reconciliation lives here once a bank is connected.',
                'cta_label': 'Connect a bank',
                'cta_href': '/bank/feeds'},
        'fr': {'what': "La conciliation bancaire apparaît ici après la connexion d'une banque.",
                'cta_label': 'Connecter une banque',
                'cta_href': '/bank/feeds'},
    },
    '/financial_statements': {
        'en': {'what': 'Generated trial balance, P&L, balance sheet appear here.',
                'cta_label': 'Complete a reconciliation first',
                'cta_href': '/reconciliation'},
        'fr': {'what': 'Bilan, résultats, balance apparaissent ici.',
                'cta_label': "Commencez par une conciliation",
                'cta_href': '/reconciliation'},
    },
    '/audit/engagements': {
        'en': {'what': 'Audit engagements and working papers appear here.',
                'cta_label': 'Create your first engagement',
                'cta_href': '/audit/engagements/new'},
        'fr': {'what': 'Mandats de vérification et documents de travail.',
                'cta_label': 'Créer un premier mandat',
                'cta_href': '/audit/engagements/new'},
    },
}


def render_empty_state(path: str, *, lang: str = 'en') -> str:
    data = _EMPTY_STATES.get(path)
    if not data:
        return ''
    copy = data.get(lang, data.get('en', {}))
    return (
        '<div class="empty-state" style="padding:2rem;text-align:center;'
        'border:2px dashed #ddd;border-radius:8px;margin:1rem 0;">'
        f'<p style="color:#555;">{_esc(copy["what"])}</p>'
        f'<a href="{_esc(copy["cta_href"])}" class="button primary" '
        'style="display:inline-block;padding:8px 16px;background:#1e40af;'
        'color:white;border-radius:4px;text-decoration:none;">'
        f'{_esc(copy["cta_label"])}</a></div>'
    )
