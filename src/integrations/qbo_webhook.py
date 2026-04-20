"""QBO webhook receiver.

Intuit POSTs ``{eventNotifications: [{realmId, dataChangeEvent:
{entities: [{name, id, operation, lastUpdated, ...}, ...]}}]}`` to
the app-level webhook URL. Requests carry an ``intuit-signature``
header — HMAC-SHA256 of the raw body, base64-encoded, keyed by the
``QBO_WEBHOOK_VERIFIER_TOKEN`` env var.

Contract:

- **Always respond 200** (Stripe-style). Failures logged, queued for
  retry, but never bubble to Intuit — they stop retrying on 5xx.
- **Idempotent** — every entity event carries its own synthetic
  ``event_id`` (``<realm>:<entity>:<id>:<lastUpdated>``). Duplicates
  are dropped.
- **Async processing** — the handler writes raw events to
  ``qbo_webhook_events`` with ``processed=0``. A worker (run in
  ``qbo_sync.py`` orchestrator or the incremental-sync cron) picks
  them up and delegates to QBOPull / QBOPush.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def verify_qbo_signature(body: bytes, signature_header: str,
                          verifier_token: str) -> bool:
    """HMAC-SHA256, base64, constant-time compare."""
    if not verifier_token or not signature_header:
        return False
    mac = hmac.new(verifier_token.encode('utf-8'), body, hashlib.sha256)
    expected = base64.b64encode(mac.digest()).decode('ascii')
    return hmac.compare_digest(expected, signature_header.strip())


def parse_webhook_body(body: bytes | str) -> list[dict[str, Any]]:
    """Flatten Intuit's nested payload into one record per entity event.

    Returns a list of ``{realm_id, entity_type, entity_id, operation,
    last_updated, event_id}``.
    """
    if isinstance(body, bytes):
        body = body.decode('utf-8')
    try:
        parsed = json.loads(body)
    except ValueError:
        return []

    events: list[dict[str, Any]] = []
    for notif in parsed.get('eventNotifications') or []:
        realm = str(notif.get('realmId') or '')
        entities = (notif.get('dataChangeEvent') or {}).get('entities') or []
        for ent in entities:
            entity_type = ent.get('name') or ''
            entity_id = str(ent.get('id') or '')
            operation = ent.get('operation') or ''
            last_upd = ent.get('lastUpdated') or ''
            if not (realm and entity_type and entity_id):
                continue
            event_id = f"{realm}:{entity_type}:{entity_id}:{last_upd}"
            events.append({
                'realm_id': realm,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'operation': operation,
                'last_updated': last_upd,
                'event_id': event_id,
            })
    return events


def store_webhook_events(
    db_path: Path | str,
    events: list[dict[str, Any]],
) -> int:
    """Idempotent insert. Returns how many rows were freshly stored."""
    stored = 0
    with sqlite3.connect(str(db_path)) as conn:
        for ev in events:
            try:
                conn.execute(
                    "INSERT INTO qbo_webhook_events "
                    "(event_id, realm_id, entity_type, entity_id, "
                    "operation, last_updated, processed) "
                    "VALUES (?,?,?,?,?,?, 0)",
                    (ev['event_id'], ev['realm_id'], ev['entity_type'],
                     ev['entity_id'], ev['operation'], ev['last_updated']),
                )
                stored += 1
            except sqlite3.IntegrityError:
                # Duplicate event_id — already seen, skip.
                continue
        conn.commit()
    return stored


def handle_webhook(
    body: bytes | str,
    signature_header: str,
    *,
    db_path: Path | str,
    verifier_token: str,
) -> dict[str, Any]:
    """Top-level handler invoked from the HTTP route.

    Returns a dict the caller uses to build the 200 response. Never
    raises on malformed input — always returns a JSON-shaped dict so
    the HTTP handler can acknowledge Intuit without exposing errors.
    """
    body_bytes = body.encode('utf-8') if isinstance(body, str) else body
    if not verify_qbo_signature(body_bytes, signature_header, verifier_token):
        log.warning("QBO webhook: signature verification failed")
        return {'ok': False, 'error': 'bad_signature', 'status': 401}

    events = parse_webhook_body(body_bytes)
    stored = store_webhook_events(db_path, events)
    return {
        'ok': True,
        'events_received': len(events),
        'events_stored': stored,
        'status': 200,
    }


# ---------------------------------------------------------------------------
# Async processing
# ---------------------------------------------------------------------------

def pending_events(db_path: Path | str, limit: int = 100) -> list[dict[str, Any]]:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM qbo_webhook_events WHERE processed=0 "
            "ORDER BY received_at, id LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_processed(
    db_path: Path | str,
    event_id: str,
    *,
    error: str | None = None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE qbo_webhook_events SET processed=1, processed_at=datetime('now'), "
            "error=? WHERE event_id=?",
            (error, event_id),
        )
        conn.commit()


def resolve_firm_client(
    db_path: Path | str,
    realm_id: str,
) -> tuple[str | None, str | None]:
    """Find which (firm_code, client_code) owns this realm."""
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT firm_code, client_code FROM qbo_connections "
            "WHERE realm_id=? AND status='active' LIMIT 1",
            (realm_id,),
        ).fetchone()
    if not row:
        return None, None
    return row['firm_code'], row['client_code']


def process_one_event(
    db_path: Path | str,
    event: dict[str, Any],
    *,
    puller_cls: Any = None,
) -> None:
    """Handle a single webhook event. Never raises — records the error
    in ``qbo_webhook_events.error``."""
    try:
        firm, client = resolve_firm_client(db_path, event['realm_id'])
        if not firm or not client:
            mark_processed(db_path, event['event_id'],
                            error=f"no connection for realm {event['realm_id']}")
            return

        op = (event.get('operation') or '').lower()
        entity_type = event['entity_type']

        if op in ('delete', 'void'):
            _mark_deleted(db_path, firm, client, entity_type, event['entity_id'])
            mark_processed(db_path, event['event_id'])
            return

        if op == 'merge':
            # Merges can't be auto-resolved; flag for review.
            _flag_merge(db_path, firm, client, entity_type, event['entity_id'],
                         event.get('last_updated'))
            mark_processed(db_path, event['event_id'])
            return

        # Create / Update -> pull the single entity.
        if puller_cls is None:
            from src.integrations.qbo_pull import QBOPull as puller_cls  # type: ignore
        puller = puller_cls(firm, client, db_path=db_path)
        q = f"SELECT * FROM {entity_type} WHERE Id = '{event['entity_id']}'"
        rows = puller._query(q, max_results=1)
        if not rows:
            mark_processed(db_path, event['event_id'],
                            error='entity not found at QBO')
            return
        upsert = getattr(puller, f"_upsert_{_snake(entity_type)}", None)
        if upsert is None:
            mark_processed(db_path, event['event_id'],
                            error=f"no handler for {entity_type}")
            return
        upsert(rows[0])
        mark_processed(db_path, event['event_id'])
    except Exception as exc:
        log.exception("webhook event failed")
        mark_processed(db_path, event['event_id'], error=str(exc))


def _mark_deleted(db_path: Path | str, firm: str, client: str,
                    entity_type: str, qbo_id: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE qbo_sync_state SET sync_status='deleted' "
            "WHERE firm_code=? AND client_code=? "
            "AND entity_type=? AND qbo_id=?",
            (firm, client, entity_type, qbo_id),
        )
        conn.commit()


def _flag_merge(db_path: Path | str, firm: str, client: str,
                 entity_type: str, qbo_id: str,
                 last_updated: str | None) -> None:
    details = json.dumps({
        'reason': 'qbo_merge_event',
        'last_updated': last_updated or '',
    })
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO qbo_sync_state "
            "(firm_code, client_code, entity_type, qbo_id, "
            "sync_status, conflict_details) "
            "VALUES (?,?,?,?, 'conflict', ?) "
            "ON CONFLICT(firm_code, client_code, entity_type, qbo_id) DO UPDATE SET "
            "sync_status='conflict', conflict_details=excluded.conflict_details",
            (firm, client, entity_type, qbo_id, details),
        )
        conn.commit()


def _snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            out.append('_')
        out.append(ch.lower())
    return ''.join(out)
