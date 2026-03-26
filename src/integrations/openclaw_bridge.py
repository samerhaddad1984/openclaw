"""
src/integrations/openclaw_bridge.py
=====================================
Bridge between OpenClaw (WhatsApp / Telegram gateway) and LedgerLink.

OpenClaw handles all messaging transport.  This module handles only the
accounting side: decode the file bytes, run OCR, store the document, and
log the event.  It does *not* send replies — OpenClaw does that.

Endpoint exposed in review_dashboard.py:
    POST /ingest/openclaw
        Content-Type: application/json
        Body: {
            "platform":      "whatsapp" | "telegram",
            "sender_id":     "<phone number or Telegram chat ID>",
            "media_url":     "<original URL, informational only>",
            "media_type":    "<MIME type, e.g. image/jpeg>",
            "client_message":"<text body of the message, if any>",
            "file_bytes":    "<base64-encoded file content>"
        }
        Response: {
            "ok":          true | false,
            "document_id": "<doc_id> | null",
            "status":      "processed" | "unknown_sender" | "no_file" | "error",
            "error":       "<message or null>"
        }
"""
from __future__ import annotations

import base64
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH    = ROOT_DIR / "data" / "ledgerlink_agent.db"
UPLOAD_DIR = ROOT_DIR / "data" / "ocr_uploads"

_VALID_PLATFORMS = {"whatsapp", "telegram"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Client lookup
# ---------------------------------------------------------------------------

def get_client_by_sender_id(
    platform: str,
    sender_id: str,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any] | None:
    """Return the dashboard_users row matching *sender_id* for *platform*.

    For WhatsApp:  matches against the ``whatsapp_number`` column using a
                   digits-only suffix match (tolerates country-code prefixes).
    For Telegram:  matches against the ``telegram_id`` column (exact string).

    Returns ``None`` if no active user is found.
    """
    if not sender_id:
        return None

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row

            if platform == "whatsapp":
                normalized = "".join(c for c in sender_id if c.isdigit())
                if not normalized:
                    return None
                rows = conn.execute(
                    "SELECT username, client_code, language, display_name, whatsapp_number "
                    "FROM dashboard_users "
                    "WHERE whatsapp_number IS NOT NULL AND active=1"
                ).fetchall()
                for row in rows:
                    stored = "".join(
                        c for c in (row["whatsapp_number"] or "") if c.isdigit()
                    )
                    if stored and (
                        stored == normalized
                        or stored.endswith(normalized)
                        or normalized.endswith(stored)
                    ):
                        return dict(row)

            elif platform == "telegram":
                row = conn.execute(
                    "SELECT username, client_code, language, display_name "
                    "FROM dashboard_users "
                    "WHERE telegram_id=? AND active=1",
                    (str(sender_id),),
                ).fetchone()
                if row:
                    return dict(row)

    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Messaging log
# ---------------------------------------------------------------------------

def log_messaging_event(
    *,
    client_code: str | None,
    platform: str,
    direction: str,
    message_type: str,
    document_id: str | None = None,
    status: str = "delivered",
    db_path: Path = DB_PATH,
) -> None:
    """Insert one row into ``messaging_log``. Silently ignores all errors."""
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                INSERT INTO messaging_log
                    (client_code, platform, direction, message_type,
                     document_id, sent_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_code, platform, direction, message_type,
                    document_id, _utc_now_iso(), status,
                ),
            )
            conn.commit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core ingest handler
# ---------------------------------------------------------------------------

def handle_openclaw_ingest(
    payload: dict[str, Any],
    *,
    db_path: Path = DB_PATH,
    upload_dir: Path = UPLOAD_DIR,
) -> dict[str, Any]:
    """Process one inbound media message forwarded by OpenClaw.

    Parameters
    ----------
    payload:
        Decoded JSON body from POST /ingest/openclaw.
    db_path:
        SQLite database path (override in tests).
    upload_dir:
        Directory where OCR uploads are saved (override in tests).

    Returns
    -------
    dict with keys: ok, document_id, status, error
    """
    platform       = str(payload.get("platform") or "").lower()
    sender_id      = str(payload.get("sender_id") or "").strip()
    media_type     = str(payload.get("media_type") or "application/octet-stream")
    client_message = str(payload.get("client_message") or "").strip()
    file_bytes_b64 = payload.get("file_bytes") or ""

    # 1. Validate platform
    if platform not in _VALID_PLATFORMS:
        return {
            "ok":          False,
            "document_id": None,
            "status":      "error",
            "error":       f"unsupported_platform: {platform!r}",
        }

    # 2. Resolve client
    client = get_client_by_sender_id(platform, sender_id, db_path=db_path)
    client_code = (client or {}).get("client_code")

    if client is None:
        log_messaging_event(
            client_code=None,
            platform=platform,
            direction="inbound",
            message_type="media" if file_bytes_b64 else "text",
            status="unknown_sender",
            db_path=db_path,
        )
        return {
            "ok":          False,
            "document_id": None,
            "status":      "unknown_sender",
            "error":       "sender_id not registered",
        }

    # 3. Text-only message (no file attached)
    if not file_bytes_b64:
        log_messaging_event(
            client_code=client_code,
            platform=platform,
            direction="inbound",
            message_type="text",
            status="delivered",
            db_path=db_path,
        )
        return {
            "ok":          True,
            "document_id": None,
            "status":      "no_file",
            "error":       None,
        }

    # 4. Decode file bytes
    try:
        if isinstance(file_bytes_b64, str):
            file_bytes = base64.b64decode(file_bytes_b64)
        else:
            file_bytes = bytes(file_bytes_b64)
    except Exception as exc:
        log_messaging_event(
            client_code=client_code,
            platform=platform,
            direction="inbound",
            message_type="media",
            status="failed",
            db_path=db_path,
        )
        return {
            "ok":          False,
            "document_id": None,
            "status":      "error",
            "error":       f"base64_decode_failed: {exc}",
        }

    # 5. Derive a filename from the MIME type
    _EXT_MAP: dict[str, str] = {
        "image/jpeg":      "photo.jpg",
        "image/jpg":       "photo.jpg",
        "image/png":       "photo.png",
        "image/webp":      "photo.webp",
        "image/tiff":      "photo.tiff",
        "image/heic":      "photo.heic",
        "application/pdf": "document.pdf",
    }
    filename = _EXT_MAP.get(media_type.lower(), "attachment.bin")

    # 6. Run through the OCR pipeline
    try:
        from src.engines.ocr_engine import process_file  # type: ignore[import]

        result = process_file(
            file_bytes,
            filename,
            client_code=client_code or "",
            submitted_by=sender_id,
            ingest_source=platform,
            db_path=db_path,
            upload_dir=upload_dir,
        )
    except Exception as exc:
        log_messaging_event(
            client_code=client_code,
            platform=platform,
            direction="inbound",
            message_type="media",
            status="failed",
            db_path=db_path,
        )
        return {
            "ok":          False,
            "document_id": None,
            "status":      "error",
            "error":       str(exc),
        }

    doc_id = result.get("document_id")
    ok     = bool(result.get("ok"))

    log_messaging_event(
        client_code=client_code,
        platform=platform,
        direction="inbound",
        message_type="media",
        document_id=doc_id,
        status="delivered" if ok else "failed",
        db_path=db_path,
    )

    return {
        "ok":          ok,
        "document_id": doc_id,
        "status":      "processed" if ok else "error",
        "error":       result.get("error"),
    }


# ---------------------------------------------------------------------------
# Troubleshoot stats helper
# ---------------------------------------------------------------------------

def get_bridge_stats(*, db_path: Path = DB_PATH) -> dict[str, Any]:
    """Return stats for the /troubleshoot OpenClaw panel.

    Returns a dict with:
        last_received_at  — ISO timestamp of last inbound message or None
        messages_today    — count of inbound messages today (UTC)
        table_exists      — whether messaging_log table exists
    """
    stats: dict[str, Any] = {
        "last_received_at": None,
        "messages_today":   0,
        "table_exists":     False,
    }
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            # Check table exists
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messaging_log'"
            ).fetchone()
            if not exists:
                return stats
            stats["table_exists"] = True

            row = conn.execute(
                "SELECT sent_at FROM messaging_log "
                "WHERE direction='inbound' "
                "ORDER BY sent_at DESC LIMIT 1"
            ).fetchone()
            if row:
                stats["last_received_at"] = row["sent_at"]

            today = datetime.now(timezone.utc).date().isoformat()
            count_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM messaging_log "
                "WHERE direction='inbound' AND sent_at LIKE ?",
                (f"{today}%",),
            ).fetchone()
            stats["messages_today"] = count_row["cnt"] if count_row else 0

    except Exception:
        pass

    return stats
