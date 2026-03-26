"""
src/integrations/whatsapp.py
============================
WhatsApp inbound webhook and outbound messaging via Twilio.

Receives incoming WhatsApp messages from Twilio, validates the HMAC
signature, extracts media attachments, runs them through process_file(),
and sends auto-replies in the client's language.

Webhook endpoint:
    POST /ingest/whatsapp
"""
from __future__ import annotations

import json
import sqlite3
import sys
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH     = ROOT_DIR / "data" / "ledgerlink_agent.db"
CONFIG_PATH = ROOT_DIR / "ledgerlink.config.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict[str, Any]:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Client lookup
# ---------------------------------------------------------------------------

def get_client_by_whatsapp_phone(phone: str) -> dict[str, Any] | None:
    """Look up a dashboard_users row by whatsapp_number matching *phone*.

    Normalises both sides to digits-only and does a suffix-match so that
    e.g. a stored "+15141234567" matches Twilio's "whatsapp:+15141234567".
    """
    normalized = "".join(c for c in phone if c.isdigit())
    if not normalized:
        return None
    try:
        with _open_db() as conn:
            rows = conn.execute(
                "SELECT username, client_code, language, display_name "
                "FROM dashboard_users "
                "WHERE whatsapp_number IS NOT NULL AND active=1"
            ).fetchall()
        for row in rows:
            stored = "".join(c for c in (row["whatsapp_number"] or "") if c.isdigit())
            if stored and (
                stored == normalized
                or stored.endswith(normalized)
                or normalized.endswith(stored)
            ):
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
    """Insert a row into messaging_log. Silently ignores all errors."""
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
# Outbound messaging
# ---------------------------------------------------------------------------

def send_whatsapp_message(to_number: str, body: str) -> bool:
    """Send a WhatsApp message via Twilio. Returns True on success."""
    cfg = _load_config().get("whatsapp", {})
    account_sid = cfg.get("account_sid", "")
    auth_token  = cfg.get("auth_token", "")
    from_number = cfg.get("from_number", "")
    if not account_sid or not auth_token or not from_number:
        return False
    try:
        from twilio.rest import Client  # type: ignore[import]
        client = Client(account_sid, auth_token)
        to = to_number if to_number.startswith("whatsapp:") else f"whatsapp:{to_number}"
        client.messages.create(body=body, from_=from_number, to=to)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Signature validation
# ---------------------------------------------------------------------------

def _validate_twilio_signature(
    signature: str, url: str, params: dict[str, str]
) -> bool:
    """Validate the X-Twilio-Signature header using the configured auth token."""
    cfg = _load_config().get("whatsapp", {})
    auth_token = cfg.get("auth_token", "")
    if not auth_token:
        return True  # Can't validate without token; accept in dev mode
    try:
        from twilio.request_validator import RequestValidator  # type: ignore[import]
        return RequestValidator(auth_token).validate(url, params, signature)
    except ImportError:
        return True  # twilio not installed — allow through
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Media download
# ---------------------------------------------------------------------------

def _download_twilio_media(media_url: str) -> bytes | None:
    """Download a media attachment from Twilio using Basic Auth credentials."""
    cfg = _load_config().get("whatsapp", {})
    account_sid = cfg.get("account_sid", "")
    auth_token  = cfg.get("auth_token", "")
    try:
        import requests as _req  # type: ignore[import]
        resp = _req.get(media_url, auth=(account_sid, auth_token), timeout=60)
        return resp.content if resp.status_code == 200 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Reply text helpers
# ---------------------------------------------------------------------------

def _get_language(client: dict[str, Any] | None) -> str:
    return (client or {}).get("language") or "fr"


def _reply_success(lang: str) -> str:
    if lang == "fr":
        return "Merci! Votre document a été reçu et sera traité sous peu."
    return "Thank you! Your document has been received and will be processed shortly."


def _reply_failure(lang: str) -> str:
    if lang == "fr":
        return (
            "Nous avons eu un problème avec votre document. "
            "Veuillez réessayer ou contacter votre comptable."
        )
    return (
        "We had a problem with your document. "
        "Please try again or contact your accountant."
    )


def _reply_instructions(lang: str) -> str:
    if lang == "fr":
        return (
            "Pour soumettre un document, envoyez une photo ou un fichier PDF. "
            "Types acceptés: factures, reçus, relevés bancaires."
        )
    return (
        "To submit a document, send a photo or PDF file. "
        "Accepted types: invoices, receipts, bank statements."
    )


def _reply_not_registered() -> str:
    return (
        "Ce numéro n'est pas enregistré. Contactez votre comptable. / "
        "This number is not registered. Please contact your accountant."
    )


# ---------------------------------------------------------------------------
# Core webhook handler
# ---------------------------------------------------------------------------

_IMAGE_MIMES = {"image/jpeg", "image/png", "image/webp", "image/tiff"}
_PDF_MIMES   = {"application/pdf"}
_AUDIO_MIMES_PREFIX = "audio/"


def handle_whatsapp_webhook(
    form_data: dict[str, str],
    signature: str,
    webhook_url: str,
) -> dict[str, Any]:
    """Process an incoming Twilio WhatsApp webhook POST.

    Parameters
    ----------
    form_data:    URL-decoded form fields from the Twilio POST body.
    signature:    Value of X-Twilio-Signature header.
    webhook_url:  Full public URL of this webhook endpoint (used for validation).

    Returns
    -------
    dict with keys: ok, reply_body, client_code, results
    """
    # 1. Validate Twilio signature
    if not _validate_twilio_signature(signature, webhook_url, form_data):
        return {"ok": False, "error": "invalid_signature", "reply_body": None, "results": []}

    from_number = form_data.get("From", "")
    num_media   = int(form_data.get("NumMedia", "0") or "0")

    # 2. Look up client by sender phone
    client      = get_client_by_whatsapp_phone(from_number)
    lang        = _get_language(client)
    client_code = (client or {}).get("client_code")

    if client is None:
        log_messaging_event(
            client_code=None, platform="whatsapp", direction="inbound",
            message_type="text" if num_media == 0 else "media",
            status="unknown_sender",
        )
        return {
            "ok": True,
            "reply_body": _reply_not_registered(),
            "client_code": None,
            "results": [],
        }

    # 3. Text-only message → send instructions
    if num_media == 0:
        log_messaging_event(
            client_code=client_code, platform="whatsapp",
            direction="inbound", message_type="text",
        )
        reply = _reply_instructions(lang)
        send_whatsapp_message(from_number, reply)
        log_messaging_event(
            client_code=client_code, platform="whatsapp",
            direction="outbound", message_type="text",
        )
        return {"ok": True, "reply_body": reply, "client_code": client_code, "results": []}

    # 4. Process media attachments
    from src.engines.ocr_engine import process_file  # type: ignore[import]

    results: list[dict[str, Any]] = []
    for i in range(num_media):
        media_url   = form_data.get(f"MediaUrl{i}", "")
        media_mime  = form_data.get(f"MediaContentType{i}", "application/octet-stream")
        if not media_url:
            continue

        # Skip audio gracefully
        if media_mime.startswith(_AUDIO_MIMES_PREFIX):
            log_messaging_event(
                client_code=client_code, platform="whatsapp",
                direction="inbound", message_type="audio",
            )
            continue

        ext_map = {
            "image/jpeg": "photo.jpg",
            "image/png":  "photo.png",
            "image/webp": "photo.webp",
            "application/pdf": "document.pdf",
        }
        filename = ext_map.get(media_mime, "attachment.bin")

        file_bytes = _download_twilio_media(media_url)
        if file_bytes is None:
            results.append({"ok": False, "error": "download_failed", "filename": filename})
            log_messaging_event(
                client_code=client_code, platform="whatsapp",
                direction="inbound", message_type="media", status="failed",
            )
            continue

        try:
            result = process_file(
                file_bytes, filename,
                client_code=client_code or "",
                submitted_by=from_number,
                ingest_source="whatsapp",
            )
            results.append(result)
            log_messaging_event(
                client_code=client_code, platform="whatsapp",
                direction="inbound", message_type="media",
                document_id=result.get("document_id"),
                status="delivered" if result.get("ok") else "failed",
            )
        except Exception as exc:
            results.append({"ok": False, "error": str(exc), "filename": filename})
            log_messaging_event(
                client_code=client_code, platform="whatsapp",
                direction="inbound", message_type="media", status="failed",
            )

    # 5. Auto-reply
    processed = [r for r in results if r.get("ok") is not None]
    any_ok = any(r.get("ok") for r in processed)

    if processed:
        reply = _reply_success(lang) if any_ok else _reply_failure(lang)
        send_whatsapp_message(from_number, reply)
        log_messaging_event(
            client_code=client_code, platform="whatsapp",
            direction="outbound", message_type="text",
        )
    else:
        reply = _reply_instructions(lang)
        send_whatsapp_message(from_number, reply)

    return {"ok": True, "reply_body": reply, "client_code": client_code, "results": results}


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class WhatsAppWebhookHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for POST /ingest/whatsapp."""

    webhook_url: str = ""  # Full public URL; set by start_whatsapp_server()

    def log_message(self, fmt: str, *args: Any) -> None:
        return  # suppress default stderr logging

    def _twiml(self, message: str, status: int = 200) -> None:
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f"<Response><Message>{message}</Message></Response>"
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _empty_twiml(self) -> None:
        body = b'<?xml version="1.0" encoding="UTF-8"?><Response></Response>'
        self.send_response(200)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if urllib.parse.urlparse(self.path).path == "/health":
            body = b'{"status":"ok","service":"whatsapp_webhook"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self) -> None:
        if urllib.parse.urlparse(self.path).path != "/ingest/whatsapp":
            self.send_response(404)
            self.end_headers()
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw_body = self.rfile.read(length) if length else b""

        form_data = dict(
            urllib.parse.parse_qsl(raw_body.decode("utf-8", errors="replace"))
        )
        signature   = self.headers.get("X-Twilio-Signature", "")
        webhook_url = self.__class__.webhook_url or "http://127.0.0.1/ingest/whatsapp"

        try:
            result = handle_whatsapp_webhook(form_data, signature, webhook_url)
        except Exception:
            self._empty_twiml()
            return

        if not result.get("ok") and result.get("error") == "invalid_signature":
            self.send_response(403)
            self.end_headers()
            return

        reply = result.get("reply_body") or ""
        if reply:
            self._twiml(reply)
        else:
            self._empty_twiml()


def start_whatsapp_server(
    host: str = "127.0.0.1",
    port: int = 8790,
    webhook_url: str = "",
) -> ThreadingHTTPServer:
    """Create (but do not serve_forever) a WhatsApp webhook server."""
    WhatsAppWebhookHandler.webhook_url = (
        webhook_url or f"http://{host}:{port}/ingest/whatsapp"
    )
    return ThreadingHTTPServer((host, port), WhatsAppWebhookHandler)
