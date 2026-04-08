"""
client_comms.py — Client communication drafting and sending for OtoCPA.

Provides:
  ensure_comms_table(conn)  — idempotent schema bootstrap
  draft_message(...)        — call AI router to generate a plain-language draft
  save_draft(...)           — persist a draft to client_communications (unsent)
  update_draft(...)         — replace the message text of an unsent draft
  send_comm(...)            — send via SMTP and mark sent_at / read_at
  get_document_comms(...)   — fetch all comms for one document
  get_client_comms(...)     — fetch all comms for a client, newest first
  get_all_comms(...)        — fetch all comms across all clients (max 1 000)
  get_unread_count(conn)    — count of messages where read_at IS NULL
  mark_read(conn, comm_id)  — set read_at = now on one message (idempotent)
  mark_all_read(conn)       — mark every unread message as read
"""
from __future__ import annotations

import json
import smtplib
import sqlite3
import uuid
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
CONFIG_PATH = ROOT_DIR / "otocpa.config.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_smtp_config() -> dict[str, Any]:
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return cfg.get("digest", {})
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_COMMS = """
CREATE TABLE IF NOT EXISTS client_communications (
    comm_id     TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    client_code TEXT NOT NULL,
    direction   TEXT NOT NULL DEFAULT 'outbound',
    message     TEXT NOT NULL,
    sent_at     TEXT,
    sent_by     TEXT,
    read_at     TEXT
)"""

_COMMS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_comms_document ON client_communications(document_id)",
    "CREATE INDEX IF NOT EXISTS idx_comms_client   ON client_communications(client_code)",
]


def ensure_comms_table(conn: sqlite3.Connection) -> None:
    """Create client_communications table and indexes if they do not exist."""
    conn.execute(_CREATE_COMMS)
    for idx in _COMMS_INDEXES:
        conn.execute(idx)
    conn.commit()


# ---------------------------------------------------------------------------
# AI draft generation
# ---------------------------------------------------------------------------

def draft_message(
    *,
    document_id: str,
    vendor: str,
    amount: str | None,
    client_code: str,
    lang: str = "fr",
    username: str | None = None,
) -> str:
    """Call the AI router to generate a plain-language client message draft.

    Returns the model text on success, or a hardcoded fallback when AI is
    unavailable (no API key configured, provider unreachable, etc.).
    """
    from src.agents.core import ai_router  # lazy — avoids circular imports at module level

    lang_instruction = (
        "Réponds uniquement en français."
        if lang == "fr"
        else "Respond only in English."
    )
    amount_part = f" for {amount}" if amount else ""
    prompt = (
        f"You are an accounting assistant at a CPA firm. "
        f"A document from vendor '{vendor}'{amount_part} "
        f"(client: {client_code}, document ID: {document_id}) has been placed "
        f"on hold because we need additional information from the client.\n\n"
        f"Write a short, polite, plain-language email body addressed to the client "
        f"explaining that we received their document and asking them to confirm "
        f"that the expense was for a business purpose, or to provide any missing "
        f"details.\n\n"
        f"{lang_instruction}\n"
        f"Do NOT include a subject line, salutation header, or signature block — "
        f"only the body paragraphs (2 to 3 sentences). "
        f"Do NOT use placeholders like [Client Name]. "
        f"Keep the tone warm and professional."
    )

    result = ai_router.call(
        "draft_client_message",
        prompt,
        username=username,
        document_id=document_id,
    )

    if result.get("error") or not result.get("result"):
        # Fallback when AI is unavailable
        if lang == "fr":
            return (
                f"Bonjour, nous avons reçu votre document de {vendor}"
                + (f" ({amount})" if amount else "")
                + ". Pouvez-vous confirmer que cette dépense était à des fins"
                + " professionnelles? Merci de nous revenir à votre convenance."
            )
        return (
            f"Hello, we received your document from {vendor}"
            + (f" ({amount})" if amount else "")
            + ". Could you please confirm that this expense was for a business"
            + " purpose? Thank you for your prompt attention."
        )

    return result["result"].strip()


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def save_draft(
    conn: sqlite3.Connection,
    *,
    document_id: str,
    client_code: str,
    message: str,
    sent_by: str,
    direction: str = "outbound",
) -> str:
    """Insert an unsent draft into client_communications. Returns comm_id."""
    comm_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO client_communications
            (comm_id, document_id, client_code, direction, message, sent_by)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (comm_id, document_id, client_code, direction, message, sent_by),
    )
    conn.commit()
    return comm_id


def update_draft(conn: sqlite3.Connection, comm_id: str, message: str) -> None:
    """Replace the message text of an existing unsent draft (no-op if already sent)."""
    conn.execute(
        "UPDATE client_communications SET message = ? WHERE comm_id = ? AND sent_at IS NULL",
        (message, comm_id),
    )
    conn.commit()


def send_comm(
    conn: sqlite3.Connection,
    *,
    comm_id: str,
    to_email: str,
    subject: str = "",
) -> None:
    """Send the message via SMTP and mark it as sent + read.

    Parameters
    ----------
    conn:       Active DB connection (used to read draft and write timestamps).
    comm_id:    UUID of the draft communication.
    to_email:   Recipient email address supplied by the accountant.
    subject:    Optional email subject line (defaults to "OtoCPA — <client>").

    Raises
    ------
    ValueError      If comm_id is not found or the message was already sent.
    RuntimeError    If SMTP is not configured or the send operation fails.
    """
    row = conn.execute(
        "SELECT * FROM client_communications WHERE comm_id = ?", (comm_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Communication {comm_id!r} not found")
    if row["sent_at"]:
        raise ValueError("This message has already been sent")

    smtp_cfg = _load_smtp_config()
    smtp_host = smtp_cfg.get("smtp_host", "").strip()
    smtp_port = int(smtp_cfg.get("smtp_port") or 587)
    smtp_user = smtp_cfg.get("smtp_user", "").strip()
    smtp_password = smtp_cfg.get("smtp_password", "").strip()
    from_address = (smtp_cfg.get("from_address") or smtp_user).strip()
    from_name = smtp_cfg.get("from_name", "OtoCPA")

    if not smtp_host or not smtp_user:
        raise RuntimeError(
            "SMTP is not configured in otocpa.config.json (digest section)"
        )

    if not subject:
        subject = f"OtoCPA — {row['client_code']}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_address}>"
    msg["To"] = to_email
    msg.attach(MIMEText(row["message"], "plain", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_address, [to_email], msg.as_bytes())
    except Exception as exc:
        raise RuntimeError(f"SMTP send failed: {exc}") from exc

    now = _utc_now_iso()
    conn.execute(
        "UPDATE client_communications SET sent_at = ?, read_at = ? WHERE comm_id = ?",
        (now, now, comm_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_document_comms(
    conn: sqlite3.Connection, document_id: str
) -> list[dict[str, Any]]:
    """Return all communications for a document, newest first."""
    rows = conn.execute(
        """
        SELECT * FROM client_communications
        WHERE document_id = ?
        ORDER BY COALESCE(sent_at, '') DESC, comm_id DESC
        """,
        (document_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_client_comms(
    conn: sqlite3.Connection, client_code: str
) -> list[dict[str, Any]]:
    """Return all communications for a client, newest first, with doc context."""
    rows = conn.execute(
        """
        SELECT cc.*, d.file_name, d.vendor, d.amount
        FROM client_communications cc
        LEFT JOIN documents d ON d.document_id = cc.document_id
        WHERE cc.client_code = ?
        ORDER BY COALESCE(cc.sent_at, '') DESC, cc.comm_id DESC
        """,
        (client_code,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_comms(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return all communications across all clients, newest first (max 1 000 rows)."""
    rows = conn.execute(
        """
        SELECT cc.*, d.file_name, d.vendor, d.amount
        FROM client_communications cc
        LEFT JOIN documents d ON d.document_id = cc.document_id
        ORDER BY COALESCE(cc.sent_at, '') DESC, cc.comm_id DESC
        LIMIT 1000
        """,
    ).fetchall()
    return [dict(r) for r in rows]


def get_unread_count(conn: sqlite3.Connection) -> int:
    """Count communications where read_at IS NULL."""
    row = conn.execute(
        "SELECT COUNT(*) FROM client_communications WHERE read_at IS NULL"
    ).fetchone()
    return int(row[0]) if row else 0


def mark_read(conn: sqlite3.Connection, comm_id: str) -> None:
    """Set read_at = now for one communication (idempotent)."""
    conn.execute(
        "UPDATE client_communications SET read_at = ? WHERE comm_id = ? AND read_at IS NULL",
        (_utc_now_iso(), comm_id),
    )
    conn.commit()


def mark_all_read(conn: sqlite3.Connection) -> None:
    """Mark every unread communication as read."""
    conn.execute(
        "UPDATE client_communications SET read_at = ? WHERE read_at IS NULL",
        (_utc_now_iso(),),
    )
    conn.commit()
