from __future__ import annotations

"""
OtoCPA — Client Portal
==============================
Run standalone on a different port (8788) or mount alongside the main dashboard.

URL: http://127.0.0.1:8788/

Clients log in with their own credentials (role='client' in dashboard_users).
They can:
  - Upload invoices / receipts / bank statements (PDF, PNG, JPG)
  - See status of their submitted documents
  - See plain-language review reasons (no GL, no posting internals)

Clients NEVER see:
  - Other clients' documents
  - GL accounts or tax codes
  - Posting status or QuickBooks data
  - Internal review notes
"""

import hashlib
import html
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import sys
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH     = ROOT_DIR / "data" / "otocpa_agent.db"
UPLOAD_DIR  = ROOT_DIR / "data" / "client_uploads"
HOST        = "127.0.0.1"
PORT        = 8788
SESSION_HOURS = 12

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Shared translation helper (JSON-backed, FR default for Quebec market).
from src.i18n import t  # noqa: E402  (import after sys.path setup)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat()

def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v), quote=True)

def urlquote(v: Any) -> str:
    return urllib.parse.quote("" if v is None else str(v), safe="")

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def normalize_text(v: Any) -> str:
    return "" if v is None else str(v).strip()


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def bootstrap_schema() -> None:
    with open_db() as conn:
        # Reuse dashboard_users — clients have role='client'
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'client',
                display_name  TEXT,
                client_code   TEXT,
                language      TEXT NOT NULL DEFAULT 'fr',
                active        INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_sessions (
                session_token TEXT PRIMARY KEY,
                username      TEXT NOT NULL,
                expires_at    TEXT NOT NULL,
                created_at    TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                document_id    TEXT PRIMARY KEY,
                file_name      TEXT,
                file_path      TEXT,
                client_code    TEXT,
                vendor         TEXT,
                doc_type       TEXT,
                amount         REAL,
                document_date  TEXT,
                gl_account     TEXT,
                tax_code       TEXT,
                category       TEXT,
                review_status  TEXT DEFAULT 'New',
                confidence     REAL,
                raw_result     TEXT,
                created_at     TEXT,
                updated_at     TEXT,
                assigned_to    TEXT,
                manual_hold_reason TEXT,
                submitted_by   TEXT,
                client_note    TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address   TEXT NOT NULL,
                username     TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                success      INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()

        # Add portal-specific columns if they don't exist yet
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        for col in ["submitted_by", "client_note"]:
            if col not in existing:
                conn.execute(f"ALTER TABLE documents ADD COLUMN {col} TEXT")
        user_cols = {r["name"] for r in conn.execute("PRAGMA table_info(dashboard_users)").fetchall()}
        for col in ["client_code", "language"]:
            if col not in user_cols:
                conn.execute(f"ALTER TABLE dashboard_users ADD COLUMN {col} TEXT")
        conn.commit()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def create_session(username: str) -> str:
    token = secrets.token_hex(32)
    expires = (utc_now() + timedelta(hours=SESSION_HOURS)).replace(microsecond=0).isoformat()
    with open_db() as conn:
        conn.execute(
            "INSERT INTO dashboard_sessions (session_token, username, expires_at, created_at) VALUES (?,?,?,?)",
            (token, username, expires, utc_now_iso()),
        )
        conn.commit()
    return token

def delete_session(token: str) -> None:
    with open_db() as conn:
        conn.execute("DELETE FROM dashboard_sessions WHERE session_token=?", (token,))
        conn.commit()

def get_session_user(handler: BaseHTTPRequestHandler) -> dict[str, Any] | None:
    cookie = handler.headers.get("Cookie", "")
    token = None
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("portal_token="):
            token = part[len("portal_token="):]
            break
    if not token:
        return None
    with open_db() as conn:
        session = conn.execute(
            "SELECT * FROM dashboard_sessions WHERE session_token=?", (token,)
        ).fetchone()
        if not session or session["expires_at"] < utc_now_iso():
            return None
        user = conn.execute(
            "SELECT * FROM dashboard_users WHERE username=? AND active=1 AND role='client'",
            (session["username"],),
        ).fetchone()
        return dict(user) if user else None

def get_token(handler: BaseHTTPRequestHandler) -> str:
    cookie = handler.headers.get("Cookie", "")
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("portal_token="):
            return part[len("portal_token="):]
    return ""

def parse_form_body(raw: bytes) -> dict[str, str]:
    parsed = urllib.parse.parse_qs(raw.decode("utf-8"), keep_blank_values=True)
    return {k: v[0] if v else "" for k, v in parsed.items()}


# ---------------------------------------------------------------------------
# Security helpers — rate limiting, IP, cookies, filename sanitization
# ---------------------------------------------------------------------------

MAX_UPLOADS_PER_DAY      = 20
_RATE_LIMIT_WINDOW_MIN   = 15
_RATE_LIMIT_MAX_FAILURES = 5
_RATE_LIMIT_MSG = (
    "Trop de tentatives. Réessayez dans 15 minutes / "
    "Too many attempts. Try again in 15 minutes."
)


def _get_client_ip(handler: BaseHTTPRequestHandler) -> str:
    """Return the real client IP, honouring CF-Connecting-IP / X-Forwarded-For."""
    for hdr in ("CF-Connecting-IP", "X-Forwarded-For"):
        val = handler.headers.get(hdr, "").split(",")[0].strip()
        if val:
            return val
    return handler.client_address[0]


def _is_https(handler: BaseHTTPRequestHandler) -> bool:
    """Detect HTTPS via the X-Forwarded-Proto header set by Cloudflare."""
    return handler.headers.get("X-Forwarded-Proto", "").lower() == "https"


def _session_cookie_attrs(handler: BaseHTTPRequestHandler) -> str:
    """Return the SameSite/Secure cookie attributes for the current context."""
    if _is_https(handler):
        return "Secure; SameSite=Strict"
    return "SameSite=Lax"


def record_login_attempt(ip: str, username: str, success: bool) -> None:
    """Record a login attempt in the login_attempts table."""
    with open_db() as conn:
        conn.execute(
            "INSERT INTO login_attempts (ip_address, username, attempted_at, success)"
            " VALUES (?,?,?,?)",
            (ip, username, utc_now_iso(), 1 if success else 0),
        )
        conn.commit()


def is_rate_limited(ip: str) -> bool:
    """Return True if this IP has ≥5 failed attempts in the last 15 minutes."""
    window_start = (
        utc_now() - timedelta(minutes=_RATE_LIMIT_WINDOW_MIN)
    ).replace(microsecond=0).isoformat()
    with open_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM login_attempts"
            " WHERE ip_address=? AND success=0 AND attempted_at>=?",
            (ip, window_start),
        ).fetchone()
    return (row[0] if row else 0) >= _RATE_LIMIT_MAX_FAILURES


def count_uploads_today(client_code: str) -> int:
    """Count uploads submitted by this client today (UTC date)."""
    today = utc_now().date().isoformat()
    with open_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE client_code=? AND DATE(created_at)=?",
            (client_code, today),
        ).fetchone()
    return row[0] if row else 0


def sanitize_filename(filename: str) -> str:
    """Strip path traversal and unsafe characters from an uploaded filename.

    Allows only: alphanumeric, dash, underscore, dot.
    Strips leading dots and all path separators.
    """
    # Remove any directory component (Windows and Unix separators)
    name = os.path.basename(filename.replace("\\", "/"))
    # Strip leading dots (avoids hidden-file tricks)
    name = name.lstrip(".")
    # Replace every character not in the safe set with underscore
    name = re.sub(r"[^A-Za-z0-9._\-]", "_", name)
    # Collapse runs of underscores and trim edges
    name = re.sub(r"_+", "_", name).strip("_")
    # Guarantee non-empty result
    if not name or set(name) <= {"."}:
        name = "document"
    return name


# ---------------------------------------------------------------------------
# Document helpers
# ---------------------------------------------------------------------------

def get_client_documents(client_code: str) -> list[sqlite3.Row]:
    with open_db() as conn:
        return conn.execute(
            """
            SELECT document_id, file_name, review_status, created_at,
                   manual_hold_reason, client_note
            FROM documents
            WHERE client_code = ?
            ORDER BY created_at DESC
            LIMIT 200
            """,
            (client_code,),
        ).fetchall()

def client_status_label(review_status: str, lang: str) -> str:
    s = normalize_text(review_status).casefold()
    if s in {"new", ""}:
        return t("status_new", lang)
    if s in {"needsreview", "needs review", "exception", "escalated"}:
        return t("status_review", lang)
    if s in {"ready", "posted"}:
        return t("status_complete", lang)
    if s in {"on hold", "hold"}:
        return t("status_hold", lang)
    return t("status_review", lang)

def client_status_css(review_status: str) -> str:
    s = normalize_text(review_status).casefold()
    if s in {"new", ""}:
        return "badge-new"
    if s in {"ready", "posted"}:
        return "badge-complete"
    if s in {"on hold", "hold"}:
        return "badge-hold"
    return "badge-review"

ALLOWED_MIME = {
    "application/pdf",
    "image/png", "image/jpeg", "image/jpg",
    "image/heic", "image/heif", "image/tiff", "image/webp",
}
MAX_BYTES = 20 * 1024 * 1024  # 20 MB

# Lazy import — ocr_engine is optional; falls back to legacy save if absent.
try:
    from src.engines.ocr_engine import process_file as _ocr_process_file
    from src.engines.ocr_engine import detect_format as _ocr_detect_format
    _HAS_OCR = True
except ImportError:
    _HAS_OCR = False
    _ocr_process_file = None   # type: ignore[assignment]
    _ocr_detect_format = None  # type: ignore[assignment]


def save_upload(
    file_bytes: bytes,
    filename: str,
    client_code: str,
    username: str,
    note: str,
) -> dict[str, Any]:
    """Save uploaded file, run OCR pipeline, create/update document record."""

    # Validate size
    if len(file_bytes) > MAX_BYTES:
        return {"ok": False, "error": "file_too_large"}

    # Validate format — prefer magic-byte detection over MIME guessing
    if _HAS_OCR and _ocr_detect_format is not None:
        detected_fmt = _ocr_detect_format(file_bytes)
        if detected_fmt == "unknown":
            return {"ok": False, "error": "invalid_type"}
    else:
        # Legacy: guess from extension / MIME
        mime, _ = mimetypes.guess_type(filename)
        if mime not in ALLOWED_MIME:
            ext = Path(filename).suffix.lower()
            if ext in {".pdf", ".png", ".jpg", ".jpeg"}:
                mime = {
                    "pdf":  "application/pdf",
                    "png":  "image/png",
                    "jpg":  "image/jpeg",
                    "jpeg": "image/jpeg",
                }[ext.lstrip(".")]
            else:
                return {"ok": False, "error": "invalid_type"}

    # Route through OCR pipeline when available
    if _HAS_OCR and _ocr_process_file is not None:
        result = _ocr_process_file(
            file_bytes,
            filename,
            client_code=client_code,
            submitted_by=username,
            client_note=normalize_text(note),
            ingest_source="portal",
            upload_dir=UPLOAD_DIR,
        )
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "upload_error")}
        return {
            "ok":          True,
            "document_id": result["document_id"],
            "file_name":   result["file_name"],
        }

    # ---- Legacy fallback (no OCR engine) --------------------------------
    safe_name = "".join(c for c in filename if c.isalnum() or c in "._- ").strip()
    if not safe_name:
        safe_name = "document"

    client_dir = UPLOAD_DIR / client_code
    client_dir.mkdir(parents=True, exist_ok=True)

    doc_id = "doc_" + secrets.token_hex(6)
    dest   = client_dir / f"{doc_id}_{safe_name}"
    dest.write_bytes(file_bytes)

    now = utc_now_iso()
    with open_db() as conn:
        conn.execute(
            """
            INSERT INTO documents
                (document_id, file_name, file_path, client_code, review_status,
                 created_at, updated_at, submitted_by, client_note)
            VALUES (?,?,?,?,'New',?,?,?,?)
            """,
            (doc_id, safe_name, str(dest), client_code, now, now, username,
             normalize_text(note) or None),
        )
        conn.commit()

    return {"ok": True, "document_id": doc_id, "file_name": safe_name}


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Arial,Helvetica,sans-serif;background:#f5f7fb;color:#111827;min-height:100vh}
header{background:#1F3864;color:white;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
header h1{font-size:18px;font-weight:600}
.header-right{display:flex;align-items:center;gap:12px;font-size:13px}
.lang-btn{background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.3);color:white;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:12px;text-decoration:none}
.logout-btn{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.2);color:white;padding:5px 12px;border-radius:6px;cursor:pointer;font-size:13px}
main{max-width:860px;margin:0 auto;padding:24px 20px 60px}
.card{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:20px 24px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,.05)}
h2{font-size:18px;font-weight:600;margin-bottom:14px;color:#1F3864}
h3{font-size:15px;font-weight:600;margin-bottom:10px;color:#374151}
.flash{padding:12px 16px;border-radius:8px;margin-bottom:18px;font-size:14px;font-weight:600}
.flash.ok{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46}
.flash.err{background:#fef2f2;border:1px solid #fecaca;color:#991b1b}
.drop-zone{border:2px dashed #d1d5db;border-radius:10px;padding:36px 20px;text-align:center;cursor:pointer;transition:border-color .2s,background .2s;margin-bottom:14px}
.drop-zone:hover,.drop-zone.drag-over{border-color:#2E5FA3;background:#EFF6FF}
.drop-zone input[type=file]{display:none}
.drop-zone p{color:#6b7280;font-size:14px;margin-top:8px}
.drop-zone .icon{font-size:32px;margin-bottom:6px}
.field{margin-bottom:14px}
label{display:block;font-size:12px;font-weight:700;color:#374151;margin-bottom:4px}
input[type=text],textarea{width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:14px;font-family:inherit}
textarea{min-height:80px;resize:vertical}
button[type=submit],.btn{background:#2E5FA3;color:white;border:0;border-radius:8px;padding:10px 22px;font-size:14px;font-weight:700;cursor:pointer;display:inline-block;text-decoration:none}
button[type=submit]:hover,.btn:hover{background:#1F3864}
table{width:100%;border-collapse:collapse;font-size:14px}
th{background:#f9fafb;padding:10px 12px;text-align:left;font-weight:600;font-size:13px;color:#374151;border-bottom:1px solid #e5e7eb}
td{padding:10px 12px;border-bottom:1px solid #f3f4f6;vertical-align:top;color:#374151}
tr:last-child td{border-bottom:none}
.badge{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700}
.badge-new{background:#dbeafe;color:#1e40af}
.badge-review{background:#fef3c7;color:#92400e}
.badge-complete{background:#dcfce7;color:#166534}
.badge-hold{background:#fde68a;color:#92400e}
.muted{color:#6b7280;font-size:13px}
.login-wrap{min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f5f7fb}
.login-box{background:white;border:1px solid #e5e7eb;border-radius:14px;padding:2.5rem 2rem;width:100%;max-width:380px;box-shadow:0 2px 12px rgba(0,0,0,.07)}
.login-box h2{text-align:center;margin-bottom:1.5rem;font-size:22px;color:#1F3864}
.login-field{margin-bottom:16px}
.login-field label{display:block;font-size:13px;font-weight:700;color:#374151;margin-bottom:5px}
.login-field input{width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:15px}
.login-btn{width:100%;padding:12px;background:#1F3864;color:white;border:0;border-radius:8px;font-size:15px;font-weight:700;cursor:pointer;margin-top:6px}
.login-lang{text-align:center;margin-top:14px;font-size:12px;color:#6b7280}
.login-lang a{color:#2E5FA3;text-decoration:none}
footer{text-align:center;padding:20px;font-size:12px;color:#9ca3af}
"""


# ---------------------------------------------------------------------------
# Page renderers
# ---------------------------------------------------------------------------

def page(title: str, body: str, user: dict | None = None, lang: str = "fr",
         flash: str = "", flash_type: str = "ok") -> str:
    flash_html = f'<div class="flash {flash_type}">{esc(flash)}</div>' if flash else ""
    user_html  = ""
    if user:
        display = esc(user.get("display_name") or user.get("username") or "")
        lang_label = t("switch_lang", lang)
        user_html = f"""
        <span>{display}</span>
        <form method="POST" action="/set_language" style="display:inline;">
            <input type="hidden" name="lang" value="{'en' if lang == 'fr' else 'fr'}">
            <button class="lang-btn" type="submit">{esc(lang_label)}</button>
        </form>
        <form method="POST" action="/logout" style="display:inline;">
            <button class="logout-btn" type="submit">{t('logout_btn', lang)}</button>
        </form>"""

    return f"""<!doctype html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<title>{esc(title)}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style>
</head>
<body>
<header>
    <h1>{t('portal_title', lang)}</h1>
    <div class="header-right">{user_html}</div>
</header>
<main>
    {flash_html}
    {body}
</main>
<footer>{t('footer_note', lang)}</footer>
</body>
</html>"""


def render_login(lang: str = "fr", error: str = "") -> str:
    err_html = f'<div class="flash err" style="margin-bottom:16px;">{esc(error)}</div>' if error else ""
    switch_lang = "en" if lang == "fr" else "fr"
    switch_label = "English" if lang == "fr" else "Français"
    return f"""<!doctype html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<title>{t('login_title', lang)} — OtoCPA</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style>
</head>
<body>
<div class="login-wrap">
    <div class="login-box">
        <h2>OtoCPA</h2>
        {err_html}
        <form method="POST" action="/login">
            <input type="hidden" name="lang" value="{lang}">
            <div class="login-field">
                <label>{t('username', lang)}</label>
                <input type="text" name="username" autofocus autocomplete="username">
            </div>
            <div class="login-field">
                <label>{t('password', lang)}</label>
                <input type="password" name="password" autocomplete="current-password">
            </div>
            <button class="login-btn" type="submit">{t('login_btn', lang)}</button>
        </form>
        <div class="login-lang">
            <a href="/login?lang={switch_lang}">{switch_label}</a>
        </div>
    </div>
</div>
</body>
</html>"""


def render_portal(user: dict, lang: str, flash: str = "", flash_type: str = "ok") -> str:
    client_code = normalize_text(user.get("client_code") or user.get("username"))
    docs = get_client_documents(client_code)

    # Upload card
    upload_card = f"""
    <div class="card">
        <h2>{t('upload_title', lang)}</h2>
        <form method="POST" action="/upload" enctype="multipart/form-data" id="upload-form">
            <div class="drop-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
                <div class="icon">&#128196;</div>
                <strong id="drop-label">{t('upload_btn', lang)}</strong>
                <p>{t('upload_hint', lang)}</p>
                <input type="file" id="file-input" name="file"
                       accept=".pdf,.png,.jpg,.jpeg"
                       onchange="handleFileSelect(this)">
            </div>
            <div class="field">
                <label>{t('note_label', lang)}</label>
                <textarea name="note" placeholder="{t('note_hint', lang)}"></textarea>
            </div>
            <button type="submit">{t('upload_btn', lang)}</button>
        </form>
    </div>"""

    # Documents table
    if docs:
        rows = ""
        for doc in docs:
            status_label = client_status_label(normalize_text(doc["review_status"]), lang)
            status_css   = client_status_css(normalize_text(doc["review_status"]))
            date_str     = normalize_text(doc["created_at"])[:10]
            note_str     = esc(doc["client_note"] or "")
            rows += f"""
            <tr>
                <td>{esc(doc["file_name"])}</td>
                <td class="muted">{esc(date_str)}</td>
                <td><span class="badge {status_css}">{esc(status_label)}</span></td>
                <td class="muted">{note_str}</td>
            </tr>"""
        docs_body = f"""
        <table>
            <thead>
                <tr>
                    <th>{t('col_file', lang)}</th>
                    <th>{t('col_date_submitted', lang)}</th>
                    <th>{t('col_status', lang)}</th>
                    <th>{t('col_note', lang)}</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>"""
    else:
        docs_body = f'<p class="muted">{t("no_documents", lang)}</p>'

    docs_card = f"""
    <div class="card">
        <h2>{t('my_documents', lang)}</h2>
        {docs_body}
    </div>"""

    # Contact card
    contact_card = f"""
    <div class="card" style="background:#f9fafb;">
        <h3>{t('contact_title', lang)}</h3>
        <p class="muted">{t('contact_body', lang)}</p>
    </div>"""

    # Drag-and-drop JS
    js = """
    <script>
    const zone = document.getElementById('drop-zone');
    const inp  = document.getElementById('file-input');
    const lbl  = document.getElementById('drop-label');
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
        e.preventDefault();
        zone.classList.remove('drag-over');
        if (e.dataTransfer.files.length) {
            inp.files = e.dataTransfer.files;
            handleFileSelect(inp);
        }
    });
    function handleFileSelect(input) {
        if (input.files && input.files[0]) {
            lbl.textContent = input.files[0].name;
        }
    }
    </script>"""

    body = upload_card + docs_card + contact_card + js
    return page(t("portal_title", lang), body, user=user, lang=lang, flash=flash, flash_type=flash_type)


# ---------------------------------------------------------------------------
# Multipart file parser (no external deps)
# ---------------------------------------------------------------------------

def parse_multipart(raw: bytes, content_type: str) -> tuple[dict[str, str], bytes, str]:
    """Returns (fields, file_bytes, filename). Simple single-file parser."""
    fields: dict[str, str] = {}
    file_bytes = b""
    filename   = ""

    boundary_str = ""
    for part in content_type.split(";"):
        part = part.strip()
        if part.startswith("boundary="):
            boundary_str = part[len("boundary="):].strip().strip('"')
            break

    if not boundary_str:
        return fields, file_bytes, filename

    boundary = ("--" + boundary_str).encode()
    parts    = raw.split(boundary)

    for chunk in parts[1:]:
        if chunk in (b"--\r\n", b"--\r\n--", b"--"):
            continue
        if chunk.startswith(b"--"):
            continue
        # Split headers from body
        if b"\r\n\r\n" not in chunk:
            continue
        header_block, body = chunk.split(b"\r\n\r\n", 1)
        # Strip trailing \r\n--
        if body.endswith(b"\r\n"):
            body = body[:-2]

        headers_raw = header_block.decode("utf-8", errors="replace")
        disp = ""
        for line in headers_raw.splitlines():
            if line.lower().startswith("content-disposition"):
                disp = line
            # detect file part
        if 'filename="' in disp:
            # extract filename
            fn_start = disp.index('filename="') + len('filename="')
            fn_end   = disp.index('"', fn_start)
            filename   = disp[fn_start:fn_end]
            file_bytes = body
        elif 'name="' in disp:
            n_start = disp.index('name="') + len('name="')
            n_end   = disp.index('"', n_start)
            name    = disp[n_start:n_end]
            fields[name] = body.decode("utf-8", errors="replace")

    return fields, file_bytes, filename


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------

class ClientPortalHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _send_html(self, content: str, status: int = 200,
                   extra: list[tuple[str, str]] | None = None) -> None:
        body = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if extra:
            for k, v in extra:
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str,
                  extra: list[tuple[str, str]] | None = None) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        if extra:
            for k, v in extra:
                self.send_header(k, v)
        self.end_headers()

    def _get_lang_from_cookie(self) -> str:
        cookie = self.headers.get("Cookie", "")
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith("portal_lang="):
                return part[len("portal_lang="):]
        return "fr"

    def do_GET(self) -> None:
        try:
            parsed = urllib.parse.urlparse(self.path)
            path   = parsed.path
            qs     = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            flash  = qs.get("flash", [""])[0]
            ferr   = qs.get("error", [""])[0]
            lang_qs = qs.get("lang", [""])[0]

            if path in ("/", "/portal", "/login"):
                lang = lang_qs or self._get_lang_from_cookie()
                user = get_session_user(self)
                if user:
                    ul = normalize_text(user.get("language") or lang)
                    self._send_html(render_portal(user, ul, flash=flash or ferr,
                                                  flash_type="ok" if flash else "err"))
                else:
                    self._send_html(render_login(lang=lang,
                                                 error=ferr))
                return

            self._send_html("<h2>Not found</h2>", status=404)

        except Exception:
            self._send_html(
                f"<pre>{html.escape(traceback.format_exc())}</pre>", status=500)

    def do_POST(self) -> None:
        try:
            path = urllib.parse.urlparse(self.path).path

            # Validate Content-Length for uploads BEFORE reading the body
            # to reject oversized requests at the HTTP level.
            if path == "/upload":
                cl = int(self.headers.get("Content-Length", "0"))
                if cl > MAX_BYTES:
                    lang = self._get_lang_from_cookie()
                    user_pre = get_session_user(self)
                    if user_pre:
                        lang = normalize_text(user_pre.get("language") or lang)
                    # Drain a small head of the body to keep the connection healthy
                    try:
                        self.rfile.read(min(cl, 65536))
                    except Exception:
                        pass
                    self._redirect(f"/?error={urlquote(t('file_too_large', lang))}")
                    return

            length = int(self.headers.get("Content-Length", "0"))
            raw    = self.rfile.read(length)
            ct     = self.headers.get("Content-Type", "")

            # --- Login ---
            if path == "/login":
                form     = parse_form_body(raw)
                lang     = normalize_text(form.get("lang") or "fr")
                username = normalize_text(form.get("username", ""))
                password = form.get("password", "")
                ip       = _get_client_ip(self)

                # Rate-limit: 5 failures per IP in 15 minutes → HTTP 429
                if is_rate_limited(ip):
                    record_login_attempt(ip, username, False)
                    body = render_login(lang=lang, error=_RATE_LIMIT_MSG).encode("utf-8")
                    self.send_response(429)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Retry-After", "900")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                with open_db() as conn:
                    user_row = conn.execute(
                        "SELECT * FROM dashboard_users"
                        " WHERE username=? AND active=1 AND role='client'",
                        (username,),
                    ).fetchone()

                # Unified error for both "user not found" and "wrong password"
                if not user_row or user_row["password_hash"] != hash_password(password):
                    record_login_attempt(ip, username, False)
                    self._send_html(render_login(lang=lang,
                                                 error=t("invalid_credentials", lang)))
                    return

                record_login_attempt(ip, username, True)
                token = create_session(username)
                ul    = normalize_text(user_row.get("language") or lang)
                sec   = _session_cookie_attrs(self)
                self._redirect("/", extra=[
                    ("Set-Cookie", f"portal_token={token}; HttpOnly; {sec}; Path=/"),
                    ("Set-Cookie", f"portal_lang={ul}; {sec}; Path=/"),
                ])
                return

            # --- Logout ---
            if path == "/logout":
                token = get_token(self)
                if token:
                    delete_session(token)
                sec = _session_cookie_attrs(self)
                self._redirect("/login", extra=[
                    ("Set-Cookie",
                     f"portal_token=; HttpOnly; {sec}; Path=/; Max-Age=0"),
                ])
                return

            # Auth required below
            user = get_session_user(self)
            if not user:
                self._redirect("/login")
                return
            lang = normalize_text(user.get("language") or "fr")

            # --- Switch language ---
            if path == "/set_language":
                form     = parse_form_body(raw)
                new_lang = normalize_text(form.get("lang", "fr"))
                if new_lang not in ("fr", "en"):
                    new_lang = "fr"
                with open_db() as conn:
                    conn.execute(
                        "UPDATE dashboard_users SET language=? WHERE username=?",
                        (new_lang, user["username"]),
                    )
                    conn.commit()
                sec = _session_cookie_attrs(self)
                self._redirect("/", extra=[
                    ("Set-Cookie", f"portal_lang={new_lang}; {sec}; Path=/"),
                ])
                return

            # --- File upload ---
            if path == "/upload":
                if "multipart/form-data" not in ct:
                    self._redirect(f"/?error={urlquote(t('upload_error', lang))}")
                    return

                client_code = normalize_text(user.get("client_code") or user.get("username"))

                # Daily upload limit: 20 uploads per client per UTC day
                if count_uploads_today(client_code) >= MAX_UPLOADS_PER_DAY:
                    self._redirect(f"/?error={urlquote(t('upload_limit_exceeded', lang))}")
                    return

                fields, file_bytes, raw_filename = parse_multipart(raw, ct)
                note     = fields.get("note", "")
                filename = sanitize_filename(raw_filename) if raw_filename else ""

                if not file_bytes or not filename:
                    self._redirect(f"/?error={urlquote(t('upload_error', lang))}")
                    return

                result = save_upload(file_bytes, filename, client_code,
                                     user["username"], note)
                if result["ok"]:
                    self._redirect(f"/?flash={urlquote(t('upload_success', lang))}")
                else:
                    err_key = result.get("error", "upload_error")
                    self._redirect(f"/?error={urlquote(t(err_key, lang))}")
                return

            self._redirect("/")

        except Exception:
            self._redirect(f"/?error={urlquote('Internal error')}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    bootstrap_schema()
    print()
    print("OTOCPA CLIENT PORTAL")
    print("=" * 60)
    print(f"Database    : {DB_PATH}")
    print(f"Upload dir  : {UPLOAD_DIR}")
    print(f"URL         : http://{HOST}:{PORT}/")
    print(f"Language    : French (default) / English")
    print()
    print("To add a client user, run:")
    print("  python scripts/manage_dashboard_users.py add-client")
    print()
    server = ThreadingHTTPServer((HOST, PORT), ClientPortalHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down client portal...")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
