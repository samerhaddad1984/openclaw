"""
src/agents/core/period_close.py — Month-end close checklist for OtoCPA.

Provides:
  - Table creation (period_close, period_close_locks)
  - Default 7-item checklist per client/period (bilingual via src.i18n)
  - Item status management (open / complete / waived)
  - Period locking (prevents further document changes)
  - PDF summary generation (requires PyMuPDF if available)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Default checklist item i18n keys (in display order)
# ---------------------------------------------------------------------------

DEFAULT_CHECKLIST_KEYS: list[str] = [
    "pc_item_all_docs_reviewed",
    "pc_item_no_unresolved_holds",
    "pc_item_gst_qst_reconciled",
    "pc_item_itc_itr_verified",
    "pc_item_je_posted_qbo",
    "pc_item_trial_balance_reviewed",
    "pc_item_manager_signoff",
]

VALID_STATUSES: frozenset[str] = frozenset({"open", "complete", "waived"})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

def ensure_period_close_tables(conn: sqlite3.Connection) -> None:
    """Create period_close and period_close_locks tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS period_close (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code      TEXT NOT NULL,
            period           TEXT NOT NULL,
            checklist_item   TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'open',
            responsible_user TEXT,
            due_date         TEXT,
            completed_by     TEXT,
            completed_at     TEXT,
            notes            TEXT,
            UNIQUE(client_code, period, checklist_item)
        );
        CREATE TABLE IF NOT EXISTS period_close_locks (
            client_code  TEXT NOT NULL,
            period       TEXT NOT NULL,
            locked_by    TEXT,
            locked_at    TEXT,
            PRIMARY KEY (client_code, period)
        );
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Checklist management
# ---------------------------------------------------------------------------

def get_or_create_period_checklist(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> list[sqlite3.Row]:
    """Return checklist rows for (client_code, period); create defaults if none exist."""
    ensure_period_close_tables(conn)
    existing = conn.execute(
        "SELECT * FROM period_close WHERE client_code=? AND period=? ORDER BY id",
        (client_code, period),
    ).fetchall()

    if not existing:
        for key in DEFAULT_CHECKLIST_KEYS:
            conn.execute(
                "INSERT OR IGNORE INTO period_close "
                "(client_code, period, checklist_item, status) VALUES (?, ?, ?, 'open')",
                (client_code, period, key),
            )
        conn.commit()
        existing = conn.execute(
            "SELECT * FROM period_close WHERE client_code=? AND period=? ORDER BY id",
            (client_code, period),
        ).fetchall()

    return existing


def update_checklist_item(
    conn: sqlite3.Connection,
    item_id: int,
    status: str,
    completed_by: str = "",
    notes: str = "",
    responsible_user: str = "",
    due_date: str = "",
) -> None:
    """Update a checklist item's status and metadata fields."""
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status!r}. Must be one of {sorted(VALID_STATUSES)}")

    completed_at = _utc_now_iso() if status == "complete" else None
    completed_by_val = completed_by or None if status == "complete" else None

    conn.execute(
        """
        UPDATE period_close
           SET status            = ?,
               completed_by      = ?,
               completed_at      = ?,
               notes             = ?,
               responsible_user  = COALESCE(NULLIF(?, ''), responsible_user),
               due_date          = COALESCE(NULLIF(?, ''), due_date)
         WHERE id = ?
        """,
        (
            status,
            completed_by_val,
            completed_at,
            notes or None,
            responsible_user,
            due_date,
            item_id,
        ),
    )
    conn.commit()


def is_period_complete(conn: sqlite3.Connection, client_code: str, period: str) -> bool:
    """Return True when all items are complete or waived (and at least one exists)."""
    total_row = conn.execute(
        "SELECT COUNT(*) FROM period_close WHERE client_code=? AND period=?",
        (client_code, period),
    ).fetchone()
    if not total_row or total_row[0] == 0:
        return False

    open_row = conn.execute(
        "SELECT COUNT(*) FROM period_close WHERE client_code=? AND period=? AND status='open'",
        (client_code, period),
    ).fetchone()
    return open_row[0] == 0


# ---------------------------------------------------------------------------
# Period locking
# ---------------------------------------------------------------------------

def is_period_locked(conn: sqlite3.Connection, client_code: str, period: str) -> bool:
    """Return True if the period has been locked."""
    ensure_period_close_tables(conn)
    row = conn.execute(
        "SELECT 1 FROM period_close_locks WHERE client_code=? AND period=?",
        (client_code, period),
    ).fetchone()
    return row is not None


def lock_period(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    locked_by: str,
) -> None:
    """Lock a period, preventing further document changes for that period."""
    ensure_period_close_tables(conn)
    conn.execute(
        "INSERT OR REPLACE INTO period_close_locks "
        "(client_code, period, locked_by, locked_at) VALUES (?, ?, ?, ?)",
        (client_code, period, locked_by, _utc_now_iso()),
    )
    conn.commit()


def get_lock_info(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> dict[str, Any] | None:
    """Return the lock record as a dict, or None if the period is not locked."""
    ensure_period_close_tables(conn)
    row = conn.execute(
        "SELECT * FROM period_close_locks WHERE client_code=? AND period=?",
        (client_code, period),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Document period helper
# ---------------------------------------------------------------------------

def get_document_period(document_date: str | None) -> str:
    """Extract the YYYY-MM period from a date string (ISO or YYYY-MM-DD), or ''."""
    if not document_date:
        return ""
    s = str(document_date).strip()
    if len(s) >= 7 and s[4] == "-":
        return s[:7]
    return ""


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

def generate_period_close_pdf(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    lang: str = "fr",
) -> bytes:
    """Generate and return a PDF summary of the period close checklist."""
    from src.i18n import t  # imported here to avoid circular imports at module level

    items = conn.execute(
        "SELECT * FROM period_close WHERE client_code=? AND period=? ORDER BY id",
        (client_code, period),
    ).fetchall()
    lock = get_lock_info(conn, client_code, period)

    try:
        import fitz  # PyMuPDF
        return _pdf_pymupdf(client_code, period, list(items), lock, lang, t)
    except ImportError:
        return _pdf_minimal(client_code, period, list(items), lock, lang, t)


def _pdf_pymupdf(
    client_code: str,
    period: str,
    items: list,
    lock: dict[str, Any] | None,
    lang: str,
    t: Any,
) -> bytes:
    import fitz  # type: ignore[import]

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)  # A4

    y = 60

    # Title
    page.insert_text(
        (50, y), t("pc_title", lang),
        fontsize=16, fontname="hebo", color=(0.08, 0.16, 0.44),
    )
    y += 26

    # Client / period line
    page.insert_text(
        (50, y),
        f"{t('doc_field_client', lang)}: {client_code}    {t('pc_period', lang)}: {period}",
        fontsize=11,
    )
    y += 18

    # Lock info
    if lock:
        page.insert_text(
            (50, y),
            f"{t('pc_locked_by', lang)}: {lock.get('locked_by') or ''}    "
            f"{t('pc_locked_at', lang)}: {lock.get('locked_at') or ''}",
            fontsize=10, color=(0.1, 0.45, 0.1),
        )
        y += 16

    y += 8
    page.draw_line((50, y), (545, y), color=(0.75, 0.75, 0.75))
    y += 16

    status_colors: dict[str, tuple] = {
        "complete": (0.1, 0.5, 0.1),
        "waived":   (0.5, 0.45, 0.0),
        "open":     (0.7, 0.15, 0.15),
    }

    for item in items:
        if y > 800:
            break
        status = item["status"] or "open"
        color = status_colors.get(status, (0.4, 0.4, 0.4))
        label = t(item["checklist_item"], lang)
        status_lbl = t(f"pc_status_{status}", lang)

        # Status badge and label on the same baseline
        page.insert_text((50, y), f"[{status_lbl}]", fontsize=10, color=color)
        page.insert_text((125, y), label, fontsize=10)
        y += 16

        if status == "complete" and item["completed_by"]:
            page.insert_text(
                (125, y),
                f"{t('pc_completed_by', lang)}: {item['completed_by']}  "
                f"{t('pc_completed_at', lang)}: {item['completed_at'] or ''}",
                fontsize=9, color=(0.4, 0.4, 0.4),
            )
            y += 13

        if item["responsible_user"]:
            page.insert_text(
                (125, y),
                f"{t('pc_responsible', lang)}: {item['responsible_user']}",
                fontsize=9, color=(0.4, 0.4, 0.4),
            )
            y += 13

        if item["notes"]:
            page.insert_text(
                (125, y),
                f"{t('col_note', lang)}: {item['notes']}",
                fontsize=9, color=(0.4, 0.4, 0.4),
            )
            y += 13

        y += 4  # spacing between items

    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


def _pdf_minimal(
    client_code: str,
    period: str,
    items: list,
    lock: dict[str, Any] | None,
    lang: str,
    t: Any,
) -> bytes:
    """Minimal PDF fallback using raw PDF syntax (no external library)."""
    lines: list[str] = [
        t("pc_title", lang),
        "",
        f"{t('doc_field_client', lang)}: {client_code}    {t('pc_period', lang)}: {period}",
    ]
    if lock:
        lines.append(
            f"{t('pc_locked_by', lang)}: {lock.get('locked_by') or ''}    "
            f"{lock.get('locked_at') or ''}"
        )
    lines.append("")

    for item in items:
        status = item["status"] or "open"
        label = t(item["checklist_item"], lang)
        lines.append(f"[{t(f'pc_status_{status}', lang)}]  {label}")
        if status == "complete" and item["completed_by"]:
            lines.append(
                f"    {t('pc_completed_by', lang)}: {item['completed_by']}  "
                f"{item['completed_at'] or ''}"
            )
        if item["responsible_user"]:
            lines.append(f"    {t('pc_responsible', lang)}: {item['responsible_user']}")
        if item["notes"]:
            lines.append(f"    {t('col_note', lang)}: {item['notes']}")

    def _enc(s: str) -> bytes:
        b = s.encode("latin-1", errors="replace")
        return b.replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")

    # Build content stream
    cmds: list[bytes] = [b"BT", b"/F1 10 Tf"]
    y_pos = 770
    for line in lines:
        if y_pos < 40:
            break
        cmds.append(f"50 {y_pos} Td".encode())
        cmds.append(b"0 0 Td")
        cmds.append(b"(" + _enc(line) + b") Tj")
        cmds.append(b"0 -14 Td")
        y_pos -= 14
    cmds.append(b"ET")
    content = b"\n".join(cmds)

    # PDF objects
    catalog = b"1 0 obj\n<</Type /Catalog /Pages 2 0 R>>\nendobj\n"
    pages   = b"2 0 obj\n<</Type /Pages /Kids [3 0 R] /Count 1>>\nendobj\n"
    page_   = (
        b"3 0 obj\n<</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
        b" /Contents 5 0 R /Resources <</Font <</F1 4 0 R>>>>>>\nendobj\n"
    )
    font    = b"4 0 obj\n<</Type /Font /Subtype /Type1 /BaseFont /Helvetica>>\nendobj\n"
    stream  = (
        b"5 0 obj\n<</Length " + str(len(content)).encode() + b">>\nstream\n"
        + content + b"\nendstream\nendobj\n"
    )

    header = b"%PDF-1.4\n"
    objs = [catalog, pages, page_, font, stream]
    body = b"".join(objs)

    offsets: list[int] = []
    pos = len(header)
    for obj in objs:
        offsets.append(pos)
        pos += len(obj)

    xref_pos = len(header) + len(body)
    xref = f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \r\n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \r\n"
    trailer = f"trailer\n<</Size {len(objs) + 1} /Root 1 0 R>>\nstartxref\n{xref_pos}\n%%EOF\n"

    return header + body + xref.encode() + trailer.encode()
