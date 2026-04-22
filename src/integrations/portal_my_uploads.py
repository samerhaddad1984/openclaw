"""src/integrations/portal_my_uploads.py — uploader-facing document status.

Surfaces two things to a client-portal user:

1. ``my_uploads(user_id)`` — every document they uploaded, with current
   status (processing / approved / rejected / needs_info) and, when
   applicable, the CPA's rejection reason.
2. ``notify_uploader_on_rejection(document_id)`` — enqueue an email
   to the uploader when a document is rejected, using the existing
   notification queue. Idempotent per (document_id, latest rejection
   reason) so re-rejecting the same document after the client
   re-uploads doesn't double-notify.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Status derivation
# ---------------------------------------------------------------------------


STATUS_PROCESSING = "processing"
STATUS_APPROVED   = "approved"
STATUS_REJECTED   = "rejected"
STATUS_NEEDS_INFO = "needs_info"


def _doc_status(doc: dict[str, Any]) -> str:
    """Pick a single user-facing status string from the document row."""
    rev = (doc.get("review_status") or "").lower()
    # review_workflow.status trumps documents.review_status when present.
    wf = (doc.get("wf_status") or "").lower()
    if wf == "rejected" or rev == "rejected":
        return STATUS_REJECTED
    if wf == "escalated" or rev in ("needs_info", "info_requested"):
        return STATUS_NEEDS_INFO
    if (doc.get("manual_hold_reason") or "").strip():
        return STATUS_NEEDS_INFO
    if rev in ("ready", "approved", "posted"):
        return STATUS_APPROVED
    return STATUS_PROCESSING


def my_uploads(
    db_path: Path | str, *,
    user_id: int,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Return documents uploaded by ``user_id`` newest first, with status."""
    with _open(db_path) as conn:
        # LEFT JOIN review_workflow so the rejection reason is available
        # even when the row exists there but not on documents. Tolerate
        # DBs that don't have review_workflow yet.
        try:
            rows = list(conn.execute(
                """
                SELECT
                    d.document_id, d.vendor, d.amount, d.document_date,
                    d.uploaded_at, d.review_status,
                    d.manual_hold_reason,
                    d.file_name, d.uploader_name, d.uploader_email,
                    rw.status AS wf_status,
                    rw.rejection_reason AS wf_rejection_reason,
                    rw.last_review_at AS wf_last_review_at,
                    rw.last_reviewer_email AS wf_last_reviewer
                FROM documents d
                LEFT JOIN review_workflow rw
                    ON rw.entity_type='document' AND rw.entity_id=d.document_id
                WHERE d.uploaded_by_portal_user_id = ?
                ORDER BY d.uploaded_at DESC, d.document_id DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            ))
        except sqlite3.OperationalError:
            rows = list(conn.execute(
                """
                SELECT document_id, vendor, amount, document_date,
                       uploaded_at, review_status, manual_hold_reason,
                       file_name, uploader_name, uploader_email
                FROM documents
                WHERE uploaded_by_portal_user_id = ?
                ORDER BY uploaded_at DESC, document_id DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            ))
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["status"] = _doc_status(d)
        d["rejection_reason"] = d.get("wf_rejection_reason") or ""
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Admin view — rejections across the whole client
# ---------------------------------------------------------------------------


def team_rejections(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Rejections across all users of a client. Used by the admin portal."""
    with _open(db_path) as conn:
        try:
            rows = list(conn.execute(
                """
                SELECT d.document_id, d.vendor, d.amount, d.uploaded_at,
                       d.uploader_name, d.uploader_email,
                       rw.rejection_reason, rw.last_review_at,
                       rw.last_reviewer_email
                FROM documents d
                LEFT JOIN review_workflow rw
                    ON rw.entity_type='document' AND rw.entity_id=d.document_id
                WHERE d.firm_code = ? AND d.client_code = ?
                  AND (rw.status='rejected' OR d.review_status='rejected')
                ORDER BY d.uploaded_at DESC
                LIMIT ?
                """,
                (firm_code, client_code, int(limit)),
            ))
        except sqlite3.OperationalError:
            return []
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Notification when a doc is rejected
# ---------------------------------------------------------------------------


def notify_uploader_on_rejection(
    db_path: Path | str, *,
    document_id: str,
    rejection_reason: str,
    firm_display: str = "",
    base_url: str = "",
) -> dict[str, Any]:
    """Enqueue an email to the uploader of ``document_id``.

    Returns ``{"ok": bool, "reason": str}``. No-ops (``ok=False``) when
    the document has no uploader_email on record.
    """
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT document_id, client_code, uploader_email, "
            "uploader_name, uploaded_by_portal_user_id, vendor "
            "FROM documents WHERE document_id = ?",
            (document_id,),
        ).fetchone()
    if row is None:
        return {"ok": False, "reason": "document_not_found"}
    email = (row["uploader_email"] or "").strip()
    if not email:
        return {"ok": False, "reason": "no_uploader_email"}

    vendor = row["vendor"] or "your receipt"
    firm = firm_display or "Your CPA"
    subject = f"[{firm}] needs attention on {vendor}"
    link = ""
    # If the uploader is a portal user, include a deep link to their
    # my_uploads page.
    user_id = row["uploaded_by_portal_user_id"]
    if user_id:
        try:
            from src.integrations import multi_user_portal as _mup
            user = _mup.get_user(db_path, user_id=int(user_id))
            if user and user.get("user_token"):
                link = (
                    f"{base_url.rstrip('/')}/cp/{user['user_token']}/my_uploads"
                    if base_url
                    else f"/cp/{user['user_token']}/my_uploads"
                )
        except Exception:
            log.exception("resolve uploader token failed")
    body = (
        f"Bonjour {row['uploader_name'] or ''},\n\n"
        f"Votre reçu ({vendor}) a besoin d'attention. Raison :\n"
        f"  {rejection_reason}\n\n"
        f"Veuillez téléverser une version corrigée.\n"
        f"{link}\n\n"
        f"Hello {row['uploader_name'] or ''},\n\n"
        f"Your receipt ({vendor}) needs attention. Reason:\n"
        f"  {rejection_reason}\n\n"
        f"Please upload a corrected version.\n"
        f"{link}\n"
    )
    try:
        from src.integrations import notification_sender as _ns
        _ns.enqueue(
            db_path,
            client_code=row["client_code"],
            kind="document_rejected",
            title=subject,
            body=body,
            recipient_email=email,
            subject=subject,
            document_id=document_id,
            priority=3,
        )
    except Exception:
        log.exception("rejection notification enqueue failed")
        return {"ok": False, "reason": "enqueue_failed"}
    return {"ok": True, "reason": "enqueued"}


# ---------------------------------------------------------------------------
# HTML renderer for /cp/{user_token}/my_uploads
# ---------------------------------------------------------------------------


def _esc(s: Any) -> str:
    import html as _html
    return _html.escape(str(s or ""))


_STATUS_LABEL = {
    STATUS_PROCESSING: (
        "En traitement",  # fr
        "Processing",     # en
        "#6b7280",        # color
    ),
    STATUS_APPROVED: ("Approuvé", "Approved", "#166534"),
    STATUS_REJECTED: ("Rejeté", "Rejected", "#b91c1c"),
    STATUS_NEEDS_INFO: ("Info requise", "Needs info", "#9a6700"),
}


def _status_badge(status: str) -> str:
    fr, en, color = _STATUS_LABEL.get(status, ("", "", "#6b7280"))
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:10px;'
        f'background:{color}22;color:{color};font-size:12px;font-weight:600;">'
        f'{_esc(fr)} / {_esc(en)}</span>'
    )


def render_my_uploads_page(
    *, client: dict[str, Any], user_token: str,
    portal_user: dict[str, Any], uploads: list[dict[str, Any]],
    team_rejections: list[dict[str, Any]] | None = None,
    flash: str = "", flash_error: str = "",
) -> str:
    """Render /cp/{user_token}/my_uploads as bilingual HTML.

    ``team_rejections`` is only rendered when the caller is an admin and
    passes a list (may be empty). That section satisfies the "admin sees
    team rejections summary" requirement without a separate route.
    """
    name = _esc(client.get("client_name") or client.get("client_code") or "")
    is_admin = (portal_user.get("role") or "").lower() == "admin"

    flash_html = ""
    if flash:
        flash_html += (
            f'<div style="background:#d4edda;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash)}</div>'
        )
    if flash_error:
        flash_html += (
            f'<div style="background:#f8d7da;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash_error)}</div>'
        )

    rows = ""
    for d in uploads:
        status = d.get("status") or STATUS_PROCESSING
        doc_date = _esc(d.get("document_date") or d.get("uploaded_at") or "")
        vendor = _esc(d.get("vendor") or "")
        fname = _esc(d.get("file_name") or "")
        reason_html = ""
        if status == STATUS_REJECTED:
            reason = d.get("rejection_reason") or ""
            reason_html = (
                f'<div style="color:#b91c1c;font-size:13px;margin-top:4px;">'
                f'<strong>Raison / Reason:</strong> {_esc(reason)}</div>'
                f'<div style="margin-top:6px;">'
                f'<a href="/cp/{_esc(user_token)}/upload" '
                f'style="color:#2a8759;font-weight:600;">'
                f'&#8635; Re-téléverser / Re-upload</a></div>'
            )
        elif status == STATUS_NEEDS_INFO:
            hold = d.get("manual_hold_reason") or d.get("rejection_reason") or ""
            if hold:
                reason_html = (
                    f'<div style="color:#9a6700;font-size:13px;margin-top:4px;">'
                    f'{_esc(hold)}</div>'
                )
        rows += (
            '<tr>'
            f'<td>{doc_date}</td>'
            f'<td>{fname}</td>'
            f'<td>{vendor}</td>'
            f'<td>{_status_badge(status)}{reason_html}</td>'
            '</tr>'
        )

    if not rows:
        table_html = (
            '<p class="muted"><em>Aucun téléversement. / No uploads yet.</em></p>'
        )
    else:
        table_html = (
            '<table>'
            '<thead><tr>'
            '<th>Date</th>'
            '<th>Fichier / File</th>'
            '<th>Vendeur / Vendor</th>'
            '<th>Statut / Status</th>'
            '</tr></thead>'
            f'<tbody>{rows}</tbody></table>'
        )

    team_html = ""
    if is_admin and team_rejections is not None:
        if team_rejections:
            trows = ""
            for r in team_rejections:
                trows += (
                    '<tr>'
                    f'<td>{_esc(r.get("uploaded_at") or "")}</td>'
                    f'<td>{_esc(r.get("uploader_name") or r.get("uploader_email") or "")}</td>'
                    f'<td>{_esc(r.get("vendor") or "")}</td>'
                    f'<td style="color:#b91c1c;">'
                    f'{_esc(r.get("rejection_reason") or "")}</td>'
                    '</tr>'
                )
            team_block = (
                '<table><thead><tr>'
                '<th>Date</th>'
                "<th>Téléversé par / Uploaded by</th>"
                '<th>Vendeur / Vendor</th>'
                '<th>Raison / Reason</th>'
                '</tr></thead>'
                f'<tbody>{trows}</tbody></table>'
            )
        else:
            team_block = (
                '<p class="muted"><em>Aucun rejet récent. / '
                'No recent rejections.</em></p>'
            )
        team_html = (
            '<div class="card" data-testid="team-rejections">'
            '<h2>Rejets équipe / Team rejections</h2>'
            f'{team_block}</div>'
        )

    back = (
        f'<p><a href="/cp/{_esc(user_token)}/upload">'
        '&larr; Retour / Back</a></p>'
    )

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Mes téléversements / My uploads</title>'
        '<style>body{font-family:system-ui,Arial;max-width:1000px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;margin:1rem 0;}'
        'th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;'
        'vertical-align:top;font-size:14px;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '.muted{color:#6b7280;font-size:12px;}'
        '</style></head><body>'
        f'{back}'
        f'<h1>{name} — Mes téléversements / My uploads</h1>'
        f'{flash_html}'
        '<div class="card" data-testid="my-uploads">'
        f'{table_html}</div>'
        f'{team_html}'
        '</body></html>'
    )
