from __future__ import annotations

"""
OtoCPA — Daily Email Digest
====================================
Sends a bilingual (FR/EN) morning summary email to owner and manager users.

Run manually:
    python scripts/daily_digest.py

Schedule via Windows Task Scheduler (runs at 7:30 AM daily):
    schtasks /create /tn "OtoCPA Daily Digest" /tr "python C:\\path\\scripts\\daily_digest.py" /sc daily /st 07:30

Schedule via cron (macOS/Linux):
    30 7 * * 1-5 /usr/bin/python3 /path/scripts/daily_digest.py

SMTP config comes from otocpa.config.json:
    {
      "digest": {
        "enabled": true,
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": "your@email.com",
        "smtp_password": "your-app-password",
        "from_address": "otocpa@yourfirm.com",
        "from_name": "OtoCPA"
      }
    }
"""

import json
import smtplib
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

ROOT_DIR    = Path(__file__).resolve().parent.parent
DB_PATH     = ROOT_DIR / "data" / "otocpa_agent.db"
CONFIG_PATH = ROOT_DIR / "otocpa.config.json"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def get_digest_config(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("digest", {})


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def normalize_text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def get_queue_summary() -> dict[str, Any]:
    """Pull all the numbers the digest needs in one DB round-trip."""
    with open_db() as conn:
        # Count by accounting status
        docs = conn.execute("""
            SELECT
                d.review_status,
                d.manual_hold_reason,
                d.updated_at,
                d.created_at,
                pj.posting_status,
                pj.approval_state,
                pj.external_id
            FROM documents d
            LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
                AND pj.rowid = (
                    SELECT pj2.rowid FROM posting_jobs pj2
                    WHERE pj2.document_id = d.document_id
                    ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC, pj2.rowid DESC
                    LIMIT 1
                )
            WHERE d.review_status != 'Ignored' OR d.review_status IS NULL
        """).fetchall()

    now        = datetime.now(timezone.utc)
    five_days_ago = (now - timedelta(days=5)).isoformat()

    counts = {
        "needs_review": 0,
        "on_hold":      0,
        "ready_to_post": 0,
        "posted_today": 0,
        "stale":        0,      # waiting > 5 business days
        "total_active": 0,
    }

    stale_items: list[dict[str, str]] = []

    for row in docs:
        status = _accounting_status(row)
        if status == "Posted":
            # Count only posted today
            updated = normalize_text(row["updated_at"] or row["created_at"])
            if updated[:10] == now.strftime("%Y-%m-%d"):
                counts["posted_today"] += 1
            continue

        counts["total_active"] += 1

        if status == "Needs Review":
            counts["needs_review"] += 1
        elif status == "On Hold":
            counts["on_hold"] += 1
        elif status == "Ready to Post":
            counts["ready_to_post"] += 1

        # Stale check
        last_activity = normalize_text(row["updated_at"] or row["created_at"])
        if last_activity and last_activity < five_days_ago:
            counts["stale"] += 1

    return counts


def _accounting_status(row: sqlite3.Row) -> str:
    rs  = normalize_text(row["review_status"])
    ps  = normalize_text(row["posting_status"])
    aas = normalize_text(row["approval_state"])
    eid = normalize_text(row["external_id"])
    hr  = normalize_text(row["manual_hold_reason"])

    if rs == "Ignored":                                                return "Ignored"
    if eid or ps == "posted":                                          return "Posted"
    if hr or aas == "pending_human_approval":                          return "On Hold"
    if ps == "ready_to_post" or aas == "approved_for_posting":        return "Ready to Post"
    if rs in {"NeedsReview", "Exception"}:                            return "Needs Review"
    if rs == "Ready":                                                  return "Ready"
    return rs or "New"


def get_recipients() -> list[dict[str, str]]:
    """Return owner and manager users who have email digest enabled."""
    with open_db() as conn:
        # Try to get email from dashboard_users; fall back to username if it looks like an email
        try:
            rows = conn.execute(
                """
                SELECT username, display_name, language,
                       COALESCE(email, '') AS email
                FROM dashboard_users
                WHERE role IN ('owner','manager') AND active=1
                """
            ).fetchall()
        except sqlite3.OperationalError:
            # email column may not exist yet
            rows = conn.execute(
                """
                SELECT username, display_name, language
                FROM dashboard_users
                WHERE role IN ('owner','manager') AND active=1
                """
            ).fetchall()

    result = []
    for r in rows:
        email = normalize_text(r["email"]) if "email" in r.keys() else ""
        if not email:
            # Use username if it looks like an email
            un = normalize_text(r["username"])
            if "@" in un:
                email = un
        if email:
            result.append({
                "email":        email,
                "display_name": normalize_text(r["display_name"] or r["username"]),
                "language":     normalize_text(r["language"] or "fr"),
            })
    return result


# ---------------------------------------------------------------------------
# Filing deadline helpers
# ---------------------------------------------------------------------------

def get_filing_deadlines_14_days() -> list[dict[str, Any]]:
    """Return all client filing deadlines in the next 14 days (unfiled only)."""
    try:
        from src.agents.core.filing_calendar import get_upcoming_deadlines
        from src.engines.tax_engine import generate_filing_summary
        from src.agents.core.filing_calendar import period_label_to_dates
    except Exception:
        return []

    today = date.today()
    try:
        conn = open_db()
        all_deadlines = get_upcoming_deadlines(conn, as_of=today, days_ahead=14)
        conn.close()
    except Exception:
        return []

    results: list[dict[str, Any]] = []
    for d in all_deadlines:
        if d["is_filed"]:
            continue
        # Get pending docs count and GST/QST amounts
        try:
            period_start, period_end = period_label_to_dates(
                d["period_label"], d.get("fiscal_year_end", "12-31")
            )
            summary = generate_filing_summary(
                d["client_code"], period_start, period_end, DB_PATH
            )
            docs_pending = summary.get("documents_pending", 0)
            gst_amount   = float(summary.get("itc_available", 0))
            qst_amount   = float(summary.get("itr_available", 0))
        except Exception:
            docs_pending = 0
            gst_amount   = 0.0
            qst_amount   = 0.0

        results.append({
            "client_code":  d["client_code"],
            "period_label": d["period_label"],
            "deadline":     d["deadline"],
            "days_until":   d["days_until"],
            "docs_pending": docs_pending,
            "gst_amount":   gst_amount,
            "qst_amount":   qst_amount,
        })

    results.sort(key=lambda x: x["deadline"])
    return results


# ---------------------------------------------------------------------------
# Email content builders
# ---------------------------------------------------------------------------

SUBJECT = {
    "fr": "Résumé quotidien OtoCPA — {date}",
    "en": "OtoCPA Daily Summary — {date}",
}

def _pluralize_fr(count: int, singular: str, plural: str) -> str:
    return singular if count == 1 else plural

def build_plain_text(
    summary: dict[str, Any],
    lang: str,
    recipient_name: str,
    filing_deadlines: list[dict[str, Any]] | None = None,
) -> str:
    today      = date.today().strftime("%d %B %Y")
    nr         = summary["needs_review"]
    oh         = summary["on_hold"]
    rtp        = summary["ready_to_post"]
    pt         = summary["posted_today"]
    stale      = summary["stale"]
    total      = summary["total_active"]

    filing_deadlines = filing_deadlines or []

    if lang == "fr":
        lines = [
            f"Bonjour {recipient_name},",
            "",
            f"Voici votre résumé OtoCPA pour le {today}.",
            "",
            "══════════════════════════════",
            "   TABLEAU DE BORD DU JOUR",
            "══════════════════════════════",
            "",
            f"  En attente de révision  : {nr}",
            f"  En attente              : {oh}",
            f"  Prêts à publier         : {rtp}",
            f"  Publiés aujourd'hui     : {pt}",
            "",
        ]
        if stale > 0:
            lines += [
                "──────────────────────────────",
                f"  ⚠  {stale} document(s) en attente depuis plus de 5 jours ouvrables.",
                "     Veuillez les examiner dès que possible.",
                "──────────────────────────────",
                "",
            ]
        if nr == 0 and oh == 0 and rtp == 0:
            lines.append("  ✓  Tout est à jour. Bonne journée !")
        else:
            lines.append(f"  Total de documents actifs : {total}")

        if filing_deadlines:
            lines += [
                "",
                "──────────────────────────────",
                "  ÉCHÉANCES À VENIR (14 PROCHAINS JOURS)",
                "──────────────────────────────",
                "",
            ]
            for fd in filing_deadlines:
                lines.append(
                    f"  {fd['client_code']:12s}  Période {fd['period_label']:10s}"
                    f"  Échéance {fd['deadline']}  ({fd['days_until']}j)"
                    f"  Docs en attente: {fd['docs_pending']}"
                    f"  TPS: ${fd['gst_amount']:,.2f}  TVQ: ${fd['qst_amount']:,.2f}"
                )
            lines.append("")

        lines += [
            "",
            "Accédez au tableau de bord : http://127.0.0.1:8787/",
            "",
            "──────────────────────────────",
            "OtoCPA — Plateforme de révision comptable",
            "Ce courriel est envoyé automatiquement. Ne pas répondre.",
        ]
    else:
        lines = [
            f"Hello {recipient_name},",
            "",
            f"Here is your OtoCPA summary for {today}.",
            "",
            "══════════════════════════════",
            "   TODAY'S DASHBOARD",
            "══════════════════════════════",
            "",
            f"  Needs Review   : {nr}",
            f"  On Hold        : {oh}",
            f"  Ready to Post  : {rtp}",
            f"  Posted Today   : {pt}",
            "",
        ]
        if stale > 0:
            lines += [
                "──────────────────────────────",
                f"  ⚠  {stale} document(s) have been waiting more than 5 business days.",
                "     Please review them as soon as possible.",
                "──────────────────────────────",
                "",
            ]
        if nr == 0 and oh == 0 and rtp == 0:
            lines.append("  ✓  Everything is up to date. Have a great day!")
        else:
            lines.append(f"  Total active documents : {total}")

        if filing_deadlines:
            lines += [
                "",
                "──────────────────────────────",
                "  UPCOMING FILING DEADLINES (NEXT 14 DAYS)",
                "──────────────────────────────",
                "",
            ]
            for fd in filing_deadlines:
                lines.append(
                    f"  {fd['client_code']:12s}  Period {fd['period_label']:10s}"
                    f"  Due {fd['deadline']}  ({fd['days_until']}d)"
                    f"  Pending docs: {fd['docs_pending']}"
                    f"  GST: ${fd['gst_amount']:,.2f}  QST: ${fd['qst_amount']:,.2f}"
                )
            lines.append("")

        lines += [
            "",
            "Open dashboard: http://127.0.0.1:8787/",
            "",
            "──────────────────────────────",
            "OtoCPA — Accounting Review Platform",
            "This email is sent automatically. Please do not reply.",
        ]

    return "\n".join(lines)


def build_html_body(
    summary: dict[str, Any],
    lang: str,
    recipient_name: str,
    filing_deadlines: list[dict[str, Any]] | None = None,
) -> str:
    today = date.today().strftime("%d %B %Y")
    nr    = summary["needs_review"]
    oh    = summary["on_hold"]
    rtp   = summary["ready_to_post"]
    pt    = summary["posted_today"]
    stale = summary["stale"]
    total = summary["total_active"]

    if lang == "fr":
        greeting    = f"Bonjour {recipient_name},"
        subtitle    = f"Votre résumé OtoCPA pour le {today}"
        labels      = ["En attente de révision", "En attente", "Prêts à publier", "Publiés aujourd'hui"]
        all_clear   = "✓ Tout est à jour. Bonne journée !"
        active_lbl  = "Documents actifs"
        stale_msg   = f"⚠ {stale} document(s) en attente depuis plus de 5 jours ouvrables."
        open_lbl    = "Ouvrir le tableau de bord"
        footer_note = "Ce courriel est envoyé automatiquement. Ne pas répondre."
    else:
        greeting    = f"Hello {recipient_name},"
        subtitle    = f"Your OtoCPA summary for {today}"
        labels      = ["Needs Review", "On Hold", "Ready to Post", "Posted Today"]
        all_clear   = "✓ Everything is up to date. Have a great day!"
        active_lbl  = "Active documents"
        stale_msg   = f"⚠ {stale} document(s) have been waiting more than 5 business days."
        open_lbl    = "Open Dashboard"
        footer_note = "This email is sent automatically. Please do not reply."

    values = [nr, oh, rtp, pt]
    colors = ["#d97706", "#d97706", "#059669", "#2563eb"]
    bg     = ["#fef3c7", "#fde68a", "#dcfce7", "#dbeafe"]

    stat_cells = ""
    for i, (lbl, val, col, bgc) in enumerate(zip(labels, values, colors, bg)):
        stat_cells += f"""
        <td style="padding:16px 20px;text-align:center;background:{bgc};border-radius:8px;min-width:110px;">
            <div style="font-size:32px;font-weight:700;color:{col};">{val}</div>
            <div style="font-size:12px;color:#374151;margin-top:4px;">{lbl}</div>
        </td>
        <td style="width:12px;"></td>"""

    stale_html = ""
    if stale > 0:
        stale_html = f"""
        <tr><td colspan="2" style="padding:16px 0;">
            <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:12px 16px;color:#991b1b;font-size:14px;">
                {stale_msg}
            </div>
        </td></tr>"""

    clear_html = ""
    if nr == 0 and oh == 0 and rtp == 0:
        clear_html = f"""
        <tr><td colspan="2" style="padding:16px 0;">
            <div style="background:#ecfdf5;border:1px solid #a7f3d0;border-radius:8px;padding:12px 16px;color:#065f46;font-size:14px;font-weight:600;">
                {all_clear}
            </div>
        </td></tr>"""

    # Build filing deadlines section
    filing_deadlines = filing_deadlines or []
    if filing_deadlines:
        if lang == "fr":
            fd_heading   = "Échéances à venir (14 prochains jours)"
            fd_col_client = "Client"
            fd_col_period = "Période"
            fd_col_due    = "Échéance"
            fd_col_docs   = "Docs att."
            fd_col_gst    = "TPS prête"
            fd_col_qst    = "TVQ prête"
        else:
            fd_heading   = "Upcoming Filing Deadlines (Next 14 Days)"
            fd_col_client = "Client"
            fd_col_period = "Period"
            fd_col_due    = "Due Date"
            fd_col_docs   = "Docs Pending"
            fd_col_gst    = "GST Ready"
            fd_col_qst    = "QST Ready"

        fd_rows = ""
        for fd in filing_deadlines:
            days = fd["days_until"]
            row_bg = "#fef2f2" if days < 7 else "#fffbeb" if days < 14 else "#f9fafb"
            fd_rows += (
                f'<tr style="background:{row_bg};">'
                f'<td style="padding:6px 8px;font-weight:600;">{fd["client_code"]}</td>'
                f'<td style="padding:6px 8px;">{fd["period_label"]}</td>'
                f'<td style="padding:6px 8px;">{fd["deadline"]}</td>'
                f'<td style="padding:6px 8px;text-align:right;">{fd["docs_pending"]}</td>'
                f'<td style="padding:6px 8px;text-align:right;">${fd["gst_amount"]:,.2f}</td>'
                f'<td style="padding:6px 8px;text-align:right;">${fd["qst_amount"]:,.2f}</td>'
                f'</tr>'
            )

        filing_section_html = f"""
        <div style="margin-top:24px;">
            <div style="font-size:14px;font-weight:700;color:#1f2937;margin-bottom:8px;
                        border-bottom:2px solid #e5e7eb;padding-bottom:4px;">
                {fd_heading}
            </div>
            <table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;font-size:12px;">
                <thead>
                    <tr style="background:#f3f4f6;">
                        <th style="padding:6px 8px;text-align:left;">{fd_col_client}</th>
                        <th style="padding:6px 8px;text-align:left;">{fd_col_period}</th>
                        <th style="padding:6px 8px;text-align:left;">{fd_col_due}</th>
                        <th style="padding:6px 8px;text-align:right;">{fd_col_docs}</th>
                        <th style="padding:6px 8px;text-align:right;">{fd_col_gst}</th>
                        <th style="padding:6px 8px;text-align:right;">{fd_col_qst}</th>
                    </tr>
                </thead>
                <tbody>{fd_rows}</tbody>
            </table>
        </div>"""
    else:
        filing_section_html = ""

    return f"""
    <!DOCTYPE html>
    <html lang="{lang}">
    <head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
    <body style="margin:0;padding:0;background:#f5f7fb;font-family:Arial,Helvetica,sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f7fb;padding:32px 0;">
        <tr><td align="center">
        <table width="580" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">

            <!-- Header -->
            <tr><td style="background:#1F3864;padding:20px 32px;">
                <div style="color:white;font-size:20px;font-weight:700;">OtoCPA</div>
                <div style="color:#93c5fd;font-size:13px;margin-top:2px;">{subtitle}</div>
            </td></tr>

            <!-- Body -->
            <tr><td style="padding:28px 32px;">
                <p style="font-size:15px;color:#111827;margin:0 0 24px;">{greeting}</p>

                <!-- Stats row -->
                <table cellpadding="0" cellspacing="0" style="width:100%;margin-bottom:24px;">
                    <tr>{stat_cells}</tr>
                </table>

                <!-- Active total -->
                <table cellpadding="0" cellspacing="0" style="width:100%;">
                    <tr>
                        <td style="font-size:13px;color:#6b7280;padding:4px 0;">{active_lbl}</td>
                        <td style="font-size:13px;color:#374151;font-weight:600;text-align:right;padding:4px 0;">{total}</td>
                    </tr>
                    {stale_html}
                    {clear_html}
                </table>

                {filing_section_html}

                <!-- CTA -->
                <div style="text-align:center;margin-top:28px;">
                    <a href="http://127.0.0.1:8787/"
                       style="background:#2E5FA3;color:white;text-decoration:none;padding:12px 28px;border-radius:8px;font-size:15px;font-weight:700;display:inline-block;">
                       {open_lbl}
                    </a>
                </div>
            </td></tr>

            <!-- Footer -->
            <tr><td style="background:#f9fafb;padding:16px 32px;border-top:1px solid #e5e7eb;">
                <p style="font-size:11px;color:#9ca3af;margin:0;text-align:center;">{footer_note}</p>
            </td></tr>

        </table>
        </td></tr>
    </table>
    </body></html>"""


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

def send_digest(config: dict[str, Any]) -> dict[str, Any]:
    dc = get_digest_config(config)

    if not dc.get("enabled", False):
        return {"status": "disabled", "sent": 0}

    smtp_host = dc.get("smtp_host", "")
    smtp_port = int(dc.get("smtp_port", 587))
    smtp_user = dc.get("smtp_user", "")
    smtp_pass = dc.get("smtp_password", "")
    from_addr = dc.get("from_address", smtp_user)
    from_name = dc.get("from_name", "OtoCPA")

    if not smtp_host or not smtp_user:
        return {"status": "error", "error": "SMTP not configured in otocpa.config.json"}

    summary          = get_queue_summary()
    recipients       = get_recipients()
    filing_deadlines = get_filing_deadlines_14_days()

    if not recipients:
        return {"status": "ok", "sent": 0, "note": "No recipients with email addresses found"}

    sent  = 0
    errors: list[str] = []
    today_str = date.today().strftime("%Y-%m-%d")

    for rec in recipients:
        lang   = rec.get("language", "fr")
        name   = rec.get("display_name", "")
        to_email = rec["email"]

        subject   = SUBJECT[lang].format(date=today_str)
        plain     = build_plain_text(summary, lang, name, filing_deadlines=filing_deadlines)
        html_body = build_html_body(summary, lang, name, filing_deadlines=filing_deadlines)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{from_name} <{from_addr}>"
        msg["To"]      = to_email

        msg.attach(MIMEText(plain, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as s:
                s.ehlo()
                s.starttls()
                s.login(smtp_user, smtp_pass)
                s.sendmail(from_addr, [to_email], msg.as_string())
            sent += 1
            print(f"  ✓ Sent to {to_email} ({lang})")
        except Exception as exc:
            errors.append(f"{to_email}: {exc}")
            print(f"  ✗ Failed {to_email}: {exc}")

    return {
        "status":    "ok" if not errors else "partial",
        "sent":      sent,
        "failed":    len(errors),
        "errors":    errors,
        "summary":   summary,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> int:
    import sys
    config = load_config()

    print()
    print("OTOCPA DAILY DIGEST")
    print("=" * 50)
    print(f"Database : {DB_PATH}")
    print(f"Config   : {CONFIG_PATH}")
    print()

    dc = get_digest_config(config)
    if not dc.get("enabled", False):
        print("Digest is DISABLED in otocpa.config.json")
        print("Set 'digest.enabled' to true to enable.")
        print()
        # Show a preview anyway
        print("Preview (not sent):")
        summary          = get_queue_summary()
        filing_deadlines = get_filing_deadlines_14_days()
        preview    = build_plain_text(summary, "fr", "Aperçu",  filing_deadlines=filing_deadlines)
        print(preview)
        print()
        print("---")
        preview_en = build_plain_text(summary, "en", "Preview", filing_deadlines=filing_deadlines)
        print(preview_en)
        return 0

    print("Sending digest...")
    result = send_digest(config)
    print()
    print(f"Status  : {result['status']}")
    print(f"Sent    : {result.get('sent', 0)}")
    if result.get("errors"):
        print("Errors  :")
        for e in result["errors"]:
            print(f"  - {e}")

    return 0 if result["status"] in ("ok", "disabled") else 1


if __name__ == "__main__":
    raise SystemExit(main())
