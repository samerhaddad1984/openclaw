"""Bulk client CSV import.

Targets firms migrating from Caseware/Sage/Excel spreadsheets — they
paste dozens-to-hundreds of clients in one go instead of typing them
one at a time on the /clients form.

Pipeline:
  1. ``generate_template_csv()``          — download a bilingual header.
  2. ``parse_csv(file_bytes)``            — UTF-8 + BOM tolerant, returns
                                            ``(rows, headers, fatal)``.
  3. ``validate_rows(rows, firm, db)``    — per-row errors dict.
  4. ``import_rows(rows, firm, db, *,
                    dry_run=False)``      — writes clients, returns a
                                            summary dict.
  5. ``generate_error_csv(rows, errors)`` — downloadable rejection log.

The schema assumed here matches what the live ``clients`` table
already carries (see ``scripts/review_dashboard.py`` for the column
list): client_code, client_name, firm_code, contact_email,
whatsapp_number, language, primary_employee_email,
secondary_employee_email. ``fiscal_year_end`` is accepted in the CSV
(and validated as an ISO date) but not persisted yet — it is kept in
the template so firms can standardise on a full row shape ahead of a
future schema migration.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)


TEMPLATE_HEADERS: tuple[str, ...] = (
    "client_code",
    "client_name",
    "firm",                 # firm_code — ignored; always uses the
                            # caller's firm scope (validation only).
    "email",
    "phone",
    "language",
    "fiscal_year_end",
    "primary_employee_email",
    "secondary_employee_email",
)


REQUIRED_HEADERS: tuple[str, ...] = ("client_code", "client_name")


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CODE_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{1,31}$")


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------


def generate_template_csv() -> bytes:
    """Return a UTF-8 BOM-prefixed CSV with bilingual comment row.

    Excel on Windows refuses to open UTF-8 without a BOM; we prefix
    one so French accents round-trip.
    """
    # Header row + a sample comment row so firms see both languages.
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(TEMPLATE_HEADERS)
    w.writerow([
        # Example / Exemple — remove before importing
        "CONS001",
        "Construction Tremblay",
        "FIRM",
        "owner@cons.com",
        "+15145551234",
        "fr",
        "2026-12-31",
        "alice@cpafirm.com",
        "bob@cpafirm.com",
    ])
    out = "﻿" + buf.getvalue()
    return out.encode("utf-8")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_csv(
    file_bytes: bytes,
) -> tuple[list[dict[str, str]], list[str], str | None]:
    """Decode UTF-8 / UTF-8-BOM, sniff header row, return (rows, headers, fatal).

    Fatal error string is non-None when the file can't be parsed at
    all — missing required header, wrong encoding, empty body.
    """
    if not file_bytes:
        return [], [], "empty_file"
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = file_bytes.decode("cp1252")
        except UnicodeDecodeError:
            return [], [], "decode_failed"
    reader = csv.reader(io.StringIO(text))
    try:
        headers = [h.strip().lower() for h in next(reader)]
    except StopIteration:
        return [], [], "empty_file"
    for req in REQUIRED_HEADERS:
        if req not in headers:
            return [], headers, f"missing_header:{req}"
    rows: list[dict[str, str]] = []
    for raw in reader:
        if not any(cell.strip() for cell in raw):
            continue  # blank row
        padded = raw + [""] * (len(headers) - len(raw))
        row = {headers[i]: (padded[i] or "").strip() for i in range(len(headers))}
        rows.append(row)
    return rows, headers, None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _known_employee(
    conn: sqlite3.Connection, firm_code: str, email: str,
) -> bool:
    """True when ``email`` exists in the firm's ``users`` table."""
    try:
        row = conn.execute(
            "SELECT 1 FROM users "
            "WHERE LOWER(email)=LOWER(?) AND firm_code=? LIMIT 1",
            (email, firm_code),
        ).fetchone()
        if row:
            return True
    except sqlite3.OperationalError:
        pass
    # Some older rows use username=email; match that too.
    try:
        row = conn.execute(
            "SELECT 1 FROM users "
            "WHERE LOWER(username)=LOWER(?) AND firm_code=? LIMIT 1",
            (email, firm_code),
        ).fetchone()
        return bool(row)
    except sqlite3.OperationalError:
        return False


def _client_code_exists(
    conn: sqlite3.Connection, firm_code: str, client_code: str,
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM clients WHERE client_code=? LIMIT 1",
        (client_code,),
    ).fetchone()
    return bool(row)


def validate_rows(
    db_path: Path | str, *,
    firm_code: str,
    rows: list[dict[str, str]],
) -> dict[int, list[str]]:
    """Return a per-row-index error list. Empty dict = nothing rejected."""
    errors: dict[int, list[str]] = {}
    seen_codes: dict[str, int] = {}
    with _open(db_path) as conn:
        for i, r in enumerate(rows):
            errs: list[str] = []
            code = (r.get("client_code") or "").strip().upper()
            name = (r.get("client_name") or "").strip()
            email = (r.get("email") or "").strip()
            phone = (r.get("phone") or "").strip()
            lang = (r.get("language") or "").strip().lower()
            fye = (r.get("fiscal_year_end") or "").strip()
            primary = (r.get("primary_employee_email") or "").strip()
            secondary = (r.get("secondary_employee_email") or "").strip()

            if not code:
                errs.append("client_code is required")
            elif not _CODE_RE.match(code):
                errs.append(
                    "client_code must be A-Z/0-9 (2-32 chars, "
                    "no spaces)"
                )
            if not name:
                errs.append("client_name is required")
            if email and not _EMAIL_RE.match(email):
                errs.append(f"invalid email: {email}")
            if lang and lang not in ("fr", "en"):
                errs.append("language must be 'fr' or 'en'")
            if fye and not _DATE_RE.match(fye):
                errs.append("fiscal_year_end must be YYYY-MM-DD")
            if fye and _DATE_RE.match(fye):
                try:
                    datetime.strptime(fye, "%Y-%m-%d")
                except ValueError:
                    errs.append("fiscal_year_end is not a real date")
            for label, val in (("primary_employee_email", primary),
                               ("secondary_employee_email", secondary)):
                if not val:
                    continue
                if not _EMAIL_RE.match(val):
                    errs.append(f"{label} invalid: {val}")
                    continue
                if not _known_employee(conn, firm_code, val):
                    errs.append(f"{label} unknown in firm: {val}")

            # Duplicate within the batch
            if code:
                if code in seen_codes:
                    errs.append(
                        f"duplicate within CSV (row {seen_codes[code] + 1})"
                    )
                else:
                    seen_codes[code] = i
                # Already exists in DB
                if _client_code_exists(conn, firm_code, code):
                    errs.append("client_code already exists")

            if errs:
                errors[i] = errs
    return errors


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


def import_rows(
    db_path: Path | str, *,
    firm_code: str,
    rows: list[dict[str, str]],
    dry_run: bool = False,
) -> dict[str, Any]:
    """Insert validated rows. Rows with errors are skipped.

    Returns::

        {
            "total":     <int, total rows submitted>,
            "imported":  <int, how many inserted>,
            "skipped":   <int, rejected>,
            "dry_run":   <bool>,
            "errors":    {row_index: [str, ...]},
            "clients":   [client_code, ...],   # successful inserts
        }
    """
    errors = validate_rows(db_path, firm_code=firm_code, rows=rows)
    imported: list[str] = []
    if not dry_run:
        with _open(db_path) as conn:
            for i, r in enumerate(rows):
                if i in errors:
                    continue
                code = (r.get("client_code") or "").strip().upper()
                name = (r.get("client_name") or "").strip()
                email = (r.get("email") or "").strip() or None
                phone = (r.get("phone") or "").strip() or None
                lang = (r.get("language") or "").strip().lower() or "fr"
                primary = (r.get("primary_employee_email") or "").strip() or None
                secondary = (r.get("secondary_employee_email") or "").strip() or None
                try:
                    conn.execute(
                        "INSERT INTO clients "
                        "(client_code, client_name, contact_email, "
                        "whatsapp_number, language, firm_code, "
                        "primary_employee_email, secondary_employee_email, "
                        "active) VALUES (?,?,?,?,?,?,?,?,1)",
                        (code, name, email, phone, lang, firm_code,
                         primary, secondary),
                    )
                    imported.append(code)
                except sqlite3.IntegrityError as exc:
                    # Race with concurrent insert — record and continue.
                    errors.setdefault(i, []).append(f"insert_failed: {exc}")
                except sqlite3.OperationalError as exc:
                    errors.setdefault(i, []).append(f"insert_failed: {exc}")
            conn.commit()
    return {
        "total": len(rows),
        "imported": len(imported),
        "skipped": len(errors),
        "dry_run": bool(dry_run),
        "errors": errors,
        "clients": imported,
    }


# ---------------------------------------------------------------------------
# Error-report CSV
# ---------------------------------------------------------------------------


def generate_error_csv(
    rows: list[dict[str, str]],
    errors: dict[int, list[str]],
) -> bytes:
    """Return a downloadable CSV listing every rejected row with reasons."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(list(TEMPLATE_HEADERS) + ["error"])
    for i, errs in sorted(errors.items()):
        if i >= len(rows):
            continue
        r = rows[i]
        w.writerow([
            r.get(h, "") for h in TEMPLATE_HEADERS
        ] + ["; ".join(errs)])
    out = "﻿" + buf.getvalue()
    return out.encode("utf-8")


# ---------------------------------------------------------------------------
# Renderer (CPA page)
# ---------------------------------------------------------------------------


def render_import_page(
    *, firm_code: str,
    preview: dict[str, Any] | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    import html as _html
    def _esc(s: Any) -> str:
        return _html.escape(str(s or ""))
    flash_html = ''
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
    preview_html = ''
    if preview is not None:
        total = int(preview.get('total') or 0)
        imported = int(preview.get('imported') or 0)
        skipped = int(preview.get('skipped') or 0)
        dry = bool(preview.get('dry_run'))
        tag = 'Dry-run' if dry else 'Imported'
        preview_html = (
            f'<div class="card">'
            f'<h2>{tag} — {imported}/{total} rows</h2>'
            f'<p>Skipped: {skipped}</p>'
        )
        err_rows = ''
        for idx, errs in (preview.get('errors') or {}).items():
            err_rows += (
                f'<tr><td>{int(idx) + 1}</td>'
                f'<td style="color:#b91c1c;">{_esc("; ".join(errs))}</td></tr>'
            )
        if err_rows:
            preview_html += (
                '<table><thead><tr><th>Row</th><th>Errors</th></tr></thead>'
                f'<tbody>{err_rows}</tbody></table>'
            )
        preview_html += '</div>'
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Bulk client import</title>'
        '<style>body{font-family:system-ui,Arial;max-width:1000px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;margin:1rem 0;}'
        'th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '.muted{color:#6b7280;font-size:12px;}'
        '</style></head><body>'
        '<h1>Bulk client import / Importer des clients</h1>'
        f'{flash_html}'
        '<div class="card">'
        '<p><a href="/clients/import/template.csv">'
        '&#128229; Download template / Télécharger le modèle</a></p>'
        '<form method="POST" action="/clients/import" '
        'enctype="multipart/form-data">'
        '<input type="file" name="file" accept=".csv,text/csv" required>'
        '<label style="display:block;margin-top:8px;">'
        '<input type="checkbox" name="dry_run" value="1" checked> '
        'Dry-run (preview only) / Aperçu seulement</label>'
        '<button type="submit" style="margin-top:8px;">'
        'Upload / Importer</button>'
        '</form></div>'
        f'{preview_html}'
        '</body></html>'
    )
