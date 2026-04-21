"""HTTP route handlers for Gap 1-5 surfaces.

This module contains renderers + dispatch functions so the 24k-line
dashboard monolith only needs thin hooks in `do_GET` / `do_POST` that
delegate here. Each handler:

1. Receives the session user + an open handler reference (for cookies/
   redirects), so it can call the shared _send_html / _redirect helpers.
2. Calls the pure engine functions in src/integrations for state.
3. Returns True when it handled the request (caller returns without
   touching the default 404 fallback).

Grouped by gap:

    Gap 1 — /onboarding*, /tour, /onboarding/checklist, welcome modal
    Gap 2 — /my_tasks, /review_queue, /document/{id}/*
    Gap 3 — /owner/dashboard*, /owner/firms/*, /owner/impersonate, feedback
    Gap 4 — /close/wizard*
    Gap 5 — /c/{token}/status, /c/{token}/activity, /c/{token}/messages*

The impersonation block is enforced here — any path that mutates state
checks `impersonation.active_session()` and 403s when one exists.
"""
from __future__ import annotations

import html as _html
import json
import logging
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.integrations import (
    client_status as _cs,
    impersonation as _imp,
    month_end_close as _close,
    notification_sender as _notify,
    onboarding_checklist as _ob,
    owner_dashboard as _od,
    review_workflow as _rw,
)

log = logging.getLogger(__name__)


def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else ''))


def _rget(row: Any, key: str, default: Any = None) -> Any:
    """Get a value from either a dict or a sqlite3.Row safely."""
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Schema bootstrap (called once from review_dashboard.bootstrap_schema)
# ---------------------------------------------------------------------------


def ensure_all_gap_schemas(db_path: Path | str) -> None:
    _ob.ensure_onboarding_schema(db_path)
    _rw.ensure_review_schema(db_path)
    _close.ensure_close_schema(db_path)
    _cs.ensure_client_status_schema(db_path)
    _od  # re-export marker
    _imp.ensure_impersonation_schema(db_path)
    _notify.ensure_sender_schema(db_path)
    _ensure_feedback_schema(db_path)


def _ensure_feedback_schema(db_path: Path | str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT,
                submitter_email TEXT,
                subject TEXT,
                body TEXT,
                response_body TEXT,
                responded_by TEXT,
                responded_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Impersonation cookie helpers
# ---------------------------------------------------------------------------


IMP_COOKIE = 'otocpa_imp_sid'


def read_imp_cookie(handler) -> str:
    cookie = handler.headers.get('Cookie', '') or ''
    for part in cookie.split(';'):
        part = part.strip()
        if part.startswith(f'{IMP_COOKIE}='):
            return part[len(f'{IMP_COOKIE}='):]
    return ''


def set_imp_cookie(sid: str) -> tuple[str, str]:
    return (
        'Set-Cookie',
        f'{IMP_COOKIE}={sid}; HttpOnly; SameSite=Lax; Path=/; Max-Age={12*3600}',
    )


def clear_imp_cookie() -> tuple[str, str]:
    return (
        'Set-Cookie',
        f'{IMP_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0',
    )


def current_impersonation(db_path: Path | str, handler) -> dict[str, Any] | None:
    sid = read_imp_cookie(handler)
    if not sid:
        return None
    return _imp.active_session(db_path, session_id=sid)


def effective_firm_code(db_path: Path | str, handler, ctx: dict[str, Any]) -> str:
    sess = current_impersonation(db_path, handler)
    if sess:
        return sess.get('impersonated_firm_code') or ctx.get('firm_code', '')
    return ctx.get('firm_code', '')


def block_write_if_impersonating(
    db_path: Path | str, handler, *, action: str, path: str, method: str,
) -> bool:
    """Returns True when the request must be blocked. Also audit-logs."""
    sess = current_impersonation(db_path, handler)
    if not sess:
        return False
    _imp.log_action(
        db_path, session_id=sess['session_id'],
        original_user_email=sess['original_user_email'],
        firm_code=sess['impersonated_firm_code'],
        action=action, path=path, method=method, blocked=True,
    )
    return True


# ---------------------------------------------------------------------------
# Gap 1 — onboarding / tour / checklist
# ---------------------------------------------------------------------------


def render_onboarding_quick_setup(
    db_path: Path | str, *, firm_code: str, lang: str = 'en',
    flash: str = '', flash_error: str = '',
) -> str:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM firms WHERE firm_code=?", (firm_code,),
        ).fetchone()
    firm = dict(row) if row else {}
    name = _esc(firm.get('name') or firm.get('firm_name') or '')
    address = _esc(firm.get('address') or '')
    phone = _esc(firm.get('phone') or '')
    default_lang = _esc(firm.get('default_lang') or 'en')
    fye = _esc(firm.get('fiscal_year_end') or '12-31')
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'
    return (
        '<div class="card" style="max-width:600px;margin:1rem auto;">'
        '<h2>Quick setup</h2>'
        '<p>Complete your firm profile to unlock the rest of the checklist.</p>'
        f'{flash_html}'
        '<form method="POST" action="/onboarding/save" style="display:grid;gap:10px;">'
        '<label>Firm name<br>'
        f'<input type="text" name="name" value="{name}" required '
        'style="width:100%;padding:8px;"></label>'
        '<label>Address<br>'
        f'<input type="text" name="address" value="{address}" required '
        'style="width:100%;padding:8px;"></label>'
        '<label>Phone<br>'
        f'<input type="text" name="phone" value="{phone}" required '
        'style="width:100%;padding:8px;"></label>'
        '<label>Default language<br>'
        '<select name="default_lang" style="padding:8px;">'
        f'<option value="en"{" selected" if default_lang=="en" else ""}>English</option>'
        f'<option value="fr"{" selected" if default_lang=="fr" else ""}>Fran&ccedil;ais</option>'
        '</select></label>'
        '<label>Fiscal year end (MM-DD)<br>'
        f'<input type="text" name="fiscal_year_end" value="{fye}" '
        'placeholder="12-31" style="padding:8px;"></label>'
        '<button type="submit" class="primary" style="padding:10px 16px;">Save</button>'
        '</form>'
        '<p style="margin-top:12px;"><a href="/">Back to dashboard</a></p>'
        '</div>'
    )


TOUR_TOTAL_STEPS = 5


# Five tour screens. Each screen has FR + EN strings, a visual aid
# (inline SVG so no external assets / CSP headaches), and an optional
# "try it" link to the relevant page. The strings live here rather
# than in a separate i18n file so the tour module stays self-contained.
_TOUR_CONTENT: list[dict[str, dict[str, str]]] = [
    # Step 1 — welcome + overview
    {
        'en': {
            'title': 'Welcome to OtoCPA',
            'subtitle': 'A 2-minute walkthrough of how the product is wired.',
            'body': (
                "You are the CPA. Clients send you receipts and invoices, "
                "you review them, post to QuickBooks, and close each month. "
                "OtoCPA does the OCR, learns your vendor mappings, and "
                "keeps the audit trail so you stop copy-pasting numbers."
            ),
            'bullets': [
                'One queue for every client — filter by status or priority.',
                'Receipts OCR\'d automatically; you just confirm + post.',
                'Month-end close wizard bundles the checks into 6 steps.',
            ],
            'try_label': 'Open the dashboard',
            'try_href': '/',
        },
        'fr': {
            'title': 'Bienvenue sur OtoCPA',
            'subtitle': 'Survol de 2 minutes du fonctionnement du produit.',
            'body': (
                "Vous êtes le comptable. Vos clients envoient reçus et "
                "factures, vous les révisez, passez l'écriture dans "
                "QuickBooks et fermez chaque mois. OtoCPA fait l'OCR, "
                "apprend vos mappages fournisseurs et maintient la piste "
                "d'audit pour que vous cessiez de recopier des chiffres."
            ),
            'bullets': [
                'Une seule file pour tous les clients — filtrage par statut ou priorité.',
                "Les reçus sont OCR'sés automatiquement ; vous confirmez + passez l'écriture.",
                'Assistant de fin de mois en 6 étapes.',
            ],
            'try_label': 'Ouvrir le tableau de bord',
            'try_href': '/',
        },
        'svg': (
            '<svg viewBox="0 0 320 140" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="Overview diagram" '
            'style="width:100%;max-width:320px;height:auto;">'
            '<rect x="10" y="15" width="80" height="40" rx="6" fill="#dbeafe" stroke="#1e40af"/>'
            '<text x="50" y="40" text-anchor="middle" font-size="10" fill="#1e40af">Client</text>'
            '<path d="M92 35 L140 35" stroke="#6b7280" marker-end="url(#arrow)"/>'
            '<rect x="142" y="15" width="80" height="40" rx="6" fill="#d1fae5" stroke="#16C172"/>'
            '<text x="182" y="30" text-anchor="middle" font-size="10" fill="#166534">OtoCPA</text>'
            '<text x="182" y="42" text-anchor="middle" font-size="9" fill="#166534">OCR + Queue</text>'
            '<path d="M224 35 L272 35" stroke="#6b7280" marker-end="url(#arrow)"/>'
            '<rect x="274" y="15" width="44" height="40" rx="6" fill="#fef3c7" stroke="#ca8a04"/>'
            '<text x="296" y="40" text-anchor="middle" font-size="9" fill="#854d0e">QBO</text>'
            '<text x="160" y="90" text-anchor="middle" font-size="9" fill="#6b7280">Receipts → Review → Posted</text>'
            '<text x="160" y="108" text-anchor="middle" font-size="9" fill="#6b7280">Month-end close wizard runs here</text>'
            '<defs><marker id="arrow" viewBox="0 0 6 6" refX="5" refY="3" markerWidth="5" markerHeight="5" orient="auto"><path d="M0 0 L6 3 L0 6 z" fill="#6b7280"/></marker></defs>'
            '</svg>'
        ),
    },
    # Step 2 — client management
    {
        'en': {
            'title': 'Clients + portals',
            'subtitle': 'Give each client a way to send you receipts.',
            'body': (
                "Every client gets a personal portal link. Share it by "
                "email or QR; they upload receipts, you receive them in "
                "your queue. For shops with multiple uploaders (bookkeeper, "
                "office manager, owner), switch the client to "
                "multi-user mode to invite each person separately."
            ),
            'bullets': [
                'Add a client — portal token auto-generated.',
                'Multi-user clients: admin invites contributors by email.',
                'Every upload is attributed to the uploader.',
            ],
            'try_label': 'Go to clients',
            'try_href': '/clients',
        },
        'fr': {
            'title': 'Clients et portails',
            'subtitle': 'Donnez à chaque client un moyen simple d\'envoyer des reçus.',
            'body': (
                "Chaque client reçoit un lien de portail personnel. Partagez-le "
                "par courriel ou code QR ; vos clients téléversent leurs reçus, "
                "vous les voyez dans votre file. Pour les entreprises avec "
                "plusieurs utilisateurs (comptable, adjointe, dirigeant), "
                "passez le client en mode multi-utilisateurs pour inviter "
                "chaque personne séparément."
            ),
            'bullets': [
                'Ajoutez un client — le jeton de portail est généré automatiquement.',
                'Client multi-utilisateurs : l\'admin invite les contributeurs par courriel.',
                'Chaque téléversement est attribué à son auteur.',
            ],
            'try_label': 'Voir les clients',
            'try_href': '/clients',
        },
        'svg': (
            '<svg viewBox="0 0 320 140" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="Clients diagram" '
            'style="width:100%;max-width:320px;height:auto;">'
            '<rect x="20" y="20" width="90" height="30" rx="4" fill="#eef2ff" stroke="#4338ca"/>'
            '<text x="65" y="39" text-anchor="middle" font-size="10" fill="#4338ca">Sole-prop</text>'
            '<rect x="20" y="58" width="90" height="30" rx="4" fill="#eef2ff" stroke="#4338ca"/>'
            '<text x="65" y="77" text-anchor="middle" font-size="10" fill="#4338ca">Multi-user</text>'
            '<path d="M112 35 L160 35" stroke="#6b7280" marker-end="url(#a2)"/>'
            '<path d="M112 73 L160 73" stroke="#6b7280" marker-end="url(#a2)"/>'
            '<rect x="162" y="15" width="100" height="80" rx="6" fill="#fff" stroke="#1e40af"/>'
            '<text x="212" y="35" text-anchor="middle" font-size="10" fill="#1e40af">/c/{token}</text>'
            '<text x="212" y="55" text-anchor="middle" font-size="9" fill="#6b7280">(single link)</text>'
            '<line x1="170" y1="65" x2="254" y2="65" stroke="#e5e7eb"/>'
            '<text x="212" y="80" text-anchor="middle" font-size="10" fill="#1e40af">/cp/{user_token}</text>'
            '<text x="212" y="92" text-anchor="middle" font-size="9" fill="#6b7280">(per invite)</text>'
            '<defs><marker id="a2" viewBox="0 0 6 6" refX="5" refY="3" markerWidth="5" markerHeight="5" orient="auto"><path d="M0 0 L6 3 L0 6 z" fill="#6b7280"/></marker></defs>'
            '</svg>'
        ),
    },
    # Step 3 — document workflow
    {
        'en': {
            'title': 'Review + post',
            'subtitle': 'Your employees submit, you approve.',
            'body': (
                "Receipts land in New → OCR'd to Processing → flip to "
                "NeedsReview when the OCR isn't sure. Your staff picks "
                "them up, fills the gaps, and submits for review. Owners "
                "and firm admins approve. Approved items post to QBO "
                "(or Acomba / Sage) automatically."
            ),
            'bullets': [
                'Assign each document to a specific employee.',
                'Submit → review_queue → Approve/Reject with notes.',
                'Bulk approve multiple at once when you are confident.',
            ],
            'try_label': 'Open my tasks',
            'try_href': '/my_tasks',
        },
        'fr': {
            'title': 'Réviser et passer l\'écriture',
            'subtitle': 'Vos employés soumettent, vous approuvez.',
            'body': (
                "Les reçus arrivent à New → OCR\'sés en Processing → passent "
                "à NeedsReview quand l\'OCR doute. Votre équipe les prend, "
                "complète les champs et les soumet pour révision. Les "
                "propriétaires et firm_admins approuvent. Les items approuvés "
                "sont passés dans QBO (ou Acomba / Sage) automatiquement."
            ),
            'bullets': [
                'Assignez chaque document à un employé.',
                'Soumettre → file de révision → Approuver/Refuser avec notes.',
                'Approbation en lot quand vous êtes certain.',
            ],
            'try_label': 'Ouvrir mes tâches',
            'try_href': '/my_tasks',
        },
        'svg': (
            '<svg viewBox="0 0 320 140" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="Workflow diagram" '
            'style="width:100%;max-width:320px;height:auto;">'
            '<rect x="8"   y="50" width="50" height="40" rx="4" fill="#fef3c7" stroke="#ca8a04"/>'
            '<text x="33"  y="75" text-anchor="middle" font-size="9">New</text>'
            '<rect x="70"  y="50" width="60" height="40" rx="4" fill="#dbeafe" stroke="#1e40af"/>'
            '<text x="100" y="75" text-anchor="middle" font-size="9">NeedsReview</text>'
            '<rect x="142" y="50" width="60" height="40" rx="4" fill="#ddd6fe" stroke="#6d28d9"/>'
            '<text x="172" y="75" text-anchor="middle" font-size="9">Submitted</text>'
            '<rect x="214" y="50" width="50" height="40" rx="4" fill="#d1fae5" stroke="#16C172"/>'
            '<text x="239" y="75" text-anchor="middle" font-size="9">Approved</text>'
            '<rect x="276" y="50" width="40" height="40" rx="4" fill="#cffafe" stroke="#0891b2"/>'
            '<text x="296" y="75" text-anchor="middle" font-size="9">Posted</text>'
            '<path d="M58  70 L70  70" stroke="#6b7280" marker-end="url(#a3)"/>'
            '<path d="M130 70 L142 70" stroke="#6b7280" marker-end="url(#a3)"/>'
            '<path d="M202 70 L214 70" stroke="#6b7280" marker-end="url(#a3)"/>'
            '<path d="M264 70 L276 70" stroke="#6b7280" marker-end="url(#a3)"/>'
            '<defs><marker id="a3" viewBox="0 0 6 6" refX="5" refY="3" markerWidth="5" markerHeight="5" orient="auto"><path d="M0 0 L6 3 L0 6 z" fill="#6b7280"/></marker></defs>'
            '</svg>'
        ),
    },
    # Step 4 — financial statements + close
    {
        'en': {
            'title': 'Close the month',
            'subtitle': 'Six steps, one lock.',
            'body': (
                "When the month is done, run the close wizard. It walks "
                "you through selecting the period, confirming every "
                "document is posted, reconciling the bank, reviewing "
                "proposed accruals (with per-asset depreciation and "
                "per-employee wage lines you can edit), generating "
                "statements, then locking the period."
            ),
            'bullets': [
                'Prior periods must be closed first — enforced.',
                'Accrual step shows each line with an editable amount.',
                'Lock is reversible by the owner only, with audit.',
            ],
            'try_label': 'Open close wizard',
            'try_href': '/close/wizard',
        },
        'fr': {
            'title': 'Fermer le mois',
            'subtitle': 'Six étapes, un verrou.',
            'body': (
                "Quand le mois est terminé, lancez l'assistant de fermeture. "
                "Il vous guide : choisir la période, confirmer que tous les "
                "documents sont passés, réconcilier la banque, réviser les "
                "charges à payer suggérées (dépréciation par actif, salaires "
                "par employé — chaque ligne modifiable), générer les états, "
                "puis verrouiller la période."
            ),
            'bullets': [
                'Les périodes antérieures doivent être fermées d\'abord.',
                'L\'étape accruals affiche chaque ligne avec montant modifiable.',
                'Le verrou n\'est levable que par le propriétaire, avec audit.',
            ],
            'try_label': 'Ouvrir l\'assistant de fermeture',
            'try_href': '/close/wizard',
        },
        'svg': (
            '<svg viewBox="0 0 320 140" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="Close wizard steps" '
            'style="width:100%;max-width:320px;height:auto;">'
            '<g font-size="9">'
            '<rect x="8"   y="40" width="46" height="30" rx="4" fill="#d1fae5" stroke="#16C172"/>'
            '<text x="31" y="58" text-anchor="middle">1 Period</text>'
            '<rect x="58"  y="40" width="50" height="30" rx="4" fill="#d1fae5" stroke="#16C172"/>'
            '<text x="83" y="58" text-anchor="middle">2 Docs</text>'
            '<rect x="112" y="40" width="50" height="30" rx="4" fill="#d1fae5" stroke="#16C172"/>'
            '<text x="137" y="58" text-anchor="middle">3 Bank</text>'
            '<rect x="166" y="40" width="58" height="30" rx="4" fill="#dbeafe" stroke="#1e40af"/>'
            '<text x="195" y="58" text-anchor="middle">4 Accruals</text>'
            '<rect x="228" y="40" width="48" height="30" rx="4" fill="#fff" stroke="#9ca3af"/>'
            '<text x="252" y="58" text-anchor="middle">5 Stmts</text>'
            '<rect x="280" y="40" width="36" height="30" rx="4" fill="#fff" stroke="#9ca3af"/>'
            '<text x="298" y="58" text-anchor="middle">6 Lock</text>'
            '</g>'
            '<text x="160" y="100" text-anchor="middle" font-size="9" fill="#6b7280">Done steps stay green; current step blue; later steps grey.</text>'
            '</svg>'
        ),
    },
    # Step 5 — next steps + help
    {
        'en': {
            'title': 'You are set up',
            'subtitle': 'A few places to go from here.',
            'body': (
                "The getting-started checklist on the right of every page "
                "tracks the first six things to do. It ticks itself as you "
                "complete each item. When the checklist is all done the "
                "widget hides itself. Reach for /owner/dashboard for "
                "firm-wide rollups when you are ready."
            ),
            'bullets': [
                'Getting-started checklist auto-completes as you act.',
                'Owner dashboard: revenue, firm health, alerts.',
                'Help? See docs/ADMIN_GUIDE.md or hit /health for status.',
            ],
            'try_label': 'Back to dashboard',
            'try_href': '/',
        },
        'fr': {
            'title': 'Vous êtes prêt',
            'subtitle': 'Quelques endroits où aller maintenant.',
            'body': (
                "La liste de démarrage à droite de chaque page suit les six "
                "premières choses à faire. Elle se coche toute seule au fur "
                "et à mesure que vous agissez. Une fois tout coché, le "
                "widget se cache. Pour les tableaux récapitulatifs du "
                "cabinet, ouvrez /owner/dashboard."
            ),
            'bullets': [
                'La liste de démarrage s\'auto-complète à mesure que vous agissez.',
                'Tableau propriétaire : revenus, santé du cabinet, alertes.',
                'Besoin d\'aide ? Voir docs/ADMIN_GUIDE.md ou /health.',
            ],
            'try_label': 'Retour au tableau de bord',
            'try_href': '/',
        },
        'svg': (
            '<svg viewBox="0 0 320 140" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="Getting started checklist" '
            'style="width:100%;max-width:320px;height:auto;">'
            '<rect x="40" y="15" width="240" height="110" rx="6" fill="#fffef5" stroke="#d4cfa8"/>'
            '<text x="160" y="32" text-anchor="middle" font-size="11" font-weight="bold">Getting started</text>'
            '<g font-size="9" fill="#333">'
            '<text x="55" y="52">&#10003; Complete firm profile</text>'
            '<text x="55" y="68">&#10003; Add first client</text>'
            '<text x="55" y="84">&#10003; Send portal link</text>'
            '<text x="55" y="100">&#9744; Upload test receipt</text>'
            '<text x="55" y="116">&#9744; Review getting-started guide</text>'
            '</g>'
            '</svg>'
        ),
    },
]


def _tour_label(lang: str, key: str) -> str:
    labels = {
        'en': {
            'step_of': 'Step {n} of {total}',
            'back': '&larr; Back',
            'next': 'Next &rarr;',
            'finish': 'Finish tour',
            'skip': 'Skip tour',
            'try_it': 'Try it:',
        },
        'fr': {
            'step_of': 'Étape {n} sur {total}',
            'back': '&larr; Retour',
            'next': 'Suivant &rarr;',
            'finish': 'Terminer la visite',
            'skip': 'Ignorer la visite',
            'try_it': 'Essayez :',
        },
    }
    return labels.get(lang if lang in labels else 'en', labels['en']).get(key, key)


def render_tour_screens(step: int, lang: str = 'en') -> str:
    """Render one of the 5 tour screens.

    The content pool lives in _TOUR_CONTENT (EN + FR + SVG per screen).
    Language falls back to EN when the requested lang isn't known; this
    matches the rest of the portal."""
    total = TOUR_TOTAL_STEPS
    step = max(1, min(total, step))
    content = _TOUR_CONTENT[step - 1]
    lang_key = 'fr' if lang == 'fr' else 'en'
    block = content.get(lang_key) or content['en']
    svg = content.get('svg') or ''

    bullets_html = ''.join(
        f'<li>{_esc(b)}</li>' for b in block.get('bullets', [])
    )

    # Navigation
    prev_html = ''
    if step > 1:
        prev_html = (
            f'<a href="/tour?step={step-1}&lang={lang_key}" '
            'style="margin-right:12px;color:#4b5563;">'
            f'{_tour_label(lang_key, "back")}</a>'
        )

    if step < total:
        next_btn = (
            f'<a href="/tour?step={step+1}&lang={lang_key}" '
            'style="background:#1e40af;color:white;padding:10px 22px;'
            'border-radius:4px;text-decoration:none;font-weight:bold;">'
            f'{_tour_label(lang_key, "next")}</a>'
        )
    else:
        next_btn = (
            '<form method="POST" action="/tour/complete" '
            'style="display:inline;margin:0;">'
            f'<input type="hidden" name="lang" value="{_esc(lang_key)}">'
            '<button type="submit" '
            'style="background:#16C172;color:black;padding:10px 22px;'
            'border:none;border-radius:4px;font-weight:bold;cursor:pointer;">'
            f'{_tour_label(lang_key, "finish")}</button></form>'
        )
    skip_html = (
        '<form method="POST" action="/tour/complete" '
        'style="display:inline;margin-left:16px;">'
        f'<input type="hidden" name="lang" value="{_esc(lang_key)}">'
        '<button type="submit" '
        'style="background:none;border:none;color:#9ca3af;cursor:pointer;'
        'text-decoration:underline;padding:0;">'
        f'{_tour_label(lang_key, "skip")}</button></form>'
    )

    try_html = ''
    if block.get('try_label') and block.get('try_href'):
        try_html = (
            f'<p style="margin-top:1rem;">'
            f'<strong>{_esc(_tour_label(lang_key, "try_it"))}</strong> '
            f'<a href="{_esc(block["try_href"])}" '
            'style="color:#1e40af;">'
            f'{_esc(block["try_label"])}</a></p>'
        )

    other_lang = 'fr' if lang_key == 'en' else 'en'
    lang_switcher = (
        f'<a href="/tour?step={step}&lang={other_lang}" '
        'style="position:absolute;top:10px;right:14px;color:#9ca3af;'
        'font-size:13px;">'
        f'{"Français" if other_lang == "fr" else "English"}</a>'
    )

    step_label = _tour_label(lang_key, 'step_of').format(n=step, total=total)

    return (
        '<div class="card" data-tour-step="' + str(step) + '" '
        'data-tour-lang="' + lang_key + '" '
        'style="max-width:640px;margin:2rem auto;padding:2rem;'
        'position:relative;background:white;">'
        f'{lang_switcher}'
        f'<div style="color:#9ca3af;font-size:13px;">{step_label}</div>'
        f'<h2 style="margin:6px 0 4px 0;">{_esc(block["title"])}</h2>'
        f'<div style="color:#6b7280;margin-bottom:1rem;">{_esc(block["subtitle"])}</div>'
        f'<div style="text-align:center;margin:1rem 0;">{svg}</div>'
        f'<p style="line-height:1.6;">{_esc(block["body"])}</p>'
        f'<ul style="margin:0.5rem 0;line-height:1.6;">{bullets_html}</ul>'
        f'{try_html}'
        '<div style="margin-top:2rem;text-align:right;">'
        f'{prev_html}{next_btn}{skip_html}'
        '</div></div>'
    )


# ---------------------------------------------------------------------------
# Gap 2 — review queue + my tasks
# ---------------------------------------------------------------------------


def render_my_tasks(
    db_path: Path | str, *, assignee_email: str, lang: str = 'en',
    flash: str = '', flash_error: str = '',
) -> str:
    tasks = _rw.my_tasks(db_path, assignee_email=assignee_email)
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'
    if not tasks:
        return (
            '<div class="card">'
            '<h2>My Tasks</h2>'
            f'{flash_html}'
            '<p>Nothing assigned to you right now. When an owner assigns a '
            'document / journal entry for your review, it will appear here.</p>'
            '</div>'
        )
    rows = []
    for t in tasks:
        eid = _esc(t['entity_id'])
        etype = _esc(t['entity_type'])
        pri = _esc(t.get('priority') or 'normal')
        status = _esc(t.get('status') or '')
        assigned = _esc(t.get('assigned_at') or '')
        submit_form = (
            f'<form method="POST" action="/document/{eid}/submit_for_review" '
            'style="display:inline;">'
            '<button type="submit" class="primary">Submit for review</button>'
            '</form>'
        )
        escalate_form = (
            f'<form method="POST" action="/document/{eid}/escalate" '
            'style="display:inline;margin-left:6px;">'
            '<button type="submit">Escalate</button>'
            '</form>'
        )
        rows.append(
            '<tr>'
            f'<td>{etype}</td>'
            f'<td><a href="/document?id={eid}">{eid}</a></td>'
            f'<td><span style="font-weight:bold;color:'
            f'{"#b91c1c" if pri == "urgent" else "#333"}">{pri}</span></td>'
            f'<td>{status}</td>'
            f'<td>{assigned}</td>'
            f'<td>{submit_form}{escalate_form}</td>'
            '</tr>'
        )
    return (
        '<div class="card">'
        f'<h2>My Tasks <span style="color:#888;">({len(tasks)})</span></h2>'
        f'{flash_html}'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr>'
        '<th>Type</th><th>ID</th><th>Priority</th>'
        '<th>Status</th><th>Assigned</th><th>Actions</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div>'
    )


def render_review_queue(
    db_path: Path | str, *, firm_code: str, lang: str = 'en',
    flash: str = '', flash_error: str = '',
) -> str:
    pending = _rw.pending_reviews(db_path, firm_code=firm_code)
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'
    if not pending:
        return (
            '<div class="card">'
            '<h2>Review Queue</h2>'
            f'{flash_html}'
            '<p>No items waiting for review. When an employee submits a '
            'document you will see it here.</p></div>'
        )
    rows = []
    for p in pending:
        eid = _esc(p['entity_id'])
        etype = _esc(p['entity_type'])
        pri = _esc(p.get('priority') or 'normal')
        status = _esc(p.get('status') or '')
        submitted_at = _esc(p.get('submitted_at') or '')
        by = _esc(p.get('submitted_by_email') or '')
        approve_form = (
            f'<form method="POST" action="/document/{eid}/approve" '
            'style="display:inline;">'
            '<button type="submit" class="primary" '
            'style="background:#16C172;color:black;">Approve</button>'
            '</form>'
        )
        reject_form = (
            f'<form method="POST" action="/document/{eid}/reject" '
            'style="display:inline;margin-left:6px;">'
            '<input type="text" name="reason" placeholder="Reason (required)" '
            'style="padding:4px;">'
            '<button type="submit" style="background:#dc2626;color:white;">'
            'Reject</button>'
            '</form>'
        )
        rows.append(
            '<tr>'
            f'<td><input type="checkbox" name="entity_ids" value="{eid}"></td>'
            f'<td>{etype}</td>'
            f'<td><a href="/document?id={eid}">{eid}</a></td>'
            f'<td>{pri}</td>'
            f'<td>{status}</td>'
            f'<td>{by}</td>'
            f'<td>{submitted_at}</td>'
            f'<td>{approve_form}{reject_form}</td>'
            '</tr>'
        )
    return (
        '<div class="card">'
        f'<h2>Review Queue <span style="color:#888;">({len(pending)})</span></h2>'
        f'{flash_html}'
        '<form method="POST" action="/review_queue/bulk_approve">'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr>'
        '<th></th><th>Type</th><th>ID</th><th>Priority</th>'
        '<th>Status</th><th>Submitted by</th><th>At</th><th>Actions</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
        '<button type="submit" class="primary" '
        'style="margin-top:1rem;background:#16C172;color:black;">'
        'Bulk approve selected</button>'
        '</form></div>'
    )


# ---------------------------------------------------------------------------
# Gap 3 — owner dashboard + impersonation
# ---------------------------------------------------------------------------


def render_owner_dashboard(
    db_path: Path | str, *, flash: str = '', flash_error: str = '',
) -> str:
    bundle = _od.build_dashboard(db_path)
    rev = bundle['revenue']
    firms = bundle['firms']
    sys_h = bundle['system']
    support = bundle['support']
    alerts = bundle['alerts']
    feedback = bundle['feedback']
    drilldown = bundle['drilldown']

    widget = (
        '<div class="owner-widget" style="background:white;border:1px solid #ddd;'
        'padding:14px;border-radius:6px;margin-bottom:12px;">'
    )
    rev_html = (
        f'{widget}<h3 style="margin-top:0;">Revenue</h3>'
        f'<div style="font-size:28px;font-weight:bold;">'
        f'${rev["mrr_cad"]:,.2f}<span style="font-size:14px;color:#888;"> MRR</span>'
        '</div>'
        f'<div>Failed payments (7d): {rev["failed_payments_7d"]}</div>'
        f'<div>At-risk subscriptions: {rev["at_risk_count"]}</div></div>'
    )
    firms_html = (
        f'{widget}<h3 style="margin-top:0;">Firms</h3>'
        f'<div>Total: {firms["total_firms"]}</div>'
        f'<div>Active this week: {firms["active_this_week"]}</div>'
        f'<div>Never logged in: {firms["never_logged_in"]}</div></div>'
    )
    sys_html = (
        f'{widget}<h3 style="margin-top:0;">System</h3>'
        f'<div>DB: {sys_h["db_size_mb"]} MB</div>'
        f'<div>Disk used: {sys_h["disk_used_percent"]}%</div>'
        f'<div>RSS: {sys_h["rss_mb"]} MB</div>'
        f'<div>Last QBO sync: {_esc(sys_h.get("last_qbo_sync_success") or "never")}</div></div>'
    )
    alerts_rows = ''.join(
        f'<li><strong style="color:#b91c1c;">[{_esc(a["severity"])}]</strong> {_esc(a["message"])}</li>'
        for a in alerts
    ) or '<li>No alerts.</li>'
    alerts_html = (
        f'{widget}<h3 style="margin-top:0;">Alerts</h3>'
        f'<ul>{alerts_rows}</ul></div>'
    )
    feedback_rows = ''.join(
        f'<li>{_esc(f.get("subject") or "(no subject)")} &mdash; '
        f'<em>{_esc(f.get("submitter_email") or "anonymous")}</em></li>'
        for f in feedback
    ) or '<li>No feedback yet.</li>'
    feedback_html = (
        f'{widget}<h3 style="margin-top:0;">Recent feedback '
        f'<a href="/owner/feedback" style="font-size:13px;">(all)</a></h3>'
        f'<ul>{feedback_rows}</ul></div>'
    )
    support_html = (
        f'{widget}<h3 style="margin-top:0;">Support queue</h3>'
        f'<div>Open feedback: {support["open_feedback"]}</div>'
        f'<div>Firms with errors (24h): '
        f'{len(support["firms_with_errors_24h"])}</div></div>'
    )
    drilldown_rows = ''.join(
        '<tr>'
        f'<td><a href="/owner/firms/{_esc(f["firm_code"])}">{_esc(f["firm_code"])}</a></td>'
        f'<td>{_esc(f.get("name") or "")}</td>'
        f'<td>{_esc(f.get("plan") or "")}</td>'
        f'<td>{_esc(f.get("last_login") or "never")}</td>'
        f'<td>{f.get("doc_count", 0)}</td>'
        f'<td>${f.get("mrr_cad", 0):.2f}</td>'
        f'<td><a href="/owner/firms/{_esc(f["firm_code"])}/impersonate" '
        'style="color:#856404;">Impersonate</a></td>'
        '</tr>'
        for f in drilldown
    )
    drilldown_html = (
        '<div class="owner-widget" style="background:white;border:1px solid #ddd;'
        'padding:14px;border-radius:6px;grid-column:1/-1;">'
        '<h3 style="margin-top:0;">Per-firm drilldown</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Firm</th><th>Name</th><th>Plan</th>'
        '<th>Last login</th><th>Docs</th><th>MRR</th><th></th></tr></thead>'
        f'<tbody>{drilldown_rows}</tbody></table></div>'
    )

    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'

    return (
        '<div style="padding:1rem;">'
        '<h1>Owner dashboard</h1>'
        f'{flash_html}'
        '<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));'
        'gap:12px;margin-bottom:12px;" id="owner-metrics-grid">'
        f'{rev_html}{firms_html}{sys_html}'
        f'{alerts_html}{feedback_html}{support_html}'
        f'{drilldown_html}'
        '</div>'
        '<script>'
        'setInterval(async()=>{try{'
        'const r=await fetch("/owner/dashboard/metrics");'
        'if(!r.ok)return;await r.json();'
        '}catch(e){}},60000);'
        '</script></div>'
    )


def render_firm_drilldown(
    db_path: Path | str, *, firm_code: str,
) -> str:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        firm = conn.execute(
            "SELECT * FROM firms WHERE firm_code=?", (firm_code,),
        ).fetchone()
        clients = conn.execute(
            "SELECT * FROM clients WHERE firm_code=?", (firm_code,),
        ).fetchall()
        users = conn.execute(
            "SELECT username, role, first_login_at "
            "FROM dashboard_users WHERE firm_code=?", (firm_code,),
        ).fetchall()
        doc_n = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE firm_code=?",
            (firm_code,),
        ).fetchone()[0]
    if not firm:
        return '<div class="card"><h2>Firm not found</h2></div>'
    client_rows = ''.join(
        f'<tr><td>{_esc(c["client_code"])}</td>'
        f'<td>{_esc(_rget(c, "client_name") or "")}</td>'
        f'<td>{_esc(_rget(c, "portal_token") or "")[:12] or "—"}</td></tr>'
        for c in clients
    ) or '<tr><td colspan="3">No clients yet.</td></tr>'
    user_rows = ''.join(
        f'<tr><td>{_esc(u["username"])}</td>'
        f'<td>{_esc(u["role"])}</td>'
        f'<td>{_esc(_rget(u, "first_login_at") or "never")}</td></tr>'
        for u in users
    ) or '<tr><td colspan="3">No users.</td></tr>'
    return (
        '<div class="card">'
        f'<h2>{_esc(firm["firm_code"])} &mdash; {_esc(_rget(firm, "name") or "(unnamed)")}</h2>'
        f'<p>Plan: <strong>{_esc(_rget(firm, "plan") or "")}</strong></p>'
        f'<p>Documents: {doc_n}</p>'
        f'<form method="POST" action="/owner/firms/{_esc(firm_code)}/impersonate">'
        '<button type="submit" class="primary" '
        'style="background:#856404;color:white;">Impersonate (read-only)</button>'
        '</form>'
        f'<h3>Clients ({len(clients)})</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Code</th><th>Name</th><th>Portal token</th></tr></thead>'
        f'<tbody>{client_rows}</tbody></table>'
        f'<h3>Users ({len(users)})</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>User</th><th>Role</th><th>First login</th></tr></thead>'
        f'<tbody>{user_rows}</tbody></table>'
        '<p style="margin-top:1rem;">'
        '<a href="/owner/dashboard">&larr; Back to owner dashboard</a></p>'
        '</div>'
    )


def render_feedback_queue(db_path: Path | str) -> str:
    _ensure_feedback_schema(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM feedback ORDER BY "
            "CASE WHEN responded_at IS NULL OR responded_at='' THEN 0 ELSE 1 END, "
            "created_at DESC LIMIT 100"
        ).fetchall()
    if not rows:
        return '<div class="card"><h2>Feedback queue</h2><p>No feedback yet.</p></div>'
    tiles = []
    for r in rows:
        r = dict(r)
        responded = bool(r.get('responded_at'))
        bg = '#e8f5e9' if responded else '#fff3cd'
        form = ''
        if not responded:
            form = (
                f'<form method="POST" action="/owner/feedback/{r["id"]}/respond" '
                'style="margin-top:10px;">'
                '<textarea name="response_body" required rows="3" '
                'style="width:100%;padding:6px;"></textarea>'
                '<button type="submit" class="primary" '
                'style="margin-top:6px;">Send response</button>'
                '</form>'
            )
        else:
            form = (
                f'<div style="margin-top:8px;color:#155724;">'
                f'<strong>Responded by {_esc(r.get("responded_by") or "")}:</strong> '
                f'{_esc(r.get("response_body") or "")}</div>'
            )
        tiles.append(
            f'<div style="background:{bg};padding:12px;border-radius:6px;'
            'margin-bottom:10px;">'
            f'<div><strong>#{r["id"]}</strong> — '
            f'{_esc(r.get("subject") or "(no subject)")}</div>'
            f'<div style="color:#666;font-size:13px;">'
            f'{_esc(r.get("submitter_email") or "anonymous")} / '
            f'{_esc(r.get("firm_code") or "")} / {_esc(r.get("created_at") or "")}'
            '</div>'
            f'<div style="margin-top:6px;">{_esc(r.get("body") or "")}</div>'
            f'{form}</div>'
        )
    return (
        '<div class="card"><h2>Feedback queue</h2>'
        + ''.join(tiles)
        + '</div>'
    )


# ---------------------------------------------------------------------------
# Gap 4 — close wizard
# ---------------------------------------------------------------------------


def render_close_wizard(
    db_path: Path | str, *, firm_code: str, client_code: str = '',
    period: str = '', step_n: int = 1,
    flash: str = '', flash_error: str = '',
) -> str:
    step_n = max(1, min(6, step_n))
    state = None
    if client_code and period:
        state = _close.get_state(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period,
        )

    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        clients = conn.execute(
            "SELECT client_code, client_name FROM clients WHERE firm_code=? "
            "ORDER BY client_code", (firm_code,),
        ).fetchall()

    if not client_code or not period:
        # Step 1: picker
        options = ''.join(
            f'<option value="{_esc(c["client_code"])}">{_esc(c["client_code"])} — '
            f'{_esc(_rget(c, "client_name") or "")}</option>'
            for c in clients
        )
        return (
            '<div class="card" style="max-width:600px;margin:1rem auto;">'
            '<h2>Month-end close wizard</h2>'
            f'{flash_html}'
            '<p>Select a client and the period you want to close.</p>'
            '<form method="POST" action="/close/wizard/advance" '
            'style="display:grid;gap:10px;">'
            '<input type="hidden" name="step" value="1">'
            '<label>Client<br>'
            f'<select name="client_code" required style="padding:8px;">'
            '<option value="">— select —</option>'
            f'{options}</select></label>'
            '<label>Period (YYYY-MM)<br>'
            '<input type="text" name="period" pattern="[0-9]{4}-[0-9]{2}" '
            'placeholder="2026-04" required style="padding:8px;"></label>'
            '<button type="submit" class="primary" style="padding:10px 16px;">'
            'Begin close &rarr;</button>'
            '</form></div>'
        )

    steps = state['steps']
    current = state['current']

    # Render stepper
    bar = '<div style="display:flex;gap:4px;margin:1rem 0;">'
    for i, s in enumerate(steps, 1):
        color = '#16C172' if s['step_status'] == 'done' else (
            '#1e40af' if s['step'] == current else '#ddd')
        label = s['step'].replace('_', ' ')
        bar += (
            f'<div style="flex:1;padding:8px;text-align:center;'
            f'background:{color};color:white;border-radius:4px;'
            'font-size:12px;">'
            f'{i}. {label}</div>'
        )
    bar += '</div>'

    # Step-specific body
    body = _render_wizard_step(db_path, firm_code=firm_code,
                                 client_code=client_code, period=period,
                                 step_n=step_n, state=state)

    back_hidden = (
        f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
        f'<input type="hidden" name="period" value="{_esc(period)}">'
        f'<input type="hidden" name="step" value="{step_n}">'
    )
    back_form = ''
    if step_n > 1:
        back_form = (
            '<form method="POST" action="/close/wizard/back" '
            'style="display:inline;">'
            f'{back_hidden}'
            '<button type="submit">&larr; Back</button></form>'
        )
    save_form = (
        '<form method="POST" action="/close/wizard/save_progress" '
        'style="display:inline;margin-left:10px;">'
        f'{back_hidden}'
        '<button type="submit">Save and exit</button></form>'
    )

    return (
        '<div class="card" style="max-width:800px;margin:1rem auto;">'
        f'<h2>Close {_esc(client_code)} — {_esc(period)}</h2>'
        f'{flash_html}{bar}{body}'
        f'<div style="margin-top:1rem;">{back_form}{save_form}</div>'
        '</div>'
    )


_STEP_NAMES = {
    1: 'select_period', 2: 'process_documents', 3: 'reconcile_bank',
    4: 'accruals', 5: 'statements', 6: 'lock',
}


def _render_wizard_step(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str, step_n: int,
    state: dict[str, Any],
) -> str:
    hidden = (
        f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
        f'<input type="hidden" name="period" value="{_esc(period)}">'
        f'<input type="hidden" name="step" value="{step_n}">'
    )
    step_row = next((s for s in state['steps']
                      if s['step'] == _STEP_NAMES.get(step_n)), None)
    done = step_row and step_row.get('step_status') == 'done'

    if step_n == 1:
        return (
            '<h3>Step 1 — confirm period</h3>'
            f'<p>Period <strong>{_esc(period)}</strong> selected. '
            'We will refuse to close this period if any earlier period is '
            'still open.</p>'
            + _advance_button(hidden, done=done, label='Confirm period')
        )
    if step_n == 2:
        return (
            '<h3>Step 2 — process documents</h3>'
            '<p>Every document dated in the period must be posted, ignored, '
            'or deleted before we can continue.</p>'
            + _advance_button(hidden, done=done, label='Mark documents processed')
        )
    if step_n == 3:
        return (
            '<h3>Step 3 — reconcile bank</h3>'
            '<p>Bank transactions must be matched to receipts (or marked as '
            'hidden duplicates). Tick the acknowledge box to bypass any '
            'remaining unmatched rows with an audit note.</p>'
            '<form method="POST" action="/close/wizard/advance" '
            'style="display:inline;">'
            f'{hidden}'
            '<label><input type="checkbox" name="acknowledge_unreconciled" '
            'value="1"> Acknowledge remaining unmatched</label>'
            '<button type="submit" class="primary" '
            'style="display:block;margin-top:10px;">Continue &rarr;</button>'
            '</form>'
        )
    if step_n == 4:
        suggestions = _close.suggest_accruals(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period,
        )
        rows = ''.join(
            '<tr>'
            f'<td><input type="checkbox" name="accepted_kinds" value="{_esc(s["kind"])}"'
            f'{" checked" if float(s.get("amount_cad") or 0)>0 else ""}></td>'
            f'<td>{_esc(s["kind"])}</td>'
            f'<td>{_esc(s["description"])}</td>'
            f'<td>${float(s.get("amount_cad") or 0):,.2f}</td>'
            f'<td style="color:#888;font-size:12px;">{_esc(s.get("amount_hint") or "")}</td>'
            '</tr>'
            for s in suggestions
        )
        return (
            '<h3>Step 4 — accruals</h3>'
            '<p>Tick the accruals you want posted as draft JEs. '
            'Amounts are computed from your current data — zero means '
            'not enough history to suggest one.</p>'
            '<form method="POST" action="/close/wizard/advance">'
            f'{hidden}'
            '<table style="width:100%;border-collapse:collapse;">'
            '<thead><tr><th></th><th>Kind</th><th>Description</th>'
            '<th>Amount (CAD)</th><th>Source</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>'
            '<button type="submit" class="primary" style="margin-top:10px;">'
            'Post all suggested accruals &rarr;</button>'
            '</form>'
        )
    if step_n == 5:
        return (
            '<h3>Step 5 — financial statements</h3>'
            '<p>Generate trial balance, income statement, and balance sheet. '
            'Any unbalanced statement surfaces as a warning.</p>'
            + _advance_button(hidden, done=done, label='Generate statements')
        )
    if step_n == 6:
        # Summary + final lock
        summary_rows = ''.join(
            f'<li>{i}. {s["step"]} — <strong>{_esc(s["step_status"])}</strong></li>'
            for i, s in enumerate(state['steps'], 1)
        )
        return (
            '<h3>Step 6 — lock period</h3>'
            f'<ul>{summary_rows}</ul>'
            '<p>Locking this period prevents further posts. You can still '
            'view reports but edits are refused. This is the final step.</p>'
            '<form method="POST" action="/close/wizard/finalize">'
            f'{hidden}'
            '<button type="submit" class="primary" '
            'style="background:#dc2626;color:white;padding:10px 20px;">'
            'Lock period</button></form>'
        )
    return '<p>Unknown step.</p>'


def _advance_button(hidden: str, *, done: bool, label: str) -> str:
    if done:
        return (
            '<p style="color:#155724;"><strong>&#10003; Already done.</strong> '
            'Click next to continue.</p>'
            '<form method="POST" action="/close/wizard/advance">'
            f'{hidden}'
            f'<button type="submit" class="primary">Next &rarr;</button></form>'
        )
    return (
        '<form method="POST" action="/close/wizard/advance">'
        f'{hidden}'
        f'<button type="submit" class="primary">{_esc(label)}</button></form>'
    )


# ---------------------------------------------------------------------------
# Gap 5 — client portal status + activity + messages
# ---------------------------------------------------------------------------


def render_portal_status_page(
    db_path: Path | str, *, client: dict[str, Any], token: str,
) -> str:
    code = client['client_code']
    bundle = _cs.build_client_status(db_path, client_code=code)
    up = bundle['upload_status']
    ytd = bundle['ytd_summary']
    notif = bundle['unread_notifications']
    threads = bundle['threads']
    act = bundle['recent_activity']

    tiles = (
        '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));'
        'gap:12px;">'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{up["this_month"]}</div>'
        '<div>Uploads this month</div></div>'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{up["processing"]}</div>'
        '<div>Being processed</div></div>'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{up["reviewed"]}</div>'
        '<div>Reviewed</div></div>'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{notif}</div>'
        '<div>Unread notifications</div></div>'
        '</div>'
    )

    act_rows = ''.join(
        f'<li><span style="color:#888;">{_esc(a.get("ts") or "")}</span> — '
        f'{_esc(a.get("summary") or "")}</li>'
        for a in act[:20]
    ) or '<li>No activity yet.</li>'

    thread_rows = ''
    for th in threads[:10]:
        tid = th['id']
        unread = th.get('unread_from_cpa') or 0
        badge = (f'<span style="background:#dc2626;color:white;'
                  f'padding:2px 6px;border-radius:10px;font-size:11px;">'
                  f'{unread}</span>') if unread else ''
        thread_rows += (
            f'<li><a href="/c/{_esc(token)}/messages?thread={tid}">'
            f'{_esc(th.get("subject") or "(no subject)")}</a> {badge}</li>'
        )
    if not thread_rows:
        thread_rows = '<li>No message threads yet.</li>'

    return (
        '<!DOCTYPE html><html><head>'
        '<meta charset="utf-8"><title>Your portal</title>'
        '<style>'
        'body{font-family:system-ui,Arial;max-width:900px;margin:2rem auto;padding:1rem;}'
        '.tile{background:#f3f4f6;padding:14px;border-radius:6px;text-align:center;}'
        'h2,h3{margin-top:1.4rem;}a{color:#1e40af;}'
        '</style></head><body>'
        f'<h1>{_esc(client.get("client_name") or code)}</h1>'
        '<p><a href="/c/' + _esc(token) + '/upload">Upload receipts</a> &middot; '
        '<a href="/c/' + _esc(token) + '/messages">Messages</a></p>'
        f'{tiles}'
        f'<h3>Recent activity</h3><ul>{act_rows}</ul>'
        '<p><a href="#" onclick="refreshActivity();return false;">Refresh</a></p>'
        f'<h3>YTD summary ({ytd["year"]})</h3>'
        f'<p>Total receipts: <strong>{ytd["total_receipts"]}</strong> &middot; '
        f'Expenses: <strong>${ytd["total_expenses_cad"]:,.2f}</strong> &middot; '
        f'This month: {ytd["this_month"]} &middot; '
        f'Prior month: {ytd["prior_month"]}</p>'
        f'<h3>Message threads</h3><ul>{thread_rows}</ul>'
        '<script>'
        'function refreshActivity(){'
        'fetch("/c/' + _esc(token) + '/activity").then(r=>r.json()).then(()=>{'
        'location.reload();});}'
        'setInterval(()=>fetch("/c/' + _esc(token) + '/activity").catch(()=>{}), 60000);'
        '</script></body></html>'
    )


def render_portal_messages_page(
    db_path: Path | str, *, client: dict[str, Any], token: str,
    thread_id: int | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    code = client['client_code']
    threads = _cs.list_threads(db_path, client_code=code)
    flash_html = ''
    if flash:
        flash_html = f'<div style="background:#d4edda;padding:8px;">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div style="background:#f8d7da;padding:8px;">{_esc(flash_error)}</div>'

    thread_sidebar = ''.join(
        '<li>'
        f'<a href="/c/{_esc(token)}/messages?thread={t["id"]}">'
        f'{_esc(t.get("subject") or "(no subject)")}</a>'
        f' <span style="color:#888;font-size:12px;">({t.get("unread_from_cpa") or 0} unread)</span>'
        '</li>'
        for t in threads
    ) or '<li>No threads yet.</li>'

    thread_body = ''
    if thread_id:
        tdata = _cs.get_thread(db_path, thread_id=thread_id,
                                 mark_read_as='client')
        header = tdata.get('header') or {}
        if header and header.get('client_code') == code:
            posts = tdata.get('posts') or []
            post_rows = ''.join(
                f'<div style="margin-bottom:8px;padding:8px;background:'
                f'{"#e8f0fe" if p["sender_type"] == "cpa" else "#f3f4f6"};'
                'border-radius:6px;">'
                f'<div style="font-size:12px;color:#555;">{_esc(p["sender_type"])} '
                f'— {_esc(p.get("created_at") or "")}</div>'
                f'<div>{_esc(p.get("body") or "")}</div></div>'
                for p in posts
            )
            thread_body = (
                f'<h3>{_esc(header.get("subject") or "")}</h3>'
                f'{post_rows}'
                '<form method="POST" '
                f'action="/c/{_esc(token)}/messages/send" '
                'style="margin-top:1rem;">'
                f'<input type="hidden" name="thread_id" value="{thread_id}">'
                '<textarea name="body" rows="3" required '
                'style="width:100%;padding:6px;"></textarea>'
                '<button type="submit" class="primary" style="margin-top:6px;">'
                'Send</button></form>'
            )
        else:
            thread_body = '<p>Thread not found.</p>'

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Messages</title>'
        '<style>body{font-family:system-ui,Arial;max-width:900px;'
        'margin:2rem auto;padding:1rem;}a{color:#1e40af;}</style></head><body>'
        f'<p><a href="/c/{_esc(token)}/status">&larr; Back to status</a></p>'
        f'<h2>Messages</h2>{flash_html}'
        '<div style="display:grid;grid-template-columns:220px 1fr;gap:20px;">'
        f'<aside><ul>{thread_sidebar}</ul>'
        '<form method="POST" '
        f'action="/c/{_esc(token)}/messages/send" '
        'style="margin-top:1rem;padding-top:1rem;border-top:1px solid #eee;">'
        '<input type="hidden" name="new_thread" value="1">'
        '<input type="text" name="subject" placeholder="Subject" required '
        'style="width:100%;padding:6px;margin-bottom:6px;">'
        '<textarea name="body" rows="2" placeholder="Start a new thread..." '
        'required style="width:100%;padding:6px;"></textarea>'
        '<button type="submit" style="margin-top:6px;">New thread</button>'
        '</form>'
        '</aside>'
        f'<main>{thread_body or "<p>Select a thread.</p>"}</main>'
        '</div></body></html>'
    )
