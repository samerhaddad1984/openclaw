"""HTTP dispatch for Gap 1-5 routes.

Thin adapters that translate BaseHTTPRequestHandler paths into
gap_routes/engine function calls. The dashboard calls
``gap_dispatch.dispatch_get`` and ``dispatch_post`` as a single hook
each; they return True once a request is handled so the monolith's
default 404 doesn't fire.

Impersonation write-blocks are enforced here — any POST that mutates
firm data checks ``block_write_if_impersonating`` and 403s with an
audit-logged entry when an owner is impersonating.
"""
from __future__ import annotations

import html as _html
import json
import logging
import re
import sqlite3
import urllib.parse
from pathlib import Path
from typing import Any, Callable

from src.integrations import client_status as _cs
from src.integrations import gap_routes as _gr
from src.integrations import impersonation as _imp
from src.integrations import month_end_close as _close
from src.integrations import notification_sender as _notify
from src.integrations import onboarding_checklist as _ob
from src.integrations import review_workflow as _rw

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Re-used matchers
# ---------------------------------------------------------------------------

_DOC_ACTION_RE = re.compile(r'^/document/([^/]+)/(submit_for_review|approve|reject|escalate|assign)$')
_PORTAL_STATUS_RE = re.compile(r'^/c/([^/]+)/(status|activity|messages|messages/send|messages/mark_read)$')
_OWNER_FIRM_RE = re.compile(r'^/owner/firms/([^/]+)(?:/(impersonate))?$')
_OWNER_FEEDBACK_RE = re.compile(r'^/owner/feedback/(\d+)/respond$')
_CLOSE_STEP_RE = re.compile(r'^/close/wizard/step/(\d+)$')


# ---------------------------------------------------------------------------
# GET dispatch
# ---------------------------------------------------------------------------


def dispatch_get(
    handler, *,
    db_path: Path | str,
    user: dict[str, Any] | None,
    ctx: dict[str, Any] | None,
    path: str,
    qs: dict[str, list[str]],
    lang: str,
    flash: str,
    flash_error: str,
    page_layout: Callable[..., str],
) -> bool:
    # -------------------- Gap 5: portal (no session auth) ----------------
    m = _PORTAL_STATUS_RE.match(path)
    if m:
        from scripts.review_dashboard import (  # type: ignore
            resolve_portal_token,
        )
        token, section = m.group(1), m.group(2)
        client = resolve_portal_token(token)
        if not client:
            _send_html(handler, '<h1>Invalid link</h1>', status=404)
            return True
        if section == 'status':
            html_str = _gr.render_portal_status_page(
                db_path, client=dict(client), token=token,
            )
            _send_html(handler, html_str)
            return True
        if section == 'activity':
            events = _cs.recent_activity(
                db_path, client_code=client['client_code'],
            )
            _send_json(handler, {'events': events})
            return True
        if section == 'messages':
            thread_id_raw = qs.get('thread', [''])[0]
            thread_id = int(thread_id_raw) if thread_id_raw.isdigit() else None
            html_str = _gr.render_portal_messages_page(
                db_path, client=dict(client), token=token,
                thread_id=thread_id,
                flash=flash, flash_error=flash_error,
            )
            _send_html(handler, html_str)
            return True

    # -------------------- Auth-required below --------------------
    if user is None or ctx is None:
        return False

    # -------------------- Gap 1: onboarding + tour --------------------
    if path == '/onboarding':
        body = _gr.render_onboarding_quick_setup(
            db_path, firm_code=ctx.get('firm_code') or 'OWNER',
            lang=lang, flash=flash, flash_error=flash_error,
        )
        _send_layout(handler, page_layout, 'Quick setup', body, user=user,
                     lang=lang)
        return True

    if path == '/onboarding/checklist':
        items = _ob.compute_checklist(
            db_path, firm_code=ctx.get('firm_code') or 'OWNER',
            username=user.get('username'),
        )
        _send_json(handler, {'items': items,
                             'all_done': _ob.all_done(items)})
        return True

    if path == '/tour':
        try:
            step = int(qs.get('step', ['1'])[0])
        except ValueError:
            step = 1
        # Explicit ?lang=fr|en overrides the session lang so the
        # language switcher on the tour page works even when the
        # user's profile is set to the other language.
        tour_lang_qs = (qs.get('lang', [''])[0] or '').strip()
        tour_lang = tour_lang_qs if tour_lang_qs in ('fr', 'en') else lang
        body = _gr.render_tour_screens(step, lang=tour_lang)
        _send_layout(handler, page_layout, 'Tour', body, user=user,
                     lang=tour_lang)
        return True

    # -------------------- Gap 2: review queue --------------------
    if path == '/my_tasks':
        body = _gr.render_my_tasks(
            db_path, assignee_email=user.get('username') or '',
            lang=lang, flash=flash, flash_error=flash_error,
        )
        _send_layout(handler, page_layout, 'My tasks', body, user=user,
                     lang=lang, flash=flash, flash_error=flash_error)
        return True

    if path == '/review_queue':
        if ctx.get('role') not in ('firm_admin', 'owner'):
            _send_layout(handler, page_layout, 'Forbidden',
                         '<div class="card"><p>Firm admin or owner only.</p></div>',
                         user=user, lang=lang, status=403)
            return True
        body = _gr.render_review_queue(
            db_path, firm_code=ctx.get('firm_code') or 'OWNER',
            lang=lang, flash=flash, flash_error=flash_error,
        )
        _send_layout(handler, page_layout, 'Review queue', body, user=user,
                     lang=lang, flash=flash, flash_error=flash_error)
        return True

    # -------------------- Gap 3: owner dashboard + firms --------------------
    if path == '/owner/dashboard':
        if user.get('role') != 'owner':
            _send_layout(handler, page_layout, 'Forbidden',
                         '<div class="card"><p>Owner only.</p></div>',
                         user=user, lang=lang, status=403)
            return True
        body = _gr.render_owner_dashboard(db_path, flash=flash,
                                           flash_error=flash_error)
        _send_layout(handler, page_layout, 'Owner dashboard', body,
                     user=user, lang=lang,
                     flash=flash, flash_error=flash_error)
        return True

    if path == '/owner/dashboard/metrics':
        if user.get('role') != 'owner':
            _send_json(handler, {'error': 'forbidden'}, status=403)
            return True
        from src.integrations.owner_dashboard import build_dashboard
        _send_json(handler, build_dashboard(db_path))
        return True

    m = _OWNER_FIRM_RE.match(path)
    if m:
        if user.get('role') != 'owner':
            _send_layout(handler, page_layout, 'Forbidden',
                         '<div class="card"><p>Owner only.</p></div>',
                         user=user, lang=lang, status=403)
            return True
        firm_code = m.group(1)
        sub = m.group(2)
        if sub == 'impersonate':
            # GET /owner/firms/X/impersonate confirms then redirects POST
            body = (
                '<div class="card" style="max-width:600px;margin:1rem auto;">'
                f'<h2>Impersonate {_html.escape(firm_code)}?</h2>'
                '<p>This gives you a <strong>read-only</strong> view of the '
                'firm. Every action is audit-logged with your account as the '
                'originator. Writes / posts / deletes will be refused.</p>'
                '<form method="POST" '
                f'action="/owner/firms/{_html.escape(firm_code)}/impersonate">'
                '<button type="submit" class="primary" '
                'style="background:#856404;color:white;">Start impersonation</button>'
                '</form>'
                '<p><a href="/owner/dashboard">Cancel</a></p></div>'
            )
        else:
            body = _gr.render_firm_drilldown(db_path, firm_code=firm_code)
        _send_layout(handler, page_layout, 'Firm drilldown', body,
                     user=user, lang=lang)
        return True

    if path == '/owner/feedback':
        if user.get('role') != 'owner':
            _send_layout(handler, page_layout, 'Forbidden',
                         '<div class="card"><p>Owner only.</p></div>',
                         user=user, lang=lang, status=403)
            return True
        body = _gr.render_feedback_queue(db_path)
        _send_layout(handler, page_layout, 'Feedback', body,
                     user=user, lang=lang)
        return True

    # -------------------- Gap 4: close wizard --------------------
    if path == '/close/wizard':
        client_code = qs.get('client_code', [''])[0].strip()
        period = qs.get('period', [''])[0].strip()
        body = _gr.render_close_wizard(
            db_path, firm_code=ctx.get('firm_code') or 'OWNER',
            client_code=client_code, period=period, step_n=1,
            flash=flash, flash_error=flash_error,
        )
        _send_layout(handler, page_layout, 'Close wizard', body,
                     user=user, lang=lang, flash=flash,
                     flash_error=flash_error)
        return True

    m = _CLOSE_STEP_RE.match(path)
    if m:
        try:
            step_n = int(m.group(1))
        except ValueError:
            step_n = 1
        client_code = qs.get('client_code', [''])[0].strip()
        period = qs.get('period', [''])[0].strip()
        body = _gr.render_close_wizard(
            db_path, firm_code=ctx.get('firm_code') or 'OWNER',
            client_code=client_code, period=period, step_n=step_n,
            flash=flash, flash_error=flash_error,
        )
        _send_layout(handler, page_layout, f'Close wizard — step {step_n}',
                     body, user=user, lang=lang, flash=flash,
                     flash_error=flash_error)
        return True

    if path == '/close/wizard/summary':
        client_code = qs.get('client_code', [''])[0].strip()
        period = qs.get('period', [''])[0].strip()
        body = _gr.render_close_wizard(
            db_path, firm_code=ctx.get('firm_code') or 'OWNER',
            client_code=client_code, period=period, step_n=6,
            flash=flash, flash_error=flash_error,
        )
        _send_layout(handler, page_layout, 'Close summary', body,
                     user=user, lang=lang)
        return True

    return False


# ---------------------------------------------------------------------------
# POST dispatch
# ---------------------------------------------------------------------------


def dispatch_post(
    handler, *,
    db_path: Path | str,
    user: dict[str, Any] | None,
    ctx: dict[str, Any] | None,
    path: str,
    form: dict[str, str],
    qs: dict[str, list[str]],
    lang: str,
    raw: bytes,
) -> bool:
    # -------------------- Gap 5: portal (token auth) ----------------
    m = _PORTAL_STATUS_RE.match(path)
    if m:
        from scripts.review_dashboard import resolve_portal_token  # type: ignore
        token, section = m.group(1), m.group(2)
        client = resolve_portal_token(token)
        if not client:
            _send_json(handler, {'error': 'invalid_token'}, status=404)
            return True
        if section == 'messages/send':
            return _handle_portal_send_message(handler, db_path, client,
                                                 token, form)
        if section == 'messages/mark_read':
            _cs.mark_notifications_read(
                db_path, client_code=client['client_code'],
            )
            _send_json(handler, {'ok': True})
            return True

    # -------------------- Auth-required below --------------------
    if user is None or ctx is None:
        return False

    # -------------------- Gap 1: onboarding POSTs --------------------
    if path == '/onboarding/save':
        return _handle_onboarding_save(handler, db_path, ctx, form)

    if path == '/onboarding/dismiss':
        _ob.dismiss(db_path, username=user.get('username') or '')
        _redirect(handler, '/')
        return True

    if path == '/onboarding/checklist/dismiss':
        _ob.dismiss(db_path, username=user.get('username') or '')
        _redirect(handler, qs.get('next', ['/'])[0] or '/')
        return True

    if path == '/tour/complete':
        _ob.mark_welcome_seen(db_path, username=user.get('username') or '',
                                tour_taken=True)
        _redirect(handler, '/')
        return True

    if path == '/onboarding/welcome/ack':
        tour = form.get('tour', '0') == '1'
        _ob.mark_welcome_seen(db_path, username=user.get('username') or '',
                                tour_taken=tour)
        _redirect(handler, '/tour' if tour else '/')
        return True

    # -------------------- Gap 2: review workflow POSTs --------------------
    m = _DOC_ACTION_RE.match(path)
    if m:
        return _handle_doc_action(
            handler, db_path, user, ctx,
            entity_id=m.group(1), action=m.group(2), form=form, path=path,
        )

    if path == '/review_queue/bulk_approve':
        return _handle_bulk_approve(handler, db_path, user, ctx, form)

    # -------------------- Gap 3: owner admin POSTs --------------------
    m = _OWNER_FIRM_RE.match(path)
    if m and m.group(2) == 'impersonate':
        if user.get('role') != 'owner':
            _send_json(handler, {'error': 'forbidden'}, status=403)
            return True
        firm_code = m.group(1)
        sid = _imp.start(db_path,
                          original_user_email=user.get('username') or '',
                          firm_code=firm_code)
        handler.send_response(303)
        handler.send_header('Location', '/')
        hdr = _gr.set_imp_cookie(sid)
        handler.send_header(hdr[0], hdr[1])
        handler.end_headers()
        return True

    if path == '/owner/impersonate/stop':
        sid = _gr.read_imp_cookie(handler)
        if sid:
            _imp.stop(db_path, session_id=sid)
        handler.send_response(303)
        handler.send_header('Location', '/owner/dashboard')
        hdr = _gr.clear_imp_cookie()
        handler.send_header(hdr[0], hdr[1])
        handler.end_headers()
        return True

    m = _OWNER_FEEDBACK_RE.match(path)
    if m:
        if user.get('role') != 'owner':
            _send_json(handler, {'error': 'forbidden'}, status=403)
            return True
        feedback_id = int(m.group(1))
        body = form.get('response_body', '').strip()
        if not body:
            _redirect(handler, '/owner/feedback?error=empty')
            return True
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE feedback SET response_body=?, responded_by=?, "
                "responded_at=? WHERE id=?",
                (body, user.get('username') or '',
                 _gr._iso_now(), feedback_id),
            )
            conn.commit()
        _redirect(handler, '/owner/feedback?flash=responded')
        return True

    # -------------------- Gap 4: close wizard POSTs --------------------
    if path == '/close/wizard/advance':
        return _handle_wizard_advance(handler, db_path, user, ctx, form)

    if path == '/close/wizard/back':
        return _handle_wizard_back(handler, db_path, form)

    if path == '/close/wizard/save_progress':
        client_code = form.get('client_code', '')
        period = form.get('period', '')
        step = form.get('step', '1')
        _redirect(handler,
                   f'/?flash=Close+progress+saved+({_esc(client_code)}/{_esc(period)}+step+{_esc(step)})')
        return True

    if path == '/close/wizard/resume':
        client_code = form.get('client_code', '') or qs.get('client_code', [''])[0]
        period = form.get('period', '') or qs.get('period', [''])[0]
        _redirect(handler,
                   f'/close/wizard?client_code={_esc(client_code)}&period={_esc(period)}')
        return True

    if path == '/close/wizard/finalize':
        return _handle_wizard_finalize(handler, db_path, user, ctx, form)

    return False


# ---------------------------------------------------------------------------
# Handler helpers
# ---------------------------------------------------------------------------


def _handle_onboarding_save(handler, db_path, ctx, form) -> bool:
    if _gr.block_write_if_impersonating(
        db_path, handler, action='onboarding_save',
        path='/onboarding/save', method='POST',
    ):
        _send_html(handler, _imp.forbidden_write_response_html(), status=403)
        return True
    firm_code = ctx.get('firm_code') or 'OWNER'
    fields = {
        'name': form.get('name', '').strip(),
        'address': form.get('address', '').strip(),
        'phone': form.get('phone', '').strip(),
        'default_lang': form.get('default_lang', 'en').strip(),
        'fiscal_year_end': form.get('fiscal_year_end', '').strip(),
    }
    if not fields['name'] or not fields['address'] or not fields['phone']:
        _redirect(handler, '/onboarding?error=Missing+required+fields')
        return True
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        existing = conn.execute(
            "SELECT 1 FROM firms WHERE firm_code=?", (firm_code,),
        ).fetchone()
        cols_info = conn.execute("PRAGMA table_info(firms)").fetchall()
        have_cols = {r[1] for r in cols_info}
        # Extend firms table with our profile columns when missing.
        wanted_cols = {
            'address': 'TEXT', 'phone': 'TEXT',
            'default_lang': "TEXT DEFAULT 'en'",
            'fiscal_year_end': 'TEXT',
        }
        for col, ddl in wanted_cols.items():
            if col not in have_cols:
                try:
                    conn.execute(f"ALTER TABLE firms ADD COLUMN {col} {ddl}")
                except sqlite3.OperationalError:
                    pass
        if existing:
            conn.execute(
                "UPDATE firms SET name=?, address=?, phone=?, "
                "default_lang=?, fiscal_year_end=? WHERE firm_code=?",
                (fields['name'], fields['address'], fields['phone'],
                 fields['default_lang'], fields['fiscal_year_end'],
                 firm_code),
            )
        else:
            conn.execute(
                "INSERT INTO firms (firm_code, name, address, phone, "
                "default_lang, fiscal_year_end) VALUES (?,?,?,?,?,?)",
                (firm_code, fields['name'], fields['address'],
                 fields['phone'], fields['default_lang'],
                 fields['fiscal_year_end']),
            )
        conn.commit()
    _redirect(handler, '/?flash=Firm+profile+saved')
    return True


def _handle_doc_action(handler, db_path, user, ctx, *,
                        entity_id, action, form, path) -> bool:
    if _gr.block_write_if_impersonating(
        db_path, handler, action=f'doc_{action}', path=path, method='POST',
    ):
        _send_html(handler, _imp.forbidden_write_response_html(), status=403)
        return True
    firm_code = ctx.get('firm_code') or 'OWNER'
    actor = user.get('username') or ''
    role = ctx.get('role') or user.get('role') or 'employee'
    try:
        if action == 'submit_for_review':
            _rw.submit_for_review(
                db_path, firm_code=firm_code, entity_type='document',
                entity_id=entity_id, actor_email=actor, actor_role=role,
                notes=form.get('notes', '') or None,
            )
            _redirect(handler, '/my_tasks?flash=Submitted+for+review')
            return True
        if action == 'approve':
            _rw.approve(
                db_path, firm_code=firm_code, entity_type='document',
                entity_id=entity_id, actor_email=actor, actor_role=role,
                notes=form.get('notes', '') or None,
            )
            _try_notify_client_on_approval(db_path, firm_code, entity_id)
            _redirect(handler, '/review_queue?flash=Approved')
            return True
        if action == 'reject':
            reason = form.get('reason', '').strip()
            if not reason:
                _redirect(handler, '/review_queue?error=Reason+required')
                return True
            _rw.reject(
                db_path, firm_code=firm_code, entity_type='document',
                entity_id=entity_id, actor_email=actor, actor_role=role,
                reason=reason,
            )
            _redirect(handler, '/review_queue?flash=Rejected')
            return True
        if action == 'escalate':
            _rw.escalate(
                db_path, firm_code=firm_code, entity_type='document',
                entity_id=entity_id, actor_email=actor, actor_role=role,
                notes=form.get('notes', '') or None,
            )
            _redirect(handler, '/my_tasks?flash=Escalated')
            return True
        if action == 'assign':
            target = form.get('assignee_email', '').strip()
            if not target:
                _redirect(handler, '/review_queue?error=Missing+assignee')
                return True
            _rw.assign(
                db_path, firm_code=firm_code, entity_type='document',
                entity_id=entity_id, assignee_email=target,
                actor_email=actor, actor_role=role,
                priority=form.get('priority', 'normal'),
            )
            _try_notify_assignee(db_path, firm_code, target, entity_id,
                                  priority=form.get('priority', 'normal'))
            _redirect(handler, '/review_queue?flash=Assigned')
            return True
    except (_rw.WorkflowPermissionError, _rw.WorkflowStateError,
             ValueError) as exc:
        _redirect(handler, f'/review_queue?error={urllib.parse.quote(str(exc))}')
        return True
    return False


def _handle_bulk_approve(handler, db_path, user, ctx, form) -> bool:
    if _gr.block_write_if_impersonating(
        db_path, handler, action='bulk_approve',
        path='/review_queue/bulk_approve', method='POST',
    ):
        _send_html(handler, _imp.forbidden_write_response_html(), status=403)
        return True
    firm_code = ctx.get('firm_code') or 'OWNER'
    actor = user.get('username') or ''
    role = ctx.get('role') or user.get('role') or 'employee'
    # parse_form_body squashes repeated values; reparse for multi-values.
    pairs = urllib.parse.parse_qsl(form.get('__raw__', ''), keep_blank_values=True)
    ids = [v for k, v in pairs if k == 'entity_ids']
    if not ids:
        # Fallback: manually parse from the form dict (may have only one id)
        if 'entity_ids' in form and form['entity_ids']:
            ids = [form['entity_ids']]
    try:
        result = _rw.bulk_approve(
            db_path, firm_code=firm_code, entity_type='document',
            entity_ids=ids, actor_email=actor, actor_role=role,
        )
    except _rw.WorkflowPermissionError as exc:
        _redirect(handler, f'/review_queue?error={urllib.parse.quote(str(exc))}')
        return True
    msg = (f'Approved+{len(result["approved"])}+of+{result["total"]}+'
            f'items+(skipped+{len(result["skipped"])})')
    _redirect(handler, f'/review_queue?flash={msg}')
    return True


def _handle_portal_send_message(handler, db_path, client, token, form) -> bool:
    code = client['client_code']
    firm = client.get('firm_code') or 'OWNER'
    body_text = form.get('body', '').strip()
    if not body_text:
        _redirect(handler, f'/c/{token}/messages?error=Empty+message')
        return True
    new_thread = form.get('new_thread', '') == '1'
    thread_id_raw = form.get('thread_id', '').strip()
    if new_thread or not thread_id_raw:
        subject = form.get('subject', '').strip() or 'New message'
        thread_id = _cs.create_thread(
            db_path, firm_code=firm, client_code=code, subject=subject,
        )
    else:
        try:
            thread_id = int(thread_id_raw)
        except ValueError:
            _redirect(handler, f'/c/{token}/messages?error=Bad+thread')
            return True
    _cs.post_message(
        db_path, thread_id=thread_id, sender_type='client',
        sender_id=code, body=body_text,
    )
    _redirect(handler, f'/c/{token}/messages?thread={thread_id}&flash=Sent')
    return True


def _handle_wizard_advance(handler, db_path, user, ctx, form) -> bool:
    if _gr.block_write_if_impersonating(
        db_path, handler, action='wizard_advance',
        path='/close/wizard/advance', method='POST',
    ):
        _send_html(handler, _imp.forbidden_write_response_html(), status=403)
        return True
    firm_code = ctx.get('firm_code') or 'OWNER'
    actor = user.get('username') or ''
    client_code = form.get('client_code', '').strip()
    period = form.get('period', '').strip()
    try:
        step = int(form.get('step', '1'))
    except ValueError:
        step = 1
    if not client_code or not period:
        _redirect(handler, '/close/wizard?error=Missing+client+or+period')
        return True

    if step == 1:
        r = _close.complete_step_1_select_period(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period, actor_email=actor,
        )
        _wizard_redirect(handler, r, client_code, period, 2)
        return True
    if step == 2:
        r = _close.complete_step_2_process_documents(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period, actor_email=actor,
        )
        _wizard_redirect(handler, r, client_code, period, 3)
        return True
    if step == 3:
        ack = form.get('acknowledge_unreconciled', '') == '1'
        r = _close.complete_step_3_reconcile_bank(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period, acknowledge_unreconciled=ack,
            actor_email=actor,
        )
        _wizard_redirect(handler, r, client_code, period, 4)
        return True
    if step == 4:
        pairs = urllib.parse.parse_qsl(form.get('__raw__', ''),
                                         keep_blank_values=True)
        kinds = [v for k, v in pairs if k == 'accepted_kinds']
        if not kinds and form.get('accepted_kinds'):
            kinds = [form['accepted_kinds']]
        # Auto-compute + post the accepted accruals.
        posted = _close.post_suggested_accruals(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period, accepted_kinds=kinds, actor_email=actor,
        )
        r = _close.complete_step_4_accruals(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period, accepted_kinds=kinds, actor_email=actor,
        )
        r.setdefault('posted', posted.get('posted'))
        _wizard_redirect(handler, r, client_code, period, 5)
        return True
    if step == 5:
        r = _close.complete_step_5_statements(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period, actor_email=actor,
        )
        _wizard_redirect(handler, r, client_code, period, 6)
        return True
    if step == 6:
        _redirect(handler,
                   f'/close/wizard/step/6?client_code={_esc(client_code)}&period={_esc(period)}')
        return True
    return False


def _handle_wizard_back(handler, db_path, form) -> bool:
    client_code = form.get('client_code', '')
    period = form.get('period', '')
    try:
        step = int(form.get('step', '1'))
    except ValueError:
        step = 1
    prev = max(1, step - 1)
    _redirect(handler,
               f'/close/wizard/step/{prev}?client_code={_esc(client_code)}&period={_esc(period)}')
    return True


def _handle_wizard_finalize(handler, db_path, user, ctx, form) -> bool:
    if _gr.block_write_if_impersonating(
        db_path, handler, action='wizard_finalize',
        path='/close/wizard/finalize', method='POST',
    ):
        _send_html(handler, _imp.forbidden_write_response_html(), status=403)
        return True
    firm_code = ctx.get('firm_code') or 'OWNER'
    actor = user.get('username') or ''
    client_code = form.get('client_code', '').strip()
    period = form.get('period', '').strip()
    if not client_code or not period:
        _redirect(handler, '/close/wizard?error=Missing+client+or+period')
        return True
    r = _close.complete_step_6_lock(
        db_path, firm_code=firm_code, client_code=client_code,
        period=period, actor_email=actor,
    )
    if r.get('ok'):
        _redirect(handler,
                   f'/close/wizard/step/6?client_code={_esc(client_code)}&period={_esc(period)}&flash=Period+locked')
    else:
        _redirect(handler,
                   f'/close/wizard/step/6?client_code={_esc(client_code)}&period={_esc(period)}&error={urllib.parse.quote(r.get("message","Lock+failed"))}')
    return True


def _wizard_redirect(handler, result, client_code, period, next_step) -> None:
    base = f'/close/wizard/step/{next_step if result.get("ok") else result.get("step", next_step - 1)}'
    qs = f'?client_code={_esc(client_code)}&period={_esc(period)}'
    if result.get('ok'):
        _redirect(handler, f'{base}{qs}&flash=Step+complete')
    else:
        msg = result.get('message', 'Step+blocked')
        current_step = next_step - 1
        _redirect(handler, f'/close/wizard/step/{current_step}{qs}'
                             f'&error={urllib.parse.quote(msg)}')


def _try_notify_client_on_approval(db_path, firm_code, doc_id) -> None:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT d.client_code, c.email "
                "FROM documents d LEFT JOIN clients c "
                "  ON c.client_code=d.client_code "
                "WHERE d.document_id=?", (doc_id,),
            ).fetchone()
        if not row or not row['client_code']:
            return
        email = row['email'] or ''
        if email:
            _notify.notify_receipt_approved(
                db_path, client_code=row['client_code'],
                recipient_email=email, document_id=doc_id,
            )
    except Exception:
        log.exception('notify_receipt_approved failed')


def _try_notify_assignee(db_path, firm_code, assignee, entity_id,
                           *, priority='normal') -> None:
    try:
        _notify.notify_review_assigned(
            db_path, firm_code=firm_code, assignee_email=assignee,
            entity_type='document', entity_id=entity_id, priority=priority,
        )
    except Exception:
        log.exception('notify_review_assigned failed')


# ---------------------------------------------------------------------------
# Tiny response helpers (avoid reaching into handler internals directly)
# ---------------------------------------------------------------------------


def _send_html(handler, content: str, status: int = 200,
                extra_headers: list[tuple[str, str]] | None = None) -> None:
    body = content.encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'text/html; charset=utf-8')
    handler.send_header('Content-Length', str(len(body)))
    if extra_headers:
        for k, v in extra_headers:
            handler.send_header(k, v)
    handler.end_headers()
    handler.wfile.write(body)


def _send_json(handler, data: Any, status: int = 200) -> None:
    body = json.dumps(data, default=str).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _send_layout(handler, page_layout, title, body_html, *,
                   user=None, lang='en', flash='', flash_error='',
                   status=200) -> None:
    html_str = page_layout(title, body_html, user=user, lang=lang,
                             flash=flash, flash_error=flash_error)
    _send_html(handler, html_str, status=status)


def _redirect(handler, location: str) -> None:
    handler.send_response(303)
    handler.send_header('Location', location)
    handler.end_headers()


def _esc(s: Any) -> str:
    return urllib.parse.quote(str(s or ''), safe='-_.~')
