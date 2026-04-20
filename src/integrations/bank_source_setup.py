"""Smart bank-setup decision engine + HTML renders + HTTP handlers.

At client setup we want to answer one question: which source(s) of
bank data should feed this client's reconciliation — QBO, Plaid, or
both? The decision depends on three inputs:

- Is QBO connected?                       (qbo_connections row, status='active')
- Does QBO have bank feeds?               (>=1 Bank account AND >=1 Purchase)
- Is Plaid connected?                     (bank_connections row, active=1)

Five states, five UX messages. The engine returns a pure dict so the
dashboard can render it with a simple template, and the HTTP handlers
can expose trigger endpoints (Sync-from-QBO, promote to 'both', etc.).
"""
from __future__ import annotations

import html
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


STATE_QBO_ONLY = 'qbo_recommended'
STATE_PLAID_RECOMMENDED = 'plaid_recommended'
STATE_PLAID_ACTIVE = 'plaid_active'
STATE_BOTH_ACTIVE = 'both_active'
STATE_CHOICE = 'choice'


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------


def _has_qbo_connection(db_path: Path | str,
                         firm_code: str, client_code: str) -> bool:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM qbo_connections "
            "WHERE firm_code=? AND client_code=? AND status='active' LIMIT 1",
            (firm_code, client_code),
        ).fetchone()
    return bool(row)


def _has_plaid_connection(db_path: Path | str, client_code: str) -> bool:
    with _open(db_path) as conn:
        try:
            row = conn.execute(
                "SELECT 1 FROM bank_connections "
                "WHERE client_code=? AND active=1 LIMIT 1",
                (client_code,),
            ).fetchone()
        except sqlite3.OperationalError:
            return False
    return bool(row)


def _current_bank_source(db_path: Path | str, client_code: str) -> str:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT bank_source FROM clients WHERE client_code=?",
            (client_code,),
        ).fetchone()
    return (row['bank_source'] if row and row['bank_source']
             else 'none')


def _set_bank_source(db_path: Path | str, client_code: str,
                      source: str) -> None:
    assert source in ('qbo', 'plaid', 'both', 'none')
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE clients SET bank_source=? WHERE client_code=?",
            (source, client_code),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Decision
# ---------------------------------------------------------------------------


def decide_bank_setup(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    qbo_puller_cls: Any = None,
    sandbox: bool = False,
) -> dict[str, Any]:
    """Return a decision dict for the UI. Never raises on missing
    dependencies — caller gets state='choice' by default."""
    has_qbo = _has_qbo_connection(db_path, firm_code, client_code)
    has_plaid = _has_plaid_connection(db_path, client_code)
    qbo_has_banks = False
    qbo_accounts: list[dict[str, Any]] = []

    if has_qbo:
        try:
            if qbo_puller_cls is None:
                from src.integrations.qbo_bank_pull import QBOBankPull as qbo_puller_cls  # type: ignore
            puller = qbo_puller_cls(
                firm_code, client_code, db_path=db_path, sandbox=sandbox,
            )
            qbo_accounts = puller.detect_bank_accounts() or []
            qbo_has_banks = puller.has_bank_feeds()
        except Exception as exc:
            log.warning(
                "decide_bank_setup: QBO probe failed firm=%s client=%s: %s",
                firm_code, client_code, exc,
            )

    if has_qbo and qbo_has_banks and has_plaid:
        state = STATE_BOTH_ACTIVE
    elif has_qbo and qbo_has_banks:
        state = STATE_QBO_ONLY
    elif has_qbo and not qbo_has_banks:
        state = STATE_PLAID_RECOMMENDED
    elif has_plaid:
        state = STATE_PLAID_ACTIVE
    else:
        state = STATE_CHOICE

    return {
        'state': state,
        'has_qbo': has_qbo,
        'has_plaid': has_plaid,
        'qbo_has_banks': qbo_has_banks,
        'qbo_accounts': [{
            'qbo_id': a.get('Id'),
            'name': a.get('Name'),
            'account_number': a.get('AcctNum'),
            'currency': (a.get('CurrencyRef') or {}).get('value'),
        } for a in qbo_accounts],
        'current_bank_source': _current_bank_source(db_path, client_code),
        'firm_code': firm_code,
        'client_code': client_code,
    }


# ---------------------------------------------------------------------------
# Render (pure functions)
# ---------------------------------------------------------------------------


def _esc(v: Any) -> str:
    return html.escape(str(v if v is not None else ''))


_PAGE = """<!doctype html>
<html lang="{lang}"><head>
<meta charset="utf-8">
<title>{title}</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #222; max-width: 880px; }}
.card {{ border: 1px solid #ddd; border-radius: 6px; padding: 1rem 1.4rem; margin-bottom: 1rem; background: #fff; }}
.badge {{ padding: 2px 8px; border-radius: 10px; font-size: .85em; background: #e0e7ff; color: #1e3a8a; }}
.badge.good {{ background: #dcfce7; color: #166534; }}
.badge.warn {{ background: #fef3c7; color: #92400e; }}
.badge.info {{ background: #dbeafe; color: #1e40af; }}
button {{ padding: 8px 16px; border-radius: 4px; border: 1px solid #888; background: #f8f8f8; cursor: pointer; font-size: 1em; }}
button.primary {{ background: #1e40af; color: white; border-color: #1e40af; }}
form {{ display: inline-block; margin-right: .4rem; }}
table {{ border-collapse: collapse; width: 100%; margin-top: .6rem; }}
th, td {{ border: 1px solid #e4e4e4; padding: 6px 10px; text-align: left; }}
th {{ background: #f4f4f4; }}
</style></head><body>
{body}
</body></html>"""


_COPY = {
    'en': {
        'title': 'Bank setup',
        'qbo_only_message': (
            "QuickBooks already has bank feeds connected. We'll sync your "
            "bank transactions from there — you don't need to connect another "
            "bank integration."),
        'both_warning': (
            "Both QuickBooks and Plaid are active. We'll automatically "
            "deduplicate transactions that appear in both. Simplify by "
            "disconnecting Plaid if you'd prefer QBO as the sole source."),
        'plaid_recommended': (
            "QuickBooks is connected but no bank feeds were detected. "
            "Connect your bank via Plaid to enable reconciliation."),
        'plaid_active': 'Bank is connected via Plaid.',
        'choice_intro': 'Pick how to feed this client bank data.',
        'sync_qbo_now': 'Sync bank transactions from QuickBooks now',
        'connect_plaid': 'Connect bank via Plaid',
        'connect_qbo': 'Connect QuickBooks first',
        'open_dedup': 'Review detected duplicates',
    },
    'fr': {
        'title': 'Configuration bancaire',
        'qbo_only_message': (
            "QuickBooks est déjà connecté à vos flux bancaires. Nous "
            "synchronisons les transactions à partir de QBO — aucune autre "
            "intégration n'est nécessaire."),
        'both_warning': (
            "QuickBooks et Plaid sont tous les deux actifs. Nous dédoublons "
            "automatiquement les transactions qui apparaissent des deux "
            "côtés. Simplifiez en déconnectant Plaid si vous préférez QBO "
            "comme source unique."),
        'plaid_recommended': (
            "QuickBooks est connecté mais aucun flux bancaire n'est détecté. "
            "Connectez votre banque via Plaid pour activer la conciliation."),
        'plaid_active': 'Banque connectée via Plaid.',
        'choice_intro': 'Choisissez comment alimenter les données bancaires '
                        'de ce client.',
        'sync_qbo_now': 'Synchroniser les transactions depuis QuickBooks maintenant',
        'connect_plaid': 'Connecter la banque via Plaid',
        'connect_qbo': 'Connecter QuickBooks d’abord',
        'open_dedup': 'Examiner les doublons détectés',
    },
}


def render_bank_setup_page(decision: dict[str, Any], *,
                             lang: str = 'en') -> str:
    copy = _COPY.get(lang, _COPY['en'])
    body = [f"<h1>{_esc(copy['title'])} — {_esc(decision['client_code'])}</h1>"]
    state = decision['state']
    client = decision['client_code']

    if state == STATE_QBO_ONLY:
        body.append(f'<div class="card"><span class="badge good">QBO</span> '
                    f'<p>{_esc(copy["qbo_only_message"])}</p>')
        body.append(_render_qbo_account_list(decision['qbo_accounts']))
        body.append(_render_sync_qbo_button(client, copy['sync_qbo_now']))
        body.append('</div>')

    elif state == STATE_BOTH_ACTIVE:
        body.append(f'<div class="card"><span class="badge warn">Both</span> '
                    f'<p>{_esc(copy["both_warning"])}</p>')
        body.append(_render_qbo_account_list(decision['qbo_accounts']))
        body.append(_render_sync_qbo_button(client, copy['sync_qbo_now']))
        body.append(
            f'<a href="/clients/{_esc(client)}/bank/dedup" '
            f'class="button">{_esc(copy["open_dedup"])}</a>'
        )
        body.append('</div>')

    elif state == STATE_PLAID_RECOMMENDED:
        body.append(f'<div class="card"><span class="badge info">QBO (no banks)</span> '
                    f'<p>{_esc(copy["plaid_recommended"])}</p>')
        body.append(
            f'<a href="/bank/connect?client_code={_esc(client)}" '
            f'class="button">{_esc(copy["connect_plaid"])}</a>'
        )
        body.append('</div>')

    elif state == STATE_PLAID_ACTIVE:
        body.append(f'<div class="card"><span class="badge good">Plaid</span> '
                    f'<p>{_esc(copy["plaid_active"])}</p>')
        body.append(
            f'<a href="/qbo/connect?client_code={_esc(client)}" '
            f'class="button">{_esc(copy["connect_qbo"])}</a>'
        )
        body.append('</div>')

    else:  # STATE_CHOICE
        body.append(f'<div class="card"><p>{_esc(copy["choice_intro"])}</p>')
        body.append(
            f'<a href="/qbo/connect?client_code={_esc(client)}" '
            f'class="button primary">{_esc(copy["connect_qbo"])}</a> '
            f'<a href="/bank/connect?client_code={_esc(client)}" '
            f'class="button">{_esc(copy["connect_plaid"])}</a>'
        )
        body.append('</div>')

    body.append(
        f'<p><small>Current bank_source: '
        f'<code>{_esc(decision["current_bank_source"])}</code></small></p>'
    )
    return _PAGE.format(lang=_esc(lang), title=_esc(copy['title']),
                        body='\n'.join(body))


def _render_qbo_account_list(accts: list[dict[str, Any]]) -> str:
    if not accts:
        return ''
    rows = '\n'.join(
        f'<tr><td>{_esc(a.get("name"))}</td>'
        f'<td>{_esc(a.get("account_number"))}</td>'
        f'<td>{_esc(a.get("currency"))}</td></tr>'
        for a in accts
    )
    return ('<table><tr><th>Account</th><th>Number</th><th>Currency</th></tr>'
            f'{rows}</table>')


def _render_sync_qbo_button(client: str, label: str) -> str:
    return (
        f'<form method="POST" action="/clients/{_esc(client)}/bank/sync_from_qbo">'
        f'  <button class="primary">{_esc(label)}</button>'
        f'</form>'
    )


def render_dedup_page(
    db_path: Path | str, *, firm_code: str, client_code: str,
    lang: str = 'en',
) -> str:
    from src.engines.bank_tx_dedup import BankTransactionDeduplicator
    dedup = BankTransactionDeduplicator(firm_code, client_code, db_path)
    log_rows = dedup.list_dedup_log()
    copy = _COPY.get(lang, _COPY['en'])
    body = [f"<h1>{_esc(copy['title'])} — {_esc(client_code)} — dedup</h1>"]
    if not log_rows:
        body.append('<p>No duplicates detected yet.</p>')
    else:
        rows_html = '\n'.join(
            f'<tr><td>{_esc(r["primary_source"])}</td>'
            f'<td>{_esc(r["primary_id"])}</td>'
            f'<td>{_esc(r["duplicate_source"])}</td>'
            f'<td>{_esc(r["duplicate_id"])}</td>'
            f'<td>{_esc(r["match_confidence"])}</td>'
            f'<td>{_esc(r["detected_at"])}</td>'
            f'<td>{_esc(r["resolved_by"] or "")}</td>'
            f'<td><form method="POST" '
            f'action="/clients/{_esc(client_code)}/bank/dedup/unmark">'
            f'<input type="hidden" name="duplicate_id" '
            f'value="{_esc(r["duplicate_id"])}">'
            f'<button>Un-dedup</button></form></td></tr>'
            for r in log_rows
        )
        body.append(
            '<table><tr>'
            '<th>Primary src</th><th>Primary id</th>'
            '<th>Dup src</th><th>Dup id</th>'
            '<th>Confidence</th><th>Detected</th>'
            '<th>Resolved by</th><th>Action</th>'
            f'</tr>{rows_html}</table>'
        )
    return _PAGE.format(lang=_esc(lang), title=_esc(copy['title']),
                        body='\n'.join(body))


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------


def handle_sync_from_qbo(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    sandbox: bool = False,
    run_dedup: bool | None = None,
) -> tuple[int, str, bytes]:
    """Pull bank transactions from QBO, refresh clients.bank_source,
    run dedup when Plaid is also connected."""
    from src.integrations.qbo_bank_pull import QBOBankPull
    try:
        puller = QBOBankPull(firm_code, client_code, db_path=db_path,
                              sandbox=sandbox)
        n = puller.pull_bank_transactions()
    except Exception as exc:
        return 502, 'application/json', json.dumps({
            'ok': False, 'error': str(exc),
        }).encode()

    has_plaid = _has_plaid_connection(db_path, client_code)
    new_source = 'both' if has_plaid else 'qbo'
    _set_bank_source(db_path, client_code, new_source)

    dedup_hidden = 0
    should_dedup = run_dedup if run_dedup is not None else has_plaid
    if should_dedup:
        from src.engines.bank_tx_dedup import BankTransactionDeduplicator
        dedup_hidden = BankTransactionDeduplicator(
            firm_code, client_code, db_path,
        ).mark_duplicates(auto_apply=True)

    return 200, 'application/json', json.dumps({
        'ok': True,
        'pulled': n,
        'bank_source': new_source,
        'duplicates_hidden': dedup_hidden,
    }).encode()


def handle_unmark_duplicate(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    duplicate_id: str,
    resolved_by: str,
) -> tuple[int, str, bytes]:
    from src.engines.bank_tx_dedup import BankTransactionDeduplicator
    ok = BankTransactionDeduplicator(
        firm_code, client_code, db_path,
    ).unmark_duplicate(duplicate_id=duplicate_id, resolved_by=resolved_by)
    return 200 if ok else 404, 'application/json', json.dumps({
        'ok': ok, 'duplicate_id': duplicate_id,
    }).encode()
