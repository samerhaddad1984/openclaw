"""HTML render helpers + HTTP handlers for the QBO bidirectional sync.

Everything is a pure function so the monolith dashboard can wire them
in with thin routing glue. No state, no imports of BaseHTTPRequestHandler.

Routes the dashboard should register:

- ``GET  /qbo/sync/dashboard``      → :func:`render_sync_dashboard`
- ``POST /qbo/sync/initial``        → :func:`handle_initial_sync`
- ``POST /qbo/sync/now``            → :func:`handle_incremental_sync`
- ``GET  /qbo/sync/status``         → :func:`handle_sync_status_api`
- ``GET  /qbo/conflicts``           → :func:`render_conflicts_page`
- ``POST /qbo/conflicts/resolve``   → :func:`handle_resolve_conflict`
- ``POST /qbo/webhook``             → :func:`handle_webhook_route`

The webhook verifier_token comes from the env var
``QBO_WEBHOOK_VERIFIER_TOKEN``.
"""
from __future__ import annotations

import html
import json
import os
from pathlib import Path
from typing import Any

from src.integrations.qbo_conflict_resolver import QBOConflictResolver
from src.integrations.qbo_sync import (
    QBOSyncOrchestrator,
    scheduled_sync_all,
    sync_status,
)
from src.integrations.qbo_webhook import handle_webhook


# ---------------------------------------------------------------------------
# Pure render helpers
# ---------------------------------------------------------------------------


_PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>{title}</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #222; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
th {{ background: #f4f4f4; }}
tr:nth-child(even) td {{ background: #fafafa; }}
.badge {{ padding: 2px 8px; border-radius: 10px; font-size: .8em; }}
.badge.native {{ background: #dbeafe; color: #1e40af; }}
.badge.qbo {{ background: #fef3c7; color: #92400e; }}
.badge.both {{ background: #dcfce7; color: #166534; }}
.badge.ok {{ background: #dcfce7; color: #166534; }}
.badge.error {{ background: #fee2e2; color: #991b1b; }}
.badge.conflict {{ background: #fef3c7; color: #92400e; }}
button {{ padding: 6px 14px; border-radius: 4px; border: 1px solid #888;
         background: #f8f8f8; cursor: pointer; }}
button.primary {{ background: #1e40af; color: white; border-color: #1e40af; }}
form {{ display: inline; }}
.row {{ display: flex; gap: 1rem; align-items: center; }}
</style>
</head><body>
{body}
</body></html>"""


def _esc(v: Any) -> str:
    return html.escape(str(v if v is not None else ''))


def render_sync_dashboard(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
) -> str:
    status = sync_status(db_path, firm_code=firm_code, client_code=client_code)
    last_ok = status.get('last_successful_sync')
    last_err = status.get('last_error')

    buttons = f"""
    <form method="POST" action="/qbo/sync/initial">
      <input type="hidden" name="client_code" value="{_esc(client_code)}">
      <button class="primary">Run initial sync</button>
    </form>
    <form method="POST" action="/qbo/sync/now">
      <input type="hidden" name="client_code" value="{_esc(client_code)}">
      <button>Run incremental sync</button>
    </form>
    """

    last_ok_html = '<em>never</em>' if not last_ok else (
        f"{_esc(last_ok.get('completed_at'))} — "
        f"{_esc(last_ok.get('entities_synced'))} entities"
    )
    last_err_html = 'none' if not last_err else (
        f"{_esc(last_err.get('completed_at'))} — {_esc(last_err.get('details'))}"
    )

    body = f"""
    <h1>QBO sync — {_esc(firm_code)} / {_esc(client_code)}</h1>
    <div class="row">{buttons}</div>
    <table>
      <tr><th>Last successful sync</th><td>{last_ok_html}</td></tr>
      <tr><th>Last error</th><td>{last_err_html}</td></tr>
      <tr><th>Conflicts pending</th>
          <td>{_esc(status['conflicts_pending'])}
              (<a href="/qbo/conflicts?client_code={_esc(client_code)}">review</a>)</td></tr>
      <tr><th>Webhooks pending</th><td>{_esc(status['webhooks_pending'])}</td></tr>
    </table>
    """
    return _PAGE.format(title='QBO Sync Dashboard', body=body)


def render_conflicts_page(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
) -> str:
    resolver = QBOConflictResolver(firm_code, client_code, db_path=db_path)
    conflicts = resolver.list_conflicts()

    if not conflicts:
        body = f"<h1>Conflicts — {_esc(firm_code)}/{_esc(client_code)}</h1><p>None.</p>"
        return _PAGE.format(title='QBO Conflicts', body=body)

    rows = []
    for c in conflicts:
        details_raw = c.get('conflict_details')
        try:
            details = json.loads(details_raw) if details_raw else {}
        except ValueError:
            details = {}
        rows.append(f"""
        <tr>
          <td>{_esc(c['entity_type'])}</td>
          <td>{_esc(c['qbo_id'])}</td>
          <td>{_esc(c.get('local_id'))}</td>
          <td>{_esc(c.get('last_pushed_at'))}</td>
          <td>{_esc(c.get('last_qbo_modified'))}</td>
          <td>{_esc(json.dumps(details)[:120])}</td>
          <td>
            <form method="POST" action="/qbo/conflicts/resolve">
              <input type="hidden" name="client_code"  value="{_esc(client_code)}">
              <input type="hidden" name="entity_type"  value="{_esc(c['entity_type'])}">
              <input type="hidden" name="qbo_id"       value="{_esc(c['qbo_id'])}">
              <button name="strategy" value="otocpa_wins">OtoCPA wins</button>
              <button name="strategy" value="qbo_wins">QBO wins</button>
              <button name="strategy" value="flag_for_review">Keep flagged</button>
            </form>
          </td>
        </tr>
        """)
    body = f"""
    <h1>Conflicts — {_esc(firm_code)}/{_esc(client_code)}</h1>
    <table>
      <tr>
        <th>Entity</th><th>QBO ID</th><th>Local ID</th>
        <th>Last pushed</th><th>Last QBO mod</th>
        <th>Details</th><th>Resolve</th>
      </tr>
      {''.join(rows)}
    </table>
    """
    return _PAGE.format(title='QBO Conflicts', body=body)


# ---------------------------------------------------------------------------
# HTTP handlers (return (status_code, content_type, body_bytes))
# ---------------------------------------------------------------------------


def handle_initial_sync(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    sandbox: bool = False,
) -> tuple[int, str, bytes]:
    orch = QBOSyncOrchestrator(firm_code, client_code, db_path=db_path,
                                  sandbox=sandbox)
    result = orch.initial_sync(triggered_by='manual')
    return 200, 'application/json', json.dumps(result).encode()


def handle_incremental_sync(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    sandbox: bool = False,
) -> tuple[int, str, bytes]:
    orch = QBOSyncOrchestrator(firm_code, client_code, db_path=db_path,
                                  sandbox=sandbox)
    result = orch.incremental_sync(triggered_by='manual')
    return 200, 'application/json', json.dumps(result).encode()


def handle_sync_status_api(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
) -> tuple[int, str, bytes]:
    status = sync_status(db_path, firm_code=firm_code, client_code=client_code)
    return 200, 'application/json', json.dumps(status).encode()


def handle_resolve_conflict(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    entity_type: str,
    qbo_id: str,
    strategy: str,
) -> tuple[int, str, bytes]:
    resolver = QBOConflictResolver(firm_code, client_code, db_path=db_path)
    try:
        result = resolver.resolve(entity_type=entity_type, qbo_id=qbo_id,
                                    strategy=strategy)
        return 200, 'application/json', json.dumps(result).encode()
    except Exception as exc:
        return 500, 'application/json', json.dumps(
            {'ok': False, 'error': str(exc)}
        ).encode()


def handle_webhook_route(
    body: bytes,
    signature_header: str,
    *,
    db_path: Path | str,
    verifier_token: str | None = None,
) -> tuple[int, str, bytes]:
    token = verifier_token or os.environ.get('QBO_WEBHOOK_VERIFIER_TOKEN', '')
    out = handle_webhook(body, signature_header,
                          db_path=db_path, verifier_token=token)
    # ALWAYS 200 even on bad signature so Intuit doesn't retry forever
    # while we investigate — logs captures the real story. This matches
    # Stripe's recommended webhook pattern.
    return 200, 'application/json', json.dumps(out).encode()


# ---------------------------------------------------------------------------
# Scheduled sync entry-point (for cron)
# ---------------------------------------------------------------------------


def run_scheduled_sync(db_path: Path | str,
                        *, sandbox: bool = False) -> dict[str, Any]:
    return scheduled_sync_all(db_path, sandbox=sandbox)
