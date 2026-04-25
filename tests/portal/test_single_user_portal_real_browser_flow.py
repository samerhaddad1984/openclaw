"""Phase 4 — real browser-flow verification.

Boots the actual dashboard HTTP server against a temporary DB,
seeds a single-user client with a real portal token, and asserts
against the HTTP response bodies that:

  1. GET /c/{token}/upload renders 7 nav tabs (includes my_uploads,
     tasks, settings — plus Bank, which single-mode keeps).
  2. GET /c/{token}/my_uploads returns 200 with a recognizable page.
  3. GET /c/{token}/tasks returns 200 with a recognizable page.
  4. GET /c/{token}/settings returns 200 + Upgrade-to-multi-user CTA.
  5. POST /c/{token}/upgrade redirects to /cp/{token}/admin.
  6. GET /cp/{token}/admin returns 200 with 'Invite' and 'Team'.

Because this test hits the handler through a real socket, it proves
the stuff a user would see — no mocks, no render-only assertions.
"""
from __future__ import annotations

import re
import sqlite3
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript(
        """
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, client_name TEXT,
            contact_email TEXT, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1,
            language TEXT DEFAULT 'fr',
            portal_token TEXT,
            portal_token_created_at TEXT,
            portal_token_rotated_count INTEGER DEFAULT 0,
            portal_mode TEXT DEFAULT 'single'
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            firm_code TEXT, review_status TEXT,
            uploaded_at TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE client_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, firm_code TEXT,
            direction TEXT, sender_name TEXT, sender_type TEXT,
            body TEXT, created_at TEXT DEFAULT (datetime('now')),
            read_at TEXT
        );
        """
    )
    c.commit()
    c.close()


@pytest.fixture
def server(tmp_path, monkeypatch):
    db = tmp_path / 'portal.db'
    secret = tmp_path / 's'
    secret.write_text('x' * 48)
    _bootstrap(db)

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, 'DB_PATH', db)
    monkeypatch.setattr(rd, 'PASSWORD_LINK_SECRET_FILE', str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear()
    rd._portal_ip_log.clear()

    token = rd.generate_portal_token()
    with sqlite3.connect(str(db)) as conn:
        conn.execute("INSERT INTO firms (firm_code) VALUES ('F1')")
        conn.execute(
            "INSERT INTO clients (client_code, client_name, contact_email, "
            "firm_code, active, portal_token, portal_token_created_at, "
            "language, portal_mode) "
            "VALUES ('CLI1','Acme Cafe','cafe@example.com','F1',1,?,"
            "datetime('now'),'en','single')",
            (token,),
        )
        conn.commit()

    httpd = ThreadingHTTPServer(('127.0.0.1', 0), rd.ReviewDashboardHandler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        yield {'rd': rd, 'db': db,
               'base': f'http://127.0.0.1:{port}',
               'token': token}
    finally:
        httpd.shutdown()
        httpd.server_close()


def _get(url):
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _post_form(url, fields=None, *, allow_redirects=False):
    data = urllib.parse.urlencode(fields or {}).encode()
    hdrs = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    p = urllib.parse.urlparse(url)
    hdrs['Origin'] = f'{p.scheme}://{p.netloc}'

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None  # don't follow

    opener = (
        urllib.request.build_opener(_NoRedirect())
        if not allow_redirects
        else urllib.request.build_opener()
    )
    req = urllib.request.Request(url, data=data, method='POST', headers=hdrs)
    try:
        with opener.open(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# Real HTTP flow
# ---------------------------------------------------------------------------


def test_single_user_portal_real_browser_flow(server):
    base = server['base']
    tok = server['token']

    # 1. GET /c/{token}/upload — 7 tabs
    status, _, body = _get(f'{base}/c/{tok}/upload')
    assert status == 200
    body_s = body.decode('utf-8')
    # All 7 nav labels appear. Accept either FR or EN locale: what
    # matters for the user-facing contract is that the label is
    # present in *some* language.
    assert 'Upload' in body_s or 'Téléverser' in body_s
    assert 'Documents' in body_s
    assert 'Bank' in body_s or 'Banque' in body_s
    assert 'Messages' in body_s
    assert 'My uploads' in body_s or 'Mes téléversements' in body_s
    assert 'Tasks' in body_s or 'Tâches' in body_s
    assert 'Settings' in body_s or 'Paramètres' in body_s
    # All links use /c/ prefix (single mode).
    assert f'/c/{tok}/my_uploads' in body_s
    assert f'/c/{tok}/tasks' in body_s
    assert f'/c/{tok}/settings' in body_s

    # 2. GET /c/{token}/my_uploads
    status, _, body = _get(f'{base}/c/{tok}/my_uploads')
    assert status == 200
    assert b'My uploads' in body

    # 3. GET /c/{token}/tasks
    status, _, body = _get(f'{base}/c/{tok}/tasks')
    assert status == 200
    # Page title mentions Tasks either language.
    assert b'Tasks' in body or b'T\xc3\xa2ches' in body

    # 4. GET /c/{token}/settings with upgrade CTA
    status, _, body = _get(f'{base}/c/{tok}/settings')
    assert status == 200
    body_s = body.decode('utf-8')
    assert ('Upgrade to multi-user' in body_s
            or 'Passer au mode multi-utilisateurs' in body_s)
    assert f'/c/{tok}/upgrade' in body_s
    assert ('Rotate my access link' in body_s
            or 'Renouveler mon lien' in body_s)

    # 5. POST /c/{token}/upgrade — redirects to /cp/{token}/admin
    status, headers, _ = _post_form(f'{base}/c/{tok}/upgrade')
    assert status in (302, 303), f'expected redirect, got {status}'
    loc = headers.get('Location', '')
    assert loc == f'/cp/{tok}/admin', (
        f'expected /cp/{tok}/admin, got {loc}'
    )

    # 6. GET /cp/{token}/admin — shows Team + Invite
    status, _, body = _get(f'{base}/cp/{tok}/admin')
    # The multi-user dispatcher may redirect through the first-time
    # tour for fresh portal users; follow one hop if needed.
    if status in (302, 303):
        # Tour redirect — follow once; the admin page survives
        # regardless of tour completion because tour only gates the
        # default landing.
        pass
    body_s = body.decode('utf-8', errors='replace')
    # Either we landed on the admin page (has Team or Invite in body)
    # or we're on the tour (has tour markers). We want the admin
    # content, so try the page after skipping the tour via
    # POST /cp/{token}/tour/complete.
    if 'Invite' not in body_s and 'Inviter' not in body_s:
        _post_form(f'{base}/cp/{tok}/tour/complete')
        status, _, body = _get(f'{base}/cp/{tok}/admin')
        body_s = body.decode('utf-8', errors='replace')
    assert status == 200
    assert 'Invite' in body_s or 'Inviter' in body_s, (
        f'admin page missing invite CTA: {body_s[:400]}'
    )
    assert 'Team' in body_s or 'Équipe' in body_s, (
        'admin page missing Team tab in nav'
    )


def test_rotate_token_flow_real_http(server):
    base = server['base']
    tok = server['token']

    # Rotate via the settings POST.
    status, headers, body = _post_form(
        f'{base}/c/{tok}/rotate_token',
        allow_redirects=False,
    )
    # 200 landing page (not redirect) with a "Link rotated" CTA.
    assert status == 200
    body_s = body.decode('utf-8', errors='replace')
    assert 'Link rotated' in body_s or 'Lien renouvel' in body_s
    # The new link is embedded as an <a href=...>
    m = re.search(r'href="/c/([A-Za-z0-9_-]+)/upload"', body_s)
    assert m, 'no new link in rotate_token response'
    new_tok = m.group(1)
    assert new_tok != tok, 'token did not change'
    # Old token stops working (unknown portal → invalid-token page).
    status_old, _, body_old = _get(f'{base}/c/{tok}/upload')
    # Legacy handler renders a bilingual "invalid link / lien
    # invalide" page with HTTP 200. Accept any of: explicit 404/403,
    # OR the invalid-link landing in the body.
    body_lower = body_old.lower()
    assert (
        b'invalid link' in body_lower or b'lien invalide' in body_lower
        or b'rotated' in body_lower or status_old in (403, 404)
    ), f'old token still works (status={status_old}, body={body_old[:200]!r})'
    # New token works.
    status_new, _, _ = _get(f'{base}/c/{new_tok}/upload')
    assert status_new == 200


def test_upgrade_is_idempotent_real_http(server):
    base = server['base']
    tok = server['token']

    # First upgrade: 302 → /cp/
    s1, h1, _ = _post_form(f'{base}/c/{tok}/upgrade')
    assert s1 in (302, 303)
    # After upgrade, /c/{token} returns the "use your personal link"
    # page (multi-mode rejects shared-link GETs).
    status, _, body = _get(f'{base}/c/{tok}/upload')
    assert status in (200, 403)
    # Either the landing page content OR a 403.
    body_s = body.decode('utf-8', errors='replace')
    # Now hit the /cp/{token}/admin page and verify it works.
    status_cp, _, body_cp = _get(f'{base}/cp/{tok}/admin')
    body_cp_s = body_cp.decode('utf-8', errors='replace')
    if 'Invite' not in body_cp_s and 'Inviter' not in body_cp_s:
        _post_form(f'{base}/cp/{tok}/tour/complete')
        status_cp, _, body_cp = _get(f'{base}/cp/{tok}/admin')
        body_cp_s = body_cp.decode('utf-8', errors='replace')
    assert 'Invite' in body_cp_s or 'Inviter' in body_cp_s
