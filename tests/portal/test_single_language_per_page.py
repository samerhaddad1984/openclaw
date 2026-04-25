"""Real-HTTP guard against bilingual leaks on portal pages.

User's language is set on the client row (`clients.language`). Each
portal page must render in exactly one language — never a mix of
"Envoyer / Upload" style strings on a page that's otherwise FR or EN.

Two server fixtures are spun up: one with `language='fr'`, one with
`language='en'`. We hit each portal tab and assert there are no
"FR-word and EN-twin" pairs in the response body.
"""
from __future__ import annotations

import re
import sqlite3
import sys
import threading
import urllib.error
import urllib.request
from html.parser import HTMLParser
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# Each pair = (fr_signature_phrase, en_signature_phrase). Both are
# matched as whole-word phrases (regex) on visible text. When a page
# resolves to one language, the *other* phrase must not appear.
# Phrases are deliberately distinctive so they do not collide with
# substrings of unrelated French/English words (e.g. "Connect" inside
# "Connecter la banque").
BILINGUAL_LEAK_PAIRS = [
    (r"\bEnvoyer\b", r"\bUpload\b"),
    (r"\bTéléverser\b", r"\bUpload\b"),
    (r"\bGlissez\b", r"\bDrop files\b"),
    (r"\bFichier\b", r"\bFile\b"),
    (r"\bFournisseur\b", r"\bVendor\b"),
    (r"\bConnectez votre banque\b", r"\bConnect your bank\b"),
    (r"\bCompte bancaire\b", r"\bBank account\b"),
    (r"\bAucun message\b", r"\bNo messages\b"),
    (r"\bÉcrivez à votre CPA\b", r"\bWrite to your CPA\b"),
    (r"\bMes documents\b", r"\bMy documents\b"),
    (r"\bAucun document\b", r"\bNo documents\b"),
    (r"\bMontant\b", r"\bAmount\b"),
    (r"\bStatut\b", r"\bStatus\b"),
]


class _StripHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self._chunks: list[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            self._chunks.append(data)

    def text(self) -> str:
        return " ".join(self._chunks)


def _visible_text(body: bytes) -> str:
    p = _StripHTML()
    p.feed(body.decode("utf-8", errors="replace"))
    return p.text()


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
            uploaded_at TEXT, created_at TEXT,
            vendor TEXT, amount REAL, manual_hold_reason TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE client_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, firm_code TEXT,
            direction TEXT, sender_name TEXT, sender_type TEXT,
            body TEXT, created_at TEXT DEFAULT (datetime('now')),
            read_at TEXT
        );
        CREATE TABLE bank_connections (
            id TEXT PRIMARY KEY,
            client_code TEXT, plaid_access_token TEXT,
            plaid_item_id TEXT, institution_name TEXT,
            account_name TEXT, account_type TEXT,
            firm_code TEXT, active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            last_sync TEXT
        );
        """
    )
    c.commit()
    c.close()


def _spin(db, monkeypatch, lang):
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, 'DB_PATH', db)
    secret = db.parent / 's'
    secret.write_text('x' * 48)
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
            "VALUES ('CLI1','Acme','c@example.com','F1',1,?,"
            "datetime('now'),?,'single')",
            (token, lang),
        )
        # Insert one document so the documents-list table renders
        # column headers (the empty-state path doesn't show them).
        # Vendor name is deliberately neutral so language assertions
        # match UI text only, not user-supplied content.
        conn.execute(
            "INSERT INTO documents (document_id, file_name, client_code, "
            "review_status, created_at, vendor, amount) "
            "VALUES ('D1','receipt.pdf','CLI1','New',"
            "datetime('now'),'Maxi 1234',12.34)"
        )
        conn.commit()

    httpd = ThreadingHTTPServer(('127.0.0.1', 0), rd.ReviewDashboardHandler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd, f'http://127.0.0.1:{port}', token


@pytest.fixture
def fr_server(tmp_path, monkeypatch):
    _bootstrap(tmp_path / 'fr.db')
    httpd, base, token = _spin(tmp_path / 'fr.db', monkeypatch, 'fr')
    try:
        yield {'base': base, 'token': token}
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.fixture
def en_server(tmp_path, monkeypatch):
    _bootstrap(tmp_path / 'en.db')
    httpd, base, token = _spin(tmp_path / 'en.db', monkeypatch, 'en')
    try:
        yield {'base': base, 'token': token}
    finally:
        httpd.shutdown()
        httpd.server_close()


def _get(url):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, r.read()


def _assert_no_bilingual_leak(text: str, page: str, lang: str):
    """Assert that we never have FR_phrase and its EN_twin in the same
    page text. The active language phrase may appear; the *other*
    language's twin must not."""
    leaks: list[str] = []
    for fr, en in BILINGUAL_LEAK_PAIRS:
        if re.search(fr, text) and re.search(en, text):
            leaks.append(f"{fr} + {en}")
    assert not leaks, (
        f"[{page}] in lang={lang!r}: bilingual leak(s): "
        + ", ".join(leaks)
    )


PORTAL_PAGES = ['', 'upload', 'documents', 'bank', 'messages']


@pytest.mark.parametrize("page", PORTAL_PAGES)
def test_portal_page_fr_only_no_en_twin(fr_server, page):
    base, tok = fr_server['base'], fr_server['token']
    url = f'{base}/c/{tok}/{page}' if page else f'{base}/c/{tok}/upload'
    status, body = _get(url)
    assert status == 200
    text = _visible_text(body)
    _assert_no_bilingual_leak(text, page or 'upload', 'fr')


@pytest.mark.parametrize("page", PORTAL_PAGES)
def test_portal_page_en_only_no_fr_twin(en_server, page):
    base, tok = en_server['base'], en_server['token']
    url = f'{base}/c/{tok}/{page}' if page else f'{base}/c/{tok}/upload'
    status, body = _get(url)
    assert status == 200
    text = _visible_text(body)
    _assert_no_bilingual_leak(text, page or 'upload', 'en')


def test_portal_upload_fr_uses_french_button_label(fr_server):
    """Sanity: FR client sees French CTA, not English."""
    base, tok = fr_server['base'], fr_server['token']
    status, body = _get(f'{base}/c/{tok}/upload')
    assert status == 200
    text = _visible_text(body)
    assert re.search(r'\bEnvoyer\b', text), "FR portal upload should show 'Envoyer'"
    assert not re.search(r'\bUpload\b', text), "FR portal upload leaked EN 'Upload'"


def test_portal_upload_en_uses_english_button_label(en_server):
    base, tok = en_server['base'], en_server['token']
    status, body = _get(f'{base}/c/{tok}/upload')
    assert status == 200
    text = _visible_text(body)
    assert re.search(r'\bUpload\b', text), "EN portal upload should show 'Upload'"
    assert not re.search(r'\bEnvoyer\b', text), "EN portal upload leaked FR 'Envoyer'"


def test_portal_documents_fr_columns(fr_server):
    base, tok = fr_server['base'], fr_server['token']
    status, body = _get(f'{base}/c/{tok}/documents')
    assert status == 200
    text = _visible_text(body)
    # FR documents page should use FR column headers
    assert re.search(r'\bFichier\b', text)
    assert re.search(r'\bFournisseur\b', text)
    assert not re.search(r'\bVendor\b', text)


def test_portal_documents_en_columns(en_server):
    base, tok = en_server['base'], en_server['token']
    status, body = _get(f'{base}/c/{tok}/documents')
    assert status == 200
    text = _visible_text(body)
    assert re.search(r'\bFile\b', text)
    assert re.search(r'\bVendor\b', text)
    assert not re.search(r'\bFichier\b', text)
    assert not re.search(r'\bFournisseur\b', text)


def test_portal_bank_fr_no_english_leak(fr_server):
    base, tok = fr_server['base'], fr_server['token']
    status, body = _get(f'{base}/c/{tok}/bank')
    assert status == 200
    text = _visible_text(body)
    # On the empty-state path (no bank_connections row), FR client
    # should see Connectez votre banque and not "Connect your bank".
    assert (re.search(r'Connectez votre banque', text)
            or re.search(r'\bCompte bancaire\b', text))
    assert not re.search(r'Connect your bank', text)
    assert not re.search(r'\bBank account\b', text)


def test_portal_bank_en_no_french_leak(en_server):
    base, tok = en_server['base'], en_server['token']
    status, body = _get(f'{base}/c/{tok}/bank')
    assert status == 200
    text = _visible_text(body)
    assert (re.search(r'Connect your bank', text)
            or re.search(r'\bBank account\b', text))
    assert not re.search(r'\bConnectez\b', text)


def test_portal_messages_fr_no_english_leak(fr_server):
    base, tok = fr_server['base'], fr_server['token']
    status, body = _get(f'{base}/c/{tok}/messages')
    assert status == 200
    text = _visible_text(body)
    assert re.search(r'\bAucun message\b', text)
    assert not re.search(r'\bNo messages\b', text)


def test_portal_messages_en_no_french_leak(en_server):
    base, tok = en_server['base'], en_server['token']
    status, body = _get(f'{base}/c/{tok}/messages')
    assert status == 200
    text = _visible_text(body)
    assert re.search(r'\bNo messages\b', text)
    assert not re.search(r'\bAucun message\b', text)
