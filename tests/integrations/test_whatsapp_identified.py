"""Phase 4: Twilio webhook tags documents with portal-user identity.

We stub :func:`process_file`, :func:`send_whatsapp_message` and
:func:`_download_twilio_media` so the test runs without Twilio + OCR.
Focus: the webhook looks up the sender in ``client_portal_users``
by normalized E.164, picks the right bilingual reply, updates each
document row with identity + channel, and auto-assigns.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import whatsapp as wa  # noqa: E402
from src.integrations import multi_user_portal as mup  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _mkdb(tmp_path):
    db = tmp_path / 'wa_wh.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY, firm_code TEXT,
                client_name TEXT, language TEXT DEFAULT 'fr',
                portal_mode TEXT DEFAULT 'multi',
                active INTEGER DEFAULT 1,
                whatsapp_number TEXT
            );
            CREATE TABLE dashboard_users (
                username TEXT PRIMARY KEY, client_code TEXT, language TEXT,
                display_name TEXT, whatsapp_number TEXT, active INTEGER DEFAULT 1
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL, client_code TEXT NOT NULL,
                email TEXT NOT NULL, full_name TEXT,
                role TEXT NOT NULL, user_token TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'invited',
                invited_by TEXT, invited_at TEXT, accepted_at TEXT,
                last_active_at TEXT, upload_count INTEGER DEFAULT 0,
                suspended_at TEXT, removed_at TEXT, version INTEGER DEFAULT 1,
                whatsapp_number TEXT,
                whatsapp_verified INTEGER DEFAULT 0,
                whatsapp_verified_at TEXT,
                UNIQUE(firm_code, client_code, email)
            );
            CREATE UNIQUE INDEX idx_cpu_whatsapp_firm
                ON client_portal_users(firm_code, whatsapp_number)
                WHERE whatsapp_number IS NOT NULL;
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER,
                firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT NOT NULL,
                detail TEXT, ip TEXT, user_agent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                client_code TEXT,
                uploaded_by_portal_user_id INTEGER,
                uploader_name TEXT, uploader_email TEXT,
                uploaded_via_channel TEXT DEFAULT 'portal',
                ingest_source TEXT
            );
            CREATE TABLE messaging_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, platform TEXT, direction TEXT,
                message_type TEXT, document_id TEXT,
                sent_at TEXT, status TEXT
            );
        """)
        conn.execute(
            "INSERT INTO firms VALUES ('F','CPA')",
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "language, portal_mode) VALUES "
            "('C1','F','Widget Co','fr','multi')",
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, client_name, "
            "language, portal_mode) VALUES "
            "('C2','F','Gadget Co','en','multi')",
        )
        conn.commit()
    return db


@pytest.fixture()
def db(tmp_path, monkeypatch):
    path = _mkdb(tmp_path)
    # Redirect all module-level DB_PATH references inside whatsapp.py.
    monkeypatch.setattr(wa, 'DB_PATH', path)
    # _open_db hardcodes DB_PATH at call time via the module constant,
    # so patching the constant is enough.
    return path


def _make_user(db_path, *, firm='F', client='C1',
                email='marie@c1', name='Marie',
                role='contributor', wa_num='+15145550301',
                status='active'):
    u = mup.create_user_direct(
        db_path, firm_code=firm, client_code=client,
        email=email, full_name=name, role=role,
        invited_by='cpa@f', status=status,
    )
    if wa_num:
        # Using the helper bypasses uniqueness check for suspended
        # retro-assignment; call through the raw SQL instead so we
        # can attach numbers to suspended/removed users too.
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE client_portal_users SET whatsapp_number=?, "
                "whatsapp_verified=1 WHERE id=?",
                (wa_num, u['id']),
            )
            conn.commit()
    return u


def _form(num_media=1, from_='whatsapp:+15145550301',
           content_type='image/jpeg'):
    f = {
        'From': from_,
        'NumMedia': str(num_media),
    }
    for i in range(num_media):
        f[f'MediaUrl{i}'] = f'https://twilio/media/{i}'
        f[f'MediaContentType{i}'] = content_type
    return f


class _FakeProcess:
    """Stub for :func:`process_file` that always succeeds."""

    def __init__(self, db_path):
        self.db_path = db_path
        self.counter = 0

    def __call__(self, file_bytes, filename, *, client_code,
                   submitted_by, ingest_source, **kwargs):
        self.counter += 1
        doc_id = f'D{self.counter:03d}'
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO documents (document_id, client_code, "
                "ingest_source) VALUES (?, ?, ?)",
                (doc_id, client_code, ingest_source),
            )
            conn.commit()
        return {'ok': True, 'document_id': doc_id}


@pytest.fixture()
def stub_pipeline(db, monkeypatch):
    """Stub out media download, processing, and outbound Twilio send."""
    monkeypatch.setattr(wa, '_download_twilio_media',
                         lambda url: b'fake-bytes')
    monkeypatch.setattr(wa, 'send_whatsapp_message',
                         lambda to, body: True)
    # auto_assign imports lazily inside the handler; stub the module.
    import src.integrations.auto_assign as aa
    monkeypatch.setattr(aa, 'auto_assign_new_document',
                         lambda document_id: None)
    # Signature validation is bypassed via the sandbox env var.
    monkeypatch.setenv('TWILIO_SANDBOX', 'true')
    fake_proc = _FakeProcess(db)
    # process_file is imported inside handle_whatsapp_webhook from
    # src.engines.ocr_engine; replace it on the module.
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, 'process_file', fake_proc)
    return fake_proc


# ---------------------------------------------------------------------------
# Identified sender
# ---------------------------------------------------------------------------

def test_known_number_creates_identified_document(db, stub_pipeline):
    user = _make_user(db)
    result = wa.handle_whatsapp_webhook(
        _form(), signature='', webhook_url='',
    )
    assert result['ok']
    assert result['client_code'] == 'C1'
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT uploaded_via_channel, uploaded_by_portal_user_id, "
            "uploader_name, uploader_email FROM documents "
            "WHERE ingest_source='whatsapp'",
        ).fetchone()
    assert row == ('whatsapp', user['id'], 'Marie', 'marie@c1')


def test_channel_marked_whatsapp(db, stub_pipeline):
    _make_user(db)
    wa.handle_whatsapp_webhook(_form(), signature='', webhook_url='')
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT uploaded_via_channel FROM documents "
            "WHERE ingest_source='whatsapp'",
        ).fetchone()
    assert row[0] == 'whatsapp'


def test_multi_media_all_attached_to_user(db, stub_pipeline):
    user = _make_user(db)
    wa.handle_whatsapp_webhook(
        _form(num_media=3), signature='', webhook_url='',
    )
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT uploaded_by_portal_user_id, uploaded_via_channel "
            "FROM documents",
        ).fetchall()
    assert len(rows) == 3
    assert all(r[0] == user['id'] for r in rows)
    assert all(r[1] == 'whatsapp' for r in rows)


def test_non_image_non_pdf_skipped(db, stub_pipeline):
    _make_user(db)
    # audio/ogg should be skipped, not processed.
    result = wa.handle_whatsapp_webhook(
        _form(content_type='audio/ogg'),
        signature='', webhook_url='',
    )
    assert result['ok']
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT * FROM documents").fetchall()
    assert rows == []


def test_auto_assignment_from_whatsapp(db, stub_pipeline, monkeypatch):
    user = _make_user(db)
    called = []
    import src.integrations.auto_assign as aa
    monkeypatch.setattr(aa, 'auto_assign_new_document',
                         lambda document_id: called.append(document_id))
    wa.handle_whatsapp_webhook(_form(), signature='', webhook_url='')
    assert len(called) == 1


def test_reply_in_user_language_fr(db, stub_pipeline):
    _make_user(db, client='C1')  # client C1 language='fr'
    result = wa.handle_whatsapp_webhook(
        _form(), signature='', webhook_url='',
    )
    # Portal-user success template is bilingual-specific: FR says
    # "Merci", EN says "Thanks".
    assert result['reply_body'].startswith('Merci')


def test_reply_in_user_language_en(db, stub_pipeline):
    _make_user(db, client='C2', email='bob@c2', name='Bob',
                 wa_num='+15145550302')
    form = _form(from_='whatsapp:+15145550302')
    result = wa.handle_whatsapp_webhook(form, signature='', webhook_url='')
    assert result['reply_body'].startswith('Thanks')


def test_upload_count_incremented(db, stub_pipeline):
    user = _make_user(db)
    wa.handle_whatsapp_webhook(_form(), signature='', webhook_url='')
    refreshed = mup.get_user(db, user_id=user['id'])
    assert refreshed['upload_count'] == 1


# ---------------------------------------------------------------------------
# Unknown / suspended / removed
# ---------------------------------------------------------------------------

def test_unknown_number_gets_rejection_reply(db, stub_pipeline):
    # No portal user registered for this number.
    result = wa.handle_whatsapp_webhook(
        _form(from_='whatsapp:+15145559999'),
        signature='', webhook_url='',
    )
    assert result['ok']
    # Bilingual rejection is still the legacy "contact your
    # accountant" string — unknown here means unknown everywhere.
    assert "n'est pas enregistré" in result['reply_body']
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT * FROM documents").fetchall()
    assert rows == []


def test_suspended_user_cannot_upload(db, stub_pipeline):
    _make_user(db, status='suspended')
    result = wa.handle_whatsapp_webhook(
        _form(), signature='', webhook_url='',
    )
    # FR reply because C1's language is 'fr'.
    assert 'suspendu' in result['reply_body'].lower()
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT * FROM documents").fetchall()
    assert rows == []


def test_removed_user_cannot_upload(db, stub_pipeline):
    _make_user(db, status='removed')
    result = wa.handle_whatsapp_webhook(
        _form(), signature='', webhook_url='',
    )
    assert 'révoqué' in result['reply_body'].lower() \
        or 'revoked' in result['reply_body'].lower()
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT * FROM documents").fetchall()
    assert rows == []


# ---------------------------------------------------------------------------
# Signature still verified
# ---------------------------------------------------------------------------

def test_twilio_signature_still_verified(db, monkeypatch):
    # Turn the sandbox bypass back off. Without a valid Twilio auth
    # token + signature, _validate_twilio_signature returns True on
    # an empty auth_token (dev mode); here we patch it to False to
    # simulate a real signature failure.
    monkeypatch.delenv('TWILIO_SANDBOX', raising=False)
    monkeypatch.setattr(wa, '_validate_twilio_signature',
                         lambda sig, url, params: False)
    result = wa.handle_whatsapp_webhook(
        _form(), signature='bogus', webhook_url='',
    )
    assert result['ok'] is False
    assert result['error'] == 'invalid_signature'
