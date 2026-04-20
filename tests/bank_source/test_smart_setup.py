"""Phase 4 — smart bank-setup decision + renders + HTTP handlers."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.qbo_oauth import _ensure_table, store_qbo_tokens  # noqa: E402
from src.integrations.bank_source_schema import ensure_bank_source_schema  # noqa: E402
from src.integrations.bank_source_setup import (  # noqa: E402
    STATE_BOTH_ACTIVE, STATE_CHOICE, STATE_PLAID_ACTIVE,
    STATE_PLAID_RECOMMENDED, STATE_QBO_ONLY,
    decide_bank_setup,
    handle_sync_from_qbo,
    handle_unmark_duplicate,
    render_bank_setup_page,
    render_dedup_page,
)


def _mk_db(tmp_path, *, client='C1', firm='F1'):
    db = tmp_path / 'bs.db'
    _ensure_table(db)
    ensure_bank_source_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS clients "
            "(client_code TEXT PRIMARY KEY, firm_code TEXT, bank_source TEXT)"
        )
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, bank_source) "
            "VALUES (?,?,?)",
            (client, firm, 'none'),
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bank_connections "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, client_code TEXT, "
            " active INTEGER DEFAULT 1)"
        )
        conn.commit()
    return db


def _connect_qbo(db, client='C1', firm='F1'):
    store_qbo_tokens(firm_code=firm, client_code=client, realm_id='R1',
                      access_token='at', refresh_token='rt',
                      expires_in=3600, db_path=db)


def _connect_plaid(db, client='C1'):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_connections (client_code, active) VALUES (?,?)",
            (client, 1),
        )
        conn.commit()


class StubQBOBankPull:
    """Lightweight stand-in for QBOBankPull.detect_bank_accounts /
    has_bank_feeds. Configured by class attributes in each test."""
    bank_accounts: list[dict] = []
    feeds_present: bool = False

    def __init__(self, firm, client, db_path, sandbox=False):
        self.firm = firm; self.client = client

    def detect_bank_accounts(self):
        return list(type(self).bank_accounts)

    def has_bank_feeds(self):
        return type(self).feeds_present


# --- decision ---

def test_decide_neither_returns_choice(tmp_path):
    db = _mk_db(tmp_path)
    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=StubQBOBankPull)
    assert d['state'] == STATE_CHOICE
    assert d['has_qbo'] is False
    assert d['has_plaid'] is False


def test_decide_plaid_only(tmp_path):
    db = _mk_db(tmp_path)
    _connect_plaid(db)
    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=StubQBOBankPull)
    assert d['state'] == STATE_PLAID_ACTIVE


def test_decide_qbo_with_banks_recommends_qbo(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db)

    class P(StubQBOBankPull):
        bank_accounts = [{'Id': '1', 'Name': 'Chequing', 'AcctNum': '1000',
                           'CurrencyRef': {'value': 'CAD'}}]
        feeds_present = True

    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=P)
    assert d['state'] == STATE_QBO_ONLY
    assert d['qbo_has_banks'] is True
    assert len(d['qbo_accounts']) == 1
    assert d['qbo_accounts'][0]['name'] == 'Chequing'


def test_decide_qbo_without_banks_recommends_plaid(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db)

    class P(StubQBOBankPull):
        bank_accounts = []
        feeds_present = False

    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=P)
    assert d['state'] == STATE_PLAID_RECOMMENDED


def test_decide_both_active(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db)
    _connect_plaid(db)

    class P(StubQBOBankPull):
        bank_accounts = [{'Id': '1', 'Name': 'Chequing'}]
        feeds_present = True

    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=P)
    assert d['state'] == STATE_BOTH_ACTIVE


def test_decide_tolerates_qbo_probe_failure(tmp_path):
    """If QBO probe explodes we fall back to PLAID_RECOMMENDED / CHOICE
    but never 500."""
    db = _mk_db(tmp_path)
    _connect_qbo(db)

    class Boom(StubQBOBankPull):
        def detect_bank_accounts(self):
            raise RuntimeError('realm unreachable')

    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=Boom)
    # has_qbo is True but qbo_has_banks stays False → recommend Plaid.
    assert d['state'] == STATE_PLAID_RECOMMENDED


# --- render ---

def test_render_choice_state_shows_connect_buttons(tmp_path):
    db = _mk_db(tmp_path)
    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=StubQBOBankPull)
    html = render_bank_setup_page(d, lang='en')
    assert '/qbo/connect?client_code=C1' in html
    assert '/bank/connect?client_code=C1' in html


def test_render_qbo_only_shows_sync_button(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db)

    class P(StubQBOBankPull):
        bank_accounts = [{'Id': '1', 'Name': 'Chequing',
                           'AcctNum': '1000',
                           'CurrencyRef': {'value': 'CAD'}}]
        feeds_present = True

    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=P)
    html = render_bank_setup_page(d, lang='en')
    assert '/clients/C1/bank/sync_from_qbo' in html
    assert 'Chequing' in html


def test_render_both_active_links_dedup(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db); _connect_plaid(db)

    class P(StubQBOBankPull):
        bank_accounts = [{'Id': '1', 'Name': 'Chequing'}]
        feeds_present = True

    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=P)
    html = render_bank_setup_page(d, lang='en')
    assert '/clients/C1/bank/dedup' in html


def test_render_french(tmp_path):
    db = _mk_db(tmp_path)
    d = decide_bank_setup(db, firm_code='F1', client_code='C1',
                            qbo_puller_cls=StubQBOBankPull)
    html = render_bank_setup_page(d, lang='fr')
    assert 'Choisissez comment alimenter' in html


# --- dedup page ---

def test_render_dedup_page_empty(tmp_path):
    db = _mk_db(tmp_path)
    html = render_dedup_page(db, firm_code='F1', client_code='C1')
    assert 'No duplicates detected yet' in html


def test_render_dedup_page_with_rows(tmp_path):
    db = _mk_db(tmp_path)
    # Seed bank_tx_dedup directly
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_tx_dedup "
            "(firm_code, client_code, primary_source, primary_id, "
            " duplicate_source, duplicate_id, match_confidence, "
            " detected_at, resolved_by) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            ('F1', 'C1', 'qbo', 'qbo-1', 'plaid', 'plaid-1',
             0.92, '2026-04-20T10:00:00Z', 'auto'),
        )
        conn.commit()
    html = render_dedup_page(db, firm_code='F1', client_code='C1')
    assert 'qbo-1' in html
    assert 'plaid-1' in html
    assert '/clients/C1/bank/dedup/unmark' in html


# --- HTTP handlers ---

def test_handle_sync_from_qbo_updates_bank_source(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db)

    class P(StubQBOBankPull):
        def pull_bank_transactions(self, since_date=None):
            return 5

    # Monkey-patch the import inside the handler
    import src.integrations.bank_source_setup as mod
    mod.__dict__.setdefault('_orig_import', None)

    import src.integrations.qbo_bank_pull as _qbp
    orig = _qbp.QBOBankPull
    _qbp.QBOBankPull = P
    try:
        status, ctype, body = handle_sync_from_qbo(
            db, firm_code='F1', client_code='C1',
        )
    finally:
        _qbp.QBOBankPull = orig

    assert status == 200
    parsed = json.loads(body)
    assert parsed['ok'] is True
    assert parsed['pulled'] == 5
    assert parsed['bank_source'] == 'qbo'
    with sqlite3.connect(db) as conn:
        src = conn.execute(
            "SELECT bank_source FROM clients WHERE client_code='C1'"
        ).fetchone()[0]
    assert src == 'qbo'


def test_handle_sync_from_qbo_marks_both_when_plaid_present(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db); _connect_plaid(db)

    class P(StubQBOBankPull):
        def pull_bank_transactions(self, since_date=None):
            return 2

    import src.integrations.qbo_bank_pull as _qbp
    orig = _qbp.QBOBankPull
    _qbp.QBOBankPull = P
    try:
        status, _, body = handle_sync_from_qbo(
            db, firm_code='F1', client_code='C1',
        )
    finally:
        _qbp.QBOBankPull = orig

    parsed = json.loads(body)
    assert parsed['bank_source'] == 'both'


def test_handle_sync_from_qbo_surface_errors_as_502(tmp_path):
    db = _mk_db(tmp_path)
    _connect_qbo(db)

    class P(StubQBOBankPull):
        def pull_bank_transactions(self, since_date=None):
            raise RuntimeError('realm unreachable')

    import src.integrations.qbo_bank_pull as _qbp
    orig = _qbp.QBOBankPull
    _qbp.QBOBankPull = P
    try:
        status, _, body = handle_sync_from_qbo(
            db, firm_code='F1', client_code='C1',
        )
    finally:
        _qbp.QBOBankPull = orig
    assert status == 502
    assert 'realm unreachable' in body.decode()


def test_handle_unmark_duplicate_restores_row(tmp_path):
    db = _mk_db(tmp_path)
    # Seed a hidden dup
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO bank_transactions "
            "(id, firm_code, client_code, source, external_id, amount, "
            " hidden_duplicate) VALUES (?,?,?,?,?,?,?)",
            ('plaid-1', 'F1', 'C1', 'plaid', 'P-1', -50.0, 1),
        )
        conn.execute(
            "INSERT INTO bank_tx_dedup "
            "(firm_code, client_code, primary_source, primary_id, "
            " duplicate_source, duplicate_id, match_confidence, "
            " detected_at, resolved_by) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            ('F1', 'C1', 'qbo', 'qbo-1', 'plaid', 'plaid-1',
             0.9, '2026-04-20T00:00:00Z', 'auto'),
        )
        conn.commit()
    status, _, body = handle_unmark_duplicate(
        db, firm_code='F1', client_code='C1',
        duplicate_id='plaid-1', resolved_by='sam',
    )
    assert status == 200
    assert json.loads(body)['ok'] is True
    with sqlite3.connect(db) as conn:
        hid = conn.execute(
            "SELECT hidden_duplicate FROM bank_transactions WHERE id='plaid-1'"
        ).fetchone()[0]
    assert hid == 0


def test_route_wiring_grep():
    """Meta-test: the dashboard source contains the new route literals.
    Catches accidental removal of wiring."""
    src = (ROOT / 'scripts' / 'review_dashboard.py').read_text()
    assert '/bank/setup' in src
    assert '/bank/dedup' in src
    assert '/bank/sync_from_qbo' in src
    assert '/bank/dedup/unmark' in src
    assert 'from src.integrations.bank_source_setup import' in src
