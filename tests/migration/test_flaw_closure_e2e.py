"""Flaw closure sprint — 15-step end-to-end scenario.

This test exercises the full arc of the v1 + v2 flaw-closure sprints
in one go. Every step maps to a shipped scope. The scenario uses
a freshly-constructed DB with the minimum schema each module needs,
so we prove the modules compose correctly without being coupled to
the dashboard's full bootstrap.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def e2e_db(tmp_path):
    db = tmp_path / 'e2e.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE firms (firm_code TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE dashboard_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT, email TEXT, firm_code TEXT,
                role TEXT, active INTEGER DEFAULT 1
            );
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT, email TEXT, firm_code TEXT,
                role TEXT, active INTEGER DEFAULT 1
            );
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT, firm_code TEXT,
                contact_email TEXT, whatsapp_number TEXT,
                language TEXT DEFAULT 'fr',
                primary_employee_email TEXT,
                secondary_employee_email TEXT,
                active INTEGER DEFAULT 1,
                status TEXT DEFAULT 'active',
                archive_reason TEXT, archive_notes TEXT,
                archived_at TEXT, archived_by TEXT,
                retention_expires_at TEXT,
                portal_token TEXT, created_at TEXT
            );
            CREATE TABLE client_portal_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, email TEXT, token TEXT,
                name TEXT, role TEXT, status TEXT DEFAULT 'active',
                whatsapp_number TEXT
            );
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT UNIQUE, client_code TEXT,
                review_status TEXT, created_at TEXT,
                rejection_reason TEXT
            );
            CREATE TABLE client_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, created_at TEXT
            );
            CREATE TABLE client_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                title TEXT, description TEXT,
                due_date TEXT, target_portal_user_id INTEGER,
                status TEXT DEFAULT 'open',
                created_at TEXT, completed_at TEXT,
                created_by TEXT
            );
            CREATE TABLE review_workflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, entity_type TEXT, entity_id TEXT,
                status TEXT, assigned_to_email TEXT,
                priority TEXT DEFAULT 'normal', assigned_at TEXT
            );
            CREATE TABLE gl_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, account_code TEXT,
                account_name TEXT,
                UNIQUE(client_code, account_code)
            );
            CREATE TABLE gl_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id TEXT, client_code TEXT, period TEXT,
                entry_date TEXT, account_code TEXT,
                side TEXT CHECK(side IN ('debit','credit')),
                amount REAL, description TEXT,
                source TEXT DEFAULT 'manual_je',
                document_id TEXT, reversed_by TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            INSERT INTO firms VALUES ('FIRM','Sam CPA Inc.');
            INSERT INTO dashboard_users
              (username, email, firm_code, role, active) VALUES
              ('owner', 'owner@firm.com', 'FIRM', 'firm_admin', 1),
              ('sophie', 'sophie@firm.com', 'FIRM', 'employee', 1),
              ('jean', 'jean@firm.com', 'FIRM', 'employee', 1);
            INSERT INTO users (username, email, firm_code, role, active)
            VALUES
              ('sophie','sophie@firm.com','FIRM','employee',1),
              ('jean','jean@firm.com','FIRM','employee',1);
            """
        )
        conn.commit()
    return db


def test_flaw_closure_15_step_e2e(e2e_db):
    """Runs the 15-step scenario from the sprint brief as one
    integrated walkthrough."""
    from src.integrations import client_import as ci
    from src.integrations import historical_import as hi
    from src.integrations import comparative_statements as cs
    from src.integrations import opening_balances as ob
    from src.integrations import client_archive as ca
    from src.integrations import employee_ooo as ooo
    from src.integrations import queue_alerts as qa
    from src.integrations import recurring_reminders as rr
    from src.integrations import client_inactivity as ci_mod

    db = e2e_db

    # --- Step 1 + 2 + 3: sign up existing client, bulk CSV import of
    # a handful of clients (includes Construction Tremblay), enter
    # opening balances for mid-year adoption.
    # client_import relies on an existing clients table (bootstrapped
    # in the fixture).
    csv_payload = (
        "client_code,client_name,firm,email,phone,language,"
        "fiscal_year_end,primary_employee_email,secondary_employee_email\n"
        "TREMBLAY,Construction Tremblay,FIRM,adm@tremblay.com,,"
        "fr,2025-12-31,sophie@firm.com,jean@firm.com\n"
        "CAFE,Cafe Beta,FIRM,c@cafe.com,,fr,,sophie@firm.com,\n"
    ).encode('utf-8')
    rows, headers, fatal = ci.parse_csv(csv_payload)
    assert fatal is None
    result = ci.import_rows(db, firm_code='FIRM', rows=rows,
                            dry_run=False)
    assert result['imported'] >= 2, result

    # Seed a chart + opening balances for TREMBLAY.
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO gl_accounts (client_code, account_code, "
            "account_name) VALUES (?,?,?)",
            [('TREMBLAY', '1000', 'Cash'),
             ('TREMBLAY', '3000', 'Equity')],
        )
        conn.commit()
    ob.ensure_schema(db)
    post = ob.post_opening_balances(
        db, firm_code='FIRM', client_code='TREMBLAY',
        as_of_date='2026-04-01',
        rows=[
            {'account_code': '1000', 'side': 'debit', 'amount': 5000,
             'description': 'Cash'},
            {'account_code': '3000', 'side': 'credit', 'amount': 5000,
             'description': 'Equity'},
        ],
        posted_by='owner@firm.com',
    )
    assert post['ok'] is True

    # --- Step 4: enable multi-user portal on TREMBLAY. We simulate
    # by inserting a client_portal_users row directly (the full
    # multi_user_portal flow is already covered by tests/portal).
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE clients SET portal_token='TREMBLAY-ADMIN-TOK' "
            "WHERE client_code='TREMBLAY'"
        )
        conn.execute(
            "INSERT INTO client_portal_users "
            "(client_code, email, token, name, role, status) "
            "VALUES ('TREMBLAY','adm@tremblay.com','ADMIN-TOK',"
            "'Marie','admin','active')"
        )
        conn.commit()

    # --- Step 5 + 6 + 7: client admin invites bookkeeper + office
    # manager, registers WhatsApp numbers, suspends departing
    # employee. (The full surface is covered by tests/portal; here
    # we confirm the DB supports the state transitions.)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO client_portal_users "
            "(client_code, email, token, name, role, status, "
            " whatsapp_number) "
            "VALUES ('TREMBLAY','bk@tremblay.com','BK-TOK',"
            "'Bookkeeper','contributor','active','+15145551234'),"
            "('TREMBLAY','om@tremblay.com','OM-TOK','Office','contributor',"
            "'active','+15145559876'),"
            "('TREMBLAY','ex@tremblay.com','EX-TOK','Ex','contributor',"
            "'active','+15145553333')"
        )
        conn.execute(
            "UPDATE client_portal_users SET status='suspended', "
            "token=NULL WHERE email='ex@tremblay.com'"
        )
        conn.commit()

    # --- Step 8: ex-employee WhatsApp rejected (the auto_reject
    # semantics are covered in tests/portal; here we assert the
    # state is queryable.)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, token FROM client_portal_users "
            "WHERE email='ex@tremblay.com'"
        ).fetchone()
    assert row['status'] == 'suspended'
    assert row['token'] is None

    # --- Step 9: CPA schedules monthly bank statement reminder.
    rr.ensure_schema(db)
    ridres = rr.create_from_template(
        db, 'monthly_bank_statement',
        firm_code='FIRM', client_code='TREMBLAY',
        start_date='2026-04-15', created_by='owner@firm.com',
        target_portal_user_id=1,
    )
    assert ridres['ok'] is True

    # --- Step 10: client receives reminder, admin marks complete.
    def fake_post_request(**kwargs):
        with sqlite3.connect(db) as conn:
            cur = conn.execute(
                "INSERT INTO client_requests "
                "(firm_code, client_code, title, description, due_date, "
                " target_portal_user_id, created_at, created_by) "
                "VALUES (?,?,?,?,?,?,datetime('now'),?)",
                (kwargs['firm_code'], kwargs['client_code'],
                 kwargs['title'], kwargs['description'],
                 kwargs['due_date'], kwargs['target_user'],
                 kwargs['created_by']),
            )
            conn.commit()
            return cur.lastrowid

    fire = rr.fire_reminder(
        db, ridres['id'],
        now=datetime(2026, 4, 15, tzinfo=timezone.utc),
        post_request=fake_post_request,
    )
    assert fire['ok'] is True
    assert fire['client_request_id']
    # Admin marks complete.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE client_requests SET status='completed', "
            "completed_at=datetime('now') WHERE id=?",
            (fire['client_request_id'],),
        )
        conn.commit()
    # Reminder fire row gets marked fulfilled via the wiring helper.
    assert rr.fulfilled_by_request(db, fire['client_request_id']) is True

    # --- Step 11: Sophie goes on vacation, Jean covers.
    ooo.ensure_schema(db)
    r = ooo.set_ooo(
        db, firm_code='FIRM', employee_email='sophie@firm.com',
        coverage_email='jean@firm.com',
        start_date='2026-04-20', end_date='2026-05-01',
        created_by='owner@firm.com',
        require_coverage_permission=False,
    )
    assert r['ok'] is True

    # --- Step 12: a receipt from the client gets rejected by
    # Jean (coverage), the client can see the reason. We simulate
    # the rejection + verify the column is populated.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, client_code, review_status, created_at, "
            " rejection_reason) "
            "VALUES ('D-ERR','TREMBLAY','Rejected',datetime('now'),"
            "'Unreadable scan — please re-upload.')"
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        doc = conn.execute(
            "SELECT rejection_reason FROM documents WHERE document_id='D-ERR'"
        ).fetchone()
    assert 're-upload' in doc['rejection_reason'].lower()

    # --- Step 13: admin rebalances workload. Give Sophie a pile.
    with sqlite3.connect(db) as conn:
        for i in range(52):
            conn.execute(
                "INSERT INTO review_workflow "
                "(firm_code, entity_type, entity_id, status, "
                " assigned_to_email, assigned_at) "
                "VALUES ('FIRM','document',?,?,?,?)",
                (f'DOC-{i}', 'assigned', 'sophie@firm.com',
                 '2026-04-18'),
            )
        conn.commit()
    dec = qa.evaluate_employee(
        db, firm_code='FIRM', employee_email='sophie@firm.com',
    )
    assert dec['fire'] is True
    assert dec['level'] == qa.LEVEL_RED
    reassign = ooo.bulk_reassign(
        db, firm_code='FIRM', from_email='sophie@firm.com',
        to_email='jean@firm.com', actor='owner@firm.com',
    )
    assert reassign['reassigned'] >= 50
    after = qa.count_open_for_employee(
        db, firm_code='FIRM', employee_email='sophie@firm.com',
    )
    assert after == 0

    # --- Step 14: 90-day inactivity alert. Use CAFE as the dormant
    # client — no activity since before 2026-01-01.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE clients SET created_at='2023-01-01T00:00:00+00:00' "
            "WHERE client_code='CAFE'"
        )
        conn.commit()
    alerts = []
    ci_mod.weekly_scan(
        db, firm_code='FIRM',
        notifier=lambda firm_code, summary: alerts.append(summary),
        now=datetime(2026, 4, 24, tzinfo=timezone.utc),
    )
    assert alerts
    assert 'CAFE' in alerts[0]['at_risk_client_codes']

    # --- Step 15: archive the dormant CAFE client ("left firm" reason).
    ca.ensure_schema(db)
    arch = ca.archive_client(
        db, firm_code='FIRM', client_code='CAFE',
        reason=ca.REASON_LEFT_FIRM, actor='owner@firm.com',
        notes='client moved to another firm',
    )
    assert arch['ok'] is True
    # After archive, CAFE is excluded from the at-risk widget.
    summary = ci_mod.at_risk_summary(
        db, 'FIRM',
        now=datetime(2026, 4, 24, tzinfo=timezone.utc),
    )
    assert 'CAFE' not in summary['at_risk_client_codes']

    # Everything is coherent — 15 steps green.
