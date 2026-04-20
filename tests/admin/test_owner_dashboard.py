"""Gap 3 — Sam admin dashboard + monitoring + alerting."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.owner_dashboard import (  # noqa: E402
    build_dashboard,
    detect_anomalies,
    dispatch_alerts,
    firm_health,
    firms_drilldown,
    recent_feedback,
    revenue_overview,
    support_queue,
    system_health,
)


def _mk(tmp_path):
    db = tmp_path / 'a.db'
    with sqlite3.connect(db) as conn:
        conn.execute("""
            CREATE TABLE firms (
                firm_code TEXT PRIMARY KEY, name TEXT, plan TEXT,
                subscription_status TEXT DEFAULT 'active'
            )
        """)
        conn.execute("""
            CREATE TABLE dashboard_users (
                username TEXT PRIMARY KEY, role TEXT, firm_code TEXT,
                first_login_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, firm_code TEXT
            )
        """)
        conn.commit()
    return db


# --- revenue ---

def test_revenue_empty(tmp_path):
    db = _mk(tmp_path)
    r = revenue_overview(db)
    assert r == {'mrr_cad': 0.0, 'failed_payments_7d': 0,
                 'at_risk_count': 0}


def test_revenue_mrr_sums_plans(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan) VALUES "
            "('F1','A','starter_monthly'),('F2','B','pro_monthly'),"
            "('F3','C','business_monthly')"
        )
        conn.commit()
    r = revenue_overview(db)
    assert r['mrr_cad'] == round(49 + 149 + 399, 2)


def test_revenue_excludes_cancelled(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan, subscription_status) "
            "VALUES ('F1','A','pro_monthly','cancelled')"
        )
        conn.commit()
    r = revenue_overview(db)
    assert r['mrr_cad'] == 0.0


def test_revenue_at_risk_counts(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan, subscription_status) "
            "VALUES ('F1','A','pro_monthly','past_due'), "
            "       ('F2','B','pro_monthly','cancel_scheduled'), "
            "       ('F3','C','pro_monthly','active')"
        )
        conn.commit()
    r = revenue_overview(db)
    assert r['at_risk_count'] == 2


# --- firm health ---

def test_firm_health_counts_all_categories(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan) VALUES "
            "('F1','A','pro_monthly'),('F2','B','pro_monthly')"
        )
        # F1 has a user that logged in this week
        conn.execute(
            "INSERT INTO dashboard_users (username, role, firm_code, "
            "first_login_at) VALUES ('u1','owner','F1', datetime('now'))"
        )
        # F2 never logged in (no dashboard_users row)
        conn.commit()
    fh = firm_health(db)
    assert fh['total_firms'] == 2
    assert fh['active_this_week'] == 1
    assert fh['never_logged_in'] == 1


# --- system health ---

def test_system_health_returns_numbers(tmp_path):
    db = _mk(tmp_path)
    sh = system_health(db)
    assert 'db_size_mb' in sh
    assert 'disk_used_percent' in sh
    assert 'rss_mb' in sh
    assert isinstance(sh['db_size_mb'], float)


# --- support + feedback ---

def test_recent_feedback_empty_when_table_absent(tmp_path):
    db = _mk(tmp_path)
    assert recent_feedback(db) == []


def test_recent_feedback_returns_rows(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE feedback (id INTEGER PRIMARY KEY, "
            "firm_code TEXT, rating INTEGER, comment TEXT, "
            "created_at TEXT, responded_at TEXT)"
        )
        conn.execute(
            "INSERT INTO feedback (firm_code, rating, comment, created_at) "
            "VALUES ('F1', 4, 'nice', '2026-04-20T10:00:00Z')"
        )
        conn.commit()
    rows = recent_feedback(db)
    assert len(rows) == 1
    assert rows[0]['rating'] == 4


def test_support_queue_counts_unresponded(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE feedback (id INTEGER PRIMARY KEY, "
            "firm_code TEXT, responded_at TEXT, created_at TEXT)"
        )
        conn.execute(
            "INSERT INTO feedback (firm_code, responded_at) VALUES "
            "('F1', NULL), ('F2', '2026-04-20T00:00:00Z'), ('F3', '')"
        )
        conn.commit()
    sq = support_queue(db)
    assert sq['open_feedback'] == 2  # F1 and F3 (NULL or empty)


# --- drilldown ---

def test_drilldown_includes_doc_count(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO firms (firm_code, name, plan) VALUES "
            "('F1', 'A', 'pro_monthly')"
        )
        for i in range(3):
            conn.execute(
                "INSERT INTO documents (document_id, firm_code) VALUES (?,?)",
                (f'D{i}', 'F1'),
            )
        conn.commit()
    rows = firms_drilldown(db)
    assert len(rows) == 1
    assert rows[0]['doc_count'] == 3
    assert rows[0]['mrr_cad'] == 149.0


# --- anomaly detection + alerting ---

def test_detect_no_anomalies_when_clean(tmp_path):
    db = _mk(tmp_path)
    alerts = detect_anomalies(db)
    # No firms, no data -> should report clean. Disk % may be high on
    # the host but that's environment-dependent; mock via thresholds.
    alerts = detect_anomalies(db, thresholds={
        'disk_pct_critical': 101.0,
        'rss_mb_critical': 100_000.0,
        'failed_payments_24h_critical': 1_000_000,
        'firms_with_errors_critical': 1_000_000,
    })
    assert alerts == []


def test_detect_disk_full_triggers_alert(tmp_path):
    db = _mk(tmp_path)
    alerts = detect_anomalies(db, thresholds={'disk_pct_critical': 0.0})
    assert any(a['kind'] == 'disk_full' for a in alerts)
    assert any(a['severity'] == 'critical' for a in alerts)


def test_detect_failed_payments_triggers_warning(tmp_path):
    db = _mk(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE stripe_events_processed ("
            "event_id TEXT, event_type TEXT, processed_at TEXT)"
        )
        for i in range(3):
            conn.execute(
                "INSERT INTO stripe_events_processed VALUES (?,?,datetime('now'))",
                (f'evt{i}', 'invoice.payment_failed'),
            )
        conn.commit()
    alerts = detect_anomalies(db, thresholds={
        'disk_pct_critical': 101.0,
        'rss_mb_critical': 100_000.0,
        'failed_payments_24h_critical': 2,
        'firms_with_errors_critical': 1_000_000,
    })
    assert any(a['kind'] == 'failed_payments' for a in alerts)


def test_dispatch_alerts_calls_email_function():
    sent = []
    def email_fn(to, subj, body):
        sent.append((to, subj, body))
    out = dispatch_alerts(
        [{'severity': 'warning', 'kind': 'test', 'message': 'hi'}],
        email_fn=email_fn, to_email='sam@example.com',
    )
    assert out['email'] == 1
    assert sent == [('sam@example.com', '[OtoCPA WARNING] test', 'hi')]


def test_dispatch_sms_only_for_critical():
    emails, texts = [], []
    def email_fn(to, subj, body): emails.append(to)
    def sms_fn(to, body): texts.append(to)
    out = dispatch_alerts(
        [{'severity': 'warning', 'kind': 'w', 'message': 'x'},
         {'severity': 'critical', 'kind': 'c', 'message': 'y'}],
        email_fn=email_fn, sms_fn=sms_fn,
        to_email='a@b.com', to_sms='+15555550000',
    )
    assert out['email'] == 2
    assert out['sms'] == 1  # only the critical one


def test_dispatch_never_raises_on_send_failure():
    def bad_email(to, subj, body):
        raise RuntimeError('smtp down')
    out = dispatch_alerts(
        [{'severity': 'critical', 'kind': 't', 'message': 'x'}],
        email_fn=bad_email, to_email='a@b.com',
    )
    assert out['email'] == 0
    assert out['failed'] == 1


# --- bundle ---

def test_build_dashboard_includes_every_section(tmp_path):
    db = _mk(tmp_path)
    out = build_dashboard(db)
    for key in ('revenue', 'firms', 'system', 'feedback',
                 'support', 'drilldown', 'alerts', 'generated_at'):
        assert key in out
