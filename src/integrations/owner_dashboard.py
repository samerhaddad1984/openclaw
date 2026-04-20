"""Owner admin dashboard: revenue / firm health / system health /
feedback / support queue.

All functions are pure aggregators over the DB. Route wiring in
review_dashboard.py gates access to role='owner'. Monitoring cron
uses these helpers too so the alert thresholds stay consistent.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso_hours_ago(h: int) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(hours=h)).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _safe_count(conn: sqlite3.Connection, sql: str,
                 args: tuple = ()) -> int:
    try:
        row = conn.execute(sql, args).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Revenue
# ---------------------------------------------------------------------------


_PLAN_MRR_CAD: dict[str, float] = {
    'starter_monthly':  49.0,
    'starter_yearly':   490.0 / 12,
    'pro_monthly':      149.0,
    'pro_yearly':       1490.0 / 12,
    'business_monthly': 399.0,
    'business_yearly':  3990.0 / 12,
}


def revenue_overview(db_path: Path | str) -> dict[str, Any]:
    """Returns current MRR, failed payments in last 7 days, at-risk
    subscriptions count. Graceful on a test DB with no billing table."""
    with _open(db_path) as conn:
        mrr = 0.0
        if _table_exists(conn, 'firms'):
            try:
                rows = conn.execute(
                    "SELECT plan FROM firms WHERE "
                    "COALESCE(subscription_status,'active') != 'cancelled'"
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            for r in rows:
                mrr += _PLAN_MRR_CAD.get((r['plan'] or '').lower(), 0.0)

        failed_7d = 0
        if _table_exists(conn, 'stripe_events_processed'):
            failed_7d = _safe_count(
                conn,
                "SELECT COUNT(*) FROM stripe_events_processed "
                "WHERE event_type LIKE '%payment_failed%' "
                "AND processed_at >= ?",
                (_iso_hours_ago(24 * 7),),
            )

        at_risk = 0
        if _table_exists(conn, 'firms'):
            at_risk = _safe_count(
                conn,
                "SELECT COUNT(*) FROM firms WHERE subscription_status "
                "IN ('past_due', 'cancel_scheduled')",
            )

    return {
        'mrr_cad': round(mrr, 2),
        'failed_payments_7d': failed_7d,
        'at_risk_count': at_risk,
    }


# ---------------------------------------------------------------------------
# Firm health
# ---------------------------------------------------------------------------


def firm_health(db_path: Path | str) -> dict[str, Any]:
    with _open(db_path) as conn:
        total = _safe_count(conn, "SELECT COUNT(*) FROM firms")
        active_week = _safe_count(
            conn,
            "SELECT COUNT(DISTINCT firm_code) FROM dashboard_users "
            "WHERE first_login_at IS NOT NULL AND "
            "first_login_at >= ?",
            (_iso_hours_ago(24 * 7),),
        )
        never_logged_in = _safe_count(
            conn,
            "SELECT COUNT(*) FROM firms f "
            "LEFT JOIN dashboard_users u ON u.firm_code=f.firm_code "
            "  AND u.first_login_at IS NOT NULL "
            "WHERE u.username IS NULL",
        )
    return {
        'total_firms': total,
        'active_this_week': active_week,
        'never_logged_in': never_logged_in,
    }


# ---------------------------------------------------------------------------
# System health
# ---------------------------------------------------------------------------


def system_health(db_path: Path | str) -> dict[str, Any]:
    db_size_mb = 0.0
    try:
        db_size_mb = round(Path(db_path).stat().st_size / (1024 * 1024), 2)
    except OSError:
        pass

    disk_pct = 0.0
    try:
        usage = shutil.disk_usage(str(Path(db_path).resolve().parent))
        disk_pct = round((usage.used / usage.total) * 100, 1)
    except OSError:
        pass

    rss_mb = 0.0
    try:
        import psutil
        rss_mb = round(psutil.Process().memory_info().rss / (1024 * 1024),
                         2)
    except Exception:
        pass

    with _open(db_path) as conn:
        last_qbo = None
        if _table_exists(conn, 'qbo_sync_log'):
            row = conn.execute(
                "SELECT MAX(completed_at) FROM qbo_sync_log WHERE errors=0"
            ).fetchone()
            last_qbo = row[0] if row else None

    return {
        'db_size_mb': db_size_mb,
        'disk_used_percent': disk_pct,
        'rss_mb': rss_mb,
        'last_qbo_sync_success': last_qbo,
    }


# ---------------------------------------------------------------------------
# Recent feedback + support queue
# ---------------------------------------------------------------------------


def recent_feedback(db_path: Path | str, limit: int = 10) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        if not _table_exists(conn, 'feedback'):
            return []
        rows = conn.execute(
            "SELECT * FROM feedback ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def support_queue(db_path: Path | str) -> dict[str, Any]:
    """Firms that need operator attention in the last 24h."""
    with _open(db_path) as conn:
        error_firms = []
        if _table_exists(conn, 'error_log'):
            rows = conn.execute(
                "SELECT firm_code, COUNT(*) AS n FROM error_log "
                "WHERE created_at >= ? GROUP BY firm_code",
                (_iso_hours_ago(24),),
            ).fetchall()
            error_firms = [dict(r) for r in rows]

        feedback_open = 0
        if _table_exists(conn, 'feedback'):
            feedback_open = _safe_count(
                conn,
                "SELECT COUNT(*) FROM feedback "
                "WHERE COALESCE(responded_at,'') = ''",
            )
    return {
        'firms_with_errors_24h': error_firms,
        'open_feedback': feedback_open,
    }


# ---------------------------------------------------------------------------
# Per-firm drilldown
# ---------------------------------------------------------------------------


def firms_drilldown(db_path: Path | str) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        if not _table_exists(conn, 'firms'):
            return []
        rows = conn.execute("""
            SELECT f.firm_code, f.name, f.plan,
                   MAX(u.first_login_at) AS last_login
            FROM firms f
            LEFT JOIN dashboard_users u ON u.firm_code = f.firm_code
            GROUP BY f.firm_code
            ORDER BY f.firm_code
        """).fetchall()
        out = []
        for r in rows:
            firm_code = r['firm_code']
            doc_count = _safe_count(
                conn,
                "SELECT COUNT(*) FROM documents WHERE firm_code=?",
                (firm_code,),
            )
            out.append({
                'firm_code': firm_code,
                'name': r['name'],
                'plan': r['plan'],
                'last_login': r['last_login'],
                'doc_count': doc_count,
                'mrr_cad': _PLAN_MRR_CAD.get((r['plan'] or '').lower(), 0.0),
            })
    return out


# ---------------------------------------------------------------------------
# Monitoring — anomaly detection + alert dispatch
# ---------------------------------------------------------------------------


DEFAULT_THRESHOLDS = {
    'disk_pct_critical': 90.0,
    'rss_mb_critical': 2_048.0,
    'failed_payments_24h_critical': 5,
    'firms_with_errors_critical': 3,
}


def detect_anomalies(
    db_path: Path | str,
    *,
    thresholds: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    th = dict(DEFAULT_THRESHOLDS)
    if thresholds:
        th.update(thresholds)
    alerts: list[dict[str, Any]] = []

    sys_h = system_health(db_path)
    if sys_h['disk_used_percent'] >= th['disk_pct_critical']:
        alerts.append({
            'severity': 'critical', 'kind': 'disk_full',
            'value': sys_h['disk_used_percent'],
            'threshold': th['disk_pct_critical'],
            'message': f"Disk at {sys_h['disk_used_percent']}% — "
                        f"threshold {th['disk_pct_critical']}%",
        })
    if sys_h['rss_mb'] >= th['rss_mb_critical']:
        alerts.append({
            'severity': 'critical', 'kind': 'rss_high',
            'value': sys_h['rss_mb'],
            'threshold': th['rss_mb_critical'],
            'message': f"RSS at {sys_h['rss_mb']} MB",
        })

    rev = revenue_overview(db_path)
    if rev['failed_payments_7d'] >= th['failed_payments_24h_critical']:
        alerts.append({
            'severity': 'warning', 'kind': 'failed_payments',
            'value': rev['failed_payments_7d'],
            'threshold': th['failed_payments_24h_critical'],
            'message': f"{rev['failed_payments_7d']} failed payments in "
                        "last 7 days",
        })

    support = support_queue(db_path)
    if len(support['firms_with_errors_24h']) >= th['firms_with_errors_critical']:
        alerts.append({
            'severity': 'warning', 'kind': 'firm_errors',
            'value': len(support['firms_with_errors_24h']),
            'threshold': th['firms_with_errors_critical'],
            'message': f"{len(support['firms_with_errors_24h'])} firms "
                        "with errors in last 24h",
        })
    return alerts


def dispatch_alerts(
    alerts: list[dict[str, Any]],
    *,
    email_fn=None,
    sms_fn=None,
    to_email: str | None = None,
    to_sms: str | None = None,
) -> dict[str, int]:
    """Send alerts via injected email/sms functions. Returns a dispatch
    tally. Never raises — failed dispatch is logged and counted."""
    sent_email = 0
    sent_sms = 0
    failed = 0
    for a in alerts:
        subject = f"[OtoCPA {a['severity'].upper()}] {a['kind']}"
        body = a['message']
        if email_fn and to_email:
            try:
                email_fn(to_email, subject, body)
                sent_email += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("email dispatch failed: %s", exc)
                failed += 1
        if a['severity'] == 'critical' and sms_fn and to_sms:
            try:
                sms_fn(to_sms, body)
                sent_sms += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("sms dispatch failed: %s", exc)
                failed += 1
    return {'email': sent_email, 'sms': sent_sms, 'failed': failed}


# ---------------------------------------------------------------------------
# Bundle — single call that returns everything the dashboard needs
# ---------------------------------------------------------------------------


def build_dashboard(db_path: Path | str) -> dict[str, Any]:
    return {
        'revenue': revenue_overview(db_path),
        'firms': firm_health(db_path),
        'system': system_health(db_path),
        'feedback': recent_feedback(db_path),
        'support': support_queue(db_path),
        'drilldown': firms_drilldown(db_path),
        'alerts': detect_anomalies(db_path),
        'generated_at': datetime.now(timezone.utc)
                         .replace(microsecond=0).isoformat(),
    }
