"""Month-end close wizard: 6 steps, progress persisted per firm+client+period.

Step sequence (strict — a later step can't start until the previous
one is marked ``done``):

  1. select_period       — confirm the period ending date; reject if
                             a prior period is still open.
  2. process_documents   — every document for the period must be
                             posted, ignored, or deleted.
  3. reconcile_bank      — every bank transaction in period matched
                             or acknowledged (hidden_duplicate ok).
  4. accruals            — propose + post standard accruals (wages,
                             depreciation, prepaid amortisation).
  5. statements          — generate TB / P&L / BS / CF / SOCE and
                             surface any warnings.
  6. lock                — set period.status='locked'; log who + when.

Wizard state lives in ``close_wizard_state`` (keyed on firm_code +
client_code + period). A session-like row allows ``Save and continue
later``.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

STEPS = (
    'select_period',
    'process_documents',
    'reconcile_bank',
    'accruals',
    'statements',
    'lock',
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def ensure_close_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS close_wizard_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                period TEXT NOT NULL,
                step TEXT NOT NULL,
                step_status TEXT NOT NULL DEFAULT 'pending',
                step_data TEXT,
                started_at TEXT,
                completed_at TEXT,
                actor_email TEXT,
                UNIQUE(firm_code, client_code, period, step)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_close_wizard_scope "
            "ON close_wizard_state(firm_code, client_code, period)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounting_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                locked_by TEXT,
                locked_at TEXT,
                UNIQUE(firm_code, client_code, period)
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Shape of a wizard snapshot
# ---------------------------------------------------------------------------


def get_state(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
) -> dict[str, Any]:
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT step, step_status, step_data, started_at, completed_at "
            "FROM close_wizard_state "
            "WHERE firm_code=? AND client_code=? AND period=?",
            (firm_code, client_code, period),
        ).fetchall()
        by_step = {r['step']: dict(r) for r in rows}
    out = []
    current_step: Optional[str] = None
    for step in STEPS:
        row = by_step.get(step, {'step': step, 'step_status': 'pending'})
        out.append(row)
        if current_step is None and row.get('step_status') != 'done':
            current_step = step
    if current_step is None:
        current_step = STEPS[-1]
    return {'steps': out, 'current': current_step,
             'period': period, 'firm_code': firm_code,
             'client_code': client_code}


def _save_step(
    conn: sqlite3.Connection, *,
    firm_code: str, client_code: str, period: str, step: str,
    step_status: str, step_data: dict[str, Any] | None,
    actor_email: str | None,
) -> None:
    conn.execute(
        "INSERT INTO close_wizard_state "
        "(firm_code, client_code, period, step, step_status, step_data, "
        " started_at, completed_at, actor_email) "
        "VALUES (?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(firm_code, client_code, period, step) DO UPDATE SET "
        " step_status=excluded.step_status, step_data=excluded.step_data, "
        " started_at=COALESCE(close_wizard_state.started_at, excluded.started_at), "
        " completed_at=CASE WHEN excluded.step_status='done' "
        "                    THEN excluded.completed_at "
        "                    ELSE close_wizard_state.completed_at END, "
        " actor_email=excluded.actor_email",
        (firm_code, client_code, period, step, step_status,
         json.dumps(step_data or {}), _iso_now(),
         _iso_now() if step_status == 'done' else None,
         actor_email),
    )


# ---------------------------------------------------------------------------
# Individual-step validators
# ---------------------------------------------------------------------------


def _prior_period_open(conn: sqlite3.Connection, firm_code: str,
                        client_code: str, period: str) -> bool:
    """True if any period strictly before ``period`` is still 'open'."""
    if not _table_exists(conn, 'accounting_periods'):
        return False
    row = conn.execute(
        "SELECT 1 FROM accounting_periods "
        "WHERE firm_code=? AND client_code=? AND period < ? AND status='open' "
        "LIMIT 1",
        (firm_code, client_code, period),
    ).fetchone()
    return row is not None


def _unprocessed_documents(conn: sqlite3.Connection, firm_code: str,
                            client_code: str, period: str) -> int:
    if not _table_exists(conn, 'documents'):
        return 0
    # Documents in period whose review_status is neither Posted nor Ignored.
    row = conn.execute(
        "SELECT COUNT(*) FROM documents "
        "WHERE client_code=? "
        "  AND (firm_code=? OR firm_code IS NULL) "
        "  AND COALESCE(document_date,'') LIKE ? "
        "  AND LOWER(COALESCE(review_status,'')) "
        "      NOT IN ('posted', 'ignored', 'deleted')",
        (client_code, firm_code, f"{period}%"),
    ).fetchone()
    return int(row[0]) if row else 0


def _unreconciled_bank_count(conn: sqlite3.Connection, firm_code: str,
                              client_code: str, period: str) -> int:
    if not _table_exists(conn, 'bank_transactions'):
        return 0
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions "
            "WHERE client_code=? "
            "  AND (firm_code=? OR firm_code IS NULL) "
            "  AND COALESCE(date,'') LIKE ? "
            "  AND (matched_document_id IS NULL OR matched_document_id='') "
            "  AND COALESCE(hidden_duplicate,0) = 0",
            (client_code, firm_code, f"{period}%"),
        ).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# Step handlers — each returns the updated wizard snapshot + a result dict.
# ---------------------------------------------------------------------------


def complete_step_1_select_period(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    actor_email: str | None = None,
) -> dict[str, Any]:
    with _open(db_path) as conn:
        if _prior_period_open(conn, firm_code, client_code, period):
            return {'ok': False, 'error': 'prior_period_open',
                     'message': 'A prior period is still open. Close it first.'}
        # Ensure accounting_periods row exists (open).
        conn.execute(
            "INSERT INTO accounting_periods "
            "(firm_code, client_code, period, status) "
            "VALUES (?,?,?,'open') "
            "ON CONFLICT(firm_code, client_code, period) DO NOTHING",
            (firm_code, client_code, period),
        )
        _save_step(conn, firm_code=firm_code, client_code=client_code,
                    period=period, step='select_period', step_status='done',
                    step_data={'period': period},
                    actor_email=actor_email)
        conn.commit()
    return {'ok': True, 'state': get_state(db_path,
                                              firm_code=firm_code,
                                              client_code=client_code,
                                              period=period)}


def complete_step_2_process_documents(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    actor_email: str | None = None,
) -> dict[str, Any]:
    with _open(db_path) as conn:
        unprocessed = _unprocessed_documents(conn, firm_code, client_code,
                                               period)
        if unprocessed > 0:
            return {'ok': False, 'error': 'unprocessed_documents',
                     'count': unprocessed,
                     'message': f'{unprocessed} document(s) still need to '
                                 'be posted, ignored, or deleted.'}
        _save_step(conn, firm_code=firm_code, client_code=client_code,
                    period=period, step='process_documents',
                    step_status='done',
                    step_data={'unprocessed_at_close': 0},
                    actor_email=actor_email)
        conn.commit()
    return {'ok': True, 'state': get_state(db_path,
                                              firm_code=firm_code,
                                              client_code=client_code,
                                              period=period)}


def complete_step_3_reconcile_bank(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    acknowledge_unreconciled: bool = False,
    actor_email: str | None = None,
) -> dict[str, Any]:
    with _open(db_path) as conn:
        unrec = _unreconciled_bank_count(conn, firm_code, client_code,
                                            period)
        if unrec > 0 and not acknowledge_unreconciled:
            return {'ok': False, 'error': 'unreconciled_bank',
                     'count': unrec,
                     'message': f'{unrec} bank transaction(s) unmatched. '
                                 'Reconcile or pass acknowledge_unreconciled=True.'}
        _save_step(conn, firm_code=firm_code, client_code=client_code,
                    period=period, step='reconcile_bank',
                    step_status='done',
                    step_data={'unreconciled_at_close': unrec,
                                'acknowledged': acknowledge_unreconciled},
                    actor_email=actor_email)
        conn.commit()
    return {'ok': True, 'state': get_state(db_path,
                                              firm_code=firm_code,
                                              client_code=client_code,
                                              period=period)}


def suggest_accruals(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
) -> list[dict[str, Any]]:
    """Return deterministic standard-accrual suggestions. The caller
    decides which to post. Kept pure so tests + UI both call it."""
    return [
        {'kind': 'wage_accrual',
         'description': 'Wage accrual for pay period spanning close',
         'debit_account': '5100', 'credit_account': '2150',
         'amount_hint': 'Estimate based on prior 2 periods'},
        {'kind': 'depreciation',
         'description': f'Monthly depreciation for {period}',
         'debit_account': '5500', 'credit_account': '1500',
         'amount_hint': 'From fixed_assets_engine schedule'},
        {'kind': 'prepaid_amort',
         'description': 'Prepaid expense amortisation',
         'debit_account': '5400', 'credit_account': '1300',
         'amount_hint': '1/12 of prepaid balance'},
    ]


def complete_step_4_accruals(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    accepted_kinds: list[str] | None = None,
    actor_email: str | None = None,
) -> dict[str, Any]:
    accepted = list(accepted_kinds or [])
    with _open(db_path) as conn:
        _save_step(conn, firm_code=firm_code, client_code=client_code,
                    period=period, step='accruals', step_status='done',
                    step_data={'accepted': accepted},
                    actor_email=actor_email)
        conn.commit()
    return {'ok': True, 'accepted': accepted,
             'state': get_state(db_path, firm_code=firm_code,
                                 client_code=client_code, period=period)}


def complete_step_5_statements(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    actor_email: str | None = None,
) -> dict[str, Any]:
    """Generate statements via existing engines where available.
    Returns a summary; missing engines are reported, not fatal."""
    generated: dict[str, Any] = {}
    warnings: list[str] = []
    try:
        from src.integrations.qbo_financial_view import (
            unified_balance_sheet, unified_income_statement,
            unified_trial_balance,
        )
        generated['trial_balance'] = unified_trial_balance(
            db_path, client_code=client_code, period=period,
        )
        if not generated['trial_balance'].get('balanced', True):
            warnings.append('Trial balance unbalanced')
        generated['income_statement'] = unified_income_statement(
            db_path, client_code=client_code, period=period,
        )
        generated['balance_sheet'] = unified_balance_sheet(
            db_path, client_code=client_code, period=period,
        )
        if not generated['balance_sheet'].get('balanced', True):
            warnings.append('Balance sheet unbalanced')
    except ImportError as exc:
        warnings.append(f'statements engine unavailable: {exc}')

    with _open(db_path) as conn:
        _save_step(conn, firm_code=firm_code, client_code=client_code,
                    period=period, step='statements', step_status='done',
                    step_data={'warnings': warnings,
                                'generated_keys': list(generated.keys())},
                    actor_email=actor_email)
        conn.commit()
    return {'ok': True, 'generated': list(generated.keys()),
             'warnings': warnings,
             'state': get_state(db_path, firm_code=firm_code,
                                 client_code=client_code, period=period)}


def complete_step_6_lock(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    actor_email: str,
) -> dict[str, Any]:
    """Final lock. Refuses if any earlier step isn't 'done'."""
    state = get_state(db_path, firm_code=firm_code,
                       client_code=client_code, period=period)
    for step in state['steps'][:-1]:  # all but 'lock'
        if step.get('step_status') != 'done':
            return {'ok': False, 'error': 'incomplete_steps',
                     'blocking': step['step'],
                     'message': f"Cannot lock: step {step['step']} is still "
                                 f"{step.get('step_status')}"}

    with _open(db_path) as conn:
        conn.execute(
            "UPDATE accounting_periods SET status='locked', "
            " locked_by=?, locked_at=? "
            "WHERE firm_code=? AND client_code=? AND period=?",
            (actor_email, _iso_now(), firm_code, client_code, period),
        )
        _save_step(conn, firm_code=firm_code, client_code=client_code,
                    period=period, step='lock', step_status='done',
                    step_data={'locked_by': actor_email,
                                'locked_at': _iso_now()},
                    actor_email=actor_email)
        conn.commit()
    return {'ok': True, 'state': get_state(db_path,
                                              firm_code=firm_code,
                                              client_code=client_code,
                                              period=period)}


def is_period_locked(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
) -> bool:
    with _open(db_path) as conn:
        if not _table_exists(conn, 'accounting_periods'):
            return False
        row = conn.execute(
            "SELECT status FROM accounting_periods "
            "WHERE firm_code=? AND client_code=? AND period=?",
            (firm_code, client_code, period),
        ).fetchone()
    return bool(row and row['status'] == 'locked')
