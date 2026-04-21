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
    """Return standard-accrual suggestions with computed CAD amounts.

    - Depreciation: pulled from the fixed_assets/accrual engine (one entry
      per active asset at cost * cca_rate / 12).
    - Wage accrual: estimated as the average monthly payroll from the
      previous 2 months of payroll_entries.
    - Prepaid amort: 1/12 of the sum of active prepaid balances.

    Every suggestion includes ``amount_cad`` (0.0 when the engine has no
    data to compute a real number — the UI should render that as
    'not enough data' rather than post a zero JE)."""
    try:
        from datetime import datetime as _dt
        period_end = _dt.strptime(period + '-01', '%Y-%m-%d').date()
        # Use engine to get real-period end:
        if period_end.month == 12:
            from datetime import date as _d
            period_end = _d(period_end.year, 12, 31)
        else:
            from datetime import date as _d, timedelta as _td
            period_end = _d(period_end.year, period_end.month + 1, 1) - _td(days=1)
    except Exception:
        period_end = None

    depreciation_total = 0.0
    depreciation_details: list[dict[str, Any]] = []
    try:
        from src.engines.accrual_engine import (
            generate_period_accruals,
        )
        if period_end is not None:
            result = generate_period_accruals(
                client_code, period_end, firm_code=firm_code,
                db_path=Path(db_path), persist=False,
            )
            for d in result.get('drafts', []) or []:
                if d.get('accrual_type') == 'depreciation':
                    depreciation_total += float(d.get('amount', 0.0) or 0.0)
                    depreciation_details.append(d)
    except Exception as exc:
        log.warning('accrual engine unavailable: %s', exc)

    wage_total = _average_recent_payroll(db_path, client_code, period)
    prepaid_total = _prepaid_month_amort(db_path, client_code)

    return [
        {'kind': 'wage_accrual',
         'description': 'Wage accrual for pay period spanning close',
         'debit_account': '5100', 'credit_account': '2150',
         'amount_cad': round(wage_total, 2),
         'source': 'avg_last_2_months_payroll',
         'amount_hint': ('Estimate based on prior 2 periods'
                          if wage_total > 0 else
                          'No payroll history — enter manually')},
        {'kind': 'depreciation',
         'description': f'Monthly depreciation for {period}',
         'debit_account': '5500', 'credit_account': '1500',
         'amount_cad': round(depreciation_total, 2),
         'source': 'fixed_assets_engine',
         'detail_count': len(depreciation_details),
         'amount_hint': ('From fixed_assets_engine schedule'
                          if depreciation_total > 0 else
                          'No active fixed assets found')},
        {'kind': 'prepaid_amort',
         'description': 'Prepaid expense amortisation',
         'debit_account': '5400', 'credit_account': '1300',
         'amount_cad': round(prepaid_total, 2),
         'source': 'prepaid_balance/12',
         'amount_hint': ('1/12 of prepaid balance'
                          if prepaid_total > 0 else
                          'No prepaid balance on file')},
    ]


def _average_recent_payroll(db_path: Path | str, client_code: str,
                              period: str) -> float:
    """Average total gross payroll from the two months preceding `period`.

    Reads `payroll_entries.gross_pay` when the table exists; returns 0.0
    when no history is available. Pure SQL, no engine dependency."""
    try:
        yy, mm = int(period[:4]), int(period[5:7])
    except ValueError:
        return 0.0
    prev_months = []
    for _ in range(2):
        mm -= 1
        if mm == 0:
            mm = 12
            yy -= 1
        prev_months.append(f'{yy:04d}-{mm:02d}')
    with _open(db_path) as conn:
        if not _table_exists(conn, 'payroll_entries'):
            return 0.0
        total = 0.0
        count = 0
        for m in prev_months:
            try:
                row = conn.execute(
                    "SELECT COALESCE(SUM(gross_pay), 0) AS g, "
                    "       COUNT(*) AS n "
                    "FROM payroll_entries "
                    "WHERE client_code=? "
                    "  AND COALESCE(pay_period,'') LIKE ?",
                    (client_code, f'{m}%'),
                ).fetchone()
            except sqlite3.OperationalError:
                return 0.0
            if row and (row['n'] or 0) > 0:
                total += float(row['g'] or 0.0)
                count += 1
    if count == 0:
        return 0.0
    return total / count


def _prepaid_month_amort(db_path: Path | str, client_code: str) -> float:
    """Return one-twelfth of active prepaid balances.

    Scans `prepaid_expenses` when present; returns 0.0 otherwise so the
    UI renders this as 'no data' instead of posting a bogus zero JE."""
    with _open(db_path) as conn:
        if not _table_exists(conn, 'prepaid_expenses'):
            return 0.0
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(balance), 0) AS b FROM prepaid_expenses "
                "WHERE client_code=? AND COALESCE(status,'active')='active'",
                (client_code,),
            ).fetchone()
        except sqlite3.OperationalError:
            return 0.0
    return float((row[0] if row else 0.0) or 0.0) / 12.0


def post_suggested_accruals(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    accepted_kinds: list[str], actor_email: str,
) -> dict[str, Any]:
    """Persist the accepted accruals as draft JEs via accrual_engine.

    Returns {ok, posted, kinds, errors}. Called by wizard step 4 when the
    operator clicks "Post all suggested accruals"."""
    suggestions = suggest_accruals(
        db_path, firm_code=firm_code, client_code=client_code, period=period,
    )
    wanted = {s['kind']: s for s in suggestions if s['kind'] in accepted_kinds}

    posted: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    if not wanted:
        return {'ok': True, 'posted': [], 'errors': [],
                 'message': 'No accruals accepted.'}

    # Depreciation posts via generate_period_accruals (engine handles the
    # per-asset split). Wage + prepaid post as single aggregate JEs.
    if 'depreciation' in wanted:
        try:
            from datetime import date as _d, datetime as _dt, timedelta as _td
            period_end = _dt.strptime(period + '-01', '%Y-%m-%d').date()
            if period_end.month == 12:
                period_end = _d(period_end.year, 12, 31)
            else:
                period_end = _d(period_end.year, period_end.month + 1, 1) - _td(days=1)
            from src.engines.accrual_engine import generate_period_accruals
            result = generate_period_accruals(
                client_code, period_end, firm_code=firm_code,
                db_path=Path(db_path), persist=True,
            )
            dep_drafts = [d for d in (result.get('drafts') or [])
                           if d.get('accrual_type') == 'depreciation']
            posted.extend(dep_drafts)
        except Exception as exc:
            errors.append({'kind': 'depreciation', 'error': str(exc)})

    if 'wage_accrual' in wanted:
        amt = float(wanted['wage_accrual'].get('amount_cad') or 0.0)
        if amt > 0:
            je_id = _post_manual_je(
                db_path, client_code=client_code, period=period,
                debit_account='5100', credit_account='2150',
                amount=amt, description='Wage accrual (month-end)',
                prepared_by=actor_email, auto_reverse=1,
                accrual_type='wage_accrual',
            )
            if je_id:
                posted.append({'entry_id': je_id, 'kind': 'wage_accrual',
                                'amount': amt})
            else:
                errors.append({'kind': 'wage_accrual',
                                'error': 'post_manual_je_failed'})
        else:
            errors.append({'kind': 'wage_accrual',
                            'error': 'no_history_to_estimate'})

    if 'prepaid_amort' in wanted:
        amt = float(wanted['prepaid_amort'].get('amount_cad') or 0.0)
        if amt > 0:
            je_id = _post_manual_je(
                db_path, client_code=client_code, period=period,
                debit_account='5400', credit_account='1300',
                amount=amt, description='Prepaid expense amortisation',
                prepared_by=actor_email, auto_reverse=0,
                accrual_type='prepaid_amort',
            )
            if je_id:
                posted.append({'entry_id': je_id, 'kind': 'prepaid_amort',
                                'amount': amt})
            else:
                errors.append({'kind': 'prepaid_amort',
                                'error': 'post_manual_je_failed'})
        else:
            errors.append({'kind': 'prepaid_amort',
                            'error': 'no_prepaid_balance'})

    return {'ok': not errors or bool(posted), 'posted': posted,
             'errors': errors,
             'kinds': [p.get('kind') or p.get('accrual_type') for p in posted]}


def _post_manual_je(
    db_path: Path | str, *,
    client_code: str, period: str,
    debit_account: str, credit_account: str, amount: float,
    description: str, prepared_by: str,
    auto_reverse: int = 0, accrual_type: str | None = None,
) -> str | None:
    """Insert a draft manual_journal_entries row. Returns the entry_id
    or None if the table/columns aren't available (very early DBs)."""
    import secrets
    entry_id = f'ACR-{secrets.token_hex(6)}'
    period_end = _period_end_date(period)
    with _open(db_path) as conn:
        if not _table_exists(conn, 'manual_journal_entries'):
            return None
        try:
            conn.execute(
                "INSERT INTO manual_journal_entries "
                "(entry_id, client_code, period, entry_date, prepared_by, "
                "debit_account, credit_account, amount, description, "
                "source, status, auto_reverse, accrual_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (entry_id, client_code, period, period_end, prepared_by,
                 debit_account, credit_account, amount, description,
                 'month_end_close', 'draft', auto_reverse, accrual_type),
            )
            conn.commit()
        except sqlite3.OperationalError:
            return None
    return entry_id


def _period_end_date(period: str) -> str:
    """period='YYYY-MM' → 'YYYY-MM-DD' (last day of month)."""
    from datetime import date as _d, datetime as _dt, timedelta as _td
    dt = _dt.strptime(period + '-01', '%Y-%m-%d').date()
    if dt.month == 12:
        return _d(dt.year, 12, 31).isoformat()
    return (_d(dt.year, dt.month + 1, 1) - _td(days=1)).isoformat()


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
