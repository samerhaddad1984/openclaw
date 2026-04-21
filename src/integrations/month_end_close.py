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
    """Return flat-summary suggestions (legacy shape).

    Kept for backward compatibility with callers that want a single
    amount per kind; the wizard UI + JE-posting path now use the
    richer :func:`suggest_accruals_detailed` output which includes
    per-line breakdown the CPA can edit before posting."""
    detailed = suggest_accruals_detailed(
        db_path, firm_code=firm_code,
        client_code=client_code, period=period,
    )
    out: list[dict[str, Any]] = []
    for kind in ('wage_accrual', 'depreciation', 'prepaid_amort'):
        section = detailed.get(kind) or {}
        summary = section.get('summary') or {}
        lines = section.get('lines') or []
        hint = section.get('hint') or ''
        out.append({
            'kind': kind,
            'description': section.get('description') or '',
            'debit_account': section.get('default_debit_account') or '',
            'credit_account': section.get('default_credit_account') or '',
            'amount_cad': round(float(summary.get('total_amount_cad') or 0.0), 2),
            'source': section.get('source') or '',
            'detail_count': len(lines),
            'amount_hint': hint,
        })
    return out


def suggest_accruals_detailed(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
) -> dict[str, Any]:
    """Return the rich per-line accrual suggestions.

    Shape::

        {
          'period': 'YYYY-MM',
          'depreciation': {
              'summary': {'total_amount_cad': 4567.89, 'line_count': 12,
                            'currency': 'CAD'},
              'lines': [
                {'line_key': 'asset:42', 'asset_id': 42,
                 'asset_name': 'Ford F-150 2024', 'class': '10',
                 'ucc_start': 45000.00, 'rate': 0.30, 'proration': 1.0,
                 'amount_cad': 6750.00,
                 'account_debit': '5580', 'account_credit': '1250',
                 'editable': True, 'source': 'accrual_engine',
                 'reason': 'class 10 @ 30% / 12 on cost 45000'},
                ...
              ],
              'source': 'accrual_engine',
              'default_debit_account': '5580',
              'default_credit_account': '1250',
              'hint': 'From fixed_assets_engine schedule',
              'description': 'Monthly depreciation for <period>',
          },
          'wage_accrual': {...},
          'prepaid_amort': {...},
        }

    Each line carries a stable ``line_key`` so the wizard can correlate
    the CPA's overrides back to the right row on POST."""
    period_end = _safe_period_end_date(period)

    # --- Depreciation: one line per active fixed asset -------------------
    dep_lines: list[dict[str, Any]] = []
    try:
        from src.engines.accrual_engine import generate_period_accruals
        if period_end is not None:
            result = generate_period_accruals(
                client_code, period_end, firm_code=firm_code,
                db_path=Path(db_path), persist=False,
            )
            for d in result.get('accruals', []) or result.get('drafts', []) or []:
                if d.get('accrual_type') != 'depreciation':
                    continue
                entry_id = d.get('entry_id') or ''
                desc = d.get('description') or ''
                # engine description looks like
                # "Monthly depreciation — Ford F-150 (asset A-42, class 10, rate 30.0%)"
                asset_id = None
                # Prefer parsed form; fall back to the raw entry id.
                import re as _re
                m = _re.search(r'asset ([A-Za-z0-9_-]+)', desc)
                if m:
                    asset_id = m.group(1)
                dep_lines.append({
                    'line_key': f'dep:{asset_id or entry_id}',
                    'asset_id': asset_id,
                    'asset_name': _extract_asset_name(desc),
                    'description': desc,
                    'amount_cad': round(float(d.get('amount', 0.0) or 0.0), 2),
                    'account_debit': d.get('debit_account') or '5580',
                    'account_credit': d.get('credit_account') or '1890',
                    'editable': True,
                    'source': 'accrual_engine',
                    'reason': desc,
                })
    except Exception as exc:
        log.warning('depreciation engine unavailable: %s', exc)

    # --- Wages: one line per employee, amounts from recent pay history ---
    wage_lines = _per_employee_wage_lines(db_path, client_code, period)

    # --- Prepaid: one line per active prepaid balance --------------------
    prepaid_lines = _per_prepaid_lines(db_path, client_code)

    return {
        'period': period,
        'firm_code': firm_code,
        'client_code': client_code,
        'depreciation': {
            'summary': {
                'total_amount_cad': round(
                    sum(float(l['amount_cad']) for l in dep_lines), 2
                ),
                'line_count': len(dep_lines),
                'currency': 'CAD',
            },
            'lines': dep_lines,
            'source': 'accrual_engine',
            'default_debit_account': '5580',
            'default_credit_account': '1890',
            'description': f'Monthly depreciation for {period}',
            'hint': (
                'From fixed_assets_engine schedule'
                if dep_lines else 'No active fixed assets found'
            ),
        },
        'wage_accrual': {
            'summary': {
                'total_amount_cad': round(
                    sum(float(l['amount_cad']) for l in wage_lines), 2
                ),
                'line_count': len(wage_lines),
                'currency': 'CAD',
            },
            'lines': wage_lines,
            'source': 'payroll_entries_avg_2mo',
            'default_debit_account': '5100',
            'default_credit_account': '2150',
            'description': 'Wage accrual for pay period spanning close',
            'hint': (
                'Average of prior 2 months of payroll per employee'
                if wage_lines else 'No payroll history — enter manually'
            ),
        },
        'prepaid_amort': {
            'summary': {
                'total_amount_cad': round(
                    sum(float(l['amount_cad']) for l in prepaid_lines), 2
                ),
                'line_count': len(prepaid_lines),
                'currency': 'CAD',
            },
            'lines': prepaid_lines,
            'source': 'prepaid_expenses/12',
            'default_debit_account': '5400',
            'default_credit_account': '1300',
            'description': 'Prepaid expense amortisation',
            'hint': (
                '1/12 of each active prepaid balance'
                if prepaid_lines else 'No prepaid balance on file'
            ),
        },
    }


def _safe_period_end_date(period: str):
    """'YYYY-MM' → last-day-of-month date (or None on parse error)."""
    try:
        from datetime import date as _d, datetime as _dt, timedelta as _td
        dt = _dt.strptime(period + '-01', '%Y-%m-%d').date()
        if dt.month == 12:
            return _d(dt.year, 12, 31)
        return _d(dt.year, dt.month + 1, 1) - _td(days=1)
    except Exception:
        return None


def _extract_asset_name(desc: str) -> str:
    """Pull the asset display name out of the engine's description line."""
    import re as _re
    m = _re.search(r'depreciation\s+[—-]+\s+(.+?)\s+\(asset', desc)
    if m:
        return m.group(1).strip()
    return ''


def _per_employee_wage_lines(
    db_path: Path | str, client_code: str, period: str,
) -> list[dict[str, Any]]:
    """Build one wage-accrual line per employee based on the avg of the
    prior 2 months' gross_pay. Returns [] when payroll_entries is empty
    or absent so the UI renders 'no history'.

    Each line has a stable ``line_key`` of ``wage:<employee_id>``."""
    try:
        yy, mm = int(period[:4]), int(period[5:7])
    except ValueError:
        return []
    prev_months: list[str] = []
    for _ in range(2):
        mm -= 1
        if mm == 0:
            mm = 12
            yy -= 1
        prev_months.append(f'{yy:04d}-{mm:02d}')

    with _open(db_path) as conn:
        if not _table_exists(conn, 'payroll_entries'):
            return []
        try:
            placeholders = ','.join('?' * len(prev_months))
            like_clauses = ' OR '.join(
                "COALESCE(pay_period,'') LIKE ?"
                for _ in prev_months
            )
            args: list[Any] = [client_code]
            args += [f'{m}%' for m in prev_months]
            rows = conn.execute(
                f"SELECT employee_id, "
                f"       MAX(COALESCE(employee_name,'')) AS name, "
                f"       AVG(gross_pay) AS avg_gross, "
                f"       COUNT(*) AS n "
                f"FROM payroll_entries "
                f"WHERE client_code=? AND ({like_clauses}) "
                f"GROUP BY employee_id "
                f"ORDER BY employee_id",
                args,
            ).fetchall()
        except sqlite3.OperationalError:
            return []
    lines: list[dict[str, Any]] = []
    for r in rows:
        avg = float(r['avg_gross'] or 0.0)
        if avg <= 0:
            continue
        emp_id = r['employee_id']
        name = r['name'] or f'Employee {emp_id}'
        lines.append({
            'line_key': f'wage:{emp_id}',
            'employee_id': emp_id,
            'employee_name': name,
            'description': f'Wage accrual — {name}',
            'amount_cad': round(avg, 2),
            'account_debit': '5100',
            'account_credit': '2150',
            'editable': True,
            'source': 'avg_last_2_months_payroll',
            'reason': f'Avg of {r["n"]} payroll entries in {",".join(prev_months)}',
        })
    return lines


def _per_prepaid_lines(
    db_path: Path | str, client_code: str,
) -> list[dict[str, Any]]:
    """One amortisation line per active prepaid balance."""
    with _open(db_path) as conn:
        if not _table_exists(conn, 'prepaid_expenses'):
            return []
        try:
            rows = conn.execute(
                "SELECT id, COALESCE(description,'') AS description, "
                "       COALESCE(balance,0) AS balance, "
                "       COALESCE(debit_account,'') AS debit_account, "
                "       COALESCE(credit_account,'') AS credit_account "
                "FROM prepaid_expenses "
                "WHERE client_code=? AND COALESCE(status,'active')='active' "
                "ORDER BY id",
                (client_code,),
            ).fetchall()
        except sqlite3.OperationalError:
            return []
    lines: list[dict[str, Any]] = []
    for r in rows:
        bal = float(r['balance'] or 0.0)
        if bal <= 0:
            continue
        amort = round(bal / 12.0, 2)
        lines.append({
            'line_key': f'prepaid:{r["id"]}',
            'prepaid_id': r['id'],
            'description': r['description'] or 'Prepaid amortisation',
            'balance': round(bal, 2),
            'amount_cad': amort,
            'account_debit': r['debit_account'] or '5400',
            'account_credit': r['credit_account'] or '1300',
            'editable': True,
            'source': 'prepaid_balance/12',
            'reason': f'1/12 of balance {bal:.2f}',
        })
    return lines


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


def _ensure_idempotency_schema(db_path: Path | str) -> None:
    """Per-request cache for wizard Post clicks.

    Double-clicking the Post button in step 4 used to double-post
    wages/prepaid accruals (depreciation was idempotent via the
    accrual engine but wage + prepaid manual_journal_entries are
    not). The frontend now sends a random request_id generated at
    page-load; we look it up here and replay the cached result
    instead of re-executing."""
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wizard_posting_attempts (
                request_id TEXT PRIMARY KEY,
                firm_code TEXT,
                client_code TEXT,
                period_end TEXT,
                started_at TEXT,
                completed_at TEXT,
                result_json TEXT
            )
        """)
        conn.commit()


def idempotent_post_accruals_lines(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    line_decisions: list[dict[str, Any]],
    actor_email: str,
    request_id: str,
) -> dict[str, Any]:
    """Idempotency wrapper around post_suggested_accruals_lines.

    When the caller supplies a ``request_id``:
    - First request: claim the slot with ``started_at`` (INSERT OR
      IGNORE on PRIMARY KEY prevents the race); execute the post;
      cache the JSON-serialised result with ``completed_at``.
    - Duplicate request_id arriving while the first is still in
      flight: return a 'pending' stub.
    - Duplicate request_id after first completed: return the cached
      result (same posted/skipped/errors as the first call).

    Different request_ids always execute independently."""
    _ensure_idempotency_schema(db_path)
    import json as _json
    now_start = _iso_now()

    # Try to claim. INSERT OR IGNORE means the winner's row persists;
    # a second caller with the same request_id sees 0 rowcount.
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO wizard_posting_attempts "
            "(request_id, firm_code, client_code, period_end, started_at) "
            "VALUES (?,?,?,?,?)",
            (request_id, firm_code, client_code, period, now_start),
        )
        claimed = cur.rowcount > 0
        conn.commit()

    if not claimed:
        # Someone else already owns this request_id. Return the cached
        # result when they've finished; otherwise return 'pending'.
        with _open(db_path) as conn:
            row = conn.execute(
                "SELECT completed_at, result_json "
                "FROM wizard_posting_attempts WHERE request_id=?",
                (request_id,),
            ).fetchone()
        if row and row['completed_at'] and row['result_json']:
            cached = _json.loads(row['result_json'])
            cached['idempotent_replay'] = True
            return cached
        return {
            'ok': True, 'posted': [], 'skipped': [], 'errors': [],
            'idempotent_replay': True,
            'idempotent_in_flight': True,
        }

    # We own the slot; execute + cache result.
    result = post_suggested_accruals_lines(
        db_path, firm_code=firm_code, client_code=client_code,
        period=period, line_decisions=line_decisions,
        actor_email=actor_email,
    )
    result.setdefault('idempotent_replay', False)
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE wizard_posting_attempts "
            "SET completed_at=?, result_json=? WHERE request_id=?",
            (_iso_now(), _json.dumps(result, default=str), request_id),
        )
        conn.commit()
    return result


def _ensure_accrual_override_schema(db_path: Path | str) -> None:
    """Audit table for per-line CPA overrides at close time.

    Written idempotently from `post_suggested_accruals_lines`. Each row
    records the suggested amount, the override amount the CPA entered,
    the include/exclude flag, and the actor."""
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accrual_line_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                period TEXT NOT NULL,
                kind TEXT NOT NULL,
                line_key TEXT NOT NULL,
                suggested_amount REAL,
                final_amount REAL,
                included INTEGER NOT NULL,
                actor_email TEXT,
                notes TEXT,
                entry_id TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_accrual_overrides_scope "
            "ON accrual_line_overrides(firm_code, client_code, period)"
        )
        conn.commit()


def post_suggested_accruals(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    accepted_kinds: list[str], actor_email: str,
) -> dict[str, Any]:
    """Legacy shape: accept all suggested lines for the kinds listed.

    Internally calls :func:`post_suggested_accruals_lines` with every
    line in each accepted kind marked ``include=True`` and no override
    amounts, so the behaviour from before the line-level detail refactor
    is preserved."""
    detailed = suggest_accruals_detailed(
        db_path, firm_code=firm_code,
        client_code=client_code, period=period,
    )
    line_decisions: list[dict[str, Any]] = []
    for kind in accepted_kinds:
        section = detailed.get(kind) or {}
        for l in section.get('lines') or []:
            line_decisions.append({
                'kind': kind,
                'line_key': l['line_key'],
                'include': True,
                'amount': float(l['amount_cad']),
                'account_debit': l.get('account_debit'),
                'account_credit': l.get('account_credit'),
                'description': l.get('description'),
                'notes': None,
            })
    return post_suggested_accruals_lines(
        db_path, firm_code=firm_code, client_code=client_code,
        period=period, line_decisions=line_decisions,
        actor_email=actor_email,
    )


def post_suggested_accruals_lines(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str,
    line_decisions: list[dict[str, Any]],
    actor_email: str,
) -> dict[str, Any]:
    """Post accrual JEs from per-line CPA decisions.

    ``line_decisions`` is a list of::

        {'kind': 'depreciation'|'wage_accrual'|'prepaid_amort',
         'line_key': 'dep:A-42',
         'include': True|False,
         'amount': 6750.00,       # CAD, may differ from suggested
         'account_debit': '5580', # optional override
         'account_credit': '1890',
         'description': '...',    # optional override
         'notes': 'CPA: adjusted for half-year rule'  # audit string
        }

    Every decision lands in ``accrual_line_overrides`` whether or not
    it was included, so the audit trail is complete. Included lines
    with amount > 0 produce a draft ``manual_journal_entries`` row.

    Returns ``{ok, posted: [...], skipped: [...], errors: [...]}``."""
    _ensure_accrual_override_schema(db_path)
    detailed = suggest_accruals_detailed(
        db_path, firm_code=firm_code,
        client_code=client_code, period=period,
    )
    suggested_by_key: dict[str, dict[str, Any]] = {}
    for kind in ('depreciation', 'wage_accrual', 'prepaid_amort'):
        for l in (detailed.get(kind) or {}).get('lines') or []:
            suggested_by_key[l['line_key']] = {
                'kind': kind,
                'amount_cad': float(l['amount_cad']),
                'account_debit': l.get('account_debit'),
                'account_credit': l.get('account_credit'),
                'description': l.get('description'),
            }

    posted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    AUTO_REVERSE = {
        'depreciation': 0,
        'wage_accrual': 1,
        'prepaid_amort': 0,
    }

    for dec in line_decisions:
        key = dec.get('line_key') or ''
        suggested = suggested_by_key.get(key, {})
        kind = dec.get('kind') or suggested.get('kind') or ''
        suggested_amount = float(suggested.get('amount_cad') or 0.0)
        final_amount = float(dec.get('amount', suggested_amount) or 0.0)
        included = bool(dec.get('include', False))
        notes = dec.get('notes')

        entry_id = None
        if included and final_amount > 0 and kind in AUTO_REVERSE:
            debit = dec.get('account_debit') or suggested.get('account_debit') or ''
            credit = dec.get('account_credit') or suggested.get('account_credit') or ''
            description = (
                dec.get('description')
                or suggested.get('description')
                or f'{kind} accrual'
            )
            entry_id = _post_manual_je(
                db_path, client_code=client_code, period=period,
                debit_account=debit or '9999',
                credit_account=credit or '9999',
                amount=final_amount, description=description,
                prepared_by=actor_email,
                auto_reverse=AUTO_REVERSE[kind],
                accrual_type=kind,
            )
            if entry_id:
                posted.append({
                    'entry_id': entry_id,
                    'kind': kind,
                    'line_key': key,
                    'amount': final_amount,
                    'suggested': suggested_amount,
                    'override': abs(final_amount - suggested_amount) > 0.005,
                })
            else:
                errors.append({
                    'kind': kind, 'line_key': key,
                    'error': 'post_manual_je_failed',
                })
        else:
            skipped.append({
                'kind': kind, 'line_key': key,
                'reason': ('not_included' if not included
                            else 'zero_or_missing_amount'),
                'amount': final_amount,
            })

        # Audit every line's decision, whether or not it was posted.
        try:
            with _open(db_path) as conn:
                conn.execute(
                    "INSERT INTO accrual_line_overrides "
                    "(firm_code, client_code, period, kind, line_key, "
                    " suggested_amount, final_amount, included, actor_email, "
                    " notes, entry_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (firm_code, client_code, period, kind, key,
                     suggested_amount, final_amount,
                     1 if included else 0, actor_email, notes, entry_id),
                )
                conn.commit()
        except sqlite3.OperationalError as exc:
            errors.append({'kind': kind, 'line_key': key,
                            'error': f'audit_write_failed: {exc}'})

    return {
        'ok': not errors or bool(posted),
        'posted': posted,
        'skipped': skipped,
        'errors': errors,
        'kinds': sorted({p['kind'] for p in posted}),
    }


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
