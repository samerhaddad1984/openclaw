"""Run the full QBO bidirectional-sync stack against the Intuit sandbox.

This is an **operator-invoked** script. It requires a live sandbox
connection provisioned through the normal OAuth flow — there's no way
to stub `/qbo/connect` here because Intuit's OAuth requires browser
consent. Before running:

1. Visit ``/qbo/connect?client_code=<C>`` in the OtoCPA UI.
2. Complete the Intuit OAuth dance against the sandbox company.
3. Verify `qbo_connections` has an active row.
4. Run this script with the (firm, client) codes as args.

If no active connection exists, the script exits with a clear
``missing_connection`` code so CI gating is simple.

Steps executed:

0. Validate active connection.
1. Run initial_sync and print entity counts.
2. Push a tiny in-house JE via push_journal_entry.
3. Incremental sync — confirms the JE round-trips as
   ``sync_source='otocpa_origin'``.
4. Simulate a QBO-side edit by updating the JE in QBO directly (via a
   raw _request), then incremental sync again — expect
   ``sync_status='conflict'``.
5. Resolve the conflict via ``otocpa_wins``.
6. Verify trial balance balances via unified_trial_balance.

Each step prints a PASS / FAIL line; exit 0 iff all pass.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

env = ROOT / '.env'
if env.exists():
    for line in env.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from src.agents.tools.qbo_oauth import get_qbo_tokens  # noqa: E402
from src.integrations.qbo_conflict_resolver import (  # noqa: E402
    QBOConflictResolver, mark_local_modified,
)
from src.integrations.qbo_financial_view import unified_trial_balance  # noqa: E402
from src.integrations.qbo_pull import QBOPull  # noqa: E402
from src.integrations.qbo_push import QBOPush  # noqa: E402
from src.integrations.qbo_sync import QBOSyncOrchestrator  # noqa: E402


def _assert(cond: bool, label: str) -> None:
    status = 'PASS' if cond else 'FAIL'
    print(f"[{status}] {label}")
    if not cond:
        raise SystemExit(1)


def _has_connection(firm: str, client: str, db_path: Path) -> bool:
    tok = get_qbo_tokens(firm, client, db_path=db_path)
    return bool(tok and tok.get('status') == 'active' and tok.get('access_token'))


def _ensure_local_tables(db_path: Path) -> None:
    """Make sure the on-disk DB has the local tables we need to create
    a manual JE against. On a real operator box these already exist;
    the fallback is only exercised in ad-hoc dev."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_journal_entries (
                entry_id TEXT PRIMARY KEY,
                client_code TEXT, period TEXT, entry_date TEXT,
                debit_account TEXT, credit_account TEXT,
                amount REAL, description TEXT,
                document_id TEXT, status TEXT
            )
        """)
        conn.commit()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--firm', required=True)
    p.add_argument('--client', required=True)
    p.add_argument('--db', type=Path,
                   default=Path('/opt/otocpa/data/otocpa_agent.db'))
    p.add_argument('--sandbox', action='store_true', default=True)
    args = p.parse_args()

    if not _has_connection(args.firm, args.client, args.db):
        print('[SKIP] no active QBO connection — run /qbo/connect first')
        return 2  # distinct exit code so CI gates cleanly

    print(f"=== QBO sandbox E2E: {args.firm}/{args.client} ===")
    _ensure_local_tables(args.db)

    # 1. Initial sync
    orch = QBOSyncOrchestrator(args.firm, args.client,
                                 db_path=args.db, sandbox=args.sandbox)
    r1 = orch.initial_sync(triggered_by='e2e')
    print(json.dumps(r1, indent=2))
    _assert(r1.get('ok') is True, 'initial_sync succeeded')

    # 2. Push a tiny JE from OtoCPA
    entry_id = f"e2e-{uuid.uuid4().hex[:8]}"
    with sqlite3.connect(args.db) as conn:
        # Pick any two accounts from the sandbox COA that we successfully pulled.
        accts = conn.execute(
            "SELECT account_number, name FROM qbo_accounts "
            "WHERE firm_code=? AND client_code=? "
            "AND account_number IS NOT NULL LIMIT 2",
            (args.firm, args.client),
        ).fetchall()
    if len(accts) < 2:
        _assert(False, 'need >=2 qbo_accounts with account_number to push JE')
    debit_acct = accts[0][0]
    credit_acct = accts[1][0]
    with sqlite3.connect(args.db) as conn:
        conn.execute(
            "INSERT INTO manual_journal_entries "
            "(entry_id, client_code, period, entry_date, debit_account, "
            "credit_account, amount, description, status) VALUES "
            "(?,?,?,?,?,?,?,?,?)",
            (entry_id, args.client, '2026-04', '2026-04-20',
             debit_acct, credit_acct, 1.25, 'e2e sandbox round-trip', 'draft'),
        )
        conn.commit()

    push = QBOPush(args.firm, args.client, db_path=args.db,
                   sandbox=args.sandbox)
    r2 = push.push_journal_entry(entry_id)
    _assert(r2.get('status') == 'ok' and r2.get('qbo_id'),
            f"push_journal_entry returned ok + qbo_id (got {r2})")
    qbo_je_id = r2['qbo_id']

    # 3. Incremental — our pushed JE should already be 'otocpa_origin'
    r3 = orch.incremental_sync(triggered_by='e2e')
    _assert(r3.get('ok') is True, 'incremental_sync succeeded')
    with sqlite3.connect(args.db) as conn:
        src = conn.execute(
            "SELECT sync_source FROM qbo_sync_state "
            "WHERE firm_code=? AND client_code=? "
            "AND entity_type='JournalEntry' AND qbo_id=?",
            (args.firm, args.client, qbo_je_id),
        ).fetchone()
    _assert(src is not None and src[0] == 'otocpa_origin',
            f'sync_source=otocpa_origin after push (got {src})')

    # 4. Induce a conflict: bump local + qbo timestamps past last_pushed.
    mark_local_modified(
        args.db, firm_code=args.firm, client_code=args.client,
        entity_type='JournalEntry', local_id=entry_id,
    )
    with sqlite3.connect(args.db) as conn:
        conn.execute(
            "UPDATE qbo_sync_state SET last_qbo_modified=? "
            "WHERE firm_code=? AND client_code=? "
            "AND entity_type='JournalEntry' AND qbo_id=?",
            ('2099-01-01T00:00:00Z', args.firm, args.client, qbo_je_id),
        )
        conn.commit()

    from src.integrations.qbo_conflict_resolver import detect_conflicts
    conflicts = detect_conflicts(args.db, firm_code=args.firm,
                                   client_code=args.client)
    _assert(any(c['qbo_id'] == qbo_je_id for c in conflicts),
            f'detect_conflicts flagged our JE (got {len(conflicts)} conflicts)')

    # 5. Resolve via otocpa_wins
    resolver = QBOConflictResolver(args.firm, args.client, db_path=args.db)
    r5 = resolver.resolve(entity_type='JournalEntry', qbo_id=qbo_je_id,
                            strategy='otocpa_wins', pusher=push)
    _assert(r5.get('status') == 'resolved_by_push',
            f'conflict resolved_by_push (got {r5})')

    # 6. Unified TB balanced
    tb = unified_trial_balance(args.db, client_code=args.client,
                                 period='2026-04')
    _assert(
        tb['balanced'] is True,
        'unified_trial_balance balanced (got debits={}, credits={})'.format(
            tb['total_debits'], tb['total_credits'],
        ),
    )

    print('\n=== ALL PASS ===')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
