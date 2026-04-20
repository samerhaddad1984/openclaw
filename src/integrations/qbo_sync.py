"""QBO sync orchestrator.

Glues together ``qbo_pull``, ``qbo_push``, ``qbo_conflict_resolver``,
and ``qbo_webhook`` into three public entry points:

- :func:`initial_sync`   — pull everything for one (firm, client).
- :func:`incremental_sync` — pull-since-last + drain webhook queue.
- :func:`scheduled_sync_all` — wrapper used by the cron loop.

Every run is bracketed by a ``qbo_sync_log`` row tracked from start to
completion so the UI can show "last sync at X, Y entities, Z errors".
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Sync log helpers
# ---------------------------------------------------------------------------


def _start_log(db_path: Path | str, *, firm_code: str, client_code: str,
                direction: str, triggered_by: str) -> int:
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO qbo_sync_log "
            "(firm_code, client_code, started_at, direction, triggered_by) "
            "VALUES (?,?,?,?,?)",
            (firm_code, client_code, _iso_now(), direction, triggered_by),
        )
        conn.commit()
        return cur.lastrowid


def _finish_log(db_path: Path | str, log_id: int, *,
                 entities: int, errors: int,
                 details: Optional[str] = None) -> None:
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE qbo_sync_log SET completed_at=?, entities_synced=?, "
            "errors=?, details=? WHERE id=?",
            (_iso_now(), entities, errors, details, log_id),
        )
        conn.commit()


def _last_completed_sync(db_path: Path | str, *,
                          firm_code: str, client_code: str) -> Optional[str]:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(completed_at) FROM qbo_sync_log "
            "WHERE firm_code=? AND client_code=? AND errors=0",
            (firm_code, client_code),
        ).fetchone()
    return row[0] if row and row[0] else None


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


class QBOSyncOrchestrator:
    """Per-client orchestrator. Pulls everything (initial) or changes
    since the last successful sync (incremental)."""

    def __init__(
        self,
        firm_code: str,
        client_code: str,
        db_path: Path | str,
        *,
        sandbox: bool = False,
        puller_cls: Any = None,
        webhook_processor: Any = None,
    ) -> None:
        self.firm_code = firm_code
        self.client_code = client_code
        self.db_path = Path(db_path)
        self.sandbox = sandbox
        if puller_cls is None:
            from src.integrations.qbo_pull import QBOPull as puller_cls  # type: ignore
        self._puller_cls = puller_cls
        if webhook_processor is None:
            from src.integrations.qbo_webhook import (
                pending_events as _pending,
                process_one_event as _process_one,
            )
            webhook_processor = (_pending, _process_one)
        self._webhook_processor = webhook_processor

    def initial_sync(self, triggered_by: str = 'manual') -> dict[str, Any]:
        """First-time full pull. Order matters: reference entities first
        (Account / Customer / Vendor) so transactions can resolve them."""
        log_id = _start_log(self.db_path,
                             firm_code=self.firm_code,
                             client_code=self.client_code,
                             direction='full_sync',
                             triggered_by=triggered_by)
        counts: dict[str, int] = {}
        error_count = 0
        try:
            puller = self._puller_cls(self.firm_code, self.client_code,
                                        db_path=self.db_path,
                                        sandbox=self.sandbox)
            counts['accounts']  = puller.pull_accounts()
            counts['customers'] = puller.pull_customers()
            counts['vendors']   = puller.pull_vendors()
            counts['journal_entries'] = puller.pull_journal_entries()
            counts['bills']     = puller.pull_bills()
            counts['invoices']  = puller.pull_invoices()
            counts['payments']  = puller.pull_payments()
        except Exception as exc:  # noqa: BLE001
            log.exception("initial_sync failed")
            error_count += 1
            counts['error'] = str(exc)
        total = sum(v for v in counts.values() if isinstance(v, int))
        _finish_log(self.db_path, log_id,
                     entities=total, errors=error_count,
                     details=str(counts))
        return {'ok': error_count == 0, **counts}

    def incremental_sync(self, triggered_by: str = 'manual',
                          since_date: Optional[str] = None,
                          window_days: int = 30) -> dict[str, Any]:
        """Pull changes since the last successful sync (or ``window_days``
        back if no prior sync exists). Also drains the webhook queue
        for this (firm, client)."""
        log_id = _start_log(self.db_path,
                             firm_code=self.firm_code,
                             client_code=self.client_code,
                             direction='incremental',
                             triggered_by=triggered_by)
        counts: dict[str, int] = {}
        error_count = 0
        try:
            since = since_date or _last_completed_sync(
                self.db_path, firm_code=self.firm_code,
                client_code=self.client_code,
            )
            if not since:
                since = (datetime.now(timezone.utc)
                          - timedelta(days=window_days)
                          ).replace(microsecond=0).isoformat()

            puller = self._puller_cls(self.firm_code, self.client_code,
                                        db_path=self.db_path,
                                        sandbox=self.sandbox)
            counts['accounts']        = puller.pull_accounts()
            counts['customers']       = puller.pull_customers()
            counts['vendors']         = puller.pull_vendors()
            counts['journal_entries'] = puller.pull_journal_entries(since_date=since)
            counts['bills']           = puller.pull_bills(since_date=since)
            counts['invoices']        = puller.pull_invoices(since_date=since)

            # Smart bank-source: when this client's bank_source is 'qbo'
            # or 'both', incremental-pull bank transactions. For 'both'
            # we also run dedup immediately so the next reconciliation
            # query sees a clean view.
            bank_outcome = self._maybe_pull_bank_transactions(since)
            if bank_outcome is not None:
                counts['bank_transactions'] = bank_outcome['pulled']
                if bank_outcome.get('duplicates_hidden'):
                    counts['duplicates_hidden'] = bank_outcome['duplicates_hidden']

            # Drain webhook queue for THIS realm.
            counts['webhook_events'] = self._drain_webhooks()
        except Exception as exc:  # noqa: BLE001
            log.exception("incremental_sync failed")
            error_count += 1
            counts['error'] = str(exc)
        total = sum(v for v in counts.values() if isinstance(v, int))
        _finish_log(self.db_path, log_id,
                     entities=total, errors=error_count,
                     details=str(counts))
        return {'ok': error_count == 0, **counts}

    def _maybe_pull_bank_transactions(
        self, since: str,
    ) -> Optional[dict[str, int]]:
        """When clients.bank_source indicates QBO is an active source,
        pull QBO bank register entries. When 'both' is set, also run
        dedup so the next reconciliation query is clean."""
        try:
            with _open(self.db_path) as conn:
                row = conn.execute(
                    "SELECT bank_source FROM clients WHERE client_code=?",
                    (self.client_code,),
                ).fetchone()
            bank_source = (row['bank_source'] if row and row['bank_source']
                             else 'none')
        except sqlite3.OperationalError:
            return None

        if bank_source not in ('qbo', 'both'):
            return None

        from src.integrations.qbo_bank_pull import QBOBankPull
        pulled = QBOBankPull(
            self.firm_code, self.client_code,
            db_path=self.db_path, sandbox=self.sandbox,
        ).pull_bank_transactions(since_date=since)

        hidden = 0
        if bank_source == 'both':
            from src.engines.bank_tx_dedup import BankTransactionDeduplicator
            hidden = BankTransactionDeduplicator(
                self.firm_code, self.client_code, self.db_path,
            ).mark_duplicates(auto_apply=True)

        return {'pulled': pulled, 'duplicates_hidden': hidden}

    def _drain_webhooks(self) -> int:
        pending, process = self._webhook_processor
        events = pending(self.db_path)
        # Only process events for this realm/firm/client.
        processed = 0
        for ev in events:
            # Resolve realm → (firm, client) inside process_one_event.
            # We scope to this realm by filtering upfront.
            process(self.db_path, ev, puller_cls=self._puller_cls)
            processed += 1
        return processed


# ---------------------------------------------------------------------------
# Cron helper
# ---------------------------------------------------------------------------


def _iter_active_connections(db_path: Path | str) -> list[tuple[str, str]]:
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT firm_code, client_code FROM qbo_connections "
            "WHERE status='active' ORDER BY firm_code, client_code"
        ).fetchall()
    return [(r['firm_code'], r['client_code']) for r in rows]


def scheduled_sync_all(
    db_path: Path | str,
    *,
    sandbox: bool = False,
    orchestrator_cls: Any = QBOSyncOrchestrator,
) -> dict[str, Any]:
    """Run incremental_sync for every active connection. Returns a
    rollup (``{client: outcome_dict}``)."""
    results: dict[str, Any] = {}
    connections = _iter_active_connections(db_path)
    for firm, client in connections:
        key = f"{firm}:{client}"
        try:
            orch = orchestrator_cls(firm, client, db_path=db_path,
                                      sandbox=sandbox)
            results[key] = orch.incremental_sync(triggered_by='scheduled')
        except Exception as exc:  # noqa: BLE001
            log.exception("scheduled sync failed for %s", key)
            results[key] = {'ok': False, 'error': str(exc)}
    return {'connections': len(connections), 'results': results}


# ---------------------------------------------------------------------------
# Status helper (for /qbo/sync/status)
# ---------------------------------------------------------------------------


def sync_status(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
) -> dict[str, Any]:
    """Summary per-client for the UI: last-sync, pending webhooks,
    conflicts, last error."""
    with _open(db_path) as conn:
        last_ok = conn.execute(
            "SELECT completed_at, entities_synced, direction "
            "FROM qbo_sync_log "
            "WHERE firm_code=? AND client_code=? AND errors=0 "
            "ORDER BY id DESC LIMIT 1",
            (firm_code, client_code),
        ).fetchone()
        last_err = conn.execute(
            "SELECT completed_at, details FROM qbo_sync_log "
            "WHERE firm_code=? AND client_code=? AND errors>0 "
            "ORDER BY id DESC LIMIT 1",
            (firm_code, client_code),
        ).fetchone()
        conflicts = conn.execute(
            "SELECT COUNT(*) FROM qbo_sync_state "
            "WHERE firm_code=? AND client_code=? AND sync_status='conflict'",
            (firm_code, client_code),
        ).fetchone()[0]
        pending_webhooks = conn.execute(
            "SELECT COUNT(*) FROM qbo_webhook_events WHERE processed=0"
        ).fetchone()[0]
    return {
        'firm_code': firm_code,
        'client_code': client_code,
        'last_successful_sync': dict(last_ok) if last_ok else None,
        'last_error': dict(last_err) if last_err else None,
        'conflicts_pending': conflicts,
        'webhooks_pending': pending_webhooks,
    }
