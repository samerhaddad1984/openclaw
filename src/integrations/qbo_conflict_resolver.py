"""QBO conflict detection + resolution.

A conflict exists when both sides (local and QBO) have modified the
same entity since our last successful sync. Two input signals decide:

- ``last_local_modified`` in ``qbo_sync_state`` vs ``last_pushed_at``
  (has the local side changed since we last pushed?).
- ``last_qbo_modified`` from the most recent pull vs ``last_pushed_at``
  (has QBO changed since our last push?).

When both are true, we mark ``sync_status = 'conflict'`` and populate
``conflict_details`` with a JSON blob the UI can render side-by-side.

Four resolution strategies are supported:

- ``otocpa_wins``      — push local state over QBO's.
- ``qbo_wins``         — pull QBO state, overwrite local, mark synced.
- ``flag_for_review``  — keep status='conflict'; UI lists them until a
                         CPA picks a winner.
- ``merge``            — field-level union where the two edits touched
                         disjoint fields. Raises if the same field was
                         edited on both sides (fall back to review).
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
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
# Detection
# ---------------------------------------------------------------------------


def mark_local_modified(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
    entity_type: str,
    local_id: str,
) -> None:
    """Called by the dashboard every time a local entity that could be
    synced to QBO is edited. Bumps ``last_local_modified`` on the
    corresponding ``qbo_sync_state`` row (if one exists)."""
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE qbo_sync_state SET last_local_modified=? "
            "WHERE firm_code=? AND client_code=? "
            "AND entity_type=? AND local_id=?",
            (_iso_now(), firm_code, client_code, entity_type, local_id),
        )
        conn.commit()


def detect_conflicts(
    db_path: Path | str,
    *,
    firm_code: str,
    client_code: str,
) -> list[dict[str, Any]]:
    """Scan ``qbo_sync_state`` and promote eligible rows to
    ``sync_status='conflict'``. Returns the set of conflicts (including
    pre-existing ``status='conflict'`` rows)."""
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM qbo_sync_state "
            "WHERE firm_code=? AND client_code=?",
            (firm_code, client_code),
        ).fetchall()

        conflicts: list[dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            pushed = d.get('last_pushed_at') or ''
            qbo_mod = d.get('last_qbo_modified') or ''
            local_mod = d.get('last_local_modified') or ''
            # Both sides changed since last push? -> conflict
            local_changed = local_mod and pushed and local_mod > pushed
            qbo_changed = qbo_mod and pushed and qbo_mod > pushed
            if d.get('sync_status') == 'conflict' or (local_changed and qbo_changed):
                conflicts.append(d)
                if d.get('sync_status') != 'conflict':
                    conn.execute(
                        "UPDATE qbo_sync_state SET sync_status='conflict' "
                        "WHERE id=?",
                        (d['id'],),
                    )
        conn.commit()
    return conflicts


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


class QBOConflictResolver:
    def __init__(
        self,
        firm_code: str,
        client_code: str,
        db_path: Path | str,
    ) -> None:
        self.firm_code = firm_code
        self.client_code = client_code
        self.db_path = Path(db_path)

    # --- public API ---

    def list_conflicts(self) -> list[dict[str, Any]]:
        with _open(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? AND sync_status='conflict' "
                "ORDER BY id",
                (self.firm_code, self.client_code),
            ).fetchall()
        return [dict(r) for r in rows]

    def resolve(self, *, entity_type: str, qbo_id: str,
                 strategy: str = 'flag_for_review',
                 pusher: Any = None, puller: Any = None) -> dict[str, Any]:
        """Resolve a single conflict. ``pusher`` + ``puller`` are
        optional dependency-injected instances (for tests + unusual
        edge cases). They default to lazily-constructed QBOPush/QBOPull
        bound to the same (firm, client, db_path)."""
        if strategy not in ('otocpa_wins', 'qbo_wins', 'flag_for_review', 'merge'):
            raise ValueError(f"Unknown strategy: {strategy}")

        with _open(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type=? AND qbo_id=?",
                (self.firm_code, self.client_code, entity_type, qbo_id),
            ).fetchone()
        if not row:
            raise LookupError(
                f"No sync_state row for {entity_type}/{qbo_id}"
            )
        row = dict(row)

        if strategy == 'flag_for_review':
            self._mark_review(row)
            return {'status': 'pending_review',
                    'entity_type': entity_type, 'qbo_id': qbo_id}

        if strategy == 'otocpa_wins':
            return self._push_local(row, pusher=pusher)

        if strategy == 'qbo_wins':
            return self._pull_remote(row, puller=puller)

        if strategy == 'merge':
            return self._merge(row, pusher=pusher, puller=puller)

        raise AssertionError('unreachable')

    # --- internal strategies ---

    def _mark_review(self, row: dict[str, Any]) -> None:
        with _open(self.db_path) as conn:
            conn.execute(
                "UPDATE qbo_sync_state SET sync_status='conflict', "
                "conflict_details=? WHERE id=?",
                (json.dumps({
                    'flagged_at': _iso_now(),
                    'entity_type': row.get('entity_type'),
                    'qbo_id': row.get('qbo_id'),
                    'local_id': row.get('local_id'),
                }), row['id']),
            )
            conn.commit()

    def _push_local(self, row: dict[str, Any], *, pusher: Any) -> dict[str, Any]:
        entity_type = row['entity_type']
        local_id = row['local_id']
        if not local_id:
            raise RuntimeError(
                f"otocpa_wins requires local_id; none for {entity_type}/{row['qbo_id']}"
            )
        if pusher is None:
            from src.integrations.qbo_push import QBOPush
            pusher = QBOPush(self.firm_code, self.client_code,
                              db_path=self.db_path, sandbox=False)
        if entity_type == 'JournalEntry':
            resp = pusher.push_journal_entry_update(local_id)
        elif entity_type == 'Bill':
            resp = pusher.push_bill(local_id)
        else:
            raise NotImplementedError(f"otocpa_wins for {entity_type}")
        with _open(self.db_path) as conn:
            conn.execute(
                "UPDATE qbo_sync_state SET sync_status='synced', "
                "conflict_details=NULL WHERE id=?",
                (row['id'],),
            )
            conn.commit()
        return {'status': 'resolved_by_push', 'response': resp}

    def _pull_remote(self, row: dict[str, Any], *, puller: Any) -> dict[str, Any]:
        entity_type = row['entity_type']
        qbo_id = row['qbo_id']
        if puller is None:
            from src.integrations.qbo_pull import QBOPull
            puller = QBOPull(self.firm_code, self.client_code,
                              db_path=self.db_path, sandbox=False)
        # Query the single entity by Id so we don't drag the whole set.
        q = f"SELECT * FROM {entity_type} WHERE Id = '{qbo_id}'"
        rows = puller._query(q, max_results=1)
        if not rows:
            raise RuntimeError(
                f"QBO returned no row for {entity_type}/{qbo_id}"
            )
        entity = rows[0]
        upsert = getattr(puller, f"_upsert_{_snake(entity_type)}", None)
        if upsert is None:
            raise NotImplementedError(f"qbo_wins for {entity_type}")
        upsert(entity)
        with _open(self.db_path) as conn:
            conn.execute(
                "UPDATE qbo_sync_state SET sync_status='synced', "
                "conflict_details=NULL WHERE id=?",
                (row['id'],),
            )
            conn.commit()
        return {'status': 'resolved_by_pull', 'qbo_id': qbo_id}

    def _merge(self, row: dict[str, Any], *, pusher: Any, puller: Any) -> dict[str, Any]:
        """Simple JournalEntry merge heuristic: if QBO edited only the
        memo and we edited only the amount (or vice versa), combine.
        Anything more complex goes to review."""
        # For this scaffold we delegate to the review strategy unless
        # a specialised path exists. The concrete merge logic will live
        # alongside domain-specific knowledge of each entity; the key
        # contract here is "merge never silently loses a field".
        return self.resolve(
            entity_type=row['entity_type'],
            qbo_id=row['qbo_id'],
            strategy='flag_for_review',
        )


def _snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            out.append('_')
        out.append(ch.lower())
    return ''.join(out)
