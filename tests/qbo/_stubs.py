"""Shared test stubs for QBO pull/push tests — avoid real Intuit HTTP."""
from __future__ import annotations

import sqlite3
import time
import urllib.parse
from pathlib import Path
from typing import Any, Callable

from src.agents.tools.qbo_oauth import (
    _ensure_table as _ensure_qbo_connection_table,
    store_qbo_tokens,
)
from src.integrations.qbo_schema import ensure_qbo_sync_schema


def make_db(tmp_path: Path, firm: str = 'F1', client: str = 'C1',
            realm: str = 'realm-1',
            access: str = 'at', refresh: str = 'rt') -> Path:
    """Create a tmp DB with qbo_connections + qbo_sync schema + tokens."""
    db = tmp_path / 'qbo.db'
    _ensure_qbo_connection_table(db)
    ensure_qbo_sync_schema(db)
    store_qbo_tokens(
        firm_code=firm, client_code=client, realm_id=realm,
        access_token=access, refresh_token=refresh,
        expires_in=3600, db_path=db,
    )
    return db


class FakeQBO:
    """Script-driven fake for QBOPull._request_fn.

    Usage::

        fake = FakeQBO()
        fake.add_query('SELECT * FROM Account', [{...}, {...}])
        pull = QBOPull('F', 'C', db_path=db)
        pull._request_fn = fake.respond

    Supports pagination: when ``len(items) > max_results`` the fake
    slices by ``STARTPOSITION``.
    """

    def __init__(self) -> None:
        self._queries: dict[str, list[dict[str, Any]]] = {}
        self.call_log: list[tuple[str, str, Any]] = []
        self.raise_401_once = False
        self.raise_429_once = False
        self.raise_500_times = 0

    def add_query(self, sql_prefix: str, items: list[dict[str, Any]]) -> None:
        self._queries[sql_prefix.strip().upper()] = items

    def respond(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        self.call_log.append((method, path, body))
        if self.raise_401_once:
            self.raise_401_once = False
            raise _FakeHTTP(401)
        if self.raise_429_once:
            self.raise_429_once = False
            raise _FakeHTTP(429, retry_after=1)
        if self.raise_500_times > 0:
            self.raise_500_times -= 1
            raise _FakeHTTP(500)
        if '/query' in path:
            # Parse the q param.
            q = urllib.parse.unquote(path.split('query=', 1)[1])
            key = q.split(' STARTPOSITION', 1)[0].strip().upper()
            items = self._queries.get(key, [])
            # Apply STARTPOSITION + MAXRESULTS
            start = 1
            maxr = 1000
            if 'STARTPOSITION' in q:
                _, _, after = q.partition('STARTPOSITION')
                parts = after.split()
                if parts:
                    start = int(parts[0])
            if 'MAXRESULTS' in q:
                _, _, after = q.partition('MAXRESULTS')
                parts = after.split()
                if parts:
                    maxr = int(parts[0])
            page = items[start - 1: start - 1 + maxr]
            # Guess entity name from FROM <Entity>
            try:
                entity = q.upper().split('FROM')[1].strip().split()[0].title()
            except IndexError:
                entity = 'Item'
            # QBO uses the entity name as the key; JournalEntry is one word
            if entity.lower() == 'journalentry':
                entity = 'JournalEntry'
            return {'QueryResponse': {entity: page}}
        if '/journalentry' in path and method == 'POST':
            return {
                'JournalEntry': {
                    'Id': 'JE-NEW',
                    'SyncToken': '0',
                    'MetaData': {'LastUpdatedTime': '2026-04-20T12:00:00Z'},
                }
            }
        return {}


class _FakeHTTP(Exception):
    """Used by FakeQBO to simulate HTTP errors inside _request_fn."""

    def __init__(self, status: int, retry_after: int = 0):
        super().__init__(f'HTTP {status}')
        self.status = status
        self.retry_after = retry_after


def patch_request_with_fake(pull, fake: FakeQBO):
    """Wire FakeQBO into a QBOPull instance; translate _FakeHTTP into the
    behaviour QBOPull's loop expects (auth refresh, rate-limit sleep)."""
    def _fake_request(method, path, body=None, max_retries=3):
        for attempt in range(max_retries):
            try:
                return fake.respond(method, path, body)
            except _FakeHTTP as exc:
                if exc.status == 401:
                    pull._refresh()
                    continue
                if exc.status == 429:
                    time.sleep(min(exc.retry_after, 0))  # 0 in tests
                    continue
                if 500 <= exc.status < 600:
                    time.sleep(0)
                    continue
                raise
        raise RuntimeError("FakeQBO: retries exhausted")
    pull._request = _fake_request  # type: ignore[method-assign]
