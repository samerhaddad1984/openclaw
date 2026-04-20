"""QBO pull — pulls entities FROM QuickBooks into OtoCPA.

The HTTP surface is encapsulated behind :class:`QBOPull` so tests can
subclass and stub ``_request`` without touching real Intuit endpoints.

Rate-limit behaviour:

- 401 → one refresh + retry (``get_qbo_tokens`` auto-refreshes ahead of
  expiry, so this only fires on surprise 401s).
- 429 → honour ``Retry-After`` (capped at 60 s), up to 3 retries.
- Other 5xx → exponential back-off 1 s, 2 s, 4 s, then raise.

Pagination is QBO's ``STARTPOSITION`` / ``MAXRESULTS`` pair (1-indexed,
max 1000 rows per page).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from src.agents.tools.qbo_oauth import (
    get_qbo_tokens,
    refresh_access_token,
)

logger = logging.getLogger(__name__)

QBO_BASE_URL = "https://quickbooks.api.intuit.com/v3/company/{realm}"
QBO_SANDBOX_BASE_URL = "https://sandbox-quickbooks.api.intuit.com/v3/company/{realm}"


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class QBORateLimitError(Exception):
    """Raised after exhausting rate-limit retries."""


class QBOAuthError(Exception):
    """Raised when OAuth tokens are missing or cannot be refreshed."""


class QBOPull:
    """Pull entities FROM a QuickBooks Online realm into local caches.

    ``firm_code`` / ``client_code`` scope everything. The caller is
    responsible for supplying a ``db_path`` pointing at a DB that has
    had ``apply_qbo_sync_schema`` applied (the dashboard bootstrap
    does this automatically).
    """

    # Instances may override this for dependency injection in tests.
    _request_fn: Optional[Callable[..., dict[str, Any]]] = None

    def __init__(
        self,
        firm_code: str,
        client_code: str,
        *,
        db_path: Path | str,
        sandbox: bool = False,
    ) -> None:
        self.firm_code = firm_code
        self.client_code = client_code
        self.db_path = Path(db_path)
        self.sandbox = sandbox
        self._tokens = self._load_tokens()
        base_template = QBO_SANDBOX_BASE_URL if sandbox else QBO_BASE_URL
        self.base_url = base_template.format(realm=self._tokens['realm_id'])

    # ------------------------------------------------------------------
    # Token helpers

    def _load_tokens(self) -> dict[str, Any]:
        tokens = get_qbo_tokens(self.firm_code, self.client_code, db_path=self.db_path)
        if not tokens or tokens.get('status') != 'active' or not tokens.get('access_token'):
            raise QBOAuthError(
                f"No active QBO connection for {self.firm_code}/{self.client_code}"
            )
        return tokens

    def _refresh(self) -> None:
        refreshed = refresh_access_token(self.firm_code, self.client_code, self.db_path)
        if refreshed is None:
            raise QBOAuthError(
                f"Refresh failed for {self.firm_code}/{self.client_code}"
            )
        self._tokens = refreshed

    # ------------------------------------------------------------------
    # HTTP

    def _http_request(self, method: str, url: str,
                       headers: dict[str, str],
                       body: Any = None,
                       timeout: int = 30) -> tuple[int, dict[str, str], bytes]:
        """Thin urllib wrapper — overridable in tests without touching urllib."""
        import urllib.error
        import urllib.request
        data = None
        if body is not None:
            if isinstance(body, (dict, list)):
                data = json.dumps(body).encode('utf-8')
            elif isinstance(body, str):
                data = body.encode('utf-8')
            else:
                data = body
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read() or b''

    def _request(self, method: str, path: str,
                  body: Any = None, max_retries: int = 3) -> dict[str, Any]:
        """Authenticated request with refresh + 429 backoff."""
        if self._request_fn is not None:
            return self._request_fn(method, path, body=body)
        url = self.base_url + path
        for attempt in range(max_retries):
            headers = {
                'Authorization': f"Bearer {self._tokens['access_token']}",
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            status, resp_headers, raw = self._http_request(method, url, headers, body)
            if status == 401:
                logger.info("QBO 401 — refreshing")
                self._refresh()
                continue
            if status == 429:
                retry_after = int(resp_headers.get('Retry-After', '60'))
                sleep_for = min(retry_after, 60)
                logger.warning("QBO 429 — sleeping %s s", sleep_for)
                time.sleep(sleep_for)
                continue
            if 500 <= status < 600:
                sleep_for = 2 ** attempt
                logger.warning("QBO %s — backing off %s s", status, sleep_for)
                time.sleep(sleep_for)
                continue
            if 200 <= status < 300:
                if not raw:
                    return {}
                try:
                    return json.loads(raw)
                except ValueError:
                    return {}
            raise RuntimeError(
                f"QBO {method} {path} failed: {status} {raw[:200]!r}"
            )
        raise QBORateLimitError(
            f"QBO {method} {path} failed after {max_retries} retries"
        )

    # ------------------------------------------------------------------
    # Query helper

    def _query(self, sql: str, max_results: int = 1000) -> list[dict[str, Any]]:
        """Run a QBO SQL-like query with pagination.

        ``max_results`` is capped at 1000 by QBO. The ``QueryResponse``
        shape has one key per entity type (``Account``, ``Customer``,
        ``JournalEntry``, ...), so callers pass in a query with an
        explicit ``FROM <EntityType>`` and we discover the key on the
        fly.
        """
        all_items: list[dict[str, Any]] = []
        start = 1
        while True:
            q = f"{sql} STARTPOSITION {start} MAXRESULTS {max_results}"
            resp = self._request(
                'GET',
                '/query?minorversion=73&query=' + urllib.parse.quote(q),
            )
            qr = resp.get('QueryResponse') or {}
            # QR contains exactly one entity list per response.
            page: list[dict[str, Any]] = []
            for k, v in qr.items():
                if isinstance(v, list):
                    page = v
                    break
            all_items.extend(page)
            if len(page) < max_results:
                break
            start += len(page)
        return all_items

    # ------------------------------------------------------------------
    # Public entity pullers

    def pull_accounts(self) -> int:
        rows = self._query("SELECT * FROM Account")
        for qa in rows:
            self._upsert_account(qa)
        return len(rows)

    def pull_customers(self) -> int:
        rows = self._query("SELECT * FROM Customer")
        for qc in rows:
            self._upsert_customer(qc)
        return len(rows)

    def pull_vendors(self) -> int:
        rows = self._query("SELECT * FROM Vendor")
        for qv in rows:
            self._upsert_vendor(qv)
        return len(rows)

    # ------------------------------------------------------------------
    # Upserts

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _record_sync_state(self, conn: sqlite3.Connection, *,
                            entity_type: str, qbo_id: str,
                            sync_token: Optional[str],
                            qbo_last_modified: Optional[str],
                            sync_source: str = 'qbo_origin') -> None:
        conn.execute(
            """
            INSERT INTO qbo_sync_state
                (firm_code, client_code, entity_type, qbo_id,
                 qbo_sync_token, last_pulled_at, last_qbo_modified,
                 sync_status, sync_source)
            VALUES (?,?,?,?,?,?,?, 'synced', ?)
            ON CONFLICT(firm_code, client_code, entity_type, qbo_id) DO UPDATE SET
                qbo_sync_token     = excluded.qbo_sync_token,
                last_pulled_at     = excluded.last_pulled_at,
                last_qbo_modified  = excluded.last_qbo_modified,
                sync_status        = CASE
                    WHEN qbo_sync_state.sync_status = 'conflict' THEN 'conflict'
                    ELSE 'synced'
                END,
                sync_source        = COALESCE(qbo_sync_state.sync_source, excluded.sync_source)
            """,
            (self.firm_code, self.client_code, entity_type, qbo_id,
             sync_token, _iso_now(), qbo_last_modified, sync_source),
        )

    @staticmethod
    def _last_updated(entity: dict[str, Any]) -> Optional[str]:
        meta = entity.get('MetaData') or {}
        return meta.get('LastUpdatedTime')

    @staticmethod
    def _sync_token(entity: dict[str, Any]) -> Optional[str]:
        v = entity.get('SyncToken')
        return str(v) if v is not None else None

    def _upsert_account(self, acct: dict[str, Any]) -> None:
        qbo_id = str(acct.get('Id') or '')
        if not qbo_id:
            return
        parent_ref = None
        pr = acct.get('ParentRef')
        if isinstance(pr, dict):
            parent_ref = pr.get('value')
        with self._open() as conn:
            # Skip write when our last_qbo_modified already covers this update.
            prev = conn.execute(
                "SELECT last_qbo_modified FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? AND entity_type='Account' AND qbo_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()
            new_mod = self._last_updated(acct)
            if prev and prev['last_qbo_modified'] and new_mod \
                    and prev['last_qbo_modified'] >= new_mod:
                return
            conn.execute(
                """
                INSERT INTO qbo_accounts
                    (firm_code, client_code, qbo_id, name, account_type,
                     account_sub_type, account_number, parent_ref, currency,
                     active, classification, balance, current_balance,
                     last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                    name              = excluded.name,
                    account_type      = excluded.account_type,
                    account_sub_type  = excluded.account_sub_type,
                    account_number    = excluded.account_number,
                    parent_ref        = excluded.parent_ref,
                    currency          = excluded.currency,
                    active            = excluded.active,
                    classification    = excluded.classification,
                    balance           = excluded.balance,
                    current_balance   = excluded.current_balance,
                    last_synced       = excluded.last_synced
                """,
                (self.firm_code, self.client_code, qbo_id,
                 acct.get('Name'),
                 acct.get('AccountType'),
                 acct.get('AccountSubType'),
                 acct.get('AcctNum'),
                 parent_ref,
                 (acct.get('CurrencyRef') or {}).get('value'),
                 int(bool(acct.get('Active', True))),
                 acct.get('Classification'),
                 acct.get('CurrentBalance'),
                 acct.get('CurrentBalanceWithSubAccounts'),
                 _iso_now()),
            )
            self._record_sync_state(
                conn,
                entity_type='Account',
                qbo_id=qbo_id,
                sync_token=self._sync_token(acct),
                qbo_last_modified=new_mod,
            )
            conn.commit()

    def _upsert_customer(self, cust: dict[str, Any]) -> None:
        qbo_id = str(cust.get('Id') or '')
        if not qbo_id:
            return
        billing = cust.get('BillAddr') or {}
        shipping = cust.get('ShipAddr') or {}
        with self._open() as conn:
            prev = conn.execute(
                "SELECT last_qbo_modified FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? AND entity_type='Customer' AND qbo_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()
            new_mod = self._last_updated(cust)
            if prev and prev['last_qbo_modified'] and new_mod \
                    and prev['last_qbo_modified'] >= new_mod:
                return
            conn.execute(
                """
                INSERT INTO qbo_customers
                    (firm_code, client_code, qbo_id, display_name,
                     company_name, email, phone, billing_address,
                     shipping_address, balance, active, last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                    display_name     = excluded.display_name,
                    company_name     = excluded.company_name,
                    email            = excluded.email,
                    phone            = excluded.phone,
                    billing_address  = excluded.billing_address,
                    shipping_address = excluded.shipping_address,
                    balance          = excluded.balance,
                    active           = excluded.active,
                    last_synced      = excluded.last_synced
                """,
                (self.firm_code, self.client_code, qbo_id,
                 cust.get('DisplayName'),
                 cust.get('CompanyName'),
                 (cust.get('PrimaryEmailAddr') or {}).get('Address'),
                 (cust.get('PrimaryPhone') or {}).get('FreeFormNumber'),
                 json.dumps(billing) if billing else None,
                 json.dumps(shipping) if shipping else None,
                 cust.get('Balance'),
                 int(bool(cust.get('Active', True))),
                 _iso_now()),
            )
            self._record_sync_state(
                conn,
                entity_type='Customer',
                qbo_id=qbo_id,
                sync_token=self._sync_token(cust),
                qbo_last_modified=new_mod,
            )
            conn.commit()

    def _upsert_vendor(self, vend: dict[str, Any]) -> None:
        qbo_id = str(vend.get('Id') or '')
        if not qbo_id:
            return
        with self._open() as conn:
            prev = conn.execute(
                "SELECT last_qbo_modified FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? AND entity_type='Vendor' AND qbo_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()
            new_mod = self._last_updated(vend)
            if prev and prev['last_qbo_modified'] and new_mod \
                    and prev['last_qbo_modified'] >= new_mod:
                return
            conn.execute(
                """
                INSERT INTO qbo_vendors
                    (firm_code, client_code, qbo_id, display_name,
                     company_name, email, phone, balance, active,
                     tax_identifier, last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                    display_name     = excluded.display_name,
                    company_name     = excluded.company_name,
                    email            = excluded.email,
                    phone            = excluded.phone,
                    balance          = excluded.balance,
                    active           = excluded.active,
                    tax_identifier   = excluded.tax_identifier,
                    last_synced      = excluded.last_synced
                """,
                (self.firm_code, self.client_code, qbo_id,
                 vend.get('DisplayName'),
                 vend.get('CompanyName'),
                 (vend.get('PrimaryEmailAddr') or {}).get('Address'),
                 (vend.get('PrimaryPhone') or {}).get('FreeFormNumber'),
                 vend.get('Balance'),
                 int(bool(vend.get('Active', True))),
                 vend.get('TaxIdentifier'),
                 _iso_now()),
            )
            self._record_sync_state(
                conn,
                entity_type='Vendor',
                qbo_id=qbo_id,
                sync_token=self._sync_token(vend),
                qbo_last_modified=new_mod,
            )
            conn.commit()
