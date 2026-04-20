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

    def pull_journal_entries(self, since_date: Optional[str] = None) -> int:
        """Pull JournalEntry rows. When ``since_date`` is given, only
        pulls entries with ``MetaData.LastUpdatedTime >= since_date``.

        QBO-origin JEs (i.e. entries not already tracked in sync_state
        as 'otocpa_origin') are also mirrored into ``gl_transactions``
        via ``_mirror_to_gl`` so unified financial statements include
        them."""
        if since_date:
            q = (f"SELECT * FROM JournalEntry WHERE "
                 f"MetaData.LastUpdatedTime >= '{since_date}'")
        else:
            q = "SELECT * FROM JournalEntry"
        rows = self._query(q)
        for je in rows:
            self._upsert_journal_entry(je)
        return len(rows)

    def pull_bills(self, since_date: Optional[str] = None) -> int:
        if since_date:
            q = (f"SELECT * FROM Bill WHERE "
                 f"MetaData.LastUpdatedTime >= '{since_date}'")
        else:
            q = "SELECT * FROM Bill"
        rows = self._query(q)
        for b in rows:
            self._upsert_bill(b)
        return len(rows)

    def pull_invoices(self, since_date: Optional[str] = None) -> int:
        if since_date:
            q = (f"SELECT * FROM Invoice WHERE "
                 f"MetaData.LastUpdatedTime >= '{since_date}'")
        else:
            q = "SELECT * FROM Invoice"
        rows = self._query(q)
        for inv in rows:
            self._upsert_invoice(inv)
        return len(rows)

    def pull_payments(self, since_date: Optional[str] = None) -> int:
        """Pull BillPayment + Payment. Currently a no-op placeholder
        because the local data model doesn't differentiate payments
        from their parent bills/invoices — the balance on the parent
        is refreshed when bills/invoices are re-pulled."""
        return 0

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

    # ------------------------------------------------------------------
    # Transaction upserts

    def _upsert_journal_entry(self, je: dict[str, Any]) -> None:
        qbo_id = str(je.get('Id') or '')
        if not qbo_id:
            return
        new_mod = self._last_updated(je)
        with self._open() as conn:
            prev_state = conn.execute(
                "SELECT sync_source, last_qbo_modified FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type='JournalEntry' AND qbo_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()
            if prev_state and prev_state['last_qbo_modified'] and new_mod \
                    and prev_state['last_qbo_modified'] >= new_mod:
                return

            source = 'qbo_origin'
            local_je_id: Optional[int] = None
            if prev_state and prev_state['sync_source'] == 'otocpa_origin':
                source = 'otocpa_origin'
                row = conn.execute(
                    "SELECT local_je_id FROM qbo_journal_entries "
                    "WHERE firm_code=? AND client_code=? AND qbo_id=?",
                    (self.firm_code, self.client_code, qbo_id),
                ).fetchone()
                if row:
                    local_je_id = row['local_je_id']

            conn.execute(
                """
                INSERT INTO qbo_journal_entries
                    (firm_code, client_code, qbo_id, doc_number, txn_date,
                     total_amount, currency, memo, adjustment, source,
                     local_je_id, last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                    doc_number    = excluded.doc_number,
                    txn_date      = excluded.txn_date,
                    total_amount  = excluded.total_amount,
                    currency      = excluded.currency,
                    memo          = excluded.memo,
                    adjustment    = excluded.adjustment,
                    last_synced   = excluded.last_synced
                """,
                (self.firm_code, self.client_code, qbo_id,
                 je.get('DocNumber'),
                 je.get('TxnDate'),
                 je.get('TotalAmt'),
                 (je.get('CurrencyRef') or {}).get('value'),
                 je.get('PrivateNote') or je.get('Memo'),
                 1 if je.get('Adjustment') else 0,
                 source,
                 local_je_id,
                 _iso_now()),
            )

            # Replace lines (QBO doesn't delete; new version has full array).
            conn.execute(
                "DELETE FROM qbo_journal_entry_lines WHERE qbo_je_id=?",
                (qbo_id,),
            )
            for line in je.get('Line') or []:
                if line.get('DetailType') != 'JournalEntryLineDetail':
                    continue
                detail = line.get('JournalEntryLineDetail') or {}
                posting = detail.get('PostingType')  # 'Debit' or 'Credit'
                account_ref = (detail.get('AccountRef') or {}).get('value')
                customer_ref = None
                vendor_ref = None
                class_ref = None
                entity = detail.get('Entity') or {}
                if entity:
                    etype = entity.get('Type')
                    ev = (entity.get('EntityRef') or {}).get('value')
                    if etype == 'Customer':
                        customer_ref = ev
                    elif etype == 'Vendor':
                        vendor_ref = ev
                cr = detail.get('ClassRef')
                if isinstance(cr, dict):
                    class_ref = cr.get('value')
                conn.execute(
                    """
                    INSERT INTO qbo_journal_entry_lines
                        (qbo_je_id, line_num, amount, debit_credit,
                         account_qbo_id, description, customer_qbo_id,
                         vendor_qbo_id, class_qbo_id)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (qbo_id, line.get('LineNum') or 0,
                     float(line.get('Amount') or 0.0),
                     posting,
                     account_ref,
                     line.get('Description'),
                     customer_ref, vendor_ref, class_ref),
                )

            self._record_sync_state(
                conn,
                entity_type='JournalEntry', qbo_id=qbo_id,
                sync_token=self._sync_token(je),
                qbo_last_modified=new_mod,
                sync_source=source,
            )
            conn.commit()

        # Mirror qbo_origin JEs into gl_transactions so unified financials
        # see them. otocpa_origin JEs already have their gl rows; skip.
        if source == 'qbo_origin':
            self._mirror_je_to_gl(qbo_id)

    def _mirror_je_to_gl(self, qbo_je_id: str) -> None:
        """Write gl_transactions rows for a qbo_origin JE so existing
        TB / P&L / BS queries (which read gl_transactions) pick up the
        data. Idempotent: replaces any prior rows for the same QBO ID.
        """
        with self._open() as conn:
            je = conn.execute(
                "SELECT * FROM qbo_journal_entries WHERE qbo_id=? "
                "AND firm_code=? AND client_code=?",
                (qbo_je_id, self.firm_code, self.client_code),
            ).fetchone()
            if not je:
                return
            lines = conn.execute(
                "SELECT * FROM qbo_journal_entry_lines WHERE qbo_je_id=?",
                (qbo_je_id,),
            ).fetchall()
            if not lines:
                return

            # Ensure gl_transactions exists even in test DBs that skipped
            # the gl_engine.ensure_schema path.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gl_transactions (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_id      TEXT NOT NULL,
                    client_code   TEXT NOT NULL,
                    period        TEXT NOT NULL,
                    entry_date    TEXT NOT NULL,
                    account_code  TEXT NOT NULL,
                    side          TEXT NOT NULL CHECK (side IN ('debit','credit')),
                    amount        REAL NOT NULL CHECK (amount > 0),
                    description   TEXT,
                    source        TEXT NOT NULL DEFAULT 'manual_je',
                    document_id   TEXT,
                    reversed_by   TEXT,
                    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """,
            )

            entry_id = f"QBO:{qbo_je_id}"
            # Replace any prior mirrored rows — the latest pull wins.
            conn.execute(
                "DELETE FROM gl_transactions WHERE entry_id=?",
                (entry_id,),
            )
            txn_date = je['txn_date'] or _iso_now()[:10]
            period = txn_date[:7] if len(txn_date) >= 7 else txn_date
            for line in lines:
                account_code = self._resolve_account_code(
                    conn, line['account_qbo_id']
                )
                if not account_code:
                    continue
                posting = (line['debit_credit'] or '').lower()
                if posting not in ('debit', 'credit'):
                    continue
                amount = float(line['amount'] or 0.0)
                if amount <= 0.005:
                    continue
                conn.execute(
                    """
                    INSERT INTO gl_transactions
                        (entry_id, client_code, period, entry_date,
                         account_code, side, amount, description, source)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (entry_id, self.client_code, period, txn_date,
                     account_code, posting, amount,
                     line['description'] or je['memo'] or '',
                     'qbo'),
                )
            conn.commit()

    def _resolve_account_code(self, conn: sqlite3.Connection,
                                qbo_account_id: Optional[str]) -> Optional[str]:
        if not qbo_account_id:
            return None
        row = conn.execute(
            "SELECT account_number, name FROM qbo_accounts "
            "WHERE firm_code=? AND client_code=? AND qbo_id=?",
            (self.firm_code, self.client_code, qbo_account_id),
        ).fetchone()
        if not row:
            return None
        # Prefer explicit account_number; fall back to name so mirroring
        # works on QBO accounts without a set number.
        return row['account_number'] or row['name']

    def _upsert_bill(self, bill: dict[str, Any]) -> None:
        qbo_id = str(bill.get('Id') or '')
        if not qbo_id:
            return
        new_mod = self._last_updated(bill)
        vendor_ref = (bill.get('VendorRef') or {}).get('value')
        with self._open() as conn:
            prev = conn.execute(
                "SELECT sync_source, last_qbo_modified FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type='Bill' AND qbo_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()
            if prev and prev['last_qbo_modified'] and new_mod \
                    and prev['last_qbo_modified'] >= new_mod:
                return
            source = 'otocpa_origin' if (prev and prev['sync_source'] == 'otocpa_origin') else 'qbo_origin'
            conn.execute(
                """
                INSERT INTO qbo_bills
                    (firm_code, client_code, qbo_id, vendor_qbo_id,
                     doc_number, txn_date, due_date, total_amount,
                     balance, memo, source, last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                    vendor_qbo_id = excluded.vendor_qbo_id,
                    doc_number    = excluded.doc_number,
                    txn_date      = excluded.txn_date,
                    due_date      = excluded.due_date,
                    total_amount  = excluded.total_amount,
                    balance       = excluded.balance,
                    memo          = excluded.memo,
                    last_synced   = excluded.last_synced
                """,
                (self.firm_code, self.client_code, qbo_id, vendor_ref,
                 bill.get('DocNumber'), bill.get('TxnDate'),
                 bill.get('DueDate'),
                 bill.get('TotalAmt'),
                 bill.get('Balance'),
                 bill.get('PrivateNote') or bill.get('Memo'),
                 source, _iso_now()),
            )
            self._record_sync_state(
                conn, entity_type='Bill', qbo_id=qbo_id,
                sync_token=self._sync_token(bill),
                qbo_last_modified=new_mod,
                sync_source=source,
            )
            conn.commit()

    def _upsert_invoice(self, inv: dict[str, Any]) -> None:
        qbo_id = str(inv.get('Id') or '')
        if not qbo_id:
            return
        new_mod = self._last_updated(inv)
        customer_ref = (inv.get('CustomerRef') or {}).get('value')
        with self._open() as conn:
            prev = conn.execute(
                "SELECT sync_source, last_qbo_modified FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type='Invoice' AND qbo_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()
            if prev and prev['last_qbo_modified'] and new_mod \
                    and prev['last_qbo_modified'] >= new_mod:
                return
            source = 'otocpa_origin' if (prev and prev['sync_source'] == 'otocpa_origin') else 'qbo_origin'
            conn.execute(
                """
                INSERT INTO qbo_invoices
                    (firm_code, client_code, qbo_id, customer_qbo_id,
                     doc_number, txn_date, due_date, total_amount,
                     balance, memo, source, last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                    customer_qbo_id = excluded.customer_qbo_id,
                    doc_number      = excluded.doc_number,
                    txn_date        = excluded.txn_date,
                    due_date        = excluded.due_date,
                    total_amount    = excluded.total_amount,
                    balance         = excluded.balance,
                    memo            = excluded.memo,
                    last_synced     = excluded.last_synced
                """,
                (self.firm_code, self.client_code, qbo_id, customer_ref,
                 inv.get('DocNumber'), inv.get('TxnDate'),
                 inv.get('DueDate'),
                 inv.get('TotalAmt'),
                 inv.get('Balance'),
                 inv.get('PrivateNote') or inv.get('Memo'),
                 source, _iso_now()),
            )
            self._record_sync_state(
                conn, entity_type='Invoice', qbo_id=qbo_id,
                sync_token=self._sync_token(inv),
                qbo_last_modified=new_mod,
                sync_source=source,
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
