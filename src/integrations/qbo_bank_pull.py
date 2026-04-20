"""QBO bank-register pull.

Extends :class:`QBOPull` with bank-feed detection + transaction pull
for four entity types that populate the QBO bank register:

- ``Purchase`` — cash outflows (with optional payee).
- ``Deposit`` — cash inflows.
- ``Transfer`` — two-sided (creates two ``bank_transactions`` rows,
  one ``out`` on the source account + one ``in`` on the destination).
- ``Check`` — paid-by-check outflows (separate QBO entity).

Writes into ``bank_transactions`` using the multi-source schema
from Phase 1 (source='qbo', external_id=QBO Id, qbo_sync_token=
SyncToken). Upsert is idempotent; re-pulling a row whose SyncToken
hasn't changed is a no-op.

QBOPull._request_fn is respected in tests via the standard FakeQBO.
"""
from __future__ import annotations

import logging
import sqlite3
import uuid
from typing import Any, Optional

from src.integrations.qbo_pull import QBOPull, _iso_now

log = logging.getLogger(__name__)


class QBOBankPull(QBOPull):
    """Pull bank register entries from a connected QBO realm."""

    # ------------------------------------------------------------------
    # Detection

    def detect_bank_accounts(self) -> list[dict[str, Any]]:
        """Return every active Bank-type account in the connected realm."""
        return self._query(
            "SELECT * FROM Account WHERE AccountType = 'Bank' AND Active = true"
        )

    def has_bank_feeds(self) -> bool:
        """True when the realm has at least one Bank account AND at
        least one Purchase posted against any of them.

        The heuristic covers the common case: a QBO company that has
        ever paid for anything out of a bank account. A company with
        a Bank account and zero transactions looks identical to one
        where bank feeds haven't been set up — both answer False here
        so the setup UI recommends Plaid."""
        accounts = self.detect_bank_accounts()
        if not accounts:
            return False
        for acct in accounts:
            acct_id = acct.get('Id')
            if not acct_id:
                continue
            rows = self._query(
                f"SELECT Id FROM Purchase WHERE AccountRef = '{acct_id}'",
                max_results=1,
            )
            if rows:
                return True
        return False

    # ------------------------------------------------------------------
    # Pull

    def pull_bank_transactions(self,
                                 since_date: Optional[str] = None) -> int:
        """Pull Purchases, Deposits, Transfers, and Checks for every
        Bank account. Returns the number of (bank_transactions) rows
        written/updated."""
        total = 0
        for acct in self.detect_bank_accounts():
            acct_id = str(acct.get('Id') or '')
            acct_name = acct.get('Name') or acct_id
            if not acct_id:
                continue
            total += self._pull_purchases(acct_id, acct_name, since_date)
            total += self._pull_deposits(acct_id, acct_name, since_date)
            total += self._pull_transfers(acct_id, acct_name, since_date)
            total += self._pull_checks(acct_id, acct_name, since_date)
        return total

    # ---- individual entity pullers --------------------------------

    def _since(self, since_date: Optional[str]) -> str:
        if since_date:
            return f" AND MetaData.LastUpdatedTime >= '{since_date}'"
        return ""

    def _pull_purchases(self, acct_id: str, acct_name: str,
                          since_date: Optional[str]) -> int:
        q = (f"SELECT * FROM Purchase WHERE AccountRef = '{acct_id}'"
             f"{self._since(since_date)}")
        rows = self._query(q)
        for p in rows:
            self._upsert_tx(self._purchase_row(p, acct_id, acct_name))
        return len(rows)

    def _pull_deposits(self, acct_id: str, acct_name: str,
                         since_date: Optional[str]) -> int:
        # QBO Deposit uses DepositToAccountRef on the header.
        q = (f"SELECT * FROM Deposit WHERE "
             f"DepositToAccountRef = '{acct_id}'"
             f"{self._since(since_date)}")
        rows = self._query(q)
        for d in rows:
            self._upsert_tx(self._deposit_row(d, acct_id, acct_name))
        return len(rows)

    def _pull_transfers(self, acct_id: str, acct_name: str,
                          since_date: Optional[str]) -> int:
        q = (f"SELECT * FROM Transfer WHERE "
             f"(FromAccountRef = '{acct_id}' OR ToAccountRef = '{acct_id}')"
             f"{self._since(since_date)}")
        rows = self._query(q)
        count = 0
        for t in rows:
            from_ref = (t.get('FromAccountRef') or {}).get('value')
            to_ref = (t.get('ToAccountRef') or {}).get('value')
            if from_ref == acct_id:
                self._upsert_tx(
                    self._transfer_row(t, acct_id, acct_name, 'out')
                )
                count += 1
            if to_ref == acct_id:
                self._upsert_tx(
                    self._transfer_row(t, acct_id, acct_name, 'in')
                )
                count += 1
        return count

    def _pull_checks(self, acct_id: str, acct_name: str,
                       since_date: Optional[str]) -> int:
        # The v3 entity is 'Check'; some minor versions expose it as
        # 'CheckPayment'. The FakeQBO / real QBO both accept the first.
        q = (f"SELECT * FROM Check WHERE AccountRef = '{acct_id}'"
             f"{self._since(since_date)}")
        try:
            rows = self._query(q)
        except Exception:
            return 0
        for c in rows:
            self._upsert_tx(self._check_row(c, acct_id, acct_name))
        return len(rows)

    # ---- row builders --------------------------------------------

    @staticmethod
    def _first_line_desc(entity: dict[str, Any]) -> Optional[str]:
        lines = entity.get('Line') or []
        for ln in lines:
            d = ln.get('Description')
            if d:
                return d
        return None

    def _purchase_row(self, p: dict[str, Any],
                        acct_id: str, acct_name: str) -> dict[str, Any]:
        entity_ref = p.get('EntityRef') or {}
        payee = entity_ref.get('name') or (p.get('PaymentRefNum') or '')
        amount = float(p.get('TotalAmt') or 0.0)
        # Purchases are outflows — store negative amount.
        return {
            'qbo_id': str(p.get('Id')),
            'sync_token': str(p.get('SyncToken', '0')),
            'account_id': acct_id,
            'account_name': acct_name,
            'date': p.get('TxnDate'),
            'amount': -abs(amount),
            'description': (payee or self._first_line_desc(p) or 'Purchase'),
            'merchant_name': payee or None,
            'category': 'Purchase',
        }

    def _deposit_row(self, d: dict[str, Any],
                       acct_id: str, acct_name: str) -> dict[str, Any]:
        amount = float(d.get('TotalAmt') or 0.0)
        return {
            'qbo_id': str(d.get('Id')),
            'sync_token': str(d.get('SyncToken', '0')),
            'account_id': acct_id,
            'account_name': acct_name,
            'date': d.get('TxnDate'),
            'amount': abs(amount),
            'description': self._first_line_desc(d) or 'Deposit',
            'merchant_name': None,
            'category': 'Deposit',
        }

    def _transfer_row(self, t: dict[str, Any],
                        acct_id: str, acct_name: str,
                        direction: str) -> dict[str, Any]:
        amount = float(t.get('Amount') or 0.0)
        if direction == 'out':
            signed = -abs(amount)
            other = (t.get('ToAccountRef') or {}).get('name', '')
        else:
            signed = abs(amount)
            other = (t.get('FromAccountRef') or {}).get('name', '')
        # Disambiguate the two legs under the UNIQUE(source, external_id)
        # index by suffixing direction to the QBO Id.
        return {
            'qbo_id': f"{t.get('Id')}:{direction}",
            'sync_token': str(t.get('SyncToken', '0')),
            'account_id': acct_id,
            'account_name': acct_name,
            'date': t.get('TxnDate'),
            'amount': signed,
            'description': (f"Transfer to {other}" if direction == 'out'
                              else f"Transfer from {other}"),
            'merchant_name': None,
            'category': 'Transfer',
        }

    def _check_row(self, c: dict[str, Any],
                     acct_id: str, acct_name: str) -> dict[str, Any]:
        amount = float(c.get('TotalAmt') or 0.0)
        entity_ref = c.get('EntityRef') or {}
        payee = entity_ref.get('name') or (c.get('DocNumber') or '')
        return {
            'qbo_id': str(c.get('Id')),
            'sync_token': str(c.get('SyncToken', '0')),
            'account_id': acct_id,
            'account_name': acct_name,
            'date': c.get('TxnDate'),
            'amount': -abs(amount),
            'description': (payee or self._first_line_desc(c) or 'Check'),
            'merchant_name': payee or None,
            'category': 'Check',
        }

    # ---- upsert ---------------------------------------------------

    def _upsert_tx(self, row: dict[str, Any]) -> None:
        """Insert / update one bank_transactions row for a QBO entity.

        Primary key is (firm_code, client_code, source='qbo', external_id).
        When sync_token hasn't changed we skip the write entirely so
        re-pulls are a no-op."""
        qbo_id = row['qbo_id']
        sync_token = row['sync_token']
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            existing = conn.execute(
                "SELECT id, qbo_sync_token FROM bank_transactions "
                "WHERE firm_code=? AND client_code=? AND source='qbo' "
                "AND external_id=?",
                (self.firm_code, self.client_code, qbo_id),
            ).fetchone()

            if existing:
                if (existing['qbo_sync_token'] or '') == sync_token:
                    return
                conn.execute(
                    "UPDATE bank_transactions SET "
                    " date=?, amount=?, description=?, merchant_name=?, "
                    " category=?, qbo_account_id=?, qbo_sync_token=? "
                    "WHERE id=?",
                    (row['date'], row['amount'], row['description'],
                     row['merchant_name'], row['category'],
                     row['account_id'], sync_token, existing['id']),
                )
                conn.commit()
                return

            new_id = f"qbo_{uuid.uuid4().hex[:16]}"
            conn.execute(
                "INSERT INTO bank_transactions "
                "(id, firm_code, client_code, source, external_id, "
                " date, amount, description, merchant_name, category, "
                " qbo_account_id, qbo_sync_token, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (new_id, self.firm_code, self.client_code, 'qbo', qbo_id,
                 row['date'], row['amount'], row['description'],
                 row['merchant_name'], row['category'],
                 row['account_id'], sync_token, _iso_now()),
            )
            conn.commit()
