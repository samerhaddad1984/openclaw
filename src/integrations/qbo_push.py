"""QBO push — sends OtoCPA-originated JEs / Bills to QuickBooks.

Reuses QBOPull's HTTP loop (OAuth + 401 refresh + 429 backoff + 5xx
retry) by subclassing. Every successful push writes the returned
``Id`` + ``SyncToken`` back into ``qbo_sync_state`` with
``sync_source = 'otocpa_origin'`` so subsequent pulls don't flip it
to ``qbo_origin`` (preserves bidirectional provenance).

Optimistic concurrency: updates require the current ``SyncToken``
from ``qbo_sync_state``. A 409-style conflict from QBO triggers a
pull-merge-retry cycle (delegated to the conflict resolver).
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.integrations.qbo_pull import QBOPull, _iso_now

log = logging.getLogger(__name__)


class QBOPushConflict(Exception):
    """Raised when QBO rejects an update due to SyncToken mismatch."""


class QBOPush(QBOPull):
    """Push OtoCPA entities to QBO. Inherits HTTP + auth from QBOPull."""

    # ------------------------------------------------------------------
    # JournalEntry push

    def push_journal_entry(self, entry_id: str) -> dict[str, Any]:
        """Create a QBO JournalEntry from a local ``manual_journal_entries``
        row. Returns a dict with ``qbo_id``, ``sync_token``, ``status``.
        """
        payload, local = self._build_je_payload(entry_id)
        resp = self._request('POST', '/journalentry?minorversion=73', body=payload)
        qbo_je = resp.get('JournalEntry') or {}
        if not qbo_je:
            raise RuntimeError(f"QBO push returned no JournalEntry: {resp}")
        qbo_id = str(qbo_je.get('Id'))
        sync_token = str(qbo_je.get('SyncToken', '0'))
        last_mod = (qbo_je.get('MetaData') or {}).get('LastUpdatedTime')

        with self._open() as conn:
            self._record_pushed_je(conn, local, qbo_id, sync_token, last_mod)
            conn.commit()

        return {
            'status': 'ok',
            'qbo_id': qbo_id,
            'sync_token': sync_token,
            'entry_id': entry_id,
        }

    def push_journal_entry_update(self, entry_id: str) -> dict[str, Any]:
        """Update an existing QBO JournalEntry. Requires the current
        ``SyncToken`` from ``qbo_sync_state``. On conflict (401-ish
        SyncToken mismatch) raises :class:`QBOPushConflict`."""
        with self._open() as conn:
            state = conn.execute(
                "SELECT qbo_id, qbo_sync_token FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type='JournalEntry' AND local_id=?",
                (self.firm_code, self.client_code, entry_id),
            ).fetchone()
        if not state or not state['qbo_id']:
            raise RuntimeError(
                f"No prior QBO push for entry_id={entry_id}; call push first"
            )

        payload, local = self._build_je_payload(
            entry_id,
            qbo_id=state['qbo_id'],
            sync_token=state['qbo_sync_token'] or '0',
        )
        try:
            resp = self._request('POST', '/journalentry?minorversion=73',
                                  body=payload)
        except RuntimeError as exc:
            # QBO sends 400 with "Stale Object Update" text on sync_token
            # mismatch. Surface as a conflict for the resolver.
            if 'stale' in str(exc).lower() or 'sync_token' in str(exc).lower():
                raise QBOPushConflict(str(exc)) from exc
            raise
        qbo_je = resp.get('JournalEntry') or {}
        qbo_id = str(qbo_je.get('Id', state['qbo_id']))
        new_token = str(qbo_je.get('SyncToken', '0'))
        last_mod = (qbo_je.get('MetaData') or {}).get('LastUpdatedTime')

        with self._open() as conn:
            self._record_pushed_je(conn, local, qbo_id, new_token, last_mod)
            conn.commit()
        return {'status': 'updated', 'qbo_id': qbo_id,
                'sync_token': new_token, 'entry_id': entry_id}

    def push_journal_entry_void(self, entry_id: str) -> dict[str, Any]:
        """QBO doesn't delete JEs; it voids them by POST + ?operation=void."""
        with self._open() as conn:
            state = conn.execute(
                "SELECT qbo_id, qbo_sync_token FROM qbo_sync_state "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type='JournalEntry' AND local_id=?",
                (self.firm_code, self.client_code, entry_id),
            ).fetchone()
        if not state or not state['qbo_id']:
            raise RuntimeError(
                f"No prior QBO push for entry_id={entry_id}; nothing to void"
            )
        payload = {
            'Id': state['qbo_id'],
            'SyncToken': state['qbo_sync_token'] or '0',
        }
        resp = self._request('POST',
                              '/journalentry?operation=void&minorversion=73',
                              body=payload)
        qbo_je = resp.get('JournalEntry') or {}
        new_token = str(qbo_je.get('SyncToken', '0'))
        with self._open() as conn:
            conn.execute(
                "UPDATE qbo_sync_state SET qbo_sync_token=?, "
                "last_pushed_at=?, sync_status='voided' "
                "WHERE firm_code=? AND client_code=? "
                "AND entity_type='JournalEntry' AND local_id=?",
                (new_token, _iso_now(), self.firm_code,
                 self.client_code, entry_id),
            )
            conn.commit()
        return {'status': 'voided', 'qbo_id': state['qbo_id']}

    # ------------------------------------------------------------------
    # Bill push

    def push_bill(self, document_id: str) -> dict[str, Any]:
        """Push an OtoCPA document (expense) as a QBO Bill.

        The document is expected to have vendor + amount + document_date
        + a qbo_account / gl_account mapping. This function assumes the
        caller has resolved those fields into a ``documents`` row.
        """
        with self._open() as conn:
            doc = conn.execute(
                "SELECT * FROM documents WHERE document_id=?",
                (document_id,),
            ).fetchone()
        if not doc:
            raise RuntimeError(f"document_id={document_id} not found")

        payload = self._build_bill_payload(doc)
        resp = self._request('POST', '/bill?minorversion=73', body=payload)
        qbo_bill = resp.get('Bill') or {}
        qbo_id = str(qbo_bill.get('Id'))
        sync_token = str(qbo_bill.get('SyncToken', '0'))
        last_mod = (qbo_bill.get('MetaData') or {}).get('LastUpdatedTime')

        with self._open() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO qbo_bills "
                "(firm_code, client_code, qbo_id, doc_number, "
                " total_amount, source, local_document_id, last_synced) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (self.firm_code, self.client_code, qbo_id,
                 doc['invoice_number'] if 'invoice_number' in doc.keys()
                 else document_id,
                 doc['amount'] if 'amount' in doc.keys() else None,
                 'otocpa_origin', document_id, _iso_now()),
            )
            self._record_push_state(
                conn,
                entity_type='Bill',
                qbo_id=qbo_id,
                sync_token=sync_token,
                qbo_last_modified=last_mod,
                local_id=document_id,
            )
            conn.commit()
        return {'status': 'ok', 'qbo_id': qbo_id, 'sync_token': sync_token}

    # ------------------------------------------------------------------
    # Payload builders

    def _build_je_payload(self, entry_id: str,
                           *, qbo_id: Optional[str] = None,
                           sync_token: Optional[str] = None) -> tuple[dict[str, Any], sqlite3.Row]:
        with self._open() as conn:
            je = conn.execute(
                "SELECT * FROM manual_journal_entries WHERE entry_id=?",
                (entry_id,),
            ).fetchone()
        if not je:
            raise RuntimeError(f"manual_journal_entries row not found: {entry_id}")

        debit_acct = self._resolve_qbo_account_id(je['debit_account'])
        credit_acct = self._resolve_qbo_account_id(je['credit_account'])
        if not debit_acct or not credit_acct:
            raise RuntimeError(
                f"Cannot push JE {entry_id}: missing QBO account mapping for "
                f"{je['debit_account']} / {je['credit_account']}"
            )

        amount = float(je['amount'] or 0.0)
        body: dict[str, Any] = {
            'TxnDate': je['entry_date'],
            'DocNumber': je['entry_id'][:21],  # QBO DocNumber max 21 chars
            'PrivateNote': je['description'] or '',
            'Line': [
                {
                    'Amount': amount,
                    'DetailType': 'JournalEntryLineDetail',
                    'Description': je['description'] or '',
                    'JournalEntryLineDetail': {
                        'PostingType': 'Debit',
                        'AccountRef': {'value': debit_acct},
                    }
                },
                {
                    'Amount': amount,
                    'DetailType': 'JournalEntryLineDetail',
                    'Description': je['description'] or '',
                    'JournalEntryLineDetail': {
                        'PostingType': 'Credit',
                        'AccountRef': {'value': credit_acct},
                    }
                },
            ],
        }
        if qbo_id:
            body['Id'] = qbo_id
        if sync_token is not None:
            body['SyncToken'] = sync_token
        return body, je

    def _build_bill_payload(self, doc: sqlite3.Row) -> dict[str, Any]:
        keys = doc.keys()
        vendor_name = (doc['vendor'] if 'vendor' in keys else None) or ''
        vendor_qbo_id = self._resolve_vendor_qbo_id(vendor_name)
        if not vendor_qbo_id:
            raise RuntimeError(f"No QBO vendor mapped for '{vendor_name}'")

        document_id = doc['document_id'] if 'document_id' in keys else None
        invoice_lines = self._fetch_invoice_lines(document_id) if document_id else []

        if invoice_lines:
            qbo_lines = self._build_expense_lines_from_invoice_lines(
                invoice_lines, document_id,
            )
        else:
            # Single-line fallback for documents without OCR line-item extraction.
            #
            # Layer 3 invariant: refuse to push documents the OCR engine
            # never categorised. Pre-Layer-2 ingest silently wrote
            # gl_account='5440' / category='operating_expense' for any
            # doc it couldn't classify (the rcpt_16.png bug). Pushing
            # those to QBO would put real money against a fake GL.
            needs_cat = (
                doc['needs_categorization'] if 'needs_categorization' in keys
                else 0
            )
            if needs_cat:
                raise RuntimeError(
                    f"Document {document_id} is flagged "
                    f"needs_categorization=1; refusing to push to QBO. "
                    f"CPA must categorise it first."
                )
            amount = float((doc['amount'] if 'amount' in keys else 0) or 0)
            account_code = doc['gl_account'] if 'gl_account' in keys else None
            if not account_code:
                raise RuntimeError(
                    f"Document {document_id} has no gl_account and no line "
                    f"items; refusing to push to QBO. CPA must categorise."
                )
            account_qbo_id = self._resolve_qbo_account_id(account_code) if account_code else None
            if not account_qbo_id:
                raise RuntimeError(f"No QBO account mapped for GL {account_code}")
            qbo_lines = [{
                'Amount': amount,
                'DetailType': 'AccountBasedExpenseLineDetail',
                'Description': vendor_name,
                'AccountBasedExpenseLineDetail': {
                    'AccountRef': {'value': account_qbo_id},
                },
            }]

        return {
            'VendorRef': {'value': vendor_qbo_id},
            'TxnDate': doc['document_date'] if 'document_date' in keys else None,
            'DueDate': doc['due_date'] if 'due_date' in keys else None,
            'Line': qbo_lines,
        }

    def _fetch_invoice_lines(self, document_id: str) -> list[dict[str, Any]]:
        """Fetch invoice_lines for a document. Returns [] if none or missing.

        Soft-deleted lines (``deleted_at`` set by a split/merge/allocate
        operation) are excluded so the QBO push reflects the CPA's final
        line layout, not the raw OCR extraction.
        """
        with self._open() as conn:
            try:
                # First try the query filtering by deleted_at. If the column
                # doesn't exist on an older DB, fall back to the unfiltered
                # SELECT.
                try:
                    rows = conn.execute(
                        """SELECT line_number, description, quantity, unit_price,
                                  line_total_pretax, tax_code, gl_account
                             FROM invoice_lines
                            WHERE document_id = ?
                              AND (deleted_at IS NULL OR deleted_at = '')
                            ORDER BY line_number""",
                        (document_id,),
                    ).fetchall()
                except sqlite3.OperationalError:
                    rows = conn.execute(
                        """SELECT line_number, description, quantity, unit_price,
                                  line_total_pretax, tax_code, gl_account
                             FROM invoice_lines
                            WHERE document_id = ?
                            ORDER BY line_number""",
                        (document_id,),
                    ).fetchall()
            except sqlite3.OperationalError:
                return []
        return [dict(r) for r in rows]

    def _build_expense_lines_from_invoice_lines(
        self,
        invoice_lines: list[dict[str, Any]],
        document_id: Optional[str],
    ) -> list[dict[str, Any]]:
        """Convert invoice_lines rows into QBO AccountBasedExpenseLineDetail entries.

        Each invoice line becomes a distinct QBO Line with its own AccountRef +
        (optional) TaxCodeRef + Description. Any line missing a QBO-mapped GL
        account is a hard error — we never silently collapse detail.
        """
        qbo_lines: list[dict[str, Any]] = []
        for line in invoice_lines:
            gl_code = line.get('gl_account')
            account_qbo_id = self._resolve_qbo_account_id(gl_code) if gl_code else None
            if not account_qbo_id:
                raise RuntimeError(
                    f"No QBO account mapped for GL {gl_code!r} on "
                    f"document_id={document_id} line {line.get('line_number')}"
                )

            amount = float(line.get('line_total_pretax') or 0)
            detail: dict[str, Any] = {'AccountRef': {'value': account_qbo_id}}
            tax_code = line.get('tax_code')
            if tax_code:
                detail['TaxCodeRef'] = {'value': str(tax_code)}
            detail['BillableStatus'] = 'NotBillable'

            qbo_lines.append({
                'Amount': amount,
                'DetailType': 'AccountBasedExpenseLineDetail',
                'Description': line.get('description') or '',
                'AccountBasedExpenseLineDetail': detail,
            })
        return qbo_lines

    # ------------------------------------------------------------------
    # Account / vendor resolution

    def _resolve_qbo_account_id(self, local_code: Optional[str]) -> Optional[str]:
        if not local_code:
            return None
        with self._open() as conn:
            row = conn.execute(
                "SELECT qbo_id FROM qbo_accounts "
                "WHERE firm_code=? AND client_code=? "
                "AND (account_number=? OR name=?) "
                "ORDER BY account_number=? DESC LIMIT 1",
                (self.firm_code, self.client_code, local_code, local_code,
                 local_code),
            ).fetchone()
        return row['qbo_id'] if row else None

    def _resolve_vendor_qbo_id(self, vendor_name: str) -> Optional[str]:
        if not vendor_name:
            return None
        with self._open() as conn:
            row = conn.execute(
                "SELECT qbo_id FROM qbo_vendors "
                "WHERE firm_code=? AND client_code=? "
                "AND (display_name=? OR company_name=?) LIMIT 1",
                (self.firm_code, self.client_code, vendor_name, vendor_name),
            ).fetchone()
        return row['qbo_id'] if row else None

    # ------------------------------------------------------------------
    # State recording

    def _record_push_state(self, conn: sqlite3.Connection, *,
                            entity_type: str, qbo_id: str,
                            sync_token: Optional[str],
                            qbo_last_modified: Optional[str],
                            local_id: Optional[str]) -> None:
        conn.execute(
            """
            INSERT INTO qbo_sync_state
                (firm_code, client_code, entity_type, qbo_id,
                 qbo_sync_token, local_id, last_pushed_at,
                 last_qbo_modified, sync_status, sync_source)
            VALUES (?,?,?,?,?,?,?,?, 'synced', 'otocpa_origin')
            ON CONFLICT(firm_code, client_code, entity_type, qbo_id) DO UPDATE SET
                qbo_sync_token   = excluded.qbo_sync_token,
                local_id         = COALESCE(excluded.local_id, qbo_sync_state.local_id),
                last_pushed_at   = excluded.last_pushed_at,
                last_qbo_modified= excluded.last_qbo_modified,
                sync_status      = 'synced',
                sync_source      = 'otocpa_origin'
            """,
            (self.firm_code, self.client_code, entity_type, qbo_id,
             sync_token, local_id, _iso_now(), qbo_last_modified),
        )

    def _record_pushed_je(self, conn: sqlite3.Connection,
                            local: sqlite3.Row, qbo_id: str,
                            sync_token: str,
                            qbo_last_modified: Optional[str]) -> None:
        # Mirror header into qbo_journal_entries with local_je_id
        conn.execute(
            """
            INSERT INTO qbo_journal_entries
                (firm_code, client_code, qbo_id, doc_number, txn_date,
                 total_amount, memo, source, local_je_id, last_synced)
            VALUES (?,?,?,?,?,?,?, 'otocpa_origin', ?, ?)
            ON CONFLICT(firm_code, client_code, qbo_id) DO UPDATE SET
                doc_number    = excluded.doc_number,
                txn_date      = excluded.txn_date,
                total_amount  = excluded.total_amount,
                memo          = excluded.memo,
                source        = 'otocpa_origin',
                local_je_id   = excluded.local_je_id,
                last_synced   = excluded.last_synced
            """,
            (self.firm_code, self.client_code, qbo_id,
             local['entry_id'], local['entry_date'],
             float(local['amount'] or 0), local['description'],
             local['entry_id'], _iso_now()),
        )
        self._record_push_state(
            conn, entity_type='JournalEntry', qbo_id=qbo_id,
            sync_token=sync_token,
            qbo_last_modified=qbo_last_modified,
            local_id=local['entry_id'],
        )
