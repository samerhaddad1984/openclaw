from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Optional

from src.agents.core.task_models import DocumentRecord


class TaskStore:

    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            review_status TEXT,
            confidence REAL,
            raw_result TEXT
        )
        """)

        self.conn.commit()

    def add_document(self, record: DocumentRecord):

        cur = self.conn.cursor()

        cur.execute("""
        INSERT INTO documents (
            document_id,
            file_name,
            file_path,
            client_code,
            vendor,
            doc_type,
            amount,
            document_date,
            gl_account,
            tax_code,
            category,
            review_status,
            confidence,
            raw_result
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.document_id,
            record.file_name,
            record.file_path,
            record.client_code,
            record.vendor,
            record.doc_type,
            record.amount,
            record.document_date,
            record.gl_account,
            record.tax_code,
            record.category,
            record.review_status,
            record.confidence,
            record.raw_result
        ))

        self.conn.commit()

    def replace_document(
        self,
        document_id: str,
        file_name: str,
        file_path: str,
        client_code: Optional[str],
        vendor: Optional[str],
        doc_type: Optional[str],
        amount: Optional[float],
        document_date: Optional[str],
        gl_account: Optional[str],
        tax_code: Optional[str],
        category: Optional[str],
        review_status: str,
        confidence: float,
        raw_result: str,
    ):

        cur = self.conn.cursor()

        cur.execute("""
        UPDATE documents
        SET
            file_name=?,
            file_path=?,
            client_code=?,
            vendor=?,
            doc_type=?,
            amount=?,
            document_date=?,
            gl_account=?,
            tax_code=?,
            category=?,
            review_status=?,
            confidence=?,
            raw_result=?
        WHERE document_id=?
        """, (
            file_name,
            file_path,
            client_code,
            vendor,
            doc_type,
            amount,
            document_date,
            gl_account,
            tax_code,
            category,
            review_status,
            confidence,
            raw_result,
            document_id
        ))

        self.conn.commit()

    def list_documents(self) -> List[DocumentRecord]:

        cur = self.conn.cursor()

        rows = cur.execute("SELECT * FROM documents").fetchall()

        docs = []

        for r in rows:
            docs.append(DocumentRecord(
                document_id=r["document_id"],
                file_name=r["file_name"],
                file_path=r["file_path"],
                client_code=r["client_code"],
                vendor=r["vendor"],
                doc_type=r["doc_type"],
                amount=r["amount"],
                document_date=r["document_date"],
                gl_account=r["gl_account"],
                tax_code=r["tax_code"],
                category=r["category"],
                review_status=r["review_status"],
                confidence=r["confidence"],
                raw_result=r["raw_result"],
            ))

        return docs

    def get_document(self, document_id: str) -> Optional[DocumentRecord]:

        cur = self.conn.cursor()

        r = cur.execute(
            "SELECT * FROM documents WHERE document_id=?",
            (document_id,)
        ).fetchone()

        if r is None:
            return None

        return DocumentRecord(
            document_id=r["document_id"],
            file_name=r["file_name"],
            file_path=r["file_path"],
            client_code=r["client_code"],
            vendor=r["vendor"],
            doc_type=r["doc_type"],
            amount=r["amount"],
            document_date=r["document_date"],
            gl_account=r["gl_account"],
            tax_code=r["tax_code"],
            category=r["category"],
            review_status=r["review_status"],
            confidence=r["confidence"],
            raw_result=r["raw_result"],
        )