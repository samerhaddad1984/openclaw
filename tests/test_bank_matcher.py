from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.bank_models import BankTransaction
from src.agents.core.task_models import DocumentRecord
from src.agents.tools.bank_matcher import BankMatcher


def make_doc(
    document_id: str,
    file_name: str,
    client_code: str,
    vendor: str,
    amount: float,
    document_date: str,
    doc_type: str = "invoice",
    raw_currency: str = "CAD",
) -> DocumentRecord:
    return DocumentRecord(
        document_id=document_id,
        file_name=file_name,
        file_path=f"tests\\{file_name}",
        client_code=client_code,
        vendor=vendor,
        doc_type=doc_type,
        amount=amount,
        document_date=document_date,
        gl_account="Software Expense",
        tax_code="GST_QST",
        category="Software",
        review_status="Ready",
        confidence=0.95,
        raw_result=f'{{"raw_rules_output": {{"currency": "{raw_currency}"}}}}',
    )


def main():
    matcher = BankMatcher()

    documents = [
        make_doc(
            document_id="doc_amazon_1",
            file_name="amazon_1.pdf",
            client_code="SOUSSOL",
            vendor="Amazon.com.ca ULC",
            amount=106.77,
            document_date="2026-02-04",
            doc_type="invoice",
            raw_currency="CAD",
        ),
        make_doc(
            document_id="doc_ms_1",
            file_name="ms_1.pdf",
            client_code="SOUSSOL",
            vendor="Microsoft Canada Inc.",
            amount=28.14,
            document_date="2026-02-20",
            doc_type="invoice",
            raw_currency="CAD",
        ),
        make_doc(
            document_id="doc_openai_1",
            file_name="openai_1.pdf",
            client_code="SOUSSOL",
            vendor="OpenAI, LLC",
            amount=23.00,
            document_date="2025-12-21",
            doc_type="receipt",
            raw_currency="CAD",
        ),
    ]

    transactions = [
        BankTransaction(
            transaction_id="txn_1",
            client_code="SOUSSOL",
            account_id="rbc_visa_1",
            posted_date="2026-02-04",
            description="AMAZON AMAZON.CA",
            memo="Amazon order 702-7297903-6603467",
            amount=106.77,
            currency="CAD",
            source="credit_card",
            raw_data={},
        ),
        BankTransaction(
            transaction_id="txn_2",
            client_code="SOUSSOL",
            account_id="rbc_visa_1",
            posted_date="2026-02-20",
            description="MICROSOFT CANADA",
            memo="Power Automate monthly charge",
            amount=28.14,
            currency="CAD",
            source="credit_card",
            raw_data={},
        ),
        BankTransaction(
            transaction_id="txn_3",
            client_code="SOUSSOL",
            account_id="rbc_visa_1",
            posted_date="2025-12-21",
            description="OPENAI CHATGPT",
            memo="ChatGPT Plus subscription",
            amount=23.00,
            currency="CAD",
            source="credit_card",
            raw_data={},
        ),
        BankTransaction(
            transaction_id="txn_4",
            client_code="SOUSSOL",
            account_id="rbc_visa_1",
            posted_date="2026-02-04",
            description="AMAZON AMAZON.CA",
            memo="possible duplicate candidate",
            amount=106.77,
            currency="CAD",
            source="credit_card",
            raw_data={},
        ),
    ]

    results = matcher.match_documents(documents, transactions)

    print()
    print("BANK MATCHER TEST")
    print("=" * 80)

    for item in results:
        print(
            f"{item.document_id} | {item.transaction_id} | {item.status} | "
            f"score={item.score} | reasons={item.reasons}"
        )

    amazon_matches = [r for r in results if r.document_id == "doc_amazon_1"]
    ms_matches = [r for r in results if r.document_id == "doc_ms_1"]
    openai_matches = [r for r in results if r.document_id == "doc_openai_1"]

    assert len(amazon_matches) == 1
    assert amazon_matches[0].transaction_id is not None
    assert amazon_matches[0].status in {"matched", "suggested"}

    assert len(ms_matches) == 1
    assert ms_matches[0].transaction_id == "txn_2"
    assert ms_matches[0].status in {"matched", "suggested"}

    assert len(openai_matches) == 1
    assert openai_matches[0].transaction_id == "txn_3"
    assert openai_matches[0].status in {"matched", "suggested"}

    used_txns = [r.transaction_id for r in results if r.transaction_id]
    assert len(used_txns) == len(set(used_txns))

    print()
    print("PASS: bank matcher basic one-to-one matching works")


if __name__ == "__main__":
    main()