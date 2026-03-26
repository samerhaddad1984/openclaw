"""
RED TEAM: Accounting Substance vs Form Destruction Tests
=========================================================

Core question: Does LedgerLink understand what a transaction IS,
or just what it LOOKS LIKE?

These tests attack the economic substance analysis capabilities of:
- RulesEngine (document classification)
- GLMapper (GL account assignment)
- VendorIntelligenceEngine (category / document family)
- PostingBuilder (entry_kind, payload assembly)
- BankMatcher (transaction matching logic)

Each test creates a realistic document or bank scenario where FORM
(what the document looks like) differs from SUBSTANCE (what the
transaction actually is), and checks whether the system silently
misclassifies or flags the discrepancy.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pytest

from src.agents.tools.gl_mapper import GLMapper, GLMapResult
from src.agents.tools.vendor_intelligence import VendorIntelligenceEngine, VendorIntelResult
from src.agents.tools.rules_engine import RulesEngine, RulesResult
from src.agents.tools.posting_builder import (
    build_payload_from_sources,
    upsert_posting_job,
    ensure_posting_job_table_minimum,
    choose_default_memo,
)
from src.agents.tools.bank_matcher import BankMatcher
from src.agents.core.bank_models import BankTransaction, MatchCandidate, MatchResult
from src.agents.core.task_models import DocumentRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmp_rules_dir(gl_map: dict | None = None, vendor_intel: dict | None = None, vendors: dict | None = None) -> Path:
    """Create a temp rules directory with optional JSON config files."""
    d = Path(tempfile.mkdtemp())
    if gl_map is not None:
        (d / "gl_map.json").write_text(json.dumps(gl_map), encoding="utf-8")
    if vendor_intel is not None:
        (d / "vendor_intel.json").write_text(json.dumps(vendor_intel), encoding="utf-8")
    if vendors is not None:
        (d / "vendors.json").write_text(json.dumps(vendors), encoding="utf-8")
    return d


def _empty_rules_dir() -> Path:
    """Rules dir with no config files -- forces pure default fallback."""
    return Path(tempfile.mkdtemp())


def _in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_posting_job_table_minimum(conn)
    return conn


def _make_doc_record(
    *,
    document_id: str | None = None,
    vendor: str | None = None,
    doc_type: str | None = None,
    amount: float | None = None,
    document_date: str | None = None,
    client_code: str = "TEST",
    gl_account: str | None = None,
    tax_code: str | None = None,
    category: str | None = None,
    review_status: str = "NeedsReview",
    confidence: float = 0.0,
    raw_result: dict | None = None,
) -> DocumentRecord:
    return DocumentRecord(
        document_id=document_id or f"doc_{uuid.uuid4().hex[:8]}",
        file_name="test.pdf",
        file_path="/tmp/test.pdf",
        client_code=client_code,
        vendor=vendor,
        doc_type=doc_type,
        amount=amount,
        document_date=document_date,
        gl_account=gl_account,
        tax_code=tax_code,
        category=category,
        review_status=review_status,
        confidence=confidence,
        raw_result=raw_result or {},
    )


def _make_txn(
    *,
    transaction_id: str | None = None,
    client_code: str = "TEST",
    description: str = "",
    memo: str = "",
    amount: float | None = None,
    posted_date: str | None = None,
    currency: str = "CAD",
) -> BankTransaction:
    return BankTransaction(
        transaction_id=transaction_id or f"txn_{uuid.uuid4().hex[:8]}",
        client_code=client_code,
        account_id="acct_001",
        posted_date=posted_date,
        description=description,
        memo=memo,
        amount=amount,
        currency=currency,
    )


def _run_full_pipeline(doc_dict: dict[str, Any], rules_dir: Path | None = None):
    """
    Run a document dict through the full classification pipeline:
    RulesEngine -> GLMapper -> VendorIntelligence -> PostingBuilder payload.
    Returns a dict with all results for assertion.
    """
    rd = rules_dir or _empty_rules_dir()
    rules = RulesEngine(rd)
    gl = GLMapper(rd)
    vi = VendorIntelligenceEngine(rd)

    text = doc_dict.get("text", "")
    rules_result = rules.run(text)

    vendor = rules_result.vendor_name or doc_dict.get("vendor")
    doc_type = rules_result.doc_type or doc_dict.get("doc_type")

    gl_result = gl.map(vendor, doc_type)
    vi_result = vi.classify(vendor, doc_type)

    # Build a posting payload
    merged_doc = {
        "document_id": doc_dict.get("document_id", f"doc_{uuid.uuid4().hex[:8]}"),
        "vendor": vendor,
        "doc_type": doc_type,
        "amount": rules_result.total or doc_dict.get("amount"),
        "document_date": rules_result.document_date or doc_dict.get("document_date"),
        "gl_account": gl_result.gl_account,
        "tax_code": gl_result.tax_code,
        "category": vi_result.category,
        "client_code": doc_dict.get("client_code", "TEST"),
        "confidence": rules_result.confidence,
        "file_name": doc_dict.get("file_name", "test.pdf"),
        "memo": doc_dict.get("memo", ""),
    }

    conn = _in_memory_db()
    posting = upsert_posting_job(conn, document=merged_doc)
    conn.close()

    return {
        "rules": rules_result,
        "gl": gl_result,
        "vi": vi_result,
        "posting": posting,
        "merged_doc": merged_doc,
    }


# =========================================================================
# SECTION 1: Economic Substance vs Form — Classification Tests (1-20)
# =========================================================================

class TestSubstanceVsForm:
    """Each test sends a document where FORM != SUBSTANCE and checks
    whether the system misclassifies or flags the discrepancy."""

    # ------------------------------------------------------------------
    # 1. Loan proceeds recorded as revenue
    # ------------------------------------------------------------------
    def test_01_loan_proceeds_as_revenue(self):
        """Bank deposit labeled 'loan disbursement' but vendor/amount looks
        like a customer payment.  Should NOT be revenue."""
        result = _run_full_pipeline({
            "text": "Deposit from BDC Business Development Bank\n"
                    "Loan Disbursement - Term Loan #TL-2025-4488\n"
                    "Amount: $50,000.00\nDate: 2025-11-01\n"
                    "Reference: Wire Transfer",
            "vendor": "BDC",
            "doc_type": "bank_deposit",
            "amount": 50000.00,
            "memo": "Loan disbursement - BDC term loan",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category
        entry_kind = result["posting"].get("entry_kind", "expense")

        # The system should NOT classify a loan as revenue or sales
        substance_ok = True
        issues = []
        if "revenue" in gl.lower() or "sales" in gl.lower() or "income" in gl.lower():
            issues.append(f"GL account '{gl}' suggests revenue -- loan is a LIABILITY")
            substance_ok = False
        if "revenue" in category.lower() or "sales" in category.lower():
            issues.append(f"Category '{category}' suggests revenue -- loan is a LIABILITY")
            substance_ok = False
        if entry_kind == "income":
            issues.append(f"entry_kind='{entry_kind}' -- loan proceeds are NOT income")
            substance_ok = False

        # FINDING: record what happened
        if not substance_ok:
            pytest.fail(
                f"SUBSTANCE FAILURE: Loan proceeds misclassified as revenue/income.\n"
                f"Issues: {issues}\n"
                f"GL={gl}, Category={category}, entry_kind={entry_kind}"
            )
        # If it fell through to Uncategorized, that is acceptable (better than wrong)
        assert True, f"System assigned GL={gl}, Category={category} -- not revenue (acceptable)"

    # ------------------------------------------------------------------
    # 2. Owner contribution recorded as customer payment
    # ------------------------------------------------------------------
    def test_02_owner_contribution_as_payment(self):
        """Owner deposits personal funds into business account."""
        result = _run_full_pipeline({
            "text": "Deposit - Owner Capital Contribution\n"
                    "From: Samer Haddad (personal account)\n"
                    "Amount: $15,000.00\nDate: 2025-10-15\n"
                    "Memo: Owner equity injection",
            "vendor": "Samer Haddad",
            "doc_type": "bank_deposit",
            "amount": 15000.00,
            "memo": "Owner capital contribution",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category

        issues = []
        if "revenue" in gl.lower() or "sales" in gl.lower() or "accounts receivable" in gl.lower():
            issues.append(f"GL '{gl}' treats owner contribution as customer payment")
        if "revenue" in category.lower() or "sales" in category.lower():
            issues.append(f"Category '{category}' treats owner contribution as income")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Owner contribution misclassified.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 3. Customer deposit recorded as earned revenue
    # ------------------------------------------------------------------
    def test_03_advance_payment_as_earned_revenue(self):
        """Advance payment received before service delivery.
        Should be deferred revenue / unearned revenue, not income."""
        result = _run_full_pipeline({
            "text": "Customer Deposit - Project #P-2025-112\n"
                    "Client: ABC Construction\n"
                    "Deposit Amount: $8,500.00\nDate: 2025-09-20\n"
                    "Note: 50% advance payment, work to begin January 2026",
            "vendor": "ABC Construction",
            "doc_type": "customer_deposit",
            "amount": 8500.00,
            "memo": "50% advance - project not yet started",
        })
        gl = result["gl"].gl_account
        # Acceptable: deferred revenue, unearned revenue, customer deposits, liability
        # Not acceptable: revenue, sales, income (earned)
        issues = []
        if "revenue" in gl.lower() and "deferred" not in gl.lower() and "unearned" not in gl.lower():
            issues.append(f"GL '{gl}' records advance as earned revenue")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Advance payment recorded as earned revenue.\n"
                f"Issues: {issues}\nGL={gl}"
            )

    # ------------------------------------------------------------------
    # 4. Prepaid insurance expensed immediately
    # ------------------------------------------------------------------
    def test_04_prepaid_insurance_expensed_immediately(self):
        """12-month insurance premium paid upfront. Should be prepaid asset,
        not immediate expense."""
        result = _run_full_pipeline({
            "text": "Intact Insurance - Annual Commercial Policy\n"
                    "Policy Period: Jan 1, 2026 - Dec 31, 2026\n"
                    "Annual Premium: $6,200.00\nPaid: December 15, 2025\n"
                    "Policy #INS-2026-445",
            "vendor": "Intact Insurance",
            "doc_type": "invoice",
            "amount": 6200.00,
            "memo": "Annual insurance premium - 12 months prepaid",
        })
        # Check the posting GL (substance classifier overrides uncategorized GL)
        gl = result["posting"].get("gl_account", result["gl"].gl_account)
        entry_kind = result["posting"].get("entry_kind", "expense")

        # The system should ideally flag this as prepaid, not expense
        # At minimum, record what it does
        issues = []
        if "expense" in gl.lower() and "prepaid" not in gl.lower():
            issues.append(
                f"GL '{gl}' expenses the full annual premium immediately. "
                f"Should be Prepaid Insurance (asset) amortized over 12 months."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Prepaid insurance expensed immediately.\n"
                f"Issues: {issues}\nGL={gl}, entry_kind={entry_kind}"
            )

    # ------------------------------------------------------------------
    # 5. Equipment purchase classified as repair expense (capex vs opex)
    # ------------------------------------------------------------------
    def test_05_capex_classified_as_opex(self):
        """$12,000 equipment purchase should be capitalized, not expensed."""
        result = _run_full_pipeline({
            "text": "Dell Technologies - Invoice #INV-88421\n"
                    "PowerEdge R750xs Server\n"
                    "Qty: 1   Unit Price: $11,500.00\n"
                    "Tax: $500.00\nTotal: $12,000.00\n"
                    "Date: 2025-11-20\n"
                    "Asset Tag: SRV-2025-014",
            "vendor": "Dell Technologies",
            "doc_type": "invoice",
            "amount": 12000.00,
            "memo": "Server purchase - data center",
        })
        # Check the posting GL (substance classifier overrides uncategorized GL)
        gl = result["posting"].get("gl_account", result["gl"].gl_account)
        category = result["vi"].category

        issues = []
        if "repair" in gl.lower() or "maintenance" in gl.lower():
            issues.append(f"GL '{gl}' classifies $12K equipment as repair/maintenance")
        if "expense" in gl.lower() and "capital" not in gl.lower() and "equipment" not in gl.lower() and "asset" not in gl.lower():
            issues.append(f"GL '{gl}' expenses equipment that should be capitalized")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Capital equipment classified as operating expense.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 6. Shareholder personal expense in business spend
    # ------------------------------------------------------------------
    def test_06_shareholder_personal_expense(self):
        """Personal vacation charged to business credit card."""
        result = _run_full_pipeline({
            "text": "Air Canada - Booking Confirmation\n"
                    "Passenger: Samer Haddad\n"
                    "Route: Montreal - Cancun (Return)\n"
                    "Travel Date: Dec 20, 2025 - Jan 3, 2026\n"
                    "Total: $2,450.00\nBooking Ref: AC-VACA-2025",
            "vendor": "Air Canada",
            "doc_type": "receipt",
            "amount": 2450.00,
            "memo": "Personal vacation flight - shareholder",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category

        # System should either flag as personal / shareholder loan or at least
        # not silently classify as business travel expense
        issues = []
        if "travel" in gl.lower() or "transport" in gl.lower():
            issues.append(
                f"GL '{gl}' silently classifies personal vacation as business travel. "
                f"Should flag as shareholder loan or personal expense."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Personal expense mixed into business.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 7. Shareholder loan repayment mistaken for income
    # ------------------------------------------------------------------
    def test_07_shareholder_loan_repayment_as_income(self):
        """Company repays shareholder loan. Should reduce liability, not be income."""
        result = _run_full_pipeline({
            "text": "Payment to Shareholder - Loan Repayment\n"
                    "Payee: Samer Haddad\n"
                    "Amount: $5,000.00\nDate: 2025-10-30\n"
                    "Reference: Shareholder Loan Account #SHL-001\n"
                    "Balance remaining: $20,000.00",
            "vendor": "Samer Haddad",
            "doc_type": "payment",
            "amount": 5000.00,
            "memo": "Shareholder loan repayment",
        })
        gl = result["gl"].gl_account
        entry_kind = result["posting"].get("entry_kind", "expense")

        issues = []
        if "income" in gl.lower() or "revenue" in gl.lower():
            issues.append(f"GL '{gl}' treats loan repayment as income")
        # A loan repayment to shareholder is a balance sheet transaction
        # (reduce liability), not an operating expense either
        if "operating" in gl.lower():
            issues.append(f"GL '{gl}' treats loan repayment as operating expense")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Shareholder loan repayment misclassified.\n"
                f"Issues: {issues}\nGL={gl}, entry_kind={entry_kind}"
            )

    # ------------------------------------------------------------------
    # 8. Employee reimbursement mistaken for expense
    # ------------------------------------------------------------------
    def test_08_employee_reimbursement_as_expense(self):
        """Employee reimbursement should reduce the AP/reimbursable balance,
        not double-count the expense."""
        result = _run_full_pipeline({
            "text": "Employee Expense Reimbursement\n"
                    "Employee: Jean-Pierre Tremblay\n"
                    "Expense Report #ER-2025-089\n"
                    "Meals: $120.00\nMileage: $85.00\nParking: $25.00\n"
                    "Total Reimbursement: $230.00\nDate: 2025-11-05\n"
                    "Approved by: Manager",
            "vendor": "Jean-Pierre Tremblay",
            "doc_type": "expense_report",
            "amount": 230.00,
            "memo": "Employee expense reimbursement",
        })
        gl = result["gl"].gl_account
        entry_kind = result["posting"].get("entry_kind", "expense")

        # The reimbursement PAYMENT is not a new expense -- the expense was
        # already recorded when the employee submitted the report.
        # The payment is a balance sheet transaction.
        issues = []
        # We check if the system recognizes this or just blindly expenses it
        # Acceptable: AP reduction, reimbursement clearing account
        # Problematic: double-counting as meals/mileage/parking expense
        if entry_kind == "expense" and "uncategorized" not in gl.lower():
            issues.append(
                f"entry_kind='{entry_kind}' with GL '{gl}' may double-count "
                f"the expense that was already recorded on the expense report"
            )

        # This is a known weakness -- record the finding
        if issues:
            pytest.fail(
                f"SUBSTANCE WARNING: Employee reimbursement may be double-counted.\n"
                f"Issues: {issues}\nGL={gl}, entry_kind={entry_kind}"
            )

    # ------------------------------------------------------------------
    # 9. Payroll remittance (to CRA) mistaken for payroll expense
    # ------------------------------------------------------------------
    def test_09_payroll_remittance_as_payroll_expense(self):
        """Payment to CRA for source deductions is NOT payroll expense --
        it's clearing of a payroll liability."""
        result = _run_full_pipeline({
            "text": "Canada Revenue Agency - Payroll Remittance\n"
                    "Business Number: 123456789RP0001\n"
                    "Period: October 2025\n"
                    "CPP Employer: $1,200.00\nEI Employer: $450.00\n"
                    "Employee Tax Withheld: $3,800.00\n"
                    "Total Remittance: $5,450.00\nDue Date: 2025-11-15",
            "vendor": "Canada Revenue Agency",
            "doc_type": "remittance",
            "amount": 5450.00,
            "memo": "CRA payroll remittance - source deductions",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category

        issues = []
        if "payroll" in gl.lower() and "expense" in gl.lower():
            issues.append(
                f"GL '{gl}' classifies CRA remittance as payroll expense. "
                f"Employee tax withheld ($3,800) is a LIABILITY clearing, not expense."
            )
        if "salary" in category.lower() or "wage" in category.lower():
            issues.append(f"Category '{category}' treats remittance as salary/wage expense")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Payroll remittance to CRA misclassified as payroll expense.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 10. Contractor invoice mistaken for payroll
    # ------------------------------------------------------------------
    def test_10_contractor_invoice_as_payroll(self):
        """Independent contractor invoice should be professional fees /
        subcontractor expense, not payroll."""
        result = _run_full_pipeline({
            "text": "Invoice from: Pierre Consulting SENC\n"
                    "NEQ: 1234567890\n"
                    "Services: IT consulting - November 2025\n"
                    "Hours: 80  Rate: $125/hr\n"
                    "Subtotal: $10,000.00\nTPS: $500.00\nTVQ: $997.50\n"
                    "Total: $11,497.50\nDate: 2025-12-01\n"
                    "Payment Terms: Net 30",
            "vendor": "Pierre Consulting SENC",
            "doc_type": "invoice",
            "amount": 11497.50,
            "memo": "IT consulting services",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category

        issues = []
        if "payroll" in gl.lower() or "salary" in gl.lower() or "wage" in gl.lower():
            issues.append(
                f"GL '{gl}' classifies contractor invoice as payroll. "
                f"Contractors are NOT employees."
            )
        if "payroll" in category.lower() or "salary" in category.lower():
            issues.append(f"Category '{category}' treats contractor as payroll")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Contractor invoice classified as payroll.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 11. Credit note mistaken for new bill
    # ------------------------------------------------------------------
    def test_11_credit_note_as_new_bill(self):
        """Credit note should REDUCE AP, not create a new payable."""
        result = _run_full_pipeline({
            "text": "CREDIT NOTE\nCredit Note #CN-2025-0034\n"
                    "From: Staples Business Advantage\n"
                    "Original Invoice: INV-2025-8812\n"
                    "Reason: Defective merchandise returned\n"
                    "Credit Amount: -$345.00\nDate: 2025-11-10\n"
                    "GST Credit: -$17.25\nQST Credit: -$34.41",
            "vendor": "Staples Business Advantage",
            "doc_type": "credit_note",
            "amount": -345.00,
            "memo": "Credit note - returned merchandise",
        })
        gl = result["gl"].gl_account
        entry_kind = result["posting"].get("entry_kind", "expense")

        issues = []
        if entry_kind == "expense":
            issues.append(
                f"entry_kind='{entry_kind}' for a credit note. "
                f"Credit note should reduce AP, not create new expense."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Credit note treated as new bill/expense.\n"
                f"Issues: {issues}\nGL={gl}, entry_kind={entry_kind}"
            )

    # ------------------------------------------------------------------
    # 12. Refund mistaken for revenue reduction vs expense recovery
    # ------------------------------------------------------------------
    def test_12_refund_as_revenue_reduction(self):
        """Vendor refund should reduce the original expense account,
        not be recorded as revenue."""
        result = _run_full_pipeline({
            "text": "REFUND NOTICE\nFrom: Amazon Business\n"
                    "Order #113-4455667-8899001\n"
                    "Refund Amount: $189.99\nDate: 2025-11-08\n"
                    "Reason: Item not as described\n"
                    "Refund to: Visa ending 4532",
            "vendor": "Amazon Business",
            "doc_type": "refund",
            "amount": 189.99,
            "memo": "Vendor refund - returned item",
        })
        gl = result["gl"].gl_account
        entry_kind = result["posting"].get("entry_kind", "expense")

        issues = []
        if "revenue" in gl.lower() or "income" in gl.lower() or "sales" in gl.lower():
            issues.append(
                f"GL '{gl}' records vendor refund as revenue/income. "
                f"Should reduce the original expense account."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Vendor refund misclassified as revenue.\n"
                f"Issues: {issues}\nGL={gl}, entry_kind={entry_kind}"
            )

    # ------------------------------------------------------------------
    # 13. Intercompany transfer mistaken for external revenue/expense
    # ------------------------------------------------------------------
    def test_13_intercompany_transfer_as_external(self):
        """Transfer between related entities should be intercompany,
        not revenue or expense."""
        result = _run_full_pipeline({
            "text": "Intercompany Transfer\n"
                    "From: Basement Systems Quebec Inc.\n"
                    "To: BSQ Property Holdings Inc.\n"
                    "Amount: $25,000.00\nDate: 2025-11-12\n"
                    "Reference: IC-2025-0045\n"
                    "Purpose: Monthly management fee allocation",
            "vendor": "BSQ Property Holdings Inc.",
            "doc_type": "transfer",
            "amount": 25000.00,
            "memo": "Intercompany management fee allocation",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category

        issues = []
        if "revenue" in gl.lower() or "sales" in gl.lower():
            issues.append(f"GL '{gl}' treats intercompany transfer as revenue")
        if "operating expense" in gl.lower():
            issues.append(f"GL '{gl}' treats intercompany transfer as operating expense")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Intercompany transfer classified as external transaction.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 14. Cash withdrawal classified as operating expense
    # ------------------------------------------------------------------
    def test_14_cash_withdrawal_as_operating_expense(self):
        """ATM withdrawal should be shareholder draw or petty cash, not expense."""
        result = _run_full_pipeline({
            "text": "ATM Withdrawal\nRBC Royal Bank - Branch 04271\n"
                    "Amount: $500.00\nDate: 2025-11-15\n"
                    "Account: Business Chequing ***8834\n"
                    "ATM Fee: $3.50",
            "vendor": "RBC Royal Bank",
            "doc_type": "bank_withdrawal",
            "amount": 500.00,
            "memo": "ATM cash withdrawal",
        })
        gl = result["gl"].gl_account
        category = result["vi"].category

        issues = []
        if "expense" in gl.lower() and "bank" not in gl.lower() and "uncategorized" not in gl.lower():
            issues.append(
                f"GL '{gl}' classifies cash withdrawal as specific operating expense. "
                f"Should be shareholder draw, petty cash, or ask for clarification."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Cash withdrawal classified as operating expense.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 15. Financing payment - principal vs interest not separated
    # ------------------------------------------------------------------
    def test_15_loan_payment_principal_interest_not_split(self):
        """Loan payment includes both principal and interest.
        System should flag that these need to be split."""
        result = _run_full_pipeline({
            "text": "BDC Loan Payment - Monthly Installment\n"
                    "Loan #TL-2025-4488\n"
                    "Monthly Payment: $2,150.00\n"
                    "  Principal: $1,800.00\n"
                    "  Interest: $350.00\n"
                    "Date: 2025-11-01\n"
                    "Remaining Balance: $48,200.00",
            "vendor": "BDC",
            "doc_type": "loan_payment",
            "amount": 2150.00,
            "memo": "Monthly loan payment - principal $1800 + interest $350",
        })
        # Check the posting GL (substance classifier overrides uncategorized GL)
        gl = result["posting"].get("gl_account", result["gl"].gl_account)
        entry_kind = result["posting"].get("entry_kind", "expense")

        issues = []
        # If the system books the full $2,150 to a single account, it fails
        # to separate principal (balance sheet) from interest (expense)
        if "interest" not in gl.lower() and "loan" not in gl.lower() and "2500" not in gl:
            issues.append(
                f"GL '{gl}' does not recognize this as a loan payment. "
                f"Principal ($1,800) should reduce liability; "
                f"interest ($350) is an expense."
            )
        # Even if it picks "interest expense", $2,150 to interest is wrong
        if "interest" in gl.lower() and "expense" in gl.lower():
            issues.append(
                f"GL '{gl}' books entire $2,150 as interest expense. "
                f"Only $350 is interest; $1,800 is principal repayment."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Loan payment not split into principal/interest.\n"
                f"Issues: {issues}\nGL={gl}, entry_kind={entry_kind}"
            )

    # ------------------------------------------------------------------
    # 16. Sales tax receivable/payable confused with expense/income
    # ------------------------------------------------------------------
    def test_16_sales_tax_as_expense_or_income(self):
        """GST/QST collected or paid should go to tax accounts,
        not revenue or expense."""
        result = _run_full_pipeline({
            "text": "Revenu Quebec - GST/QST Remittance\n"
                    "Period: Q3 2025\n"
                    "GST Collected: $4,500.00\nGST Paid (ITC): $2,100.00\n"
                    "GST Net Owing: $2,400.00\n"
                    "QST Collected: $8,966.25\nQST Paid (ITR): $4,185.00\n"
                    "QST Net Owing: $4,781.25\n"
                    "Total Remittance: $7,181.25\nDue: 2025-10-31",
            "vendor": "Revenu Quebec",
            "doc_type": "tax_remittance",
            "amount": 7181.25,
            "memo": "Quarterly GST/QST remittance",
        })
        # Check the posting GL (substance classifier overrides uncategorized GL)
        gl = result["posting"].get("gl_account", result["gl"].gl_account)
        category = result["vi"].category

        issues = []
        if "expense" in gl.lower() and "tax" not in gl.lower():
            issues.append(f"GL '{gl}' treats tax remittance as operating expense")
        if "revenue" in gl.lower() or "income" in gl.lower():
            issues.append(f"GL '{gl}' treats tax remittance as revenue/income")

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Sales tax remittance misclassified.\n"
                f"Issues: {issues}\nGL={gl}, Category={category}"
            )

    # ------------------------------------------------------------------
    # 17. Deposit from related party (loan) treated as sales
    # ------------------------------------------------------------------
    def test_17_related_party_loan_as_sales(self):
        """Loan from a related company should be a liability, not sales."""
        result = _run_full_pipeline({
            "text": "Wire Transfer Received\n"
                    "From: BSQ Property Holdings Inc.\n"
                    "Amount: $30,000.00\nDate: 2025-11-18\n"
                    "Reference: Related party loan\n"
                    "Memo: Short-term loan to cover payroll",
            "vendor": "BSQ Property Holdings Inc.",
            "doc_type": "bank_deposit",
            "amount": 30000.00,
            "memo": "Related party loan - payroll coverage",
        })
        gl = result["gl"].gl_account

        issues = []
        if "sales" in gl.lower() or "revenue" in gl.lower() or "income" in gl.lower():
            issues.append(
                f"GL '{gl}' records related-party loan as sales/revenue. "
                f"This is a LIABILITY."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Related party loan treated as sales.\n"
                f"Issues: {issues}\nGL={gl}"
            )

    # ------------------------------------------------------------------
    # 18. Prepaid rent recorded as current expense
    # ------------------------------------------------------------------
    def test_18_prepaid_rent_as_current_expense(self):
        """Rent paid 3 months in advance should be prepaid, not all expensed now."""
        result = _run_full_pipeline({
            "text": "Rent Payment - 3 Months Advance\n"
                    "Landlord: Immeubles Capitale Inc.\n"
                    "Period: January - March 2026\n"
                    "Monthly Rent: $4,500.00\n"
                    "Total Prepaid: $13,500.00\nPaid: December 1, 2025\n"
                    "Property: 1500 rue St-Denis, Montreal",
            "vendor": "Immeubles Capitale Inc.",
            "doc_type": "invoice",
            "amount": 13500.00,
            "memo": "3 months prepaid rent Jan-Mar 2026",
        })
        gl = result["gl"].gl_account

        issues = []
        if "rent" in gl.lower() and "expense" in gl.lower() and "prepaid" not in gl.lower():
            issues.append(
                f"GL '{gl}' expenses 3 months of future rent immediately. "
                f"Should be Prepaid Rent (asset) amortized over 3 months."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Prepaid rent expensed immediately.\n"
                f"Issues: {issues}\nGL={gl}"
            )

    # ------------------------------------------------------------------
    # 19. Security deposit recorded as expense (should be asset)
    # ------------------------------------------------------------------
    def test_19_security_deposit_as_expense(self):
        """Security deposit is a recoverable asset, not an expense."""
        result = _run_full_pipeline({
            "text": "Security Deposit Payment\n"
                    "Landlord: Groupe Immobilier Laval\n"
                    "Amount: $9,000.00 (2 months rent)\n"
                    "Date: 2025-12-01\n"
                    "Refundable upon lease termination\n"
                    "Lease #L-2025-0078",
            "vendor": "Groupe Immobilier Laval",
            "doc_type": "receipt",
            "amount": 9000.00,
            "memo": "Refundable security deposit",
        })
        # Check the posting GL (substance classifier overrides uncategorized GL)
        gl = result["posting"].get("gl_account", result["gl"].gl_account)

        issues = []
        if "expense" in gl.lower() and "deposit" not in gl.lower():
            issues.append(
                f"GL '{gl}' expenses a refundable security deposit. "
                f"Should be Security Deposits (Other Asset)."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE FAILURE: Security deposit classified as expense.\n"
                f"Issues: {issues}\nGL={gl}"
            )

    # ------------------------------------------------------------------
    # 20. Gift card purchase - should be prepaid, not expense
    # ------------------------------------------------------------------
    def test_20_gift_card_as_expense(self):
        """Gift card purchase is prepaid/stored value until used."""
        result = _run_full_pipeline({
            "text": "Amazon Business - Gift Card Purchase\n"
                    "Order #GC-2025-11220\n"
                    "Qty: 10 x $50.00 Gift Cards\n"
                    "Total: $500.00\nDate: 2025-12-10\n"
                    "Purpose: Employee holiday gifts",
            "vendor": "Amazon Business",
            "doc_type": "receipt",
            "amount": 500.00,
            "memo": "Gift cards for employee gifts",
        })
        # Check the posting GL (substance classifier overrides uncategorized GL)
        gl = result["posting"].get("gl_account", result["gl"].gl_account)

        # Gift cards could legitimately be expensed if for employee gifts
        # (benefits expense). But the system should recognize the substance.
        # We record the finding either way.
        issues = []
        if "uncategorized" in gl.lower():
            issues.append(
                f"GL '{gl}' -- system did not categorize gift card purchase at all. "
                f"Should be either Employee Benefits expense (if gifts) or Prepaid."
            )

        if issues:
            pytest.fail(
                f"SUBSTANCE NOTE: Gift card classification: {issues}\nGL={gl}"
            )


# =========================================================================
# SECTION 2: Bank Matcher Attacks (21-27)
# =========================================================================

class TestBankMatcherSubstance:
    """Attack the BankMatcher with scenarios where naive matching fails."""

    def setup_method(self):
        self.matcher = BankMatcher()

    # ------------------------------------------------------------------
    # 21. One payment matching multiple invoices
    # ------------------------------------------------------------------
    def test_21_one_payment_multiple_invoices(self):
        """Single $3,000 payment covers three invoices ($1000 each).
        Matcher uses 1:1 logic -- should flag, not silently match to one."""
        inv1 = _make_doc_record(document_id="inv_001", vendor="Acme Corp", amount=1000.00, document_date="2025-11-01")
        inv2 = _make_doc_record(document_id="inv_002", vendor="Acme Corp", amount=1000.00, document_date="2025-11-05")
        inv3 = _make_doc_record(document_id="inv_003", vendor="Acme Corp", amount=1000.00, document_date="2025-11-10")

        payment = _make_txn(
            transaction_id="txn_batch",
            description="Acme Corp",
            amount=3000.00,
            posted_date="2025-11-15",
        )

        results = self.matcher.match_documents([inv1, inv2, inv3], [payment])

        matched = [r for r in results if r.status == "matched"]
        suggested = [r for r in results if r.status == "suggested"]
        unmatched = [r for r in results if r.status in ("unmatched", "ambiguous")]

        # The $3000 payment does NOT match any single $1000 invoice
        # If it matches one, that's wrong (amount diff is $2000)
        issues = []
        if len(matched) > 0:
            issues.append(
                f"{len(matched)} invoice(s) auto-matched to a $3000 payment "
                f"when each invoice is only $1000. This is a batch payment "
                f"that the system cannot handle with 1:1 matching."
            )

        if issues:
            pytest.fail(f"BANK MATCHER FAILURE: {issues}")

        # All three should be unmatched since no single invoice matches
        assert len(matched) == 0, "No invoice should auto-match a batch payment"

    # ------------------------------------------------------------------
    # 22. Overpayment scenario
    # ------------------------------------------------------------------
    def test_22_overpayment(self):
        """Customer pays $1,100 on a $1,000 invoice. $100 overpayment."""
        invoice = _make_doc_record(
            document_id="inv_over",
            vendor="Client ABC",
            amount=1000.00,
            document_date="2025-11-01",
        )

        payment = _make_txn(
            transaction_id="txn_over",
            description="Client ABC",
            amount=1100.00,
            posted_date="2025-11-05",
        )

        results = self.matcher.match_documents([invoice], [payment])
        result = results[0]

        # $100 diff on $1000 = 10% divergence
        issues = []
        if result.status == "matched":
            issues.append(
                f"Auto-matched despite $100 overpayment (10% divergence). "
                f"Should flag for review -- overpayment needs to be recorded "
                f"as customer credit or refunded."
            )

        if issues:
            pytest.fail(f"BANK MATCHER FAILURE: {issues}")

        # Should be suggested or have divergence flag
        assert result.status != "matched" or "amount_divergence_over_10pct" in result.reasons, \
            "Overpayment should be flagged"

    # ------------------------------------------------------------------
    # 23. Underpayment with bank fee deduction
    # ------------------------------------------------------------------
    def test_23_underpayment_bank_fee(self):
        """Payment of $970 on $1,000 invoice -- bank deducted $30 fee."""
        invoice = _make_doc_record(
            document_id="inv_under",
            vendor="Supplier XYZ",
            amount=1000.00,
            document_date="2025-11-01",
        )

        payment = _make_txn(
            transaction_id="txn_under",
            description="Supplier XYZ",
            amount=970.00,
            posted_date="2025-11-03",
        )

        candidate = self.matcher.evaluate_candidate(invoice, payment)

        # $30 difference -- within tolerance but should note it
        issues = []
        if candidate is not None and candidate.status == "matched":
            if "amount_divergence" not in " ".join(candidate.reasons):
                issues.append(
                    "Auto-matched $970 to $1000 without noting the $30 discrepancy. "
                    "Bank fee deduction needs separate recording."
                )

        if issues:
            pytest.fail(f"BANK MATCHER FAILURE: {issues}")

    # ------------------------------------------------------------------
    # 24. Same-amount invoices from different vendors
    # ------------------------------------------------------------------
    def test_24_same_amount_different_vendors(self):
        """Two invoices for $500 from different vendors. One payment from Vendor A.
        Should match Vendor A's invoice, not Vendor B's."""
        inv_a = _make_doc_record(
            document_id="inv_a",
            vendor="Vendor Alpha",
            amount=500.00,
            document_date="2025-11-01",
        )
        inv_b = _make_doc_record(
            document_id="inv_b",
            vendor="Vendor Beta",
            amount=500.00,
            document_date="2025-11-01",
        )

        payment = _make_txn(
            transaction_id="txn_alpha",
            description="Vendor Alpha payment",
            amount=500.00,
            posted_date="2025-11-01",
        )

        results = self.matcher.match_documents([inv_a, inv_b], [payment])

        # Find which document got matched
        matched_results = [r for r in results if r.status in ("matched", "suggested")]
        alpha_match = [r for r in matched_results if r.document_id == "inv_a"]
        beta_match = [r for r in matched_results if r.document_id == "inv_b"]

        issues = []
        if beta_match and not alpha_match:
            issues.append(
                "Matched Vendor Beta's invoice instead of Vendor Alpha's. "
                "Payment description clearly says 'Vendor Alpha'."
            )

        # Check for ambiguity detection
        ambiguous = [r for r in results if r.status == "ambiguous"]
        if ambiguous:
            # Ambiguous is actually a reasonable outcome if vendor matching isn't strong
            pass

        if issues:
            pytest.fail(f"BANK MATCHER FAILURE: {issues}")

        # Alpha should be matched/suggested, not Beta
        if alpha_match:
            assert alpha_match[0].score >= (beta_match[0].score if beta_match else 0), \
                "Vendor Alpha should score higher than Vendor Beta"

    # ------------------------------------------------------------------
    # 25. Cross-client contamination
    # ------------------------------------------------------------------
    def test_25_cross_client_contamination(self):
        """Same vendor and amount in different client codes.
        Must NOT match across clients."""
        doc_client_a = _make_doc_record(
            document_id="inv_cli_a",
            vendor="Bell Canada",
            amount=250.00,
            document_date="2025-11-01",
            client_code="CLIENT_A",
        )

        txn_client_b = _make_txn(
            transaction_id="txn_cli_b",
            client_code="CLIENT_B",
            description="Bell Canada",
            amount=250.00,
            posted_date="2025-11-01",
        )

        candidate = self.matcher.evaluate_candidate(doc_client_a, txn_client_b)

        # Client gate should block this
        assert candidate is None, (
            "CROSS-CLIENT CONTAMINATION: Matched document from CLIENT_A "
            "to transaction from CLIENT_B. Client isolation is broken."
        )

    # ------------------------------------------------------------------
    # 26. Payment reference says INV-100 but amount matches INV-200
    # ------------------------------------------------------------------
    def test_26_reference_amount_conflict(self):
        """Payment memo says INV-100 but the amount matches INV-200.
        System should flag the conflict, not silently match by amount."""
        inv_100 = _make_doc_record(
            document_id="INV-100",
            vendor="Fournisseur ABC",
            amount=750.00,
            document_date="2025-11-01",
        )
        inv_200 = _make_doc_record(
            document_id="INV-200",
            vendor="Fournisseur ABC",
            amount=1200.00,
            document_date="2025-11-05",
        )

        payment = _make_txn(
            transaction_id="txn_ref_conflict",
            description="Fournisseur ABC",
            memo="Payment for INV-100",
            amount=1200.00,  # Matches INV-200's amount, not INV-100
            posted_date="2025-11-10",
        )

        results = self.matcher.match_documents([inv_100, inv_200], [payment])

        # The system has no invoice-reference parsing -- it matches by amount.
        # INV-200 will likely match because the amount is exact.
        # This is a known gap: the system ignores payment references.
        matched_200 = [r for r in results if r.document_id == "INV-200" and r.status in ("matched", "suggested")]

        issues = []
        if matched_200:
            issues.append(
                "System matched by amount to INV-200, ignoring the payment memo "
                "that explicitly references INV-100. Reference parsing is not implemented."
            )

        # Record finding but don't fail -- this is a design limitation
        if issues:
            pytest.fail(
                f"BANK MATCHER GAP: {issues}\n"
                f"The system has no invoice reference parsing capability."
            )

    # ------------------------------------------------------------------
    # 27. Transfer between own bank accounts classified as expense/revenue
    # ------------------------------------------------------------------
    def test_27_own_account_transfer_as_expense(self):
        """Transfer from business chequing to business savings.
        Should be recognized as internal transfer, not expense/revenue."""
        # Outgoing side (from chequing)
        doc_out = _make_doc_record(
            document_id="xfer_out",
            vendor="RBC Transfer",
            doc_type="bank_transfer",
            amount=10000.00,
            document_date="2025-11-20",
        )

        # Incoming side (to savings) -- same client, same date, same amount
        txn_in = _make_txn(
            transaction_id="txn_savings_in",
            description="Transfer from Chequing",
            amount=10000.00,
            posted_date="2025-11-20",
        )

        candidate = self.matcher.evaluate_candidate(doc_out, txn_in)

        # If it matches, the combined effect is: the outgoing is an "expense"
        # and the incoming is "income" -- double-counting.
        # Internal transfers should net to zero.
        issues = []
        if candidate is not None and candidate.status == "matched":
            issues.append(
                "Internal bank transfer matched as a regular transaction. "
                "Both sides will be recorded, creating phantom expense and income. "
                "System needs transfer detection logic."
            )

        if issues:
            pytest.fail(
                f"BANK MATCHER FAILURE: Internal transfer not detected.\n"
                f"Issues: {issues}"
            )


# =========================================================================
# SECTION 3: Pipeline Integration -- Default Fallback Behavior
# =========================================================================

class TestDefaultFallbackBehavior:
    """When rules have no specific vendor/doc_type match, verify the system
    defaults are safe (Uncategorized) rather than dangerously specific."""

    def test_unknown_vendor_gets_uncategorized(self):
        """A vendor the system has never seen should get Uncategorized, not
        a random specific GL account."""
        rd = _empty_rules_dir()
        gl = GLMapper(rd)
        vi = VendorIntelligenceEngine(rd)

        gl_result = gl.map("Completely Unknown Vendor XYZ123", "invoice")
        vi_result = vi.classify("Completely Unknown Vendor XYZ123", "invoice")

        assert "uncategorized" in gl_result.gl_account.lower() or gl_result.source == "default", \
            f"Unknown vendor got specific GL: {gl_result.gl_account} from {gl_result.source}"
        assert "uncategorized" in vi_result.category.lower() or vi_result.source == "default", \
            f"Unknown vendor got specific category: {vi_result.category} from {vi_result.source}"

    def test_no_text_produces_low_confidence(self):
        """Empty document text should produce low confidence and no vendor match."""
        rd = _empty_rules_dir()
        rules = RulesEngine(rd)
        result = rules.run("")

        assert result.confidence <= 0.5, \
            f"Empty text got confidence {result.confidence} -- should be low"
        assert result.vendor_name is None, \
            f"Empty text matched vendor: {result.vendor_name}"

    def test_posting_builder_preserves_entry_kind(self):
        """PostingBuilder should not override entry_kind when explicitly set."""
        conn = _in_memory_db()
        doc = {
            "document_id": "test_entry_kind",
            "vendor": "Test",
            "amount": 100.00,
            "file_name": "test.pdf",
        }
        result = upsert_posting_job(conn, document=doc, entry_kind="credit_note")
        conn.close()

        assert result.get("entry_kind") == "credit_note" or \
            (result.get("payload_json") and "credit_note" in str(result.get("payload_json"))), \
            f"entry_kind was overridden: {result.get('entry_kind')}"


# =========================================================================
# SECTION 4: Substance-Aware GL Mapping with Rules
# =========================================================================

class TestSubstanceAwareMapping:
    """Test that when vendor_intel and gl_map rules exist, the system
    can handle substance-specific scenarios."""

    def _rules_dir_with_substance(self) -> Path:
        """Create rules dir with substance-aware mappings."""
        gl_map = {
            "vendors": {
                "Canada Revenue Agency": {"gl_account": "Payroll Liabilities", "tax_code": "EXEMPT"},
                "Revenu Quebec": {"gl_account": "Tax Liabilities - GST/QST", "tax_code": "EXEMPT"},
            },
            "doc_types": {
                "credit_note": {"gl_account": "Accounts Payable", "tax_code": "GST_QST"},
                "loan_payment": {"gl_account": "Long-term Debt", "tax_code": "EXEMPT"},
            },
            "default": {"gl_account": "Uncategorized Expense", "tax_code": "GST_QST"},
        }
        vendor_intel = {
            "vendors": {
                "Canada Revenue Agency": {
                    "category": "Government Remittance",
                    "gl_account": "Payroll Liabilities",
                    "tax_code": "EXEMPT",
                    "document_family": "remittance",
                },
            },
            "doc_type_defaults": {
                "credit_note": {
                    "category": "Credit Note",
                    "gl_account": "Accounts Payable",
                    "tax_code": "GST_QST",
                },
            },
            "default": {"category": "Uncategorized", "gl_account": "Uncategorized Expense", "tax_code": "GST_QST"},
        }
        return _tmp_rules_dir(gl_map=gl_map, vendor_intel=vendor_intel)

    def test_cra_remittance_with_rules(self):
        """With proper rules, CRA payment should go to Payroll Liabilities."""
        rd = self._rules_dir_with_substance()
        gl = GLMapper(rd)
        result = gl.map("Canada Revenue Agency", "remittance")
        assert "liabilit" in result.gl_account.lower(), \
            f"CRA payment went to {result.gl_account} instead of a liability account"

    def test_credit_note_with_rules(self):
        """Credit note should go to AP, not expense."""
        rd = self._rules_dir_with_substance()
        gl = GLMapper(rd)
        result = gl.map("Some Vendor", "credit_note")
        assert "payable" in result.gl_account.lower(), \
            f"Credit note went to {result.gl_account} instead of AP"

    def test_vendor_intel_cra_family(self):
        """VendorIntelligence should identify CRA as government remittance."""
        rd = self._rules_dir_with_substance()
        vi = VendorIntelligenceEngine(rd)
        result = vi.classify("Canada Revenue Agency", "remittance")
        assert result.document_family == "remittance", \
            f"CRA document family: {result.document_family}"
        assert "remittance" in result.category.lower() or "government" in result.category.lower(), \
            f"CRA category: {result.category}"
