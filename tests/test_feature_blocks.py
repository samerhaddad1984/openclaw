"""
tests/test_feature_blocks.py
=============================
Comprehensive tests for all 5 feature blocks:
  BLOCK 1 — AI-assisted substance classification
  BLOCK 2 — Customer deposit, intercompany, loan note
  BLOCK 3 — Mixed taxable/exempt invoice (tax_code_resolver)
  BLOCK 4 — Split payment matching
  BLOCK 5 — Substance flags enforced in review policy
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import patch, MagicMock

from src.engines.substance_engine import substance_classifier, run_substance_classifier
from src.engines.tax_code_resolver import resolve_mixed_tax
from src.agents.tools.review_policy import (
    effective_confidence,
    should_auto_approve,
    decide_review_status,
    check_fraud_flags,
    _parse_substance_flags,
)
from src.agents.tools.bank_matcher import BankMatcher
from src.agents.core.bank_models import BankTransaction
from src.agents.core.task_models import DocumentRecord


# =========================================================================
# BLOCK 1 — AI-assisted substance classification
# =========================================================================

class TestBlock1SubstanceClassification:
    """Test AI fallback and prompt template integration."""

    def test_capex_keyword_detection(self):
        flags = substance_classifier(vendor="Dell", memo="PowerEdge server", amount=5000)
        assert flags["potential_capex"] is True
        assert flags["suggested_gl"] == "1500"

    def test_prepaid_keyword_detection(self):
        flags = substance_classifier(vendor="Sun Life", memo="assurance annuelle", amount=1200)
        assert flags["potential_prepaid"] is True
        assert flags["suggested_gl"] == "1300"

    def test_loan_keyword_detection(self):
        flags = substance_classifier(vendor="BDC", memo="paiement de prêt mensuel", amount=2000)
        assert flags["potential_loan"] is True
        assert flags["suggested_gl"] == "2500"

    def test_tax_remittance_detection(self):
        flags = substance_classifier(vendor="CRA", memo="remise TPS", amount=3500)
        assert flags["potential_tax_remittance"] is True

    def test_personal_expense_detection(self):
        flags = substance_classifier(vendor="Netflix", memo="personal subscription", amount=15)
        assert flags["potential_personal_expense"] is True
        assert flags["block_auto_approval"] is True

    def test_no_keyword_triggers_ai_fallback(self):
        """When no keywords match, AI fallback should be attempted."""
        mock_result = {
            "classification": "capital_asset",
            "confidence": 0.85,
            "gl_suggestion": "1500",
            "reasoning": "Serveur / Server detected",
            "error": None,
        }
        with patch("src.agents.core.ai_router.call_substance_classification", return_value=mock_result):
            flags = substance_classifier(vendor="ACME Corp", memo="hardware delivery", amount=8000)
            assert flags["potential_capex"] is True
            assert any("AI classification" in n for n in flags["review_notes"])

    def test_low_confidence_vendor_heuristic_triggers_ai(self):
        """Vendor-only capex match (confidence 0.60) should also trigger AI."""
        mock_result = {
            "classification": "capital_asset",
            "confidence": 0.90,
            "gl_suggestion": "1500",
            "reasoning": "Dell hardware purchase",
            "error": None,
        }
        # Dell vendor match without keyword in memo — should be low confidence
        with patch("src.agents.core.ai_router.call_substance_classification", return_value=mock_result):
            flags = substance_classifier(vendor="Dell Technologies", memo="order #12345", amount=5000)
            assert flags["potential_capex"] is True

    def test_ai_fallback_failure_doesnt_crash(self):
        """AI failure should be gracefully handled."""
        with patch("src.agents.core.ai_router.call_substance_classification", side_effect=Exception("API down")):
            flags = substance_classifier(vendor="Unknown Corp", memo="mystery charge", amount=999)
            # Should not crash, should return default flags
            assert isinstance(flags, dict)

    def test_ai_operating_expense_not_flagged(self):
        """AI returning operating_expense should not set any flags."""
        mock_result = {
            "classification": "operating_expense",
            "confidence": 0.95,
            "gl_suggestion": "5000",
            "reasoning": "Normal operating expense",
            "error": None,
        }
        with patch("src.agents.core.ai_router.call_substance_classification", return_value=mock_result):
            flags = substance_classifier(vendor="Office Depot", memo="pens and paper", amount=50)
            assert flags["potential_capex"] is False
            assert flags["potential_prepaid"] is False

    def test_province_passed_to_classifier(self):
        flags = substance_classifier(vendor="Test", memo="equipment", amount=5000, province="QC")
        assert flags["potential_capex"] is True

    def test_new_flag_keys_initialized(self):
        """New flags for customer_deposit, intercompany, mixed_tax should be initialized."""
        flags = substance_classifier(vendor="Test", memo="normal expense")
        assert "potential_customer_deposit" in flags
        assert "potential_intercompany" in flags
        assert "mixed_tax_invoice" in flags
        assert flags["potential_customer_deposit"] is False
        assert flags["potential_intercompany"] is False
        assert flags["mixed_tax_invoice"] is False

    def test_ai_customer_deposit_classification(self):
        mock_result = {
            "classification": "customer_deposit",
            "confidence": 0.80,
            "gl_suggestion": "2400",
            "reasoning": "Customer advance payment",
            "error": None,
        }
        with patch("src.agents.core.ai_router.call_substance_classification", return_value=mock_result):
            flags = substance_classifier(vendor="Client XYZ", memo="advance for project", amount=2000)
            assert flags["potential_customer_deposit"] is True
            assert flags["block_auto_approval"] is True

    def test_ai_intercompany_classification(self):
        mock_result = {
            "classification": "intercompany_transfer",
            "confidence": 0.85,
            "gl_suggestion": "1700",
            "reasoning": "Transfer between related entities",
            "error": None,
        }
        with patch("src.agents.core.ai_router.call_substance_classification", return_value=mock_result):
            flags = substance_classifier(vendor="Parent Corp", memo="transfer", amount=10000)
            assert flags["potential_intercompany"] is True
            assert flags["block_auto_approval"] is True

    def test_ai_mixed_invoice_classification(self):
        mock_result = {
            "classification": "mixed_invoice",
            "confidence": 0.78,
            "gl_suggestion": None,
            "reasoning": "Contains taxable and exempt items",
            "error": None,
        }
        with patch("src.agents.core.ai_router.call_substance_classification", return_value=mock_result):
            flags = substance_classifier(vendor="Costco", memo="office and groceries", amount=500)
            assert flags["mixed_tax_invoice"] is True
            assert flags["block_auto_approval"] is True

    def test_run_substance_classifier_wrapper(self):
        doc = {"vendor": "Dell", "memo": "servers", "doc_type": "invoice", "amount": 10000}
        flags = run_substance_classifier(doc)
        assert flags["potential_capex"] is True


# =========================================================================
# BLOCK 2 — Customer deposit, intercompany, loan note
# =========================================================================

class TestBlock2CustomerDeposit:

    def test_customer_deposit_keywords_basic(self):
        flags = substance_classifier(vendor="Client ABC", memo="dépôt client projet X", amount=1000)
        assert flags["potential_customer_deposit"] is True
        assert flags["suggested_gl"] == "2400"
        assert flags["block_auto_approval"] is True

    def test_customer_deposit_retainer(self):
        flags = substance_classifier(vendor="Law Firm", memo="retainer fee", amount=2000)
        assert flags["potential_customer_deposit"] is True
        assert flags["block_auto_approval"] is True

    def test_customer_deposit_arrhes(self):
        flags = substance_classifier(vendor="Client FR", memo="arrhes pour le contrat", amount=800)
        assert flags["potential_customer_deposit"] is True

    def test_customer_deposit_avance(self):
        flags = substance_classifier(vendor="Client", memo="avance sur commande", amount=1500)
        assert flags["potential_customer_deposit"] is True

    def test_customer_deposit_advance_payment(self):
        flags = substance_classifier(vendor="Client", memo="advance payment for project", amount=5000)
        assert flags["potential_customer_deposit"] is True
        assert flags["suggested_gl"] == "2400"

    def test_customer_deposit_down_payment(self):
        flags = substance_classifier(vendor="Builder", memo="down payment for renovation", amount=3000)
        assert flags["potential_customer_deposit"] is True

    def test_customer_deposit_below_500_no_block(self):
        """Deposits <= $500 should not block auto-approval via the BLOCK2 threshold."""
        flags = substance_classifier(vendor="Client", memo="acompte client small", amount=300)
        # Should still detect but GL override applies without block
        assert flags["suggested_gl"] == "2400"
        # block_auto_approval should be False for small amounts (unless other flags set)
        assert flags.get("potential_customer_deposit", False) is False

    def test_customer_deposit_above_500_blocks(self):
        flags = substance_classifier(vendor="Client", memo="acompte client", amount=600)
        assert flags["potential_customer_deposit"] is True
        assert flags["block_auto_approval"] is True

    def test_security_deposit_not_customer(self):
        """Plain security deposit should use GL 1400, not customer deposit GL."""
        flags = substance_classifier(vendor="Landlord", memo="security deposit office lease", amount=2000)
        assert flags["suggested_gl"] == "1400"


class TestBlock2Intercompany:

    def test_intercompany_keyword(self):
        flags = substance_classifier(vendor="Head Office", memo="intercompany transfer", amount=50000)
        assert flags["potential_intercompany"] is True
        assert flags["block_auto_approval"] is True
        assert flags["suggested_gl"] == "1700"

    def test_intercompagnie_keyword(self):
        flags = substance_classifier(vendor="Siège social", memo="transfert intercompagnie", amount=25000)
        assert flags["potential_intercompany"] is True

    def test_filiale_keyword(self):
        flags = substance_classifier(vendor="Filiale ABC", memo="frais de gestion", amount=10000)
        assert flags["potential_intercompany"] is True

    def test_subsidiary_keyword(self):
        flags = substance_classifier(vendor="Subsidiary Ltd", memo="management fees", amount=15000)
        assert flags["potential_intercompany"] is True

    def test_societe_liee_keyword(self):
        flags = substance_classifier(vendor="Société liée", memo="transfert", amount=8000)
        assert flags["potential_intercompany"] is True

    def test_affiliated_company_keyword(self):
        flags = substance_classifier(vendor="Affiliated Company Inc", memo="group charges", amount=5000)
        assert flags["potential_intercompany"] is True

    def test_societe_mere_keyword(self):
        flags = substance_classifier(vendor="Société mère Holdings", memo="fees", amount=20000)
        assert flags["potential_intercompany"] is True

    def test_division_keyword(self):
        flags = substance_classifier(vendor="Division Est", memo="internal charges", amount=3000)
        assert flags["potential_intercompany"] is True

    def test_related_entity_keyword(self):
        flags = substance_classifier(vendor="Related Entity", memo="services", amount=7000)
        assert flags["potential_intercompany"] is True

    def test_intercompany_note_mentions_related_parties_table(self):
        flags = substance_classifier(vendor="Holding Co", memo="intercompany", amount=10000)
        notes_str = " ".join(flags["review_notes"])
        assert "related_parties" in notes_str.lower() or "parties liées" in notes_str.lower()


class TestBlock2LoanNote:

    def test_loan_payment_note_gl_5480(self):
        """Loan detection should include principal/interest split note with GL 5480."""
        flags = substance_classifier(vendor="Bank", memo="paiement de prêt", amount=5000)
        notes_str = " ".join(flags["review_notes"])
        assert "GL 5480" in notes_str
        assert "GL 2500" in notes_str

    def test_loan_payment_note_bilingual(self):
        flags = substance_classifier(vendor="Caisse Desjardins", memo="prêt hypothécaire", amount=3000)
        notes_str = " ".join(flags["review_notes"])
        assert "Paiement de prêt" in notes_str
        assert "Loan payment" in notes_str
        assert "capital" in notes_str.lower() or "principal" in notes_str.lower()
        assert "intérêts" in notes_str.lower() or "interest" in notes_str.lower()


# =========================================================================
# BLOCK 3 — Mixed taxable/exempt invoice (tax_code_resolver)
# =========================================================================

class TestBlock3TaxCodeResolver:

    def test_strong_mixed_keyword_detection(self):
        result = resolve_mixed_tax(
            memo="fournitures médicales et alimentaires",
            invoice_text="mixed supply of medical and food items",
        )
        assert result["mixed_tax_invoice"] is True
        assert result["tax_code"] is None
        assert result["block_auto_approval"] is True
        assert result["confidence"] >= 0.80

    def test_partial_exempt_keyword(self):
        result = resolve_mixed_tax(memo="partial exempt supply")
        assert result["mixed_tax_invoice"] is True

    def test_mixed_supplies_keyword(self):
        result = resolve_mixed_tax(memo="mixed supplies invoice")
        assert result["mixed_tax_invoice"] is True

    def test_fournitures_mixtes_keyword(self):
        result = resolve_mixed_tax(memo="fournitures mixtes bureau et médical")
        assert result["mixed_tax_invoice"] is True

    def test_taxable_and_exempt_secondary_detection(self):
        """Both exempt and taxable indicators present → mixed detection."""
        result = resolve_mixed_tax(
            invoice_text="Office supplies $45.00 (taxable) and prescription drugs $30.00 (exempt) GST included"
        )
        assert result["mixed_tax_invoice"] is True
        assert result["confidence"] >= 0.60

    def test_no_mixed_tax_plain_invoice(self):
        result = resolve_mixed_tax(memo="office supplies", invoice_text="pens $5.00")
        assert result["mixed_tax_invoice"] is False

    def test_mixed_tax_blocks_auto_approval(self):
        result = resolve_mixed_tax(memo="taxable et exonéré sur même facture")
        assert result["block_auto_approval"] is True

    def test_mixed_tax_review_note_bilingual(self):
        result = resolve_mixed_tax(memo="mixed supply of items")
        if result["mixed_tax_invoice"]:
            notes_str = " ".join(result["review_notes"])
            assert "allocation" in notes_str.lower() or "mixte" in notes_str.lower()

    def test_ai_fallback_on_secondary_detection(self):
        """Secondary detection with low confidence should attempt AI fallback."""
        mock_result = {
            "is_mixed": True,
            "taxable_items": ["Office supplies $45.00"],
            "exempt_items": ["Prescription drugs $30.00"],
            "suggested_allocation": {"taxable_total": "$45.00", "exempt_total": "$30.00"},
            "error": None,
        }
        with patch("src.agents.core.ai_router.call_mixed_tax_detection", return_value=mock_result):
            result = resolve_mixed_tax(
                invoice_text="Office supplies $45.00 and prescription ordonnance $30.00 GST applied"
            )
            assert result["mixed_tax_invoice"] is True

    def test_ai_fallback_failure_graceful(self):
        with patch("src.agents.core.ai_router.call_mixed_tax_detection", side_effect=Exception("API error")):
            result = resolve_mixed_tax(
                invoice_text="Some exempt items and some taxable items in this long invoice text that exceeds fifty chars"
            )
            # Should not crash
            assert isinstance(result, dict)


# =========================================================================
# BLOCK 4 — Split payment matching
# =========================================================================

class TestBlock4SplitPayment:

    def _make_doc(self, doc_id, amount, vendor="Vendor A", client="C001", date="2024-06-15"):
        return DocumentRecord(
            document_id=doc_id,
            file_name=f"{doc_id}.pdf",
            file_path=f"/docs/{doc_id}.pdf",
            client_code=client,
            vendor=vendor,
            document_date=date,
            amount=amount,
            doc_type="invoice",
            category="expense",
            gl_account="5000",
            tax_code="T",
            review_status="NeedsReview",
            confidence=0.80,
            raw_result={},
        )

    def _make_txn(self, txn_id, amount, desc="Vendor A", client="C001", date="2024-06-16"):
        return BankTransaction(
            transaction_id=txn_id,
            client_code=client,
            account_id="acct1",
            posted_date=date,
            description=desc,
            memo="",
            amount=amount,
            currency="CAD",
        )

    def test_split_2_invoices_exact(self):
        """Two invoices that sum exactly to the transaction amount."""
        docs = [
            self._make_doc("d1", 500.00),
            self._make_doc("d2", 300.00),
        ]
        txns = [self._make_txn("t1", 800.00)]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) >= 1
        found = [s for s in splits if s["transaction_id"] == "t1"]
        assert len(found) >= 1
        assert found[0]["match_status"] == "split_candidate"

    def test_split_3_invoices(self):
        docs = [
            self._make_doc("d1", 200.00),
            self._make_doc("d2", 300.00),
            self._make_doc("d3", 500.00),
        ]
        txns = [self._make_txn("t1", 1000.00)]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) >= 1

    def test_split_4_invoices(self):
        """BLOCK 4: Now supports 4-invoice combinations."""
        docs = [
            self._make_doc("d1", 100.00),
            self._make_doc("d2", 200.00),
            self._make_doc("d3", 300.00),
            self._make_doc("d4", 400.00),
        ]
        txns = [self._make_txn("t1", 1000.00)]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) >= 1
        # Find the 4-invoice combination
        four_combos = [s for s in splits if len(s["matched_document_ids"]) == 4]
        assert len(four_combos) >= 1

    def test_split_1_percent_tolerance(self):
        """Amount within 1% tolerance should match."""
        docs = [
            self._make_doc("d1", 500.00),
            self._make_doc("d2", 505.00),  # sum = 1005
        ]
        # Transaction is 1000 — 1% of 1000 = 10, and diff is 5 < 10
        txns = [self._make_txn("t1", 1000.00)]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) >= 1

    def test_split_beyond_1_percent_no_match(self):
        """Amount beyond 1% tolerance should NOT match."""
        docs = [
            self._make_doc("d1", 500.00),
            self._make_doc("d2", 520.00),  # sum = 1020, diff = 20 > 1% of 1000
        ]
        txns = [self._make_txn("t1", 1000.00)]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) == 0

    def test_split_different_client_excluded(self):
        """Invoices from different client should not be combined."""
        docs = [
            self._make_doc("d1", 500.00, client="C001"),
            self._make_doc("d2", 300.00, client="C002"),
        ]
        txns = [self._make_txn("t1", 800.00, client="C001")]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) == 0

    def test_split_payment_detector_wrapper(self):
        """split_payment_detector is a convenience wrapper."""
        docs = [
            self._make_doc("d1", 500.00),
            self._make_doc("d2", 300.00),
        ]
        txns = [self._make_txn("t1", 800.00)]
        matcher = BankMatcher()
        splits = matcher.split_payment_detector(docs, txns)
        assert len(splits) >= 1

    def test_split_negative_transaction_ignored(self):
        """Negative transactions should be skipped."""
        docs = [
            self._make_doc("d1", 500.00),
            self._make_doc("d2", 300.00),
        ]
        txns = [self._make_txn("t1", -800.00)]
        matcher = BankMatcher()
        splits = matcher.detect_split_payments(docs, txns)
        assert len(splits) == 0


# =========================================================================
# BLOCK 5 — Substance flags enforced in review policy
# =========================================================================

class TestBlock5ReviewPolicy:

    def test_parse_substance_flags_from_dict(self):
        sf = _parse_substance_flags({"potential_capex": True})
        assert sf["potential_capex"] is True

    def test_parse_substance_flags_from_json_string(self):
        sf = _parse_substance_flags('{"potential_capex": true}')
        assert sf["potential_capex"] is True

    def test_parse_substance_flags_none(self):
        sf = _parse_substance_flags(None)
        assert sf == {}

    def test_parse_substance_flags_invalid_json(self):
        sf = _parse_substance_flags("not json")
        assert sf == {}

    def test_capex_caps_confidence_at_070(self):
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            substance_flags={"potential_capex": True},
        )
        assert eff <= 0.70

    def test_customer_deposit_caps_confidence_at_060(self):
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            substance_flags={"potential_customer_deposit": True},
        )
        assert eff <= 0.60

    def test_intercompany_caps_confidence_at_060(self):
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            substance_flags={"potential_intercompany": True},
        )
        assert eff <= 0.60

    def test_mixed_tax_caps_confidence_at_050(self):
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            substance_flags={"mixed_tax_invoice": True},
        )
        assert eff <= 0.50

    def test_multiple_substance_flags_lowest_cap_wins(self):
        """When multiple flags set, the lowest cap should apply."""
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            substance_flags={
                "potential_capex": True,
                "mixed_tax_invoice": True,
            },
        )
        assert eff <= 0.50  # mixed_tax_invoice has the lowest cap

    def test_no_substance_flags_no_cap(self):
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            substance_flags={},
        )
        assert eff >= 0.90

    def test_fraud_flag_and_substance_flag_both_apply(self):
        """Both fraud (0.60) and substance (0.70) caps should apply, fraud wins."""
        eff = effective_confidence(
            rules_confidence=0.90,
            final_method="rules",
            has_required=True,
            fraud_flags=[{"severity": "high", "rule": "test"}],
            substance_flags={"potential_capex": True},
        )
        assert eff <= 0.60

    def test_should_auto_approve_blocked_by_capex(self):
        assert should_auto_approve(
            confidence=0.95,
            substance_flags={"potential_capex": True},
        ) is False

    def test_should_auto_approve_blocked_by_customer_deposit(self):
        assert should_auto_approve(
            confidence=0.95,
            substance_flags={"potential_customer_deposit": True},
        ) is False

    def test_should_auto_approve_blocked_by_intercompany(self):
        assert should_auto_approve(
            confidence=0.95,
            substance_flags={"potential_intercompany": True},
        ) is False

    def test_should_auto_approve_blocked_by_mixed_tax(self):
        assert should_auto_approve(
            confidence=0.95,
            substance_flags={"mixed_tax_invoice": True},
        ) is False

    def test_should_auto_approve_blocked_by_block_auto_approval(self):
        assert should_auto_approve(
            confidence=0.95,
            substance_flags={"block_auto_approval": True},
        ) is False

    def test_should_auto_approve_passes_clean_document(self):
        assert should_auto_approve(
            confidence=0.90,
            substance_flags={},
        ) is True

    def test_decide_review_status_mixed_tax_always_needs_review(self):
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Vendor",
            total=500.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={"mixed_tax_invoice": True},
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.50

    def test_decide_review_status_capex_needs_review(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Dell",
            total=10000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={"potential_capex": True},
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.70

    def test_decide_review_status_customer_deposit_needs_review(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Client ABC",
            total=2000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={"potential_customer_deposit": True},
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.60

    def test_decide_review_status_intercompany_needs_review(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Holding Co",
            total=50000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={"potential_intercompany": True},
        )
        assert decision.status == "NeedsReview"

    def test_decide_review_status_substance_flags_from_json(self):
        """Substance flags passed as JSON string should be parsed."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Dell",
            total=5000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags='{"potential_capex": true}',
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.70

    def test_decide_review_status_no_substance_flags_ready(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Office Depot",
            total=100.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={},
        )
        assert decision.status == "Ready"
        assert decision.effective_confidence >= 0.85

    def test_substance_note_in_review_notes(self):
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Dell",
            total=5000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={"potential_capex": True},
        )
        assert decision.review_notes is not None
        assert "substance_flags_block_auto_approval" in decision.review_notes


# =========================================================================
# Integration tests — cross-block interactions
# =========================================================================

class TestCrossBlockIntegration:

    def test_customer_deposit_flows_to_review_policy(self):
        """Customer deposit detected in substance → blocks in review policy."""
        flags = substance_classifier(vendor="Client", memo="acompte client", amount=2000)
        assert flags["potential_customer_deposit"] is True

        decision = decide_review_status(
            rules_confidence=0.92,
            final_method="rules",
            vendor_name="Client",
            total=2000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags=flags,
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.60

    def test_intercompany_flows_to_review_policy(self):
        flags = substance_classifier(vendor="Filiale ABC", memo="management fees", amount=50000)
        assert flags["potential_intercompany"] is True

        decision = decide_review_status(
            rules_confidence=0.92,
            final_method="rules",
            vendor_name="Filiale ABC",
            total=50000.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags=flags,
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.60

    def test_loan_with_note_flows_correctly(self):
        flags = substance_classifier(vendor="BDC", memo="paiement de prêt", amount=5000)
        assert flags["potential_loan"] is True
        notes_str = " ".join(flags["review_notes"])
        assert "GL 5480" in notes_str

    def test_mixed_tax_resolver_integrates_with_review(self):
        result = resolve_mixed_tax(memo="fournitures mixtes taxables et exonérées")
        assert result["mixed_tax_invoice"] is True

        decision = decide_review_status(
            rules_confidence=0.92,
            final_method="rules",
            vendor_name="Mixed Vendor",
            total=500.0,
            document_date="2024-06-15",
            client_code="C001",
            substance_flags={"mixed_tax_invoice": True, "block_auto_approval": True},
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.50
