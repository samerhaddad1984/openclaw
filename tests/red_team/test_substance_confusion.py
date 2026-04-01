"""
RED TEAM: Substance Engine Confusion Tests
===========================================

Attack surface: invoices that could plausibly be classified as multiple
economic substance types.  The engine must not force a single classification
with fake confidence when the data is genuinely ambiguous.

Tested confusion vectors:
  - expense vs prepaid vs fixed asset
  - loan payment vs expense
  - customer deposit vs revenue
  - intercompany vs vendor payment
  - personal expense vs legitimate business expense
  - mixed invoice (taxable + exempt)
  - replacement vs repair (CapEx negative-keyword override)
  - CapEx vendor memory must not overrule actual context
  - ambiguous classification must yield PARTIAL or BLOCK

Fail criteria:
  System forces one classification with fake confidence (i.e. no review
  flag, no block, confidence >= 0.80 on genuinely ambiguous input).
"""

from __future__ import annotations

import pytest

from src.engines.substance_engine import substance_classifier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flags(**kw):
    """Shorthand to call substance_classifier with keyword args."""
    return substance_classifier(**kw)


def _is_blocked_or_multi(flags: dict) -> bool:
    """Return True if the engine flagged ambiguity (block or multiple flags)."""
    substance_flags = [
        flags.get("potential_capex"),
        flags.get("potential_prepaid"),
        flags.get("potential_loan"),
        flags.get("potential_tax_remittance"),
        flags.get("potential_personal_expense"),
        flags.get("potential_customer_deposit"),
        flags.get("potential_intercompany"),
        flags.get("mixed_tax_invoice"),
    ]
    count = sum(1 for f in substance_flags if f)
    blocked = flags.get("block_auto_approval", False)
    return blocked or count >= 2 or count == 0


# =========================================================================
# SECTION 1: Invoices that straddle multiple classifications
# =========================================================================

class TestSubstanceConfusion:
    """Invoices designed to confuse the substance engine.
    Each one could be 2+ classifications. The engine MUST NOT force a single
    answer with high confidence and no review flag."""

    # ------------------------------------------------------------------
    # 1. Expense vs Prepaid — annual software subscription
    # ------------------------------------------------------------------
    def test_01_expense_vs_prepaid_annual_subscription(self):
        """Annual SaaS subscription: is it an immediate expense or prepaid?
        Engine should at minimum flag as prepaid for review."""
        f = _flags(
            vendor="Adobe Inc.",
            memo="Adobe Creative Cloud - Annual Plan Jan 2026 to Dec 2026",
            doc_type="invoice",
            amount=4200.00,
        )
        # Must not silently expense a 12-month prepaid
        has_prepaid = f["potential_prepaid"]
        blocked = f["block_auto_approval"]
        has_review = len(f.get("review_notes", [])) > 0

        assert has_prepaid or blocked or has_review, (
            f"CONFUSION FAIL: Annual subscription silently classified with no "
            f"prepaid flag or review. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 2. Expense vs Fixed Asset — $3,000 laptop
    # ------------------------------------------------------------------
    def test_02_expense_vs_fixed_asset_laptop(self):
        """$3,000 laptop from Dell. CapEx threshold is $1,500.
        Should flag as potential CapEx."""
        f = _flags(
            vendor="Dell Technologies",
            memo="Latitude 7440 laptop - employee workstation",
            doc_type="invoice",
            amount=3000.00,
        )
        assert f["potential_capex"], (
            f"CONFUSION FAIL: $3,000 Dell laptop not flagged as CapEx. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 3. Expense vs Loan Payment — line of credit payment
    # ------------------------------------------------------------------
    def test_03_expense_vs_loan_payment(self):
        """Monthly line of credit payment. Must be liability, not expense."""
        f = _flags(
            vendor="Banque Nationale",
            memo="Paiement mensuel - marge de crédit #MC-2024-112",
            doc_type="bank_payment",
            amount=2500.00,
        )
        assert f["potential_loan"], (
            f"CONFUSION FAIL: Line of credit payment not flagged as loan. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 4. Customer Deposit vs Revenue — advance for project
    # ------------------------------------------------------------------
    def test_04_customer_deposit_vs_revenue(self):
        """Client advance payment for unstarted project.
        Must be deferred revenue (liability), not income."""
        f = _flags(
            vendor="ABC Construction",
            memo="Acompte client - Projet rénovation #P-2026-44, travaux débutent mai 2026",
            doc_type="invoice",
            amount=12000.00,
        )
        assert f["potential_customer_deposit"], (
            f"CONFUSION FAIL: Customer advance not flagged as deposit. Flags: {f}"
        )
        assert f["block_auto_approval"], (
            f"CONFUSION FAIL: Customer deposit not blocked for review. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 5. Intercompany vs vendor payment
    # ------------------------------------------------------------------
    def test_05_intercompany_vs_vendor(self):
        """Management fees to parent company. Must flag intercompany."""
        f = _flags(
            vendor="Groupe BSQ Holdings Inc.",
            memo="Frais de gestion - filiale Q3 2025",
            doc_type="invoice",
            amount=8500.00,
        )
        assert f["potential_intercompany"], (
            f"CONFUSION FAIL: Intercompany management fees not flagged. Flags: {f}"
        )
        assert f["block_auto_approval"], (
            f"CONFUSION FAIL: Intercompany not blocked for review. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 6. Personal expense vs business expense — gym for employee
    # ------------------------------------------------------------------
    def test_06_personal_vs_business_gym(self):
        """Gym membership labeled as employee benefit.
        'gym' is a personal keyword — engine should flag it."""
        f = _flags(
            vendor="Énergie Cardio",
            memo="Abonnement gym - programme bien-être employés",
            doc_type="invoice",
            amount=1200.00,
        )
        # The word "gym" should trigger personal expense detection
        assert f["potential_personal_expense"], (
            f"CONFUSION FAIL: Gym expense not flagged as potential personal. Flags: {f}"
        )
        assert f["block_auto_approval"], (
            f"CONFUSION FAIL: Potential personal expense not blocked. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 7. Mixed invoice — taxable + exempt items
    # ------------------------------------------------------------------
    def test_07_mixed_taxable_exempt_invoice(self):
        """Invoice with both taxable and exempt items."""
        f = _flags(
            vendor="Fournisseur Médical XYZ",
            memo="Fournitures mixtes: équipement taxable + médicaments exonérés",
            doc_type="invoice",
            amount=5600.00,
        )
        has_mixed_note = any(
            "mixte" in n.lower() or "mixed" in n.lower()
            for n in f.get("review_notes", [])
        )
        assert has_mixed_note or f.get("mixed_tax_invoice"), (
            f"CONFUSION FAIL: Mixed invoice not flagged. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 8. Replacement vs Repair — remplacement complet
    # ------------------------------------------------------------------
    def test_08_replacement_overrides_repair_negative(self):
        """'Remplacement complet du système HVAC' — the replacement keyword
        should override the repair/maintenance negative keyword."""
        f = _flags(
            vendor="Climatisation ABC",
            memo="Remplacement complet du système HVAC - entretien annuel terminé, "
                 "système irréparable",
            doc_type="invoice",
            amount=18000.00,
        )
        assert f["potential_capex"], (
            f"CONFUSION FAIL: Full HVAC replacement not flagged as CapEx despite "
            f"'remplacement complet' keyword. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 9. Repair that should NOT be CapEx despite equipment keyword
    # ------------------------------------------------------------------
    def test_09_repair_not_capex(self):
        """Simple equipment repair — CapEx keyword + negative keyword
        should NOT result in confident CapEx classification."""
        f = _flags(
            vendor="Réparation Express",
            memo="Réparation imprimante HP LaserJet - remplacement cartouche et nettoyage",
            doc_type="invoice",
            amount=350.00,
        )
        # Should NOT be flagged as CapEx (repair + low amount)
        # Either no CapEx flag, or blocked for review
        if f["potential_capex"]:
            assert f["block_auto_approval"], (
                f"CONFUSION FAIL: Repair confidently classified as CapEx "
                f"without review block. Flags: {f}"
            )

    # ------------------------------------------------------------------
    # 10. CapEx vendor memory must NOT overrule actual context
    # ------------------------------------------------------------------
    def test_10_capex_vendor_does_not_overrule_context(self):
        """Dell invoice for monthly SaaS subscription ($50/month).
        Dell is a known CapEx vendor but amount is tiny and memo says
        'monthly subscription' — should NOT be CapEx."""
        f = _flags(
            vendor="Dell Technologies",
            memo="Dell SaaS monthly subscription - cloud backup service",
            doc_type="invoice",
            amount=50.00,
        )
        # $50 is well below the $1,500 CapEx vendor threshold
        # AND memo says "monthly subscription" (CapEx negative keyword)
        assert not f["potential_capex"], (
            f"CONFUSION FAIL: CapEx vendor memory overruled context. "
            f"$50 monthly SaaS subscription flagged as CapEx. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 11. Ambiguous: could be expense, prepaid, OR fixed asset
    # ------------------------------------------------------------------
    def test_11_triple_ambiguity_must_not_force_one(self):
        """Invoice from unknown vendor with mixed signals.
        System must not force a single classification without review."""
        f = _flags(
            vendor="Solutions Intégrées QC",
            memo="Abonnement annuel - équipement informatique, maintenance préventive incluse",
            doc_type="invoice",
            amount=8500.00,
        )
        # Contains: subscription (prepaid), equipment (CapEx), maintenance (CapEx negative)
        # The engine should either block or flag multiple types
        assert _is_blocked_or_multi(f), (
            f"CONFUSION FAIL: Triple-ambiguous invoice forced into single "
            f"classification without review. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 12. Loan payment disguised as vendor invoice
    # ------------------------------------------------------------------
    def test_12_loan_payment_as_vendor_invoice(self):
        """Capital lease payment formatted as a regular vendor invoice."""
        f = _flags(
            vendor="Caterpillar Financial",
            memo="Paiement mensuel crédit-bail #CB-2024-009 - excavatrice CAT 320",
            doc_type="invoice",
            amount=4200.00,
        )
        assert f["potential_loan"], (
            f"CONFUSION FAIL: Capital lease payment not flagged as loan. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 13. Owner false positive — common first name in vendor
    # ------------------------------------------------------------------
    def test_13_owner_false_positive_common_name(self):
        """Owner named 'Jean'. Vendor is 'Jean Coutu' (pharmacy chain).
        Must NOT flag as personal expense."""
        f = _flags(
            vendor="Jean Coutu",
            memo="Fournitures de bureau et papeterie",
            doc_type="invoice",
            amount=85.00,
            owner_names=["Jean"],
        )
        # Single-word owner name "Jean" should NOT match "Jean Coutu"
        # because Jean Coutu has multiple meaningful words
        assert not f["potential_personal_expense"], (
            f"CONFUSION FAIL: Owner false positive — 'Jean' matched 'Jean Coutu'. "
            f"Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 14. Owner TRUE positive — vendor IS the owner
    # ------------------------------------------------------------------
    def test_14_owner_true_positive(self):
        """Vendor name matches owner exactly. Must flag as personal."""
        f = _flags(
            vendor="Samer Haddad",
            memo="Remboursement - achat personnel Amazon",
            doc_type="expense_report",
            amount=250.00,
            owner_names=["Samer Haddad"],
        )
        assert f["potential_personal_expense"], (
            f"CONFUSION FAIL: Owner name exact match not flagged. Flags: {f}"
        )
        assert f["block_auto_approval"], (
            f"CONFUSION FAIL: Owner match not blocked for review. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 15. Prepaid vs expense — insurance that is NOT annual
    # ------------------------------------------------------------------
    def test_15_monthly_insurance_not_prepaid(self):
        """Monthly insurance premium — should still flag as prepaid/insurance
        for review even if monthly (the engine flags all insurance)."""
        f = _flags(
            vendor="Intact Assurance",
            memo="Prime d'assurance mensuelle - couverture commerciale mars 2026",
            doc_type="invoice",
            amount=520.00,
        )
        # Insurance keyword should still trigger prepaid flag
        assert f["potential_prepaid"], (
            f"CONFUSION FAIL: Insurance invoice not flagged as prepaid. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 16. Tax remittance vs expense — GST/TPS payment
    # ------------------------------------------------------------------
    def test_16_tax_remittance_not_expense(self):
        """GST/TPS remittance to CRA. Must be tax liability clearing,
        never an expense."""
        f = _flags(
            vendor="Revenu Québec",
            memo="Remise TPS/TVQ - période janvier à mars 2026",
            doc_type="bank_payment",
            amount=3400.00,
        )
        assert f["potential_tax_remittance"], (
            f"CONFUSION FAIL: Tax remittance not flagged. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 17. Intercompany disguised as normal expense
    # ------------------------------------------------------------------
    def test_17_intercompany_subsidiary_invoice(self):
        """Invoice from subsidiary with 'related entity' in memo."""
        f = _flags(
            vendor="BSQ Services Techniques",
            memo="Services rendus - related entity transfer Q4 2025",
            doc_type="invoice",
            amount=15000.00,
        )
        assert f["potential_intercompany"], (
            f"CONFUSION FAIL: Related entity invoice not flagged. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 18. CapEx vendor + low amount = NOT CapEx
    # ------------------------------------------------------------------
    def test_18_capex_vendor_below_threshold(self):
        """HP invoice for $200 toner cartridge. Below CapEx threshold."""
        f = _flags(
            vendor="HP",
            memo="Toner cartridge HP LaserJet 26A",
            doc_type="invoice",
            amount=200.00,
        )
        # Below $1,500 threshold even for known CapEx vendors
        assert not f["potential_capex"], (
            f"CONFUSION FAIL: $200 toner from HP flagged as CapEx. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 19. Genuine ambiguity — customer deposit + intercompany
    # ------------------------------------------------------------------
    def test_19_deposit_plus_intercompany(self):
        """Advance payment from a subsidiary. Could be customer deposit
        OR intercompany. Must block for review."""
        f = _flags(
            vendor="Filiale BSQ Nord",
            memo="Acompte client - projet division nord, avance sur travaux",
            doc_type="invoice",
            amount=25000.00,
        )
        assert f["block_auto_approval"], (
            f"CONFUSION FAIL: Deposit + intercompany ambiguity not blocked. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 20. Fake confidence on totally ambiguous input
    # ------------------------------------------------------------------
    def test_20_ambiguous_must_not_fake_confidence(self):
        """Bare-bones invoice with no classification signals.
        Engine must not invent a classification."""
        f = _flags(
            vendor="XYZ-12345",
            memo="INV-2026-001",
            doc_type="invoice",
            amount=7777.77,
        )
        # No keywords match — engine should not flag any substance type
        substance_flags = [
            f.get("potential_capex"),
            f.get("potential_prepaid"),
            f.get("potential_loan"),
            f.get("potential_tax_remittance"),
            f.get("potential_personal_expense"),
            f.get("potential_customer_deposit"),
            f.get("potential_intercompany"),
        ]
        count = sum(1 for fl in substance_flags if fl)
        # Either nothing flagged (honest "I don't know") or blocked for review
        assert count == 0 or f["block_auto_approval"], (
            f"CONFUSION FAIL: Engine invented a classification on bare input "
            f"without blocking for review. Active flags: {count}. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 21. Personal negative override — HR staffing service
    # ------------------------------------------------------------------
    def test_21_hr_staffing_not_personal(self):
        """'Service du personnel temporaire' is HR staffing, NOT personal."""
        f = _flags(
            vendor="Agence de Placement ABC",
            memo="Service du personnel temporaire - semaine du 10 mars 2026",
            doc_type="invoice",
            amount=3200.00,
        )
        assert not f["potential_personal_expense"], (
            f"CONFUSION FAIL: HR staffing flagged as personal expense. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 22. Loan negative override — prêt-à-porter
    # ------------------------------------------------------------------
    def test_22_pret_a_porter_not_loan(self):
        """Fashion retailer with 'prêt-à-porter' in name.
        Must NOT trigger loan detection."""
        f = _flags(
            vendor="Boutique Prêt-à-porter Élégance",
            memo="Uniformes employés - collection printemps 2026",
            doc_type="invoice",
            amount=1800.00,
        )
        assert not f["potential_loan"], (
            f"CONFUSION FAIL: 'prêt-à-porter' triggered loan detection. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 23. Gift card — employee benefit, not personal
    # ------------------------------------------------------------------
    def test_23_gift_card_employee_benefit(self):
        """Employee gift cards — should flag as gift/benefit expense."""
        f = _flags(
            vendor="Amazon",
            memo="Cartes-cadeau employés - programme reconnaissance Q4",
            doc_type="invoice",
            amount=2500.00,
        )
        has_gift_note = any(
            "cadeau" in n.lower() or "gift" in n.lower()
            for n in f.get("review_notes", [])
        )
        assert has_gift_note, (
            f"CONFUSION FAIL: Gift card invoice not flagged. Flags: {f}"
        )

    # ------------------------------------------------------------------
    # 24. Large bank wire — should trigger loan review
    # ------------------------------------------------------------------
    def test_24_large_bank_wire_triggers_review(self):
        """$75,000 wire from BDC. Even without 'loan' keyword, bank name
        + large amount should trigger loan review."""
        f = _flags(
            vendor="BDC - Banque de développement du Canada",
            memo="Virement #VR-2026-112",
            doc_type="bank_deposit",
            amount=75000.00,
        )
        assert f["potential_loan"], (
            f"CONFUSION FAIL: Large bank wire not flagged as potential loan. Flags: {f}"
        )
        assert f["block_auto_approval"], (
            f"CONFUSION FAIL: Large bank wire not blocked for review. Flags: {f}"
        )
