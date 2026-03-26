"""
src/engines/uncertainty_engine.py — Provenance-preserving uncertainty engine.

Handles all 21 failure modes with structured uncertainty tracking.
No fake confidence. Either prove it or preserve why you cannot.

Public interface
----------------
UncertaintyReason          — structured reason with bilingual descriptions
UncertaintyState           — full uncertainty state for a document
DateResolutionState        — date ambiguity tracking
PostingDecision            — posting readiness evaluation result
evaluate_uncertainty       — build UncertaintyState from field confidences
evaluate_posting_readiness — determine if document is safe to post
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# =========================================================================
# PART 1 — Structured uncertainty model
# =========================================================================

# Reason codes covering all 21 failure modes
REASON_CODES = {
    "VENDOR_IDENTITY_UNPROVEN",
    "ALLOCATION_GAP_UNEXPLAINED",
    "TAX_REGISTRATION_INCOMPLETE",
    "VENDOR_NAME_CONFLICT",
    "INVOICE_NUMBER_OCR_CONFLICT",
    "DATE_AMBIGUOUS",
    "SETTLEMENT_STATE_UNRESOLVED",
    "PAYEE_IDENTITY_UNPROVEN",
    "CUSTOMS_NOTE_SCOPE_LIMITED",
    "BOILERPLATE_TAX_DISCLAIMER",
    "MISSING_SUPPORTING_VENDOR_DOCUMENT",
    "DUPLICATE_INGESTION_CANDIDATE",
    "REINGEST_WITH_VARIATION",
    "CROSS_ENTITY_PAYMENT_UNCERTAIN",
    "TAX_IDENTITY_UNRESOLVED",
    "POSSIBLE_FRAUD_DIVERSION",
    "FX_RATE_DATE_AMBIGUOUS",
    "AGING_BUCKET_AMBIGUOUS",
    "PERIOD_CLASSIFICATION_AMBIGUOUS",
    "CREDIT_MEMO_PRE_POST_CLOSE_AMBIGUOUS",
    "DUPLICATE_WINDOW_AMBIGUOUS",
    # Trap 1 — filed period amendment
    "FILED_PERIOD_AMENDMENT_NEEDED",
    # Trap 2 — credit memo tax decomposition
    "CREDIT_MEMO_TAX_SPLIT_UNPROVEN",
    "CREDIT_MEMO_PARTIAL_DECOMPOSITION",
    # Trap 3 — subcontractor overlap
    "SUBCONTRACTOR_WORK_SCOPE_OVERLAP",
    # Trap 4 — recognition timing
    "RECOGNITION_TIMING_DEFERRED",
    "PRIOR_TREATMENT_CONTRADICTION",
    # Trap 5 — duplicate cluster member
    "DUPLICATE_CLUSTER_NON_HEAD",
    # Trap 6 — stale version
    "STALE_VERSION_DETECTED",
    # Trap 7 — manual journal collision
    "MANUAL_JOURNAL_COLLISION",
    # Trap 8 — rollback
    "REIMPORT_BLOCKED_AFTER_ROLLBACK",
}

# Posting recommendation constants
SAFE_TO_POST = "SAFE_TO_POST"
PARTIAL_POST_WITH_FLAGS = "PARTIAL_POST_WITH_FLAGS"
BLOCK_PENDING_REVIEW = "BLOCK_PENDING_REVIEW"


@dataclass
class UncertaintyReason:
    """A structured reason explaining why a field cannot be resolved."""

    reason_code: str
    description_fr: str
    description_en: str
    evidence_available: str
    evidence_needed: str

    def to_dict(self) -> dict[str, str]:
        return {
            "reason_code": self.reason_code,
            "description_fr": self.description_fr,
            "description_en": self.description_en,
            "evidence_available": self.evidence_available,
            "evidence_needed": self.evidence_needed,
        }


@dataclass
class UncertaintyState:
    """Full uncertainty state for a document or posting candidate."""

    can_post: bool = False
    partial_post_allowed: bool = False
    must_block: bool = False
    unresolved_reasons: list[UncertaintyReason] = field(default_factory=list)
    confidence_by_field: dict[str, float] = field(default_factory=dict)
    posting_recommendation: str = BLOCK_PENDING_REVIEW

    def to_dict(self) -> dict[str, Any]:
        return {
            "can_post": self.can_post,
            "partial_post_allowed": self.partial_post_allowed,
            "must_block": self.must_block,
            "unresolved_reasons": [r.to_dict() for r in self.unresolved_reasons],
            "confidence_by_field": dict(self.confidence_by_field),
            "posting_recommendation": self.posting_recommendation,
        }

    def add_reason(self, reason: UncertaintyReason) -> None:
        self.unresolved_reasons.append(reason)


def evaluate_uncertainty(
    confidence_by_field: dict[str, float],
    reasons: list[UncertaintyReason] | None = None,
) -> UncertaintyState:
    """Build an UncertaintyState from per-field confidence scores.

    Rules:
    - If any field has confidence < 0.60: must_block=True
    - If any field has confidence 0.60-0.79: partial_post_allowed=True, can_post=False
    - If all fields >= 0.80: can_post=True
    - NEVER return a clean result when underlying evidence is incomplete
    """
    state = UncertaintyState(
        confidence_by_field=dict(confidence_by_field),
        unresolved_reasons=list(reasons) if reasons else [],
    )

    if not confidence_by_field:
        state.must_block = True
        state.posting_recommendation = BLOCK_PENDING_REVIEW
        return state

    min_confidence = min(confidence_by_field.values())
    has_medium = any(0.60 <= v < 0.80 for v in confidence_by_field.values())

    if min_confidence < 0.60:
        state.must_block = True
        state.can_post = False
        state.partial_post_allowed = False
        state.posting_recommendation = BLOCK_PENDING_REVIEW
    elif has_medium:
        state.must_block = False
        state.can_post = False
        state.partial_post_allowed = True
        state.posting_recommendation = PARTIAL_POST_WITH_FLAGS
    else:
        # All fields >= 0.80
        state.must_block = False
        state.can_post = True
        state.partial_post_allowed = False
        state.posting_recommendation = SAFE_TO_POST

    # If there are unresolved reasons, never allow clean posting
    if state.unresolved_reasons and state.can_post:
        state.can_post = False
        state.partial_post_allowed = True
        state.posting_recommendation = PARTIAL_POST_WITH_FLAGS

    return state


# =========================================================================
# PART 7 — Date ambiguity propagation
# =========================================================================

@dataclass
class DateResolutionState:
    """Track date ambiguity and its downstream effects."""

    resolved_date: str | None = None
    date_range: list[str] = field(default_factory=list)
    date_confidence: float = 0.0
    date_affects: list[dict[str, str]] = field(default_factory=list)

    def is_ambiguous(self) -> bool:
        return self.resolved_date is None and len(self.date_range) >= 2

    def to_dict(self) -> dict[str, Any]:
        return {
            "resolved_date": self.resolved_date,
            "date_range": list(self.date_range),
            "date_confidence": self.date_confidence,
            "date_affects": list(self.date_affects),
        }


def build_date_resolution(
    raw_date: str,
    language: str | None = None,
) -> DateResolutionState:
    """Evaluate date ambiguity for DD/MM vs MM/DD formats.

    When date is ambiguous, flags all date-sensitive downstream effects.
    """
    import re

    state = DateResolutionState()

    if not raw_date or not raw_date.strip():
        state.date_confidence = 0.0
        return state

    raw_date = raw_date.strip()

    # ISO format — unambiguous
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", raw_date):
        state.resolved_date = raw_date
        state.date_confidence = 1.0
        return state

    # DD/MM/YYYY or MM/DD/YYYY
    m = re.fullmatch(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", raw_date)
    if not m:
        state.date_confidence = 0.3
        return state

    a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))

    # Unambiguous: one value > 12
    if a > 12 and 1 <= b <= 12 and 1 <= a <= 31:
        state.resolved_date = f"{y}-{b:02d}-{a:02d}"
        state.date_confidence = 1.0
        return state
    if b > 12 and 1 <= a <= 12 and 1 <= b <= 31:
        state.resolved_date = f"{y}-{a:02d}-{b:02d}"
        state.date_confidence = 1.0
        return state

    # Both <= 12 — ambiguous unless language context resolves it
    lang = (language or "").strip().lower()[:2]

    if lang == "fr":
        # French: DD/MM/YYYY
        if 1 <= b <= 12 and 1 <= a <= 31:
            state.resolved_date = f"{y}-{b:02d}-{a:02d}"
            state.date_confidence = 0.85
            return state
    elif lang == "en":
        # English: MM/DD/YYYY
        if 1 <= a <= 12 and 1 <= b <= 31:
            state.resolved_date = f"{y}-{a:02d}-{b:02d}"
            state.date_confidence = 0.85
            return state

    # Truly ambiguous — both interpretations valid
    date1 = f"{y}-{a:02d}-{b:02d}"  # MM/DD
    date2 = f"{y}-{b:02d}-{a:02d}"  # DD/MM
    state.date_range = [date1, date2]
    state.date_confidence = 0.40

    state.date_affects = [
        {
            "module": "fx_rate_selection",
            "impact_en": f"Which Bank of Canada rate? {date1} vs {date2}",
            "impact_fr": f"Quel taux de la Banque du Canada? {date1} vs {date2}",
        },
        {
            "module": "aging_bucket",
            "impact_en": f"30/60/90 day aging categorization differs between {date1} and {date2}",
            "impact_fr": f"La catégorisation 30/60/90 jours diffère entre {date1} et {date2}",
        },
        {
            "module": "duplicate_window",
            "impact_en": f"30-day duplicate detection window shifts depending on date interpretation",
            "impact_fr": f"La fenêtre de détection de doublons de 30 jours change selon l'interprétation",
        },
        {
            "module": "period_end_accrual",
            "impact_en": f"Quarter classification may differ: {date1} vs {date2}",
            "impact_fr": f"La classification trimestrielle peut différer: {date1} vs {date2}",
        },
        {
            "module": "credit_memo_status",
            "impact_en": f"Pre/post close status depends on date interpretation",
            "impact_fr": f"Le statut avant/après clôture dépend de l'interprétation de la date",
        },
    ]

    return state


# =========================================================================
# PART 8 — Structured uncertainty reason builders
# =========================================================================

def reason_vendor_name_conflict(
    ocr_text: str = "",
    alias_detected: str = "",
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="VENDOR_NAME_CONFLICT",
        description_fr="Ambiguïté OCR du nom du fournisseur ou alias détecté",
        description_en="Vendor name OCR ambiguity or alias detected",
        evidence_available=f"OCR text: {ocr_text[:100]}" if ocr_text else "No OCR text",
        evidence_needed="Verified vendor identity from source document or vendor registry",
    )


def reason_invoice_number_ocr_conflict(
    raw_number: str = "",
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="INVOICE_NUMBER_OCR_CONFLICT",
        description_fr="Numéro de facture ambigu en OCR (O vs 0, I vs 1)",
        description_en="Invoice number has OCR ambiguity (O vs 0, I vs 1)",
        evidence_available=f"Raw OCR: {raw_number}" if raw_number else "No raw number",
        evidence_needed="Original document scan or vendor confirmation of invoice number",
    )


def reason_date_ambiguous(
    raw_date: str = "",
    date_range: list[str] | None = None,
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="DATE_AMBIGUOUS",
        description_fr="Format de date ambigu — affecte la classification de période",
        description_en="Date format ambiguous — affects period classification",
        evidence_available=f"Raw date: {raw_date}, possible dates: {date_range or []}",
        evidence_needed="Vendor confirmation of invoice date or language context to resolve DD/MM vs MM/DD",
    )


def reason_allocation_gap(
    invoice_total: str = "",
    documented_value: str = "",
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="ALLOCATION_GAP_UNEXPLAINED",
        description_fr="Le total de la facture dépasse les valeurs documentées des composantes",
        description_en="Invoice total exceeds documented component values",
        evidence_available=f"Invoice total: {invoice_total}, documented: {documented_value}",
        evidence_needed="Breakdown of gap into service, shipping, insurance, customs, or discount components",
    )


def reason_tax_registration_incomplete(
    vendor: str = "",
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="TAX_REGISTRATION_INCOMPLETE",
        description_fr="Numéro TPS/TVQ du fournisseur manquant ou non vérifié",
        description_en="Vendor GST/QST number missing or unverified",
        evidence_available=f"Vendor: {vendor}" if vendor else "Unknown vendor",
        evidence_needed="GST registration number (RT####) and QST number from vendor invoice or CRA registry",
    )


def reason_settlement_unresolved(
    credit_memo_id: str = "",
    bank_deposit_amount: str = "",
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="SETTLEMENT_STATE_UNRESOLVED",
        description_fr="Note de crédit + dépôt bancaire du même montant dans la même période",
        description_en="Credit memo + bank deposit same amount same period",
        evidence_available=f"Credit memo: {credit_memo_id}, bank amount: {bank_deposit_amount}",
        evidence_needed="Accountant confirmation: two separate events, settlement, or duplicate ingestion",
    )


def reason_payee_identity_unproven(
    invoice_vendor: str = "",
    bank_payee: str = "",
    similarity: float = 0.0,
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="PAYEE_IDENTITY_UNPROVEN",
        description_fr="Le bénéficiaire bancaire diffère du fournisseur de la facture",
        description_en="Bank payee differs from invoice vendor",
        evidence_available=f"Invoice vendor: {invoice_vendor}, bank payee: {bank_payee}, similarity: {similarity:.2f}",
        evidence_needed="Confirmation that payee is authorized representative or affiliate of invoice vendor",
    )


def reason_filed_period_amendment(
    filed_period: str = "",
    trigger_document: str = "",
) -> UncertaintyReason:
    """Trap 1: Filed period needs amendment due to later evidence."""
    return UncertaintyReason(
        reason_code="FILED_PERIOD_AMENDMENT_NEEDED",
        description_fr=f"La période déclarée {filed_period} nécessite une modification suite à de nouvelles preuves",
        description_en=f"Filed period {filed_period} needs amendment due to later evidence",
        evidence_available=f"Trigger document: {trigger_document}",
        evidence_needed="Amended return to be filed with Revenu Québec",
    )


def reason_credit_memo_tax_split_unproven(
    credit_memo_id: str = "",
    decomposition_method: str = "",
) -> UncertaintyReason:
    """Trap 2: Credit memo tax split cannot be proven from the document."""
    return UncertaintyReason(
        reason_code="CREDIT_MEMO_TAX_SPLIT_UNPROVEN",
        description_fr="La ventilation de taxes de la note de crédit ne peut être prouvée",
        description_en="Credit memo tax split cannot be proven from document",
        evidence_available=f"Credit memo: {credit_memo_id}, method: {decomposition_method}",
        evidence_needed="Original vendor confirmation of GST/QST breakdown or amended invoice with tax detail",
    )


def reason_subcontractor_overlap(
    vendor_a: str = "",
    vendor_b: str = "",
    keywords: str = "",
) -> UncertaintyReason:
    """Trap 3: Cross-vendor work scope overlap detected."""
    return UncertaintyReason(
        reason_code="SUBCONTRACTOR_WORK_SCOPE_OVERLAP",
        description_fr=f"Chevauchement de portée de travail détecté entre {vendor_a} et {vendor_b}",
        description_en=f"Work scope overlap detected between {vendor_a} and {vendor_b}",
        evidence_available=f"Shared keywords: {keywords}",
        evidence_needed="Confirmation that work was performed by separate vendors for separate scopes",
    )


def reason_recognition_timing_deferred(
    document_date: str = "",
    activation_date: str = "",
) -> UncertaintyReason:
    """Trap 4: Recognition must be deferred to activation date."""
    return UncertaintyReason(
        reason_code="RECOGNITION_TIMING_DEFERRED",
        description_fr=f"La comptabilisation doit être reportée de {document_date} à {activation_date}",
        description_en=f"Recognition must be deferred from {document_date} to {activation_date}",
        evidence_available=f"Document date: {document_date}, activation: {activation_date}",
        evidence_needed="Confirmation of actual service activation date",
    )


def reason_prior_treatment_contradiction(
    original_period: str = "",
    correct_period: str = "",
) -> UncertaintyReason:
    """Trap 4: Prior period treatment contradicted by later evidence."""
    return UncertaintyReason(
        reason_code="PRIOR_TREATMENT_CONTRADICTION",
        description_fr=f"Traitement en {original_period} contredit par des preuves ultérieures (correct: {correct_period})",
        description_en=f"Treatment in {original_period} contradicted by later evidence (correct: {correct_period})",
        evidence_available=f"Originally recognized in {original_period}",
        evidence_needed=f"Correction entries to move recognition to {correct_period}",
    )


def reason_duplicate_cluster_non_head(
    cluster_head_id: str = "",
    document_id: str = "",
) -> UncertaintyReason:
    """Trap 5: Document is a duplicate cluster member, not the head."""
    return UncertaintyReason(
        reason_code="DUPLICATE_CLUSTER_NON_HEAD",
        description_fr=f"Le document {document_id} est un doublon — le chef du cluster est {cluster_head_id}",
        description_en=f"Document {document_id} is a duplicate — cluster head is {cluster_head_id}",
        evidence_available=f"Cluster head: {cluster_head_id}",
        evidence_needed="No action needed — this document should not create additional economic effects",
    )


def reason_stale_version(
    entity_type: str = "",
    expected: int = 0,
    current: int = 0,
) -> UncertaintyReason:
    """Trap 6: Stale version detected on approval."""
    return UncertaintyReason(
        reason_code="STALE_VERSION_DETECTED",
        description_fr=f"Version périmée: attendue {expected}, actuelle {current}. Rafraîchir et réessayer.",
        description_en=f"Stale version: expected {expected}, current {current}. Refresh and retry.",
        evidence_available=f"Entity: {entity_type}, expected v{expected}, actual v{current}",
        evidence_needed="User must refresh their view and re-approve with current version",
    )


def reason_manual_journal_collision(
    entry_id: str = "",
    collision_type: str = "",
) -> UncertaintyReason:
    """Trap 7: Manual journal collides with document-backed correction."""
    return UncertaintyReason(
        reason_code="MANUAL_JOURNAL_COLLISION",
        description_fr=f"Le journal manuel {entry_id} entre en conflit avec une correction documentée",
        description_en=f"Manual journal {entry_id} collides with document-backed correction",
        evidence_available=f"Entry: {entry_id}, collision: {collision_type}",
        evidence_needed="Review to confirm manual journal is needed despite existing correction",
    )


def reason_reimport_blocked(
    document_id: str = "",
) -> UncertaintyReason:
    """Trap 8: Re-import blocked after explicit rollback."""
    return UncertaintyReason(
        reason_code="REIMPORT_BLOCKED_AFTER_ROLLBACK",
        description_fr=f"Réimportation bloquée pour {document_id} après annulation explicite",
        description_en=f"Re-import blocked for {document_id} after explicit rollback",
        evidence_available=f"Document: {document_id}",
        evidence_needed="Manager override to unblock re-import, or new evidence submission",
    )


def reason_customs_note_scope_limited(
    goods_value: str = "",
    total_value: str = "",
) -> UncertaintyReason:
    return UncertaintyReason(
        reason_code="CUSTOMS_NOTE_SCOPE_LIMITED",
        description_fr="Taxe payée aux douanes s'applique uniquement à la portion des biens",
        description_en="Tax-paid-at-customs applies to goods portion only",
        evidence_available=f"Goods value: {goods_value}, total: {total_value}",
        evidence_needed="Separate GST/QST analysis for service component of invoice",
    )


# =========================================================================
# PART 11 — Posting readiness evaluation
# =========================================================================

@dataclass
class PostingDecision:
    """Result of posting readiness evaluation."""

    outcome: str  # SAFE_TO_POST, PARTIAL_POST_WITH_FLAGS, BLOCK_PENDING_REVIEW
    can_post: bool
    reviewer_notes: list[str] = field(default_factory=list)
    blocked_fields: list[dict[str, str]] = field(default_factory=list)
    uncertainty_state: UncertaintyState | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "outcome": self.outcome,
            "can_post": self.can_post,
            "reviewer_notes": list(self.reviewer_notes),
            "blocked_fields": list(self.blocked_fields),
        }
        if self.uncertainty_state:
            result["uncertainty_state"] = self.uncertainty_state.to_dict()
        return result


def evaluate_posting_readiness(
    document: dict[str, Any],
    uncertainty_state: UncertaintyState,
) -> PostingDecision:
    """Determine if a document is safe to post given its uncertainty state.

    PostingDecision has three outcomes:
    1. SAFE_TO_POST: all fields resolved, confidence >= 0.80
    2. PARTIAL_POST_WITH_FLAGS: some fields uncertain 0.60-0.79
    3. BLOCK_PENDING_REVIEW: any field < 0.60 or must_block=True
    """
    decision = PostingDecision(
        outcome=uncertainty_state.posting_recommendation,
        can_post=False,
        uncertainty_state=uncertainty_state,
    )

    if uncertainty_state.must_block:
        decision.outcome = BLOCK_PENDING_REVIEW
        decision.can_post = False
        # Show exactly which fields need resolution
        for field_name, conf in uncertainty_state.confidence_by_field.items():
            if conf < 0.60:
                decision.blocked_fields.append({
                    "field": field_name,
                    "confidence": f"{conf:.2f}",
                    "status": "BLOCKED — confidence below 0.60",
                })
        for reason in uncertainty_state.unresolved_reasons:
            decision.reviewer_notes.append(
                f"[{reason.reason_code}] {reason.description_en} / {reason.description_fr} "
                f"| Evidence: {reason.evidence_available} | Needed: {reason.evidence_needed}"
            )
        return decision

    if uncertainty_state.partial_post_allowed:
        decision.outcome = PARTIAL_POST_WITH_FLAGS
        decision.can_post = True  # Can post but with flags
        for reason in uncertainty_state.unresolved_reasons:
            decision.reviewer_notes.append(
                f"[{reason.reason_code}] {reason.description_en} / {reason.description_fr}"
            )
        for field_name, conf in uncertainty_state.confidence_by_field.items():
            if 0.60 <= conf < 0.80:
                decision.reviewer_notes.append(
                    f"Field '{field_name}' posted with uncertainty (confidence: {conf:.2f})"
                )
        return decision

    if uncertainty_state.can_post:
        decision.outcome = SAFE_TO_POST
        decision.can_post = True
        return decision

    # Fallback — should not reach here but safety net
    decision.outcome = BLOCK_PENDING_REVIEW
    decision.can_post = False
    return decision
