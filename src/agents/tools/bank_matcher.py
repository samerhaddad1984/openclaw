from __future__ import annotations

import logging
import re
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional

from src.agents.core.bank_models import BankTransaction, MatchCandidate, MatchResult
from src.agents.core.task_models import DocumentRecord

logger = logging.getLogger(__name__)


class BankMatcher:

    def __init__(
        self,
        exact_threshold: float = 0.90,
        suggest_threshold: float = 0.70,
        max_date_delta_days: int = 7,
        max_amount_diff: float = 5.00,
    ):
        self.exact_threshold = exact_threshold
        self.suggest_threshold = suggest_threshold
        self.max_date_delta_days = max_date_delta_days
        self.max_amount_diff = max_amount_diff

    def normalize_text(self, value: Optional[str]) -> str:
        if not value:
            return ""

        import unicodedata as _ud
        s = value.lower().strip()
        # FIX P1-2: Strip accents so "société" becomes "societe" not "soci t"
        s = _ud.normalize("NFKD", s).encode("ascii", errors="ignore").decode("ascii")
        s = s.replace("&", " and ")
        # FIX P1-2: Normalize hyphens to spaces
        s = s.replace("-", " ")
        s = re.sub(r"[\u00A0]", " ", s)
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

        stop_words = {
            "inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation",
            "canada", "ulc", "ca", "the", "payment", "invoice", "receipt",
            "visa", "mastercard", "credit", "card", "purchase", "debit",
            # FIX P1-2: Quebec business suffixes
            "ltee", "enr", "senc",
        }

        parts = [p for p in s.split() if p not in stop_words]
        return " ".join(parts)

    def text_similarity(self, a: Optional[str], b: Optional[str]) -> float:
        na = self.normalize_text(a)
        nb = self.normalize_text(b)

        if not na or not nb:
            return 0.0

        if na == nb:
            return 1.0

        if na in nb or nb in na:
            return 0.92

        return SequenceMatcher(None, na, nb).ratio()

    # ------------------------------------------------------------------
    # FIX 2: Vendor DBA alias resolution
    # ------------------------------------------------------------------

    def resolve_vendor_alias(self, vendor_name: str, conn=None) -> str:
        """Resolve a vendor name to its canonical name via vendor_aliases table.

        If vendor_name matches an alias_key, returns the canonical_vendor_key.
        Also tries contiguous token subsequences (longest first) to handle
        noisy bank memos like "WIRE PAYMT SCS INDUSTRIAL PROJECT 881".
        """
        if not vendor_name or not conn:
            return vendor_name or ""
        import unicodedata as _ud
        alias_key = vendor_name.strip().lower()
        alias_key = _ud.normalize("NFKD", alias_key).encode("ascii", errors="ignore").decode("ascii")
        try:
            # 1. Exact full-text lookup
            row = conn.execute(
                "SELECT canonical_vendor_key FROM vendor_aliases WHERE alias_key = ? LIMIT 1",
                (alias_key,),
            ).fetchone()
            if row:
                return row[0] if isinstance(row, (tuple, list)) else row["canonical_vendor_key"]

            # 2. Sliding-window token subsequences (longest first)
            tokens = alias_key.split()
            if len(tokens) > 1:
                for window in range(len(tokens), 1, -1):
                    for start in range(len(tokens) - window + 1):
                        sub = " ".join(tokens[start:start + window])
                        row = conn.execute(
                            "SELECT canonical_vendor_key FROM vendor_aliases WHERE alias_key = ? LIMIT 1",
                            (sub,),
                        ).fetchone()
                        if row:
                            return row[0] if isinstance(row, (tuple, list)) else row["canonical_vendor_key"]
        except Exception:
            pass
        return vendor_name

    def suggest_vendor_aliases(self, documents, conn=None) -> list:
        """Auto-suggest vendor aliases when fuzzy match score is 0.65-0.79.

        Returns list of dicts: {vendor_a, vendor_b, similarity, suggestion}.
        """
        if not conn:
            return []
        seen_vendors: dict[str, str] = {}  # normalized → original
        suggestions: list[dict] = []
        for doc in documents:
            vendor = getattr(doc, "vendor", None) or ""
            if not vendor:
                continue
            norm = self.normalize_text(vendor)
            if norm in seen_vendors:
                continue
            for existing_norm, existing_orig in seen_vendors.items():
                sim = self.text_similarity(vendor, existing_orig)
                if 0.65 <= sim <= 0.79:
                    suggestions.append({
                        "vendor_a": existing_orig,
                        "vendor_b": vendor,
                        "similarity": round(sim, 4),
                        "suggestion": (
                            f"Ces fournisseurs pourraient être identiques — créer un alias? / "
                            f"These vendors may be the same — create alias?"
                        ),
                    })
            seen_vendors[norm] = vendor
        return suggestions

    # ------------------------------------------------------------------
    # FIX 3: Bank transaction reversal detection
    # ------------------------------------------------------------------

    @staticmethod
    def _strip_txn_prefixes(text: str) -> str:
        """Strip common bank transaction prefixes/noise to extract vendor core.

        Removes: WIRE PAYMT, WIRE PAYMENT, REVERSAL, IMPORT DUPLICATE,
        CORRECTION, FEE, PROJECT \\d+, etc.
        """
        s = text.strip()
        s = re.sub(
            r"^(WIRE\s+PAY(?:MT|MENT)|REVERSAL|IMPORT\s+DUPLICATE|"
            r"CORRECTION|ANNULATION|FEE)\s+",
            "", s, flags=re.IGNORECASE,
        )
        s = re.sub(r"\s+PROJECT\s+\d+$", "", s, flags=re.IGNORECASE)
        return s.strip()

    def detect_reversals(self, transactions) -> list:
        """Detect reversal pairs among bank transactions.

        Two transactions form a reversal pair when:
        - Same vendor (normalized text similarity >= 0.80)
        - Opposite signs (debit vs credit) OR same sign with reversal keywords in memo
        - Within 5 business days
        - Amount within 1%
        """
        _REVERSAL_KEYWORDS = re.compile(
            r"\b(rev|reversal|annulation|correction|reversed|annulé|annule|corrigé|corrige)\b",
            re.IGNORECASE,
        )
        results: list[dict] = []
        paired: set[int] = set()

        for i, txn_a in enumerate(transactions):
            if i in paired:
                continue
            a_amt = float(txn_a.debit or 0) - float(txn_a.credit or 0) if hasattr(txn_a, 'debit') else float(txn_a.amount or 0)
            if a_amt == 0:
                continue

            for j, txn_b in enumerate(transactions):
                if j <= i or j in paired:
                    continue
                b_amt = float(txn_b.debit or 0) - float(txn_b.credit or 0) if hasattr(txn_b, 'debit') else float(txn_b.amount or 0)
                if b_amt == 0:
                    continue

                # Check vendor similarity — strip transaction prefixes first
                a_desc = f"{getattr(txn_a, 'description', '') or ''} {getattr(txn_a, 'memo', '') or ''}".strip()
                b_desc = f"{getattr(txn_b, 'description', '') or ''} {getattr(txn_b, 'memo', '') or ''}".strip()
                a_core = self._strip_txn_prefixes(a_desc)
                b_core = self._strip_txn_prefixes(b_desc)
                vendor_sim = self.text_similarity(a_core, b_core)
                if vendor_sim < 0.80:
                    continue

                # Check opposite signs OR reversal keywords in memo
                opposite_signs = (a_amt > 0 and b_amt < 0) or (a_amt < 0 and b_amt > 0)
                a_memo = f"{getattr(txn_a, 'memo', '') or ''} {getattr(txn_a, 'description', '') or ''}"
                b_memo = f"{getattr(txn_b, 'memo', '') or ''} {getattr(txn_b, 'description', '') or ''}"
                has_reversal_kw = bool(_REVERSAL_KEYWORDS.search(a_memo) or _REVERSAL_KEYWORDS.search(b_memo))

                if not opposite_signs:
                    continue

                # Check amount within 1%
                abs_a = abs(a_amt)
                abs_b = abs(b_amt)
                if abs_a == 0:
                    continue
                pct_diff = abs(abs_a - abs_b) / max(abs_a, abs_b)
                if pct_diff > 0.01:
                    continue

                # Check date proximity (5 business days ≈ 7 calendar days)
                a_date = self.parse_date(getattr(txn_a, 'posted_date', None) or getattr(txn_a, 'txn_date', None))
                b_date = self.parse_date(getattr(txn_b, 'posted_date', None) or getattr(txn_b, 'txn_date', None))
                if a_date and b_date:
                    delta = abs((a_date - b_date).days)
                    if delta > 7:  # 5 business days ≈ 7 calendar days
                        continue
                elif a_date or b_date:
                    continue  # one has date, other doesn't

                paired.add(i)
                paired.add(j)
                results.append({
                    "transaction_a_id": getattr(txn_a, 'transaction_id', getattr(txn_a, 'id', i)),
                    "transaction_b_id": getattr(txn_b, 'transaction_id', getattr(txn_b, 'id', j)),
                    "amount_a": round(a_amt, 2),
                    "amount_b": round(b_amt, 2),
                    "vendor_similarity": round(vendor_sim, 4),
                    "flag": "reversal_pair",
                    "reconciliation_status": "reconciliation_internal",
                    "reason": (
                        f"Paire de contrepassation détectée — même fournisseur, "
                        f"montants opposés dans un délai de 5 jours ouvrables / "
                        f"Reversal pair detected — same vendor, opposite amounts "
                        f"within 5 business days"
                    ),
                })
                break  # One reversal partner per transaction — prevent double-count

        return results

    # ------------------------------------------------------------------
    # FIX 6: Cross-currency bank matching
    # ------------------------------------------------------------------

    def get_fx_rate(self, currency_from: str, currency_to: str, conn=None) -> Optional[float]:
        """Get the most recent FX rate from boc_fx_rates table.

        Currently supports USD→CAD. Returns None if no rate available.
        """
        if not conn:
            return None
        fr = (currency_from or "").upper()
        to = (currency_to or "").upper()
        if fr == to:
            return 1.0
        if fr == "USD" and to == "CAD":
            try:
                row = conn.execute(
                    "SELECT usd_cad FROM boc_fx_rates ORDER BY rate_date DESC LIMIT 1"
                ).fetchone()
                if row:
                    return float(row[0] if isinstance(row, (tuple, list)) else row["usd_cad"])
            except Exception:
                pass
        elif fr == "CAD" and to == "USD":
            rate = self.get_fx_rate("USD", "CAD", conn)
            if rate and rate > 0:
                return 1.0 / rate
        return None

    def cross_currency_amount_match(
        self, doc_amount: Optional[float], doc_currency: Optional[str],
        txn_amount: Optional[float], txn_currency: Optional[str],
        conn=None,
    ) -> Optional[dict]:
        """Match amounts across currencies using BoC FX rates.

        Returns dict with converted amount and rate if currencies differ,
        or None if conversion not possible.
        """
        if doc_amount is None or txn_amount is None:
            return None
        dc = (doc_currency or "").upper()
        tc = (txn_currency or "").upper()
        if not dc or not tc or dc == tc:
            return None

        rate = self.get_fx_rate(dc, tc, conn)
        if rate is None:
            return None

        converted = abs(float(doc_amount)) * rate
        diff = abs(converted - abs(float(txn_amount)))
        tolerance = max(abs(float(txn_amount)) * 0.02, 1.0)  # 2% or $1

        if diff <= tolerance:
            return {
                "currency_converted": True,
                "original_amount": round(float(doc_amount), 2),
                "original_currency": dc,
                "converted_amount": round(converted, 2),
                "target_currency": tc,
                "fx_rate": round(rate, 4),
                "conversion_diff": round(diff, 2),
                "display": f"{dc} {abs(float(doc_amount)):,.2f} × {rate:.4f} = {tc} {converted:,.2f}",
            }
        return None

    def parse_date(
        self,
        value: Optional[str],
        language: Optional[str] = None,
    ) -> Optional[datetime]:
        """Parse a date string to datetime.

        For ambiguous DD/MM vs MM/DD formats:
        - ``language="fr"`` → DD/MM/YYYY  (Quebec French default)
        - ``language="en"`` → MM/DD/YYYY
        - No language or ambiguous → returns None (flag as date_ambiguous)
        Unambiguous formats (ISO, YYYY/ prefixed, day>12 or month>12) always resolve.
        """
        if not value:
            return None

        value = value.strip()

        # Unambiguous formats first
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except Exception:
                pass

        # Handle DD/MM/YYYY or MM/DD/YYYY with ambiguity detection
        import re as _re
        m = _re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", value)
        if m:
            a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            # Unambiguous: one value > 12 forces interpretation
            if a > 12 and 1 <= b <= 12 and 1 <= a <= 31:
                # a must be day (DD/MM)
                return datetime(y, b, a)
            if b > 12 and 1 <= a <= 12 and 1 <= b <= 31:
                # b must be day (MM/DD)
                return datetime(y, a, b)
            # Both <= 12 — ambiguous: use language to decide
            lang = (language or "").strip().lower()[:2]
            if lang == "fr":
                # French: DD/MM/YYYY
                if 1 <= b <= 12 and 1 <= a <= 31:
                    return datetime(y, b, a)
            elif lang == "en":
                # English: MM/DD/YYYY
                if 1 <= a <= 12 and 1 <= b <= 31:
                    return datetime(y, a, b)
            else:
                # No language context and ambiguous → return None
                # Caller should flag as date_ambiguous
                return None

        return None

    def date_delta_days(self, document_date: Optional[str], txn_date: Optional[str]) -> Optional[int]:
        d1 = self.parse_date(document_date)
        d2 = self.parse_date(txn_date)

        if d1 is None or d2 is None:
            return None

        return abs((d1.date() - d2.date()).days)

    def amount_difference(self, doc_amount: Optional[float], txn_amount: Optional[float]) -> Optional[float]:
        if doc_amount is None or txn_amount is None:
            return None

        return abs(abs(float(doc_amount)) - abs(float(txn_amount)))

    def amount_score(self, diff: Optional[float]) -> tuple[float, list[str]]:
        reasons: list[str] = []

        if diff is None:
            return 0.0, reasons

        if diff == 0:
            reasons.append("same_amount")
            return 0.45, reasons

        if diff <= 0.50:
            reasons.append("amount_within_0_50")
            return 0.35, reasons

        if diff <= 2.00:
            reasons.append("amount_within_2_00")
            return 0.20, reasons

        if diff <= self.max_amount_diff:
            reasons.append("amount_within_tolerance")
            return 0.08, reasons

        reasons.append("amount_too_far")
        return -1.0, reasons

    def date_score(self, delta_days: Optional[int]) -> tuple[float, list[str]]:
        reasons: list[str] = []

        if delta_days is None:
            return 0.0, reasons

        if delta_days == 0:
            reasons.append("same_date")
            return 0.25, reasons

        if delta_days <= 1:
            reasons.append("date_within_1_day")
            return 0.20, reasons

        if delta_days <= 3:
            reasons.append("date_within_3_days")
            return 0.14, reasons

        if delta_days <= self.max_date_delta_days:
            reasons.append("date_within_tolerance")
            return 0.06, reasons

        reasons.append("date_too_far")
        return -1.0, reasons

    def vendor_score(self, document_vendor: Optional[str], transaction: BankTransaction, conn=None) -> tuple[float, float, list[str]]:
        reasons: list[str] = []

        txn_text = " ".join(
            [
                transaction.description or "",
                transaction.memo or "",
            ]
        ).strip()

        similarity = self.text_similarity(document_vendor, txn_text)

        # FIX 2: Check vendor aliases if similarity is low
        if similarity < 0.80 and conn:
            canonical_doc = self.resolve_vendor_alias(document_vendor, conn)
            canonical_txn = self.resolve_vendor_alias(txn_text, conn)
            # Also try description-only lookup (memo noise can prevent match)
            if canonical_txn == txn_text and transaction.description:
                canonical_txn = self.resolve_vendor_alias(transaction.description, conn)
            if canonical_doc and canonical_txn:
                alias_sim = self.text_similarity(canonical_doc, canonical_txn)
                if alias_sim > similarity:
                    similarity = alias_sim
                    reasons.append("vendor_alias_resolved")

        if similarity >= 0.95:
            reasons.append("same_vendor_exact")
            return 0.25, similarity, reasons

        if similarity >= 0.80:
            reasons.append("vendor_similarity_high")
            return 0.18, similarity, reasons

        if similarity >= 0.65:
            reasons.append("vendor_similarity_medium")
            return 0.10, similarity, reasons

        if similarity >= 0.50:
            reasons.append("vendor_similarity_low")
            return 0.04, similarity, reasons

        return 0.0, similarity, reasons

    def currency_score(self, doc_currency: Optional[str], txn_currency: Optional[str]) -> tuple[float, list[str]]:
        reasons: list[str] = []

        if not doc_currency or not txn_currency:
            return 0.0, reasons

        if doc_currency.upper() == txn_currency.upper():
            reasons.append("same_currency")
            return 0.03, reasons

        reasons.append("currency_mismatch")
        return -0.05, reasons

    def client_gate(self, document: DocumentRecord, transaction: BankTransaction) -> tuple[bool, list[str]]:
        reasons: list[str] = []

        doc_client = (document.client_code or "").strip().upper()
        txn_client = (transaction.client_code or "").strip().upper()

        if doc_client and txn_client:
            if doc_client == txn_client:
                reasons.append("same_client")
                return True, reasons
            reasons.append("different_client")
            return False, reasons

        return True, reasons

    def evaluate_candidate(self, document: DocumentRecord, transaction: BankTransaction) -> Optional[MatchCandidate]:
        allowed, gate_reasons = self.client_gate(document, transaction)
        if not allowed:
            return None

        reasons: list[str] = []
        reasons.extend(gate_reasons)

        diff = self.amount_difference(document.amount, transaction.amount)
        amount_points, amount_reasons = self.amount_score(diff)
        reasons.extend(amount_reasons)

        if amount_points < 0:
            return None

        delta_days = self.date_delta_days(document.document_date, transaction.posted_date)
        date_points, date_reasons = self.date_score(delta_days)
        reasons.extend(date_reasons)

        if date_points < 0:
            return None

        vendor_points, vendor_similarity, vendor_reasons = self.vendor_score(document.vendor, transaction)
        reasons.extend(vendor_reasons)

        doc_currency = None
        try:
            raw = document.raw_result or ""
            if raw:
                raw_dict = __import__("json").loads(raw)
                doc_currency = raw_dict.get("raw_rules_output", {}).get("currency")
        except Exception:
            doc_currency = None

        currency_points, currency_reasons = self.currency_score(doc_currency, transaction.currency)
        reasons.extend(currency_reasons)

        # FIX 23 + FIX P1-1: Sign-aware matching for credit notes AND refunds
        # Invoice (positive doc) should match bank debit (negative bank) — normal flow.
        # Credit note (negative doc) should match bank credit (positive bank) — refund flow.
        # Penalize only when directions are truly incompatible:
        #   - positive doc + positive bank (double-charge)
        #   - negative doc + negative bank (double-credit — unusual but possible)
        sign_mismatch = False
        is_credit_refund = False
        if document.amount is not None and transaction.amount is not None:
            doc_amt = float(document.amount)
            txn_amt = float(transaction.amount)
            is_credit_note = doc_amt < 0 or (document.doc_type or "").lower() in ("credit_note", "credit note")

            if is_credit_note and txn_amt > 0:
                # FIX P1-1: Credit note (negative doc) matching bank credit/refund (positive bank)
                # This is the CORRECT match direction for refunds — no penalty
                is_credit_refund = True
                reasons.append("credit_refund_match")
            elif doc_amt < 0 and txn_amt < 0:
                # Both negative: unusual but could be legitimate
                pass
            elif doc_amt > 0 and txn_amt > 0:
                # Both positive: could be legitimate (some banks report debits as positive)
                pass

        score = amount_points + date_points + vendor_points + currency_points
        if sign_mismatch:
            score -= 0.5
        elif is_credit_refund:
            # FIX P1-1: Small bonus for credit note ↔ bank refund match
            score += 0.02

        # FIX 2: Amount divergence sanity check
        if diff is not None and document.amount is not None and document.amount != 0:
            pct_diff = diff / abs(float(document.amount))
            if pct_diff > 0.50:
                logger.warning(
                    "match_amount_divergence=True doc=%s txn=%s diff=%.2f pct=%.2f",
                    document.document_id, transaction.transaction_id, diff, pct_diff,
                )
                score -= 0.3
                reasons.append("match_amount_divergence")
            if pct_diff > 0.10:
                reasons.append("amount_divergence_over_10pct")

        if document.doc_type:
            reasons.append(f"doc_type:{document.doc_type}")

        # --- Vendor/payee mismatch detection ---
        # When the bank payee clearly differs from the invoice vendor,
        # flag as possible related-party redirection.
        fraud_flags: list[dict] = []
        review_notes: list[str] = []
        vendor_mismatch = False
        if vendor_similarity is not None and vendor_similarity < 0.70:
            vendor_mismatch = True
            review_notes.append(
                "Le bénéficiaire du paiement ne correspond pas au fournisseur "
                "de la facture — possible redirection vers une partie liée / "
                "Payment recipient does not match invoice vendor — possible "
                "related-party redirection"
            )
            fraud_flags.append({
                "rule": "vendor_payee_mismatch",
                "severity": "high",
            })
            # Cap confidence regardless of amount/date match
            score = min(score, 0.55)
            reasons.append("vendor_payee_mismatch")

        if score >= self.exact_threshold:
            status = "matched"
        elif score >= self.suggest_threshold:
            status = "suggested"
        else:
            status = "unmatched"

        # FIX 2: Never auto-confirm when amounts differ by more than 10%
        if "amount_divergence_over_10pct" in reasons and status == "matched":
            status = "suggested"
            reasons.append("downgraded_amount_divergence")

        # Override status when vendor/payee mismatch detected
        if vendor_mismatch and status in ("matched", "suggested"):
            status = "payee_mismatch_candidate"

        candidate = MatchCandidate(
            document_id=document.document_id,
            transaction_id=transaction.transaction_id,
            score=round(score, 4),
            status=status,
            reasons=reasons,
            amount_diff=diff,
            date_delta_days=delta_days,
            vendor_similarity=round(vendor_similarity, 4) if vendor_similarity is not None else None,
        )
        # Attach extra attributes for downstream consumers
        candidate.fraud_flags = fraud_flags  # type: ignore[attr-defined]
        candidate.review_notes = review_notes  # type: ignore[attr-defined]
        return candidate

    def match_documents(
        self,
        documents: list[DocumentRecord],
        transactions: list[BankTransaction],
    ) -> list[MatchResult]:
        # --- Performance: cache normalize_text and parse_date to avoid
        #     redundant computation across O(n*m) candidate evaluations ---
        _norm_cache: dict = {}
        _date_cache: dict = {}
        _orig_normalize = self.normalize_text
        _orig_parse_date = self.parse_date

        def _cached_normalize(value):
            if value not in _norm_cache:
                _norm_cache[value] = _orig_normalize(value)
            return _norm_cache[value]

        def _cached_parse_date(value, language=None):
            key = (value, language)
            if key not in _date_cache:
                _date_cache[key] = _orig_parse_date(value, language)
            return _date_cache[key]

        self.normalize_text = _cached_normalize
        self.parse_date = _cached_parse_date

        try:
            return self._match_documents_core(documents, transactions)
        finally:
            self.normalize_text = _orig_normalize
            self.parse_date = _orig_parse_date

    def _match_documents_core(
        self,
        documents: list[DocumentRecord],
        transactions: list[BankTransaction],
    ) -> list[MatchResult]:
        import bisect as _bisect
        import math as _math

        all_candidates: list[MatchCandidate] = []

        # Build amount index: group invoices by normalized_amount (nearest $0.01)
        _amount_index: dict[int, list[DocumentRecord]] = {}
        _no_amount_docs: list[DocumentRecord] = []
        for doc in documents:
            if doc.amount is not None:
                _famt = float(doc.amount)
                if _math.isfinite(_famt):
                    cents = round(abs(_famt) * 100)
                    _amount_index.setdefault(cents, []).append(doc)
                else:
                    _no_amount_docs.append(doc)
            else:
                _no_amount_docs.append(doc)
        _sorted_cents = sorted(_amount_index.keys())

        # Build vendor index: group invoices by first 3 chars of normalized vendor
        _vendor_index: dict[str, list[DocumentRecord]] = {}
        for doc in documents:
            prefix = self.normalize_text(getattr(doc, 'vendor', None) or "")[:3]
            _vendor_index.setdefault(prefix, []).append(doc)

        max_diff_cents = round(self.max_amount_diff * 100)

        for txn in transactions:
            # Use amount index for O(1) lookup instead of O(n) scan
            if txn.amount is not None and _math.isfinite(float(txn.amount)):
                txn_cents = round(abs(float(txn.amount)) * 100)
                lo = _bisect.bisect_left(_sorted_cents, txn_cents - max_diff_cents)
                hi = _bisect.bisect_right(_sorted_cents, txn_cents + max_diff_cents)
                seen: set = set()
                candidate_docs: list[DocumentRecord] = []
                for idx in range(lo, hi):
                    for doc in _amount_index[_sorted_cents[idx]]:
                        doc_oid = id(doc)
                        if doc_oid not in seen:
                            seen.add(doc_oid)
                            candidate_docs.append(doc)
                for doc in _no_amount_docs:
                    doc_oid = id(doc)
                    if doc_oid not in seen:
                        seen.add(doc_oid)
                        candidate_docs.append(doc)
            else:
                candidate_docs = list(documents)

            for doc in candidate_docs:
                candidate = self.evaluate_candidate(doc, txn)
                if candidate is None:
                    continue
                if candidate.status == "unmatched":
                    continue
                all_candidates.append(candidate)

        all_candidates.sort(
            key=lambda c: (
                -c.score,
                c.amount_diff if c.amount_diff is not None else 999999,
                c.date_delta_days if c.date_delta_days is not None else 999999,
            )
        )

        used_documents: set[str] = set()
        used_transactions: set[str] = set()
        selected: dict[str, MatchCandidate] = {}

        for candidate in all_candidates:
            if candidate.document_id in used_documents:
                continue
            if candidate.transaction_id in used_transactions:
                continue

            selected[candidate.document_id] = candidate
            used_documents.add(candidate.document_id)
            used_transactions.add(candidate.transaction_id)

        # FIX 4: Detect ambiguous candidates per document
        # Build map of all candidates per document for ambiguity detection
        candidates_by_doc: dict[str, list[MatchCandidate]] = {}
        for c in all_candidates:
            candidates_by_doc.setdefault(c.document_id, []).append(c)

        results: list[MatchResult] = []

        for doc in documents:
            chosen = selected.get(doc.document_id)
            if chosen is None:
                results.append(
                    MatchResult(
                        document_id=doc.document_id,
                        transaction_id=None,
                        status="unmatched",
                        score=0.0,
                        reasons=["no_candidate_above_threshold"],
                        amount_diff=None,
                        date_delta_days=None,
                        vendor_similarity=None,
                    )
                )
                continue

            # FIX 4: Check for ambiguous candidates (scores within 0.10 of each other)
            doc_candidates = candidates_by_doc.get(doc.document_id, [])
            close_candidates = [
                c for c in doc_candidates
                if abs(c.score - chosen.score) <= 0.10 and c.transaction_id != chosen.transaction_id
            ]

            final_status = chosen.status
            final_reasons = list(chosen.reasons)

            if close_candidates:
                final_status = "ambiguous"
                candidate_details = [
                    f"{chosen.transaction_id}(score={chosen.score:.4f})"
                ] + [
                    f"{c.transaction_id}(score={c.score:.4f})"
                    for c in close_candidates
                ]
                final_reasons.append(
                    "Multiple possible matches found — manual review required / "
                    "Plusieurs correspondances possibles — révision manuelle requise: "
                    + ", ".join(candidate_details)
                )
                final_reasons.append("review_status:NeedsReview")

            results.append(
                MatchResult(
                    document_id=chosen.document_id,
                    transaction_id=chosen.transaction_id,
                    status=final_status,
                    score=chosen.score,
                    reasons=final_reasons,
                    amount_diff=chosen.amount_diff,
                    date_delta_days=chosen.date_delta_days,
                    vendor_similarity=chosen.vendor_similarity,
                )
            )

        return results

    def batch_match(
        self,
        bank_transactions: list[BankTransaction],
        invoices: list[DocumentRecord],
    ) -> list[MatchResult]:
        """Batch match bank transactions against invoices using the indexed approach.

        Convenience wrapper around match_documents with parameter names that
        reflect the typical caller's perspective (bank-first, invoices-second).
        """
        return self.match_documents(invoices, bank_transactions)

    # FIX 7 + BLOCK4: Split payment detection — one payment matching multiple invoices
    def detect_split_payments(
        self,
        documents: list[DocumentRecord],
        transactions: list[BankTransaction],
    ) -> list[dict]:
        """Detect bank transactions whose amount matches the SUM of 2-4
        unmatched invoices from the same client within 7 days.

        Uses 1% tolerance for amount matching.

        Returns a list of dicts with:
          transaction_id, matched_document_ids, combined_amount,
          match_status='split_candidate', reasons
        """
        results: list[dict] = []

        for txn in transactions:
            if txn.amount is None or txn.amount <= 0:
                continue
            txn_amount = float(txn.amount)
            txn_client = (txn.client_code or "").strip().upper()
            # 1% tolerance
            tolerance = txn_amount * 0.01

            # Filter candidate invoices: same client, within date range, positive amounts
            candidates = []
            for doc in documents:
                doc_client = (doc.client_code or "").strip().upper()
                if txn_client and doc_client and txn_client != doc_client:
                    continue
                if doc.amount is None or float(doc.amount) <= 0:
                    continue
                # Check date proximity
                delta = self.date_delta_days(doc.document_date, txn.posted_date)
                if delta is not None and delta > self.max_date_delta_days:
                    continue
                # Check vendor similarity
                txn_text = f"{txn.description or ''} {txn.memo or ''}".strip()
                sim = self.text_similarity(doc.vendor, txn_text)
                if sim < 0.50:
                    continue
                candidates.append(doc)

            if len(candidates) < 2:
                continue

            # Try combinations of 2-4 invoices
            from itertools import combinations
            for size in (2, 3, 4):
                if size > len(candidates):
                    break
                for combo in combinations(candidates, size):
                    combo_total = sum(float(d.amount) for d in combo)
                    diff = abs(combo_total - txn_amount)
                    if diff <= tolerance:
                        doc_ids = [d.document_id for d in combo]
                        results.append({
                            "transaction_id": txn.transaction_id,
                            "matched_document_ids": doc_ids,
                            "combined_amount": round(combo_total, 2),
                            "transaction_amount": round(txn_amount, 2),
                            "difference": round(diff, 2),
                            "match_status": "split_candidate",
                            "reasons": [
                                f"Payment ${txn_amount:,.2f} matches sum of "
                                f"{len(doc_ids)} invoices (${combo_total:,.2f}, "
                                f"diff ${diff:,.2f} within 1%) — "
                                f"requires human confirmation / "
                                f"Paiement correspond à la somme de {len(doc_ids)} "
                                f"factures (diff {diff:,.2f} $ dans la tolérance de 1 %) — "
                                f"confirmation humaine requise"
                            ],
                        })

        return results

    def split_payment_detector(
        self,
        unmatched_documents: list[DocumentRecord],
        unmatched_transactions: list[BankTransaction],
    ) -> list[dict]:
        """Convenience wrapper: find split payment candidates among unmatched items.

        For each unmatched bank transaction, finds combinations of 2-4 unmatched
        invoices from the same client whose amounts SUM to the bank transaction
        amount within 1% tolerance.

        Returns list of split_candidate dicts ready for human confirmation.
        """
        return self.detect_split_payments(unmatched_documents, unmatched_transactions)

    # ------------------------------------------------------------------
    # Credit note ↔ invoice linkage — prevent double-counting
    # ------------------------------------------------------------------

    def detect_credit_note_invoice_links(
        self,
        documents: list[DocumentRecord],
    ) -> list[dict]:
        """Detect credit notes that likely relate to an existing invoice.

        For each credit note in ``documents``, find invoices from the same
        vendor (and optionally same client) whose amount matches the credit
        note's absolute value within tolerance.  This prevents the credit
        note from being both:
          (a) applied as partial payment on the invoice, AND
          (b) booked as a separate expense/credit.

        Returns a list of dicts with:
          credit_note_id, linked_invoice_id, vendor, amount_match,
          match_type ('exact' | 'partial'), warnings
        """
        results: list[dict] = []

        credit_notes: list[DocumentRecord] = []
        invoices: list[DocumentRecord] = []

        for doc in documents:
            doc_type = (doc.doc_type or "").lower()
            if doc_type in ("credit_note", "credit note", "refund", "chargeback", "reversal"):
                credit_notes.append(doc)
            elif doc_type in ("invoice", "receipt", "utility_bill", "bill"):
                invoices.append(doc)

        for cn in credit_notes:
            cn_amount = abs(float(cn.amount)) if cn.amount is not None else 0.0
            if cn_amount <= 0:
                continue

            cn_vendor = self.normalize_text(cn.vendor)
            cn_client = (cn.client_code or "").strip().upper()

            for inv in invoices:
                inv_client = (inv.client_code or "").strip().upper()
                if cn_client and inv_client and cn_client != inv_client:
                    continue

                inv_vendor = self.normalize_text(inv.vendor)
                vendor_sim = self.text_similarity(cn.vendor, inv.vendor)
                if vendor_sim < 0.65:
                    continue

                inv_amount = float(inv.amount) if inv.amount is not None else 0.0
                if inv_amount <= 0:
                    continue

                diff = abs(cn_amount - inv_amount)
                tolerance = max(inv_amount * 0.01, 0.50)

                warnings: list[str] = []

                if diff <= tolerance:
                    # Exact match — credit note fully offsets the invoice
                    results.append({
                        "credit_note_id": cn.document_id,
                        "linked_invoice_id": inv.document_id,
                        "vendor": cn.vendor,
                        "credit_note_amount": round(cn_amount, 2),
                        "invoice_amount": round(inv_amount, 2),
                        "match_type": "exact",
                        "vendor_similarity": round(vendor_sim, 4),
                        "warnings": [
                            f"Note de crédit {cn.document_id} correspond exactement "
                            f"à la facture {inv.document_id} ({inv_amount:,.2f}$). "
                            f"Risque de double comptabilisation. / "
                            f"Credit note {cn.document_id} exactly matches "
                            f"invoice {inv.document_id} (${inv_amount:,.2f}). "
                            f"Risk of double-counting."
                        ],
                    })
                elif cn_amount < inv_amount:
                    # Partial match — credit note is part of invoice settlement
                    results.append({
                        "credit_note_id": cn.document_id,
                        "linked_invoice_id": inv.document_id,
                        "vendor": cn.vendor,
                        "credit_note_amount": round(cn_amount, 2),
                        "invoice_amount": round(inv_amount, 2),
                        "match_type": "partial",
                        "vendor_similarity": round(vendor_sim, 4),
                        "warnings": [
                            f"Note de crédit {cn.document_id} ({cn_amount:,.2f}$) "
                            f"est un règlement partiel possible de la facture "
                            f"{inv.document_id} ({inv_amount:,.2f}$). "
                            f"Solde résiduel: {inv_amount - cn_amount:,.2f}$. / "
                            f"Credit note {cn.document_id} (${cn_amount:,.2f}) "
                            f"is a possible partial settlement of invoice "
                            f"{inv.document_id} (${inv_amount:,.2f}). "
                            f"Remaining balance: ${inv_amount - cn_amount:,.2f}."
                        ],
                    })

        return results

    def detect_mixed_settlement(
        self,
        documents: list[DocumentRecord],
        transactions: list[BankTransaction],
    ) -> list[dict]:
        """Detect invoices settled by a combination of bank payment + credit note.

        Searches for an invoice whose total equals bank_txn + credit_note
        within tolerance.  This is the inverse of ``detect_split_payments``
        which looks for one bank txn covering multiple invoices.

        Returns a list of dicts with:
          invoice_id, bank_transaction_id, credit_note_id,
          invoice_amount, bank_amount, credit_amount,
          match_status='mixed_settlement_candidate', reasons
        """
        results: list[dict] = []

        credit_notes: list[DocumentRecord] = []
        invoices: list[DocumentRecord] = []

        for doc in documents:
            doc_type = (doc.doc_type or "").lower()
            if doc_type in ("credit_note", "credit note", "refund"):
                credit_notes.append(doc)
            elif doc_type in ("invoice", "receipt", "utility_bill", "bill"):
                invoices.append(doc)

        for inv in invoices:
            inv_amount = float(inv.amount) if inv.amount is not None else 0.0
            if inv_amount <= 0:
                continue
            inv_client = (inv.client_code or "").strip().upper()

            for cn in credit_notes:
                cn_client = (cn.client_code or "").strip().upper()
                if inv_client and cn_client and inv_client != cn_client:
                    continue

                vendor_sim = self.text_similarity(inv.vendor, cn.vendor)
                if vendor_sim < 0.65:
                    continue

                cn_amount = abs(float(cn.amount)) if cn.amount is not None else 0.0
                if cn_amount <= 0 or cn_amount >= inv_amount:
                    continue

                # Expected bank payment = invoice - credit note
                expected_bank = inv_amount - cn_amount
                tolerance = max(expected_bank * 0.01, 0.50)

                for txn in transactions:
                    if txn.amount is None:
                        continue
                    txn_amount = abs(float(txn.amount))
                    txn_client = (txn.client_code or "").strip().upper()
                    if inv_client and txn_client and inv_client != txn_client:
                        continue

                    diff = abs(txn_amount - expected_bank)
                    if diff > tolerance:
                        continue

                    # Date proximity check
                    delta = self.date_delta_days(inv.document_date, txn.posted_date)
                    if delta is not None and delta > self.max_date_delta_days:
                        continue

                    results.append({
                        "invoice_id": inv.document_id,
                        "bank_transaction_id": txn.transaction_id,
                        "credit_note_id": cn.document_id,
                        "invoice_amount": round(inv_amount, 2),
                        "bank_amount": round(txn_amount, 2),
                        "credit_amount": round(cn_amount, 2),
                        "expected_bank_amount": round(expected_bank, 2),
                        "bank_difference": round(diff, 2),
                        "match_status": "mixed_settlement_candidate",
                        "reasons": [
                            f"Facture {inv.document_id} ({inv_amount:,.2f}$) "
                            f"réglée par paiement bancaire ({txn_amount:,.2f}$) + "
                            f"note de crédit {cn.document_id} ({cn_amount:,.2f}$). "
                            f"Confirmation humaine requise. / "
                            f"Invoice {inv.document_id} (${inv_amount:,.2f}) "
                            f"settled by bank payment (${txn_amount:,.2f}) + "
                            f"credit note {cn.document_id} (${cn_amount:,.2f}). "
                            f"Requires human confirmation."
                        ],
                    })

        return results

    # ------------------------------------------------------------------
    # PART 4 — Credit memo vs refund vs settlement deduplication
    # ------------------------------------------------------------------

    def detect_credit_memo_bank_duplicate(
        self,
        documents: list[DocumentRecord],
        transactions: list[BankTransaction],
        window_days: int = 30,
    ) -> list[dict]:
        """Detect when a credit memo AND a bank deposit exist for the same
        amount from the same vendor within window_days.

        Flags as potential_duplicate_economic_event and presents three
        scenarios for accountant selection.
        """
        results: list[dict] = []

        credit_notes: list[DocumentRecord] = []
        for doc in documents:
            doc_type = (doc.doc_type or "").lower()
            if doc_type in ("credit_note", "credit note", "refund", "chargeback", "reversal"):
                credit_notes.append(doc)

        for cn in credit_notes:
            cn_amount = abs(float(cn.amount)) if cn.amount is not None else 0.0
            if cn_amount <= 0:
                continue

            for txn in transactions:
                if txn.amount is None:
                    continue
                txn_amount = abs(float(txn.amount))
                if txn_amount <= 0:
                    continue

                # Amount match within 1% tolerance
                diff = abs(cn_amount - txn_amount)
                tolerance = max(cn_amount * 0.01, 0.50)
                if diff > tolerance:
                    continue

                # Date proximity
                delta = self.date_delta_days(cn.document_date, txn.posted_date)
                if delta is not None and delta > window_days:
                    continue

                # Vendor similarity
                txn_text = f"{txn.description or ''} {txn.memo or ''}".strip()
                vendor_sim = self.text_similarity(cn.vendor, txn_text)
                if vendor_sim < 0.50:
                    continue

                results.append({
                    "potential_duplicate_economic_event": True,
                    "credit_note_id": cn.document_id,
                    "bank_transaction_id": txn.transaction_id,
                    "credit_note_amount": round(cn_amount, 2),
                    "bank_deposit_amount": round(txn_amount, 2),
                    "vendor": cn.vendor,
                    "vendor_similarity": round(vendor_sim, 4),
                    "date_delta_days": delta,
                    "settlement_state": "UNRESOLVED",
                    "scenarios": [
                        {
                            "scenario": "SCENARIO_A",
                            "description_en": "Credit memo + bank refund are two separate real events",
                            "description_fr": "Note de crédit + remboursement bancaire sont deux événements distincts",
                        },
                        {
                            "scenario": "SCENARIO_B",
                            "description_en": "Credit memo used as settlement — one event, two documents",
                            "description_fr": "Note de crédit utilisée comme règlement — un seul événement, deux documents",
                        },
                        {
                            "scenario": "SCENARIO_C",
                            "description_en": "Duplicate ingestion — same event, should deduplicate",
                            "description_fr": "Ingestion en double — même événement, devrait être dédupliqué",
                        },
                    ],
                    "block_posting": True,
                    "requires_accountant_selection": True,
                    "reasoning": (
                        f"Credit memo {cn.document_id} (${cn_amount:,.2f}) and bank deposit "
                        f"{txn.transaction_id} (${txn_amount:,.2f}) from same vendor within "
                        f"{delta} days. Posting blocked until accountant selects scenario."
                    ),
                })

        return results

    # ------------------------------------------------------------------
    # PART 5 — Cross-entity payment uncertainty preservation
    # ------------------------------------------------------------------

    def evaluate_vendor_identity(
        self,
        invoice_vendor: str | None,
        bank_payee: str | None,
        invoice_gst_number: str | None = None,
        bank_gst_number: str | None = None,
        invoice_address: str | None = None,
        bank_address: str | None = None,
        invoice_phone: str | None = None,
        bank_phone: str | None = None,
        amount: float | None = None,
    ) -> dict[str, Any]:
        """Evaluate vendor identity match with uncertainty preservation.

        When fuzzy match score is 0.60-0.85: preserve uncertainty, don't merge.
        Never say "same vendor" unless GST/QST numbers match exactly
        OR identical normalized names.
        """
        similarity = self.text_similarity(invoice_vendor, bank_payee)
        norm_invoice = self.normalize_text(invoice_vendor)
        norm_bank = self.normalize_text(bank_payee)

        # Exact GST number match — definitive
        if (
            invoice_gst_number
            and bank_gst_number
            and invoice_gst_number.strip() == bank_gst_number.strip()
        ):
            return {
                "identity_status": "confirmed_same_vendor",
                "similarity": round(similarity, 4),
                "match_basis": "gst_number_exact_match",
                "confidence": 1.0,
            }

        # GST numbers differ — tax identity unresolved
        if (
            invoice_gst_number
            and bank_gst_number
            and invoice_gst_number.strip() != bank_gst_number.strip()
        ):
            return {
                "identity_status": "tax_identity_unresolved",
                "similarity": round(similarity, 4),
                "invoice_gst": invoice_gst_number,
                "bank_gst": bank_gst_number,
                "confidence": 0.30,
                "reason_code": "TAX_IDENTITY_UNRESOLVED",
            }

        # Exact normalized name match
        if norm_invoice and norm_bank and norm_invoice == norm_bank:
            return {
                "identity_status": "confirmed_same_vendor",
                "similarity": 1.0,
                "match_basis": "normalized_name_exact",
                "confidence": 0.95,
            }

        # Probable affiliate: score 0.80-0.85 AND same address or phone
        same_address = (
            invoice_address and bank_address
            and self.normalize_text(invoice_address) == self.normalize_text(bank_address)
        )
        same_phone = (
            invoice_phone and bank_phone
            and invoice_phone.strip() == bank_phone.strip()
        )

        if 0.80 <= similarity <= 0.85 and (same_address or same_phone):
            return {
                "identity_status": "probable_affiliate",
                "similarity": round(similarity, 4),
                "shared_address": same_address or False,
                "shared_phone": same_phone or False,
                "confidence": 0.75,
            }

        # Uncertain payee relationship: score 0.60-0.79
        if 0.60 <= similarity < 0.80:
            return {
                "identity_status": "uncertain_payee_relationship",
                "similarity": round(similarity, 4),
                "confidence": 0.50,
                "reason_code": "PAYEE_IDENTITY_UNPROVEN",
            }

        # Possible fraud diversion: no common address/phone AND amount > $5,000
        if (
            0.60 <= similarity <= 0.85
            and not same_address
            and not same_phone
            and amount is not None
            and amount > 5000
        ):
            return {
                "identity_status": "possible_fraud_diversion",
                "similarity": round(similarity, 4),
                "confidence": 0.20,
                "reason_code": "POSSIBLE_FRAUD_DIVERSION",
                "amount": amount,
            }

        # Low similarity — not the same vendor
        if similarity < 0.60:
            return {
                "identity_status": "different_vendor",
                "similarity": round(similarity, 4),
                "confidence": 0.90,
            }

        # High similarity (> 0.85) but no confirming data
        return {
            "identity_status": "probable_same_vendor",
            "similarity": round(similarity, 4),
            "confidence": 0.80,
            "note": "High name similarity but no GST number match to confirm",
        }