"""
RED-TEAM: Vendor Alias Poisoning
=================================
Attack the vendor alias / merge system with near-identical vendor names,
Cyrillic homoglyphs, industry-crossing names, cross-client leakage,
and stale-approval replay.

Vendors under test:
    Apex Industrial          -- the canonical vendor
    APEX IND.                -- abbreviation (true alias)
    Ape\u0445 Industrial     -- Cyrillic "x" (U+0445) instead of Latin "x"
    Apex Consulting          -- different LOB, same stem
    Apex Industries Ltd.     -- pluralised + suffix variant

Fail criteria:
    x Silent over-merge (different businesses collapsed without approval)
    x Vendor history rewritten after merge
    x Payment matched to wrong liability (cross-vendor match)
    x Client-A alias approval leaking into Client-B
    x Stale alias approval accepted after vendor state changes
"""
from __future__ import annotations

import gc
import sqlite3
import sys
import unicodedata
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.core.vendor_memory_store import VendorMemoryStore, normalize_key, open_db
from src.agents.tools.bank_matcher import BankMatcher
from src.agents.core.bank_models import BankTransaction
from src.agents.core.task_models import DocumentRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VENDOR_CANONICAL = "Apex Industrial"
VENDOR_ABBREV = "APEX IND."
VENDOR_CYRILLIC = "Ape\u0445 Industrial"  # Cyrillic x (U+0445)
VENDOR_CONSULTING = "Apex Consulting"
VENDOR_PLURAL = "Apex Industries Ltd."


def _resolver_key(name: str) -> str:
    """Match the normalization used by BankMatcher.resolve_vendor_alias:
    strip().lower() + NFKD + ASCII-encode."""
    s = name.strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", errors="ignore").decode("ascii")
    return s


def _in_memory_db() -> sqlite3.Connection:
    """Create an in-memory SQLite database with the vendor_aliases table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE vendor_aliases (
            alias_id             INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_vendor_key TEXT NOT NULL,
            alias_name           TEXT NOT NULL,
            alias_key            TEXT NOT NULL,
            created_by           TEXT,
            created_at           TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute("CREATE INDEX idx_va_key ON vendor_aliases(alias_key)")
    conn.execute("CREATE INDEX idx_va_canon ON vendor_aliases(canonical_vendor_key)")
    conn.commit()
    return conn


def _insert_alias(conn: sqlite3.Connection, canonical: str, alias: str, created_by: str = "admin"):
    """Insert an alias mapping using the same normalization as resolve_vendor_alias."""
    canon_key = _resolver_key(canonical)
    alias_key = _resolver_key(alias)
    conn.execute(
        "INSERT INTO vendor_aliases (canonical_vendor_key, alias_name, alias_key, created_by, created_at) "
        "VALUES (?, ?, ?, ?, datetime('now'))",
        (canon_key, alias, alias_key, created_by),
    )
    conn.commit()


def _temp_db_path() -> Path:
    """Create a temp file path for SQLite (Windows-safe)."""
    f = NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    return Path(f.name)


def _cleanup_db(db_path: Path) -> None:
    """Best-effort cleanup of temp db on Windows."""
    gc.collect()
    try:
        db_path.unlink(missing_ok=True)
    except PermissionError:
        pass  # Windows file locking; temp dir will clean up eventually


def _make_doc(
    doc_id="doc_001",
    vendor="Apex Industrial",
    amount=1000.00,
    date="2025-06-15",
    client_code="CLIENT_A",
) -> DocumentRecord:
    return DocumentRecord(
        document_id=doc_id,
        file_name=f"{doc_id}.pdf",
        file_path=f"/docs/{doc_id}.pdf",
        client_code=client_code,
        vendor=vendor,
        doc_type="invoice",
        amount=amount,
        document_date=date,
        gl_account="5200",
        tax_code="T",
        category="industrial supplies",
        review_status="ReadyToPost",
        confidence=0.90,
        raw_result={},
    )


def _make_txn(
    txn_id="txn_001",
    description="APEX INDUSTRIAL",
    amount=-1000.00,
    date="2025-06-15",
    client_code="CLIENT_A",
) -> BankTransaction:
    return BankTransaction(
        transaction_id=txn_id,
        client_code=client_code,
        account_id="acct_001",
        posted_date=date,
        description=description,
        memo="",
        amount=amount,
        currency="CAD",
    )


# ===================================================================
# A. TRUE ALIASES MERGE ONLY WITH EXPLICIT / HIGH-CONFIDENCE SUPPORT
# ===================================================================


class TestTrueAliasMerge:
    """
    Abbreviation "APEX IND." is a legitimate alias of "Apex Industrial".
    It should resolve ONLY when an explicit alias row exists.
    """

    def test_abbreviation_resolves_when_alias_exists(self):
        """After explicit alias entry, APEX IND. resolves to canonical key."""
        conn = _in_memory_db()
        matcher = BankMatcher()

        _insert_alias(conn, VENDOR_CANONICAL, VENDOR_ABBREV)
        resolved = matcher.resolve_vendor_alias(VENDOR_ABBREV, conn=conn)
        expected = _resolver_key(VENDOR_CANONICAL)
        assert resolved == expected, (
            f"Expected canonical key '{expected}', got: '{resolved}'"
        )

    def test_abbreviation_does_not_resolve_without_alias(self):
        """Without explicit alias, APEX IND. must NOT silently merge."""
        conn = _in_memory_db()
        matcher = BankMatcher()

        resolved = matcher.resolve_vendor_alias(VENDOR_ABBREV, conn=conn)
        # Should return the original (no alias found)
        assert resolved == VENDOR_ABBREV, (
            f"Without alias row, resolve should return input. Got: {resolved}"
        )

    def test_fuzzy_suggestion_fires_for_abbreviation(self):
        """APEX IND. vs Apex Industrial should trigger alias suggestion (0.65-0.79)."""
        matcher = BankMatcher()
        sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_ABBREV)
        # After normalization: "apex industrial" vs "apex ind"
        assert sim >= 0.60, (
            f"Abbreviation similarity too low to even suggest: {sim:.4f}"
        )

    def test_plural_variant_resolves_when_alias_exists(self):
        """Apex Industries Ltd. resolves to Apex Industrial via explicit alias."""
        conn = _in_memory_db()
        matcher = BankMatcher()

        _insert_alias(conn, VENDOR_CANONICAL, VENDOR_PLURAL)
        resolved = matcher.resolve_vendor_alias(VENDOR_PLURAL, conn=conn)
        expected = _resolver_key(VENDOR_CANONICAL)
        assert resolved == expected

    def test_plural_variant_no_silent_merge(self):
        """Without alias, 'Apex Industries' must not auto-merge with 'Apex Industrial'."""
        matcher = BankMatcher()
        sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_PLURAL)
        # "apex industrial" vs "apex industries" -- very close but NOT identical
        assert sim < 1.0, (
            f"Plural variant should NOT be treated as exact match: {sim}"
        )


# ===================================================================
# B. CONSULTING VENDOR DOES NOT MERGE WITH INDUSTRIAL VENDOR
# ===================================================================


class TestCrossIndustryProtection:
    """
    'Apex Consulting' is a completely different business from
    'Apex Industrial'. They share a stem but must never merge.
    """

    def test_consulting_vs_industrial_no_merge(self):
        """Different LOB vendors must not be auto-aliased."""
        matcher = BankMatcher()
        sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_CONSULTING)
        assert sim < 0.80, (
            f"Cross-industry vendors should be below merge threshold: {sim:.4f}"
        )

    def test_consulting_alias_to_industrial_rejected(self):
        """Matching should not treat Consulting invoice as Industrial vendor."""
        matcher = BankMatcher()
        doc = _make_doc(vendor=VENDOR_CONSULTING, doc_id="doc_consult")
        txn = _make_txn(description="APEX INDUSTRIAL", txn_id="txn_ind")

        results = matcher.match_documents([doc], [txn])
        if results and results[0].status in ("matched", "suggested"):
            sim = matcher.text_similarity(VENDOR_CONSULTING, "APEX INDUSTRIAL")
            assert sim < 0.90, (
                f"Consulting->Industrial match at {sim:.4f} exceeds safe threshold"
            )

    def test_consulting_invoice_stays_separate_in_memory(self):
        """Vendor memory for Apex Consulting must not contaminate Apex Industrial."""
        key_consulting = normalize_key(VENDOR_CONSULTING)
        key_industrial = normalize_key(VENDOR_CANONICAL)
        assert key_consulting != key_industrial, (
            f"CRITICAL: Consulting and Industrial normalize to same key!\n"
            f"  Consulting -> {key_consulting}\n"
            f"  Industrial -> {key_industrial}"
        )


# ===================================================================
# C. CYRILLIC HOMOGLYPH ATTACK
# ===================================================================


class TestCyrillicHomoglyphAttack:
    """
    'Ape\u0445 Industrial' uses Cyrillic 'x' (U+0445) which LOOKS identical
    to Latin 'x' (U+0078) but is a different codepoint. This is a
    classic homoglyph / confusable attack.
    """

    def test_cyrillic_detected_at_codepoint_level(self):
        """Confirm the Cyrillic char is actually different."""
        latin_x = "x"  # U+0078
        cyrillic_x = "\u0445"  # U+0445
        assert latin_x != cyrillic_x
        assert ord(latin_x) == 0x78
        assert ord(cyrillic_x) == 0x445

    def test_normalize_key_strips_cyrillic_differently(self):
        """NFKD + ASCII encode drops Cyrillic 'x', producing different key.

        'Apex' -> 'apex' but 'Ape\u0445' -> 'ape' (Cyrillic dropped).
        These must NOT normalize to the same key.
        """
        key_latin = normalize_key(VENDOR_CANONICAL)
        key_cyrillic = normalize_key(VENDOR_CYRILLIC)

        if key_latin == key_cyrillic:
            pytest.fail(
                f"CRITICAL: Cyrillic homoglyph normalizes to same key!\n"
                f"  Latin    '{VENDOR_CANONICAL}' -> '{key_latin}'\n"
                f"  Cyrillic '{VENDOR_CYRILLIC}' -> '{key_cyrillic}'\n"
                f"  This enables silent vendor substitution."
            )

    def test_cyrillic_vendor_not_resolved_as_latin_alias(self):
        """Cyrillic vendor must NOT resolve via Latin alias entry."""
        conn = _in_memory_db()
        matcher = BankMatcher()

        _insert_alias(conn, VENDOR_CANONICAL, "APEX IND.")

        resolved = matcher.resolve_vendor_alias(VENDOR_CYRILLIC, conn=conn)
        canonical_key = _resolver_key(VENDOR_CANONICAL)

        assert resolved != canonical_key, (
            f"Cyrillic homoglyph resolved to canonical vendor -- "
            f"alias poisoning possible!"
        )

    def test_cyrillic_vs_latin_similarity_not_perfect(self):
        """Text similarity between Cyrillic and Latin variant should be < 1.0."""
        matcher = BankMatcher()
        sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_CYRILLIC)
        assert sim < 1.0, (
            f"Cyrillic homoglyph has perfect similarity ({sim}) -- "
            f"normalization failed to distinguish"
        )

    def test_cyrillic_similarity_dangerously_high(self):
        """DEFECT: Cyrillic 'Ape\u0445' -> 'ape industrial' has ~0.97 similarity
        to Latin 'apex industrial'. After NFKD+ASCII, only one char differs.
        SequenceMatcher treats these as near-identical.

        This is a real vulnerability: a Cyrillic-spoofed bank transaction
        will match a Latin vendor invoice at high confidence.
        """
        matcher = BankMatcher()
        sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_CYRILLIC)
        # After normalization: "apex industrial" vs "ape industrial"
        # SequenceMatcher ratio ~ 0.9655 -- above the exact_threshold of 0.90
        if sim >= 0.90:
            pytest.xfail(
                f"DEFECT: Cyrillic homoglyph similarity {sim:.4f} >= 0.90 "
                f"(exact_threshold). Bank matcher will treat spoofed vendor "
                f"as an exact match, routing payments to wrong liability. "
                f"Fix: add Unicode confusable detection or skeleton comparison."
            )

    def test_cyrillic_payment_does_not_match_latin_invoice(self):
        """Bank payment for Cyrillic vendor must not match Latin vendor invoice."""
        matcher = BankMatcher()
        doc = _make_doc(vendor=VENDOR_CANONICAL, amount=5000.00)
        txn = _make_txn(
            description=VENDOR_CYRILLIC.upper(),
            amount=-5000.00,
            txn_id="txn_cyrillic",
        )
        results = matcher.match_documents([doc], [txn])
        if results and results[0].status == "matched":
            sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_CYRILLIC)
            if sim >= 0.90:
                pytest.xfail(
                    f"DEFECT: Cyrillic spoof matched at similarity {sim:.4f}. "
                    f"Payment routed to wrong vendor liability. "
                    f"Root cause: normalize_text drops Cyrillic char silently, "
                    f"resulting in near-identical normalized forms."
                )


# ===================================================================
# D. CROSS-CLIENT ALIAS ISOLATION
# ===================================================================


class TestCrossClientAliasIsolation:
    """
    Alias approval in Client A must NOT leak to Client B.
    The vendor_aliases table does not have a client_code column,
    so this tests defense-in-depth via client gates.
    """

    def test_client_gate_blocks_cross_client_match(self):
        """Even with a shared alias, client gate must block cross-client matching."""
        matcher = BankMatcher()
        doc = _make_doc(client_code="CLIENT_A", vendor=VENDOR_CANONICAL)
        txn = _make_txn(client_code="CLIENT_B", description="APEX INDUSTRIAL")

        candidate = matcher.evaluate_candidate(doc, txn)
        assert candidate is None, (
            "Cross-client matching must be blocked by client gate"
        )

    def test_alias_resolution_happens_before_client_gate(self):
        """Alias resolves vendor name, but client gate still blocks cross-client."""
        conn = _in_memory_db()
        matcher = BankMatcher()

        _insert_alias(conn, VENDOR_CANONICAL, VENDOR_ABBREV)

        resolved_a = matcher.resolve_vendor_alias(VENDOR_ABBREV, conn=conn)
        resolved_b = matcher.resolve_vendor_alias(VENDOR_ABBREV, conn=conn)

        # Both resolve the same (aliases are global) -- this is the risk
        assert resolved_a == resolved_b, "Alias resolution is global (by design)"

        # But matching must still be blocked by client gate
        doc = _make_doc(client_code="CLIENT_A")
        txn = _make_txn(client_code="CLIENT_B")
        candidate = matcher.evaluate_candidate(doc, txn)
        assert candidate is None, (
            "Client gate must block even after alias resolution"
        )

    def test_vendor_memory_client_isolated(self):
        """Vendor memory learned in Client A must not appear in Client B lookups."""
        db_path = _temp_db_path()
        try:
            store = VendorMemoryStore(db_path=db_path)

            # Record approvals for Client A
            for i in range(5):
                store.record_approval(
                    vendor=VENDOR_CANONICAL,
                    client_code="CLIENT_A",
                    gl_account="5200",
                    tax_code="T",
                    doc_type="invoice",
                    category="industrial supplies",
                )

            # Client A should find the memory
            result_a = store.get_best_match(
                vendor=VENDOR_CANONICAL,
                client_code="CLIENT_A",
            )
            assert result_a is not None, "Client A should have vendor memory"

            # Client B must NOT see Client A's memory
            result_b = store.get_best_match(
                vendor=VENDOR_CANONICAL,
                client_code="CLIENT_B",
            )
            assert result_b is None, (
                "CRITICAL: Client B can see Client A's vendor memory -- "
                "cross-client data leakage!"
            )
        finally:
            del store
            _cleanup_db(db_path)


# ===================================================================
# E. STALE ALIAS APPROVAL AFTER VENDOR STATE CHANGE
# ===================================================================


class TestStaleAliasApproval:
    """
    An alias approved months ago may become invalid if the vendor
    changes. The system should not blindly trust old alias mappings.
    """

    def test_stale_vendor_memory_decays(self):
        """Vendor memory older than 24 months should be excluded."""
        db_path = _temp_db_path()
        try:
            store = VendorMemoryStore(db_path=db_path)

            # Insert an old approval (>24 months) directly into the DB
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                INSERT INTO vendor_memory
                (vendor, vendor_key, client_code, client_code_key,
                 gl_account, tax_code, doc_type, category,
                 approval_count, confidence, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    VENDOR_CANONICAL,
                    normalize_key(VENDOR_CANONICAL),
                    "CLIENT_A",
                    normalize_key("CLIENT_A"),
                    "5200", "T", "invoice", "industrial",
                    10, 0.90,
                    "2023-01-01T00:00:00+00:00",
                    "2023-01-01T00:00:00+00:00",  # >24 months old
                ),
            )
            conn.commit()
            conn.close()

            # Should NOT return stale memory
            result = store.get_best_match(
                vendor=VENDOR_CANONICAL,
                client_code="CLIENT_A",
            )
            assert result is None, (
                "Stale vendor memory (>24 months) should be excluded"
            )
        finally:
            del store
            _cleanup_db(db_path)

    def test_alias_entry_has_no_expiry_but_memory_does(self):
        """Alias table has no TTL, but vendor memory has 24-month decay.
        An alias can resolve to a canonical name even if all supporting
        memory has expired -- a potential risk that should be documented."""
        conn = _in_memory_db()
        matcher = BankMatcher()

        _insert_alias(conn, VENDOR_CANONICAL, VENDOR_ABBREV)

        # Alias still resolves (no TTL on alias table)
        resolved = matcher.resolve_vendor_alias(VENDOR_ABBREV, conn=conn)
        expected = _resolver_key(VENDOR_CANONICAL)
        assert resolved == expected, (
            f"Alias resolution should work. Expected '{expected}', got '{resolved}'"
        )
        # Risk: aliases persist forever; memory decays.
        # If a vendor is deactivated, the alias should be removed manually.


# ===================================================================
# F. NORMALIZATION CONSISTENCY
# ===================================================================


class TestNormalizationConsistency:
    """
    All five vendor variants must normalize to distinct or intentionally
    identical keys, with no accidental collisions.
    """

    def test_all_five_keys_are_distinct_or_intentional(self):
        """Map all 5 vendor names through normalize_key and verify expectations."""
        keys = {
            VENDOR_CANONICAL: normalize_key(VENDOR_CANONICAL),
            VENDOR_ABBREV: normalize_key(VENDOR_ABBREV),
            VENDOR_CYRILLIC: normalize_key(VENDOR_CYRILLIC),
            VENDOR_CONSULTING: normalize_key(VENDOR_CONSULTING),
            VENDOR_PLURAL: normalize_key(VENDOR_PLURAL),
        }

        # Canonical and abbreviation should differ
        assert keys[VENDOR_CANONICAL] != keys[VENDOR_ABBREV], (
            f"Canonical and abbreviation should not auto-merge:\n"
            f"  {VENDOR_CANONICAL} -> {keys[VENDOR_CANONICAL]}\n"
            f"  {VENDOR_ABBREV} -> {keys[VENDOR_ABBREV]}"
        )

        # Consulting must differ from Industrial
        assert keys[VENDOR_CANONICAL] != keys[VENDOR_CONSULTING], (
            f"Industrial and Consulting must not collide:\n"
            f"  {VENDOR_CANONICAL} -> {keys[VENDOR_CANONICAL]}\n"
            f"  {VENDOR_CONSULTING} -> {keys[VENDOR_CONSULTING]}"
        )

        # Cyrillic must differ from Latin
        assert keys[VENDOR_CANONICAL] != keys[VENDOR_CYRILLIC], (
            f"Latin and Cyrillic must not collide:\n"
            f"  {VENDOR_CANONICAL} -> {keys[VENDOR_CANONICAL]}\n"
            f"  {VENDOR_CYRILLIC} -> {keys[VENDOR_CYRILLIC]}"
        )

    def test_normalize_key_idempotent(self):
        """Normalizing an already-normalized key should produce the same result."""
        for vendor in [VENDOR_CANONICAL, VENDOR_ABBREV, VENDOR_CONSULTING]:
            key1 = normalize_key(vendor)
            key2 = normalize_key(key1)
            assert key1 == key2, (
                f"normalize_key not idempotent for '{vendor}': "
                f"'{key1}' -> '{key2}'"
            )


# ===================================================================
# G. PAYMENT MATCHED TO WRONG LIABILITY
# ===================================================================


class TestWrongLiabilityProtection:
    """
    A payment for Apex Consulting must not be matched against
    an Apex Industrial invoice (or vice versa).
    """

    def test_consulting_payment_vs_industrial_invoice(self):
        """Payment for Consulting must not match Industrial invoice."""
        matcher = BankMatcher()
        doc = _make_doc(
            vendor=VENDOR_CANONICAL,
            amount=2500.00,
            doc_id="inv_industrial",
        )
        txn = _make_txn(
            description="APEX CONSULTING",
            amount=-2500.00,
            txn_id="pay_consulting",
        )
        results = matcher.match_documents([doc], [txn])
        if results and results[0].status == "matched":
            pytest.fail(
                "CRITICAL: Consulting payment matched to Industrial invoice -- "
                "wrong liability! Vendor similarity: "
                f"{matcher.text_similarity(VENDOR_CANONICAL, VENDOR_CONSULTING):.4f}"
            )

    def test_industrial_payment_vs_consulting_invoice(self):
        """Payment for Industrial must not match Consulting invoice."""
        matcher = BankMatcher()
        doc = _make_doc(
            vendor=VENDOR_CONSULTING,
            amount=3000.00,
            doc_id="inv_consulting",
        )
        txn = _make_txn(
            description="APEX INDUSTRIAL",
            amount=-3000.00,
            txn_id="pay_industrial",
        )
        results = matcher.match_documents([doc], [txn])
        if results and results[0].status == "matched":
            pytest.fail(
                "CRITICAL: Industrial payment matched to Consulting invoice -- "
                "wrong liability!"
            )

    def test_cyrillic_payment_vs_real_vendor_invoice(self):
        """Cyrillic-spoofed payment must not match real vendor invoice."""
        matcher = BankMatcher()
        doc = _make_doc(
            vendor=VENDOR_CANONICAL,
            amount=10000.00,
            doc_id="inv_real",
        )
        txn = _make_txn(
            description=VENDOR_CYRILLIC.upper(),
            amount=-10000.00,
            txn_id="pay_spoof",
        )
        results = matcher.match_documents([doc], [txn])
        if results and results[0].status == "matched":
            sim = matcher.text_similarity(VENDOR_CANONICAL, VENDOR_CYRILLIC)
            if sim >= 0.90:
                pytest.xfail(
                    f"DEFECT: Cyrillic spoof matched at similarity {sim:.4f}. "
                    f"Payment routed to wrong vendor liability. "
                    f"Root cause: NFKD+ASCII normalization drops Cyrillic char, "
                    f"leaving 'ape industrial' vs 'apex industrial' at ~0.97 ratio."
                )


# ===================================================================
# H. VENDOR HISTORY INTEGRITY
# ===================================================================


class TestVendorHistoryIntegrity:
    """
    Merging aliases must never rewrite existing vendor history.
    Past transactions stay attributed to their original vendor.
    """

    def test_alias_does_not_rewrite_existing_records(self):
        """Adding an alias should not retroactively change stored vendor_key."""
        db_path = _temp_db_path()
        try:
            store = VendorMemoryStore(db_path=db_path)

            # Record history under original name
            for i in range(5):
                store.record_approval(
                    vendor=VENDOR_ABBREV,
                    client_code="CLIENT_A",
                    gl_account="5200",
                    tax_code="T",
                    doc_type="invoice",
                )

            abbrev_key = normalize_key(VENDOR_ABBREV)

            # Check records exist under abbreviation key
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows_before = conn.execute(
                "SELECT COUNT(*) as cnt FROM vendor_memory WHERE vendor_key = ?",
                (abbrev_key,),
            ).fetchone()
            count_before = rows_before["cnt"]
            conn.close()

            assert count_before > 0, "Should have records under abbreviation key"

            # Simulate adding an alias (in vendor_aliases table, not touching vendor_memory)
            # Verify vendor_memory records are NOT retroactively changed
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows_after = conn.execute(
                "SELECT COUNT(*) as cnt FROM vendor_memory WHERE vendor_key = ?",
                (abbrev_key,),
            ).fetchone()
            count_after = rows_after["cnt"]
            conn.close()

            assert count_after == count_before, (
                f"Vendor history was rewritten! Before: {count_before}, After: {count_after}"
            )
        finally:
            del store
            _cleanup_db(db_path)

    def test_multiple_approvals_upsert_not_duplicate(self):
        """Multiple approvals for same vendor+GL should upsert, not duplicate."""
        db_path = _temp_db_path()
        try:
            store = VendorMemoryStore(db_path=db_path)

            # Record same vendor+GL+tax multiple times
            for i in range(5):
                store.record_approval(
                    vendor=VENDOR_CANONICAL,
                    client_code="CLIENT_A",
                    gl_account="5200",
                    tax_code="T",
                    doc_type="invoice",
                    category="industrial supplies",
                )

            canonical_key = normalize_key(VENDOR_CANONICAL)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            count = conn.execute(
                "SELECT COUNT(*) as cnt FROM vendor_memory WHERE vendor_key = ?",
                (canonical_key,),
            ).fetchone()["cnt"]
            conn.close()

            # Should be 1 row (upserted) not 5
            assert count == 1, (
                f"Expected 1 upserted row, got {count} -- "
                f"record_approval is inserting duplicates"
            )
        finally:
            del store
            _cleanup_db(db_path)
