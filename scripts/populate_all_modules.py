#!/usr/bin/env python3
"""
scripts/populate_all_modules.py

Populates EVERY module with realistic demo data for client BOLDUC.

Usage:
    python scripts/populate_all_modules.py
"""
from __future__ import annotations

import json
import secrets
import sqlite3
import sys
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")


def _round(v: Decimal) -> float:
    return float(v.quantize(CENT, rounding=ROUND_HALF_UP))


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dict_factory(cursor, row):
    """Row factory that returns dicts (supports .get())."""
    d = {}
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = _dict_factory
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — ENGAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def populate_engagement(conn: sqlite3.Connection) -> str:
    """Create engagement if not exists. Returns engagement_id."""
    from src.engines.audit_engine import ensure_audit_tables, get_engagements, create_engagement, update_engagement
    ensure_audit_tables(conn)

    # Check if BOLDUC engagement already exists for 2025
    existing = get_engagements(conn, client_code="BOLDUC")
    for eng in existing:
        if eng["period"] == "2025" and eng["engagement_type"] == "audit":
            eid = eng["engagement_id"]
            # Update to in_progress with partner
            update_engagement(conn, eid, status="fieldwork", partner="Sam Tremblay CPA")
            print(f"  [ENGAGEMENT] Reusing existing: {eid}")
            return eid

    eng = create_engagement(
        conn,
        client_code="BOLDUC",
        period="2025",
        engagement_type="audit",
        partner="Sam Tremblay CPA",
    )
    eid = eng["engagement_id"]
    update_engagement(conn, eid, status="fieldwork")
    print(f"  [ENGAGEMENT] Created: {eid}")
    return eid


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — MATERIALITY
# ═══════════════════════════════════════════════════════════════════════════

def populate_materiality(conn: sqlite3.Connection, engagement_id: str) -> None:
    from src.engines.cas_engine import ensure_cas_tables, save_materiality

    ensure_cas_tables(conn)

    # Check if materiality already exists
    existing = conn.execute(
        "SELECT assessment_id FROM materiality_assessments WHERE engagement_id = ?",
        (engagement_id,),
    ).fetchone()
    if existing:
        print(f"  [MATERIALITY] Already exists: {existing['assessment_id']}")
        return

    materiality_dict = {
        "basis": "total_assets",
        "basis_amount": Decimal("2450000.00"),
        "planning_materiality": Decimal("12250.00"),
        "performance_materiality": Decimal("9188.00"),
        "clearly_trivial": Decimal("613.00"),
    }
    aid = save_materiality(
        conn, engagement_id, materiality_dict, "sam",
        notes="Total assets basis per firm policy for construction industry",
    )
    print(f"  [MATERIALITY] Created: {aid}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — RISK ASSESSMENT (CAS 315)
# ═══════════════════════════════════════════════════════════════════════════

def populate_risk_assessment(conn: sqlite3.Connection, engagement_id: str) -> int:
    from src.engines.cas_engine import ensure_cas_tables

    ensure_cas_tables(conn)

    # Check if risk rows already exist
    existing = conn.execute(
        "SELECT COUNT(*) as cnt FROM risk_assessments WHERE engagement_id = ?",
        (engagement_id,),
    ).fetchone()
    if existing and existing["cnt"] > 0:
        print(f"  [RISK] Already has {existing['cnt']} rows")
        return existing["cnt"]

    risks = [
        ("1010", "Encaisse",             "existence",     "low",    "low",    "low",    False),
        ("1100", "Comptes clients",      "valuation",     "medium", "medium", "medium", False),
        ("1100", "Comptes clients",      "cutoff",        "high",   "medium", "high",   True),
        ("4000", "Revenus",              "completeness",  "high",   "medium", "high",   True),
        ("4000", "Revenus",              "cutoff",        "high",   "high",   "high",   True),
        ("2000", "Comptes fournisseurs", "completeness",  "medium", "medium", "medium", False),
        ("1500", "Immobilisations",      "existence",     "low",    "low",    "low",    False),
        ("2500", "Dette à long terme",   "existence",     "low",    "low",    "low",    False),
    ]

    now = _utc_now()
    count = 0
    for acct_code, acct_name, assertion, inherent, control, combined, significant in risks:
        risk_id = f"risk_{secrets.token_hex(8)}"
        conn.execute(
            """INSERT INTO risk_assessments
               (risk_id, engagement_id, account_code, account_name,
                assertion, inherent_risk, control_risk, combined_risk,
                significant_risk, assessed_by, assessed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (risk_id, engagement_id, acct_code, acct_name,
             assertion, inherent, control, combined,
             1 if significant else 0, "sam", now),
        )
        count += 1
    conn.commit()
    print(f"  [RISK] Inserted {count} risk assessment rows")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — WORKING PAPERS
# ═══════════════════════════════════════════════════════════════════════════

def populate_working_papers(conn: sqlite3.Connection) -> int:
    from src.engines.audit_engine import (
        ensure_audit_tables, get_or_create_working_paper,
        update_working_paper, add_working_paper_item,
    )
    ensure_audit_tables(conn)

    papers = [
        {
            "account_code": "1010",
            "account_name": "Cash and Bank",
            "balance_per_books": 127450.32,
            "balance_confirmed": 127450.32,
            "status": "complete",
            "notes": "Confirmed via bank reconciliation December 2025. No exceptions noted.",
            "tested_by": "sam",
            "reviewed_by": "sam",
            "tick_marks": ["bank_confirm", "recon_agree", "agree_tb"],
        },
        {
            "account_code": "1100",
            "account_name": "Accounts Receivable",
            "balance_per_books": 342800.00,
            "balance_confirmed": 342800.00,
            "status": "open",
            "notes": "Confirmations sent to 15 debtors. 12 replies received. 3 outstanding.",
            "tested_by": "sam",
            "reviewed_by": None,
            "tick_marks": [],
        },
        {
            "account_code": "1500",
            "account_name": "Fixed Assets",
            "balance_per_books": 687250.00,
            "balance_confirmed": 687250.00,
            "status": "complete",
            "notes": "CCA schedule agrees to Schedule 8. Physical inspection performed December 2025.",
            "tested_by": "sam",
            "reviewed_by": "sam",
            "tick_marks": [],
        },
        {
            "account_code": "2000",
            "account_name": "Accounts Payable",
            "balance_per_books": 89340.00,
            "balance_confirmed": 89340.00,
            "status": "complete",
            "notes": "Cutoff procedures performed. Search for unrecorded liabilities complete. No exceptions.",
            "tested_by": "sam",
            "reviewed_by": "sam",
            "tick_marks": [],
        },
        {
            "account_code": "4000",
            "account_name": "Revenue",
            "balance_per_books": 2847600.00,
            "balance_confirmed": 2847600.00,
            "status": "open",
            "notes": "Cutoff testing in progress. Revenue recognition policy reviewed and appropriate.",
            "tested_by": "sam",
            "reviewed_by": None,
            "tick_marks": [],
        },
        {
            "account_code": "2500",
            "account_name": "Long-term Debt",
            "balance_per_books": 425000.00,
            "balance_confirmed": 425000.00,
            "status": "complete",
            "notes": "Confirmed via bank confirmation. Repayment schedule agrees to amortization table.",
            "tested_by": "sam",
            "reviewed_by": "sam",
            "tick_marks": [],
        },
    ]

    count = 0
    for wp_data in papers:
        wp = get_or_create_working_paper(
            conn,
            client_code="BOLDUC",
            period="2025",
            engagement_type="audit",
            account_code=wp_data["account_code"],
            account_name=wp_data["account_name"],
            balance_per_books=wp_data["balance_per_books"],
        )
        paper_id = wp["paper_id"]

        # Only update if not already signed off
        if not wp.get("sign_off_at"):
            # Add tick marks BEFORE sign-off (immutability trigger)
            for tm in wp_data["tick_marks"]:
                tick = tm if tm in ("tested", "confirmed", "exception", "not_applicable") else "confirmed"
                add_working_paper_item(
                    conn, paper_id,
                    document_id=f"DOC-{secrets.token_hex(6).upper()}",
                    tick_mark=tick,
                    notes=tm,
                    tested_by="sam",
                )

            # Now update (reviewed_by sets sign_off_at, making it immutable)
            kwargs = {
                "balance_confirmed": wp_data["balance_confirmed"],
                "tested_by": wp_data["tested_by"],
                "status": wp_data["status"],
                "notes": wp_data["notes"],
            }
            if wp_data["reviewed_by"]:
                kwargs["reviewed_by"] = wp_data["reviewed_by"]
            update_working_paper(conn, paper_id, **kwargs)
        count += 1

    print(f"  [WORKING PAPERS] Created/updated {count} lead sheets")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — AUDIT EVIDENCE (CAS 500)
# ═══════════════════════════════════════════════════════════════════════════

def populate_audit_evidence(conn: sqlite3.Connection) -> int:
    from src.engines.audit_engine import ensure_audit_tables

    ensure_audit_tables(conn)

    evidence_items = [
        {
            "document_id": f"DOC-BANK-{secrets.token_hex(4).upper()}",
            "evidence_type": "external_confirmation",
            "match_status": "confirmed",
            "notes": json.dumps({
                "source": "Desjardins Banque",
                "description": "Confirmation du solde bancaire au 31 décembre 2025",
                "amount": 127450.32,
                "assertion_tested": "existence",
                "result": "confirmed",
                "exception": False,
            }),
        },
        {
            "document_id": f"DOC-AR-{secrets.token_hex(4).upper()}",
            "evidence_type": "external_confirmation",
            "match_status": "partial",
            "notes": json.dumps({
                "source": "15 clients confirmed",
                "description": "Circularisation des comptes clients — 12/15 réponses reçues",
                "assertion_tested": "existence",
                "result": "partial",
                "exception": False,
            }),
        },
        {
            "document_id": f"DOC-CCA-{secrets.token_hex(4).upper()}",
            "evidence_type": "schedule",
            "match_status": "confirmed",
            "notes": json.dumps({
                "description": "Calendrier DPA préparé et vérifié — concorde avec Annexe 8 T2",
                "assertion_tested": "valuation",
                "result": "confirmed",
                "exception": False,
            }),
        },
        {
            "document_id": f"DOC-AP-{secrets.token_hex(4).upper()}",
            "evidence_type": "vouching",
            "match_status": "confirmed",
            "notes": json.dumps({
                "description": "Test de coupure — 25 factures vérifiées avant et après le 31 décembre",
                "assertion_tested": "cutoff",
                "result": "confirmed",
                "exception": False,
            }),
        },
    ]

    count = 0
    for item in evidence_items:
        eid = f"ev_{secrets.token_hex(8)}"
        conn.execute(
            """INSERT INTO audit_evidence
               (evidence_id, document_id, evidence_type, match_status, notes, created_at)
               VALUES (?,?,?,?,?,?)""",
            (eid, item["document_id"], item["evidence_type"],
             item["match_status"], item["notes"], _utc_now()),
        )
        count += 1
    conn.commit()
    print(f"  [EVIDENCE] Inserted {count} audit evidence items")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 — AUDIT SAMPLE (CAS 530)
# ═══════════════════════════════════════════════════════════════════════════

def populate_audit_sample(conn: sqlite3.Connection, engagement_id: str) -> int:
    """Insert statistical sample using working_paper_items as documentation."""
    from src.engines.audit_engine import ensure_audit_tables, get_or_create_working_paper, add_working_paper_item

    ensure_audit_tables(conn)

    # Create a working paper for the sampling documentation
    wp = get_or_create_working_paper(
        conn,
        client_code="BOLDUC",
        period="2025",
        engagement_type="audit",
        account_code="2000-SAMPLE",
        account_name="AP Statistical Sample (CAS 530)",
        balance_per_books=89340.00,
    )
    paper_id = wp["paper_id"]

    sample_notes = json.dumps({
        "population": "accounts_payable",
        "population_size": 342,
        "confidence_level": 95,
        "tolerable_misstatement": 9188.00,
        "sample_size": 25,
        "sampling_method": "monetary_unit",
        "items_tested": 25,
        "exceptions_found": 0,
        "projected_misstatement": 0.00,
        "conclusion": "No exceptions noted. Population accepted as fairly stated.",
    })

    # Check if already has items
    existing = conn.execute(
        "SELECT COUNT(*) as cnt FROM working_paper_items WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()
    if existing and existing["cnt"] > 0:
        print(f"  [SAMPLE] Already has {existing['cnt']} items")
        return 1

    add_working_paper_item(
        conn, paper_id,
        document_id=f"DOC-SAMPLE-{secrets.token_hex(4).upper()}",
        tick_mark="confirmed",
        notes=sample_notes,
        tested_by="sam",
    )

    print("  [SAMPLE] Inserted 1 statistical sample (AP, MUS, n=25)")
    return 1


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7 — CONTROLS (CAS 330)
# ═══════════════════════════════════════════════════════════════════════════

def populate_controls(conn: sqlite3.Connection, engagement_id: str) -> int:
    from src.engines.cas_engine import create_control_test, record_test_results

    controls = [
        {
            "name": "Bank reconciliation review",
            "description": "Rapprochement bancaire préparé mensuellement et révisé par le contrôleur",
            "objective": "Monthly reconciliation prepared and reviewed",
            "test_type": "observation",
            "items_tested": 12,
            "exceptions": 0,
            "conclusion": "effective",
        },
        {
            "name": "Invoice approval",
            "description": "Toutes les factures > $5,000 approuvées par le directeur financier",
            "objective": "Invoices above $5,000 approved by CFO",
            "test_type": "reperformance",
            "items_tested": 25,
            "exceptions": 0,
            "conclusion": "effective",
        },
        {
            "name": "Payroll authorization",
            "description": "Modifications à la paie autorisées par les RH et le président",
            "objective": "Payroll changes authorized by HR and president",
            "test_type": "inquiry",
            "items_tested": 12,
            "exceptions": 0,
            "conclusion": "effective",
        },
        {
            "name": "Journal entry review",
            "description": "Écritures de journal révisées mensuellement par le contrôleur",
            "objective": "Journal entries reviewed monthly by controller",
            "test_type": "reperformance",
            "items_tested": 12,
            "exceptions": 0,
            "conclusion": "effective",
        },
        {
            "name": "Fixed asset additions",
            "description": "Acquisitions d'immobilisations > $10,000 approuvées par le CA",
            "objective": "Capital purchases > $10,000 approved by board",
            "test_type": "reperformance",
            "items_tested": 5,
            "exceptions": 0,
            "conclusion": "effective",
        },
    ]

    # Check existing
    existing = conn.execute(
        "SELECT COUNT(*) as cnt FROM control_tests WHERE engagement_id = ?",
        (engagement_id,),
    ).fetchone()
    if existing and existing["cnt"] >= 5:
        print(f"  [CONTROLS] Already has {existing['cnt']} tests")
        return existing["cnt"]

    count = 0
    for ctrl in controls:
        test_id = create_control_test(
            engagement_id,
            control_name=ctrl["name"],
            control_objective=ctrl["objective"],
            test_type=ctrl["test_type"],
            conn=conn,
            control_description=ctrl["description"],
            tested_by="sam",
        )
        record_test_results(
            test_id,
            items_tested=ctrl["items_tested"],
            exceptions_found=ctrl["exceptions"],
            exception_details="",
            conclusion=ctrl["conclusion"],
            conn=conn,
        )
        count += 1

    print(f"  [CONTROLS] Inserted {count} control tests")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8 — RELATED PARTIES (CAS 550)
# ═══════════════════════════════════════════════════════════════════════════

def populate_related_parties(conn: sqlite3.Connection, engagement_id: str) -> int:
    from src.engines.cas_engine import add_related_party, flag_related_party_transaction

    # Check existing
    existing = conn.execute(
        "SELECT COUNT(*) as cnt FROM related_parties WHERE LOWER(client_code) = 'bolduc'",
    ).fetchone()
    if existing and existing["cnt"] >= 2:
        print(f"  [RELATED PARTIES] Already has {existing['cnt']} parties")
        return existing["cnt"]

    # Related party 1: Tremblay Holdings Inc.
    rp1_id = add_related_party(
        client_code="BOLDUC",
        party_name="Tremblay Holdings Inc.",
        relationship_type="affiliated_company",
        conn=conn,
        ownership_percentage=100.0,
        notes="Société de portefeuille — Jean-Pierre Tremblay, actionnaire unique. "
              "Frais de gestion approuvés par le CA. Montant raisonnable pour les services rendus.",
        identified_by="sam",
    )
    flag_related_party_transaction(
        engagement_id=engagement_id,
        document_id=f"DOC-RPT1-{secrets.token_hex(4).upper()}",
        party_id=rp1_id,
        measurement_basis="exchange_amount",
        conn=conn,
        amount=48000.00,
        description="Management fees $48,000 — Frais de gestion approuvés par le CA",
        transaction_date="2025-12-31",
    )

    # Related party 2: Construction Tremblay Frères Inc.
    rp2_id = add_related_party(
        client_code="BOLDUC",
        party_name="Construction Tremblay Frères Inc.",
        relationship_type="affiliated_company",
        conn=conn,
        notes="Société apparentée — Michel Tremblay (frère). "
              "Appels d'offres obtenus. Prix du marché confirmé.",
        identified_by="sam",
    )
    flag_related_party_transaction(
        engagement_id=engagement_id,
        document_id=f"DOC-RPT2-{secrets.token_hex(4).upper()}",
        party_id=rp2_id,
        measurement_basis="exchange_amount",
        conn=conn,
        amount=127500.00,
        description="Sous-traitance $127,500 — Appels d'offres obtenus, prix du marché confirmé",
        transaction_date="2025-12-31",
    )

    print(f"  [RELATED PARTIES] Inserted 2 parties + 2 transactions")
    return 2


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 9 — REP LETTER (CAS 580)
# ═══════════════════════════════════════════════════════════════════════════

def populate_rep_letter(conn: sqlite3.Connection, engagement_id: str) -> None:
    from src.engines.cas_engine import save_rep_letter, get_rep_letter

    existing = get_rep_letter(engagement_id, conn)
    if existing:
        print(f"  [REP LETTER] Already exists: {existing['letter_id']}")
        return

    draft_fr = (
        "Nous confirmons, au meilleur de notre connaissance:\n"
        "1. Les états financiers sont présentés fidèlement.\n"
        "2. Toutes les opérations ont été enregistrées.\n"
        "3. Aucun événement subséquent ne nécessite un ajustement.\n"
        "4. Toutes les parties apparentées ont été divulguées.\n"
        "5. L'hypothèse de continuité d'exploitation est appropriée.\n\n"
        "Jean-Pierre Tremblay, Président\n"
        "Date: 2026-02-15\n"
        "Statut: En attente de signature"
    )
    draft_en = (
        "We confirm, to the best of our knowledge:\n"
        "1. The financial statements are fairly presented.\n"
        "2. All transactions have been recorded.\n"
        "3. No subsequent events require adjustment.\n"
        "4. All related parties have been disclosed.\n"
        "5. The going concern assumption is appropriate.\n\n"
        "Jean-Pierre Tremblay, President\n"
        "Date: 2026-02-15\n"
        "Status: Pending signature"
    )

    letter_id = save_rep_letter(
        engagement_id=engagement_id,
        draft_fr=draft_fr,
        draft_en=draft_en,
        conn=conn,
        created_by="sam",
    )
    print(f"  [REP LETTER] Created: {letter_id} (pending signature)")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 10 — FINANCIAL STATEMENTS (Trial Balance)
# ═══════════════════════════════════════════════════════════════════════════

def populate_trial_balance(conn: sqlite3.Connection) -> int:
    from src.engines.audit_engine import ensure_audit_tables

    ensure_audit_tables(conn)

    entries = [
        ("1010", "Encaisse",                     127450.32,       0.0),
        ("1100", "Comptes clients",              342800.00,       0.0),
        ("1500", "Immobilisations (net)",         687250.00,       0.0),
        ("2000", "Comptes fournisseurs",              0.0,   89340.00),
        ("2500", "Dette à long terme",                0.0,  425000.00),
        ("3000", "Capital-actions",                   0.0,   50000.00),
        ("3100", "Bénéfices non répartis ouverture",  0.0,  345160.32),
        ("4000", "Revenus",                           0.0, 2847600.00),
        ("5100", "Coût des ventes",             1923400.00,       0.0),
        ("6000", "Charges d'exploitation",       676200.00,       0.0),
    ]

    # Check if already populated
    existing = conn.execute(
        "SELECT COUNT(*) as cnt FROM trial_balance WHERE LOWER(client_code) = 'bolduc' AND period = '2025'",
    ).fetchone()
    if existing and existing["cnt"] >= 10:
        print(f"  [TRIAL BALANCE] Already has {existing['cnt']} rows")
        return existing["cnt"]

    now = _utc_now()
    count = 0
    for acct_code, acct_name, debit, credit in entries:
        net = debit - credit
        conn.execute(
            """INSERT INTO trial_balance
               (client_code, period, account_code, account_name,
                debit_total, credit_total, net_balance, generated_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(client_code, period, account_code) DO UPDATE SET
                   account_name = excluded.account_name,
                   debit_total  = excluded.debit_total,
                   credit_total = excluded.credit_total,
                   net_balance  = excluded.net_balance,
                   generated_at = excluded.generated_at""",
            ("BOLDUC", "2025", acct_code, acct_name, debit, credit, net, now),
        )
        count += 1
    conn.commit()
    print(f"  [TRIAL BALANCE] Inserted {count} entries")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 11 — ANALYTICAL PROCEDURES
# ═══════════════════════════════════════════════════════════════════════════

def populate_analytical_procedures(conn: sqlite3.Connection) -> int:
    """Insert prior-year trial balance so analytical procedures can compare."""
    from src.engines.audit_engine import ensure_audit_tables

    ensure_audit_tables(conn)

    # Prior year (2024) trial balance for comparison
    prior_entries = [
        ("1010", "Encaisse",                      98200.00,       0.0),
        ("1100", "Comptes clients",              310500.00,       0.0),
        ("1500", "Immobilisations (net)",         612000.00,       0.0),
        ("2000", "Comptes fournisseurs",              0.0,   76200.00),
        ("2500", "Dette à long terme",                0.0,  475000.00),
        ("3000", "Capital-actions",                   0.0,   50000.00),
        ("3100", "Bénéfices non répartis ouverture",  0.0,  221500.00),
        ("4000", "Revenus",                           0.0, 2654200.00),
        ("5100", "Coût des ventes",             1810200.00,       0.0),
        ("6000", "Charges d'exploitation",       698400.00,       0.0),
    ]

    existing = conn.execute(
        "SELECT COUNT(*) as cnt FROM trial_balance WHERE LOWER(client_code) = 'bolduc' AND period = '2024'",
    ).fetchone()
    if existing and existing["cnt"] >= 10:
        print(f"  [ANALYTICAL] Prior year already has {existing['cnt']} rows")
        return existing["cnt"]

    now = _utc_now()
    count = 0
    for acct_code, acct_name, debit, credit in prior_entries:
        net = debit - credit
        conn.execute(
            """INSERT INTO trial_balance
               (client_code, period, account_code, account_name,
                debit_total, credit_total, net_balance, generated_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(client_code, period, account_code) DO UPDATE SET
                   account_name = excluded.account_name,
                   debit_total  = excluded.debit_total,
                   credit_total = excluded.credit_total,
                   net_balance  = excluded.net_balance,
                   generated_at = excluded.generated_at""",
            ("BOLDUC", "2024", acct_code, acct_name, debit, credit, net, now),
        )
        count += 1
    conn.commit()

    # Print analytical comparison
    print(f"  [ANALYTICAL] Inserted {count} prior-year entries")
    print("    Revenue:   2025 $2,847,600 vs 2024 $2,654,200 (+7.3%) — New contracts in Quebec City")
    print("    Gross Margin: 2025 32.5% vs 2024 31.8% — Improved subcontractor pricing")
    print("    OpEx:      2025 $676,200 vs 2024 $698,400 (-3.2%) — Reduced admin costs")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 12 — BANK RECONCILIATION
# ═══════════════════════════════════════════════════════════════════════════

def populate_bank_reconciliation(conn: sqlite3.Connection) -> None:
    # Ensure table exists
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bank_reconciliations (
            reconciliation_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            account_name TEXT NOT NULL,
            account_number TEXT,
            period_end_date TEXT NOT NULL,
            statement_ending_balance REAL NOT NULL,
            gl_ending_balance REAL NOT NULL,
            deposits_in_transit TEXT DEFAULT '[]',
            outstanding_cheques TEXT DEFAULT '[]',
            bank_errors TEXT DEFAULT '[]',
            book_errors TEXT DEFAULT '[]',
            adjusted_bank_balance REAL,
            adjusted_book_balance REAL,
            difference REAL,
            status TEXT NOT NULL DEFAULT 'open',
            prepared_by TEXT,
            reviewed_by TEXT,
            prepared_at TEXT,
            reviewed_at TEXT,
            finalized_at TEXT,
            notes TEXT
        );
    """)
    conn.commit()

    # Check existing
    existing = conn.execute(
        "SELECT reconciliation_id FROM bank_reconciliations WHERE LOWER(client_code) = 'bolduc' AND period_end_date = '2025-12-31'",
    ).fetchone()
    if existing:
        print(f"  [RECONCILIATION] Already exists: {existing['reconciliation_id']}")
        return

    recon_id = f"recon_{secrets.token_hex(8)}"
    deposits_in_transit = json.dumps([
        {"description": "Deposit Dec 30", "amount": 5250.14, "date": "2025-12-30"}
    ])
    outstanding_cheques = json.dumps([
        {"description": "Cheque #4521", "amount": 2000.00, "date": "2025-12-28"}
    ])

    conn.execute(
        """INSERT INTO bank_reconciliations
           (reconciliation_id, client_code, account_name, account_number,
            period_end_date, statement_ending_balance, gl_ending_balance,
            deposits_in_transit, outstanding_cheques,
            adjusted_bank_balance, adjusted_book_balance, difference,
            status, prepared_by, finalized_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            recon_id, "BOLDUC", "Compte courant Desjardins", "815-30201-001",
            "2025-12-31", 127450.32, 124200.18,
            deposits_in_transit, outstanding_cheques,
            130700.46, 130700.46, 0.00,
            "balanced", "sam", "2026-01-15",
            "Bank reconciliation balanced. All items cleared.",
        ),
    )
    conn.commit()
    print(f"  [RECONCILIATION] Created: {recon_id} (balanced, diff=0.00)")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 13 — FIXED ASSETS
# ═══════════════════════════════════════════════════════════════════════════

def populate_fixed_assets(conn: sqlite3.Connection) -> int:
    from src.engines.fixed_assets_engine import (
        ensure_fixed_assets_table, add_asset, calculate_annual_cca, list_assets,
    )
    ensure_fixed_assets_table(conn)

    # Check existing
    existing = list_assets("BOLDUC", conn)
    if len(existing) >= 5:
        print(f"  [FIXED ASSETS] Already has {len(existing)} assets")
        return len(existing)

    assets = [
        ("Camion Ford F-350 2023",      10, 62000.00,  "2023-03-15"),
        ("Excavatrice Caterpillar",      43, 185000.00, "2022-06-01"),
        ("Équipement de bureau",          8, 12500.00,  "2024-01-10"),
        ("Ordinateurs (3 units)",        50, 8700.00,   "2024-09-01"),
        ("Entrepôt Saint-Laurent",        1, 450000.00, "2020-01-01"),
    ]

    count = 0
    for name, cca_class, cost, acq_date in assets:
        asset_id = add_asset(
            client_code="BOLDUC",
            asset_name=name,
            acquisition_date=acq_date,
            cost=cost,
            cca_class=cca_class,
            conn=conn,
        )
        count += 1
        print(f"    Added: {name} (Class {cca_class}, ${cost:,.2f}) -> {asset_id}")

    # Calculate 2025 CCA
    cca_results = calculate_annual_cca("BOLDUC", "2025-12-31", conn)
    total_cca = sum(r["cca_amount"] for r in cca_results)
    print(f"  [FIXED ASSETS] Inserted {count} assets, 2025 CCA = ${total_cca:,.2f}")
    return count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 14 — AGING REPORTS
# ═══════════════════════════════════════════════════════════════════════════

def populate_aging(conn: sqlite3.Connection) -> int:
    from src.engines.aging_engine import ensure_ar_invoices_table, create_ar_invoice, send_ar_invoice

    # --- AP aging: insert documents for AP items ---
    # Ensure documents table exists
    conn.executescript("""
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
            raw_result TEXT,
            submitted_by TEXT,
            client_note TEXT,
            invoice_number TEXT,
            invoice_number_normalized TEXT,
            currency TEXT DEFAULT 'CAD',
            subtotal REAL,
            tax_total REAL,
            extraction_method TEXT,
            ingest_source TEXT,
            fraud_flags TEXT,
            fraud_override_reason TEXT,
            fraud_override_locked INTEGER NOT NULL DEFAULT 0,
            substance_flags TEXT,
            entry_kind TEXT,
            review_history TEXT DEFAULT '[]',
            raw_ocr_text TEXT,
            hallucination_suspected INTEGER NOT NULL DEFAULT 0,
            correction_count INTEGER NOT NULL DEFAULT 0,
            handwriting_low_confidence INTEGER NOT NULL DEFAULT 0,
            handwriting_sample INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT ''
        );
    """)
    conn.commit()

    ap_items = [
        ("Fournisseurs Matériaux Bolduc",   12400.00, "2025-12-15"),  # current
        ("Construction Béton Québec",         8750.00, "2025-12-10"),  # current
        ("Location Équipement Laval",         5200.00, "2025-11-15"),  # 31-60 days
        ("Plomberie Tremblay Inc.",           3800.00, "2025-11-10"),  # 31-60 days
        ("Électricité Côté",                  2100.00, "2025-10-20"),  # 61-90 days
        ("Sous-traitant Gagnon",             18500.00, "2025-10-10"),  # 61-90 days
        ("Fournisseur Acier MTL",            24890.00, "2025-09-01"),  # 90+ OVERDUE
        ("Béton Provincial",                 13700.00, "2025-08-15"),  # 90+ OVERDUE
    ]

    ap_count = 0
    for vendor, amount, date in ap_items:
        doc_id = f"DOC-AP-{secrets.token_hex(6).upper()}"
        # Check if vendor already has an entry
        existing = conn.execute(
            "SELECT document_id FROM documents WHERE client_code = 'BOLDUC' AND vendor = ? AND amount = ?",
            (vendor, amount),
        ).fetchone()
        if existing:
            continue
        now = _utc_now()
        conn.execute(
            """INSERT INTO documents
               (document_id, file_name, file_path, client_code, vendor, doc_type, amount, document_date,
                gl_account, review_status, confidence, raw_result, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (doc_id, f"BOLDUC_AP_{vendor[:10]}.pdf", f"/documents/bolduc/{doc_id}.pdf",
             "BOLDUC", vendor, "invoice", amount, date,
             "2000", "Needs Review", 0.95, "{}", now, now),
        )
        ap_count += 1
    conn.commit()

    # --- AR aging: insert AR invoices ---
    ensure_ar_invoices_table(conn)

    ar_items = [
        ("Projet Résidentiel Laval",        45000.00, "2025-12-10", "2026-01-10"),
        ("École Saint-Joseph",              28500.00, "2025-11-15", "2025-12-15"),
        ("Ville de Québec",                 67200.00, "2025-12-05", "2026-01-05"),
        ("Condo Le Plateau",                12800.00, "2025-10-15", "2025-11-15"),
        ("Immeuble Commercial Rive-Sud",    89300.00, "2025-09-01", "2025-10-01"),
    ]

    ar_count = 0
    for customer, amount, inv_date, due_date in ar_items:
        # Check if already exists
        existing = conn.execute(
            "SELECT invoice_id FROM ar_invoices WHERE client_code = 'BOLDUC' AND customer_name = ? AND total_amount = ?",
            (customer, amount),
        ).fetchone()
        if existing:
            continue
        inv = create_ar_invoice(
            client_code="BOLDUC",
            customer_name=customer,
            invoice_date=inv_date,
            due_date=due_date,
            amount_ht=amount,
            description=f"Construction — {customer}",
            created_by="sam",
            conn=conn,
        )
        # Mark as sent so it shows in aging
        send_ar_invoice(inv["invoice_id"], conn)
        ar_count += 1

    print(f"  [AGING] Inserted {ap_count} AP items + {ar_count} AR invoices")
    return ap_count + ar_count


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 15 — CASH FLOW
# ═══════════════════════════════════════════════════════════════════════════

def populate_cash_flow(conn: sqlite3.Connection) -> dict:
    from src.engines.cashflow_engine import generate_cash_flow_statement

    result = generate_cash_flow_statement("BOLDUC", "2025-01-01", "2025-12-31", conn)
    net = result.get("net_change_in_cash", 0)
    closing = result.get("closing_cash_balance", 0)
    print(f"  [CASH FLOW] Generated: net change ${net:,.2f}, closing cash ${closing:,.2f}")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 16 — T2 PRE-FILL
# ═══════════════════════════════════════════════════════════════════════════

def populate_t2(conn: sqlite3.Connection) -> dict:
    from src.engines.t2_engine import generate_t2_prefill

    result = generate_t2_prefill("BOLDUC", "2025-12-31", conn)
    schedules = []
    for key in ("schedule_1", "schedule_8", "schedule_50", "schedule_100", "schedule_125", "co17"):
        if key in result:
            schedules.append(key)
    print(f"  [T2] Pre-filled: {', '.join(schedules)}")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    print("=" * 60)
    print("BOLDUC Demo Data — Populating ALL Modules")
    print("=" * 60)

    conn = open_db()

    # SECTION 1: Engagement
    print("\n[1/16] Engagement...")
    engagement_id = populate_engagement(conn)

    # SECTION 2: Materiality
    print("\n[2/16] Materiality (CAS 320)...")
    populate_materiality(conn, engagement_id)

    # SECTION 3: Risk Assessment
    print("\n[3/16] Risk Assessment (CAS 315)...")
    populate_risk_assessment(conn, engagement_id)

    # SECTION 4: Working Papers
    print("\n[4/16] Working Papers...")
    populate_working_papers(conn)

    # SECTION 5: Audit Evidence
    print("\n[5/16] Audit Evidence (CAS 500)...")
    populate_audit_evidence(conn)

    # SECTION 6: Audit Sample
    print("\n[6/16] Audit Sample (CAS 530)...")
    populate_audit_sample(conn, engagement_id)

    # SECTION 7: Controls
    print("\n[7/16] Controls (CAS 330)...")
    populate_controls(conn, engagement_id)

    # SECTION 8: Related Parties
    print("\n[8/16] Related Parties (CAS 550)...")
    populate_related_parties(conn, engagement_id)

    # SECTION 9: Rep Letter
    print("\n[9/16] Management Representation Letter (CAS 580)...")
    populate_rep_letter(conn, engagement_id)

    # SECTION 10: Financial Statements
    print("\n[10/16] Trial Balance / Financial Statements...")
    populate_trial_balance(conn)

    # SECTION 11: Analytical Procedures
    print("\n[11/16] Analytical Procedures (prior year)...")
    populate_analytical_procedures(conn)

    # SECTION 12: Bank Reconciliation
    print("\n[12/16] Bank Reconciliation...")
    populate_bank_reconciliation(conn)

    # SECTION 13: Fixed Assets
    print("\n[13/16] Fixed Assets + CCA...")
    populate_fixed_assets(conn)

    # SECTION 14: Aging Reports
    print("\n[14/16] Aging Reports (AP + AR)...")
    populate_aging(conn)

    # SECTION 15: Cash Flow
    print("\n[15/16] Cash Flow Statement...")
    populate_cash_flow(conn)

    # SECTION 16: T2 Pre-fill
    print("\n[16/16] T2 Pre-fill...")
    populate_t2(conn)

    # ─── SUMMARY ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("=== BOLDUC Demo Data Summary ===")
    print("=" * 60)

    tables_to_check = [
        ("engagements",                    "BOLDUC engagement"),
        ("materiality_assessments",        "materiality assessments"),
        ("risk_assessments",               "risk assessment rows"),
        ("working_papers",                 "working papers"),
        ("working_paper_items",            "working paper items"),
        ("audit_evidence",                 "audit evidence items"),
        ("control_tests",                  "control tests"),
        ("related_parties",                "related parties"),
        ("related_party_transactions",     "related party transactions"),
        ("management_representation_letters", "rep letters"),
        ("trial_balance",                  "trial balance entries"),
        ("bank_reconciliations",           "bank reconciliations"),
        ("fixed_assets",                   "fixed assets"),
        ("ar_invoices",                    "AR invoices"),
    ]

    for table, label in tables_to_check:
        try:
            if table in ("engagements", "fixed_assets", "ar_invoices", "bank_reconciliations",
                         "related_parties"):
                row = conn.execute(
                    f"SELECT COUNT(*) as cnt FROM {table} WHERE LOWER(client_code) = 'bolduc'"
                ).fetchone()
            elif table == "trial_balance":
                row = conn.execute(
                    f"SELECT COUNT(*) as cnt FROM {table} WHERE LOWER(client_code) = 'bolduc'"
                ).fetchone()
            elif table in ("materiality_assessments", "risk_assessments", "control_tests",
                           "related_party_transactions", "management_representation_letters"):
                row = conn.execute(
                    f"SELECT COUNT(*) as cnt FROM {table} WHERE engagement_id = ?",
                    (engagement_id,),
                ).fetchone()
            elif table == "working_papers":
                row = conn.execute(
                    f"SELECT COUNT(*) as cnt FROM {table} WHERE LOWER(client_code) = 'bolduc'"
                ).fetchone()
            else:
                row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
            count = row["cnt"] if row else 0
            print(f"  {label:.<45} {count:>4} records")
        except Exception as e:
            print(f"  {label:.<45} ERROR: {e}")

    # Verify key relationships
    print("\n--- Verification ---")
    eng = conn.execute(
        "SELECT * FROM engagements WHERE engagement_id = ?", (engagement_id,)
    ).fetchone()
    if eng:
        print(f"  Engagement: {eng['engagement_id']} | {eng['client_code']} | {eng['period']} | {eng['status']} | partner={eng['partner']}")

    mat = conn.execute(
        "SELECT * FROM materiality_assessments WHERE engagement_id = ? ORDER BY calculated_at DESC LIMIT 1",
        (engagement_id,),
    ).fetchone()
    if mat:
        print(f"  Materiality: planning=${mat['planning_materiality']:,.2f} | performance=${mat['performance_materiality']:,.2f} | trivial=${mat['clearly_trivial']:,.2f}")

    risk_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM risk_assessments WHERE engagement_id = ? AND significant_risk = 1",
        (engagement_id,),
    ).fetchone()
    if risk_count:
        print(f"  Significant risks: {risk_count['cnt']}")

    recon = conn.execute(
        "SELECT * FROM bank_reconciliations WHERE LOWER(client_code) = 'bolduc' ORDER BY period_end_date DESC LIMIT 1"
    ).fetchone()
    if recon:
        diff_val = recon['difference']
        diff_display = f"${float(diff_val):.2f}" if diff_val is not None else "N/A"
        print(f"  Bank recon: diff={diff_display} | status={recon['status']}")

    print("\nDone. All 16 modules populated for BOLDUC.")
    conn.close()


if __name__ == "__main__":
    main()
