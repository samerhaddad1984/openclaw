#!/usr/bin/env python3
"""
scripts/generate_canada_quebec_stress_test.py
=============================================
Comprehensive stress test covering all Quebec and Canadian CPA-specific traps.

Parts
-----
1. Quebec GST/QST Tax Traps           — 200 transactions (8 error types)
2. ASPE vs IFRS Traps                 — 20 financial statement scenarios
3. CAS Logic Stress Tests             — 30 audit scenarios
4. Quebec Payroll Audit               — 100 payroll transactions
5. CCA (Capital Cost Allowance)       — 50 capital asset purchases

Usage
-----
    python scripts/generate_canada_quebec_stress_test.py
    python scripts/generate_canada_quebec_stress_test.py --validate
    python scripts/generate_canada_quebec_stress_test.py --only part1
"""
from __future__ import annotations

import argparse
import json
import random
import secrets
import sqlite3
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.engines.tax_engine import (
    GST_RATE,
    QST_RATE,
    HST_RATE_ON,
    validate_quebec_tax_compliance,
    _round,
    _to_decimal,
)
from src.engines.payroll_engine import (
    validate_hsf_rate,
    validate_qpp_cpp,
    validate_qpip_ei,
    reconcile_rl1_t4,
    validate_cnesst_rate,
    CNESST_INDUSTRY_RATES,
    HSF_MAX_RATE,
    EI_RATE_REGULAR,
    EI_RATE_QUEBEC,
)

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SEED = 2025
_rng = random.Random(SEED)

_CENT = Decimal("0.01")
_GST = Decimal("0.05")
_QST = Decimal("0.09975")
_OLD_QST = Decimal("0.095")
_T_DIV = Decimal("1.14975")


def _d(v: Any) -> Decimal:
    return Decimal(str(v))


def _r2(v: Decimal) -> Decimal:
    return v.quantize(_CENT, rounding=ROUND_HALF_UP)


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _uid() -> str:
    return uuid.uuid4().hex[:12]


def _random_date(start: date = date(2025, 1, 1), end: date = date(2025, 12, 31)) -> date:
    delta = (end - start).days
    return start + timedelta(days=_rng.randint(0, max(delta, 1)))


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

def _open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_tables(conn: sqlite3.Connection) -> None:
    """Create all tables needed for stress test data."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT DEFAULT '',
            file_path TEXT DEFAULT '',
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            review_status TEXT DEFAULT 'Pending',
            confidence REAL DEFAULT 0.0,
            raw_result TEXT DEFAULT '',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT '',
            fraud_flags TEXT DEFAULT '[]',
            currency TEXT DEFAULT 'CAD',
            subtotal REAL,
            tax_total REAL,
            demo INTEGER DEFAULT 0,
            stress_test_type TEXT,
            stress_test_flag TEXT
        );

        CREATE TABLE IF NOT EXISTS audit_findings (
            finding_id TEXT PRIMARY KEY,
            engagement_id TEXT,
            client_code TEXT,
            finding_type TEXT,
            title TEXT,
            content TEXT,
            severity TEXT,
            cas_reference TEXT,
            standard_framework TEXT,
            created_at TEXT,
            created_by TEXT DEFAULT 'stress_test',
            status TEXT DEFAULT 'draft'
        );

        CREATE TABLE IF NOT EXISTS going_concern_assessments (
            assessment_id TEXT PRIMARY KEY,
            client_code TEXT NOT NULL,
            period TEXT,
            working_capital REAL,
            current_ratio REAL,
            debt_covenant_breached INTEGER DEFAULT 0,
            recurring_losses_years INTEGER DEFAULT 0,
            net_income_y1 REAL,
            net_income_y2 REAL,
            net_income_y3 REAL,
            loan_due_within_12m REAL DEFAULT 0,
            total_debt REAL DEFAULT 0,
            material_uncertainty INTEGER DEFAULT 0,
            cas_570_flag TEXT,
            going_concern_opinion TEXT,
            assessed_at TEXT,
            assessed_by TEXT DEFAULT 'stress_test'
        );

        CREATE TABLE IF NOT EXISTS payroll_transactions (
            transaction_id TEXT PRIMARY KEY,
            client_code TEXT,
            employee_name TEXT,
            employee_province TEXT DEFAULT 'QC',
            period TEXT,
            gross_pay REAL,
            total_payroll REAL,
            pension_plan TEXT,
            pension_contribution REAL,
            ei_rate REAL,
            ei_premium REAL,
            qpip_premium REAL,
            hsf_rate REAL,
            hsf_contribution REAL,
            cnesst_rate REAL,
            cnesst_contribution REAL,
            industry_code TEXT,
            rl1_data TEXT,
            t4_data TEXT,
            stress_test_flag TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS capital_assets (
            asset_id TEXT PRIMARY KEY,
            client_code TEXT,
            description TEXT,
            cca_class INTEGER,
            cost REAL,
            acquisition_date TEXT,
            available_for_use_date TEXT,
            cca_rate_used REAL,
            cca_rate_correct REAL,
            half_year_applied INTEGER DEFAULT 1,
            half_year_correct INTEGER DEFAULT 1,
            aia_eligible INTEGER DEFAULT 0,
            aia_applied INTEGER DEFAULT 0,
            year_1_cca_claimed REAL,
            year_1_cca_correct REAL,
            stress_test_flag TEXT,
            created_at TEXT
        );
    """)
    # Add stress_test columns to documents if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    for col, typedef in [
        ("stress_test_type", "TEXT"),
        ("stress_test_flag", "TEXT"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE documents ADD COLUMN {col} {typedef}")
    conn.commit()


# ---------------------------------------------------------------------------
# Quebec vendor / company names
# ---------------------------------------------------------------------------

_QC_VENDORS = [
    "Sysco Québec", "Distribution Métro Inc", "Rona Pro Laval",
    "Groupe BMR", "Papiers Cascades", "Hydro-Québec",
    "Bell Communications", "Vidéotron Affaires", "Desjardins",
    "Mouvement des caisses populaires", "Les Brasseurs du Nord",
    "Première Moisson", "Provigo Commerce", "IGA Extra",
    "Jean Coutu Group", "Alimentation Couche-Tard",
    "CAA-Québec", "Gaz Métro / Énergir", "SNC-Lavalin",
    "CGI Group", "National Bank of Canada", "Saputo Inc",
    "Bombardier", "WSP Global", "Transcontinental",
]

_ON_VENDORS = [
    "Staples Ontario", "Loblaws Toronto", "Rogers Communications",
    "TD Canada Trust", "Shoppers Drug Mart", "Canadian Tire ON",
    "Purolator Ottawa", "Bell Ontario", "Home Depot Mississauga",
    "CIBC Toronto",
]

_QC_COMPANIES = [
    ("STRESS_REST", "Restaurant Le Festin QC", "72010"),
    ("STRESS_CONST", "Construction Bélanger Inc", "23010"),
    ("STRESS_TECH", "Solutions TI Montréal", "54020"),
    ("STRESS_TRANS", "Transport Léveillé Inc", "48010"),
    ("STRESS_CLEAN", "Services Nettoyage Brillant", "56010"),
    ("STRESS_RETAIL", "Boutique Mode St-Laurent", "52010"),
    ("STRESS_DENTAL", "Cabinet Dentaire Laval", "62010"),
    ("STRESS_DAYCARE", "Garderie Arc-en-Ciel", "71010"),
    ("STRESS_PLUMB", "Plomberie Gagnon Inc", "23040"),
    ("STRESS_ROOF", "Toitures Québec Inc", "23050"),
]

_EMPLOYEE_NAMES = [
    "Jean-Pierre Tremblay", "Marie-Josée Gagnon", "Pierre-Luc Bouchard",
    "Isabelle Côté", "François Lapointe", "Nathalie Bélanger",
    "Stéphane Gauthier", "Véronique Morin", "Luc Pelletier",
    "Chantal Bergeron", "Patrick Roy", "Julie Deschênes",
    "Martin Fortin", "Sylvie Leblanc", "André Simard",
    "Caroline Ouellet", "Réjean Girard", "Manon Théberge",
    "Daniel Beaulieu", "Sophie Savard",
]


# ═══════════════════════════════════════════════════════════════════════════
# PART 1 — Quebec GST/QST Tax Traps (200 transactions)
# ═══════════════════════════════════════════════════════════════════════════

def generate_part1_tax_traps(conn: sqlite3.Connection) -> list[dict]:
    """Generate 200 transactions with 8 types of Quebec tax errors."""
    print("\n== PART 1: Quebec GST/QST Tax Traps (200 transactions) ==")
    transactions: list[dict] = []
    now = _utcnow()

    def _insert(doc: dict) -> None:
        raw = json.dumps(doc.get("raw_result", {}), ensure_ascii=False)
        conn.execute(
            """INSERT OR REPLACE INTO documents
               (document_id, file_name, file_path, client_code, vendor,
                doc_type, amount, document_date, gl_account, tax_code,
                category, review_status, confidence, raw_result,
                subtotal, tax_total,
                created_at, updated_at, stress_test_type, stress_test_flag, demo)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (
                doc["document_id"], f"{doc['document_id']}.pdf", "",
                doc.get("client_code", "STRESS_REST"),
                doc["vendor"], "invoice", doc["amount"],
                doc["document_date"], doc.get("gl_account", "Achats"),
                doc.get("tax_code", "T"), doc.get("category", ""),
                "Pending", 0.95, raw,
                doc.get("subtotal", 0), doc.get("tax_total", 0),
                now, now, "part1_tax_trap", doc["flag"],
            ),
        )
        transactions.append(doc)

    # ── 1. Tax-on-tax trap (10 transactions) ──
    for i in range(10):
        subtotal = _r2(_d(_rng.uniform(100, 5000)))
        correct_gst = _r2(subtotal * _GST)
        correct_qst = _r2(subtotal * _QST)
        # Wrong: QST on GST-inclusive amount
        wrong_qst = _r2((subtotal + correct_gst) * _QST)
        total = float(subtotal + correct_gst + wrong_qst)
        _insert({
            "document_id": f"st1_tax_on_tax_{i:03d}",
            "vendor": _rng.choice(_QC_VENDORS),
            "amount": total,
            "subtotal": float(subtotal),
            "tax_total": float(correct_gst + wrong_qst),
            "document_date": _random_date().isoformat(),
            "flag": "tax_on_tax_error",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(correct_gst),
                "qst": float(wrong_qst),
                "qst_correct": float(correct_qst),
                "error": "QST calculated on GST-inclusive amount",
            },
        })
    print(f"  [1/8] tax_on_tax_error: 10 transactions")

    # ── 2. Large business ITR restriction (10 transactions) ──
    for i in range(10):
        subtotal = _r2(_d(_rng.uniform(500, 10000)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        total = float(subtotal + gst + qst)
        expense_types = ["fuel", "vehicle", "road_vehicle", "carburant", "véhicule_routier"]
        _insert({
            "document_id": f"st1_large_itr_{i:03d}",
            "client_code": "STRESS_TRANS",
            "vendor": _rng.choice(["Petro-Canada", "Shell Québec", "Esso Laval",
                                   "Irving Oil", "Ultramar", "Couche-Tard Carburant"]),
            "amount": total,
            "subtotal": float(subtotal),
            "tax_total": float(gst + qst),
            "document_date": _random_date().isoformat(),
            "gl_account": "Carburant et véhicules",
            "flag": "large_business_itr_restricted",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(gst),
                "qst": float(qst),
                "company_revenue": 15000000,
                "expense_type": _rng.choice(expense_types),
                "itr_claimed": float(qst),
                "restriction": "Large business ITR restricted on fuel/vehicles",
            },
        })
    print(f"  [2/8] large_business_itr_restricted: 10 transactions")

    # ── 3. Unregistered supplier charging tax (10 transactions) ──
    small_vendors = [
        "Marie's Homemade Jams", "Artisan Bois Lévis", "Couture Maison QC",
        "Pâtisserie du Coin", "Jardinage Petit-Bonheur", "Tricot Main Alma",
        "Savons Naturels Gaspé", "Poterie Artisanale Mtl", "Bijoux Fait-Main",
        "Bougies Québec Enr",
    ]
    for i in range(10):
        subtotal = _r2(_d(_rng.uniform(50, 500)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        vendor_rev = _rng.uniform(5000, 28000)
        _insert({
            "document_id": f"st1_unreg_supplier_{i:03d}",
            "vendor": small_vendors[i],
            "amount": float(subtotal + gst + qst),
            "subtotal": float(subtotal),
            "tax_total": float(gst + qst),
            "document_date": _random_date().isoformat(),
            "flag": "unregistered_supplier_charging_tax",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(gst),
                "qst": float(qst),
                "vendor_revenue": round(vendor_rev, 2),
                "error": "Small supplier under $30K threshold charging tax",
            },
        })
    print(f"  [3/8] unregistered_supplier_charging_tax: 10 transactions")

    # ── 4. Wrong QST rate (5 transactions) ──
    for i in range(5):
        subtotal = _r2(_d(_rng.uniform(200, 3000)))
        gst = _r2(subtotal * _GST)
        wrong_qst = _r2(subtotal * _OLD_QST)  # 9.5% instead of 9.975%
        correct_qst = _r2(subtotal * _QST)
        _insert({
            "document_id": f"st1_wrong_qst_{i:03d}",
            "vendor": _rng.choice(_QC_VENDORS),
            "amount": float(subtotal + gst + wrong_qst),
            "subtotal": float(subtotal),
            "tax_total": float(gst + wrong_qst),
            "document_date": _random_date().isoformat(),
            "flag": "wrong_qst_rate",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(gst),
                "qst": float(wrong_qst),
                "qst_rate_used": 0.095,
                "qst_correct": float(correct_qst),
                "error": "Old 9.5% QST rate used instead of 9.975%",
            },
        })
    print(f"  [4/8] wrong_qst_rate: 5 transactions")

    # ── 5. Missing registration number (10 transactions) ──
    for i in range(10):
        subtotal = _r2(_d(_rng.uniform(50, 2000)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        _insert({
            "document_id": f"st1_no_reg_{i:03d}",
            "vendor": _rng.choice(_QC_VENDORS),
            "amount": float(subtotal + gst + qst),
            "subtotal": float(subtotal),
            "tax_total": float(gst + qst),
            "document_date": _random_date().isoformat(),
            "flag": "missing_registration_number",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(gst),
                "qst": float(qst),
                "gst_registration": "",
                "qst_registration": "",
                "error": "No GST/QST registration numbers on invoice >$30",
            },
        })
    print(f"  [5/8] missing_registration_number: 10 transactions")

    # ── 6. Cross-provincial error (10 transactions) ──
    for i in range(10):
        subtotal = _r2(_d(_rng.uniform(200, 5000)))
        if i < 5:
            # Ontario vendor charging QST instead of HST
            vendor = _rng.choice(_ON_VENDORS)
            qst = _r2(subtotal * _QST)
            gst = _r2(subtotal * _GST)
            _insert({
                "document_id": f"st1_cross_prov_{i:03d}",
                "vendor": vendor,
                "amount": float(subtotal + gst + qst),
                "subtotal": float(subtotal),
                "tax_total": float(gst + qst),
                "document_date": _random_date().isoformat(),
                "flag": "wrong_provincial_tax",
                "raw_result": {
                    "subtotal": float(subtotal),
                    "gst": float(gst),
                    "qst": float(qst),
                    "vendor_province": "ON",
                    "error": "Ontario vendor charging QST instead of HST",
                    "correct_tax": f"HST at 13% = ${float(_r2(subtotal * Decimal('0.13')))}",
                },
            })
        else:
            # Quebec vendor charging HST
            vendor = _rng.choice(_QC_VENDORS)
            hst = _r2(subtotal * Decimal("0.13"))
            _insert({
                "document_id": f"st1_cross_prov_{i:03d}",
                "vendor": vendor,
                "amount": float(subtotal + hst),
                "subtotal": float(subtotal),
                "tax_total": float(hst),
                "tax_code": "HST",
                "document_date": _random_date().isoformat(),
                "flag": "wrong_provincial_tax",
                "raw_result": {
                    "subtotal": float(subtotal),
                    "hst": float(hst),
                    "vendor_province": "QC",
                    "error": "Quebec vendor charging HST instead of GST+QST",
                },
            })
    print(f"  [6/8] wrong_provincial_tax: 10 transactions")

    # ── 7. Exempt item taxed incorrectly (10 transactions) ──
    exempt_items = [
        ("basic_groceries", "IGA Extra", "Milk, bread, eggs"),
        ("basic_groceries", "Provigo Commerce", "Fresh produce"),
        ("basic_groceries", "Métro Épicerie", "Rice, flour, sugar"),
        ("basic_groceries", "SuperC Québec", "Canned vegetables"),
        ("basic_groceries", "Maxi Laval", "Frozen meat"),
        ("medical_services", "Dr. Tremblay Cabinet", "Medical consultation"),
        ("medical_services", "Clinique Santé Plus", "Blood test"),
        ("medical_services", "Physiothérapie Laval", "Physiotherapy session"),
        ("medical_services", "Optométrie Vision QC", "Eye exam"),
        ("medical_services", "Dentiste Dr. Roy", "Dental cleaning"),
    ]
    for i, (cat, vendor, desc) in enumerate(exempt_items):
        subtotal = _r2(_d(_rng.uniform(20, 500)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST) if cat == "basic_groceries" else Decimal("0")
        _insert({
            "document_id": f"st1_exempt_taxed_{i:03d}",
            "vendor": vendor,
            "category": cat,
            "amount": float(subtotal + gst + qst),
            "subtotal": float(subtotal),
            "tax_total": float(gst + qst),
            "document_date": _random_date().isoformat(),
            "gl_account": "Achats exemptés" if "groceries" in cat else "Services médicaux",
            "flag": "exempt_item_taxed",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(gst),
                "qst": float(qst),
                "category": cat,
                "description": desc,
                "error": f"Exempt category '{cat}' incorrectly taxed",
            },
        })
    print(f"  [7/8] exempt_item_taxed: 10 transactions")

    # ── 8. Quick Method error (5 transactions) ──
    for i in range(5):
        subtotal = _r2(_d(_rng.uniform(1000, 20000)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        total = subtotal + gst + qst
        qm_type = "services" if i < 3 else "goods"
        correct_rate = Decimal("0.036") if qm_type == "services" else Decimal("0.066")
        # Error: using full GST+QST rate instead of Quick Method rate
        wrong_rate = _GST + _QST  # 14.975%
        _insert({
            "document_id": f"st1_quick_method_{i:03d}",
            "client_code": _rng.choice(["STRESS_REST", "STRESS_RETAIL"]),
            "vendor": "Revenue collected",
            "amount": float(total),
            "subtotal": float(subtotal),
            "tax_total": float(gst + qst),
            "document_date": _random_date().isoformat(),
            "flag": "quick_method_rate_error",
            "raw_result": {
                "subtotal": float(subtotal),
                "total_with_tax": float(total),
                "quick_method": True,
                "quick_method_type": qm_type,
                "remittance_rate_used": float(wrong_rate),
                "correct_rate": float(correct_rate),
                "over_remittance": float(_r2(total * (wrong_rate - correct_rate))),
                "error": f"Quick Method: using full rate instead of {correct_rate}",
            },
        })
    print(f"  [8/8] quick_method_rate_error: 5 transactions")

    # ── Fill remaining with correct transactions to reach 200 ──
    correct_count = 200 - len(transactions)
    for i in range(correct_count):
        subtotal = _r2(_d(_rng.uniform(50, 5000)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        _insert({
            "document_id": f"st1_correct_{i:03d}",
            "vendor": _rng.choice(_QC_VENDORS),
            "amount": float(subtotal + gst + qst),
            "subtotal": float(subtotal),
            "tax_total": float(gst + qst),
            "document_date": _random_date().isoformat(),
            "flag": "correct",
            "raw_result": {
                "subtotal": float(subtotal),
                "gst": float(gst),
                "qst": float(qst),
                "gst_registration": f"RT{_rng.randint(100000000, 999999999)}",
                "qst_registration": f"TQ{_rng.randint(1000000000, 9999999999)}",
            },
        })
    print(f"  [+] correct baseline: {correct_count} transactions")

    conn.commit()
    print(f"  TOTAL Part 1: {len(transactions)} transactions inserted")
    return transactions


# ═══════════════════════════════════════════════════════════════════════════
# PART 2 — ASPE vs IFRS Traps (20 scenarios)
# ═══════════════════════════════════════════════════════════════════════════

def generate_part2_aspe_ifrs(conn: sqlite3.Connection) -> list[dict]:
    """Generate financial statement data for ASPE/IFRS mismatch scenarios."""
    print("\n== PART 2: ASPE vs IFRS Traps (20 scenarios) ==")
    findings: list[dict] = []
    now = _utcnow()

    def _wp(paper: dict) -> None:
        conn.execute(
            """INSERT OR REPLACE INTO audit_findings
               (finding_id, engagement_id, client_code, title,
                content, finding_type, severity, cas_reference,
                standard_framework, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                paper["paper_id"], paper.get("engagement_id", f"eng_{paper['client_code']}"),
                paper["client_code"], paper["title"],
                json.dumps(paper["content"], ensure_ascii=False),
                paper["finding_type"], paper["severity"],
                paper.get("cas_reference", ""), paper.get("standard_framework", ""),
                now,
            ),
        )
        findings.append(paper)

    # ── 1. Lease classification mismatch (5 companies) ──
    lease_companies = [
        ("STRESS_LEASE1", "Imprimerie Rapide QC", 120000, 60, 950),
        ("STRESS_LEASE2", "Transport Express Mtl", 350000, 48, 6500),
        ("STRESS_LEASE3", "Services TI Laval", 85000, 36, 2200),
        ("STRESS_LEASE4", "Restaurant Gourmet QC", 200000, 60, 3100),
        ("STRESS_LEASE5", "Construction Moderne", 500000, 84, 5800),
    ]
    for client_code, name, lease_value, months, monthly_payment in lease_companies:
        total_payments = monthly_payment * months
        rou_asset = round(lease_value * 0.92, 2)  # Present value approx
        _wp({
            "paper_id": f"wp_lease_{client_code}",
            "client_code": client_code,
            "paper_type": "audit_finding",
            "title": f"ASPE/IFRS Lease Classification — {name}",
            "finding_type": "aspe_ifrs_lease_mismatch",
            "severity": "high",
            "cas_reference": "CAS 540 / IFRS 16 / ASPE 3065",
            "standard_framework": "IFRS",
            "content": {
                "company": name,
                "lease_value": lease_value,
                "monthly_payment": monthly_payment,
                "term_months": months,
                "total_payments": total_payments,
                "aspe_treatment": {
                    "classification": "operating_lease",
                    "on_balance_sheet": False,
                    "expense_per_month": monthly_payment,
                    "note": "ASPE 3065 allows operating lease off-balance-sheet",
                },
                "ifrs_treatment": {
                    "classification": "right_of_use_asset",
                    "on_balance_sheet": True,
                    "rou_asset": rou_asset,
                    "lease_liability": rou_asset,
                    "depreciation_per_year": round(rou_asset / (months / 12), 2),
                    "note": "IFRS 16 requires all leases on balance sheet",
                },
                "error": "Company claims IFRS but treats lease as operating (ASPE treatment)",
                "impact": f"Assets and liabilities understated by ~${rou_asset:,.2f}",
            },
        })
    print(f"  [1/4] aspe_ifrs_lease_mismatch: 5 findings")

    # ── 2. Related party transaction measurement (5 transactions) ──
    rp_txns = [
        ("STRESS_RP1", "Gestion Tremblay", "Building", 500000, 850000),
        ("STRESS_RP2", "Holdings Gagnon", "Land", 200000, 375000),
        ("STRESS_RP3", "Famille Bouchard Inc", "Equipment", 80000, 145000),
        ("STRESS_RP4", "Propriétés Roy", "Office condo", 350000, 600000),
        ("STRESS_RP5", "Investissements Côté", "Vehicle fleet", 120000, 210000),
    ]
    for client_code, related_party, asset, carrying, inflated in rp_txns:
        _wp({
            "paper_id": f"wp_rp_{client_code}",
            "client_code": client_code,
            "paper_type": "audit_finding",
            "title": f"Related Party Measurement — {asset} to {related_party}",
            "finding_type": "related_party_measurement_error",
            "severity": "critical",
            "cas_reference": "CAS 550 / ASPE 3840",
            "standard_framework": "ASPE",
            "content": {
                "related_party": related_party,
                "asset_description": asset,
                "carrying_amount": carrying,
                "transaction_price": inflated,
                "overstatement": inflated - carrying,
                "aspe_3840_rule": (
                    "ASPE Section 3840 requires related party transactions "
                    "to be measured at carrying amount (exchange amount) unless "
                    "the transaction has commercial substance and the change "
                    "in future cash flows is significant."
                ),
                "correct_treatment": f"Should be recorded at carrying amount ${carrying:,}",
                "error": f"Asset transferred at inflated value ${inflated:,} (carrying: ${carrying:,})",
                "gain_overstatement": inflated - carrying,
            },
        })
    print(f"  [2/4] related_party_measurement_error: 5 findings")

    # ── 3. Revenue recognition errors (5 scenarios) ──
    rev_scenarios = [
        ("STRESS_REV1", "Construction Bolduc", 2500000, 18, 0.65),
        ("STRESS_REV2", "Bâtiments Québec Inc", 4200000, 24, 0.40),
        ("STRESS_REV3", "Ponts & Routes QC", 8000000, 36, 0.55),
        ("STRESS_REV4", "Rénovations Prestige", 1200000, 12, 0.75),
        ("STRESS_REV5", "Infra-Routes Montréal", 6500000, 30, 0.30),
    ]
    for client_code, name, contract_value, months, pct_complete in rev_scenarios:
        rev_poc = round(contract_value * pct_complete, 2)
        cost_incurred = round(contract_value * 0.7 * pct_complete, 2)
        _wp({
            "paper_id": f"wp_rev_{client_code}",
            "client_code": client_code,
            "paper_type": "audit_finding",
            "title": f"Revenue Recognition — {name}",
            "finding_type": "revenue_recognition_error",
            "severity": "critical",
            "cas_reference": "IFRS 15 / ASPE 3400",
            "standard_framework": "IFRS",
            "content": {
                "company": name,
                "contract_value": contract_value,
                "contract_months": months,
                "pct_complete": pct_complete,
                "costs_incurred": cost_incurred,
                "ifrs_15_treatment": {
                    "method": "percentage_of_completion",
                    "revenue_recognized": rev_poc,
                    "note": "IFRS 15 requires over-time recognition for construction",
                },
                "aspe_treatment": {
                    "method": "completed_contract",
                    "revenue_recognized": 0 if pct_complete < 1.0 else contract_value,
                    "note": "ASPE 3400 allows completed-contract method",
                },
                "error": (
                    f"Construction company using completed-contract method under IFRS. "
                    f"Revenue understated by ${rev_poc:,.2f} at {pct_complete:.0%} completion."
                ),
            },
        })
    print(f"  [3/4] revenue_recognition_error: 5 findings")

    # ── 4. Financial instrument measurement (5 scenarios) ──
    fi_scenarios = [
        ("STRESS_FI1", "Holdings QC", "Private equity investment", 250000, 310000),
        ("STRESS_FI2", "Capital Montréal", "Unlisted shares", 100000, 85000),
        ("STRESS_FI3", "Placements Laval", "Private debt fund", 500000, 520000),
        ("STRESS_FI4", "Investissements Lévis", "Joint venture interest", 175000, 205000),
        ("STRESS_FI5", "Patrimoine Québec", "Private REIT units", 400000, 450000),
    ]
    for client_code, name, desc, cost, fair_value in fi_scenarios:
        _wp({
            "paper_id": f"wp_fi_{client_code}",
            "client_code": client_code,
            "paper_type": "audit_finding",
            "title": f"Financial Instrument — {desc} ({name})",
            "finding_type": "financial_instrument_error",
            "severity": "high",
            "cas_reference": "IFRS 9 / ASPE 3856",
            "standard_framework": "IFRS",
            "content": {
                "company": name,
                "instrument": desc,
                "cost_basis": cost,
                "fair_value": fair_value,
                "difference": fair_value - cost,
                "aspe_treatment": {
                    "method": "cost",
                    "carrying_value": cost,
                    "note": "ASPE 3856 allows cost method for private company investments",
                },
                "ifrs_treatment": {
                    "method": "fair_value_through_PL",
                    "carrying_value": fair_value,
                    "note": "IFRS 9 requires fair value measurement",
                },
                "error": (
                    f"Investment reported at cost ${cost:,} under IFRS — "
                    f"should be fair value ${fair_value:,} (difference: ${fair_value - cost:,})"
                ),
            },
        })
    print(f"  [4/4] financial_instrument_error: 5 findings")

    conn.commit()
    print(f"  TOTAL Part 2: {len(findings)} working paper findings inserted")
    return findings


# ═══════════════════════════════════════════════════════════════════════════
# PART 3 — CAS Logic Stress Tests (30 scenarios)
# ═══════════════════════════════════════════════════════════════════════════

def generate_part3_cas_logic(conn: sqlite3.Connection) -> list[dict]:
    """Generate CAS audit logic stress test scenarios."""
    print("\n== PART 3: CAS Logic Stress Tests (30 scenarios) ==")
    results: list[dict] = []
    now = _utcnow()

    # ── 1. Related party ghost company (10 transactions) ──
    owner_names = [
        ("Pierre Tremblay", "9876 Rue des Érables, Québec"),
        ("Marc Gagnon", "1234 Boul. Laurier, Laval"),
        ("Jean Bouchard", "555 Av. Mont-Royal, Montréal"),
        ("Luc Côté", "789 Ch. Ste-Foy, Québec"),
        ("André Roy", "321 Rue St-Jean, Québec"),
    ]
    for i in range(10):
        owner_name, owner_addr = owner_names[i % len(owner_names)]
        # Create vendor name similar to owner
        parts = owner_name.split()
        ghost_variants = [
            f"Gestion {parts[-1]} Inc",
            f"Services {parts[0][0]}. {parts[-1]}",
            f"Entreprises {parts[-1]} Enr",
            f"Consultation {parts[-1]} QC",
        ]
        ghost_vendor = _rng.choice(ghost_variants)
        subtotal = _r2(_d(_rng.uniform(2000, 50000)))
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        doc_id = f"st3_ghost_rp_{i:03d}"
        conn.execute(
            """INSERT OR REPLACE INTO documents
               (document_id, file_name, file_path, client_code, vendor,
                doc_type, amount,
                document_date, gl_account, tax_code, review_status,
                confidence, raw_result, subtotal, tax_total,
                created_at, updated_at, stress_test_type, stress_test_flag, demo)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (
                doc_id, f"{doc_id}.pdf", "",
                _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
                ghost_vendor, "invoice", float(subtotal + gst + qst),
                _random_date().isoformat(), "Services professionnels", "T",
                "Pending", 0.90,
                json.dumps({
                    "vendor": ghost_vendor,
                    "vendor_address": owner_addr,
                    "owner_name": owner_name,
                    "owner_address": owner_addr,
                    "similarity": "Name/address matches owner",
                    "cas_550_required": True,
                }),
                float(subtotal), float(gst + qst),
                now, now, "part3_cas_logic", "potential_related_party",
            ),
        )
        results.append({"id": doc_id, "flag": "potential_related_party"})
    print(f"  [1/4] potential_related_party: 10 transactions")

    # ── 2. Going concern shocks (5 companies) ──
    gc_companies = [
        ("STRESS_GC1", "Manufacture Dubois QC", -250000, 0.65, -180000, -95000, -320000, 2000000),
        ("STRESS_GC2", "Transport Léveillé", -180000, 0.72, -50000, -120000, -200000, 1500000),
        ("STRESS_GC3", "Restaurant Le Délice", -45000, 0.48, -15000, -22000, -55000, 500000),
        ("STRESS_GC4", "Services TI Québec", -320000, 0.55, -100000, -250000, -400000, 3000000),
        ("STRESS_GC5", "Construction Avenir", -500000, 0.40, -200000, -350000, -600000, 5000000),
    ]
    for (client, name, wc, curr_ratio, ni1, ni2, ni3, loan_due) in gc_companies:
        assessment_id = f"gc_{client}"
        conn.execute(
            """INSERT OR REPLACE INTO going_concern_assessments
               (assessment_id, client_code, period, working_capital,
                current_ratio, debt_covenant_breached, recurring_losses_years,
                net_income_y1, net_income_y2, net_income_y3,
                loan_due_within_12m, total_debt, material_uncertainty,
                cas_570_flag, going_concern_opinion, assessed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                assessment_id, client, "2025",
                wc, curr_ratio,
                1 if curr_ratio < 1.0 else 0,
                3,  # 3 years of losses
                ni1, ni2, ni3,
                loan_due, loan_due * 2.5,
                1,  # material_uncertainty
                json.dumps({
                    "indicators": [
                        f"Negative working capital: ${wc:,}",
                        f"Current ratio {curr_ratio} < 1.0 (covenant breach)",
                        f"Recurring net losses: Y1=${ni1:,}, Y2=${ni2:,}, Y3=${ni3:,}",
                        f"Significant loan of ${loan_due:,} due within 12 months",
                    ],
                    "cas_570_conclusion": "Material uncertainty related to going concern",
                    "required_disclosure": "Emphasis of matter paragraph in auditor's report",
                }),
                f"Material Uncertainty — {name}",
                now,
            ),
        )
        results.append({"id": assessment_id, "flag": "going_concern"})
    print(f"  [2/4] going_concern (CAS 570): 5 assessments")

    # ── 3. Ghost bank account (5 cases) ──
    ghost_banks = [
        ("STRESS_GB1", "1234-567", "Unknown Bank Account #1"),
        ("STRESS_GB2", "9876-543", "Unverified Account Cayman"),
        ("STRESS_GB3", "5555-001", "Offshore Account NR"),
        ("STRESS_GB4", "7777-888", "Personal Account Owner"),
        ("STRESS_GB5", "3210-999", "Dormant Account — No Statement"),
    ]
    for client_code, acct_num, desc in ghost_banks:
        doc_id = f"st3_ghost_bank_{client_code}"
        conn.execute(
            """INSERT OR REPLACE INTO documents
               (document_id, file_name, file_path, client_code, vendor,
                doc_type, amount,
                document_date, gl_account, tax_code, review_status,
                confidence, raw_result, created_at, updated_at,
                stress_test_type, stress_test_flag, demo)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (
                doc_id, f"{doc_id}.pdf", "", client_code,
                f"GL Entry - Bank {acct_num}", "journal_entry",
                float(_rng.uniform(5000, 100000)),
                _random_date().isoformat(),
                f"Bank Account {acct_num}", "NONE",
                "Pending", 0.50,
                json.dumps({
                    "bank_account": acct_num,
                    "description": desc,
                    "bank_statement_on_file": False,
                    "cas_505_required": True,
                    "confirmation_type": "external_bank_confirmation",
                    "error": "GL bank account with no corresponding bank statement",
                }),
                now, now, "part3_cas_logic", "unconfirmed_bank_account",
            ),
        )
        results.append({"id": doc_id, "flag": "unconfirmed_bank_account"})
    print(f"  [3/4] unconfirmed_bank_account (CAS 505): 5 cases")

    # ── 4. Professional skepticism triggers (10 transactions) ──
    skepticism_scenarios = [
        # (description, cas_ref, flag)
        ("Journal entry posted 15 days after period end", "CAS 240", "post_period_entry"),
        ("Journal entry posted 30 days after period end", "CAS 240", "post_period_entry"),
        ("Journal entry posted 45 days after year-end", "CAS 240", "post_period_entry"),
        ("Round $10,000 journal entry — no supporting docs", "CAS 240", "round_number_je"),
        ("Round $25,000 journal entry — owner approved", "CAS 240", "round_number_je"),
        ("Round $50,000 journal entry — no approval", "CAS 240", "round_number_je"),
        ("Entry posted directly by owner (bypassing AP)", "CAS 240", "owner_posted_entry"),
        ("Owner manual entry — no supporting invoice", "CAS 240", "owner_posted_entry"),
        ("Entry reversing prior year audit adjustment #AJ-001", "CAS 710", "reversal_of_audit_adj"),
        ("Entry reversing audit adjustment #AJ-005 from Q2", "CAS 710", "reversal_of_audit_adj"),
    ]
    round_amounts = [10000, 25000, 50000, 10000, 25000, 50000, 15000, 20000, 8500, 12000]
    for i, (desc, cas_ref, flag) in enumerate(skepticism_scenarios):
        doc_id = f"st3_skepticism_{i:03d}"
        amount = round_amounts[i]
        post_date = _random_date()
        if "after period end" in desc or "after year-end" in desc:
            # Make the entry date after period end
            period_end = date(2025, 12, 31)
            days_after = int(desc.split()[3])
            post_date = period_end + timedelta(days=days_after)

        conn.execute(
            """INSERT OR REPLACE INTO documents
               (document_id, file_name, file_path, client_code, vendor,
                doc_type, amount,
                document_date, gl_account, tax_code, review_status,
                confidence, raw_result, created_at, updated_at,
                stress_test_type, stress_test_flag, demo)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (
                doc_id, f"{doc_id}.pdf", "",
                _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
                "Manual Journal Entry", "journal_entry", float(amount),
                post_date.isoformat(), "Adjustments", "NONE",
                "Pending", 0.60,
                json.dumps({
                    "description": desc,
                    "cas_reference": cas_ref,
                    "skepticism_flag": flag,
                    "amount": amount,
                    "posted_by": "owner" if "owner" in desc.lower() else "system",
                    "supporting_docs": False if "no supporting" in desc.lower() else True,
                    "reversal": "audit_adjustment" if "reversing" in desc.lower() else None,
                }),
                now, now, "part3_cas_logic", flag,
            ),
        )
        results.append({"id": doc_id, "flag": flag})
    print(f"  [4/4] professional_skepticism: 10 transactions")

    conn.commit()
    print(f"  TOTAL Part 3: {len(results)} CAS scenarios inserted")
    return results


# ═══════════════════════════════════════════════════════════════════════════
# PART 4 — Quebec Payroll Audit (100 transactions)
# ═══════════════════════════════════════════════════════════════════════════

def generate_part4_payroll(conn: sqlite3.Connection) -> list[dict]:
    """Generate 100 payroll transactions with Quebec-specific traps."""
    print("\n== PART 4: Quebec Payroll Audit (100 transactions) ==")
    transactions: list[dict] = []
    now = _utcnow()

    def _payroll_insert(txn: dict) -> None:
        conn.execute(
            """INSERT OR REPLACE INTO payroll_transactions
               (transaction_id, client_code, employee_name, employee_province,
                period, gross_pay, total_payroll, pension_plan,
                pension_contribution, ei_rate, ei_premium, qpip_premium,
                hsf_rate, hsf_contribution, cnesst_rate, cnesst_contribution,
                industry_code, rl1_data, t4_data, stress_test_flag, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                txn["transaction_id"], txn["client_code"],
                txn["employee_name"], txn.get("employee_province", "QC"),
                txn.get("period", "2025"), txn["gross_pay"],
                txn.get("total_payroll", 0),
                txn.get("pension_plan", "QPP"), txn.get("pension_contribution", 0),
                txn.get("ei_rate", 0), txn.get("ei_premium", 0),
                txn.get("qpip_premium", 0),
                txn.get("hsf_rate", 0), txn.get("hsf_contribution", 0),
                txn.get("cnesst_rate", 0), txn.get("cnesst_contribution", 0),
                txn.get("industry_code", ""),
                json.dumps(txn.get("rl1_data", {})),
                json.dumps(txn.get("t4_data", {})),
                txn["flag"], now,
            ),
        )
        transactions.append(txn)

    # ── 1. HSF rate errors (20 employees) ──
    hsf_test_payrolls = [
        # (total_payroll, wrong_rate, correct_description)
        (800000, 0.0200, "Using 2.00% for payroll under $1M (should be 1.25%)"),
        (950000, 0.0370, "Using 3.70% for payroll under $1M (should be 1.25%)"),
        (1200000, 0.0125, "Using 1.25% for payroll $1M-$2M (should be 1.65%)"),
        (1500000, 0.0426, "Using 4.26% for payroll $1M-$2M"),
        (1800000, 0.0125, "Using 1.25% for payroll $1M-$2M"),
        (2200000, 0.0125, "Using 1.25% for payroll $2M-$3M (should be 1.65%)"),
        (2500000, 0.0426, "Using 4.26% for payroll $2M-$3M"),
        (2800000, 0.0125, "Using 1.25% for payroll $2M-$3M"),
        (3500000, 0.0125, "Using 1.25% for payroll $3M-$5M (should be 2.00%)"),
        (4000000, 0.0426, "Using 4.26% for payroll $3M-$5M"),
        (4500000, 0.0165, "Using 1.65% for payroll $3M-$5M"),
        (5500000, 0.0125, "Using 1.25% for payroll $5M-$7M (should be 2.50%)"),
        (6000000, 0.0200, "Using 2.00% for payroll $5M-$7M"),
        (6500000, 0.0165, "Using 1.65% for payroll $5M-$7M"),
        (7500000, 0.0250, "Using 2.50% for payroll >$7M (should be 4.26%)"),
        (8000000, 0.0200, "Using 2.00% for payroll >$7M"),
        (9000000, 0.0165, "Using 1.65% for payroll >$7M"),
        (10000000, 0.0125, "Using 1.25% for payroll >$7M"),
        (12000000, 0.0250, "Using 2.50% for payroll >$7M"),
        (15000000, 0.0370, "Using 3.70% for payroll >$7M"),
    ]
    for i, (total_pay, wrong_rate, desc) in enumerate(hsf_test_payrolls):
        gross = _rng.uniform(40000, 85000)
        _payroll_insert({
            "transaction_id": f"st4_hsf_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "employee_name": _EMPLOYEE_NAMES[i % len(_EMPLOYEE_NAMES)],
            "gross_pay": round(gross, 2),
            "total_payroll": total_pay,
            "hsf_rate": wrong_rate,
            "hsf_contribution": round(gross * wrong_rate, 2),
            "flag": "hsf_rate_error",
        })
    print(f"  [1/5] hsf_rate_error: 20 transactions")

    # ── 2. QPP vs CPP confusion (10 transactions) ──
    for i in range(10):
        gross = _rng.uniform(35000, 90000)
        if i < 7:
            # Quebec employee with CPP (wrong)
            _payroll_insert({
                "transaction_id": f"st4_qpp_cpp_{i:03d}",
                "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
                "employee_name": _EMPLOYEE_NAMES[i % len(_EMPLOYEE_NAMES)],
                "employee_province": "QC",
                "gross_pay": round(gross, 2),
                "pension_plan": "CPP",
                "pension_contribution": round(gross * 0.0595, 2),
                "flag": "qpp_cpp_error",
            })
        else:
            # Ontario employee with QPP (wrong)
            _payroll_insert({
                "transaction_id": f"st4_qpp_cpp_{i:03d}",
                "client_code": "STRESS_TECH",
                "employee_name": f"Employee ON-{i}",
                "employee_province": "ON",
                "gross_pay": round(gross, 2),
                "pension_plan": "QPP",
                "pension_contribution": round(gross * 0.064, 2),
                "flag": "qpp_cpp_error",
            })
    print(f"  [2/5] qpp_cpp_error: 10 transactions")

    # ── 3. QPIP vs EI confusion (10 transactions) ──
    for i in range(10):
        gross = _rng.uniform(35000, 80000)
        if i < 7:
            # Quebec employee with full EI rate (wrong — should be reduced)
            _payroll_insert({
                "transaction_id": f"st4_qpip_ei_{i:03d}",
                "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
                "employee_name": _EMPLOYEE_NAMES[i % len(_EMPLOYEE_NAMES)],
                "employee_province": "QC",
                "gross_pay": round(gross, 2),
                "ei_rate": float(EI_RATE_REGULAR),  # 1.66% instead of 1.32%
                "ei_premium": round(gross * float(EI_RATE_REGULAR), 2),
                "qpip_premium": round(gross * 0.00494, 2),
                "flag": "qpip_ei_error",
            })
        else:
            # Non-QC employee with reduced Quebec EI rate (wrong)
            _payroll_insert({
                "transaction_id": f"st4_qpip_ei_{i:03d}",
                "client_code": "STRESS_TECH",
                "employee_name": f"Employee AB-{i}",
                "employee_province": "AB",
                "gross_pay": round(gross, 2),
                "ei_rate": float(EI_RATE_QUEBEC),  # 1.32% instead of 1.66%
                "ei_premium": round(gross * float(EI_RATE_QUEBEC), 2),
                "flag": "qpip_ei_error",
            })
    print(f"  [3/5] qpip_ei_error: 10 transactions")

    # ── 4. RL-1 vs T4 mismatch (20 employees) ──
    for i in range(20):
        gross = round(_rng.uniform(35000, 120000), 2)
        qpp_contrib = round(gross * 0.064, 2)
        ei_premium = round(gross * 0.0132, 2)
        tax_deducted = round(gross * _rng.uniform(0.15, 0.35), 2)
        qpip = round(gross * 0.00494, 2)

        # Create matching RL-1 and T4 with deliberate mismatches
        rl1 = {
            "A": gross,
            "B": gross,
            "C": qpp_contrib,
            "D": tax_deducted,
            "E": min(gross, 63200),  # EI insurable max
            "F": ei_premium,
            "G": round(_rng.uniform(0, 1500), 2),
            "H": qpip,
        }
        t4 = dict(rl1)  # Start with same values

        # Introduce 1-3 mismatches per employee
        mismatch_boxes = _rng.sample(list(rl1.keys()), k=_rng.randint(1, 3))
        t4_mapped = {"A": "14", "B": "26", "C": "16", "D": "22",
                     "E": "24", "F": "18", "G": "44", "H": "55"}
        t4_data: dict[str, float] = {}
        for box in rl1:
            t4_box = t4_mapped[box]
            if box in mismatch_boxes:
                # Introduce mismatch: off by $50-$500
                diff = round(_rng.uniform(50, 500), 2)
                t4_data[t4_box] = round(rl1[box] + (_rng.choice([-1, 1]) * diff), 2)
            else:
                t4_data[t4_box] = rl1[box]

        _payroll_insert({
            "transaction_id": f"st4_rl1_t4_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "employee_name": _EMPLOYEE_NAMES[i % len(_EMPLOYEE_NAMES)],
            "gross_pay": gross,
            "pension_plan": "QPP",
            "pension_contribution": qpp_contrib,
            "ei_rate": 0.0132,
            "ei_premium": ei_premium,
            "qpip_premium": qpip,
            "rl1_data": rl1,
            "t4_data": t4_data,
            "flag": "rl1_t4_mismatch",
        })
    print(f"  [4/5] rl1_t4_mismatch: 20 transactions")

    # ── 5. CNESST premium errors (20 transactions) ──
    industry_codes = list(CNESST_INDUSTRY_RATES.keys())
    for i in range(20):
        code = industry_codes[i % len(industry_codes)]
        correct_rate = float(CNESST_INDUSTRY_RATES[code]["rate"])
        # Pick a wrong rate from a different industry
        wrong_code = _rng.choice([c for c in industry_codes if c != code])
        wrong_rate = float(CNESST_INDUSTRY_RATES[wrong_code]["rate"])
        # Ensure it's actually wrong
        if abs(wrong_rate - correct_rate) < 0.001:
            wrong_rate = correct_rate * 1.5

        gross = round(_rng.uniform(30000, 80000), 2)
        _payroll_insert({
            "transaction_id": f"st4_cnesst_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "employee_name": _EMPLOYEE_NAMES[i % len(_EMPLOYEE_NAMES)],
            "gross_pay": gross,
            "industry_code": code,
            "cnesst_rate": wrong_rate,
            "cnesst_contribution": round(gross * wrong_rate, 2),
            "flag": "cnesst_rate_error",
        })
    print(f"  [5/5] cnesst_rate_error: 20 transactions")

    # ── Fill remaining to reach 100 ──
    remaining = 100 - len(transactions)
    for i in range(remaining):
        gross = round(_rng.uniform(35000, 85000), 2)
        code = _rng.choice(industry_codes)
        rate = float(CNESST_INDUSTRY_RATES[code]["rate"])
        _payroll_insert({
            "transaction_id": f"st4_correct_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "employee_name": _EMPLOYEE_NAMES[i % len(_EMPLOYEE_NAMES)],
            "employee_province": "QC",
            "gross_pay": gross,
            "total_payroll": 500000,
            "pension_plan": "QPP",
            "pension_contribution": round(gross * 0.064, 2),
            "ei_rate": float(EI_RATE_QUEBEC),
            "ei_premium": round(gross * float(EI_RATE_QUEBEC), 2),
            "qpip_premium": round(gross * 0.00494, 2),
            "hsf_rate": 0.0125,
            "hsf_contribution": round(gross * 0.0125, 2),
            "cnesst_rate": rate,
            "cnesst_contribution": round(gross * rate, 2),
            "industry_code": code,
            "flag": "correct",
        })
    print(f"  [+] correct baseline: {remaining} transactions")

    conn.commit()
    print(f"  TOTAL Part 4: {len(transactions)} payroll transactions inserted")
    return transactions


# ═══════════════════════════════════════════════════════════════════════════
# PART 5 — CCA (Capital Cost Allowance) Stress Test (50 assets)
# ═══════════════════════════════════════════════════════════════════════════

# CCA class definitions: (class, description, base_rate, aia_rate, half_year_exempt)
CCA_CLASSES: dict[int, dict[str, Any]] = {
    1:  {"desc": "Buildings (post-1987)", "rate": 0.04, "aia_rate": 0.04, "half_year_exempt": False},
    6:  {"desc": "Buildings (frame/log)", "rate": 0.10, "aia_rate": 0.10, "half_year_exempt": False},
    8:  {"desc": "Furniture & fixtures", "rate": 0.20, "aia_rate": 0.20, "half_year_exempt": False},
    10: {"desc": "Vehicles & equipment", "rate": 0.30, "aia_rate": 0.30, "half_year_exempt": False},
    12: {"desc": "Computer software, tools <$500", "rate": 1.00, "aia_rate": 1.00, "half_year_exempt": True},
    13: {"desc": "Leasehold improvements", "rate": 0.00, "aia_rate": 0.00, "half_year_exempt": False},
    14: {"desc": "Patents, franchises (limited life)", "rate": 0.00, "aia_rate": 0.00, "half_year_exempt": True},
    43: {"desc": "Manufacturing machinery (pre-2024)", "rate": 0.30, "aia_rate": 0.30, "half_year_exempt": False},
    46: {"desc": "Data network equipment", "rate": 0.30, "aia_rate": 0.30, "half_year_exempt": False},
    50: {"desc": "Computer hardware", "rate": 0.55, "aia_rate": 0.55, "half_year_exempt": False},
    53: {"desc": "Manufacturing machinery (post-2015)", "rate": 0.50, "aia_rate": 1.00, "half_year_exempt": False},
    54: {"desc": "Zero-emission vehicles", "rate": 0.30, "aia_rate": 1.00, "half_year_exempt": False},
}


def generate_part5_cca(conn: sqlite3.Connection) -> list[dict]:
    """Generate 50 capital asset purchases with CCA traps."""
    print("\n== PART 5: CCA Stress Test (50 assets) ==")
    assets: list[dict] = []
    now = _utcnow()

    def _asset_insert(asset: dict) -> None:
        conn.execute(
            """INSERT OR REPLACE INTO capital_assets
               (asset_id, client_code, description, cca_class, cost,
                acquisition_date, available_for_use_date,
                cca_rate_used, cca_rate_correct,
                half_year_applied, half_year_correct,
                aia_eligible, aia_applied,
                year_1_cca_claimed, year_1_cca_correct,
                stress_test_flag, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                asset["asset_id"], asset["client_code"],
                asset["description"], asset["cca_class"], asset["cost"],
                asset["acquisition_date"], asset.get("available_for_use_date", asset["acquisition_date"]),
                asset["cca_rate_used"], asset["cca_rate_correct"],
                asset.get("half_year_applied", 1), asset.get("half_year_correct", 1),
                asset.get("aia_eligible", 0), asset.get("aia_applied", 0),
                asset["year_1_cca_claimed"], asset["year_1_cca_correct"],
                asset["flag"], now,
            ),
        )
        assets.append(asset)

    # ── 1. Accelerated Investment Incentive errors (20 assets) ──
    aia_assets = [
        (53, "CNC milling machine", 150000),
        (53, "Industrial laser cutter", 280000),
        (53, "Assembly line robot", 450000),
        (53, "Metal stamping press", 120000),
        (53, "Packaging machine", 95000),
        (54, "Tesla Model 3 company car", 55000),
        (54, "Chevrolet Bolt fleet", 42000),
        (54, "Electric delivery van", 85000),
        (54, "Hyundai Ioniq 5", 48000),
        (54, "Ford F-150 Lightning", 75000),
        (53, "3D printing system", 200000),
        (53, "Automated welding station", 175000),
        (53, "Injection molding machine", 320000),
        (53, "Conveyor system", 110000),
        (53, "Food processing line", 250000),
        (54, "Electric forklift fleet", 60000),
        (54, "BYD electric bus", 350000),
        (53, "Textile loom (automated)", 180000),
        (53, "Paint spray booth", 140000),
        (53, "CNC lathe", 220000),
    ]
    for i, (cca_class, desc, cost) in enumerate(aia_assets):
        cls_info = CCA_CLASSES[cca_class]
        correct_rate = cls_info["aia_rate"]  # Should be 100% for class 53/54
        wrong_rate = cls_info["rate"]  # Using base rate instead

        # For class 53: base = 50%, AIA = 100%. For class 54: base = 30%, AIA = 100%
        correct_y1 = round(cost * correct_rate, 2)
        wrong_y1 = round(cost * wrong_rate * 0.5, 2)  # Wrong: base rate + half-year

        _asset_insert({
            "asset_id": f"st5_aia_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "description": desc,
            "cca_class": cca_class,
            "cost": cost,
            "acquisition_date": _random_date(date(2024, 1, 1), date(2025, 6, 30)).isoformat(),
            "cca_rate_used": wrong_rate,
            "cca_rate_correct": correct_rate,
            "half_year_applied": 1,
            "half_year_correct": 0,  # AIA overrides half-year rule
            "aia_eligible": 1,
            "aia_applied": 0,  # Error: AIA not applied
            "year_1_cca_claimed": wrong_y1,
            "year_1_cca_correct": correct_y1,
            "flag": "aia_rate_error",
        })
    print(f"  [1/4] aia_rate_error: 20 assets")

    # ── 2. Half-year rule errors (10 assets) ──
    half_year_assets = [
        (12, "Accounting software license", 5000, True),   # Class 12 exempt
        (12, "Microsoft Office licenses", 2500, True),     # Class 12 exempt
        (12, "Custom ERP module", 15000, True),            # Class 12 exempt
        (14, "10-year franchise agreement", 50000, True),  # Class 14 exempt
        (14, "Patent (5-year life)", 30000, True),         # Class 14 exempt
        (8, "Office desks and chairs", 12000, False),      # Not exempt — half-year applies
        (10, "Delivery truck", 65000, False),              # Not exempt
        (50, "Server hardware", 25000, False),             # Not exempt
        (46, "Network switches", 8000, False),             # Not exempt
        (1, "Commercial building", 500000, False),         # Not exempt
    ]
    for i, (cca_class, desc, cost, is_exempt) in enumerate(half_year_assets):
        cls_info = CCA_CLASSES[cca_class]
        rate = cls_info["rate"]

        if is_exempt:
            # Error: half-year rule applied to exempt class
            correct_y1 = round(cost * rate, 2)
            wrong_y1 = round(cost * rate * 0.5, 2)  # Wrong: applied half-year
            _asset_insert({
                "asset_id": f"st5_half_{i:03d}",
                "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
                "description": desc,
                "cca_class": cca_class,
                "cost": cost,
                "acquisition_date": _random_date().isoformat(),
                "cca_rate_used": rate,
                "cca_rate_correct": rate,
                "half_year_applied": 1,
                "half_year_correct": 0,
                "year_1_cca_claimed": wrong_y1,
                "year_1_cca_correct": correct_y1,
                "flag": "half_year_rule_error",
            })
        else:
            # Error: half-year rule NOT applied to non-exempt class
            correct_y1 = round(cost * rate * 0.5, 2)
            wrong_y1 = round(cost * rate, 2)  # Wrong: full rate in year 1
            _asset_insert({
                "asset_id": f"st5_half_{i:03d}",
                "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
                "description": desc,
                "cca_class": cca_class,
                "cost": cost,
                "acquisition_date": _random_date().isoformat(),
                "cca_rate_used": rate,
                "cca_rate_correct": rate,
                "half_year_applied": 0,
                "half_year_correct": 1,
                "year_1_cca_claimed": wrong_y1,
                "year_1_cca_correct": correct_y1,
                "flag": "half_year_rule_error",
            })
    print(f"  [2/4] half_year_rule_error: 10 assets")

    # ── 3. Available for use rule (10 assets) ──
    for i in range(10):
        cca_class = _rng.choice([8, 10, 43, 50, 53])
        cls_info = CCA_CLASSES[cca_class]
        cost = round(_rng.uniform(10000, 300000), 2)
        acq_date = _random_date(date(2025, 1, 1), date(2025, 6, 30))
        # Available for use is AFTER the fiscal year end
        afu_date = acq_date + timedelta(days=_rng.randint(180, 400))
        rate = cls_info["rate"]

        _asset_insert({
            "asset_id": f"st5_afu_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "description": f"Equipment (not yet operational) — Class {cca_class}",
            "cca_class": cca_class,
            "cost": cost,
            "acquisition_date": acq_date.isoformat(),
            "available_for_use_date": afu_date.isoformat(),
            "cca_rate_used": rate,
            "cca_rate_correct": 0.0,  # Should be 0 — not yet available for use
            "half_year_applied": 1,
            "half_year_correct": 0,
            "year_1_cca_claimed": round(cost * rate * 0.5, 2),
            "year_1_cca_correct": 0.0,
            "flag": "available_for_use_error",
        })
    print(f"  [3/4] available_for_use_error: 10 assets")

    # ── 4. Class misclassification (10 assets) ──
    misclass_assets = [
        ("Laptop computer", 50, 8, 2500),       # Should be 50 (computer), not 8 (furniture)
        ("iPad Pro", 50, 10, 1500),              # Should be 50, not 10 (vehicle/equip)
        ("Office building", 1, 8, 800000),       # Should be 1 (building), not 8 (furniture)
        ("Delivery van", 10, 8, 45000),          # Should be 10 (vehicle), not 8
        ("Server rack", 50, 46, 35000),          # Should be 50 (computer HW), not 46
        ("Custom software", 12, 50, 20000),      # Should be 12 (software), not 50
        ("Electric car", 54, 10, 55000),         # Should be 54 (ZEV), not 10
        ("CNC machine", 53, 43, 180000),         # Should be 53 (post-2015), not 43
        ("Warehouse", 1, 6, 400000),             # Could be 1 or 6, depends on construction
        ("Network cables", 46, 8, 5000),         # Should be 46 (data network), not 8
    ]
    for i, (desc, correct_class, wrong_class, cost) in enumerate(misclass_assets):
        correct_info = CCA_CLASSES[correct_class]
        wrong_info = CCA_CLASSES[wrong_class]

        correct_rate = correct_info["rate"]
        wrong_rate = wrong_info["rate"]

        # Calculate CCA with wrong class
        half_year = 0.5 if not wrong_info.get("half_year_exempt") else 1.0
        wrong_y1 = round(cost * wrong_rate * half_year, 2)

        half_year_c = 0.5 if not correct_info.get("half_year_exempt") else 1.0
        correct_y1 = round(cost * correct_rate * half_year_c, 2)

        _asset_insert({
            "asset_id": f"st5_misclass_{i:03d}",
            "client_code": _QC_COMPANIES[i % len(_QC_COMPANIES)][0],
            "description": f"{desc} (classified as Class {wrong_class}, should be {correct_class})",
            "cca_class": wrong_class,
            "cost": cost,
            "acquisition_date": _random_date().isoformat(),
            "cca_rate_used": wrong_rate,
            "cca_rate_correct": correct_rate,
            "year_1_cca_claimed": wrong_y1,
            "year_1_cca_correct": correct_y1,
            "flag": "class_misclassification",
        })
    print(f"  [4/4] class_misclassification: 10 assets")

    conn.commit()
    print(f"  TOTAL Part 5: {len(assets)} capital assets inserted")
    return assets


# ═══════════════════════════════════════════════════════════════════════════
# Validation runner
# ═══════════════════════════════════════════════════════════════════════════

def run_validation(conn: sqlite3.Connection) -> dict[str, Any]:
    """Validate all stress test data using engine functions."""
    print("\n== VALIDATION ==")
    results: dict[str, Any] = {"parts": {}, "total_issues": 0, "total_checked": 0}

    # ── Part 1: Tax compliance ──
    rows = conn.execute(
        "SELECT * FROM documents WHERE stress_test_type = 'part1_tax_trap'"
    ).fetchall()
    tax_issues = 0
    tax_checked = 0
    for row in rows:
        raw = json.loads(row["raw_result"]) if row["raw_result"] else {}
        doc = {
            "subtotal": row["subtotal"] or 0,
            "gst_amount": raw.get("gst", 0),
            "qst_amount": raw.get("qst", 0),
            "vendor_province": raw.get("vendor_province", "QC"),
            "vendor_revenue": raw.get("vendor_revenue", 0),
            "gst_registration": raw.get("gst_registration", ""),
            "qst_registration": raw.get("qst_registration", ""),
            "category": raw.get("category", ""),
            "company_revenue": raw.get("company_revenue", 0),
            "expense_type": raw.get("expense_type", ""),
            "itr_claimed": raw.get("itr_claimed", 0),
            "hst_amount": raw.get("hst", 0),
            "quick_method": raw.get("quick_method", False),
            "quick_method_type": raw.get("quick_method_type", "services"),
            "remittance_rate": raw.get("remittance_rate_used", 0),
        }
        issues = validate_quebec_tax_compliance(doc)
        expected_flag = row["stress_test_flag"]
        if expected_flag != "correct":
            tax_checked += 1
            if any(iss["error_type"] == expected_flag for iss in issues):
                tax_issues += 1

    detection_rate = (tax_issues / tax_checked * 100) if tax_checked else 0
    results["parts"]["part1_tax"] = {
        "checked": tax_checked,
        "detected": tax_issues,
        "detection_rate": f"{detection_rate:.1f}%",
    }
    results["total_issues"] += tax_issues
    results["total_checked"] += tax_checked
    print(f"  Part 1 Tax: {tax_issues}/{tax_checked} detected ({detection_rate:.1f}%)")

    # ── Part 4: Payroll validation ──
    payroll_rows = conn.execute(
        "SELECT * FROM payroll_transactions WHERE stress_test_flag != 'correct'"
    ).fetchall()
    payroll_issues = 0
    payroll_checked = len(payroll_rows)
    for row in payroll_rows:
        flag = row["stress_test_flag"]
        detected = False
        if flag == "hsf_rate_error":
            r = validate_hsf_rate(row["total_payroll"] or 0, row["hsf_rate"] or 0)
            detected = not r["valid"]
        elif flag == "qpp_cpp_error":
            r = validate_qpp_cpp(row["employee_province"] or "QC", row["pension_plan"] or "QPP")
            detected = not r["valid"]
        elif flag == "qpip_ei_error":
            r = validate_qpip_ei(row["employee_province"] or "QC", row["ei_rate"] or 0)
            detected = not r["valid"]
        elif flag == "rl1_t4_mismatch":
            rl1 = json.loads(row["rl1_data"]) if row["rl1_data"] else {}
            t4 = json.loads(row["t4_data"]) if row["t4_data"] else {}
            r = reconcile_rl1_t4(rl1, t4)
            detected = not r["valid"]
        elif flag == "cnesst_rate_error":
            r = validate_cnesst_rate(row["industry_code"] or "", row["cnesst_rate"] or 0)
            detected = not r["valid"]
        if detected:
            payroll_issues += 1

    payroll_rate = (payroll_issues / payroll_checked * 100) if payroll_checked else 0
    results["parts"]["part4_payroll"] = {
        "checked": payroll_checked,
        "detected": payroll_issues,
        "detection_rate": f"{payroll_rate:.1f}%",
    }
    results["total_issues"] += payroll_issues
    results["total_checked"] += payroll_checked
    print(f"  Part 4 Payroll: {payroll_issues}/{payroll_checked} detected ({payroll_rate:.1f}%)")

    # ── Part 5: CCA validation ──
    cca_rows = conn.execute(
        "SELECT * FROM capital_assets WHERE stress_test_flag != 'correct'"
    ).fetchall()
    cca_issues = 0
    cca_checked = len(cca_rows)
    for row in cca_rows:
        claimed = row["year_1_cca_claimed"] or 0
        correct = row["year_1_cca_correct"] or 0
        if abs(claimed - correct) > 0.01:
            cca_issues += 1

    cca_rate = (cca_issues / cca_checked * 100) if cca_checked else 0
    results["parts"]["part5_cca"] = {
        "checked": cca_checked,
        "detected": cca_issues,
        "detection_rate": f"{cca_rate:.1f}%",
    }
    results["total_issues"] += cca_issues
    results["total_checked"] += cca_checked
    print(f"  Part 5 CCA: {cca_issues}/{cca_checked} detected ({cca_rate:.1f}%)")

    # ── Summary ──
    total_rate = (results["total_issues"] / results["total_checked"] * 100) if results["total_checked"] else 0
    results["overall_detection_rate"] = f"{total_rate:.1f}%"
    print(f"\n  OVERALL: {results['total_issues']}/{results['total_checked']} issues detected ({total_rate:.1f}%)")

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Canada/Quebec CPA stress test data"
    )
    parser.add_argument(
        "--only", choices=["part1", "part2", "part3", "part4", "part5"],
        help="Generate only a specific part",
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Run validation after generation",
    )
    parser.add_argument(
        "--db", type=str, default=str(DB_PATH),
        help="Path to SQLite database",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  OtoCPA — Canada/Quebec CPA Stress Test Generator")
    print("=" * 70)
    print(f"  Database: {db_path}")
    print(f"  Seed: {SEED}")

    with _open_db(db_path) as conn:
        _ensure_tables(conn)

        parts = args.only
        summary: dict[str, int] = {}

        if not parts or parts == "part1":
            r = generate_part1_tax_traps(conn)
            summary["part1_tax_traps"] = len(r)

        if not parts or parts == "part2":
            r = generate_part2_aspe_ifrs(conn)
            summary["part2_aspe_ifrs"] = len(r)

        if not parts or parts == "part3":
            r = generate_part3_cas_logic(conn)
            summary["part3_cas_logic"] = len(r)

        if not parts or parts == "part4":
            r = generate_part4_payroll(conn)
            summary["part4_payroll"] = len(r)

        if not parts or parts == "part5":
            r = generate_part5_cca(conn)
            summary["part5_cca"] = len(r)

        print("\n" + "=" * 70)
        print("  GENERATION SUMMARY")
        print("=" * 70)
        total = 0
        for part, count in summary.items():
            print(f"  {part}: {count} records")
            total += count
        print(f"  TOTAL: {total} records generated")

        if args.validate:
            validation = run_validation(conn)
            # Save validation report
            report_path = ROOT_DIR / "data" / "stress_test_validation.json"
            report_path.write_text(
                json.dumps(validation, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"\n  Validation report saved to: {report_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
