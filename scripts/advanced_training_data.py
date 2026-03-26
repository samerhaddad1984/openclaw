#!/usr/bin/env python3
"""
scripts/advanced_training_data.py
=================================
5 advanced training-data generators for LedgerLink.

Generators
----------
1. generate_forensic_traps     — 500 transactions, 25 injected anomalies
2. generate_complex_invoices   — 20 multi-format Quebec invoices (text files)
3. generate_messy_emails       — 10 messy Quebec client emails + parsed JSON
4. generate_edge_cases         — 300 edge-case transactions inserted into DB
5. generate_utility_bills      — 5,000 realistic Quebec utility bills (JSON)

Usage
-----
    python scripts/advanced_training_data.py          # generate all
    python scripts/advanced_training_data.py --only forensic_traps
    python scripts/advanced_training_data.py --validate
"""
from __future__ import annotations

import csv
import json
import math
import os
import random
import sqlite3
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.engines.fraud_engine import run_fraud_detection, _quebec_holidays
from src.agents.core.hallucination_guard import verify_ai_output, verify_numeric_totals

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
TRAINING_DIR = ROOT_DIR / "data" / "training"

SEED = 42
_rng = random.Random(SEED)

_CENT = Decimal("0.01")
_GST = Decimal("0.05")
_QST = Decimal("0.09975")
_T_DIV = Decimal("1.14975")


def _d(v: Any) -> Decimal:
    return Decimal(str(v))


def _r2(v: Decimal) -> Decimal:
    return v.quantize(_CENT, rounding=ROUND_HALF_UP)


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _breakdown(total: Decimal, tax_code: str = "T") -> dict[str, float]:
    total = _r2(_d(total))
    if tax_code in ("T", "M"):
        subtotal = _r2(total / _T_DIV)
        gst = _r2(subtotal * _GST)
        qst = _r2(subtotal * _QST)
        tax_total = gst + qst
    else:
        subtotal = total
        gst = Decimal("0.00")
        qst = Decimal("0.00")
        tax_total = Decimal("0.00")
    return {
        "subtotal": float(subtotal),
        "gst": float(gst),
        "qst": float(qst),
        "tax_total": float(tax_total),
    }


def _random_date(start: date = date(2025, 1, 1), end: date = date(2025, 12, 31)) -> date:
    delta = (end - start).days
    return start + timedelta(days=_rng.randint(0, max(delta, 1)))


def _uid() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_documents_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
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
            physical_id TEXT,
            logical_fingerprint TEXT,
            cleanup_note TEXT,
            assigned_to TEXT,
            manual_hold_reason TEXT,
            manual_hold_by TEXT,
            manual_hold_at TEXT,
            submitted_by TEXT,
            client_note TEXT,
            currency TEXT DEFAULT 'CAD',
            subtotal REAL,
            tax_total REAL,
            extraction_method TEXT,
            ingest_source TEXT,
            fraud_flags TEXT DEFAULT '[]',
            raw_ocr_text TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            correction_count INTEGER DEFAULT 0,
            demo INTEGER DEFAULT 0,
            handwriting_low_confidence INTEGER DEFAULT 0,
            handwriting_sample INTEGER DEFAULT 0
        )
    """)
    # Add edge_case_type column if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if "edge_case_type" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN edge_case_type TEXT")
    if "fraud_flags" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN fraud_flags TEXT DEFAULT '[]'")
    conn.commit()


_INSERT_SQL = """
INSERT OR IGNORE INTO documents (
    document_id, file_name, file_path, client_code,
    vendor, doc_type, amount, document_date,
    gl_account, tax_code, category, review_status,
    confidence, raw_result, created_at, updated_at,
    currency, subtotal, tax_total, extraction_method, ingest_source,
    fraud_flags, edge_case_type,
    handwriting_low_confidence, handwriting_sample
) VALUES (
    :document_id, :file_name, :file_path, :client_code,
    :vendor, :doc_type, :amount, :document_date,
    :gl_account, :tax_code, :category, :review_status,
    :confidence, :raw_result, :created_at, :updated_at,
    :currency, :subtotal, :tax_total, :extraction_method, :ingest_source,
    :fraud_flags, :edge_case_type,
    :handwriting_low_confidence, :handwriting_sample
)
"""


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATOR 1 — Forensic accounting traps
# ═══════════════════════════════════════════════════════════════════════════════

_FORTIER_VENDORS = [
    ("Matériaux Roy", "Matériaux et fournitures", "T", 300, 8000),
    ("Béton Provincial", "Matériaux et fournitures", "T", 500, 12000),
    ("Location Équipement Pro", "Location d'équipement", "T", 400, 5000),
    ("Quincaillerie Laval", "Matériaux et fournitures", "T", 100, 3000),
    ("Ultramar Carburant", "Carburant et huile", "T", 80, 600),
    ("Petro-Canada Fleet", "Carburant et huile", "T", 80, 600),
    ("Hydro-Québec", "Électricité et gaz", "E", 150, 800),
    ("Énergir Distribution", "Électricité et gaz", "E", 100, 400),
    ("Bell Communications", "Télécommunications", "T", 100, 350),
    ("Desjardins Frais Bancaires", "Frais bancaires", "E", 20, 150),
    ("Rona Pro Construction", "Matériaux et fournitures", "T", 200, 6000),
    ("Home Depot Pro", "Matériaux et fournitures", "T", 250, 7000),
    ("BMR Construction", "Matériaux et fournitures", "T", 200, 5000),
    ("Sunbelt Location", "Location d'équipement", "T", 300, 4000),
    ("Intact Assurance", "Assurances", "I", 600, 3000),
    ("SAAQ Immatriculation", "Permis et immatriculations", "T", 200, 1500),
    ("Bureau en Gros", "Fournitures de bureau", "T", 30, 300),
    ("Vidéotron Affaires", "Télécommunications", "T", 80, 300),
    ("Mark's Work Wearhouse", "Équipements de protection", "T", 80, 500),
    ("Canadian Tire Pro", "Outillage", "T", 50, 800),
]

_PAYMENT_METHODS = ["cheque", "virement", "carte_credit", "debit", "comptant"]


def generate_forensic_traps() -> Path:
    """Generate 500 transactions for Construction Fortier Inc with 25 anomalies."""
    out_dir = TRAINING_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "forensic_traps.csv"

    rows: list[dict[str, Any]] = []
    anomaly_index = 0

    def _add(vendor, category, amount, dt, memo, gst, qst, method, anomaly_type=""):
        nonlocal anomaly_index
        rows.append({
            "Date": dt.isoformat(),
            "Vendor": vendor,
            "Category": category,
            "Amount": round(amount, 2),
            "Memo": memo,
            "GST": round(gst, 2),
            "QST": round(qst, 2),
            "PaymentMethod": method,
            "anomaly_type": anomaly_type,
        })

    # --- Normal transactions (475) ---
    for i in range(475):
        v, cat, tc, lo, hi = _rng.choice(_FORTIER_VENDORS)
        amt = round(_rng.uniform(lo, hi), 2)
        dt = _random_date()
        bd = _breakdown(_d(str(amt)), tc)
        memo = f"Facture #{_rng.randint(10000, 99999)}"
        method = _rng.choice(_PAYMENT_METHODS)
        _add(v, cat, amt, dt, memo, bd["gst"], bd["qst"], method, "")

    # --- ANOMALY 1: 3 duplicate payments (near-amount, close dates) ---
    for _ in range(3):
        v, cat, tc, lo, hi = _rng.choice(_FORTIER_VENDORS[:6])
        base_amt = round(_rng.uniform(max(lo, 500), hi), 2)
        base_date = _random_date(date(2025, 3, 1), date(2025, 9, 30))
        bd = _breakdown(_d(str(base_amt)), tc)
        _add(v, cat, base_amt, base_date,
             f"Facture #{_rng.randint(10000, 99999)}", bd["gst"], bd["qst"],
             "cheque", "duplicate_payment")
        dup_amt = round(base_amt + _rng.uniform(-5, 5), 2)
        dup_date = base_date + timedelta(days=_rng.randint(1, 3))
        bd2 = _breakdown(_d(str(dup_amt)), tc)
        _add(v, cat, dup_amt, dup_date,
             f"Facture #{_rng.randint(10000, 99999)}", bd2["gst"], bd2["qst"],
             "cheque", "duplicate_payment")

    # --- ANOMALY 2: 2 personal expenses disguised ---
    _add("Spa Nordique Montréal", "Fournitures de bureau", 245.00,
         _random_date(), "Fournitures de bureau", 10.65, 21.21, "carte_credit",
         "personal_expense_disguised")
    _add("Restaurant L'Entrecôte", "Repas d'affaires client", 187.50,
         date(2025, 6, 15), "Repas d'affaires client — anniversaire",
         8.15, 16.24, "carte_credit", "personal_expense_disguised")

    # --- ANOMALY 3: 3 Benford's Law violations ---
    for leading_digit in [7, 8, 9]:
        base = leading_digit * 1000 + _rng.randint(0, 999)
        amt = round(base + _rng.random(), 2)
        dt = _random_date()
        bd = _breakdown(_d(str(amt)), "T")
        _add("Matériaux Roy", "Matériaux et fournitures", amt, dt,
             f"Commande spéciale #{_rng.randint(100, 999)}", bd["gst"], bd["qst"],
             "cheque", "benford_violation")

    # --- ANOMALY 4: 2 round-trip transactions ---
    for _ in range(2):
        v = _rng.choice(["Béton Provincial", "Rona Pro Construction"])
        amt = round(_rng.uniform(2000, 8000), 2)
        dt = _random_date(date(2025, 2, 1), date(2025, 10, 1))
        bd = _breakdown(_d(str(amt)), "T")
        _add(v, "Matériaux et fournitures", amt, dt,
             f"Paiement #{_rng.randint(1000, 9999)}", bd["gst"], bd["qst"],
             "virement", "round_trip")
        refund_date = dt + timedelta(days=_rng.randint(2, 7))
        _add(v, "Matériaux et fournitures", -amt, refund_date,
             f"Remboursement #{_rng.randint(1000, 9999)}", -bd["gst"], -bd["qst"],
             "virement", "round_trip")

    # --- ANOMALY 5: 2 vendor name variations (same vendor, different spellings) ---
    variants = [
        ("Matériaux Roy", "Materiaux Roy Inc", "Roy Matériaux"),
    ]
    for orig, v2, v3 in variants:
        for vname in [v2, v3]:
            amt = round(_rng.uniform(500, 3000), 2)
            dt = _random_date()
            bd = _breakdown(_d(str(amt)), "T")
            _add(vname, "Matériaux et fournitures", amt, dt,
                 f"Facture #{_rng.randint(10000, 99999)}", bd["gst"], bd["qst"],
                 "cheque", "vendor_name_variation")

    # --- ANOMALY 6: 3 end-of-period anomalies ---
    eop_dates = [date(2025, 12, 30), date(2025, 12, 31), date(2025, 3, 31)]
    for dt in eop_dates:
        amt = round(_rng.uniform(8000, 25000), 2)
        bd = _breakdown(_d(str(amt)), "T")
        _add("Home Depot Pro", "Matériaux et fournitures", amt, dt,
             "Gros achat fin d'exercice", bd["gst"], bd["qst"],
             "cheque", "end_of_period")

    # --- ANOMALY 7: 2 split payments (avoid $8000 threshold) ---
    for _ in range(2):
        base_amt = 8000.0
        for j in range(4):
            split_amt = base_amt / 4
            dt = _random_date(date(2025, 4, 1), date(2025, 4, 15))
            bd = _breakdown(_d(str(split_amt)), "T")
            _add("Quincaillerie Laval", "Matériaux et fournitures", split_amt,
                 dt + timedelta(days=j),
                 f"Paiement partiel {j+1}/4", bd["gst"], bd["qst"],
                 "cheque", "split_payment")

    # --- ANOMALY 8: 2 ghost vendors ---
    _add("Services GBX Enr", "Consultation", 5000.00,
         _random_date(), "Services professionnels", 0.0, 0.0,
         "comptant", "ghost_vendor")
    _add("Groupe ZZK Inc", "Consultation", 3000.00,
         _random_date(), "Mandat spécial", 0.0, 0.0,
         "comptant", "ghost_vendor")

    # --- ANOMALY 9: 3 unusual timing ---
    _add("Rona Pro Construction", "Matériaux et fournitures", 2500.00,
         date(2025, 12, 25), "Achat jour de Noël", 108.72, 216.58,
         "carte_credit", "unusual_timing")
    _add("Béton Provincial", "Matériaux et fournitures", 4200.00,
         _random_date(), "Facture 23h — paiement tardif", 182.64, 363.88,
         "virement", "unusual_timing")
    # Accountant vacation week (July)
    _add("Location Équipement Pro", "Location d'équipement", 3800.00,
         date(2025, 7, 14), "Paiement pendant vacances comptable", 165.24, 329.19,
         "virement", "unusual_timing")

    # --- ANOMALY 10: 3 amount creep (5% monthly increase, 6 months) ---
    for _ in range(3):
        v = _rng.choice(["BMR Construction", "Rona Pro Construction"])
        base = round(_rng.uniform(1000, 3000), 2)
        for month in range(6):
            amt = round(base * (1.05 ** month), 2)
            dt = date(2025, month + 1, 15)
            bd = _breakdown(_d(str(amt)), "T")
            _add(v, "Matériaux et fournitures", amt, dt,
                 f"Facture mensuelle #{month+1}", bd["gst"], bd["qst"],
                 "cheque",
                 "amount_creep" if month > 0 else "")

    # Shuffle to mix anomalies into normal
    _rng.shuffle(rows)

    # Write CSV
    fieldnames = ["Date", "Vendor", "Category", "Amount", "Memo",
                  "GST", "QST", "PaymentMethod", "anomaly_type"]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    anomaly_count = sum(1 for r in rows if r["anomaly_type"])
    print(f"[forensic_traps] {len(rows)} rows, {anomaly_count} anomalies -> {out_path}")
    return out_path


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATOR 2 — Complex multi-format invoices
# ═══════════════════════════════════════════════════════════════════════════════

_INVOICE_TEMPLATES: list[dict[str, Any]] = [
    # --- Medical (4) ---
    {
        "industry": "medical", "vendor": "Cabinet médical Dr. Tremblay",
        "bill_to": "Construction Fortier Inc\n1450 boul. Charest O.\nQuébec, QC G1N 2E5",
        "ship_to": "Chantier Fortier — Site Lévis\n88 rue des Pionniers\nLévis, QC G6V 4T3",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Examen médical pré-emploi (RAMQ code 09700)", "qty": 5, "unit": 85.00, "taxable": False},
            {"desc": "Tests audiométriques (code 08361)", "qty": 5, "unit": 45.00, "taxable": False},
            {"desc": "Vaccin Tétanos-Diphtérie", "qty": 5, "unit": 35.00, "taxable": True},
            {"desc": "Trousse premiers soins industrielle", "qty": 2, "unit": 125.00, "taxable": True},
        ],
        "note": "[Payé comptant — escompte de 50$ appliqué]",
        "prorated": "Examen prorated: 3/5 employés terminés, solde à facturer",
    },
    {
        "industry": "medical", "vendor": "Clinique Santé-Travail Laval",
        "bill_to": "Gestion Immobilière Tremblay\n200 boul. René-Lévesque E.\nMontréal, QC H2X 1N6",
        "ship_to": "Immeuble Place Royale\n15 rue du Fort\nMontréal, QC H2Y 1A1",
        "date_format": "March 19 2026",
        "lines": [
            {"desc": "Consultation médecin (honoraires)", "qty": 1, "unit": 250.00, "taxable": True},
            {"desc": "Analyse sanguine — bilan complet", "qty": 3, "unit": 95.00, "taxable": False},
            {"desc": "Certificat médical d'aptitude", "qty": 3, "unit": 50.00, "taxable": False},
            {"desc": "Fournitures médicales jetables", "qty": 1, "unit": 78.50, "taxable": True},
        ],
        "note": "[Reçu RAMQ non applicable — employeur privé]",
        "prorated": None,
    },
    {
        "industry": "medical", "vendor": "Dr. Sophie Lavoie, omnipraticienne",
        "bill_to": "Transport Lapointe Inc\n5600 boul. Métropolitain E.\nMontréal, QC H1R 1Z4",
        "ship_to": "Même adresse",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Examen de la vue professionnel (code RAMQ 09860)", "qty": 8, "unit": 65.00, "taxable": False},
            {"desc": "Lentilles correctrices industrielles", "qty": 8, "unit": 185.00, "taxable": True},
            {"desc": "Rapport médical chauffeur classe 1", "qty": 8, "unit": 40.00, "taxable": False},
        ],
        "note": "[Solde partiel — 4 employés à examiner au prochain rendez-vous]",
        "prorated": "Crédit de 120$ appliqué sur facture précédente #MED-2024-089",
    },
    {
        "industry": "medical", "vendor": "Pharmacie Jean Coutu — Succursale 412",
        "bill_to": "Services de plomberie Gagnon Inc\n320 rue St-Joseph\nQuébec, QC G1K 3B2",
        "ship_to": "Entrepôt Gagnon\n14 rue Industrielle\nSt-Augustin, QC G3A 1W2",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Prescriptions employés — lot mars 2026", "qty": 1, "unit": 456.80, "taxable": False},
            {"desc": "Bandages et pansements industriels", "qty": 10, "unit": 12.99, "taxable": True},
            {"desc": "Gel antiseptique 4L (x6)", "qty": 6, "unit": 24.99, "taxable": True},
            {"desc": "Gants nitrile boîte 100", "qty": 5, "unit": 18.50, "taxable": True},
        ],
        "note": "[Facture groupée — certains items exempts de taxes]",
        "prorated": None,
    },
    # --- SaaS (4) ---
    {
        "industry": "saas", "vendor": "Logiciels CloudCompta Inc",
        "bill_to": "Cabinet juridique Beauchamp\n800 place d'Youville\nMontréal, QC H2Y 2B6",
        "ship_to": "Même adresse (licence numérique)",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Licence annuelle CloudCompta Pro (12 mois)", "qty": 1, "unit": 4800.00, "taxable": True},
            {"desc": "Module paie québécois — addon", "qty": 1, "unit": 1200.00, "taxable": True},
            {"desc": "Crédit prorata mois de mars (9 jours inutilisés)", "qty": 1, "unit": -120.00, "taxable": True},
            {"desc": "Support prioritaire 24/7", "qty": 1, "unit": 600.00, "taxable": True},
        ],
        "note": "[Montant USD 4,710.00 converti à 1.3742 CAD/USD = 6,471.58 CAD]",
        "prorated": "Prorated credit: -$120.00 for 9 unused days in billing cycle",
    },
    {
        "industry": "saas", "vendor": "Atlassian Pty Ltd (Canada)",
        "bill_to": "Services TI Laval Inc\n3500 boul. de la Concorde\nLaval, QC H7E 2B4",
        "ship_to": "N/A — cloud service",
        "date_format": "March 19 2026",
        "lines": [
            {"desc": "Jira Software Cloud — 25 users, monthly", "qty": 1, "unit": 187.50, "taxable": True},
            {"desc": "Confluence Cloud — 25 users, monthly", "qty": 1, "unit": 137.50, "taxable": True},
            {"desc": "Additional storage 500GB", "qty": 1, "unit": 50.00, "taxable": True},
            {"desc": "Credit: Migration discount (one-time)", "qty": 1, "unit": -75.00, "taxable": True},
        ],
        "note": "[Billed in USD: $218.25 × 1.3742 = $299.73 CAD]",
        "prorated": None,
    },
    {
        "industry": "saas", "vendor": "Microsoft Ireland Operations Ltd",
        "bill_to": "Agence de communication Pixel\n125 rue St-Paul O.\nMontréal, QC H2Y 1Z5",
        "ship_to": "N/A",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Microsoft 365 Business Premium — 15 licences", "qty": 15, "unit": 28.10, "taxable": True},
            {"desc": "Azure DevOps Basic — 10 users", "qty": 10, "unit": 8.20, "taxable": True},
            {"desc": "Power BI Pro — 5 licences", "qty": 5, "unit": 13.70, "taxable": True},
            {"desc": "Prorated adjustment (mid-cycle add 3 users)", "qty": 3, "unit": 14.05, "taxable": True},
        ],
        "note": "[Converti EUR → CAD au taux de 1.4821]",
        "prorated": "Prorated: 3 users added mid-cycle, 15/30 days = 50% of monthly rate",
    },
    {
        "industry": "saas", "vendor": "Shopify Inc — Plan Plus",
        "bill_to": "Boutique Mode Québec Inc\n750 rue Ste-Catherine E.\nMontréal, QC H2L 2C3",
        "ship_to": "Entrepôt Mode QC\n2200 boul. Industriel\nSt-Hubert, QC J3Y 8Y9",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Shopify Plus — monthly subscription", "qty": 1, "unit": 2500.00, "taxable": True},
            {"desc": "Transaction fees (2.4% on $48,200)", "qty": 1, "unit": 1156.80, "taxable": True},
            {"desc": "Credit: Volume discount (>$40k GMV)", "qty": 1, "unit": -250.00, "taxable": True},
            {"desc": "POS Pro addon — 2 locations", "qty": 2, "unit": 89.00, "taxable": True},
        ],
        "note": "[Credit card processing fees are taxable in Quebec]",
        "prorated": None,
    },
    # --- Construction (4) ---
    {
        "industry": "construction", "vendor": "Matériaux de Construction Laval Inc",
        "bill_to": "Construction Fortier Inc\n1450 boul. Charest O.\nQuébec, QC G1N 2E5",
        "ship_to": "Chantier résidentiel Ste-Foy\n400 ch. Ste-Foy\nQuébec, QC G1S 2J5",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Bois d'oeuvre 2x4x8 SPF #2 (pieds)", "qty": 500, "unit": 4.89, "taxable": True},
            {"desc": "Contreplaqué 4x8 3/4\" sapin", "qty": 80, "unit": 52.99, "taxable": True},
            {"desc": "Vis à construction 3\" (boîte 5000)", "qty": 4, "unit": 89.99, "taxable": True},
            {"desc": "Livraison grue — chantier Ste-Foy", "qty": 1, "unit": 350.00, "taxable": True},
            {"desc": "Surcharge carburant 4.5%", "qty": 1, "unit": 357.76, "taxable": True},
        ],
        "note": "[Livraison en 2 temps — 60% reçu, 40% en commande]",
        "prorated": None,
    },
    {
        "industry": "construction", "vendor": "Béton Provincial — Division Québec",
        "bill_to": "Construction Fortier Inc\n1450 boul. Charest O.\nQuébec, QC G1N 2E5",
        "ship_to": "Chantier commercial Charlesbourg\n8000 1ère Avenue\nQuébec, QC G1H 2T5",
        "date_format": "March 19 2026",
        "lines": [
            {"desc": "Béton prêt-à-l'emploi 30 MPa (m³)", "qty": 45, "unit": 185.00, "taxable": True},
            {"desc": "Pompage — bras de 42m (heures)", "qty": 6, "unit": 275.00, "taxable": True},
            {"desc": "Adjuvant accélérateur (litres)", "qty": 200, "unit": 3.50, "taxable": True},
            {"desc": "Frais de lavage bétonnière", "qty": 3, "unit": 85.00, "taxable": True},
        ],
        "note": "[Temps d'attente chantier facturé après 15 min: 2 × 95$/h]",
        "prorated": "Attente: 2h × $95.00 = $190.00 ajouté au total",
    },
    {
        "industry": "construction", "vendor": "Toitures Supérieures Québec Inc",
        "bill_to": "Gestion Immobilière Tremblay\n200 boul. René-Lévesque E.\nMontréal, QC H2X 1N6",
        "ship_to": "Immeuble 455 St-Antoine\nMontréal, QC H2Z 1J1",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Membrane élastomère 2 couches (pi²)", "qty": 5000, "unit": 2.85, "taxable": True},
            {"desc": "Isolation polyiso 3\" (pi²)", "qty": 5000, "unit": 1.95, "taxable": True},
            {"desc": "Main d'oeuvre — couvreurs (heures)", "qty": 320, "unit": 55.00, "taxable": True},
            {"desc": "Solins aluminium sur mesure (pied lin.)", "qty": 200, "unit": 12.50, "taxable": True},
        ],
        "note": "[Dépôt de 30% déjà reçu — facture #TOI-2025-088. Solde 70% dû net 30.]",
        "prorated": "Deposit already paid: $12,915.00 — balance: $30,135.00",
    },
    {
        "industry": "construction", "vendor": "Plomberie Industrielle Montréal",
        "bill_to": "Construction Fortier Inc\n1450 boul. Charest O.\nQuébec, QC G1N 2E5",
        "ship_to": "Projet condo Phase 3\n1200 rue Berri\nMontréal, QC H2L 4E7",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Tuyaux cuivre 3/4\" type L (pieds)", "qty": 600, "unit": 8.75, "taxable": True},
            {"desc": "Raccords PEX assortis (lot)", "qty": 12, "unit": 145.00, "taxable": True},
            {"desc": "Chauffe-eau commercial 80 gal", "qty": 2, "unit": 2450.00, "taxable": True},
            {"desc": "Installation et raccordement (heures)", "qty": 48, "unit": 75.00, "taxable": True},
            {"desc": "Permis plomberie — Ville de Montréal", "qty": 1, "unit": 350.00, "taxable": False},
        ],
        "note": "[Permis municipal exempt de taxes — voir reçu joint]",
        "prorated": None,
    },
    # --- Restaurant/Catering (4) ---
    {
        "industry": "restaurant", "vendor": "Traiteur Saveurs du Québec",
        "bill_to": "Agence de communication Pixel\n125 rue St-Paul O.\nMontréal, QC H2Y 1Z5",
        "ship_to": "Salle de conférence Hôtel Le Germain\n2050 rue Mansfield\nMontréal, QC H3A 1Y9",
        "date_format": "March 19 2026",
        "lines": [
            {"desc": "Repas complet — poulet de grain, légumes de saison (personnes)", "qty": 45, "unit": 42.00, "taxable": True},
            {"desc": "Service de bar ouvert 3h (personnes)", "qty": 45, "unit": 28.00, "taxable": True},
            {"desc": "Alcool — vin & bière (bouteilles/fûts)", "qty": 1, "unit": 1350.00, "taxable": True},
            {"desc": "Gâteau corporatif personnalisé", "qty": 1, "unit": 185.00, "taxable": True},
            {"desc": "Pourboire inclus (18%)", "qty": 1, "unit": 756.00, "taxable": False},
        ],
        "note": "[Alcool comptabilisé séparément — pourboire non taxable]",
        "prorated": None,
    },
    {
        "industry": "restaurant", "vendor": "Restaurant Le Continental — Québec",
        "bill_to": "Cabinet juridique Beauchamp\n800 place d'Youville\nMontréal, QC H2Y 2B6",
        "ship_to": "Même adresse (dîner sur place)",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Table d'hôte Déjeuner d'affaires (pers.)", "qty": 8, "unit": 65.00, "taxable": True},
            {"desc": "Bouteille vin — Château des Charmes", "qty": 3, "unit": 68.00, "taxable": True},
            {"desc": "Café et desserts", "qty": 8, "unit": 12.00, "taxable": True},
            {"desc": "Pourboire (15%, exclu des taxes)", "qty": 1, "unit": 126.60, "taxable": False},
        ],
        "note": "[Reçu carte Visa Affaires — 8 convives, dossier Tremblay c. Gagnon]",
        "prorated": None,
    },
    {
        "industry": "restaurant", "vendor": "Brasserie Artisanale Le Trou du Diable",
        "bill_to": "Services TI Laval Inc\n3500 boul. de la Concorde\nLaval, QC H7E 2B4",
        "ship_to": "5-à-7 d'équipe — sur place",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Plateau charcuteries & fromages (pers.)", "qty": 20, "unit": 18.50, "taxable": True},
            {"desc": "Bières de microbrasserie — pichet", "qty": 12, "unit": 24.00, "taxable": True},
            {"desc": "Cocktails signature", "qty": 8, "unit": 16.00, "taxable": True},
            {"desc": "Nachos et ailes de poulet (assiettes)", "qty": 6, "unit": 22.00, "taxable": True},
            {"desc": "Pourboire 20%", "qty": 1, "unit": 200.60, "taxable": False},
        ],
        "note": "[Reçu séparé alcool vs nourriture sur demande]",
        "prorated": None,
    },
    {
        "industry": "restaurant", "vendor": "Service de Lunch Corporatif Le Plateau",
        "bill_to": "Conseil en gestion RH Beaulieu\n1000 rue Sherbrooke O.\nMontréal, QC H3A 3G4",
        "ship_to": "Bureau 400\n1000 rue Sherbrooke O.\nMontréal, QC H3A 3G4",
        "date_format": "March 19 2026",
        "lines": [
            {"desc": "Boîte à lunch premium (personnes)", "qty": 30, "unit": 22.50, "taxable": True},
            {"desc": "Salade César en bol collectif", "qty": 3, "unit": 45.00, "taxable": True},
            {"desc": "Jus et boissons gazeuses", "qty": 30, "unit": 3.50, "taxable": True},
            {"desc": "Livraison et installation buffet", "qty": 1, "unit": 75.00, "taxable": True},
        ],
        "note": "[Pas de pourboire — forfait tout inclus sauf taxes]",
        "prorated": None,
    },
    # --- Legal (4) ---
    {
        "industry": "legal", "vendor": "Cabinet Beauchamp, Côté & Associés, avocats",
        "bill_to": "Construction Fortier Inc\n1450 boul. Charest O.\nQuébec, QC G1N 2E5",
        "ship_to": "N/A",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Honoraires — Me Beauchamp (heures)", "qty": 12.5, "unit": 350.00, "taxable": True},
            {"desc": "Honoraires — Me Côté (heures)", "qty": 8.0, "unit": 275.00, "taxable": True},
            {"desc": "Recherche juridique stagiaire (heures)", "qty": 15.0, "unit": 125.00, "taxable": True},
            {"desc": "Débours: frais de cour", "qty": 1, "unit": 450.00, "taxable": False},
            {"desc": "Débours: huissier signification", "qty": 1, "unit": 185.00, "taxable": False},
            {"desc": "Photocopies et reliure (pages)", "qty": 850, "unit": 0.25, "taxable": True},
        ],
        "note": "[GST sur honoraires seulement — débours exempts selon Barreau]",
        "prorated": "Honoraires: GST/QST applicable. Disbursements: exempt.",
    },
    {
        "industry": "legal", "vendor": "Notaire Marie-Claire Dufresne",
        "bill_to": "Gestion Immobilière Tremblay\n200 boul. René-Lévesque E.\nMontréal, QC H2X 1N6",
        "ship_to": "N/A",
        "date_format": "19/03/2026",
        "lines": [
            {"desc": "Honoraires — acte de vente immeuble", "qty": 1, "unit": 1800.00, "taxable": True},
            {"desc": "Recherche de titres et registre foncier", "qty": 1, "unit": 350.00, "taxable": True},
            {"desc": "Débours: droits de mutation (taxe de bienvenue)", "qty": 1, "unit": 12500.00, "taxable": False},
            {"desc": "Débours: certificat de localisation", "qty": 1, "unit": 800.00, "taxable": False},
        ],
        "note": "[Droits de mutation = 1.5% sur 833,333$ — non taxable]",
        "prorated": None,
    },
    {
        "industry": "legal", "vendor": "Lapointe Rosenstein Marchand Melançon LLP",
        "bill_to": "Fabrication Dubois Inc\n6200 boul. Henri-Bourassa E.\nMontréal-Nord, QC H1G 5W9",
        "ship_to": "N/A",
        "date_format": "March 19 2026",
        "lines": [
            {"desc": "Révision contrat fournisseur (heures)", "qty": 6, "unit": 425.00, "taxable": True},
            {"desc": "Rédaction clause de non-concurrence", "qty": 3, "unit": 425.00, "taxable": True},
            {"desc": "Consultation droit du travail (heures)", "qty": 2, "unit": 375.00, "taxable": True},
            {"desc": "Débours: service de traduction assermentée", "qty": 1, "unit": 680.00, "taxable": False},
        ],
        "note": "[Traduction anglais-français — contrat pour client ontarien]",
        "prorated": None,
    },
    {
        "industry": "legal", "vendor": "BCF Avocats d'affaires — Bureau de Québec",
        "bill_to": "Clinique médicale du Parc\n3875 rue St-Urbain\nMontréal, QC H2W 1T1",
        "ship_to": "N/A",
        "date_format": "2026-03-19",
        "lines": [
            {"desc": "Consultation — conformité LPRPDE (heures)", "qty": 4, "unit": 395.00, "taxable": True},
            {"desc": "Rédaction politique de confidentialité", "qty": 1, "unit": 2800.00, "taxable": True},
            {"desc": "Formation personnel (demi-journée)", "qty": 1, "unit": 1500.00, "taxable": True},
            {"desc": "Débours: impression guides (100 copies)", "qty": 100, "unit": 4.50, "taxable": False},
        ],
        "note": "[Formation exempt de taxes si organisme accrédité — vérifier admissibilité]",
        "prorated": "Partial billing: Phase 1 of 3 — Phases 2-3 to follow in Q2",
    },
]


def generate_complex_invoices() -> Path:
    """Generate 20 complex Quebec invoices as text files."""
    out_dir = TRAINING_DIR / "invoices"
    out_dir.mkdir(parents=True, exist_ok=True)

    for idx, tpl in enumerate(_INVOICE_TEMPLATES, 1):
        lines_text = []
        subtotal_taxable = 0.0
        subtotal_exempt = 0.0

        for ln in tpl["lines"]:
            line_total = round(ln["qty"] * ln["unit"], 2)
            tax_label = "TX" if ln["taxable"] else "EX"
            lines_text.append(
                f"  {ln['desc']:<60s} {ln['qty']:>6.1f} × ${ln['unit']:>10.2f} = ${line_total:>10.2f}  [{tax_label}]"
            )
            if ln["taxable"]:
                subtotal_taxable += line_total
            else:
                subtotal_exempt += line_total

        subtotal = round(subtotal_taxable + subtotal_exempt, 2)
        gst = round(subtotal_taxable * 0.05, 2)
        qst = round(subtotal_taxable * 0.09975, 2)
        total = round(subtotal + gst + qst, 2)

        content = f"""{'='*80}
FACTURE / INVOICE
{'='*80}

De / From: {tpl['vendor']}
Date: {tpl['date_format']}
Facture #: INV-ADV-{idx:03d}

Facturer à / Bill To:
{tpl['bill_to']}

Livrer à / Ship To:
{tpl['ship_to']}

{'─'*80}
DÉTAIL / LINE ITEMS
{'─'*80}
{chr(10).join(lines_text)}

{'─'*80}
Sous-total articles taxables / Taxable subtotal:     ${subtotal_taxable:>10.2f}
Sous-total articles exempts / Exempt subtotal:        ${subtotal_exempt:>10.2f}
Sous-total / Subtotal:                                ${subtotal:>10.2f}
TPS/GST (5%):                                         ${gst:>10.2f}
TVQ/QST (9.975%):                                     ${qst:>10.2f}
TOTAL:                                                 ${total:>10.2f}

Notes: {tpl['note']}
"""
        if tpl.get("prorated"):
            content += f"\n*** {tpl['prorated']} ***\n"

        content += f"\nIndustrie / Industry: {tpl['industry'].upper()}\n"

        filepath = out_dir / f"invoice_{idx:02d}.txt"
        filepath.write_text(content, encoding="utf-8")

    print(f"[complex_invoices] 20 invoices -> {out_dir}")
    return out_dir


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATOR 3 — Messy client emails
# ═══════════════════════════════════════════════════════════════════════════════

_MESSY_EMAILS: list[dict[str, Any]] = [
    {
        "subject": "Mes dépenses du mois",
        "body": """Salut Jean-Pierre,

J'espère que t'es pas trop occupé. Bon, j'ai plein de receipts à te donner mais j'en ai perdu quelques-uns, sorry about that.

Ok fait que, cette semaine:
- J'ai payé le gars qui répare les tuyaux, y m'a chargé 485$ cash. J'ai pas de receipt mais je l'ai payé c'est sûr.
- Rona pour des matériaux, environ 1,250$ je pense? La carte Visa.
- Home Depot, 3 voyages: une fois 89$, une fois genre 350$, pis une autre fois je me souviens plus mais c'était dans les 200$ environ.
- Mon cell Rogers, 125.50$ c'est le montant exact celui-là.
- J'ai mis du gaz dans le truck, Ultramar, mettons 4 fois ce mois-ci, à peu près 85$ chaque fois.
- Le lunch avec le client Tremblay au restaurant Pacini, on était 4, ça a coûté about two hundred with the tip.
- Mon kid avait sa fête, j'ai commandé le gâteau chez Pâtisserie Duc de Lorraine, 65$... c'est-tu business ça? Y'avait du monde du bureau...
- Assurance du truck, Intact, 345$ par mois comme d'habitude.
- Bureau en Gros pour l'imprimante, une cinquantaine de dollars en cartouches.
- Amazon, j'ai commandé des affaires pour le bureau mais aussi des cadeaux de Noël... le total était 423.67$ mais faudrait séparer ça.
- Le parking downtown quand je suis allé voir le client, 28$ par jour, 3 jours.
- J'ai donné 100$ à mon gars pour qu'il aide sur le chantier samedi, c'est-tu déductible?
- Bell Internet au bureau, 89.95$.
- Le comptoir de cuisine qu'on a installé chez nous... euh au bureau je veux dire, 4,200$ chez IKEA. C'est pour la salle de lunch des employés.

Anyway merci de dealer avec tout ça, je sais que c'est le bordel!

Marc""",
        "parsed": {
            "expenses": [
                {"vendor": "Plombier (inconnu)", "amount": 485.00, "category": "Réparations plomberie", "tax_code": "T", "receipt": False, "note": "Cash, pas de reçu — à clarifier"},
                {"vendor": "Rona", "amount": 1250.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Montant approximatif, carte Visa"},
                {"vendor": "Home Depot", "amount": 89.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Visite 1"},
                {"vendor": "Home Depot", "amount": 350.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Visite 2, montant approximatif"},
                {"vendor": "Home Depot", "amount": 200.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Visite 3, montant estimé ~200$"},
                {"vendor": "Rogers", "amount": 125.50, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": "Cellulaire — usage mixte?"},
                {"vendor": "Ultramar", "amount": 340.00, "category": "Carburant et huile", "tax_code": "T", "receipt": True, "note": "4 × ~85$ estimé"},
                {"vendor": "Pacini", "amount": 200.00, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "4 convives, client Tremblay, pourboire inclus"},
                {"vendor": "Pâtisserie Duc de Lorraine", "amount": 65.00, "category": "PERSONNEL — à exclure ou clarifier", "tax_code": "T", "receipt": True, "note": "Fête enfant — mixte personnel/affaires, à clarifier"},
                {"vendor": "Intact Assurance", "amount": 345.00, "category": "Assurances", "tax_code": "I", "receipt": True, "note": "Prime mensuelle truck"},
                {"vendor": "Bureau en Gros", "amount": 50.00, "category": "Fournitures de bureau", "tax_code": "T", "receipt": True, "note": "Cartouches imprimante, montant approximatif"},
                {"vendor": "Amazon", "amount": 423.67, "category": "MIXTE — séparer affaires/personnel", "tax_code": "T", "receipt": True, "note": "Inclut cadeaux personnels, à ventiler"},
                {"vendor": "Stationnement (inconnu)", "amount": 84.00, "category": "Stationnement", "tax_code": "E", "receipt": True, "note": "3 jours × 28$"},
                {"vendor": "Aide chantier (employé?)", "amount": 100.00, "category": "Salaires / Sous-traitance", "tax_code": "E", "receipt": False, "note": "Paiement cash, aucun reçu — DAS requis?"},
                {"vendor": "Bell", "amount": 89.95, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": "Internet bureau"},
                {"vendor": "IKEA", "amount": 4200.00, "category": "Améliorations locatives / Immobilisations", "tax_code": "T", "receipt": True, "note": "Comptoir cuisine salle lunch — immobilisation si >500$?"}
            ],
            "flags": ["Reçu manquant: plombier", "Reçu manquant: aide chantier", "À séparer: Amazon (personnel vs affaires)", "À clarifier: gâteau fête enfant", "Usage mixte possible: cellulaire Rogers"]
        }
    },
    {
        "subject": "RE: Mes receipts — URGENT",
        "body": """Jean-Pierre,

Désolé du retard, j'ai été sur le chantier toute la semaine. Voici les dépenses que j'ai pu retrouver.

Côté business:
- Hydro-Québec: 287.45$ pour le local
- Gaz Énergir: dans les 150$ (j'ai perdu la facture mais c'est autour de ça)
- Le gars qui fait le ménage au bureau, je le paye 200$ cash aux 2 semaines, ça fait 400$ ce mois-ci. Y'a pas de facture, c'est un monsieur qui fait ça au noir... je sais, je sais.
- Matériaux chez Patrick Morin: 2,340.89$ exactement, j'ai le receipt.
- Pneus d'hiver pour le truck de la compagnie, 1,100$ chez Kal Tire.
- Le meeting avec l'architecte au Château Frontenac, souper + drinks, 380$ total pour 3 personnes.
- Windows Office pour les ordis, 249$ ou 259$ je me rappelle plus, c'est Microsoft en tout cas.
- J'ai avancé 500$ de ma poche pour payer l'inspecteur municipal, faut me rembourser ça.
- Canadian Tire pour des tools, trois quatre cents piastres.
- Le wrap du truck avec le logo, 2,800$ chez Wrap Zone Québec.

Côté personnel (à PAS mettre dans la business):
- Spa avec ma blonde, 300$
- Cadeau fête des mères, 150$ chez Simons

Aussi, question: le gars qui m'a fait le deck chez nous l'été passé, 8,500$, y'a pas chargé de taxes. C'est correct ça?

Merci!
Steve""",
        "parsed": {
            "expenses": [
                {"vendor": "Hydro-Québec", "amount": 287.45, "category": "Électricité et gaz", "tax_code": "E", "receipt": True, "note": "Local commercial"},
                {"vendor": "Énergir", "amount": 150.00, "category": "Électricité et gaz", "tax_code": "E", "receipt": False, "note": "Montant approximatif, reçu perdu"},
                {"vendor": "Ménage (travailleur non déclaré)", "amount": 400.00, "category": "Entretien et nettoyage", "tax_code": "E", "receipt": False, "note": "ALERTE: paiement au noir, pas de facture, non déductible sans reçu"},
                {"vendor": "Patrick Morin", "amount": 2340.89, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Montant exact avec reçu"},
                {"vendor": "Kal Tire", "amount": 1100.00, "category": "Entretien véhicules", "tax_code": "T", "receipt": True, "note": "Pneus hiver truck compagnie"},
                {"vendor": "Château Frontenac", "amount": 380.00, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "3 convives, architecte, souper + drinks"},
                {"vendor": "Microsoft", "amount": 254.00, "category": "Logiciels et abonnements", "tax_code": "T", "receipt": True, "note": "Montant à confirmer (249$ ou 259$)"},
                {"vendor": "Inspecteur municipal", "amount": 500.00, "category": "Permis et inspections", "tax_code": "E", "receipt": True, "note": "Avance personnelle à rembourser"},
                {"vendor": "Canadian Tire", "amount": 375.00, "category": "Outillage", "tax_code": "T", "receipt": True, "note": "Montant approximatif ~350-400$"},
                {"vendor": "Wrap Zone Québec", "amount": 2800.00, "category": "Publicité et marketing", "tax_code": "T", "receipt": True, "note": "Lettrage véhicule commercial"}
            ],
            "flags": ["PERSONNEL EXCLU: Spa 300$", "PERSONNEL EXCLU: Simons 150$", "ALERTE: travailleur au noir (ménage)", "À vérifier: deck 8500$ sans taxes — possible évasion fiscale du contracteur", "Reçu manquant: Énergir"]
        }
    },
    {
        "subject": "dépenses mois de mars svp",
        "body": """Allo,

Voici ce que j'ai pour mars. J'ai tout mis dans une enveloppe brune mais je l'ai oubliée dans mon char... je vais te l'apporter demain.

- Sysco livraison: 3,456.78$
- Metro grocery pour le resto: 2 fois, 1,200$ et 890$
- Bell téléphone: 156.30$
- Vidéotron internet: 112.45$
- Les nappes et serviettes chez Linen Chest: environ 300$
- Le repair du four, le technicien Frigidaire est venu, 650$ avec les pièces
- Nettoyage de la hotte, Ventilation Expert Québec, 800$
- Produits chimiques pour le dishwasher, euh JohnsonDiversey je pense, dans les 200$
- Mon parking au centre-ville, 180$ pour le mois
- Caisse populaire, les frais: 45.90$
- 3 cases de vin pour le resto, chez la SAQ, 1,080$ total
- Publicité dans le journal local, Le Soleil, 450$
- Le gars du snow removal, 350$ pour mars
- J'ai acheté un iPad pour prendre les commandes, 899.99$ chez Apple
- Licence MAPAQ renouvellement, 250$ ou 275$, me rappelle pu

C'est pas mal ça! Ah oui pis ma fille a pris un lunch au resto avec ses amies sur la carte business, 87$, mais c'est personnel ça. Enlève-le svp.

Merci!
Marcel""",
        "parsed": {
            "expenses": [
                {"vendor": "Sysco", "amount": 3456.78, "category": "Achats et matières premières", "tax_code": "T", "receipt": True, "note": "Livraison fournitures restaurant"},
                {"vendor": "Metro", "amount": 1200.00, "category": "Achats et matières premières", "tax_code": "T", "receipt": True, "note": "Visite 1"},
                {"vendor": "Metro", "amount": 890.00, "category": "Achats et matières premières", "tax_code": "T", "receipt": True, "note": "Visite 2"},
                {"vendor": "Bell", "amount": 156.30, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": "Téléphone"},
                {"vendor": "Vidéotron", "amount": 112.45, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": "Internet"},
                {"vendor": "Linen Chest", "amount": 300.00, "category": "Fournitures de restaurant", "tax_code": "T", "receipt": True, "note": "Nappes/serviettes, montant approximatif"},
                {"vendor": "Frigidaire (technicien)", "amount": 650.00, "category": "Réparations équipement", "tax_code": "T", "receipt": True, "note": "Réparation four incluant pièces"},
                {"vendor": "Ventilation Expert Québec", "amount": 800.00, "category": "Entretien et nettoyage", "tax_code": "T", "receipt": True, "note": "Nettoyage hotte commerciale"},
                {"vendor": "JohnsonDiversey", "amount": 200.00, "category": "Fournitures de nettoyage", "tax_code": "T", "receipt": True, "note": "Produits lave-vaisselle, ~200$"},
                {"vendor": "Stationnement centre-ville", "amount": 180.00, "category": "Stationnement", "tax_code": "E", "receipt": True, "note": "Mensuel"},
                {"vendor": "Caisse Desjardins", "amount": 45.90, "category": "Frais bancaires", "tax_code": "E", "receipt": True, "note": "Frais mensuels"},
                {"vendor": "SAQ", "amount": 1080.00, "category": "Achats boissons", "tax_code": "T", "receipt": True, "note": "3 caisses de vin pour le restaurant"},
                {"vendor": "Le Soleil", "amount": 450.00, "category": "Publicité et marketing", "tax_code": "T", "receipt": True, "note": "Publicité journal local"},
                {"vendor": "Déneigement (inconnu)", "amount": 350.00, "category": "Entretien et nettoyage", "tax_code": "T", "receipt": False, "note": "Déneigement mars, pas de nom d'entreprise"},
                {"vendor": "Apple", "amount": 899.99, "category": "Équipement informatique", "tax_code": "T", "receipt": True, "note": "iPad pour commandes — immobilisation?"},
                {"vendor": "MAPAQ", "amount": 262.50, "category": "Permis et licences", "tax_code": "E", "receipt": True, "note": "Renouvellement licence, montant entre 250-275$"}
            ],
            "flags": ["PERSONNEL EXCLU: lunch fille 87$", "Reçus dans l'enveloppe brune à récupérer", "Reçu manquant: déneigement", "À confirmer: montant exact MAPAQ"]
        }
    },
    {
        "subject": "Fw: TOUTES mes dépenses!!",
        "body": """Bonjour!!

Bon je sais que ça fait 2 mois que je t'ai rien envoyé. Voici TOUT ce que j'ai pu retrouver. J'ai fouillé dans mes poches de manteau, mon dashboard, everywhere.

Janvier:
- Tim Hortons genre 15 fois, mettons 6$ en moyenne... 90$ total
- Costco membership renewal 65$ (c'est-tu déductible?)
- Costco achats: 850$ mais y'a de la bouffe personnelle là-dedans, mettons 60% business
- Staples, environ 200$ de fournitures
- Mon assurance auto Desjardins, 189.50$/mois

Février:
- Le même Timmy's, encore 90$
- Bell cellulaire 95.40$
- Essence Shell, j'ai rempli genre 5-6 fois, environ 75$ chaque shot, disons 400$ pour être safe
- Réparation de mon laptop chez un gars sur Kijiji, 150$ cash (j'ai pas de reçu mais bon)
- Publicité Facebook, 500$ US... c'est combien en canadien? Dans les 680$ je pense?
- Un webinaire de formation, 299$ chez HubSpot Academy
- La teinture des vitres du truck, 250$ (c'est pour la business le truck!)

Weird expense: mon voisin m'a échangé 2 jours de travail de peinture contre un BBQ que j'avais, faut-tu que je déclare ça? Le BBQ valait genre 400$.

Merci de ta patience!
Nathalie""",
        "parsed": {
            "expenses": [
                {"vendor": "Tim Hortons", "amount": 90.00, "category": "Repas d'affaires", "tax_code": "M", "receipt": False, "note": "Janvier — estimé ~15 × 6$, pas de reçus individuels"},
                {"vendor": "Costco", "amount": 65.00, "category": "Frais d'adhésion", "tax_code": "E", "receipt": True, "note": "Membership — déductible si usage affaires"},
                {"vendor": "Costco", "amount": 510.00, "category": "Fournitures / Achats mixtes", "tax_code": "T", "receipt": True, "note": "850$ × 60% affaires = 510$, à ventiler"},
                {"vendor": "Staples/Bureau en Gros", "amount": 200.00, "category": "Fournitures de bureau", "tax_code": "T", "receipt": True, "note": "Montant approximatif"},
                {"vendor": "Desjardins Assurance", "amount": 189.50, "category": "Assurances", "tax_code": "I", "receipt": True, "note": "Janvier — auto mensuel"},
                {"vendor": "Tim Hortons", "amount": 90.00, "category": "Repas d'affaires", "tax_code": "M", "receipt": False, "note": "Février — même estimation"},
                {"vendor": "Bell", "amount": 95.40, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": "Cellulaire février"},
                {"vendor": "Shell", "amount": 400.00, "category": "Carburant et huile", "tax_code": "T", "receipt": True, "note": "Estimé 5-6 pleins × ~75$"},
                {"vendor": "Réparation laptop (Kijiji)", "amount": 150.00, "category": "Entretien équipement", "tax_code": "E", "receipt": False, "note": "Cash, pas de reçu — non déductible sans justificatif"},
                {"vendor": "Facebook/Meta Ads", "amount": 680.00, "category": "Publicité et marketing", "tax_code": "T", "receipt": True, "note": "500 USD converti ~680 CAD, à confirmer taux exact"},
                {"vendor": "HubSpot Academy", "amount": 299.00, "category": "Formation", "tax_code": "T", "receipt": True, "note": "Webinaire formation"},
                {"vendor": "Teinte de vitres", "amount": 250.00, "category": "Entretien véhicules", "tax_code": "T", "receipt": True, "note": "Truck de la compagnie"},
                {"vendor": "Desjardins Assurance", "amount": 189.50, "category": "Assurances", "tax_code": "I", "receipt": True, "note": "Février — auto mensuel"}
            ],
            "flags": ["Reçus manquants: Tim Hortons (tous), réparation Kijiji", "Costco: ventilation personnelle/affaires requise", "TROC: échange BBQ (~400$) contre services peinture — valeur marchande à déclarer comme revenu et dépense", "Teinte vitres: vérifier si truck 100% business"]
        }
    },
    {
        "subject": "les affaires du mois",
        "body": """Hey,

Je t'envoie ça vite fait entre deux clients.

Mes dépenses:
- Déplacement Montréal-Québec en char: 250 km × 0.70$/km = 175$
- Hôtel Marriott Québec 2 nuits: 189$ + 189$ = 378$ plus les taxes
- Le souper au Laurie Raphaël avec le client Côté: 285$ pour 2 personnes, incluant une bouteille de vin à 95$
- Uber x3 dans Québec: 18$, 24$, 31$
- Papeterie Nota Bene: 67.80$ pour des cartes d'affaires
- Impression 500 dépliants chez Copies du Centre: 325$
- Abonnement LinkedIn Premium: 79.99$ USD (combien en CAD?)
- Domaine web renouvellement GoDaddy: 25$ US
- Mailchimp mensuel: 45$ US
- Achat logiciel Canva Pro: 169$ pour l'année (c'est-tu amortissable?)
- Cadeau client — bouteille de scotch 120$ SAQ
- Café avec prospect, Second Cup, 12.50$

J'ai aussi payé un designer sur Fiverr, 200$ US, pour refaire mon logo. C'est payé par PayPal mais j'ai la confirmation.

Truc bizarre: un client m'a payé en crypto (0.015 BTC) pour une consultation. Comment on gère ça?

Cheers,
Sophie""",
        "parsed": {
            "expenses": [
                {"vendor": "Kilométrage personnel", "amount": 175.00, "category": "Transport et déplacements", "tax_code": "E", "receipt": False, "note": "250 km × 0.70$/km, log de kilométrage requis"},
                {"vendor": "Marriott Québec", "amount": 378.00, "category": "Hébergement", "tax_code": "T", "receipt": True, "note": "2 nuits avant taxes — total avec taxes à vérifier sur le reçu"},
                {"vendor": "Laurie Raphaël", "amount": 285.00, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "2 convives, client Côté, vin 95$ inclus"},
                {"vendor": "Uber", "amount": 73.00, "category": "Transport et déplacements", "tax_code": "T", "receipt": True, "note": "3 courses: 18+24+31$"},
                {"vendor": "Nota Bene", "amount": 67.80, "category": "Impression et papeterie", "tax_code": "T", "receipt": True, "note": "Cartes d'affaires"},
                {"vendor": "Copies du Centre", "amount": 325.00, "category": "Impression et papeterie", "tax_code": "T", "receipt": True, "note": "500 dépliants"},
                {"vendor": "LinkedIn", "amount": 109.00, "category": "Publicité et marketing", "tax_code": "T", "receipt": True, "note": "79.99 USD → ~109 CAD, confirmer taux"},
                {"vendor": "GoDaddy", "amount": 34.00, "category": "Logiciels et abonnements", "tax_code": "T", "receipt": True, "note": "25 USD → ~34 CAD"},
                {"vendor": "Mailchimp", "amount": 62.00, "category": "Logiciels et abonnements", "tax_code": "T", "receipt": True, "note": "45 USD → ~62 CAD"},
                {"vendor": "Canva", "amount": 169.00, "category": "Logiciels et abonnements", "tax_code": "T", "receipt": True, "note": "Annuel — non amortissable car < 500$"},
                {"vendor": "SAQ", "amount": 120.00, "category": "Représentation", "tax_code": "T", "receipt": True, "note": "Cadeau client — limite 50%"},
                {"vendor": "Second Cup", "amount": 12.50, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "Café prospect"},
                {"vendor": "Fiverr (designer)", "amount": 274.00, "category": "Sous-traitance", "tax_code": "E", "receipt": True, "note": "200 USD → ~274 CAD, PayPal confirmation"}
            ],
            "flags": ["Kilométrage: log de déplacement requis pour déduction", "USD conversions à confirmer avec taux du jour", "CRYPTO: 0.015 BTC reçu — déclarer comme revenu au taux du jour de réception", "Cadeau client: déductible à 50% seulement"]
        }
    },
    {
        "subject": "Factures en retard désolé!!!",
        "body": """Bonjour Jean-Pierre,

Je suis TELLEMENT en retard c'est pas drôle. J'ai des factures de 3 mois en arrière. Ma blonde va me tuer si le comptable me lâche.

OK let's go:
- Loyer du local: 2,500$ × 3 mois = 7,500$ payé à Gestion Immobilière Tremblay
- Hydro 3 mois: j'ai pas les factures mais d'habitude c'est entre 200$ et 280$ par mois
- Internet Vidéotron: 99.95$ × 3 = 299.85$
- Assurance du local, La Personnelle: 425$/mois × 3
- J'ai fait faire un site web par une agence à Sherbrooke, ça m'a coûté 6,500$. Le gars s'appelle... Créations Web JS? Quelque chose comme ça.
- Ameublement du bureau: 2 chaises chez Structube 600$ chaque = 1,200$, un bureau chez IKEA 450$
- Alarme Protectron: 49.95$/mois × 3
- Mon véhicule personnel utilisé pour affaires: environ 2,000 km sur 3 mois. C'est quoi le rate déjà? 0.68$/km?
- Achat d'un deuxième écran Dell pour le bureau, 549.99$ sur Amazon
- Carte d'affaires et enveloppes chez Imprimerie Rapide, genre 180$ ou 200$

Ah pis j'oubliais: j'ai prêté 2,000$ à mon partenaire d'affaires (son entreprise), y m'a remboursé 1,500$ mais y me doit encore 500$. C'est-tu une dépense ça?

MERCI de pas me juger lol

François""",
        "parsed": {
            "expenses": [
                {"vendor": "Gestion Immobilière Tremblay", "amount": 7500.00, "category": "Loyer", "tax_code": "E", "receipt": True, "note": "3 mois × 2,500$ — loyer commercial exempt de taxes"},
                {"vendor": "Hydro-Québec", "amount": 720.00, "category": "Électricité et gaz", "tax_code": "E", "receipt": False, "note": "3 mois estimé ~240$/mois, reçus manquants"},
                {"vendor": "Vidéotron", "amount": 299.85, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": "3 mois × 99.95$"},
                {"vendor": "La Personnelle", "amount": 1275.00, "category": "Assurances", "tax_code": "I", "receipt": True, "note": "3 mois × 425$"},
                {"vendor": "Créations Web JS (Sherbrooke)", "amount": 6500.00, "category": "Services professionnels / Marketing", "tax_code": "T", "receipt": True, "note": "Site web — nom exact à confirmer"},
                {"vendor": "Structube", "amount": 1200.00, "category": "Ameublement de bureau", "tax_code": "T", "receipt": True, "note": "2 chaises × 600$ — immobilisation si >500$ unitaire"},
                {"vendor": "IKEA", "amount": 450.00, "category": "Ameublement de bureau", "tax_code": "T", "receipt": True, "note": "Bureau"},
                {"vendor": "Protectron", "amount": 149.85, "category": "Sécurité et surveillance", "tax_code": "T", "receipt": True, "note": "3 mois × 49.95$"},
                {"vendor": "Kilométrage personnel", "amount": 1360.00, "category": "Transport et déplacements", "tax_code": "E", "receipt": False, "note": "2,000 km × 0.68$/km — log requis"},
                {"vendor": "Amazon (Dell)", "amount": 549.99, "category": "Équipement informatique", "tax_code": "T", "receipt": True, "note": "Écran Dell — immobilisation?"},
                {"vendor": "Imprimerie Rapide", "amount": 190.00, "category": "Impression et papeterie", "tax_code": "T", "receipt": True, "note": "Cartes + enveloppes, montant ~180-200$"}
            ],
            "flags": ["Reçus manquants: Hydro-Québec (3 mois)", "Prêt 2,000$ au partenaire: NON une dépense — c'est un prêt/avance, le 500$ restant est une créance", "Kilométrage: besoin d'un log détaillé", "Factures de 3 mois: vérifier les dates pour la bonne période comptable"]
        }
    },
    {
        "subject": "Re: Re: Fwd: receipts",
        "body": """Allo encore,

Mon chum dit que faut que j'envoie TOUT même les petits montants. So here goes:

- Coffee Starbucks: 5.75$ (est-ce que ça vaut la peine?)
- Autre Starbucks: 6.20$
- Encore Starbucks: 7.45$ (j'avais un muffin avec)
- Tim's: 4.50$
- Tim's: 5.80$
- Dollarama pour des affaires de bureau: 23.40$
- Walmart pour de l'encre et du papier: 87.60$ (mais j'ai aussi acheté du shampoing et des bas, enlève maybe 15$?)
- Jean Coutu: 34.50$ — produits de nettoyage pour le bureau
- Uber Eats pour le dîner au bureau: 28.90$
- Skip the Dishes: 22.40$
- DoorDash: 35.60$ (c'était pour une réunion d'équipe, on était 3)
- Mon gym: 55$/mois — c'est-tu déductible si c'est pour ma santé mentale au travail? lol
- Netflix: 22.99$ — on regarde des documentaires pour la business... non je niaise, c'est personnel
- Spotify: 11.99$ — same thing, personnel
- Costco essence: 78.45$
- Couche-Tard gaz: 62.30$
- Lavage d'auto: 18$ chez Petro-Canada

J'pense que c'est tout pour cette semaine!

Chantal""",
        "parsed": {
            "expenses": [
                {"vendor": "Starbucks", "amount": 19.40, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "3 visites: 5.75+6.20+7.45$"},
                {"vendor": "Tim Hortons", "amount": 10.30, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "2 visites: 4.50+5.80$"},
                {"vendor": "Dollarama", "amount": 23.40, "category": "Fournitures de bureau", "tax_code": "T", "receipt": True, "note": "Fournitures bureau"},
                {"vendor": "Walmart", "amount": 72.60, "category": "Fournitures de bureau", "tax_code": "T", "receipt": True, "note": "87.60$ - 15$ personnel = 72.60$"},
                {"vendor": "Jean Coutu", "amount": 34.50, "category": "Fournitures de nettoyage", "tax_code": "T", "receipt": True, "note": "Produits nettoyage bureau"},
                {"vendor": "Uber Eats", "amount": 28.90, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "Dîner au bureau"},
                {"vendor": "Skip the Dishes", "amount": 22.40, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "Dîner"},
                {"vendor": "DoorDash", "amount": 35.60, "category": "Repas d'affaires", "tax_code": "M", "receipt": True, "note": "Réunion 3 personnes"},
                {"vendor": "Costco", "amount": 78.45, "category": "Carburant et huile", "tax_code": "T", "receipt": True, "note": "Essence"},
                {"vendor": "Couche-Tard", "amount": 62.30, "category": "Carburant et huile", "tax_code": "T", "receipt": True, "note": "Essence"},
                {"vendor": "Petro-Canada", "amount": 18.00, "category": "Entretien véhicules", "tax_code": "T", "receipt": True, "note": "Lavage auto"}
            ],
            "flags": ["PERSONNEL EXCLU: gym 55$", "PERSONNEL EXCLU: Netflix 22.99$", "PERSONNEL EXCLU: Spotify 11.99$", "Walmart: 15$ personnel retiré", "Repas individuels: limités en déductibilité si pas de client/prospect"]
        }
    },
    {
        "subject": "Dépenses clinic + question taxes",
        "body": """Bonjour,

Les dépenses de la clinique pour le mois:

- McKesson fournitures médicales: 4,567.89$ (y'a des items taxables et non-taxables mélangés, good luck avec ça)
- Sirona nouveau fauteuil dentaire: 18,500$ — c'est un gros achat, how do we depreciate that?
- Pharmascience: 1,234.56$ en prescriptions
- Lyreco fournitures bureau: 345.67$
- Stérilisation Pro: 289.00$
- Loyer Dr. Tremblay Immeubles: 3,800$
- Hydro: 234.50$
- Bell: 187.90$
- Zoom Pro: 20.99$ US
- Formation continue Dr. Lavoie: 1,500$ (un congrès à Toronto, flight 450$, hôtel 3 nuits × 220$ = 660$, inscription 390$)
- Lab Biron analyses: 890.00$ (certaines couvertes RAMQ, d'autres pas)

Oh aussi, une patiente a payé son filling 200$ cash et on s'est trompé, faut rembourser 200$ parce que la RAMQ couvre finalement. C'est un credit note ça?

Aussi: le parking de la clinique, 3 spots à 200$/mois = 600$. C'est au nom du Dr. mais c'est la clinique qui paye.

Merci!
Dre. Lavoie""",
        "parsed": {
            "expenses": [
                {"vendor": "McKesson", "amount": 4567.89, "category": "Fournitures médicales", "tax_code": "T", "receipt": True, "note": "Mix taxable/exempt — ventilation requise sur la facture"},
                {"vendor": "Sirona", "amount": 18500.00, "category": "Équipement médical — Immobilisation", "tax_code": "T", "receipt": True, "note": "Fauteuil dentaire — amortissement classe 8 (20% dégressif)"},
                {"vendor": "Pharmascience", "amount": 1234.56, "category": "Fournitures médicales", "tax_code": "T", "receipt": True, "note": "Prescriptions"},
                {"vendor": "Lyreco", "amount": 345.67, "category": "Fournitures de bureau", "tax_code": "T", "receipt": True, "note": ""},
                {"vendor": "Stérilisation Pro", "amount": 289.00, "category": "Fournitures médicales", "tax_code": "T", "receipt": True, "note": ""},
                {"vendor": "Dr. Tremblay Immeubles", "amount": 3800.00, "category": "Loyer", "tax_code": "E", "receipt": True, "note": "Loyer mensuel clinique"},
                {"vendor": "Hydro-Québec", "amount": 234.50, "category": "Électricité et gaz", "tax_code": "E", "receipt": True, "note": ""},
                {"vendor": "Bell", "amount": 187.90, "category": "Télécommunications", "tax_code": "T", "receipt": True, "note": ""},
                {"vendor": "Zoom", "amount": 29.00, "category": "Logiciels et abonnements", "tax_code": "T", "receipt": True, "note": "20.99 USD → ~29 CAD"},
                {"vendor": "Formation/Congrès Toronto", "amount": 1500.00, "category": "Formation professionnelle", "tax_code": "T", "receipt": True, "note": "Vol 450$ + hôtel 660$ + inscription 390$ — hors-QC: HST"},
                {"vendor": "Lab Biron", "amount": 890.00, "category": "Frais de laboratoire", "tax_code": "E", "receipt": True, "note": "À ventiler: certains items couverts RAMQ"},
                {"vendor": "Stationnement clinique", "amount": 600.00, "category": "Stationnement", "tax_code": "E", "receipt": True, "note": "3 spots × 200$/mois"}
            ],
            "flags": ["Note de crédit: remboursement 200$ patiente (RAMQ couvre)", "Sirona 18,500$: immobilisation, pas une dépense courante", "Formation Toronto: vérifier si HST vs GST+QST applicable", "McKesson: ventilation taxable/exempt nécessaire"]
        }
    },
    {
        "subject": "HELP je comprends rien aux taxes",
        "body": """Jean-Pierre!!

OK question existentielle: pourquoi des fois y'a des taxes sur mes factures pis des fois y'en a pas?? Ça me gosse.

Anyway voici le mois:
- Fournisseur en Ontario, "Industrial Parts Canada" — y m'a chargé 13% HST sur 3,200$. C'est correct ça? Moi je suis au Québec!
- Un gars du Nouveau-Brunswick, "Maritime Welding Ltd" — y m'a chargé 15% HST sur 1,800$
- Amazon.ca: y m'ont chargé GST+QST sur certains items pis juste GST sur d'autres? Le total est 567.89$
- J'ai acheté des pièces aux États sur eBay: 450$ US, j'ai payé des frais de douane de 89$ et broker fees de 25$
- Un consultant en France m'a facturé 2,000 EUR pour un rapport. Pas de taxes du tout sur sa facture. C'est-tu correct?
- Mon assurance auto: pas de taxes, 345$/mois
- Loyer: pas de taxes, 2,800$/mois
- Frais bancaires Desjardins: pas de taxes, 67.45$
- Google Ads: y disent que c'est GST seulement? 1,200$ le mois

Aussi j'ai un sub-contractor qui m'envoie des factures sans numéro de TPS. Y dit qu'y fait moins de 30,000$. C'est legit?

Le montant du sub-contractor c'est 1,500$ par mois.

Alain""",
        "parsed": {
            "expenses": [
                {"vendor": "Industrial Parts Canada (ON)", "amount": 3200.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "13% HST facturé — devrait être GST+QST pour livraison au QC. Vérifier lieu de livraison."},
                {"vendor": "Maritime Welding Ltd (NB)", "amount": 1800.00, "category": "Sous-traitance", "tax_code": "T", "receipt": True, "note": "15% HST — même enjeu, devrait être GST+QST si service au QC"},
                {"vendor": "Amazon.ca", "amount": 567.89, "category": "Fournitures", "tax_code": "T", "receipt": True, "note": "Mix de vendeurs — certains QC (GST+QST), certains hors-QC"},
                {"vendor": "eBay (USA)", "amount": 617.00, "category": "Matériaux et fournitures", "tax_code": "E", "receipt": True, "note": "450 USD → ~617 CAD + douane 89$ + courtier 25$"},
                {"vendor": "Douanes Canada", "amount": 89.00, "category": "Droits de douane", "tax_code": "E", "receipt": True, "note": "Frais d'importation"},
                {"vendor": "Courtier en douane", "amount": 25.00, "category": "Frais de courtage", "tax_code": "T", "receipt": True, "note": "Broker fees"},
                {"vendor": "Consultant France", "amount": 2920.00, "category": "Services professionnels", "tax_code": "E", "receipt": True, "note": "2,000 EUR → ~2,920 CAD — autoliquidation TPS/TVQ à considérer"},
                {"vendor": "Assurance auto", "amount": 345.00, "category": "Assurances", "tax_code": "I", "receipt": True, "note": "9% taxe sur assurance QC"},
                {"vendor": "Loyer", "amount": 2800.00, "category": "Loyer", "tax_code": "E", "receipt": True, "note": "Exempt de taxes"},
                {"vendor": "Desjardins", "amount": 67.45, "category": "Frais bancaires", "tax_code": "E", "receipt": True, "note": "Services financiers exempts"},
                {"vendor": "Google Ads", "amount": 1200.00, "category": "Publicité et marketing", "tax_code": "T", "receipt": True, "note": "Non-résident — GST seulement peut être correct"},
                {"vendor": "Sous-traitant (petit fournisseur)", "amount": 1500.00, "category": "Sous-traitance", "tax_code": "E", "receipt": True, "note": "Exempt si <30k$ annuel — demander confirmation écrite du statut"}
            ],
            "flags": ["HST hors-province: vérifier si CTI/RTI applicable vs demander factures corrigées", "Import USA: autoliquidation possible", "Consultant France: règles autoliquidation TPS/TVQ pour services importés", "Sous-traitant sans TPS#: légal si <30k$ mais obtenir déclaration"]
        }
    },
    {
        "subject": "receipts dans un sac ziploc lol",
        "body": """Bon matin!

J'ai FINALEMENT retrouvé mes receipts. Y étaient dans un sac ziploc dans le fond de mon sac de gym (oui je sais...). La plupart sont lisibles mais y'en a quelques-uns qui ont passé dans la machine à laver oops.

Ce que je peux lire:
- Un receipt de Rona, le montant est 1,__7.45$ (le chiffre du milieu est illisible). C'est soit 1,047.45 ou 1,147.45 ou 1,247.45...
- Canadian Tire: 234.89$ — celui-là est correct
- Un receipt tout pâle, je pense c'est Réno-Dépôt? Le montant ressemble à 890$ quelque chose
- Shell: 89.45$
- Esso: 76.30$
- Un receipt thermique tout noir, literally can't read anything. Mais je PENSE c'était pour du bois chez un lumber yard, maybe 500$ something?
- Metro pour les lunchs: 145.67$
- IGA: 89.30$ — mix personnel/business, disons 50/50?
- Dollarama: 12.45$
- Un virement Interac à "Martin Construction" pour 3,500$ — j'ai le relevé bancaire au moins
- Paiement par chèque à "L. Gagnon Excavation" pour 7,800$ — le chèque a été encaissé mais j'ai pas de facture

J'ai aussi trouvé un receipt de spa... oublie ça c'est personnel!

Aussi y'a 3 receipts que j'arrive pas du tout à identifier le vendor. Les montants sont 45$, 78$ et 156$. Je vais essayer de matcher avec mon relevé de carte.

Bonne chance avec tout ça!
Pierre""",
        "parsed": {
            "expenses": [
                {"vendor": "Rona", "amount": 1147.45, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Montant illisible: 1,_47.45$ — estimé 1,147.45$, à confirmer avec relevé carte"},
                {"vendor": "Canadian Tire", "amount": 234.89, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Montant confirmé"},
                {"vendor": "Réno-Dépôt (probable)", "amount": 890.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": True, "note": "Receipt pâle, vendor et montant approximatifs"},
                {"vendor": "Shell", "amount": 89.45, "category": "Carburant et huile", "tax_code": "T", "receipt": True, "note": ""},
                {"vendor": "Esso", "amount": 76.30, "category": "Carburant et huile", "tax_code": "T", "receipt": True, "note": ""},
                {"vendor": "Inconnu (lumber yard)", "amount": 500.00, "category": "Matériaux et fournitures", "tax_code": "T", "receipt": False, "note": "Receipt illisible, ~500$, non utilisable sans identification"},
                {"vendor": "Metro", "amount": 145.67, "category": "Repas / Fournitures", "tax_code": "T", "receipt": True, "note": "Lunchs bureau"},
                {"vendor": "IGA", "amount": 44.65, "category": "Repas / Fournitures", "tax_code": "T", "receipt": True, "note": "89.30$ × 50% affaires"},
                {"vendor": "Dollarama", "amount": 12.45, "category": "Fournitures de bureau", "tax_code": "T", "receipt": True, "note": ""},
                {"vendor": "Martin Construction", "amount": 3500.00, "category": "Sous-traitance", "tax_code": "T", "receipt": True, "note": "Virement Interac, relevé bancaire comme preuve"},
                {"vendor": "L. Gagnon Excavation", "amount": 7800.00, "category": "Sous-traitance", "tax_code": "T", "receipt": False, "note": "Chèque encaissé mais AUCUNE facture — demander facture immédiatement"}
            ],
            "flags": ["Receipts illisibles: lumber yard, Rona partiel", "PERSONNEL EXCLU: spa", "Facture manquante: L. Gagnon 7,800$ — URGENT", "3 receipts non identifiés: 45$+78$+156$ = 279$ à matcher avec relevé", "IGA: ventilation 50/50 estimée"]
        }
    },
]


def generate_messy_emails() -> Path:
    """Generate 10 messy client emails + parsed JSON."""
    out_dir = TRAINING_DIR / "messy_emails"
    out_dir.mkdir(parents=True, exist_ok=True)

    for idx, email in enumerate(_MESSY_EMAILS, 1):
        # Write email text
        email_path = out_dir / f"email_{idx:02d}.txt"
        email_content = f"Subject: {email['subject']}\n\n{email['body']}"
        email_path.write_text(email_content, encoding="utf-8")

        # Write parsed JSON
        parsed_path = out_dir / f"email_{idx:02d}_parsed.json"
        parsed_path.write_text(
            json.dumps(email["parsed"], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    print(f"[messy_emails] {len(_MESSY_EMAILS)} emails -> {out_dir}")
    return out_dir


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATOR 4 — Edge case scenarios
# ═══════════════════════════════════════════════════════════════════════════════

def _build_edge_doc(
    doc_id: str,
    vendor: str,
    amount: float,
    doc_date: date,
    edge_type: str,
    *,
    tax_code: str = "T",
    gl_account: str = "Charges-diverses",
    memo: str = "",
    raw_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = _utcnow()
    bd = _breakdown(_d(str(abs(amount))), tax_code if amount >= 0 else "E")
    raw = {
        "vendor": vendor,
        "subtotal": bd["subtotal"] if amount >= 0 else amount,
        "gst_amount": bd["gst"],
        "qst_amount": bd["qst"],
        "total": round(amount, 2),
        "currency": "CAD",
        "province": "QC",
        "memo": memo,
    }
    if raw_extra:
        raw.update(raw_extra)
    return {
        "document_id": doc_id,
        "file_name": f"{doc_id}.pdf",
        "file_path": f"/test_data/edge_cases/{doc_id}.pdf",
        "client_code": "EDGE_TEST",
        "vendor": vendor,
        "doc_type": "invoice",
        "amount": round(amount, 2),
        "document_date": doc_date.isoformat(),
        "gl_account": gl_account,
        "tax_code": tax_code,
        "category": "expense",
        "review_status": "ReadyToPost",
        "confidence": round(_rng.uniform(0.7, 0.99), 4),
        "raw_result": json.dumps(raw, ensure_ascii=False),
        "created_at": now,
        "updated_at": now,
        "currency": "CAD",
        "subtotal": bd["subtotal"],
        "tax_total": bd["tax_total"],
        "extraction_method": "generated_edge_case",
        "ingest_source": f"edge:{edge_type}",
        "fraud_flags": "[]",
        "edge_case_type": edge_type,
        "handwriting_low_confidence": 0,
        "handwriting_sample": 0,
    }


def generate_edge_cases(*, db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    """Generate 300 edge case transactions and insert into DB."""
    docs: list[dict[str, Any]] = []
    n = 90000  # start high to avoid collisions

    # 1. Refunds before original charge (20)
    for i in range(20):
        refund_date = _random_date(date(2025, 3, 1), date(2025, 6, 30))
        charge_date = refund_date + timedelta(days=_rng.randint(3, 15))
        amt = round(_rng.uniform(100, 5000), 2)
        v = f"Fournisseur Remboursement {i+1}"
        docs.append(_build_edge_doc(f"edge_{n}", v, -amt, refund_date,
                                     "refund_before_charge", memo="Credit note antérieure"))
        n += 1
        docs.append(_build_edge_doc(f"edge_{n}", v, amt, charge_date,
                                     "refund_before_charge", memo="Facture originale"))
        n += 1

    # 2. Vendor name changes mid-year (20)
    for i in range(20):
        gst_num = f"RT{_rng.randint(100000000, 999999999)}"
        old_name = f"Ancien Fournisseur {i+1} Inc"
        new_name = f"Nouveau Nom Fournisseur {i+1} Ltée"
        d1 = _random_date(date(2025, 1, 1), date(2025, 5, 31))
        d2 = _random_date(date(2025, 6, 1), date(2025, 12, 31))
        amt = round(_rng.uniform(500, 5000), 2)
        docs.append(_build_edge_doc(f"edge_{n}", old_name, amt, d1,
                                     "vendor_name_change",
                                     raw_extra={"gst_number": gst_num}))
        n += 1
        docs.append(_build_edge_doc(f"edge_{n}", new_name, amt, d2,
                                     "vendor_name_change",
                                     raw_extra={"gst_number": gst_num}))
        n += 1

    # 3. Split payments across 3 methods (20)
    for i in range(20):
        total = round(_rng.uniform(1000, 10000), 2)
        v = f"Vendeur Split {i+1}"
        dt = _random_date()
        splits = [round(total * 0.4, 2), round(total * 0.35, 2)]
        splits.append(round(total - sum(splits), 2))
        methods = ["carte_credit", "cheque", "virement"]
        for j, (split_amt, method) in enumerate(zip(splits, methods)):
            docs.append(_build_edge_doc(
                f"edge_{n}", v, split_amt, dt, "split_payment_methods",
                memo=f"Paiement {j+1}/3 par {method}"))
            n += 1

    # 4. Foreign currency (20)
    currencies = [("USD", 1.37), ("EUR", 1.48)]
    for i in range(20):
        curr, rate = _rng.choice(currencies)
        foreign_amt = round(_rng.uniform(200, 5000), 2)
        cad_amt = round(foreign_amt * rate, 2)
        v = f"Foreign Vendor {curr} {i+1}"
        dt = _random_date()
        docs.append(_build_edge_doc(
            f"edge_{n}", v, cad_amt, dt, "foreign_currency",
            raw_extra={"original_currency": curr, "original_amount": foreign_amt,
                       "exchange_rate": rate}))
        n += 1

    # 5. Recurring stop-restart (20)
    for i in range(20):
        v = f"Abonnement Service {i+1}"
        amt = round(_rng.uniform(50, 500), 2)
        # 3 months, gap of 2, then 2 more
        for month in [1, 2, 3, 6, 7]:
            dt = date(2025, month, 15)
            docs.append(_build_edge_doc(f"edge_{n}", v, amt, dt,
                                         "recurring_stop_restart"))
            n += 1

    # 6. Intercompany (20)
    for i in range(20):
        company_a = f"Entreprise Groupe {i+1}A Inc"
        company_b = f"Entreprise Groupe {i+1}B Inc"
        amt = round(_rng.uniform(5000, 50000), 2)
        dt = _random_date()
        docs.append(_build_edge_doc(
            f"edge_{n}", company_b, amt, dt, "intercompany",
            gl_account="Intercompany-receivable",
            raw_extra={"related_party": True, "paying_entity": company_a}))
        n += 1

    # 7. Advance payments (20)
    for i in range(20):
        v = f"Fournisseur Avance {i+1}"
        deposit = round(_rng.uniform(1000, 10000), 2)
        dt = _random_date(date(2025, 1, 1), date(2025, 6, 30))
        docs.append(_build_edge_doc(
            f"edge_{n}", v, deposit, dt, "advance_payment",
            gl_account="Deposits-prepaid",
            memo="Dépôt avant réception facture"))
        n += 1

    # 8. Credit card chargebacks (20)
    for i in range(20):
        v = f"Marchand Chargeback {i+1}"
        amt = round(_rng.uniform(100, 3000), 2)
        charge_dt = _random_date(date(2025, 1, 1), date(2025, 8, 31))
        chargeback_dt = charge_dt + timedelta(days=_rng.randint(15, 60))
        docs.append(_build_edge_doc(f"edge_{n}", v, amt, charge_dt,
                                     "chargeback", memo="Original charge"))
        n += 1
        docs.append(_build_edge_doc(f"edge_{n}", v, -amt, chargeback_dt,
                                     "chargeback", memo="Chargeback reversal"))
        n += 1

    # 9. HST vs GST+QST confusion (20)
    for i in range(20):
        v = f"Ontario Vendor {i+1} Ltd"
        amt = round(_rng.uniform(500, 5000), 2)
        dt = _random_date()
        hst_amt = round(amt * 0.13, 2)
        docs.append(_build_edge_doc(
            f"edge_{n}", v, amt, dt, "hst_gst_qst_confusion",
            raw_extra={"tax_charged": "HST", "hst_amount": hst_amt,
                       "province_vendor": "ON", "province_buyer": "QC"}))
        n += 1

    # 10. Cash transactions no GST number (20)
    for i in range(20):
        v = f"Petit Commerce {i+1}"
        amt = round(_rng.uniform(10, 500), 2)
        dt = _random_date()
        docs.append(_build_edge_doc(
            f"edge_{n}", v, amt, dt, "cash_no_gst",
            raw_extra={"payment_method": "cash", "gst_number": None}))
        n += 1

    # 11. Employee expense reimbursements (20)
    for i in range(20):
        emp = f"Employé {_rng.choice(['Tremblay','Gagnon','Côté','Roy','Bouchard'])} {i+1}"
        amt = round(_rng.uniform(50, 2000), 2)
        dt = _random_date()
        docs.append(_build_edge_doc(
            f"edge_{n}", emp, amt, dt, "employee_reimbursement",
            gl_account="Employee-advances",
            raw_extra={"reimbursement_type": "mixed_business_personal",
                       "business_portion": round(_rng.uniform(0.5, 1.0), 2)}))
        n += 1

    # 12. Capital vs expense boundary (20)
    for i in range(20):
        v = _rng.choice(["Home Depot Pro", "Rona Pro", "Canadian Tire"])
        # Amounts near $500 threshold
        amt = round(_rng.uniform(400, 600), 2)
        dt = _random_date()
        is_improvement = _rng.choice([True, False])
        docs.append(_build_edge_doc(
            f"edge_{n}", v, amt, dt, "capital_vs_expense",
            gl_account="Repairs" if not is_improvement else "Capital-improvements",
            memo="Repair" if not is_improvement else "Improvement to existing asset"))
        n += 1

    # 13. Prepaid expenses spanning multiple periods (20)
    for i in range(20):
        v = f"Fournisseur Prépayé {i+1}"
        total = round(_rng.uniform(1200, 12000), 2)
        months = _rng.choice([6, 12])
        dt = _random_date(date(2025, 1, 1), date(2025, 6, 30))
        docs.append(_build_edge_doc(
            f"edge_{n}", v, total, dt, "prepaid_multiperiod",
            gl_account="Prepaid-expenses",
            raw_extra={"coverage_months": months,
                       "monthly_amount": round(total / months, 2)}))
        n += 1

    # 14. Accrued expenses (20)
    for i in range(20):
        v = f"Fournisseur Couru {i+1}"
        amt = round(_rng.uniform(500, 8000), 2)
        dt = _random_date()
        docs.append(_build_edge_doc(
            f"edge_{n}", v, amt, dt, "accrued_expense",
            gl_account="Accrued-liabilities",
            memo="Service reçu — facture non encore reçue"))
        n += 1

    # 15. Truly unusual (10)
    unusual_types = [
        ("barter", "Échange Peinture Voisin", 400.0, "Barter: BBQ échangé contre services"),
        ("barter", "Troc Services Marketing", 1500.0, "Échange de services: web design contre consultation"),
        ("crypto", "CoinPayments Gateway", 2500.0, "0.025 BTC reçu comme paiement"),
        ("crypto", "BitPay Invoice #4821", 890.0, "0.0089 BTC — converted at day rate"),
        ("crypto", "Ethereum Payment Service", 3200.0, "1.2 ETH payment for consulting"),
        ("forgiven_loan", "Ex-partenaire Dubois", 5000.0, "Prêt de 5000$ radié — non recouvrable"),
        ("forgiven_loan", "Client en faillite Gagnon", 8500.0, "Créance irrécouvrable — mise en faillite"),
        ("barter", "Ferme Bio St-Laurent", 300.0, "Échange: 2 jours travail contre produits fermiers"),
        ("crypto", "Lightning Network Payment", 150.0, "Micropaiement via Lightning Network"),
        ("forgiven_loan", "Ancien employé Martin", 2000.0, "Avance salariale non remboursée — radiation"),
    ]
    for utype, vendor, amt, memo in unusual_types:
        dt = _random_date()
        docs.append(_build_edge_doc(
            f"edge_{n}", vendor, amt, dt, f"unusual_{utype}",
            tax_code="E", memo=memo))
        n += 1

    # Insert all into DB
    with _open_db(db_path) as conn:
        _ensure_documents_table(conn)
        conn.executemany(_INSERT_SQL, docs)
        conn.commit()

    print(f"[edge_cases] {len(docs)} edge case documents -> DB ({db_path.name})")
    return docs


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATOR 5 — Volume scalability test (utility bills)
# ═══════════════════════════════════════════════════════════════════════════════

_QC_MUNICIPALITIES = [
    "Montréal", "Québec", "Laval", "Gatineau", "Longueuil", "Sherbrooke",
    "Saguenay", "Lévis", "Trois-Rivières", "Terrebonne", "Saint-Jean-sur-Richelieu",
    "Repentigny", "Brossard", "Drummondville", "Saint-Jérôme", "Granby",
    "Blainville", "Shawinigan", "Dollard-des-Ormeaux", "Rimouski",
    "Châteauguay", "Saint-Hyacinthe", "Mascouche", "Victoriaville",
    "Rouyn-Noranda", "Val-d'Or", "Sorel-Tracy", "Alma", "Sainte-Julie",
    "Boucherville",
]

_QC_STREETS = [
    "rue Principale", "boul. Laurier", "av. du Parc", "ch. Sainte-Foy",
    "rue St-Jean", "boul. René-Lévesque", "rue de la Montagne",
    "boul. Henri-Bourassa", "rue Notre-Dame", "av. Cartier",
    "rue des Érables", "boul. Industriel", "rue du Commerce",
    "ch. de la Côte-des-Neiges", "rue Sherbrooke",
]


def generate_utility_bills() -> Path:
    """Generate 5,000 Quebec utility bills using Faker."""
    from faker import Faker
    fake = Faker("fr_CA")
    Faker.seed(SEED)

    out_dir = TRAINING_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "utility_bills.json"

    bills: list[dict[str, Any]] = []

    for i in range(5000):
        month = (i % 12) + 1
        year = 2025
        is_commercial = _rng.random() < 0.35
        account_type = "commercial" if is_commercial else "residential"
        municipality = _rng.choice(_QC_MUNICIPALITIES)
        street = _rng.choice(_QC_STREETS)
        civic = _rng.randint(1, 9999)
        address = f"{civic} {street}, {municipality}, QC"
        postal = fake.postcode()

        # Determine provider
        is_hydro = _rng.random() < 0.6  # 60% Hydro, 40% Énergir
        provider = "Hydro-Québec" if is_hydro else "Énergir"

        if is_hydro:
            # Seasonal: higher in winter
            if month in (12, 1, 2, 3):
                base = _rng.uniform(150, 400) if not is_commercial else _rng.uniform(300, 1200)
            elif month in (6, 7, 8):
                base = _rng.uniform(60, 120) if not is_commercial else _rng.uniform(120, 400)
            else:
                base = _rng.uniform(90, 200) if not is_commercial else _rng.uniform(200, 600)
        else:
            # Gas: high winter, near zero summer
            if month in (12, 1, 2, 3):
                base = _rng.uniform(80, 300) if not is_commercial else _rng.uniform(200, 800)
            elif month in (6, 7, 8):
                base = _rng.uniform(5, 25) if not is_commercial else _rng.uniform(10, 50)
            else:
                base = _rng.uniform(30, 100) if not is_commercial else _rng.uniform(60, 250)

        base = round(base, 2)

        # Late fees (8%)
        has_late_fee = _rng.random() < 0.08
        late_fee = round(_rng.uniform(15, 45), 2) if has_late_fee else 0.0

        subtotal = round(base + late_fee, 2)
        gst = round(subtotal * 0.05, 2)
        qst = round(subtotal * 0.09975, 2)
        total = round(subtotal + gst + qst, 2)

        # Account number format
        if is_hydro:
            acct = f"HQ-{_rng.randint(100000, 999999)}-{_rng.randint(10, 99)}"
        else:
            acct = f"EN-{_rng.randint(1000000, 9999999)}"

        # Language
        is_bilingual = _rng.random() < 0.40
        lang = "fr/en" if is_bilingual else "fr"

        bill_date = date(year, month, _rng.randint(1, 28))
        due_date = bill_date + timedelta(days=21)

        bill: dict[str, Any] = {
            "bill_id": f"UTIL-{i+1:05d}",
            "provider": provider,
            "account_number": acct,
            "account_type": account_type,
            "bill_date": bill_date.isoformat(),
            "due_date": due_date.isoformat(),
            "billing_period": f"{date(year, month, 1).isoformat()} to {date(year, month, 28).isoformat()}",
            "address": address,
            "postal_code": postal,
            "municipality": municipality,
            "language": lang,
            "base_amount": base,
        }
        if has_late_fee:
            bill["late_fee"] = late_fee
        bill.update({
            "subtotal": subtotal,
            "gst": gst,
            "qst": qst,
            "total": total,
            "currency": "CAD",
        })
        bills.append(bill)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(bills, f, indent=1, ensure_ascii=False)

    print(f"[utility_bills] {len(bills)} bills -> {out_path}")
    return out_path


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def validate_forensic_traps(csv_path: Path, *, db_path: Path = DB_PATH) -> dict[str, Any]:
    """Insert forensic traps into DB and run fraud_engine. Return detection stats."""
    # Read CSV
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    anomaly_rows = [r for r in rows if r.get("anomaly_type")]
    total_anomalies = len(anomaly_rows)

    # Insert into DB for fraud_engine
    with _open_db(db_path) as conn:
        _ensure_documents_table(conn)
        now = _utcnow()
        for i, row in enumerate(rows):
            doc_id = f"forensic_{i:05d}"
            amt = float(row["Amount"])
            bd = _breakdown(_d(str(abs(amt))), "T")
            conn.execute(
                _INSERT_SQL,
                {
                    "document_id": doc_id,
                    "file_name": f"{doc_id}.csv",
                    "file_path": f"/training/forensic/{doc_id}",
                    "client_code": "FORTIER",
                    "vendor": row["Vendor"],
                    "doc_type": "invoice",
                    "amount": amt,
                    "document_date": row["Date"],
                    "gl_account": row["Category"],
                    "tax_code": "T",
                    "category": "expense",
                    "review_status": "ReadyToPost",
                    "confidence": 0.95,
                    "raw_result": json.dumps({"vendor": row["Vendor"], "total": amt,
                                              "subtotal": bd["subtotal"],
                                              "gst_amount": float(row["GST"]),
                                              "qst_amount": float(row["QST"])},
                                             ensure_ascii=False),
                    "created_at": now,
                    "updated_at": now,
                    "currency": "CAD",
                    "subtotal": bd["subtotal"],
                    "tax_total": bd["tax_total"],
                    "extraction_method": "generated_forensic",
                    "ingest_source": "forensic_traps",
                    "fraud_flags": "[]",
                    "edge_case_type": row.get("anomaly_type", ""),
                    "handwriting_low_confidence": 0,
                    "handwriting_sample": 0,
                },
            )
        conn.commit()

    # Run fraud_engine on each
    flagged_anomalies = 0
    total_flags = 0
    for i, row in enumerate(rows):
        doc_id = f"forensic_{i:05d}"
        flags = run_fraud_detection(doc_id, db_path=db_path)
        if flags and row.get("anomaly_type"):
            flagged_anomalies += 1
        total_flags += len(flags)

    detection_rate = flagged_anomalies / total_anomalies if total_anomalies else 0
    result = {
        "total_rows": len(rows),
        "total_anomalies": total_anomalies,
        "anomalies_flagged": flagged_anomalies,
        "detection_rate": round(detection_rate, 4),
        "total_fraud_flags": total_flags,
    }
    print(f"[validate_forensic] Detection rate: {flagged_anomalies}/{total_anomalies} "
          f"({detection_rate:.1%})")
    return result


def validate_edge_cases(edge_docs: list[dict[str, Any]]) -> dict[str, Any]:
    """Run hallucination_guard on edge case documents."""
    flagged = 0
    total = len(edge_docs)
    flagged_types: dict[str, int] = {}

    for doc in edge_docs:
        raw = json.loads(doc["raw_result"]) if isinstance(doc["raw_result"], str) else doc["raw_result"]
        # Build result dict for hallucination guard
        result = {
            "vendor_name": doc["vendor"],
            "vendor": doc["vendor"],
            "total": doc["amount"],
            "amount": doc["amount"],
            "document_date": doc["document_date"],
            "gl_account": doc["gl_account"],
            "tax_code": doc["tax_code"],
            "confidence": doc["confidence"],
            "subtotal": raw.get("subtotal"),
            "taxes": [
                {"type": "GST", "amount": raw.get("gst_amount", 0)},
                {"type": "QST", "amount": raw.get("qst_amount", 0)},
            ],
        }

        guard_result = verify_ai_output(result)
        numeric_result = verify_numeric_totals(result)

        if guard_result["hallucination_suspected"] or not numeric_result.get("ok", True):
            flagged += 1
            etype = doc.get("edge_case_type", "unknown")
            flagged_types[etype] = flagged_types.get(etype, 0) + 1

    result_summary = {
        "total_edge_cases": total,
        "flagged_by_guard": flagged,
        "flag_rate": round(flagged / total, 4) if total else 0,
        "flagged_by_type": flagged_types,
    }
    print(f"[validate_edge] Flagged: {flagged}/{total} ({flagged/total:.1%})")
    return result_summary


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def generate_all(*, db_path: Path = DB_PATH, validate: bool = True) -> dict[str, Any]:
    """Run all 5 generators and optionally validate."""
    results: dict[str, Any] = {}

    # 1. Forensic traps
    csv_path = generate_forensic_traps()
    results["forensic_traps"] = {"path": str(csv_path)}

    # 2. Complex invoices
    inv_dir = generate_complex_invoices()
    results["complex_invoices"] = {"path": str(inv_dir), "count": 20}

    # 3. Messy emails
    email_dir = generate_messy_emails()
    results["messy_emails"] = {"path": str(email_dir), "count": len(_MESSY_EMAILS)}

    # 4. Edge cases
    edge_docs = generate_edge_cases(db_path=db_path)
    results["edge_cases"] = {"count": len(edge_docs)}

    # 5. Utility bills
    util_path = generate_utility_bills()
    results["utility_bills"] = {"path": str(util_path), "count": 5000}

    # Count total records
    forensic_count = len(list(csv.DictReader(open(csv_path, encoding="utf-8-sig"))))
    total = forensic_count + 20 + len(_MESSY_EMAILS) + len(edge_docs) + 5000
    results["total_training_records"] = total
    print(f"\n{'='*60}")
    print(f"Total training records generated: {total}")

    # Validation
    if validate:
        print(f"\n{'-'*60}")
        print("Running validation...")
        results["forensic_validation"] = validate_forensic_traps(csv_path, db_path=db_path)
        results["edge_case_validation"] = validate_edge_cases(edge_docs)
        print(f"{'='*60}")

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Advanced training data generators")
    parser.add_argument("--only", choices=[
        "forensic_traps", "complex_invoices", "messy_emails",
        "edge_cases", "utility_bills",
    ])
    parser.add_argument("--validate", action="store_true",
                        help="Run fraud_engine and hallucination_guard validation")
    parser.add_argument("--no-validate", action="store_true",
                        help="Skip validation even in full run")
    args = parser.parse_args()

    if args.only:
        gen_map = {
            "forensic_traps": generate_forensic_traps,
            "complex_invoices": generate_complex_invoices,
            "messy_emails": generate_messy_emails,
            "edge_cases": generate_edge_cases,
            "utility_bills": generate_utility_bills,
        }
        gen_map[args.only]()
    else:
        generate_all(validate=not args.no_validate)
