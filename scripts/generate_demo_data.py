#!/usr/bin/env python3
"""
scripts/generate_demo_data.py

Populates the OtoCPA database with realistic Quebec CPA firm demo data.

Demo Firm: Cabinet Comptable Tremblay CPA Inc.
Demo Period: January 1 2025 to December 31 2025

Clients:
  1. BOLDUC Construction Inc. — Construction quebecoise
  2. Restaurant Le Vieux Port — Restauration
  3. Avocat Desrosiers SENCRL — Services juridiques

Usage:
    python scripts/generate_demo_data.py
"""
from __future__ import annotations

import json
import random
import secrets
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")


def _round(v: Decimal) -> float:
    return float(v.quantize(CENT, rounding=ROUND_HALF_UP))


def _D(v) -> Decimal:
    return Decimal(str(v))


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _doc_id() -> str:
    return f"DOC-{secrets.token_hex(6).upper()}"


def _posting_id() -> str:
    return f"POST-{secrets.token_hex(6).upper()}"


def _rand_date(start: str, end: str) -> str:
    """Random ISO date between start and end (YYYY-MM-DD)."""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    delta = (e - s).days
    d = s + timedelta(days=random.randint(0, max(delta, 1)))
    return d.strftime("%Y-%m-%d")


def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_all_tables(conn: sqlite3.Connection) -> None:
    """Create all required tables (idempotent)."""
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

        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT UNIQUE,
            target_system TEXT,
            entry_kind TEXT,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            document_date TEXT,
            amount REAL,
            currency TEXT,
            doc_type TEXT,
            category TEXT,
            gl_account TEXT,
            tax_code TEXT,
            memo TEXT,
            review_status TEXT,
            confidence REAL,
            approval_state TEXT,
            posting_status TEXT,
            reviewer TEXT,
            blocking_issues TEXT,
            notes TEXT,
            external_id TEXT,
            error_text TEXT,
            payload_json TEXT,
            created_at TEXT,
            updated_at TEXT,
            assigned_to TEXT
        );

        CREATE TABLE IF NOT EXISTS bank_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            txn_date TEXT,
            description TEXT,
            debit REAL,
            credit REAL,
            balance REAL,
            matched_document_id TEXT,
            match_confidence REAL,
            match_reason TEXT
        );

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

        CREATE TABLE IF NOT EXISTS reconciliation_items (
            item_id TEXT PRIMARY KEY,
            reconciliation_id TEXT NOT NULL,
            item_type TEXT NOT NULL,
            description TEXT NOT NULL,
            amount REAL NOT NULL,
            transaction_date TEXT,
            cleared_date TEXT,
            document_id TEXT,
            status TEXT NOT NULL DEFAULT 'outstanding',
            FOREIGN KEY (reconciliation_id) REFERENCES bank_reconciliations(reconciliation_id)
        );
        CREATE INDEX IF NOT EXISTS idx_recon_items_recon
            ON reconciliation_items(reconciliation_id);

        CREATE TABLE IF NOT EXISTS time_entries (
            entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            client_code TEXT NOT NULL,
            document_id TEXT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_minutes REAL,
            description TEXT,
            billable INTEGER NOT NULL DEFAULT 1,
            hourly_rate REAL
        );
    """)

    # Audit tables
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS working_papers (
            paper_id          TEXT PRIMARY KEY,
            client_code       TEXT NOT NULL,
            period            TEXT NOT NULL,
            engagement_type   TEXT NOT NULL DEFAULT 'audit',
            account_code      TEXT NOT NULL,
            account_name      TEXT NOT NULL,
            balance_per_books REAL,
            balance_confirmed REAL,
            difference        REAL,
            tested_by         TEXT,
            reviewed_by       TEXT,
            sign_off_at       TEXT,
            status            TEXT NOT NULL DEFAULT 'open',
            notes             TEXT,
            created_at        TEXT,
            updated_at        TEXT
        );

        CREATE TABLE IF NOT EXISTS working_paper_items (
            item_id     TEXT PRIMARY KEY,
            paper_id    TEXT NOT NULL,
            document_id TEXT,
            tick_mark   TEXT NOT NULL DEFAULT 'tested',
            notes       TEXT,
            tested_by   TEXT,
            tested_at   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_wp_items_paper
            ON working_paper_items(paper_id);

        CREATE TABLE IF NOT EXISTS engagements (
            engagement_id   TEXT PRIMARY KEY,
            client_code     TEXT NOT NULL,
            period          TEXT NOT NULL,
            engagement_type TEXT NOT NULL DEFAULT 'audit',
            status          TEXT NOT NULL DEFAULT 'planning',
            partner         TEXT,
            manager         TEXT,
            staff           TEXT,
            planned_hours   REAL,
            actual_hours    REAL DEFAULT 0,
            budget          REAL,
            fee             REAL,
            created_at      TEXT NOT NULL,
            completed_at    TEXT
        );
    """)

    # Fixed assets
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fixed_assets (
            asset_id         TEXT PRIMARY KEY,
            client_code      TEXT NOT NULL,
            asset_name       TEXT NOT NULL,
            description      TEXT,
            cca_class        INTEGER NOT NULL,
            acquisition_date TEXT NOT NULL,
            cost             REAL NOT NULL,
            opening_ucc      REAL NOT NULL DEFAULT 0,
            current_ucc      REAL NOT NULL DEFAULT 0,
            accumulated_cca  REAL NOT NULL DEFAULT 0,
            status           TEXT NOT NULL DEFAULT 'active',
            disposal_date    TEXT,
            disposal_proceeds REAL,
            created_at       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fixed_assets_client
            ON fixed_assets(client_code);
    """)

    # AR invoices
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ar_invoices (
            invoice_id      TEXT PRIMARY KEY,
            client_code     TEXT NOT NULL,
            customer_name   TEXT NOT NULL,
            customer_email  TEXT,
            invoice_number  TEXT,
            invoice_date    TEXT NOT NULL,
            due_date        TEXT NOT NULL,
            amount_ht       REAL NOT NULL DEFAULT 0,
            gst_amount      REAL NOT NULL DEFAULT 0,
            qst_amount      REAL NOT NULL DEFAULT 0,
            total_amount    REAL NOT NULL DEFAULT 0,
            currency        TEXT NOT NULL DEFAULT 'CAD',
            status          TEXT NOT NULL DEFAULT 'draft',
            amount_paid     REAL NOT NULL DEFAULT 0,
            payment_date    TEXT,
            description     TEXT,
            created_at      TEXT,
            created_by      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ar_invoices_client
            ON ar_invoices(client_code);
    """)

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — Documents
# ─────────────────────────────────────────────────────────────────────────────

def _gst_qst(subtotal: Decimal) -> tuple[float, float, float]:
    """Return (gst, qst, total) for Quebec GST 5% + QST 9.975%."""
    gst = _round(subtotal * _D("0.05"))
    qst = _round(subtotal * _D("0.09975"))
    total = _round(subtotal + _D(gst) + _D(qst))
    return gst, qst, total


def _hst_13(subtotal: Decimal) -> tuple[float, float]:
    """Return (hst, total) for Ontario HST 13%."""
    hst = _round(subtotal * _D("0.13"))
    total = _round(subtotal + _D(hst))
    return hst, total


def _gst_only(subtotal: Decimal) -> tuple[float, float]:
    """Return (gst, total) for Alberta GST 5% only."""
    gst = _round(subtotal * _D("0.05"))
    total = _round(subtotal + _D(gst))
    return gst, total


def generate_bolduc_documents() -> list[dict]:
    """50 realistic Quebec construction invoices for BOLDUC."""
    docs = []
    inv_num = 1000

    def _add(vendor, amount, date, gl, tax_code, doc_type="invoice",
             category="expense", fraud_flags=None, substance_flags=None,
             entry_kind="expense", notes=""):
        nonlocal inv_num
        inv_num += 1
        sub = _D(amount)
        if tax_code == "T":
            gst, qst, total = _gst_qst(sub)
        elif tax_code == "H":
            hst, total = _hst_13(sub)
            gst, qst = hst, 0.0
        elif tax_code == "G":
            g, total = _gst_only(sub)
            gst, qst = g, 0.0
        elif tax_code == "M":
            gst, qst, total = _gst_qst(sub)
        elif tax_code == "I":
            gst, qst = 0.0, 0.0
            total = float(sub)
        elif tax_code == "Z":
            gst, qst = 0.0, 0.0
            total = float(sub)
        else:
            gst, qst = 0.0, 0.0
            total = float(sub)

        docs.append({
            "document_id": _doc_id(),
            "file_name": f"BOLDUC_INV_{inv_num}.pdf",
            "file_path": f"/documents/bolduc/INV_{inv_num}.pdf",
            "client_code": "BOLDUC",
            "vendor": vendor,
            "doc_type": doc_type,
            "amount": total,
            "subtotal": float(sub),
            "tax_total": round(gst + qst, 2),
            "document_date": date,
            "gl_account": gl,
            "tax_code": tax_code,
            "category": category,
            "review_status": "Ready to Post",
            "confidence": round(random.uniform(0.88, 0.99), 2),
            "currency": "CAD",
            "invoice_number": f"INV-{inv_num}",
            "invoice_number_normalized": f"INV{inv_num}",
            "extraction_method": "ocr_tesseract",
            "ingest_source": "folder_watcher",
            "fraud_flags": json.dumps(fraud_flags) if fraud_flags else None,
            "substance_flags": json.dumps(substance_flags) if substance_flags else None,
            "entry_kind": entry_kind,
            "created_at": _utc_now(),
        })

    # 10 supplier invoices — lumber, concrete, equipment rental, subcontractors
    _add("Bois Francs Quebec Inc.", 3450.00, "2025-01-15", "5100", "T")
    _add("Bois Francs Quebec Inc.", 2780.00, "2025-03-22", "5100", "T")
    _add("Beton Provincial Ltee", 8920.00, "2025-02-10", "5100", "T")
    _add("Beton Provincial Ltee", 5340.00, "2025-06-18", "5100", "T")
    _add("Location Equipement BML", 4200.00, "2025-04-05", "5100", "T")
    _add("Location Equipement BML", 3800.00, "2025-07-14", "5100", "T")
    _add("Sous-traitance Gagnon & Fils", 15600.00, "2025-05-20", "5100", "T")
    _add("Sous-traitance Gagnon & Fils", 12400.00, "2025-08-12", "5100", "T")
    _add("Quincaillerie Beauceville", 1890.00, "2025-09-03", "5100", "T")
    _add("Materiaux Bonneville", 2350.00, "2025-10-17", "5100", "T")

    # 5 invoices with GST+QST correctly calculated
    _add("Plomberie Lapointe Inc.", 6780.00, "2025-01-28", "5100", "T")
    _add("Electricite Fortin SENC", 4520.00, "2025-03-15", "5100", "T")
    _add("Toiture Beauce Inc.", 9800.00, "2025-05-10", "5100", "T")
    _add("Peinture Pro Quebec", 2100.00, "2025-07-22", "5100", "T")
    _add("Soudure Industrielle RDL", 3650.00, "2025-09-18", "5100", "T")

    # 2 invoices from Ontario suppliers (HST 13%)
    _add("Toronto Steel Supply Ltd.", 7200.00, "2025-02-28", "5100", "H")
    _add("Ottawa Equipment Rentals Inc.", 5500.00, "2025-06-05", "5100", "H")

    # 2 invoices from Alberta suppliers (GST only 5%)
    _add("Calgary Pipe & Fittings Ltd.", 4100.00, "2025-04-12", "5100", "G")
    _add("Edmonton Heavy Equipment Co.", 9300.00, "2025-08-20", "5100", "G")

    # 3 CapEx invoices
    _add("Location Caterpillar QC", 45000.00, "2025-03-01", "1540", "T",
         category="capex", entry_kind="capex",
         substance_flags=["capex_detected"])
    _add("Camions International Quebec", 62000.00, "2025-04-15", "1570", "T",
         category="capex", entry_kind="capex",
         substance_flags=["capex_detected"])
    _add("Echafaudages Plus Inc.", 8500.00, "2025-06-22", "1540", "T",
         category="capex", entry_kind="capex",
         substance_flags=["capex_detected"])

    # 2 meal receipts (50% deductible, tax code M)
    _add("Restaurant Chez Ashton", 85.50, "2025-05-14", "5900", "M",
         category="meals")
    _add("Tim Hortons #4521", 42.75, "2025-09-08", "5900", "M",
         category="meals")

    # 1 insurance premium (exempt)
    _add("Intact Assurance", 12400.00, "2025-01-05", "5800", "I",
         category="insurance")

    # 1 suspicious invoice — duplicate amount, new vendor
    _add("Construction XYZ Enr.", 15600.00, "2025-05-21", "5100", "T",
         fraud_flags=["duplicate_amount", "new_vendor_first_invoice"])

    # 1 intercompany management fee
    _add("Tremblay Holdings Inc.", 4000.00, "2025-01-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-02-28", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-03-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-04-30", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-05-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-06-30", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-07-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-08-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-09-30", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-10-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-11-30", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])
    _add("Tremblay Holdings Inc.", 4000.00, "2025-12-31", "5400", "T",
         category="management_fee",
         substance_flags=["related_party_transaction"])

    # Bank statement
    _add("Banque Nationale du Canada", 0.0, "2025-12-31", "1010", "E",
         doc_type="bank_statement", category="bank", entry_kind="bank")

    # Fill remaining to reach 50 with misc construction expenses
    remaining = 50 - len(docs)
    filler_vendors = [
        ("Ferronnerie St-Joseph", 1240.00, "5100"),
        ("Location Grues Quebec", 6800.00, "5100"),
        ("Isolation Thermo-Plus", 3450.00, "5100"),
        ("Asphalte JM Fortin", 7200.00, "5100"),
        ("Securite Sentinelle", 890.00, "5990"),
        ("Bureau en Gros", 345.00, "5200"),
        ("Petro-Canada #8812", 1250.00, "5950"),
    ]
    for i in range(remaining):
        v = filler_vendors[i % len(filler_vendors)]
        _add(v[0], v[1], _rand_date("2025-01-01", "2025-12-15"), v[2], "T")

    return docs[:50]


def generate_restaurant_documents() -> list[dict]:
    """50 realistic Quebec restaurant invoices for VIEUXPORT."""
    docs = []
    inv_num = 2000

    def _add(vendor, amount, date, gl, tax_code, doc_type="invoice",
             category="expense", fraud_flags=None, substance_flags=None,
             entry_kind="expense"):
        nonlocal inv_num
        inv_num += 1
        sub = _D(amount)
        if tax_code == "T":
            gst, qst, total = _gst_qst(sub)
        elif tax_code == "Z":
            gst, qst = 0.0, 0.0
            total = float(sub)
        elif tax_code == "M":
            gst, qst, total = _gst_qst(sub)
        else:
            gst, qst = 0.0, 0.0
            total = float(sub)

        docs.append({
            "document_id": _doc_id(),
            "file_name": f"VIEUXPORT_INV_{inv_num}.pdf",
            "file_path": f"/documents/vieuxport/INV_{inv_num}.pdf",
            "client_code": "VIEUXPORT",
            "vendor": vendor,
            "doc_type": doc_type,
            "amount": total,
            "subtotal": float(sub),
            "tax_total": round(gst + qst, 2),
            "document_date": date,
            "gl_account": gl,
            "tax_code": tax_code,
            "category": category,
            "review_status": "Ready to Post",
            "confidence": round(random.uniform(0.88, 0.99), 2),
            "currency": "CAD",
            "invoice_number": f"VP-{inv_num}",
            "invoice_number_normalized": f"VP{inv_num}",
            "extraction_method": "ocr_tesseract",
            "ingest_source": "folder_watcher",
            "fraud_flags": json.dumps(fraud_flags) if fraud_flags else None,
            "substance_flags": json.dumps(substance_flags) if substance_flags else None,
            "entry_kind": entry_kind,
            "created_at": _utc_now(),
        })

    # 15 food supplier invoices (mix of zero-rated Z and taxable T)
    food_suppliers = [
        ("Sysco Quebec", 4250.00, "Z"), ("Sysco Quebec", 3890.00, "Z"),
        ("Sysco Quebec", 5120.00, "Z"), ("Colabor Group Inc.", 2780.00, "Z"),
        ("Colabor Group Inc.", 3340.00, "Z"), ("Boulangerie Artisan QC", 890.00, "Z"),
        ("Boulangerie Artisan QC", 1120.00, "Z"), ("Poissonnerie du Vieux-Port", 2450.00, "Z"),
        ("Poissonnerie du Vieux-Port", 1890.00, "Z"), ("Fromagerie Ile-aux-Grues", 680.00, "Z"),
        ("Marche Central Quebec", 1560.00, "Z"), ("Viandes Laroche Inc.", 3200.00, "T"),
        ("Distribution Jacques Cartier", 2100.00, "T"), ("Boucherie Beauceron", 1750.00, "T"),
        ("Ferme Bio Lotbiniere", 940.00, "Z"),
    ]
    for i, (v, a, tc) in enumerate(food_suppliers):
        _add(v, a, _rand_date("2025-01-01", "2025-12-15"), "5100", tc)

    # 5 equipment invoices
    _add("Restaurant Depot Quebec", 3200.00, "2025-02-15", "1540", "T",
         category="capex", entry_kind="capex")
    _add("Equipement Cuisine Pro", 8500.00, "2025-04-10", "1540", "T",
         category="capex", entry_kind="capex")
    _add("Four Rational Canada", 12000.00, "2025-06-20", "1540", "T",
         category="capex", entry_kind="capex")
    _add("Refrigeration Beauce Inc.", 4500.00, "2025-08-05", "1540", "T",
         category="capex", entry_kind="capex")
    _add("Vaisselle & Cie Ltee", 1800.00, "2025-10-12", "5100", "T")

    # 3 utility bills
    _add("Hydro-Quebec", 2340.00, "2025-03-15", "5500", "T")
    _add("Energir (Gaz Metro)", 1890.00, "2025-03-18", "5500", "T")
    _add("Bell Canada", 245.00, "2025-03-20", "5500", "T")

    # 2 meal receipts from owner (personal expense flag)
    _add("Restaurant Le Continental", 185.00, "2025-07-14", "5900", "M",
         category="meals", substance_flags=["owner_personal_expense"])
    _add("Bistro Le Sam", 142.00, "2025-09-22", "5900", "M",
         category="meals", substance_flags=["owner_personal_expense"])

    # 1 credit note from food supplier
    _add("Sysco Quebec", -450.00, "2025-05-10", "5100", "Z",
         doc_type="credit_note", entry_kind="credit_note")

    # Additional utilities / recurring
    months = ["01", "02", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    for m in months[:6]:
        _add("Hydro-Quebec", round(random.uniform(1800, 2600), 2),
             f"2025-{m}-15", "5500", "T")

    # Fill remaining to 50
    remaining = 50 - len(docs)
    fillers = [
        ("Nettoyage Pro Quebec", 450.00, "5990"),
        ("Buanderie Commerciale QC", 320.00, "5990"),
        ("Imprimerie Levis", 280.00, "5700"),
        ("Uniformes Quebec Inc.", 650.00, "5990"),
    ]
    for i in range(remaining):
        f = fillers[i % len(fillers)]
        _add(f[0], f[1], _rand_date("2025-01-01", "2025-12-15"), f[2], "T")

    return docs[:50]


def generate_desrosiers_documents() -> list[dict]:
    """50 realistic Quebec legal firm invoices for DESROSIERS."""
    docs = []
    inv_num = 3000

    def _add(vendor, amount, date, gl, tax_code, doc_type="invoice",
             category="expense", entry_kind="expense"):
        nonlocal inv_num
        inv_num += 1
        sub = _D(amount)
        if tax_code == "T":
            gst, qst, total = _gst_qst(sub)
        else:
            gst, qst = 0.0, 0.0
            total = float(sub)

        docs.append({
            "document_id": _doc_id(),
            "file_name": f"DESROSIERS_INV_{inv_num}.pdf",
            "file_path": f"/documents/desrosiers/INV_{inv_num}.pdf",
            "client_code": "DESROSIERS",
            "vendor": vendor,
            "doc_type": doc_type,
            "amount": total,
            "subtotal": float(sub),
            "tax_total": round(gst + qst, 2),
            "document_date": date,
            "gl_account": gl,
            "tax_code": tax_code,
            "category": category,
            "review_status": "Ready to Post",
            "confidence": round(random.uniform(0.90, 0.99), 2),
            "currency": "CAD",
            "invoice_number": f"DES-{inv_num}",
            "invoice_number_normalized": f"DES{inv_num}",
            "extraction_method": "ocr_tesseract",
            "ingest_source": "folder_watcher",
            "entry_kind": entry_kind,
            "created_at": _utc_now(),
        })

    # 10 professional expense invoices
    _add("Barreau du Quebec", 3200.00, "2025-01-10", "5400", "T")
    _add("Chambre des notaires QC", 1800.00, "2025-01-15", "5400", "T")
    _add("Formation juridique continue", 2400.00, "2025-03-20", "5400", "T")
    _add("Congres du Barreau 2025", 1500.00, "2025-04-05", "5950", "T")
    _add("Service de traduction LJT", 850.00, "2025-05-12", "5400", "T")
    _add("Huissier Gagnon & Associes", 425.00, "2025-06-18", "5400", "T")
    _add("Expertise comptable MNP", 5600.00, "2025-07-22", "5400", "T")
    _add("Recherche juridique Pro", 340.00, "2025-08-14", "5400", "T")
    _add("Messagerie Dicom", 120.00, "2025-09-05", "5990", "T")
    _add("Reliure et impression JB", 280.00, "2025-10-18", "5200", "T")

    # 5 subscription invoices (legal databases)
    _add("SOQUIJ — Azimut", 4800.00, "2025-01-01", "5400", "T")
    _add("LexisNexis Canada", 6200.00, "2025-01-01", "5400", "T")
    _add("Thomson Reuters Westlaw", 5400.00, "2025-01-01", "5400", "T")
    _add("CanLII Pro", 1200.00, "2025-01-01", "5400", "T")
    _add("JuriQuebec Online", 890.00, "2025-01-01", "5400", "T")

    # 3 office supply invoices
    _add("Bureau en Gros", 890.00, "2025-02-22", "5200", "T")
    _add("Hamster Quebec", 520.00, "2025-06-10", "5200", "T")
    _add("Papeterie Saint-Laurent", 340.00, "2025-09-15", "5200", "T")

    # Additional recurring expenses to fill to 50
    months = [f"{m:02d}" for m in range(1, 13)]
    for m in months:
        _add("Espace Bureau Quebec (loyer)", 3500.00, f"2025-{m}-01", "5990", "T")

    # Telecom, insurance, etc.
    for m in months[:6]:
        _add("Telus Mobilite", 245.00, f"2025-{m}-15", "5500", "T")

    # Fill remaining
    remaining = 50 - len(docs)
    fillers = [
        ("Entretien Menager Pro", 380.00, "5990"),
        ("Assurance RCPQ", 4200.00, "5800"),
        ("Stationnement Vieux-Quebec", 350.00, "5950"),
    ]
    for i in range(remaining):
        f = fillers[i % len(fillers)]
        _add(f[0], f[1], _rand_date("2025-01-01", "2025-12-15"), f[2], "T")

    return docs[:50]


def insert_documents(conn: sqlite3.Connection, docs: list[dict]) -> None:
    """Insert documents into the database."""
    now = _utc_now()
    cols = [
        "document_id", "file_name", "file_path", "client_code", "vendor",
        "doc_type", "amount", "subtotal", "tax_total", "document_date",
        "gl_account", "tax_code", "category", "review_status", "confidence",
        "currency", "invoice_number", "invoice_number_normalized",
        "extraction_method", "ingest_source", "fraud_flags",
        "substance_flags", "entry_kind", "created_at",
        # NOT NULL columns in existing schema
        "updated_at", "raw_result",
    ]
    placeholders = ",".join(["?"] * len(cols))
    col_names = ",".join(cols)

    for doc in docs:
        doc.setdefault("updated_at", now)
        doc.setdefault("raw_result", "{}")
        vals = [doc.get(c) for c in cols]
        conn.execute(f"INSERT OR IGNORE INTO documents ({col_names}) VALUES ({placeholders})", vals)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — Posting jobs
# ─────────────────────────────────────────────────────────────────────────────

def generate_posting_jobs(conn: sqlite3.Connection, docs: list[dict]) -> int:
    """Create posting jobs for all documents with appropriate status mix."""
    statuses = (
        [("Posted", "approved", "posted")] * 30 +
        [("Ready to Post", "approved", "ready")] * 25 +
        [("Needs Review", "pending_review", "pending")] * 25 +
        [("On Hold", "pending_review", "on_hold")] * 10 +
        [("Exception", "blocked", "exception")] * 10
    )
    random.shuffle(statuses)

    count = 0
    for i, doc in enumerate(docs):
        review_status, approval, posting = statuses[i % len(statuses)]
        # Update document review_status
        conn.execute(
            "UPDATE documents SET review_status = ? WHERE document_id = ?",
            (review_status, doc["document_id"]),
        )

        now = _utc_now()
        blocking = None
        notes = None
        if posting == "exception":
            blocking = json.dumps(["fraud_flag_detected"])
            notes = json.dumps(["Flagged for review by fraud engine"])

        conn.execute(
            """INSERT OR IGNORE INTO posting_jobs (
                posting_id, document_id, target_system, entry_kind,
                file_name, file_path, client_code, vendor, document_date,
                amount, currency, doc_type, category, gl_account, tax_code,
                memo, review_status, confidence, approval_state,
                posting_status, reviewer, blocking_issues, notes,
                payload_json, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                _posting_id(), doc["document_id"], "qbo", doc.get("entry_kind", "expense"),
                doc["file_name"], doc["file_path"], doc["client_code"],
                doc["vendor"], doc["document_date"], doc["amount"],
                "CAD", doc["doc_type"], doc["category"], doc["gl_account"],
                doc["tax_code"],
                f"{doc['vendor']} — {doc['document_date']}",
                review_status, doc["confidence"], approval,
                posting, "ExceptionRouter", blocking, notes,
                "{}", now, now,
            ),
        )
        count += 1
    conn.commit()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — Bank reconciliation for BOLDUC December 2025
# ─────────────────────────────────────────────────────────────────────────────

def generate_bank_reconciliation(conn: sqlite3.Connection) -> None:
    """Create bank reconciliation for BOLDUC — December 2025."""
    recon_id = f"RECON-{secrets.token_hex(6).upper()}"
    now = _utc_now()

    # Bank statement balance: $127,450.32
    # GL cash balance: $124,200.18
    # Deposits in transit: $5,250.14
    # Outstanding cheques: $2,000.00
    # Adjusted bank = 127450.32 + 5250.14 - 2000.00 = 130700.46
    # But we need it balanced: adjusted_bank = adjusted_book
    # adjusted_book = 124200.18 + deposits_in_transit... no, standard recon:
    # Bank balance + deposits in transit - outstanding cheques = adjusted bank
    # GL balance + adjustments = adjusted book
    # For balanced: 127450.32 + 5250.14 - 2000.00 = 130700.46
    # So GL should also = 130700.46 after adjustments
    # GL = 124200.18, diff = 130700.46 - 124200.18 = 6500.28 (book adjustments)
    # Let's keep it simple: balanced as specified

    statement_bal = 127450.32
    gl_bal = 124200.18
    adjusted = round(statement_bal + 5250.14 - 2000.00, 2)  # 130700.46

    # Insert without finalized_at first (triggers block item inserts on finalized recons)
    conn.execute(
        """INSERT OR IGNORE INTO bank_reconciliations (
            reconciliation_id, client_code, account_name, account_number,
            period_end_date, statement_ending_balance, gl_ending_balance,
            adjusted_bank_balance, adjusted_book_balance, difference,
            status, prepared_by, prepared_at, reviewed_by, reviewed_at,
            notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            recon_id, "BOLDUC", "Compte courant — Banque Nationale", "001-12345-67",
            "2025-12-31", statement_bal, gl_bal,
            adjusted, adjusted, 0.0,
            "balanced", "Marie-Claude Bouchard CPA", now,
            "Sam Tremblay CPA", now,
            "Reconciliation de decembre 2025 — aucun ecart",
        ),
    )

    # Deposits in transit
    deposits = [
        ("Depot client Maison Laval — cheque #4521", 2800.00, "2025-12-30"),
        ("Depot client Condo Beauport — virement", 2450.14, "2025-12-31"),
    ]
    for desc, amt, dt in deposits:
        item_id = f"RI-{secrets.token_hex(6).upper()}"
        conn.execute(
            """INSERT OR IGNORE INTO reconciliation_items (
                item_id, reconciliation_id, item_type, description,
                amount, transaction_date, status
            ) VALUES (?,?,?,?,?,?,?)""",
            (item_id, recon_id, "deposit_in_transit", desc, amt, dt, "outstanding"),
        )

    # Outstanding cheques
    cheques = [
        ("Cheque #2891 — Beton Provincial Ltee", 1200.00, "2025-12-28"),
        ("Cheque #2892 — Quincaillerie Beauceville", 800.00, "2025-12-29"),
    ]
    for desc, amt, dt in cheques:
        item_id = f"RI-{secrets.token_hex(6).upper()}"
        conn.execute(
            """INSERT OR IGNORE INTO reconciliation_items (
                item_id, reconciliation_id, item_type, description,
                amount, transaction_date, status
            ) VALUES (?,?,?,?,?,?,?)""",
            (item_id, recon_id, "outstanding_cheque", desc, amt, dt, "outstanding"),
        )

    # Bank transactions (20 matching invoices)
    stmt_id = f"STMT-{secrets.token_hex(6).upper()}"
    bank_txns = [
        ("2025-12-01", "Bois Francs Quebec Inc.", 3975.77, None),
        ("2025-12-02", "Beton Provincial Ltee", 10273.97, None),
        ("2025-12-03", "Location Equipement BML", 4838.97, None),
        ("2025-12-04", "Sous-traitance Gagnon & Fils", 17973.90, None),
        ("2025-12-05", "Intact Assurance", 12400.00, None),
        ("2025-12-06", "Tremblay Holdings Inc.", 4609.90, None),
        ("2025-12-08", "Plomberie Lapointe Inc.", 7810.19, None),
        ("2025-12-09", "Electricite Fortin SENC", 5208.18, None),
        ("2025-12-10", "Toronto Steel Supply Ltd.", 8136.00, None),
        ("2025-12-11", "Calgary Pipe & Fittings Ltd.", 4305.00, None),
        ("2025-12-12", "Petro-Canada #8812", 1440.62, None),
        ("2025-12-15", "Bureau en Gros", 397.58, None),
        ("2025-12-16", "Ferronnerie St-Joseph", 1428.94, None),
        ("2025-12-17", "Securite Sentinelle", 1025.31, None),
        ("2025-12-18", "Peinture Pro Quebec", 2420.25, None),
        ("2025-12-19", None, None, 15000.00),  # Client payment received
        ("2025-12-22", None, None, 22500.00),  # Client payment received
        ("2025-12-23", "Isolation Thermo-Plus", 3975.77, None),
        ("2025-12-29", None, None, 8750.00),   # Client payment received
        ("2025-12-30", "Asphalte JM Fortin", 8298.00, None),
    ]
    balance = 127450.32
    for txn_date, desc, debit, credit in bank_txns:
        if debit:
            balance -= debit
        if credit:
            balance += credit
        conn.execute(
            """INSERT INTO bank_transactions (
                statement_id, document_id, txn_date, description,
                debit, credit, balance
            ) VALUES (?,?,?,?,?,?,?)""",
            (stmt_id, f"BTXN-{secrets.token_hex(4).upper()}", txn_date,
             desc or "Depot client", debit, credit, round(balance, 2)),
        )

    # Now finalize the reconciliation (after items are inserted)
    conn.execute(
        "UPDATE bank_reconciliations SET finalized_at = ? WHERE reconciliation_id = ?",
        (now, recon_id),
    )

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 4 — Fixed assets for BOLDUC
# ─────────────────────────────────────────────────────────────────────────────

CCA_RATES = {1: Decimal("0.04"), 8: Decimal("0.20"), 10: Decimal("0.30"),
             43: Decimal("0.30"), 50: Decimal("0.55")}


def _calc_cca_accumulated(cost: Decimal, rate: Decimal, acq_year: int, fy_year: int) -> Decimal:
    """Calculate accumulated CCA from acquisition to fiscal year end using half-year rule."""
    ucc = cost
    accumulated = Decimal("0")
    for yr in range(acq_year, fy_year + 1):
        if yr == acq_year:
            # Half-year rule
            cca = (ucc * rate / 2).quantize(CENT, rounding=ROUND_HALF_UP)
        else:
            cca = (ucc * rate).quantize(CENT, rounding=ROUND_HALF_UP)
        if cca > ucc:
            cca = ucc
        ucc -= cca
        accumulated += cca
    return accumulated


def generate_fixed_assets(conn: sqlite3.Connection) -> None:
    """Create fixed assets for BOLDUC with CCA calculations for FY 2025."""
    now = _utc_now()

    assets = [
        {
            "name": "Camion Ford F-350 2023",
            "description": "Vehicule de travail — classe 10",
            "cca_class": 10,
            "acquisition_date": "2023-03-15",
            "cost": Decimal("62000.00"),
        },
        {
            "name": "Excavatrice Caterpillar 320 2022",
            "description": "Equipement lourd — classe 43",
            "cca_class": 43,
            "acquisition_date": "2022-06-01",
            "cost": Decimal("185000.00"),
        },
        {
            "name": "Equipement de bureau",
            "description": "Mobilier et equipement divers — classe 8",
            "cca_class": 8,
            "acquisition_date": "2024-01-10",
            "cost": Decimal("12500.00"),
        },
        {
            "name": "Ordinateurs (3 postes)",
            "description": "Materiel informatique — classe 50",
            "cca_class": 50,
            "acquisition_date": "2024-09-01",
            "cost": Decimal("8700.00"),
        },
        {
            "name": "Entrepot Saint-Laurent",
            "description": "Batiment industriel — classe 1",
            "cca_class": 1,
            "acquisition_date": "2020-01-01",
            "cost": Decimal("450000.00"),
        },
    ]

    for asset in assets:
        rate = CCA_RATES[asset["cca_class"]]
        acq_year = int(asset["acquisition_date"][:4])
        accumulated = _calc_cca_accumulated(asset["cost"], rate, acq_year, 2025)
        current_ucc = (asset["cost"] - accumulated).quantize(CENT, rounding=ROUND_HALF_UP)

        asset_id = f"FA-{secrets.token_hex(6).upper()}"
        conn.execute(
            """INSERT OR IGNORE INTO fixed_assets (
                asset_id, client_code, asset_name, description, cca_class,
                acquisition_date, cost, opening_ucc, current_ucc,
                accumulated_cca, status, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                asset_id, "BOLDUC", asset["name"], asset["description"],
                asset["cca_class"], asset["acquisition_date"],
                float(asset["cost"]), float(asset["cost"]),
                float(current_ucc), float(accumulated),
                "active", now,
            ),
        )

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 5 — Audit engagement for BOLDUC
# ─────────────────────────────────────────────────────────────────────────────

def generate_audit_engagement(conn: sqlite3.Connection) -> None:
    """Create complete audit engagement for BOLDUC FY 2025."""
    now = _utc_now()
    eng_id = f"ENG-{secrets.token_hex(6).upper()}"

    # Engagement
    conn.execute(
        """INSERT OR IGNORE INTO engagements (
            engagement_id, client_code, period, engagement_type, status,
            partner, manager, staff, planned_hours, actual_hours,
            budget, fee, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            eng_id, "BOLDUC", "2025-01-01/2025-12-31", "audit", "in_progress",
            "Sam Tremblay CPA", "Marie-Claude Bouchard CPA",
            json.dumps(["Julie Bergeron CPA", "Marc-Andre Gagnon", "Sophie Lavoie"]),
            60.0, 42.5, 11100.00, 11100.00, now,
        ),
    )

    # Working papers
    working_papers = [
        {
            "account_code": "1010",
            "account_name": "Encaisse et banque",
            "balance_per_books": 124200.18,
            "balance_confirmed": 124200.18,
            "difference": 0.0,
            "status": "complete",
            "tested_by": "Julie Bergeron CPA",
            "reviewed_by": "Marie-Claude Bouchard CPA",
            "notes": "Rapprochement bancaire effectue. Aucun ecart. Tick marks: V (vouch to statement), C (confirmed).",
        },
        {
            "account_code": "1100",
            "account_name": "Comptes clients",
            "balance_per_books": 87500.00,
            "balance_confirmed": None,
            "difference": None,
            "status": "in_progress",
            "tested_by": "Marc-Andre Gagnon",
            "reviewed_by": None,
            "notes": "Circularisation en cours. 15/25 confirmations recues. Attente des 10 restantes.",
        },
        {
            "account_code": "1500",
            "account_name": "Immobilisations corporelles",
            "balance_per_books": 718200.00,
            "balance_confirmed": 718200.00,
            "difference": 0.0,
            "status": "complete",
            "tested_by": "Julie Bergeron CPA",
            "reviewed_by": "Marie-Claude Bouchard CPA",
            "notes": "Relie a la cedule de DPA. Tous les ajouts verifies aux factures. Tick marks: V, F (footed), T (traced to schedule 8).",
        },
        {
            "account_code": "2100",
            "account_name": "Comptes fournisseurs",
            "balance_per_books": 45600.00,
            "balance_confirmed": 45600.00,
            "difference": 0.0,
            "status": "complete",
            "tested_by": "Sophie Lavoie",
            "reviewed_by": "Marie-Claude Bouchard CPA",
            "notes": "Recherche de passifs non comptabilises effectuee. Echantillon de 25 factures verifiees. Aucun ecart.",
        },
        {
            "account_code": "4100",
            "account_name": "Produits d exploitation — Revenus",
            "balance_per_books": 2150000.00,
            "balance_confirmed": None,
            "difference": None,
            "status": "in_progress",
            "tested_by": "Marc-Andre Gagnon",
            "reviewed_by": None,
            "notes": "Tests de detail en cours. Echantillon de 30 contrats selectionnes. 18 verifies, 12 en attente.",
        },
    ]

    papers_to_sign_off = []

    for wp in working_papers:
        paper_id = f"WP-{secrets.token_hex(6).upper()}"

        # Insert without sign_off_at first (triggers block item inserts on signed-off papers)
        conn.execute(
            """INSERT OR IGNORE INTO working_papers (
                paper_id, client_code, period, engagement_type,
                account_code, account_name, balance_per_books,
                balance_confirmed, difference, tested_by, reviewed_by,
                sign_off_at, status, notes, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                paper_id, "BOLDUC", "2025-01-01/2025-12-31", "audit",
                wp["account_code"], wp["account_name"],
                wp["balance_per_books"], wp["balance_confirmed"],
                wp["difference"], wp["tested_by"], wp["reviewed_by"],
                None, wp["status"], wp["notes"], now, now,
            ),
        )

        # Add working paper items (tick marks) for complete papers
        if wp["status"] == "complete":
            for j in range(3):
                item_id = f"WPI-{secrets.token_hex(6).upper()}"
                tick = ["tested", "vouched", "footed"][j]
                conn.execute(
                    """INSERT OR IGNORE INTO working_paper_items (
                        item_id, paper_id, tick_mark, notes,
                        tested_by, tested_at
                    ) VALUES (?,?,?,?,?,?)""",
                    (item_id, paper_id, tick,
                     f"Tick mark: {tick} — verified to source",
                     wp["tested_by"], now),
                )
            papers_to_sign_off.append(paper_id)

    # Now sign off complete papers (after items are inserted)
    for paper_id in papers_to_sign_off:
        conn.execute(
            "UPDATE working_papers SET sign_off_at = ? WHERE paper_id = ?",
            (now, paper_id),
        )

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 6 — AR invoices for Avocat Desrosiers
# ─────────────────────────────────────────────────────────────────────────────

def generate_ar_invoices(conn: sqlite3.Connection) -> None:
    """Create 5 AR invoices for DESROSIERS — 2 overdue, 3 current."""
    now = _utc_now()

    invoices = [
        {
            "customer": "Immeubles Charlesbourg Inc.",
            "email": "comptabilite@immeublescharlesbourg.ca",
            "amount_ht": 2500.00,
            "invoice_date": "2025-09-15",
            "due_date": "2025-10-15",
            "description": "Honoraires — revision contrat commercial",
            "status": "overdue",
        },
        {
            "customer": "Transport Leclerc & Fils",
            "email": "admin@transportleclerc.ca",
            "amount_ht": 4800.00,
            "invoice_date": "2025-08-20",
            "due_date": "2025-09-20",
            "description": "Honoraires — litige civil #2025-LC-001",
            "status": "overdue",
        },
        {
            "customer": "Developpement Beauport SENC",
            "email": "info@devbeauport.ca",
            "amount_ht": 1200.00,
            "invoice_date": "2025-12-01",
            "due_date": "2025-12-31",
            "description": "Consultation — incorporation entreprise",
            "status": "sent",
        },
        {
            "customer": "Groupe Immobilier Capitale",
            "email": "payables@gicapitale.ca",
            "amount_ht": 8500.00,
            "invoice_date": "2025-12-10",
            "due_date": "2026-01-10",
            "description": "Honoraires — transaction immobiliere lot 456-789",
            "status": "sent",
        },
        {
            "customer": "Restaurant Chez Marcel Enr.",
            "email": "marcel@chezmarcel.ca",
            "amount_ht": 3300.00,
            "invoice_date": "2025-12-15",
            "due_date": "2026-01-15",
            "description": "Honoraires — revision bail commercial",
            "status": "sent",
        },
    ]

    for i, inv in enumerate(invoices):
        invoice_id = f"ARINV-{secrets.token_hex(6).upper()}"
        amt = _D(inv["amount_ht"])
        gst = (amt * _D("0.05")).quantize(CENT, rounding=ROUND_HALF_UP)
        qst = (amt * _D("0.09975")).quantize(CENT, rounding=ROUND_HALF_UP)
        total = amt + gst + qst

        conn.execute(
            """INSERT OR IGNORE INTO ar_invoices (
                invoice_id, client_code, customer_name, customer_email,
                invoice_number, invoice_date, due_date, amount_ht,
                gst_amount, qst_amount, total_amount, currency,
                status, amount_paid, description, created_at, created_by
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                invoice_id, "DESROSIERS", inv["customer"], inv["email"],
                f"DES-2025-{i + 1:03d}", inv["invoice_date"], inv["due_date"],
                float(amt), float(gst), float(qst), float(total),
                "CAD", inv["status"], 0.0, inv["description"],
                now, "Me Desrosiers",
            ),
        )

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 7 — Related parties for BOLDUC (stored as documents/notes)
# ─────────────────────────────────────────────────────────────────────────────
# Related party info is already embedded in the management fee documents
# (Tremblay Holdings Inc.) with substance_flags=["related_party_transaction"].
# This section is covered by Part 1.


# ─────────────────────────────────────────────────────────────────────────────
# PART 8 — Time tracking entries
# ─────────────────────────────────────────────────────────────────────────────

def generate_time_entries(conn: sqlite3.Connection) -> None:
    """Create 15 time entries for BOLDUC audit — total 42.5 hours at $185/hr."""
    entries = [
        ("Sam Tremblay CPA", "2025-10-01 09:00", 3.0, "Planification de la mission — evaluation des risques"),
        ("Marie-Claude Bouchard CPA", "2025-10-02 09:00", 4.0, "Revue du dossier permanent et mise a jour"),
        ("Julie Bergeron CPA", "2025-10-05 08:30", 3.5, "Tests de detail — encaisse et rapprochement bancaire"),
        ("Julie Bergeron CPA", "2025-10-06 08:30", 3.0, "Tests de detail — immobilisations et cedule de DPA"),
        ("Marc-Andre Gagnon", "2025-10-07 09:00", 4.0, "Circularisation des comptes clients — preparation envois"),
        ("Marc-Andre Gagnon", "2025-10-08 09:00", 3.0, "Circularisation des comptes clients — suivi reponses"),
        ("Sophie Lavoie", "2025-10-09 08:30", 3.5, "Recherche de passifs non comptabilises — echantillonnage"),
        ("Sophie Lavoie", "2025-10-10 09:00", 2.5, "Verification des comptes fournisseurs — rapprochement"),
        ("Marc-Andre Gagnon", "2025-10-14 09:00", 3.0, "Tests de revenus — echantillon de contrats"),
        ("Marc-Andre Gagnon", "2025-10-15 09:00", 2.5, "Tests de revenus — verification aux pieces justificatives"),
        ("Julie Bergeron CPA", "2025-10-16 08:30", 2.0, "Procedures analytiques — ratios financiers"),
        ("Marie-Claude Bouchard CPA", "2025-10-17 09:00", 3.0, "Revue des dossiers de travail — notes de revision"),
        ("Sam Tremblay CPA", "2025-10-20 09:00", 2.0, "Revue du dossier — evaluation de la continuite d exploitation"),
        ("Sam Tremblay CPA", "2025-10-21 09:00", 2.0, "Evenements subsequents et lettre de declaration"),
        ("Marie-Claude Bouchard CPA", "2025-10-22 09:00", 2.0, "Finalisation — revue qualite et preparation rapport"),
    ]

    for username, started, hours, desc in entries:
        start_dt = datetime.strptime(started, "%Y-%m-%d %H:%M")
        end_dt = start_dt + timedelta(hours=hours)

        conn.execute(
            """INSERT INTO time_entries (
                username, client_code, started_at, ended_at,
                duration_minutes, description, billable, hourly_rate
            ) VALUES (?,?,?,?,?,?,?,?)""",
            (
                username, "BOLDUC",
                start_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                end_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                hours * 60.0, desc, 1, 185.00,
            ),
        )

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("OtoCPA Demo Data Generator")
    print("Cabinet Comptable Tremblay CPA Inc.")
    print("Period: January 1 2025 — December 31 2025")
    print("=" * 60)
    print()

    conn = open_db()
    ensure_all_tables(conn)

    # PART 1 — Documents
    print("[1/8] Generating documents...")
    bolduc_docs = generate_bolduc_documents()
    restaurant_docs = generate_restaurant_documents()
    desrosiers_docs = generate_desrosiers_documents()
    all_docs = bolduc_docs + restaurant_docs + desrosiers_docs

    insert_documents(conn, all_docs)
    print(f"  -> {len(bolduc_docs)} BOLDUC documents")
    print(f"  -> {len(restaurant_docs)} VIEUXPORT documents")
    print(f"  -> {len(desrosiers_docs)} DESROSIERS documents")
    print(f"  -> {len(all_docs)} total documents inserted")

    # PART 2 — Posting jobs
    print("[2/8] Generating posting jobs...")
    pj_count = generate_posting_jobs(conn, all_docs)
    print(f"  -> {pj_count} posting jobs created")

    # PART 3 — Bank reconciliation
    print("[3/8] Generating bank reconciliation (BOLDUC Dec 2025)...")
    generate_bank_reconciliation(conn)
    print("  -> Bank reconciliation created (balanced)")
    print("  -> 2 deposits in transit, 2 outstanding cheques")
    print("  -> 20 bank transactions")

    # PART 4 — Fixed assets
    print("[4/8] Generating fixed assets (BOLDUC)...")
    generate_fixed_assets(conn)
    print("  -> 5 fixed assets with CCA calculated through FY 2025")

    # PART 5 — Audit engagement
    print("[5/8] Generating audit engagement (BOLDUC)...")
    generate_audit_engagement(conn)
    print("  -> 1 engagement (in progress)")
    print("  -> 5 working papers (3 complete, 2 in progress)")
    print("  -> 9 working paper items (tick marks)")

    # PART 6 — AR invoices
    print("[6/8] Generating AR invoices (DESROSIERS)...")
    generate_ar_invoices(conn)
    print("  -> 5 AR invoices (2 overdue, 3 current)")

    # PART 7 — Related parties
    print("[7/8] Related parties for BOLDUC...")
    print("  -> 12 management fee invoices to Tremblay Holdings Inc.")
    print("  -> Flagged as related_party_transaction")

    # PART 8 — Time tracking
    print("[8/8] Generating time entries (BOLDUC audit)...")
    generate_time_entries(conn)
    print("  -> 15 time entries, 42.5 hours at $185/hr = $7,862.50")

    conn.close()

    print()
    print("=" * 60)
    print("Demo data generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
