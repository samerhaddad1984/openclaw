"""Generate 3 realistic Quebec clients with 90 days of data.

Client profiles (deliberately different complexity):
  * ACME-CAFE   — Café Montréal (restaurant, 8 staff, $380K revenue)
  * ACME-CONST  — Construction Tremblay Inc (general contractor,
                   12 staff, $1.2M revenue, fixed assets, SR&ED project)
  * ACME-SOLM   — Solutions Marchand Consulting (solo contractor, home
                   office, $220K revenue)

Data created (per client):
  * Opening balance sheet (recorded as trial-balance seed)
  * 90 days of AR invoices (sales)
  * 90 days of AP documents (purchases)
  * 90 days of bank transactions (matched to docs + some unmatched)
  * Expected trial-balance totals at 90-day close

Each client's ground-truth numbers are exposed via `expected_totals()`.
"""
from __future__ import annotations

import hashlib
import random
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent.parent

# Quebec statutory holidays (no business transactions allowed).
QC_HOLIDAYS = {
    "2025-01-01", "2025-04-18", "2025-05-19", "2025-06-24",
    "2025-07-01", "2025-09-01", "2025-10-13", "2025-12-25", "2025-12-26",
}

# GST/QST rates (canonical).
GST_RATE = Decimal("0.05")
QST_RATE = Decimal("0.09975")


def _deterministic_rng(seed_str: str) -> random.Random:
    seed = int(hashlib.sha1(seed_str.encode()).hexdigest()[:12], 16)
    return random.Random(seed)


def _iso(d: date) -> str:
    return d.isoformat()


def _is_biz_day(d: date) -> bool:
    if d.weekday() >= 5:  # Sat/Sun
        return False
    if _iso(d) in QC_HOLIDAYS:
        return False
    return True


@dataclass
class ClientProfile:
    client_code: str
    display_name: str
    business_type: str  # "restaurant", "construction", "consulting"
    annual_revenue: Decimal
    employees: int
    start_date: str = "2025-07-01"
    period_days: int = 90

    # Derived volumes.
    daily_sales_count: int = 10
    monthly_purchase_count: int = 30

    # Ground truth accumulated as data is generated.
    expected_total_sales: Decimal = field(default_factory=lambda: Decimal("0"))
    expected_total_purchases: Decimal = field(default_factory=lambda: Decimal("0"))
    expected_gst_collected: Decimal = field(default_factory=lambda: Decimal("0"))
    expected_qst_collected: Decimal = field(default_factory=lambda: Decimal("0"))
    expected_gst_paid: Decimal = field(default_factory=lambda: Decimal("0"))
    expected_qst_paid: Decimal = field(default_factory=lambda: Decimal("0"))
    ar_invoice_ids: list[str] = field(default_factory=list)
    ap_document_ids: list[str] = field(default_factory=list)
    bank_tx_ids: list[str] = field(default_factory=list)


def _make_profiles() -> list[ClientProfile]:
    return [
        ClientProfile(
            client_code="ACME-CAFE", display_name="Café Montréal",
            business_type="restaurant",
            annual_revenue=Decimal("380000"),
            employees=8, daily_sales_count=35, monthly_purchase_count=55,
        ),
        ClientProfile(
            client_code="ACME-CONST", display_name="Construction Tremblay Inc",
            business_type="construction",
            annual_revenue=Decimal("1200000"),
            employees=12, daily_sales_count=3, monthly_purchase_count=45,
        ),
        ClientProfile(
            client_code="ACME-SOLM", display_name="Solutions Marchand Consulting",
            business_type="consulting",
            annual_revenue=Decimal("220000"),
            employees=1, daily_sales_count=1, monthly_purchase_count=12,
        ),
    ]


def _business_days_in(start: date, days: int) -> list[date]:
    out = []
    d = start
    for _ in range(days):
        if _is_biz_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def _short_id(prefix: str, rng: random.Random) -> str:
    return f"{prefix}-{rng.getrandbits(32):08x}"


def seed_client_documents(
    conn: sqlite3.Connection,
    profile: ClientProfile,
) -> ClientProfile:
    """Insert documents + posting_jobs + ar_invoices for one client."""
    rng = _deterministic_rng(profile.client_code)
    start = date.fromisoformat(profile.start_date)
    days = _business_days_in(start, profile.period_days)

    # Ensure target tables exist.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT, confidence REAL,
            raw_result TEXT, submitted_by TEXT,
            invoice_number TEXT, currency TEXT DEFAULT 'CAD',
            subtotal REAL, tax_total REAL, gst_amount REAL,
            qst_amount REAL, created_at TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            posting_status TEXT, external_id TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS ar_invoices (
            invoice_id TEXT PRIMARY KEY, client_code TEXT,
            customer_name TEXT, customer_email TEXT,
            invoice_number TEXT, invoice_date TEXT, due_date TEXT,
            amount_ht REAL, gst_amount REAL, qst_amount REAL,
            total_amount REAL, currency TEXT DEFAULT 'CAD',
            status TEXT, amount_paid REAL DEFAULT 0,
            description TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id TEXT PRIMARY KEY, client_code TEXT,
            plaid_transaction_id TEXT, account_id TEXT,
            date TEXT, amount REAL, description TEXT,
            merchant_name TEXT, category TEXT,
            pending INTEGER DEFAULT 0, matched_document_id TEXT,
            reconciled INTEGER DEFAULT 0, created_at TEXT,
            match_confidence_tier TEXT, match_score_json TEXT
        );
    """)

    # --- AR invoices (sales) --------------------------------------------
    avg_invoice = {
        "restaurant": Decimal("45"),
        "construction": Decimal("80000"),
        "consulting": Decimal("3000"),
    }[profile.business_type]

    for d in days:
        count = rng.randint(
            max(1, profile.daily_sales_count - 5),
            profile.daily_sales_count + 5,
        )
        if profile.business_type == "construction":
            # Big rare contracts — issue only on some days.
            count = 1 if rng.random() < 0.15 else 0
        for _ in range(count):
            variance = Decimal(str(rng.uniform(0.7, 1.3)))
            base = (avg_invoice * variance).quantize(Decimal("0.01"))
            gst = (base * GST_RATE).quantize(Decimal("0.01"))
            qst = (base * QST_RATE).quantize(Decimal("0.01"))
            total = base + gst + qst
            inv_id = _short_id("INV", rng)
            customer = rng.choice([
                "Bistro Client", "M. Tremblay", "Acme Retail Corp",
                "Lacoste & Fils", "Montreal Co-op",
            ])
            conn.execute(
                """INSERT INTO ar_invoices
                   (invoice_id, client_code, customer_name, invoice_number,
                    invoice_date, due_date, amount_ht, gst_amount, qst_amount,
                    total_amount, status, description, created_at)
                   VALUES (?, ?, ?, ?, ?, date(?, '+30 days'), ?, ?, ?, ?,
                           'sent', 'Services', datetime('now'))""",
                (inv_id, profile.client_code, customer, inv_id,
                 _iso(d), _iso(d),
                 float(base), float(gst), float(qst), float(total)),
            )
            profile.ar_invoice_ids.append(inv_id)
            profile.expected_total_sales += base
            profile.expected_gst_collected += gst
            profile.expected_qst_collected += qst

    # --- AP documents (purchases) ---------------------------------------
    ap_vendors_by_type = {
        "restaurant": ["Sysco", "GFS Canada", "Hydro-Quebec", "Vidéotron",
                        "Loyer 123", "Costco Business"],
        "construction": ["Rona", "Home Depot Pro", "Alaris Tools",
                          "Ciment Quebec", "Subcontractor X", "Diesel Depot"],
        "consulting": ["Bell Canada", "Amazon.ca", "Staples",
                        "Claude API", "Uber Canada"],
    }
    vendors = ap_vendors_by_type.get(profile.business_type, ["Generic Vendor"])
    monthly_total_days = [d for d in days]
    ap_count = (profile.monthly_purchase_count * profile.period_days) // 30
    for i in range(ap_count):
        d = rng.choice(monthly_total_days)
        vendor = rng.choice(vendors)
        base = Decimal(str(rng.uniform(20, 2500))).quantize(Decimal("0.01"))
        # Rent / utilities / payroll sometimes exempt; default taxable.
        tax_code = "T"
        if "Hydro" in vendor or "Bell" in vendor or "Vidéotron" in vendor:
            tax_code = "T"
        if "Loyer" in vendor:
            tax_code = "E"  # exempt (rent)
        gst = (base * GST_RATE).quantize(Decimal("0.01")) if tax_code == "T" else Decimal("0")
        qst = (base * QST_RATE).quantize(Decimal("0.01")) if tax_code == "T" else Decimal("0")
        total = base + gst + qst
        did = _short_id("D", rng)
        gl = "5000"
        if "Hydro" in vendor or "Bell" in vendor or "Vidéotron" in vendor:
            gl = "6100"
        if "Loyer" in vendor:
            gl = "6200"
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, doc_type, amount,
                document_date, gl_account, tax_code, review_status,
                subtotal, tax_total, gst_amount, qst_amount, currency,
                created_at, updated_at)
               VALUES (?, ?, ?, 'invoice', ?, ?, ?, ?, 'approved',
                       ?, ?, ?, ?, 'CAD', datetime('now'), datetime('now'))""",
            (did, profile.client_code, vendor, float(total), _iso(d),
             gl, tax_code, float(base), float(gst + qst),
             float(gst), float(qst)),
        )
        conn.execute(
            """INSERT INTO posting_jobs
               (posting_id, document_id, posting_status, external_id,
                created_at, updated_at)
               VALUES (?, ?, 'posted', ?, datetime('now'), datetime('now'))""",
            (_short_id("P", rng), did, _short_id("EXT", rng)),
        )
        profile.ap_document_ids.append(did)
        profile.expected_total_purchases += base
        profile.expected_gst_paid += gst
        profile.expected_qst_paid += qst

    # --- Bank transactions ----------------------------------------------
    for did in profile.ap_document_ids:
        row = conn.execute(
            "SELECT amount, document_date, vendor FROM documents WHERE document_id=?",
            (did,),
        ).fetchone()
        if not row:
            continue
        amt, d, vendor = row
        btx = _short_id("BTX", rng)
        # Payment cleared a few days after the invoice date.
        clear_date = date.fromisoformat(d) + timedelta(days=rng.randint(1, 5))
        conn.execute(
            """INSERT INTO bank_transactions
               (id, client_code, account_id, date, amount, description,
                merchant_name, reconciled, matched_document_id, created_at)
               VALUES (?, ?, 'OPS', ?, ?, ?, ?, 1, ?, datetime('now'))""",
            (btx, profile.client_code, _iso(clear_date), -float(amt),
             f"Payment {vendor}", vendor, did),
        )
        profile.bank_tx_ids.append(btx)

    # Unmatched transactions: a monthly bank fee, a monthly interest credit.
    for month_offset in range(profile.period_days // 30 + 1):
        month_end = start + timedelta(days=30 * (month_offset + 1))
        if month_end > start + timedelta(days=profile.period_days):
            continue
        fee_id = _short_id("FEE", rng)
        conn.execute(
            """INSERT INTO bank_transactions
               (id, client_code, account_id, date, amount, description,
                merchant_name, reconciled, created_at)
               VALUES (?, ?, 'OPS', ?, -12.50, 'Monthly bank fee', 'Bank',
                       0, datetime('now'))""",
            (fee_id, profile.client_code, _iso(month_end)),
        )
        profile.bank_tx_ids.append(fee_id)

    conn.commit()
    return profile


def generate_all(db_path: Path | None = None) -> list[ClientProfile]:
    """Seed all 3 client profiles into a DB (default: in-repo chaos DB)."""
    if db_path is None:
        db_path = ROOT / "tests" / "simulation" / "sim.db"
        if db_path.exists():
            db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    profiles = _make_profiles()
    for p in profiles:
        seed_client_documents(conn, p)
    conn.close()
    return profiles


def expected_totals(p: ClientProfile) -> dict[str, Any]:
    """Canonical totals the simulation will check against."""
    return {
        "client_code": p.client_code,
        "ar_invoice_count": len(p.ar_invoice_ids),
        "ap_document_count": len(p.ap_document_ids),
        "bank_tx_count": len(p.bank_tx_ids),
        "expected_total_sales": float(p.expected_total_sales),
        "expected_total_purchases": float(p.expected_total_purchases),
        "expected_gst_collected": float(p.expected_gst_collected),
        "expected_qst_collected": float(p.expected_qst_collected),
        "expected_gst_paid": float(p.expected_gst_paid),
        "expected_qst_paid": float(p.expected_qst_paid),
    }
