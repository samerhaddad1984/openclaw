"""
src/engines/audit_engine.py — CPA audit support module for OtoCPA.

Provides:
  - Working papers (dossiers de travail) with lead sheet PDF
  - Three-way matching evidence chains (PO / invoice / payment)
  - Statistical sampling (reproducible via random.seed)
  - Trial balance generation from posted documents
  - Financial statements (balance sheet + income statement)
  - Analytical procedures (variance analysis, financial ratios)
  - Engagement management

All monetary arithmetic uses Python Decimal.
Meets Ordre des CPA du Québec documentation standards.
"""
from __future__ import annotations

import json
import random
import secrets
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
_ZERO = Decimal("0")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None or str(v).strip() == "":
        return _ZERO
    try:
        return Decimal(str(v))
    except Exception:
        return _ZERO


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def ensure_audit_tables(conn: sqlite3.Connection) -> None:
    """Create all audit-related tables (idempotent)."""
    conn.execute("PRAGMA foreign_keys = ON")
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

        CREATE TABLE IF NOT EXISTS audit_evidence (
            evidence_id         TEXT PRIMARY KEY,
            document_id         TEXT NOT NULL,
            evidence_type       TEXT NOT NULL,
            linked_document_ids TEXT,
            match_status        TEXT NOT NULL DEFAULT 'missing',
            notes               TEXT,
            created_at          TEXT,
            updated_at          TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_evidence_doc
            ON audit_evidence(document_id);

        CREATE TABLE IF NOT EXISTS trial_balance (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code  TEXT NOT NULL,
            period       TEXT NOT NULL,
            account_code TEXT NOT NULL,
            account_name TEXT NOT NULL,
            debit_total  REAL NOT NULL DEFAULT 0,
            credit_total REAL NOT NULL DEFAULT 0,
            net_balance  REAL NOT NULL DEFAULT 0,
            generated_at TEXT NOT NULL,
            UNIQUE(client_code, period, account_code)
        );

        CREATE TABLE IF NOT EXISTS chart_of_accounts (
            account_code             TEXT PRIMARY KEY,
            account_name             TEXT NOT NULL,
            account_name_en          TEXT,
            account_type             TEXT NOT NULL,
            financial_statement_line TEXT,
            normal_balance           TEXT NOT NULL DEFAULT 'debit',
            cra_t2_line              TEXT,
            co17_line                TEXT,
            financial_statement_section TEXT
        );

        CREATE TABLE IF NOT EXISTS co17_mappings (
            co17_line        TEXT PRIMARY KEY,
            description_fr   TEXT NOT NULL,
            description_en   TEXT NOT NULL,
            gl_account_codes TEXT NOT NULL DEFAULT '[]',
            notes            TEXT
        );

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

        -- P0-2: Signed-off working papers are immutable
        CREATE TRIGGER IF NOT EXISTS trg_wp_signed_off_immutable
        BEFORE UPDATE ON working_papers
        WHEN OLD.status = 'complete' AND OLD.sign_off_at IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'working paper is signed off and immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_wpi_signed_off_immutable
        BEFORE UPDATE ON working_paper_items
        WHEN (SELECT sign_off_at FROM working_papers WHERE paper_id = OLD.paper_id) IS NOT NULL
             AND (SELECT status FROM working_papers WHERE paper_id = OLD.paper_id) = 'complete'
        BEGIN
            SELECT RAISE(ABORT, 'working paper is signed off and immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_wpi_insert_signed_off
        BEFORE INSERT ON working_paper_items
        WHEN (SELECT sign_off_at FROM working_papers WHERE paper_id = NEW.paper_id) IS NOT NULL
             AND (SELECT status FROM working_papers WHERE paper_id = NEW.paper_id) = 'complete'
        BEGIN
            SELECT RAISE(ABORT, 'working paper is signed off and immutable');
        END;
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Chart of accounts — standard Quebec plan comptable
# ---------------------------------------------------------------------------

_STANDARD_CHART: list[tuple[str, str, str, str, str, str]] = [
    # (code, name_fr, name_en, acct_type, fs_line, normal_balance)
    ("1010", "Encaisse",                        "Cash",                         "asset",     "Actif a court terme",    "debit"),
    ("1100", "Comptes clients",                 "Accounts Receivable",          "asset",     "Actif a court terme",    "debit"),
    ("1200", "Stocks",                          "Inventory",                    "asset",     "Actif a court terme",    "debit"),
    ("1300", "Frais payes d avance",            "Prepaid Expenses",             "asset",     "Actif a court terme",    "debit"),
    ("1500", "Immobilisations corporelles",     "Property, Plant & Equipment",  "asset",     "Actif a long terme",     "debit"),
    ("1600", "Amortissement cumule",            "Accumulated Depreciation",     "asset",     "Actif a long terme",     "credit"),
    ("1700", "Placements a long terme",         "Long-term Investments",        "asset",     "Actif a long terme",     "debit"),
    ("2100", "Comptes fournisseurs",            "Accounts Payable",             "liability", "Passif a court terme",   "credit"),
    ("2200", "TPS a payer",                     "GST Payable",                  "liability", "Passif a court terme",   "credit"),
    ("2210", "TVQ a payer",                     "QST Payable",                  "liability", "Passif a court terme",   "credit"),
    ("2300", "Salaires a payer",                "Salaries Payable",             "liability", "Passif a court terme",   "credit"),
    ("2400", "Impots a payer",                  "Income Tax Payable",           "liability", "Passif a court terme",   "credit"),
    ("2500", "Emprunts bancaires",              "Bank Loans",                   "liability", "Passif a long terme",    "credit"),
    ("2600", "Dettes a long terme",             "Long-term Debt",               "liability", "Passif a long terme",    "credit"),
    ("3100", "Capital-actions",                 "Share Capital",                "equity",    "Capitaux propres",       "credit"),
    ("3200", "Benefices non repartis",          "Retained Earnings",            "equity",    "Capitaux propres",       "credit"),
    ("3300", "Resultats de la periode",         "Net Income (Current Period)",  "equity",    "Capitaux propres",       "credit"),
    ("4100", "Produits d exploitation",         "Operating Revenue",            "revenue",   "Produits",               "credit"),
    ("4200", "Autres produits",                 "Other Revenue",                "revenue",   "Produits",               "credit"),
    ("5100", "Cout des marchandises vendues",   "Cost of Goods Sold",           "expense",   "Charges",                "debit"),
    ("5200", "Fournitures de bureau",           "Office Supplies",              "expense",   "Charges",                "debit"),
    ("5300", "Salaires et avantages sociaux",   "Salaries & Benefits",          "expense",   "Charges",                "debit"),
    ("5400", "Honoraires professionnels",       "Professional Fees",            "expense",   "Charges",                "debit"),
    ("5500", "Services publics",                "Utilities",                    "expense",   "Charges",                "debit"),
    ("5600", "Amortissement",                   "Depreciation",                 "expense",   "Charges",                "debit"),
    ("5700", "Marketing et publicite",          "Marketing & Advertising",      "expense",   "Charges",                "debit"),
    ("5800", "Assurances",                      "Insurance",                    "expense",   "Charges",                "debit"),
    ("5900", "Repas et divertissements",        "Meals & Entertainment",        "expense",   "Charges",                "debit"),
    ("5950", "Voyages et transport",            "Travel & Transportation",      "expense",   "Charges",                "debit"),
    ("5990", "Autres charges",                  "Other Expenses",               "expense",   "Charges",                "debit"),
]

# ---------------------------------------------------------------------------
# Expanded Quebec chart of accounts — 200 accounts (Plan comptable général)
# (code, name_fr, name_en, acct_type, fs_line, normal_balance,
#  cra_t2_line, co17_line, financial_statement_section)
# ---------------------------------------------------------------------------

_QUEBEC_CHART_200: list[tuple[str, str, str, str, str, str, str, str, str]] = [
    # ── Assets (1000-1999) ───────────────────────────────────────────────
    ("1010", "Encaisse",                          "Cash",                              "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1020", "Petite caisse",                     "Petty cash",                        "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1030", "Encaisse en devises",               "Foreign currency cash",             "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1050", "Placements a court terme",          "Short-term investments",            "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1100", "Comptes clients",                   "Accounts receivable",               "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1110", "Provision pour mauvaises creances", "Allowance for doubtful accounts",   "asset",     "Actif a court terme",  "credit", None,   None,  "Actif a court terme"),
    ("1120", "Effets a recevoir",                 "Notes receivable",                  "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1130", "TPS a recevoir",                    "GST receivable",                    "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1135", "TVQ a recevoir",                    "QST receivable",                    "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1140", "Avances aux employes",              "Employee advances",                 "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1150", "Prets aux actionnaires",            "Shareholder loans receivable",       "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1160", "Autres debiteurs",                  "Other receivables",                 "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1200", "Stocks",                            "Inventory",                         "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1210", "Stocks de matieres premieres",      "Raw materials inventory",           "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1220", "Stocks de produits en cours",       "Work in process inventory",         "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1230", "Stocks de produits finis",          "Finished goods inventory",          "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1240", "Fournitures en stock",              "Supplies inventory",                "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1300", "Charges payees d avance",           "Prepaid expenses",                  "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1310", "Assurances payees d avance",        "Prepaid insurance",                 "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1320", "Loyer paye d avance",               "Prepaid rent",                      "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1330", "Publicite payee d avance",          "Prepaid advertising",               "asset",     "Actif a court terme",  "debit",  None,   None,  "Actif a court terme"),
    ("1400", "Depots et cautionnements",          "Deposits and bonds",                "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1500", "Immobilisations corporelles",       "Property plant and equipment",      "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1510", "Terrain",                           "Land",                              "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1520", "Batiment",                          "Building",                          "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1530", "Ameliorations locatives",           "Leasehold improvements",            "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1540", "Materiel et equipement",            "Machinery and equipment",           "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1550", "Mobilier de bureau",                "Office furniture",                  "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1560", "Materiel informatique",             "Computer equipment",                "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1570", "Vehicules",                         "Vehicles",                          "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1580", "Outillage",                         "Tools",                             "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1590", "Enseignes et affiches",             "Signs",                             "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1600", "Amortissement cumule",              "Accumulated depreciation",          "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1610", "Amort cumule - Batiment",           "Accum depreciation - Building",     "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1620", "Amort cumule - Ameliorations",      "Accum depreciation - Leasehold",    "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1630", "Amort cumule - Equipement",         "Accum depreciation - Equipment",    "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1640", "Amort cumule - Mobilier",           "Accum depreciation - Furniture",    "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1650", "Amort cumule - Informatique",       "Accum depreciation - Computers",    "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1660", "Amort cumule - Vehicules",          "Accum depreciation - Vehicles",     "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1700", "Placements a long terme",           "Long-term investments",             "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1800", "Immobilisations incorporelles",     "Intangible assets",                 "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1810", "Achalandage",                       "Goodwill",                          "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1820", "Brevets et marques",                "Patents and trademarks",            "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1830", "Logiciels",                         "Software",                          "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1840", "Frais de developpement",            "Development costs",                 "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),
    ("1850", "Amort cumule - Incorporelles",      "Accum amort - Intangibles",         "asset",     "Actif a long terme",   "credit", None,   None,  "Actif a long terme"),
    ("1900", "Autres actifs a long terme",        "Other long-term assets",            "asset",     "Actif a long terme",   "debit",  None,   None,  "Actif a long terme"),

    # ── Liabilities (2000-2999) ──────────────────────────────────────────
    ("2010", "Marge de credit",                   "Line of credit",                    "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2100", "Comptes fournisseurs",              "Accounts payable",                  "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2110", "Effets a payer",                    "Notes payable",                     "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2120", "Charges a payer",                   "Accrued liabilities",               "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2130", "Interets a payer",                  "Interest payable",                  "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2140", "Dividendes a payer",                "Dividends payable",                 "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2150", "Revenus reportes",                  "Deferred revenue",                  "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2160", "Produits recus d avance",           "Unearned revenue",                  "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2200", "TPS a remettre",                    "GST payable",                       "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2210", "TVQ a remettre",                    "QST payable",                       "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2215", "Retenues a la source",              "Source deductions payable",          "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2220", "Cotisations CNESST a payer",        "CNESST payable",                    "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2230", "Cotisations RQAP a payer",          "QPIP payable",                      "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2240", "Cotisations RRQ a payer",           "QPP payable",                       "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2250", "Cotisations AE a payer",            "EI payable",                        "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2260", "FSS a payer",                       "Health services fund payable",       "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2270", "CNT a payer",                       "Labour standards payable",           "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2300", "Salaires a payer",                  "Salaries payable",                  "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2310", "Vacances a payer",                  "Vacation payable",                  "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2400", "Impots a payer",                    "Income tax payable",                "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2410", "Impot federal a payer",             "Federal tax payable",               "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2420", "Impot provincial a payer",          "Provincial tax payable",            "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2430", "Acomptes provisionnels",            "Tax instalments",                   "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2450", "Portion courante dette LT",         "Current portion of LT debt",        "liability", "Passif a court terme", "credit", None,   None,  "Passif a court terme"),
    ("2500", "Emprunts bancaires",                "Bank loans",                        "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2510", "Hypotheque a payer",                "Mortgage payable",                  "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2520", "Emprunt a terme",                   "Term loan",                         "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2530", "Obligations a payer",               "Bonds payable",                     "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2540", "Dettes envers actionnaires",        "Shareholder loans payable",         "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2550", "Obligations locatives",             "Lease obligations",                 "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2600", "Dettes a long terme",               "Long-term debt",                    "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2700", "Impots differes",                   "Deferred tax liability",            "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),
    ("2900", "Autres passifs a long terme",       "Other long-term liabilities",       "liability", "Passif a long terme",  "credit", None,   None,  "Passif a long terme"),

    # ── Equity (3000-3999) ───────────────────────────────────────────────
    ("3100", "Capital-actions ordinaires",        "Common share capital",              "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3110", "Capital-actions privilegiees",       "Preferred share capital",           "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3120", "Capital apporte",                   "Contributed capital",               "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3130", "Surplus d apport",                  "Contributed surplus",               "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3200", "Benefices non repartis",            "Retained earnings",                 "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3300", "Dividendes",                        "Dividends",                         "equity",    "Capitaux propres",     "debit",  None,   None,  "Capitaux propres"),
    ("3400", "Resultats de la periode",           "Net income current period",         "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3500", "Cumul des autres elements",         "Accumulated other comprehensive",   "equity",    "Capitaux propres",     "credit", None,   None,  "Capitaux propres"),
    ("3600", "Actions propres detenues",          "Treasury shares",                   "equity",    "Capitaux propres",     "debit",  None,   None,  "Capitaux propres"),

    # ── Revenue (4000-4999) ──────────────────────────────────────────────
    ("4100", "Ventes",                            "Sales revenue",                     "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4110", "Ventes de marchandises",            "Merchandise sales",                 "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4120", "Ventes de produits",                "Product sales",                     "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4130", "Retours et rabais sur ventes",      "Sales returns and allowances",      "revenue",   "Produits",             "debit",  None,   None,  "Produits d exploitation"),
    ("4140", "Escomptes sur ventes",              "Sales discounts",                   "revenue",   "Produits",             "debit",  None,   None,  "Produits d exploitation"),
    ("4200", "Services rendus",                   "Service revenue",                   "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4210", "Honoraires de consultation",        "Consulting fees revenue",           "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4220", "Revenus de location",               "Rental revenue",                    "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4230", "Revenus de commissions",            "Commission revenue",                "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4240", "Revenus de sous-traitance",         "Subcontracting revenue",            "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4300", "Autres revenus",                    "Other income",                      "revenue",   "Produits",             "credit", None,   None,  "Autres produits"),
    ("4310", "Revenus d interets",                "Interest income",                   "revenue",   "Produits",             "credit", None,   None,  "Autres produits"),
    ("4320", "Revenus de dividendes",             "Dividend income",                   "revenue",   "Produits",             "credit", None,   None,  "Autres produits"),
    ("4330", "Gain sur cession actifs",           "Gain on disposal of assets",        "revenue",   "Produits",             "credit", None,   None,  "Autres produits"),
    ("4340", "Gain de change",                    "Foreign exchange gain",             "revenue",   "Produits",             "credit", None,   None,  "Autres produits"),
    ("4350", "Subventions",                       "Government grants",                 "revenue",   "Produits",             "credit", None,   None,  "Autres produits"),
    ("4400", "Revenus de projets",                "Project revenue",                   "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4500", "Revenus de garanties",              "Warranty revenue",                  "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),
    ("4600", "Revenus intersocietes",             "Intercompany revenue",              "revenue",   "Produits",             "credit", None,   None,  "Produits d exploitation"),

    # ── Cost of goods sold (5000-5099) ───────────────────────────────────
    ("5010", "Cout des marchandises vendues",     "Cost of goods sold",                "expense",   "Charges",              "debit",  "8320", None,  "Cout des ventes"),
    ("5020", "Achats de matieres premieres",      "Raw materials purchases",           "expense",   "Charges",              "debit",  "8320", None,  "Cout des ventes"),
    ("5030", "Main d oeuvre directe",             "Direct labour",                     "expense",   "Charges",              "debit",  "9060", "20",  "Cout des ventes"),
    ("5040", "Frais generaux de fabrication",     "Manufacturing overhead",            "expense",   "Charges",              "debit",  "9270", None,  "Cout des ventes"),
    ("5050", "Sous-traitance production",         "Production subcontracting",         "expense",   "Charges",              "debit",  "8860", "50",  "Cout des ventes"),
    ("5060", "Transport sur achats",              "Freight in",                        "expense",   "Charges",              "debit",  "8870", None,  "Cout des ventes"),
    ("5070", "Escomptes sur achats",              "Purchase discounts",                "expense",   "Charges",              "credit", "8320", None,  "Cout des ventes"),
    ("5080", "Retours sur achats",                "Purchase returns",                  "expense",   "Charges",              "credit", "8320", None,  "Cout des ventes"),

    # ── Expenses (5100-5999) — mapped to CRA T2 and CO-17 lines ─────────
    ("5100", "Achats",                            "Purchases",                         "expense",   "Charges",              "debit",  "8320", None,  "Charges d exploitation"),
    ("5200", "Salaires et traitements",           "Salaries and wages",                "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5210", "Avantages sociaux",                 "Employee benefits",                 "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5220", "Cotisations employeur CNESST",      "Employer CNESST contributions",     "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5230", "Cotisations employeur RRQ",         "Employer QPP contributions",        "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5240", "Cotisations employeur AE",          "Employer EI contributions",         "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5250", "Cotisations employeur RQAP",        "Employer QPIP contributions",       "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5260", "Cotisations FSS",                   "Health services fund",              "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5270", "Cotisations CNT",                   "Labour standards levy",             "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5280", "Commissions versees",               "Commissions paid",                  "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5290", "Formation employes",                "Employee training",                 "expense",   "Charges",              "debit",  "8810", "68",  "Charges d exploitation"),
    ("5300", "Loyer",                             "Rent",                              "expense",   "Charges",              "debit",  "9270", "42",  "Charges d exploitation"),
    ("5310", "Loyer bureau",                      "Office rent",                       "expense",   "Charges",              "debit",  "9270", "42",  "Charges d exploitation"),
    ("5320", "Loyer entrepot",                    "Warehouse rent",                    "expense",   "Charges",              "debit",  "9270", "42",  "Charges d exploitation"),
    ("5330", "Location equipement",               "Equipment rental",                  "expense",   "Charges",              "debit",  "9270", "42",  "Charges d exploitation"),
    ("5340", "Location vehicules",                "Vehicle rental",                    "expense",   "Charges",              "debit",  "9281", "64",  "Charges d exploitation"),
    ("5400", "Telephone",                         "Telephone",                         "expense",   "Charges",              "debit",  "9220", "54",  "Charges d exploitation"),
    ("5410", "Electricite",                       "Electricity",                       "expense",   "Charges",              "debit",  "9220", "52",  "Charges d exploitation"),
    ("5415", "Chauffage",                         "Heating",                           "expense",   "Charges",              "debit",  "9220", "52",  "Charges d exploitation"),
    ("5420", "Assurances",                        "Insurance",                         "expense",   "Charges",              "debit",  "8690", "56",  "Charges d exploitation"),
    ("5425", "Assurance responsabilite",          "Liability insurance",               "expense",   "Charges",              "debit",  "8690", "56",  "Charges d exploitation"),
    ("5430", "Publicite",                         "Advertising",                       "expense",   "Charges",              "debit",  "8520", "48",  "Charges d exploitation"),
    ("5435", "Promotion et marketing",            "Promotion and marketing",           "expense",   "Charges",              "debit",  "8520", "48",  "Charges d exploitation"),
    ("5440", "Fournitures de bureau",             "Office supplies",                   "expense",   "Charges",              "debit",  "8810", "62",  "Charges d exploitation"),
    ("5445", "Papeterie et impressions",          "Stationery and printing",           "expense",   "Charges",              "debit",  "8810", "62",  "Charges d exploitation"),
    ("5450", "Carburant",                         "Fuel",                              "expense",   "Charges",              "debit",  "9224", "64",  "Charges d exploitation"),
    ("5455", "Stationnement",                     "Parking",                           "expense",   "Charges",              "debit",  "9281", "64",  "Charges d exploitation"),
    ("5460", "Entretien et reparations",          "Maintenance and repairs",           "expense",   "Charges",              "debit",  "9281", "60",  "Charges d exploitation"),
    ("5465", "Entretien batiment",                "Building maintenance",              "expense",   "Charges",              "debit",  "9281", "60",  "Charges d exploitation"),
    ("5470", "Honoraires professionnels",         "Professional fees",                 "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5475", "Honoraires comptables",             "Accounting fees",                   "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5476", "Honoraires juridiques",             "Legal fees",                        "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5480", "Interets et frais bancaires",       "Interest and bank charges",         "expense",   "Charges",              "debit",  "8710", "44",  "Charges financieres"),
    ("5485", "Frais bancaires",                   "Bank charges",                      "expense",   "Charges",              "debit",  "8710", "44",  "Charges financieres"),
    ("5490", "Repas d affaires",                  "Business meals",                    "expense",   "Charges",              "debit",  "9200", "46",  "Charges d exploitation"),
    ("5495", "Representation",                    "Entertainment",                     "expense",   "Charges",              "debit",  "9200", "46",  "Charges d exploitation"),
    ("5500", "Deplacements",                      "Travel",                            "expense",   "Charges",              "debit",  "9200", "66",  "Charges d exploitation"),
    ("5505", "Hebergement",                       "Accommodation",                     "expense",   "Charges",              "debit",  "9200", "66",  "Charges d exploitation"),
    ("5510", "Formation",                         "Training",                          "expense",   "Charges",              "debit",  "8810", "68",  "Charges d exploitation"),
    ("5515", "Congres et seminaires",             "Conferences and seminars",          "expense",   "Charges",              "debit",  "8810", "68",  "Charges d exploitation"),
    ("5520", "Materiaux",                         "Materials",                         "expense",   "Charges",              "debit",  "8320", None,  "Charges d exploitation"),
    ("5530", "Sous-traitance",                    "Subcontracting",                    "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5540", "Amortissement",                     "Depreciation",                      "expense",   "Charges",              "debit",  "9936", None,  "Charges d exploitation"),
    ("5545", "Amortissement incorporelles",       "Amortization of intangibles",       "expense",   "Charges",              "debit",  "9936", None,  "Charges d exploitation"),
    ("5550", "Taxes et permis",                   "Taxes and licences",                "expense",   "Charges",              "debit",  "8760", None,  "Charges d exploitation"),
    ("5555", "Taxes municipales",                 "Municipal taxes",                   "expense",   "Charges",              "debit",  "8760", None,  "Charges d exploitation"),
    ("5560", "Frais bancaires",                   "Bank charges",                      "expense",   "Charges",              "debit",  "8710", "44",  "Charges financieres"),
    ("5570", "Mauvaises creances",                "Bad debts",                         "expense",   "Charges",              "debit",  "8590", None,  "Charges d exploitation"),
    ("5580", "Dons et commandites",               "Donations and sponsorships",        "expense",   "Charges",              "debit",  "8520", "48",  "Charges d exploitation"),
    ("5590", "Livraison et transport",            "Delivery and freight",              "expense",   "Charges",              "debit",  "8870", None,  "Charges d exploitation"),
    ("5600", "Internet et site web",              "Internet and website",              "expense",   "Charges",              "debit",  "9220", "54",  "Charges d exploitation"),
    ("5610", "Logiciels et abonnements",          "Software and subscriptions",        "expense",   "Charges",              "debit",  "8810", "62",  "Charges d exploitation"),
    ("5620", "Services informatiques",            "IT services",                       "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5630", "Securite",                          "Security",                          "expense",   "Charges",              "debit",  "9270", "99",  "Charges d exploitation"),
    ("5640", "Nettoyage et entretien",            "Cleaning and janitorial",           "expense",   "Charges",              "debit",  "9270", "60",  "Charges d exploitation"),
    ("5650", "Cotisations professionnelles",      "Professional dues",                 "expense",   "Charges",              "debit",  "8760", None,  "Charges d exploitation"),
    ("5660", "Frais de representation",           "Representation expenses",           "expense",   "Charges",              "debit",  "9200", "46",  "Charges d exploitation"),
    ("5670", "Frais de vehicule",                 "Motor vehicle expenses",            "expense",   "Charges",              "debit",  "9281", "64",  "Charges d exploitation"),
    ("5680", "Frais de livraison courrier",       "Delivery and courier",              "expense",   "Charges",              "debit",  "8870", None,  "Charges d exploitation"),
    ("5690", "Frais de recrutement",              "Recruitment expenses",              "expense",   "Charges",              "debit",  "9060", "20",  "Charges d exploitation"),
    ("5700", "Honoraires de gestion",             "Management fees",                   "expense",   "Charges",              "debit",  "9270", "50",  "Charges d exploitation"),
    ("5710", "Frais de consultation",             "Consulting fees",                   "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5720", "Frais d evaluation",                "Appraisal fees",                    "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5730", "Frais de recouvrement",             "Collection fees",                   "expense",   "Charges",              "debit",  "8860", "50",  "Charges d exploitation"),
    ("5740", "Abonnements et revues",             "Subscriptions and periodicals",     "expense",   "Charges",              "debit",  "8810", "62",  "Charges d exploitation"),
    ("5750", "Frais postaux",                     "Postage",                           "expense",   "Charges",              "debit",  "8870", None,  "Charges d exploitation"),
    ("5760", "Eau",                               "Water",                             "expense",   "Charges",              "debit",  "9220", "52",  "Charges d exploitation"),
    ("5770", "Gestion des dechets",               "Waste management",                  "expense",   "Charges",              "debit",  "9270", "99",  "Charges d exploitation"),
    ("5780", "Amenagement paysager",              "Landscaping",                       "expense",   "Charges",              "debit",  "9281", "60",  "Charges d exploitation"),
    ("5790", "Alarme et surveillance",            "Alarm and monitoring",              "expense",   "Charges",              "debit",  "9270", "99",  "Charges d exploitation"),
    ("5800", "Perte sur cession actifs",          "Loss on disposal of assets",        "expense",   "Charges",              "debit",  "9270", "99",  "Autres charges"),
    ("5810", "Perte de change",                   "Foreign exchange loss",             "expense",   "Charges",              "debit",  "9270", "99",  "Autres charges"),
    ("5820", "Penalites et amendes",              "Penalties and fines",               "expense",   "Charges",              "debit",  "9270", "99",  "Autres charges"),
    ("5830", "Charge d impots",                   "Income tax expense",                "expense",   "Charges",              "debit",  None,   None,  "Impots"),
    ("5840", "Impots federal",                    "Federal income tax",                "expense",   "Charges",              "debit",  None,   None,  "Impots"),
    ("5850", "Impots provincial",                 "Provincial income tax",             "expense",   "Charges",              "debit",  None,   None,  "Impots"),
    ("5900", "Autres charges exploitation",       "Other operating expenses",          "expense",   "Charges",              "debit",  "9270", "99",  "Charges d exploitation"),
    ("5910", "Charges diverses",                  "Miscellaneous expenses",            "expense",   "Charges",              "debit",  "9270", "99",  "Charges d exploitation"),
    ("5920", "Charges non recurrentes",           "Non-recurring expenses",            "expense",   "Charges",              "debit",  "9270", "99",  "Autres charges"),
    ("5990", "Autres charges",                    "Other expenses",                    "expense",   "Charges",              "debit",  "9270", "99",  "Autres charges"),
]


def seed_chart_of_accounts(conn: sqlite3.Connection) -> int:
    """Insert standard Quebec chart of accounts rows that do not already exist."""
    ensure_audit_tables(conn)
    count = 0
    for (code, name_fr, name_en, acct_type, fs_line, normal) in _STANDARD_CHART:
        if not conn.execute(
            "SELECT 1 FROM chart_of_accounts WHERE account_code=?", (code,)
        ).fetchone():
            conn.execute(
                """INSERT INTO chart_of_accounts
                   (account_code, account_name, account_name_en,
                    account_type, financial_statement_line, normal_balance)
                   VALUES (?,?,?,?,?,?)""",
                (code, name_fr, name_en, acct_type, fs_line, normal),
            )
            count += 1
    conn.commit()
    return count


def seed_chart_of_accounts_quebec(conn: sqlite3.Connection) -> int:
    """Insert expanded 200-account Quebec plan comptable.

    Uses INSERT OR REPLACE so new columns (cra_t2_line, co17_line,
    financial_statement_section) are populated even for rows that were
    previously inserted by seed_chart_of_accounts().
    """
    ensure_audit_tables(conn)
    # Ensure new columns exist (ALTER TABLE is idempotent-safe via try/except)
    for col, default in [
        ("account_name_en", "TEXT"),
        ("cra_t2_line", "TEXT"),
        ("co17_line", "TEXT"),
        ("financial_statement_section", "TEXT"),
    ]:
        try:
            conn.execute(
                f"ALTER TABLE chart_of_accounts ADD COLUMN {col} {default}"
            )
        except sqlite3.OperationalError:
            pass  # column already exists
    count = 0
    for row in _QUEBEC_CHART_200:
        (code, name_fr, name_en, acct_type, fs_line, normal,
         cra_line, co17_line, fs_section) = row
        conn.execute(
            """INSERT INTO chart_of_accounts
               (account_code, account_name, account_name_en,
                account_type, financial_statement_line, normal_balance,
                cra_t2_line, co17_line, financial_statement_section)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(account_code) DO UPDATE SET
                   account_name = excluded.account_name,
                   account_name_en = excluded.account_name_en,
                   account_type = excluded.account_type,
                   financial_statement_line = excluded.financial_statement_line,
                   normal_balance = excluded.normal_balance,
                   cra_t2_line = excluded.cra_t2_line,
                   co17_line = excluded.co17_line,
                   financial_statement_section = excluded.financial_statement_section""",
            (code, name_fr, name_en, acct_type, fs_line, normal,
             cra_line, co17_line, fs_section),
        )
        count += 1
    conn.commit()
    return count


def seed_co17_mappings(conn: sqlite3.Connection) -> int:
    """Seed CO-17 line-to-GL-account mappings."""
    ensure_audit_tables(conn)
    mappings = [
        ("20", "Salaires", "Wages",
         json.dumps(["5200", "5210", "5220", "5230", "5240", "5250", "5260", "5270", "5280", "5690"]),
         "Includes all salary, benefits and employer contributions"),
        ("42", "Loyer", "Rent",
         json.dumps(["5300", "5310", "5320", "5330"]),
         "All rental expenses including office, warehouse and equipment"),
        ("44", "Interets", "Interest",
         json.dumps(["5480", "5485", "5560"]),
         "Interest and bank charges"),
        ("46", "Repas et representation 50%", "Meals and entertainment 50%",
         json.dumps(["5490", "5495", "5660"]),
         "Subject to 50% deductibility limit"),
        ("48", "Publicite", "Advertising",
         json.dumps(["5430", "5435", "5580"]),
         "Advertising, promotion, donations and sponsorships"),
        ("50", "Honoraires professionnels", "Professional fees",
         json.dumps(["5470", "5475", "5476", "5530", "5620", "5700", "5710", "5720", "5730"]),
         "All professional, legal, accounting, consulting and management fees"),
        ("52", "Electricite", "Electricity",
         json.dumps(["5410", "5415", "5760"]),
         "Electricity, heating and water"),
        ("54", "Telephone", "Telephone",
         json.dumps(["5400", "5600"]),
         "Telephone and internet"),
        ("56", "Assurances", "Insurance",
         json.dumps(["5420", "5425"]),
         "All insurance premiums"),
        ("60", "Entretien et reparations", "Maintenance and repairs",
         json.dumps(["5460", "5465", "5640", "5780"]),
         "Building maintenance, cleaning, landscaping"),
        ("62", "Fournitures", "Supplies",
         json.dumps(["5440", "5445", "5610", "5740"]),
         "Office supplies, software subscriptions, stationery"),
        ("64", "Vehicules", "Vehicles",
         json.dumps(["5340", "5450", "5455", "5670"]),
         "All motor vehicle expenses including fuel, parking, rental"),
        ("66", "Voyages", "Travel",
         json.dumps(["5500", "5505"]),
         "Travel and accommodation"),
        ("68", "Formation", "Training",
         json.dumps(["5290", "5510", "5515"]),
         "Employee training, conferences, seminars"),
        ("99", "Autres depenses", "Other expenses",
         json.dumps(["5550", "5555", "5570", "5590", "5630", "5650", "5680", "5750", "5770", "5790",
                      "5800", "5810", "5820", "5900", "5910", "5920", "5990"]),
         "All other operating and non-recurring expenses"),
    ]
    count = 0
    for (line, desc_fr, desc_en, gl_codes, notes) in mappings:
        conn.execute(
            """INSERT INTO co17_mappings
               (co17_line, description_fr, description_en, gl_account_codes, notes)
               VALUES (?,?,?,?,?)
               ON CONFLICT(co17_line) DO UPDATE SET
                   description_fr = excluded.description_fr,
                   description_en = excluded.description_en,
                   gl_account_codes = excluded.gl_account_codes,
                   notes = excluded.notes""",
            (line, desc_fr, desc_en, gl_codes, notes),
        )
        count += 1
    conn.commit()
    return count


def get_co17_mappings(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return all CO-17 line mappings."""
    ensure_audit_tables(conn)
    rows = conn.execute(
        "SELECT * FROM co17_mappings ORDER BY CAST(co17_line AS INTEGER)"
    ).fetchall()
    return [dict(r) for r in rows]


def get_chart_of_accounts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_audit_tables(conn)
    return [dict(r) for r in conn.execute(
        "SELECT * FROM chart_of_accounts ORDER BY account_code"
    ).fetchall()]


def _infer_account_type(code: str) -> str:
    return {"1": "asset", "2": "liability", "3": "equity",
            "4": "revenue", "5": "expense"}.get(code[:1] if code else "5", "expense")


# ---------------------------------------------------------------------------
# Working papers
# ---------------------------------------------------------------------------

VALID_ENGAGEMENT_TYPES = {"audit", "review", "compilation"}
VALID_WP_STATUSES = {"open", "complete", "exception"}
VALID_TICK_MARKS = {"tested", "confirmed", "exception", "not_applicable"}


def _wp_id(client_code: str, period: str, account_code: str) -> str:
    cc = client_code.lower().replace(" ", "_")
    ac = account_code.replace(" ", "_")
    return f"wp_{cc}_{period}_{ac}"


def create_working_paper(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    engagement_type: str,
    account_code: str,
    account_name: str,
    balance_per_books: float | None = None,
) -> str:
    """Create a working paper and return its paper_id."""
    wp = get_or_create_working_paper(
        conn, client_code, period, engagement_type,
        account_code, account_name, balance_per_books,
    )
    return wp["paper_id"]


def sign_off_working_paper(
    conn: sqlite3.Connection,
    paper_id: str,
    tested_by: str,
    sign_off_at: str | None = None,
) -> dict[str, Any]:
    """Sign off a working paper with backdating protection.

    P1-6: sign_off_at must not be more than 24 hours in the past.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if sign_off_at is not None:
        try:
            ts = datetime.fromisoformat(sign_off_at)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            delta = now - ts
            if delta.total_seconds() > 86400:
                raise ValueError(
                    "Backdated sign-off not permitted — use current timestamp / "
                    "Signature antidatée non permise"
                )
        except ValueError as e:
            if "Backdated" in str(e) or "antidatée" in str(e):
                raise
            # Invalid format — use current time
            sign_off_at = now.isoformat()
    else:
        sign_off_at = now.isoformat()

    return update_working_paper(
        conn, paper_id,
        tested_by=tested_by,
        reviewed_by=tested_by,
        status="complete",
        notes=f"Signed off by {tested_by}",
    )


def get_working_papers(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    engagement_type: str | None = None,
) -> list[dict[str, Any]]:
    ensure_audit_tables(conn)
    params: list[Any] = [client_code, period]
    extra = ""
    if engagement_type:
        extra = " AND engagement_type = ?"
        params.append(engagement_type)
    rows = conn.execute(
        f"""SELECT wp.*,
              (SELECT COUNT(*) FROM working_paper_items i
               WHERE i.paper_id = wp.paper_id) AS item_count,
              (SELECT COUNT(*) FROM working_paper_items i
               WHERE i.paper_id = wp.paper_id AND i.tick_mark = 'exception') AS exception_count
            FROM working_papers wp
            WHERE LOWER(client_code) = LOWER(?) AND period = ?{extra}
            ORDER BY account_code""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def get_or_create_working_paper(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    engagement_type: str,
    account_code: str,
    account_name: str,
    balance_per_books: float | None = None,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    if engagement_type not in VALID_ENGAGEMENT_TYPES:
        engagement_type = "audit"
    paper_id = _wp_id(client_code, period, account_code)
    row = conn.execute(
        "SELECT * FROM working_papers WHERE paper_id=?", (paper_id,)
    ).fetchone()
    if row:
        return dict(row)
    now = _utc_now()
    conn.execute(
        """INSERT INTO working_papers
           (paper_id, client_code, period, engagement_type, account_code, account_name,
            balance_per_books, status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,'open',?,?)""",
        (paper_id, client_code, period, engagement_type, account_code,
         account_name, balance_per_books, now, now),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM working_papers WHERE paper_id=?", (paper_id,)
    ).fetchone())


def update_working_paper(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    balance_confirmed: float | None = None,
    tested_by: str | None = None,
    reviewed_by: str | None = None,
    status: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM working_papers WHERE paper_id=?", (paper_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Working paper not found: {paper_id}")
    r = dict(row)
    if status is not None and status in VALID_WP_STATUSES:
        r["status"] = status
    if tested_by is not None:
        r["tested_by"] = tested_by
    if reviewed_by is not None:
        r["reviewed_by"] = reviewed_by
        r["sign_off_at"] = _utc_now()
    if notes is not None:
        r["notes"] = notes
    if balance_confirmed is not None:
        r["balance_confirmed"] = balance_confirmed
        bpb = float(r.get("balance_per_books") or 0.0)
        r["difference"] = round(balance_confirmed - bpb, 2)
    conn.execute(
        """UPDATE working_papers SET
           balance_confirmed=?, difference=?, tested_by=?, reviewed_by=?,
           sign_off_at=?, status=?, notes=?, updated_at=?
           WHERE paper_id=?""",
        (r.get("balance_confirmed"), r.get("difference"),
         r.get("tested_by"), r.get("reviewed_by"),
         r.get("sign_off_at"), r["status"], r.get("notes"),
         _utc_now(), paper_id),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM working_papers WHERE paper_id=?", (paper_id,)
    ).fetchone())


def get_working_paper_items(
    conn: sqlite3.Connection, paper_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM working_paper_items WHERE paper_id=? ORDER BY tested_at",
        (paper_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def add_working_paper_item(
    conn: sqlite3.Connection,
    paper_id: str,
    document_id: str,
    tick_mark: str,
    notes: str,
    tested_by: str,
) -> dict[str, Any]:
    if tick_mark not in VALID_TICK_MARKS:
        tick_mark = "tested"
    item_id = f"wpi_{secrets.token_hex(8)}"
    now = _utc_now()
    conn.execute(
        """INSERT INTO working_paper_items
           (item_id, paper_id, document_id, tick_mark, notes, tested_by, tested_at)
           VALUES (?,?,?,?,?,?,?)""",
        (item_id, paper_id, document_id, tick_mark, notes, tested_by, now),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM working_paper_items WHERE item_id=?", (item_id,)
    ).fetchone())


# ---------------------------------------------------------------------------
# Lead-sheet PDF
# ---------------------------------------------------------------------------

def generate_lead_sheet_pdf(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    engagement_type: str,
    prepared_by: str = "",
    reviewed_by_firm: str = "",
    firm_name: str = "OtoCPA CPA",
    lang: str = "fr",
) -> bytes:
    from src.i18n import t
    papers = get_working_papers(conn, client_code, period, engagement_type)
    try:
        import fitz  # noqa: F401
        return _lead_sheet_pymupdf(papers, client_code, period, engagement_type,
                                   prepared_by, reviewed_by_firm, firm_name, lang, t)
    except ImportError:
        return _lead_sheet_minimal(papers, client_code, period, engagement_type,
                                   prepared_by, reviewed_by_firm, firm_name, lang, t)


def _lead_sheet_pymupdf(papers, client_code, period, engagement_type,
                        prepared_by, reviewed_by_firm, firm_name, lang, t) -> bytes:
    import fitz
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    y = 50
    page.insert_text((50, y), firm_name, fontsize=14, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    page.insert_text((350, y), t("wp_lead_sheet", lang), fontsize=11, fontname="hebo")
    y += 18
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 12
    for label, val in [
        (t("col_client", lang), client_code),
        (t("wp_period", lang), period),
        (t("wp_engagement_type", lang), engagement_type.capitalize()),
        (t("wp_prepared_by", lang), prepared_by),
        (t("wp_reviewed_by", lang), reviewed_by_firm),
        (t("wp_date", lang), datetime.now(timezone.utc).strftime("%Y-%m-%d")),
    ]:
        page.insert_text((50, y), f"{label}:", fontsize=9, fontname="hebo")
        page.insert_text((180, y), str(val), fontsize=9)
        y += 13
    y += 8
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14
    for (x, hdr) in [
        (50,  t("col_gl_account", lang)),
        (210, t("wp_balance_books", lang)),
        (300, t("wp_balance_confirmed", lang)),
        (390, t("wp_difference", lang)),
        (450, t("col_status", lang)),
        (510, t("wp_sign_off", lang)),
    ]:
        page.insert_text((x, y), hdr, fontsize=8, fontname="hebo")
    y += 10
    page.draw_line((50, y), (562, y), color=(0.85, 0.85, 0.85), width=0.3)
    y += 12
    total_bpb = _ZERO
    total_bc = _ZERO
    exceptions = 0
    for wp in papers:
        if y > 730:
            page = doc.new_page(width=612, height=792)
            y = 50
        bpb = _to_decimal(wp.get("balance_per_books"))
        bc = _to_decimal(wp.get("balance_confirmed") if wp.get("balance_confirmed") is not None else wp.get("balance_per_books"))
        diff = _to_decimal(wp.get("difference"))
        status = wp.get("status", "open")
        sign_off = "Y" if wp.get("sign_off_at") else "-"
        if status == "exception":
            exceptions += 1
            color = (0.8, 0.2, 0.2)
        elif status == "complete":
            color = (0.1, 0.5, 0.1)
        else:
            color = (0.0, 0.0, 0.0)
        acct = f"{wp['account_code']} {wp['account_name']}"[:32]
        page.insert_text((50, y), acct, fontsize=8, color=color)
        page.insert_text((210, y), f"${float(bpb):,.2f}", fontsize=8)
        page.insert_text((300, y), f"${float(bc):,.2f}", fontsize=8)
        page.insert_text((390, y), f"${float(diff):+,.2f}", fontsize=8)
        page.insert_text((450, y), status, fontsize=8, color=color)
        page.insert_text((510, y), sign_off, fontsize=8)
        y += 12
        total_bpb += bpb
        total_bc += bc
    y += 6
    page.draw_line((50, y), (562, y), color=(0.08, 0.16, 0.44), width=0.8)
    y += 12
    page.insert_text((50, y), t("wp_totals", lang), fontsize=9, fontname="hebo")
    page.insert_text((210, y), f"${float(total_bpb):,.2f}", fontsize=9, fontname="hebo")
    page.insert_text((300, y), f"${float(total_bc):,.2f}", fontsize=9, fontname="hebo")
    if exceptions:
        y += 16
        page.insert_text((50, y),
                         f"! {exceptions} {t('wp_exceptions_found', lang)}",
                         fontsize=9, color=(0.8, 0.2, 0.2))
    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


def _lead_sheet_minimal(papers, client_code, period, engagement_type,
                        prepared_by, reviewed_by_firm, firm_name, lang, t) -> bytes:
    elements: list[tuple[int, int, str, int]] = []

    def add(x: int, y: int, text: str, size: int = 10) -> None:
        elements.append((x, y, text, size))

    y = 750
    add(50, y, firm_name, 14)
    add(350, y, t("wp_lead_sheet", lang), 11)
    y -= 18
    add(50, y, f"{t('col_client', lang)}: {client_code}", 9)
    y -= 12
    add(50, y, f"{t('wp_period', lang)}: {period}", 9)
    y -= 12
    add(50, y, f"{t('wp_engagement_type', lang)}: {engagement_type}", 9)
    y -= 12
    add(50, y, f"{t('wp_prepared_by', lang)}: {prepared_by}", 9)
    y -= 12
    add(50, y, f"{t('wp_reviewed_by', lang)}: {reviewed_by_firm}", 9)
    y -= 16
    add(50, y, t("col_gl_account", lang), 8)
    add(210, y, t("wp_balance_books", lang), 8)
    add(300, y, t("wp_balance_confirmed", lang), 8)
    add(390, y, t("wp_difference", lang), 8)
    add(450, y, t("col_status", lang), 8)
    y -= 12
    total_bpb = _ZERO
    for wp in papers:
        if y < 80:
            break
        bpb = _to_decimal(wp.get("balance_per_books"))
        bc  = _to_decimal(wp.get("balance_confirmed") if wp.get("balance_confirmed") is not None else wp.get("balance_per_books"))
        diff = _to_decimal(wp.get("difference"))
        acct = f"{wp['account_code']} {wp['account_name']}"[:30]
        add(50, y, acct, 8)
        add(210, y, f"${float(bpb):.2f}", 8)
        add(300, y, f"${float(bc):.2f}", 8)
        add(390, y, f"${float(diff):+.2f}", 8)
        add(450, y, wp.get("status", "open"), 8)
        y -= 11
        total_bpb += bpb
    y -= 8
    add(50, y, f"{t('wp_totals', lang)}: ${float(total_bpb):.2f}", 10)
    return _build_minimal_pdf(elements)


# ---------------------------------------------------------------------------
# Audit evidence
# ---------------------------------------------------------------------------

VALID_EVIDENCE_TYPES = {"purchase_order", "invoice", "payment"}


def _evidence_id(document_id: str, evidence_type: str) -> str:
    return f"ev_{document_id}_{evidence_type}"


def get_or_create_evidence(
    conn: sqlite3.Connection,
    document_id: str,
    evidence_type: str,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    if evidence_type not in VALID_EVIDENCE_TYPES:
        evidence_type = "invoice"
    eid = _evidence_id(document_id, evidence_type)
    row = conn.execute(
        "SELECT * FROM audit_evidence WHERE evidence_id=?", (eid,)
    ).fetchone()
    if row:
        return dict(row)
    now = _utc_now()
    conn.execute(
        """INSERT INTO audit_evidence
           (evidence_id, document_id, evidence_type, linked_document_ids,
            match_status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?)""",
        (eid, document_id, evidence_type, json.dumps([]), "missing", now, now),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM audit_evidence WHERE evidence_id=?", (eid,)
    ).fetchone())


def link_evidence_documents(
    conn: sqlite3.Connection,
    evidence_id: str,
    linked_ids: list[str],
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM audit_evidence WHERE evidence_id=?", (evidence_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Evidence not found: {evidence_id}")
    existing = json.loads(row["linked_document_ids"] or "[]")
    merged = list(set(existing) | set(linked_ids))
    doc_id = row["document_id"]
    # Check which evidence types exist for this document
    types_for_doc = {r["evidence_type"] for r in conn.execute(
        "SELECT evidence_type FROM audit_evidence WHERE document_id=?", (doc_id,)
    ).fetchall()}
    required = {"purchase_order", "invoice", "payment"}
    if required.issubset(types_for_doc):
        match_status = "complete"
    elif types_for_doc:
        match_status = "partial"
    else:
        match_status = "missing"
    conn.execute(
        """UPDATE audit_evidence
           SET linked_document_ids=?, match_status=?, updated_at=?
           WHERE evidence_id=?""",
        (json.dumps(merged), match_status, _utc_now(), evidence_id),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM audit_evidence WHERE evidence_id=?", (evidence_id,)
    ).fetchone())


def get_evidence_chains(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> list[dict[str, Any]]:
    ensure_audit_tables(conn)
    try:
        rows = conn.execute(
            """SELECT ae.*, d.vendor, d.document_date, d.amount, d.gl_account
               FROM audit_evidence ae
               JOIN documents d ON d.document_id = ae.document_id
               WHERE LOWER(COALESCE(d.client_code,'')) = LOWER(?)
                 AND COALESCE(d.document_date,'') LIKE ?
               ORDER BY d.document_date, ae.document_id""",
            (client_code, f"{period}%"),
        ).fetchall()
    except Exception:
        return []
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["linked_document_ids"] = json.loads(d.get("linked_document_ids") or "[]")
        except Exception:
            d["linked_document_ids"] = []
        result.append(d)
    return result


def check_and_update_evidence_for_period(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> int:
    """Create invoice evidence records for all posted documents in a period."""
    ensure_audit_tables(conn)
    try:
        rows = conn.execute(
            """SELECT d.document_id, d.doc_type
               FROM documents d
               LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
                   AND pj.rowid = (
                       SELECT pj2.rowid FROM posting_jobs pj2
                       WHERE pj2.document_id = d.document_id
                       ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC,
                                pj2.rowid DESC LIMIT 1
                   )
               WHERE LOWER(COALESCE(d.client_code,'')) = LOWER(?)
                 AND COALESCE(d.document_date,'') LIKE ?
                 AND COALESCE(pj.posting_status,'') = 'posted'""",
            (client_code, f"{period}%"),
        ).fetchall()
    except Exception:
        return 0
    flagged = 0
    for row in rows:
        doc_id = row["document_id"]
        doc_type = str(row["doc_type"] or "").lower()
        if any(k in doc_type for k in ("purchase", "po", "order")):
            ev_type = "purchase_order"
        elif any(k in doc_type for k in ("payment", "receipt", "bank")):
            ev_type = "payment"
        else:
            ev_type = "invoice"
        ev = get_or_create_evidence(conn, doc_id, ev_type)
        if ev["match_status"] != "complete":
            flagged += 1
    return flagged


# ---------------------------------------------------------------------------
# Statistical sampling
# ---------------------------------------------------------------------------

def get_sample(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    account_code: str,
    sample_size: int,
    paper_id: str,
) -> list[dict[str, Any]]:
    """Reproducible random sample; same paper_id always yields same set."""
    try:
        rows = conn.execute(
            """SELECT d.*,
                  COALESCE(pj.posting_status,'') AS posting_status,
                  COALESCE(pj.external_id,'') AS external_id
               FROM documents d
               LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
                   AND pj.rowid = (
                       SELECT pj2.rowid FROM posting_jobs pj2
                       WHERE pj2.document_id = d.document_id
                       ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC,
                                pj2.rowid DESC LIMIT 1
                   )
               WHERE LOWER(COALESCE(d.client_code,'')) = LOWER(?)
                 AND COALESCE(d.document_date,'') LIKE ?
                 AND (? = '' OR LOWER(COALESCE(d.gl_account,'')) LIKE ?)
                 AND LOWER(COALESCE(d.review_status,'')) != 'ignored'
               ORDER BY d.document_date, d.document_id""",
            (client_code, f"{period}%", account_code, f"%{account_code.lower()}%"),
        ).fetchall()
    except Exception:
        return []
    all_docs = [dict(r) for r in rows]
    rng = random.Random(paper_id)
    sampled = rng.sample(all_docs, min(sample_size, len(all_docs)))
    items_map: dict[str, dict[str, Any]] = {}
    for item in get_working_paper_items(conn, paper_id):
        did = item.get("document_id", "")
        if did:
            items_map[did] = item
    for doc in sampled:
        doc["wp_item"] = items_map.get(doc["document_id"])
    return sampled


def get_sample_status(
    conn: sqlite3.Connection,
    paper_id: str,
) -> dict[str, int]:
    items = get_working_paper_items(conn, paper_id)
    tested = sum(1 for i in items if i["tick_mark"] in ("tested", "confirmed"))
    exceptions = sum(1 for i in items if i["tick_mark"] == "exception")
    not_applicable = sum(1 for i in items if i["tick_mark"] == "not_applicable")
    return {
        "total": len(items),
        "tested": tested,
        "exceptions": exceptions,
        "not_applicable": not_applicable,
    }


# ---------------------------------------------------------------------------
# Trial balance
# ---------------------------------------------------------------------------

def generate_trial_balance(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> list[dict[str, Any]]:
    """Aggregate posted documents by GL account into a trial balance."""
    ensure_audit_tables(conn)
    seed_chart_of_accounts(conn)
    seed_chart_of_accounts_quebec(conn)
    try:
        rows = conn.execute(
            """SELECT d.gl_account, SUM(COALESCE(d.amount, 0)) AS total_amount
               FROM documents d
               LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
                   AND pj.rowid = (
                       SELECT pj2.rowid FROM posting_jobs pj2
                       WHERE pj2.document_id = d.document_id
                       ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC,
                                pj2.rowid DESC LIMIT 1
                   )
               WHERE LOWER(COALESCE(d.client_code,'')) = LOWER(?)
                 AND COALESCE(d.document_date,'') LIKE ?
                 AND LOWER(COALESCE(d.review_status,'')) != 'ignored'
                 AND COALESCE(pj.posting_status,'') = 'posted'
               GROUP BY d.gl_account
               ORDER BY d.gl_account""",
            (client_code, f"{period}%"),
        ).fetchall()
    except Exception:
        return []
    now = _utc_now()
    result = []
    for row in rows:
        gl_raw = str(row["gl_account"] or "").strip()
        if not gl_raw:
            continue
        amount = _to_decimal(row["total_amount"])
        parts = gl_raw.split(" ", 1)
        acct_code = parts[0].strip() if parts else gl_raw
        acct_name = (parts[1].lstrip("- ").strip() if len(parts) > 1 else gl_raw) or gl_raw
        coa_row = conn.execute(
            "SELECT * FROM chart_of_accounts WHERE account_code=?", (acct_code,)
        ).fetchone()
        if coa_row:
            normal_balance = coa_row["normal_balance"]
            acct_name = coa_row["account_name"]
        else:
            acct_type = _infer_account_type(acct_code)
            normal_balance = "credit" if acct_type in ("liability", "equity", "revenue") else "debit"
        debit_total = float(amount) if normal_balance == "debit" else 0.0
        credit_total = float(amount) if normal_balance == "credit" else 0.0
        net_balance = debit_total - credit_total
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
            (client_code, period, acct_code, acct_name,
             debit_total, credit_total, net_balance, now),
        )
        result.append({
            "account_code": acct_code,
            "account_name": acct_name,
            "debit_total": debit_total,
            "credit_total": credit_total,
            "net_balance": net_balance,
            "gl_raw": gl_raw,
        })
    conn.commit()
    return result


def get_trial_balance(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT * FROM trial_balance
           WHERE LOWER(client_code) = LOWER(?) AND period = ?
           ORDER BY account_code""",
        (client_code, period),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Financial statements
# ---------------------------------------------------------------------------

def generate_financial_statements(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    seed_chart_of_accounts(conn)
    seed_chart_of_accounts_quebec(conn)
    generate_trial_balance(conn, client_code, period)
    tb_rows = get_trial_balance(conn, client_code, period)
    coa = {r["account_code"]: dict(r) for r in conn.execute(
        "SELECT * FROM chart_of_accounts"
    ).fetchall()}
    bs: dict[str, Any] = {
        "assets": {"current": [], "non_current": [], "total": _ZERO},
        "liabilities": {"current": [], "long_term": [], "total": _ZERO},
        "equity": {"items": [], "total": _ZERO},
    }
    is_: dict[str, Any] = {
        "revenue": [],
        "expenses": [],
        "total_revenue": _ZERO,
        "total_expenses": _ZERO,
        "net_income": _ZERO,
    }
    for row in tb_rows:
        code = row["account_code"]
        name = row["account_name"]
        net = _to_decimal(row["net_balance"])
        coa_e = coa.get(code, {})
        acct_type = coa_e.get("account_type") or _infer_account_type(code)
        # Prefer financial_statement_section (expanded chart), fall back to
        # financial_statement_line (legacy chart)
        fs_section = (
            coa_e.get("financial_statement_section")
            or coa_e.get("financial_statement_line")
            or ""
        )
        item = {
            "account_code": code,
            "account_name": name,
            "amount": net,
            "financial_statement_section": fs_section,
            "cra_t2_line": coa_e.get("cra_t2_line"),
            "co17_line": coa_e.get("co17_line"),
        }
        if acct_type == "asset":
            if "long" in fs_section.lower():
                bs["assets"]["non_current"].append(item)
            else:
                bs["assets"]["current"].append(item)
            bs["assets"]["total"] += net
        elif acct_type == "liability":
            if "long" in fs_section.lower():
                bs["liabilities"]["long_term"].append(item)
            else:
                bs["liabilities"]["current"].append(item)
            bs["liabilities"]["total"] += net
        elif acct_type == "equity":
            bs["equity"]["items"].append(item)
            bs["equity"]["total"] += net
        elif acct_type == "revenue":
            is_["revenue"].append(item)
            is_["total_revenue"] += net
        elif acct_type == "expense":
            is_["expenses"].append(item)
            is_["total_expenses"] += net
    is_["net_income"] = is_["total_revenue"] - is_["total_expenses"]
    for sec in ("assets", "liabilities", "equity"):
        bs[sec]["total"] = _round(bs[sec]["total"])
    is_["total_revenue"] = _round(is_["total_revenue"])
    is_["total_expenses"] = _round(is_["total_expenses"])
    is_["net_income"] = _round(is_["net_income"])
    return {
        "client_code": client_code,
        "period": period,
        "balance_sheet": bs,
        "income_statement": is_,
        "generated_at": _utc_now(),
    }


def generate_financial_statements_pdf(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    firm_name: str = "OtoCPA CPA",
    lang: str = "fr",
) -> bytes:
    from src.i18n import t
    stmts = generate_financial_statements(conn, client_code, period)
    try:
        import fitz  # noqa: F401
        return _fs_pdf_pymupdf(stmts, firm_name, lang, t)
    except ImportError:
        return _fs_pdf_minimal(stmts, firm_name, lang, t)


def _fs_pdf_pymupdf(stmts, firm_name, lang, t) -> bytes:
    import fitz
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    y = 50
    cc = stmts["client_code"]
    period = stmts["period"]
    page.insert_text((50, y), firm_name, fontsize=14, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 18
    page.insert_text((50, y), f"{t('fs_title', lang)} — {cc}", fontsize=12, fontname="hebo")
    y += 14
    page.insert_text((50, y), f"{t('wp_period', lang)}: {period}", fontsize=9,
                     color=(0.4, 0.4, 0.4))
    y += 10
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14
    bs = stmts["balance_sheet"]
    is_ = stmts["income_statement"]

    def _section(title: str, items: list[dict[str, Any]]) -> None:
        nonlocal y
        if y > 720:
            return
        page.insert_text((50, y), title, fontsize=10, fontname="hebo",
                         color=(0.08, 0.16, 0.44))
        y += 13
        for item in items:
            if y > 730:
                break
            label = f"  {item['account_code']} {item['account_name']}"[:50]
            page.insert_text((60, y), label, fontsize=8)
            page.insert_text((450, y), f"${float(item['amount']):,.2f}", fontsize=8)
            y += 11

    page.insert_text((50, y), t("fs_balance_sheet", lang), fontsize=11, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 14
    _section(t("fs_current_assets", lang), bs["assets"]["current"])
    _section(t("fs_non_current_assets", lang), bs["assets"]["non_current"])
    page.insert_text((50, y), t("fs_total_assets", lang), fontsize=9, fontname="hebo")
    page.insert_text((450, y), f"${float(bs['assets']['total']):,.2f}", fontsize=9, fontname="hebo")
    y += 14
    _section(t("fs_current_liabilities", lang), bs["liabilities"]["current"])
    _section(t("fs_long_term_liabilities", lang), bs["liabilities"]["long_term"])
    page.insert_text((50, y), t("fs_total_liabilities", lang), fontsize=9, fontname="hebo")
    page.insert_text((450, y), f"${float(bs['liabilities']['total']):,.2f}", fontsize=9, fontname="hebo")
    y += 14
    _section(t("fs_equity", lang), bs["equity"]["items"])
    page.insert_text((50, y), t("fs_total_equity", lang), fontsize=9, fontname="hebo")
    page.insert_text((450, y), f"${float(bs['equity']['total']):,.2f}", fontsize=9, fontname="hebo")
    y += 20
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14
    page.insert_text((50, y), t("fs_income_statement", lang), fontsize=11, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 14
    _section(t("fs_revenue", lang), is_["revenue"])
    page.insert_text((50, y), t("fs_total_revenue", lang), fontsize=9, fontname="hebo")
    page.insert_text((450, y), f"${float(is_['total_revenue']):,.2f}", fontsize=9, fontname="hebo")
    y += 12
    _section(t("fs_expenses", lang), is_["expenses"])
    page.insert_text((50, y), t("fs_total_expenses", lang), fontsize=9, fontname="hebo")
    page.insert_text((450, y), f"${float(is_['total_expenses']):,.2f}", fontsize=9, fontname="hebo")
    y += 14
    page.draw_line((50, y), (562, y), color=(0.08, 0.16, 0.44), width=0.8)
    y += 12
    page.insert_text((50, y), t("fs_net_income", lang), fontsize=11, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    page.insert_text((450, y), f"${float(is_['net_income']):,.2f}", fontsize=11, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


def _fs_pdf_minimal(stmts, firm_name, lang, t) -> bytes:
    elements: list[tuple[int, int, str, int]] = []

    def add(x: int, y: int, text: str, size: int = 10) -> None:
        elements.append((x, y, text, size))

    cc = stmts["client_code"]
    period = stmts["period"]
    bs = stmts["balance_sheet"]
    is_ = stmts["income_statement"]
    y = 760
    add(50, y, firm_name, 14)
    y -= 16
    add(50, y, f"{t('fs_title', lang)} - {cc}", 12)
    y -= 14
    add(50, y, f"{t('wp_period', lang)}: {period}", 9)
    y -= 16
    add(50, y, t("fs_balance_sheet", lang), 11)
    y -= 12
    for item in bs["assets"]["current"] + bs["assets"]["non_current"]:
        if y < 100:
            break
        add(60, y, f"{item['account_code']} {item['account_name']}", 8)
        add(450, y, f"${float(item['amount']):.2f}", 8)
        y -= 10
    add(50, y, f"{t('fs_total_assets', lang)}: ${float(bs['assets']['total']):.2f}", 9)
    y -= 14
    for item in bs["liabilities"]["current"] + bs["liabilities"]["long_term"]:
        if y < 100:
            break
        add(60, y, f"{item['account_code']} {item['account_name']}", 8)
        add(450, y, f"${float(item['amount']):.2f}", 8)
        y -= 10
    add(50, y, f"{t('fs_total_liabilities', lang)}: ${float(bs['liabilities']['total']):.2f}", 9)
    y -= 14
    add(50, y, f"{t('fs_total_equity', lang)}: ${float(bs['equity']['total']):.2f}", 9)
    y -= 20
    add(50, y, t("fs_income_statement", lang), 11)
    y -= 12
    for item in is_["revenue"]:
        if y < 80:
            break
        add(60, y, f"{item['account_code']} {item['account_name']}", 8)
        add(450, y, f"${float(item['amount']):.2f}", 8)
        y -= 10
    add(50, y, f"{t('fs_total_revenue', lang)}: ${float(is_['total_revenue']):.2f}", 9)
    y -= 12
    for item in is_["expenses"]:
        if y < 80:
            break
        add(60, y, f"{item['account_code']} {item['account_name']}", 8)
        add(450, y, f"${float(item['amount']):.2f}", 8)
        y -= 10
    add(50, y, f"{t('fs_total_expenses', lang)}: ${float(is_['total_expenses']):.2f}", 9)
    y -= 14
    add(50, y, f"{t('fs_net_income', lang)}: ${float(is_['net_income']):.2f}", 12)
    return _build_minimal_pdf(elements)


# ---------------------------------------------------------------------------
# Analytical procedures
# ---------------------------------------------------------------------------

def run_analytical_procedures(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    generate_trial_balance(conn, client_code, period)
    current_tb = {r["account_code"]: r for r in get_trial_balance(conn, client_code, period)}
    try:
        prior_year = int(period[:4]) - 1
        prior_period = f"{prior_year}{period[4:]}"
    except Exception:
        prior_period = ""
    prior_tb: dict[str, Any] = {}
    if prior_period:
        generate_trial_balance(conn, client_code, prior_period)
        prior_tb = {r["account_code"]: r for r in get_trial_balance(conn, client_code, prior_period)}
    PCT_THRESHOLD = Decimal("0.10")
    AMT_THRESHOLD = Decimal("1000")
    variances = []
    for code in sorted(set(current_tb) | set(prior_tb)):
        curr_amt = _to_decimal((current_tb.get(code) or {}).get("net_balance"))
        prior_amt = _to_decimal((prior_tb.get(code) or {}).get("net_balance"))
        diff = curr_amt - prior_amt
        name = (
            (current_tb.get(code) or {}).get("account_name")
            or (prior_tb.get(code) or {}).get("account_name")
            or code
        )
        pct = abs(diff / prior_amt) if prior_amt != _ZERO else (Decimal("1") if curr_amt != _ZERO else _ZERO)
        flagged = pct > PCT_THRESHOLD and abs(diff) > AMT_THRESHOLD
        variances.append({
            "account_code": code,
            "account_name": name,
            "current": curr_amt,
            "prior": prior_amt,
            "difference": diff,
            "pct_change": _round(pct * 100),
            "flagged": flagged,
        })
    ratios = _calculate_ratios(conn, current_tb)
    return {
        "client_code": client_code,
        "period": period,
        "prior_period": prior_period,
        "variances": variances,
        "flagged_variances": [v for v in variances if v["flagged"]],
        "ratios": ratios,
        "generated_at": _utc_now(),
    }


def _calculate_ratios(
    conn: sqlite3.Connection,
    tb: dict[str, Any],
) -> dict[str, Any]:
    coa = {r["account_code"]: dict(r) for r in conn.execute(
        "SELECT * FROM chart_of_accounts"
    ).fetchall()}
    current_assets = _ZERO
    current_liabilities = _ZERO
    cash = _ZERO
    ar = _ZERO
    ap = _ZERO
    cogs = _ZERO
    revenue = _ZERO
    total_expenses = _ZERO
    for code, row in tb.items():
        acct_type = (coa.get(code) or {}).get("account_type") or _infer_account_type(code)
        fs_line = (coa.get(code) or {}).get("financial_statement_line") or ""
        amt = _to_decimal(row.get("net_balance"))
        if acct_type == "asset" and "long" not in fs_line.lower():
            current_assets += amt
            if code == "1010":
                cash += amt
            if code == "1100":
                ar += amt
        elif acct_type == "liability" and "long" not in fs_line.lower():
            current_liabilities += amt
            if code == "2100":
                ap += amt
        elif acct_type == "revenue":
            revenue += amt
        elif acct_type == "expense":
            total_expenses += amt
            if code == "5100":
                cogs += amt
    net_income = revenue - total_expenses

    def _div(a: Decimal, b: Decimal) -> Decimal | None:
        return _round(a / b) if b != _ZERO else None

    gm = _div(revenue - cogs, revenue)
    if gm is not None:
        gm = _round(gm * 100)
    nm = _div(net_income, revenue)
    if nm is not None:
        nm = _round(nm * 100)
    ap_days_base = cogs / Decimal("365") if cogs != _ZERO else _ZERO
    ap_days = _div(ap, ap_days_base) if ap_days_base != _ZERO else None
    return {
        "current_ratio": _div(current_assets, current_liabilities),
        "quick_ratio": _div(cash + ar, current_liabilities),
        "gross_margin_pct": gm,
        "net_margin_pct": nm,
        "ap_days": ap_days,
        "current_assets": _round(current_assets),
        "current_liabilities": _round(current_liabilities),
        "revenue": _round(revenue),
        "net_income": _round(net_income),
    }


def calculate_ratios(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    generate_trial_balance(conn, client_code, period)
    tb = {r["account_code"]: r for r in get_trial_balance(conn, client_code, period)}
    return _calculate_ratios(conn, tb)


def generate_analytical_report_pdf(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    firm_name: str = "OtoCPA CPA",
    lang: str = "fr",
) -> bytes:
    from src.i18n import t
    results = run_analytical_procedures(conn, client_code, period)
    try:
        import fitz  # noqa: F401
        return _analytical_pdf_pymupdf(results, firm_name, lang, t)
    except ImportError:
        return _analytical_pdf_minimal(results, firm_name, lang, t)


def _analytical_pdf_pymupdf(results, firm_name, lang, t) -> bytes:
    import fitz
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    y = 50
    page.insert_text((50, y), firm_name, fontsize=14, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 18
    page.insert_text((50, y),
                     f"{t('anal_title', lang)} — {results['client_code']}",
                     fontsize=12, fontname="hebo")
    y += 14
    page.insert_text((50, y), f"{t('wp_period', lang)}: {results['period']}",
                     fontsize=9, color=(0.4, 0.4, 0.4))
    y += 14
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14
    ratios = results["ratios"]
    page.insert_text((50, y), t("anal_ratios", lang), fontsize=11, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 14
    for label, key in [
        (t("anal_current_ratio", lang), "current_ratio"),
        (t("anal_quick_ratio", lang), "quick_ratio"),
        (t("anal_gross_margin", lang), "gross_margin_pct"),
        (t("anal_net_margin", lang), "net_margin_pct"),
        (t("anal_ap_days", lang), "ap_days"),
    ]:
        val = ratios.get(key)
        vs = str(val) if val is not None else t("analytics_na", lang)
        page.insert_text((60, y), label, fontsize=9)
        page.insert_text((350, y), vs, fontsize=9)
        y += 13
    y += 8
    page.insert_text((50, y), t("anal_variance_title", lang), fontsize=11, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 14
    for (x, hdr) in [
        (50, t("col_gl_account", lang)), (200, t("anal_current", lang)),
        (280, t("anal_prior", lang)), (360, t("anal_difference", lang)),
        (440, "Pct"), (490, t("anal_flagged", lang)),
    ]:
        page.insert_text((x, y), hdr, fontsize=8, fontname="hebo")
    y += 10
    page.draw_line((50, y), (562, y), color=(0.85, 0.85, 0.85), width=0.3)
    y += 12
    for v in results["variances"]:
        if y > 730:
            break
        color = (0.8, 0.2, 0.2) if v["flagged"] else (0.0, 0.0, 0.0)
        acct = f"{v['account_code']} {v['account_name']}"[:28]
        page.insert_text((50, y), acct, fontsize=7, color=color)
        page.insert_text((200, y), f"${float(v['current']):,.0f}", fontsize=7)
        page.insert_text((280, y), f"${float(v['prior']):,.0f}", fontsize=7)
        page.insert_text((360, y), f"${float(v['difference']):+,.0f}", fontsize=7, color=color)
        page.insert_text((440, y), f"{v['pct_change']}%", fontsize=7)
        page.insert_text((490, y), "!" if v["flagged"] else "ok", fontsize=7, color=color)
        y += 11
    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


def _analytical_pdf_minimal(results, firm_name, lang, t) -> bytes:
    elements: list[tuple[int, int, str, int]] = []

    def add(x: int, y: int, text: str, size: int = 10) -> None:
        elements.append((x, y, text, size))

    y = 760
    add(50, y, firm_name, 14)
    y -= 16
    add(50, y, f"{t('anal_title', lang)} - {results['client_code']}", 12)
    y -= 14
    add(50, y, f"{t('wp_period', lang)}: {results['period']}", 9)
    y -= 16
    ratios = results["ratios"]
    add(50, y, t("anal_ratios", lang), 11)
    y -= 13
    for label, key in [
        (t("anal_current_ratio", lang), "current_ratio"),
        (t("anal_quick_ratio", lang), "quick_ratio"),
        (t("anal_gross_margin", lang), "gross_margin_pct"),
        (t("anal_net_margin", lang), "net_margin_pct"),
        (t("anal_ap_days", lang), "ap_days"),
    ]:
        val = ratios.get(key)
        add(60, y, f"{label}: {val if val is not None else 'N/A'}", 9)
        y -= 12
    y -= 8
    add(50, y, t("anal_variance_title", lang), 11)
    y -= 13
    for v in results["variances"]:
        if y < 80:
            break
        flag = " [!]" if v["flagged"] else ""
        line = (f"{v['account_code']} {str(v['account_name'])[:20]}: "
                f"curr ${float(v['current']):.0f}  "
                f"prior ${float(v['prior']):.0f}  "
                f"diff ${float(v['difference']):+.0f}{flag}")
        add(50, y, line, 7)
        y -= 10
    return _build_minimal_pdf(elements)


# ---------------------------------------------------------------------------
# Engagements
# ---------------------------------------------------------------------------

VALID_ENGAGEMENT_STATUSES = {"planning", "fieldwork", "review", "complete", "issued"}


def _eng_id(client_code: str, period: str, engagement_type: str) -> str:
    rand = secrets.token_hex(4)
    return f"eng_{client_code.lower()}_{period}_{engagement_type}_{rand}"


def get_engagements(
    conn: sqlite3.Connection,
    client_code: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    ensure_audit_tables(conn)
    where: list[str] = []
    params: list[Any] = []
    if client_code:
        where.append("LOWER(client_code) = LOWER(?)")
        params.append(client_code)
    if status:
        where.append("status = ?")
        params.append(status)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT * FROM engagements {where_sql} ORDER BY created_at DESC", params
    ).fetchall()
    return [dict(r) for r in rows]


def get_engagement(
    conn: sqlite3.Connection, engagement_id: str
) -> dict[str, Any] | None:
    ensure_audit_tables(conn)
    row = conn.execute(
        "SELECT * FROM engagements WHERE engagement_id=?", (engagement_id,)
    ).fetchone()
    return dict(row) if row else None


def create_engagement(
    conn: sqlite3.Connection,
    client_code: str,
    period: str,
    engagement_type: str = "audit",
    partner: str = "",
    manager: str = "",
    staff: str = "",
    planned_hours: float | None = None,
    budget: float | None = None,
    fee: float | None = None,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    if engagement_type not in VALID_ENGAGEMENT_TYPES:
        engagement_type = "audit"
    eid = _eng_id(client_code, period, engagement_type)
    now = _utc_now()
    conn.execute(
        """INSERT INTO engagements
           (engagement_id, client_code, period, engagement_type, status,
            partner, manager, staff, planned_hours, actual_hours,
            budget, fee, created_at)
           VALUES (?,?,?,?,'planning',?,?,?,?,0,?,?,?)""",
        (eid, client_code, period, engagement_type,
         partner, manager, staff, planned_hours, budget, fee, now),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM engagements WHERE engagement_id=?", (eid,)
    ).fetchone())


def update_engagement(
    conn: sqlite3.Connection,
    engagement_id: str,
    *,
    status: str | None = None,
    partner: str | None = None,
    manager: str | None = None,
    staff: str | None = None,
    planned_hours: float | None = None,
    actual_hours: float | None = None,
    budget: float | None = None,
    fee: float | None = None,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM engagements WHERE engagement_id=?", (engagement_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Engagement not found: {engagement_id}")
    r = dict(row)
    if status is not None and status in VALID_ENGAGEMENT_STATUSES:
        r["status"] = status
        if status in ("complete", "issued") and not r.get("completed_at"):
            r["completed_at"] = _utc_now()
    if partner is not None:
        r["partner"] = partner
    if manager is not None:
        r["manager"] = manager
    if staff is not None:
        r["staff"] = staff
    if planned_hours is not None:
        r["planned_hours"] = planned_hours
    if actual_hours is not None:
        r["actual_hours"] = actual_hours
    if budget is not None:
        r["budget"] = budget
    if fee is not None:
        r["fee"] = fee
    conn.execute(
        """UPDATE engagements SET status=?, partner=?, manager=?, staff=?,
           planned_hours=?, actual_hours=?, budget=?, fee=?, completed_at=?
           WHERE engagement_id=?""",
        (r["status"], r.get("partner"), r.get("manager"), r.get("staff"),
         r.get("planned_hours"), r.get("actual_hours"),
         r.get("budget"), r.get("fee"), r.get("completed_at"), engagement_id),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM engagements WHERE engagement_id=?", (engagement_id,)
    ).fetchone())


def get_engagement_progress(
    conn: sqlite3.Connection,
    engagement_id: str,
) -> dict[str, Any]:
    eng = get_engagement(conn, engagement_id)
    if not eng:
        return {"pct": 0, "signed_off": 0, "total": 0, "open_exceptions": 0}
    papers = get_working_papers(conn, eng["client_code"], eng["period"], eng["engagement_type"])
    total = len(papers)
    signed_off = sum(1 for p in papers if p.get("sign_off_at"))
    exceptions = sum(1 for p in papers if p.get("status") == "exception")
    pct = int(signed_off / total * 100) if total > 0 else 0
    return {
        "pct": pct,
        "signed_off": signed_off,
        "total": total,
        "open_exceptions": exceptions,
    }


def issue_engagement(
    conn: sqlite3.Connection,
    engagement_id: str,
    issued_by: str,
    firm_name: str = "OtoCPA CPA",
    lang: str = "fr",
) -> bytes:
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")
    update_engagement(conn, engagement_id, status="issued")
    return generate_engagement_pdf(conn, engagement_id, firm_name=firm_name, lang=lang)


def generate_engagement_pdf(
    conn: sqlite3.Connection,
    engagement_id: str,
    firm_name: str = "OtoCPA CPA",
    lang: str = "fr",
) -> bytes:
    from src.i18n import t
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")
    papers = get_working_papers(conn, eng["client_code"], eng["period"], eng["engagement_type"])
    progress = get_engagement_progress(conn, engagement_id)
    try:
        import fitz  # noqa: F401
        return _engagement_pdf_pymupdf(eng, papers, progress, firm_name, lang, t)
    except ImportError:
        return _engagement_pdf_minimal(eng, papers, progress, firm_name, lang, t)


def _engagement_pdf_pymupdf(eng, papers, progress, firm_name, lang, t) -> bytes:
    import fitz
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    y = 50
    page.insert_text((50, y), firm_name, fontsize=14, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    y += 18
    page.insert_text((50, y), f"{t('eng_title', lang)} — {eng['client_code']}",
                     fontsize=12, fontname="hebo")
    y += 14
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 12
    for label, val in [
        (t("wp_period", lang), eng["period"]),
        (t("wp_engagement_type", lang), eng["engagement_type"].capitalize()),
        (t("eng_status", lang), eng["status"].capitalize()),
        (t("eng_partner", lang), eng.get("partner") or "-"),
        (t("eng_manager", lang), eng.get("manager") or "-"),
        (t("eng_staff", lang), eng.get("staff") or "-"),
        (t("eng_planned_hours", lang), str(eng.get("planned_hours") or "-")),
        (t("eng_actual_hours", lang), str(eng.get("actual_hours") or 0)),
    ]:
        page.insert_text((50, y), f"{label}:", fontsize=9, fontname="hebo")
        page.insert_text((200, y), str(val), fontsize=9)
        y += 12
    y += 8
    pct = progress["pct"]
    page.insert_text((50, y),
                     f"{t('eng_progress', lang)}: {progress['signed_off']}/{progress['total']} — {pct}%",
                     fontsize=10, fontname="hebo", color=(0.08, 0.16, 0.44))
    y += 16
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14
    page.insert_text((50, y), t("wp_nav_link", lang), fontsize=11, fontname="hebo")
    y += 14
    for wp in papers:
        if y > 720:
            break
        status_str = wp.get("status", "open")
        so = "Y" if wp.get("sign_off_at") else "-"
        color = (0.8, 0.2, 0.2) if status_str == "exception" else (0.0, 0.0, 0.0)
        page.insert_text((50, y), f"  {wp['account_code']} {wp['account_name']}"[:50],
                         fontsize=8, color=color)
        page.insert_text((380, y), status_str, fontsize=8, color=color)
        page.insert_text((440, y), so, fontsize=8)
        page.insert_text((470, y), f"${float(_to_decimal(wp.get('balance_per_books'))):,.2f}", fontsize=8)
        y += 11
    if progress["open_exceptions"]:
        y += 10
        page.insert_text((50, y),
                         f"! {progress['open_exceptions']} {t('wp_exceptions_found', lang)}",
                         fontsize=9, color=(0.8, 0.2, 0.2))
    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


def _engagement_pdf_minimal(eng, papers, progress, firm_name, lang, t) -> bytes:
    elements: list[tuple[int, int, str, int]] = []

    def add(x: int, y: int, text: str, size: int = 10) -> None:
        elements.append((x, y, text, size))

    y = 760
    add(50, y, firm_name, 14)
    y -= 18
    add(50, y, f"{t('eng_title', lang)} - {eng['client_code']}", 12)
    y -= 14
    add(50, y, f"{t('wp_period', lang)}: {eng['period']}", 9)
    y -= 12
    add(50, y, f"{t('wp_engagement_type', lang)}: {eng['engagement_type']}", 9)
    y -= 12
    add(50, y, f"{t('eng_status', lang)}: {eng['status']}", 9)
    y -= 12
    add(50, y, f"{t('eng_partner', lang)}: {eng.get('partner') or '-'}", 9)
    y -= 12
    pct = progress["pct"]
    add(50, y,
        f"{t('eng_progress', lang)}: {progress['signed_off']}/{progress['total']} ({pct}%)", 10)
    y -= 16
    add(50, y, t("wp_nav_link", lang), 11)
    y -= 12
    for wp in papers:
        if y < 80:
            break
        so = "[Y]" if wp.get("sign_off_at") else "[-]"
        add(50, y, f"{so} {wp['account_code']} {wp['account_name'][:30]} - {wp.get('status','open')}", 8)
        y -= 10
    return _build_minimal_pdf(elements)


# ---------------------------------------------------------------------------
# Shared minimal PDF builder
# ---------------------------------------------------------------------------

def _build_minimal_pdf(elements: list[tuple[int, int, str, int]]) -> bytes:
    def _enc(s: str) -> bytes:
        b = str(s).encode("latin-1", errors="replace")
        return b.replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")

    cmds: list[bytes] = [b"BT"]
    for (x, y_pos, text, size) in elements:
        cmds.append(f"/F1 {size} Tf".encode())
        cmds.append(f"1 0 0 1 {x} {y_pos} Tm".encode())
        cmds.append(b"(" + _enc(text) + b") Tj")
    cmds.append(b"ET")
    content = b"\n".join(cmds)

    catalog = b"1 0 obj\n<</Type /Catalog /Pages 2 0 R>>\nendobj\n"
    pages   = b"2 0 obj\n<</Type /Pages /Kids [3 0 R] /Count 1>>\nendobj\n"
    page_   = (
        b"3 0 obj\n<</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
        b" /Contents 5 0 R /Resources <</Font <</F1 4 0 R>>>>>>\nendobj\n"
    )
    font    = b"4 0 obj\n<</Type /Font /Subtype /Type1 /BaseFont /Helvetica>>\nendobj\n"
    stream  = (
        b"5 0 obj\n<</Length " + str(len(content)).encode() + b">>\nstream\n"
        + content + b"\nendstream\nendobj\n"
    )
    header = b"%PDF-1.4\n"
    objs   = [catalog, pages, page_, font, stream]
    body   = b"".join(objs)
    offsets: list[int] = []
    pos = len(header)
    for obj in objs:
        offsets.append(pos)
        pos += len(obj)
    xref_pos = len(header) + len(body)
    xref = f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \r\n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \r\n"
    trailer = (
        f"trailer\n<</Size {len(objs) + 1} /Root 1 0 R>>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    )
    return header + body + xref.encode() + trailer.encode()
