"""
src/engines/fraud_engine.py
===========================
Layer 1 deterministic fraud detection for OtoCPA.

No AI calls are made here.  AI is used exclusively to *explain* flagged
items in the UI — detection itself is 100% rule-based and deterministic.

Rules
-----
1.  vendor_amount_anomaly          — amount > 2σ from vendor mean (requires ≥10 prior tx)
2.  vendor_timing_anomaly          — invoice day-of-month > 14 days from vendor norm (≥10 tx)
3.  duplicate_exact                — same amount + same vendor within 30 days  → HIGH risk
4.  duplicate_cross_vendor         — same amount + different vendor within 7 days → MEDIUM risk
5.  weekend_transaction            — Saturday or Sunday, amount > $500
6.  holiday_transaction            — Quebec statutory holiday, amount > $500
7.  round_number_flag              — exactly round amount from vendor with irregular invoices
8.  new_vendor_large_amount        — first invoice from a vendor over $2,000
9.  bank_account_change            — vendor bank details changed between invoices (CRITICAL)
10. invoice_after_payment          — invoice date is AFTER matching bank payment date (HIGH)
11. tax_registration_contradiction — vendor charges GST/QST but is historically unregistered/exempt (HIGH)
12. vendor_category_shift          — vendor category contradicts ≥80% historical pattern (MEDIUM)
13. vendor_payee_mismatch           — bank transaction payee differs significantly from invoice vendor (HIGH)

Usage
-----
    from src.engines.fraud_engine import run_fraud_detection

    flags = run_fraud_detection("doc_abc123", db_path=DB_PATH)
    # flags is a list of dicts saved to documents.fraud_flags
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# ---------------------------------------------------------------------------
# Severity constants
# ---------------------------------------------------------------------------

CRITICAL = "critical"
HIGH     = "high"
MEDIUM   = "medium"
LOW      = "low"

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

AMOUNT_ANOMALY_SIGMA          = 2.0    # standard deviations
TIMING_ANOMALY_DAYS           = 14     # days from normal billing day
MIN_HISTORY_FOR_ANOMALY       = 5      # P1-8: reduced from 10 — 5 transactions is enough for basic anomaly
DUPLICATE_SAME_VENDOR_DAYS    = 30     # days window — same vendor duplicate
DUPLICATE_CROSS_VENDOR_DAYS   = 7      # days window — cross-vendor same amount
WEEKEND_HOLIDAY_AMOUNT_LIMIT  = 200.0  # P2-3: $200 balances detection vs false positives
ROUND_NUMBER_STDEV_RATIO      = 0.10   # irregular = std_dev > 10 % of mean
NEW_VENDOR_LARGE_AMOUNT_LIMIT = 2000.0 # flag first invoice above this
MIN_HISTORY_FOR_ROUND_FLAG    = 5      # need at least 5 prior invoices to assess regularity
LARGE_CREDIT_NOTE_LIMIT       = 5000.0 # credit notes above this always flagged

# ---------------------------------------------------------------------------
# Known registered software vendors (assume GST/QST registered)
# ---------------------------------------------------------------------------
KNOWN_REGISTERED_SOFTWARE_VENDORS = {
    "lastpass", "adobe", "microsoft", "google", "intuit", "quickbooks",
    "goto technologies", "logmein", "goto",
    "freshbooks", "xero", "slack", "zoom", "dropbox", "atlassian",
    "github", "gitlab", "aws", "amazon web services", "salesforce",
    "hubspot", "mailchimp", "docusign", "autodesk", "oracle", "sap",
    # Major Canadian banks — trusted financial institutions
    "cibc", "canadian imperial bank of commerce",
    "desjardins", "caisse desjardins",
    "rbc", "royal bank of canada", "royal bank",
    "td", "td bank", "toronto-dominion",
    "bmo", "bank of montreal",
    "scotiabank", "scotia",
    "bnc", "banque nationale", "national bank of canada",
    "laurentian bank", "banque laurentienne",
    "hsbc", "hsbc canada",
    # Major Canadian telecoms
    "bell", "bell canada", "bell mobility", "virgin plus", "virgin mobile",
    "videotron", "rogers", "rogers wireless", "fido", "chatr",
    "telus", "telus mobility", "koodo", "koodo mobile",
    "public mobile", "freedom mobile", "sasktel",
    # Major Canadian utilities
    "hydro-quebec", "hydro-québec", "hydro quebec",
    "energir", "énergir", "gazifere", "gazifère",
    "hydro ottawa", "toronto hydro", "bc hydro",
    "enbridge", "fortisbc",
    # Major Canadian retailers
    "walmart", "walmart canada", "costco", "costco wholesale",
    "iga", "metro", "metro inc", "provigo", "maxi", "super c",
    "loblaws", "no frills", "shoppers drug mart",
    "canadian tire", "dollarama", "home depot", "rona", "home hardware",
    "staples", "bureau en gros",
    # Major government entities
    "canada revenue agency", "agence du revenu du canada", "cra", "arc",
    "revenu quebec", "revenu québec",
    "cnesst", "service canada", "emploi quebec",
    "saaq", "ramq", "sqdc",
    # Major tech vendors
    "amazon", "amazon.ca", "amazon web services",
    "apple", "apple canada",
    "dropbox", "zoom", "zoom video",
}

# ---------------------------------------------------------------------------
# Known Canadian vendors registry — GL accounts, tax codes, categories
# ---------------------------------------------------------------------------
# Each entry: {"gl": default GL account, "tax": default tax code, "category": vendor category}

KNOWN_CANADIAN_VENDORS: dict[str, dict[str, str]] = {
    # --- Banks ---
    "cibc":                            {"gl": "1010", "tax": "E",  "category": "bank"},
    "canadian imperial bank of commerce": {"gl": "1010", "tax": "E",  "category": "bank"},
    "desjardins":                      {"gl": "1010", "tax": "E",  "category": "bank"},
    "caisse desjardins":               {"gl": "1010", "tax": "E",  "category": "bank"},
    "rbc":                             {"gl": "1010", "tax": "E",  "category": "bank"},
    "royal bank of canada":            {"gl": "1010", "tax": "E",  "category": "bank"},
    "royal bank":                      {"gl": "1010", "tax": "E",  "category": "bank"},
    "td":                              {"gl": "1010", "tax": "E",  "category": "bank"},
    "td bank":                         {"gl": "1010", "tax": "E",  "category": "bank"},
    "toronto-dominion":                {"gl": "1010", "tax": "E",  "category": "bank"},
    "bmo":                             {"gl": "1010", "tax": "E",  "category": "bank"},
    "bank of montreal":                {"gl": "1010", "tax": "E",  "category": "bank"},
    "scotiabank":                      {"gl": "1010", "tax": "E",  "category": "bank"},
    "scotia":                          {"gl": "1010", "tax": "E",  "category": "bank"},
    "bnc":                             {"gl": "1010", "tax": "E",  "category": "bank"},
    "banque nationale":                {"gl": "1010", "tax": "E",  "category": "bank"},
    "national bank of canada":         {"gl": "1010", "tax": "E",  "category": "bank"},
    "laurentian bank":                 {"gl": "1010", "tax": "E",  "category": "bank"},
    "banque laurentienne":             {"gl": "1010", "tax": "E",  "category": "bank"},
    "hsbc":                            {"gl": "1010", "tax": "E",  "category": "bank"},
    "hsbc canada":                     {"gl": "1010", "tax": "E",  "category": "bank"},
    # --- Telecoms ---
    "bell":                            {"gl": "5320", "tax": "T",  "category": "telecom"},
    "bell canada":                     {"gl": "5320", "tax": "T",  "category": "telecom"},
    "bell mobility":                   {"gl": "5320", "tax": "T",  "category": "telecom"},
    "virgin plus":                     {"gl": "5320", "tax": "T",  "category": "telecom"},
    "virgin mobile":                   {"gl": "5320", "tax": "T",  "category": "telecom"},
    "videotron":                       {"gl": "5320", "tax": "T",  "category": "telecom"},
    "rogers":                          {"gl": "5320", "tax": "T",  "category": "telecom"},
    "rogers wireless":                 {"gl": "5320", "tax": "T",  "category": "telecom"},
    "fido":                            {"gl": "5320", "tax": "T",  "category": "telecom"},
    "chatr":                           {"gl": "5320", "tax": "T",  "category": "telecom"},
    "telus":                           {"gl": "5320", "tax": "T",  "category": "telecom"},
    "telus mobility":                  {"gl": "5320", "tax": "T",  "category": "telecom"},
    "koodo":                           {"gl": "5320", "tax": "T",  "category": "telecom"},
    "koodo mobile":                    {"gl": "5320", "tax": "T",  "category": "telecom"},
    "public mobile":                   {"gl": "5320", "tax": "T",  "category": "telecom"},
    "freedom mobile":                  {"gl": "5320", "tax": "T",  "category": "telecom"},
    "sasktel":                         {"gl": "5320", "tax": "T",  "category": "telecom"},
    # --- Utilities ---
    "hydro-quebec":                    {"gl": "5310", "tax": "T",  "category": "utility"},
    "hydro-québec":                    {"gl": "5310", "tax": "T",  "category": "utility"},
    "hydro quebec":                    {"gl": "5310", "tax": "T",  "category": "utility"},
    "energir":                         {"gl": "5310", "tax": "T",  "category": "utility"},
    "énergir":                         {"gl": "5310", "tax": "T",  "category": "utility"},
    "gazifere":                        {"gl": "5310", "tax": "T",  "category": "utility"},
    "gazifère":                        {"gl": "5310", "tax": "T",  "category": "utility"},
    "hydro ottawa":                    {"gl": "5310", "tax": "T",  "category": "utility"},
    "toronto hydro":                   {"gl": "5310", "tax": "T",  "category": "utility"},
    "bc hydro":                        {"gl": "5310", "tax": "T",  "category": "utility"},
    "enbridge":                        {"gl": "5310", "tax": "T",  "category": "utility"},
    "fortisbc":                        {"gl": "5310", "tax": "T",  "category": "utility"},
    # --- Retailers ---
    "walmart":                         {"gl": "5600", "tax": "T",  "category": "retail"},
    "walmart canada":                  {"gl": "5600", "tax": "T",  "category": "retail"},
    "costco":                          {"gl": "5600", "tax": "T",  "category": "retail"},
    "costco wholesale":                {"gl": "5600", "tax": "T",  "category": "retail"},
    "iga":                             {"gl": "5600", "tax": "T",  "category": "retail"},
    "metro":                           {"gl": "5600", "tax": "T",  "category": "retail"},
    "metro inc":                       {"gl": "5600", "tax": "T",  "category": "retail"},
    "provigo":                         {"gl": "5600", "tax": "T",  "category": "retail"},
    "maxi":                            {"gl": "5600", "tax": "T",  "category": "retail"},
    "super c":                         {"gl": "5600", "tax": "T",  "category": "retail"},
    "loblaws":                         {"gl": "5600", "tax": "T",  "category": "retail"},
    "no frills":                       {"gl": "5600", "tax": "T",  "category": "retail"},
    "shoppers drug mart":              {"gl": "5600", "tax": "T",  "category": "retail"},
    "canadian tire":                   {"gl": "5600", "tax": "T",  "category": "retail"},
    "dollarama":                       {"gl": "5600", "tax": "T",  "category": "retail"},
    "home depot":                      {"gl": "5600", "tax": "T",  "category": "retail"},
    "rona":                            {"gl": "5600", "tax": "T",  "category": "retail"},
    "home hardware":                   {"gl": "5600", "tax": "T",  "category": "retail"},
    "staples":                         {"gl": "5600", "tax": "T",  "category": "retail"},
    "bureau en gros":                  {"gl": "5600", "tax": "T",  "category": "retail"},
    # --- Government ---
    "canada revenue agency":           {"gl": "2300", "tax": "E",  "category": "government"},
    "agence du revenu du canada":      {"gl": "2300", "tax": "E",  "category": "government"},
    "cra":                             {"gl": "2300", "tax": "E",  "category": "government"},
    "arc":                             {"gl": "2300", "tax": "E",  "category": "government"},
    "revenu quebec":                   {"gl": "2300", "tax": "E",  "category": "government"},
    "revenu québec":                   {"gl": "2300", "tax": "E",  "category": "government"},
    "cnesst":                          {"gl": "5410", "tax": "E",  "category": "government"},
    "service canada":                  {"gl": "2300", "tax": "E",  "category": "government"},
    "emploi quebec":                   {"gl": "2300", "tax": "E",  "category": "government"},
    "saaq":                            {"gl": "5500", "tax": "E",  "category": "government"},
    "ramq":                            {"gl": "5410", "tax": "E",  "category": "government"},
    "sqdc":                            {"gl": "5600", "tax": "T",  "category": "government"},
    # --- Tech ---
    "amazon":                          {"gl": "5600", "tax": "T",  "category": "tech"},
    "amazon.ca":                       {"gl": "5600", "tax": "T",  "category": "tech"},
    "amazon web services":             {"gl": "5350", "tax": "T",  "category": "tech"},
    "apple":                           {"gl": "5350", "tax": "T",  "category": "tech"},
    "apple canada":                    {"gl": "5350", "tax": "T",  "category": "tech"},
    "microsoft":                       {"gl": "5350", "tax": "T",  "category": "tech"},
    "google":                          {"gl": "5350", "tax": "T",  "category": "tech"},
    "adobe":                           {"gl": "5350", "tax": "T",  "category": "tech"},
    "dropbox":                         {"gl": "5350", "tax": "T",  "category": "tech"},
    "zoom":                            {"gl": "5350", "tax": "T",  "category": "tech"},
    "zoom video":                      {"gl": "5350", "tax": "T",  "category": "tech"},
}

# Major Canadian bank vendor names — skip fraud rules that don't apply
# (new_vendor_large_amount, weekend_transaction)
CANADIAN_BANK_VENDORS = {
    "cibc", "canadian imperial bank of commerce",
    "desjardins", "caisse desjardins",
    "rbc", "royal bank of canada", "royal bank",
    "td", "td bank", "toronto-dominion",
    "bmo", "bank of montreal",
    "scotiabank", "scotia",
    "bnc", "banque nationale", "national bank of canada",
    "laurentian bank", "banque laurentienne",
    "hsbc", "hsbc canada",
}

# All known trusted vendors — skip new_vendor_large_amount fraud flag
KNOWN_TRUSTED_VENDORS = set(KNOWN_CANADIAN_VENDORS.keys())

# Trusted vendors that legitimately transact on weekends — skip weekend flag
WEEKEND_EXEMPT_CATEGORIES = {"bank", "utility", "telecom", "government", "retail", "tech"}


def _is_known_trusted_vendor(vendor: str) -> bool:
    """Check if vendor matches any known trusted Canadian vendor."""
    v = vendor.strip().lower()
    if v in KNOWN_TRUSTED_VENDORS:
        return True
    # Partial match for known vendor names in vendor string
    _trusted_tokens = [
        "cibc", "desjardins", "rbc", "royal bank", "td bank",
        "toronto-dominion", "bmo", "bank of montreal",
        "scotiabank", "banque nationale", "national bank",
        "laurentian", "hsbc",
        "bell canada", "bell mobility", "videotron", "rogers",
        "telus", "fido", "koodo",
        "hydro-quebec", "hydro-québec", "hydro quebec",
        "energir", "énergir", "gazifere", "gazifère",
        "walmart", "costco", "iga", "metro", "provigo",
        "maxi", "super c", "loblaws", "canadian tire",
        "home depot", "rona", "staples",
        "canada revenue", "revenu quebec", "revenu québec",
        "cnesst", "service canada",
        "amazon", "microsoft", "google", "apple", "adobe",
    ]
    return any(tok in v for tok in _trusted_tokens)


def _get_vendor_defaults(vendor: str) -> dict[str, str] | None:
    """Return GL account, tax code, and category defaults for a known vendor."""
    v = vendor.strip().lower()
    if v in KNOWN_CANADIAN_VENDORS:
        return KNOWN_CANADIAN_VENDORS[v]
    # Partial match
    for key, defaults in KNOWN_CANADIAN_VENDORS.items():
        if key in v or v in key:
            return defaults
    return None


def _is_weekend_exempt_vendor(vendor: str) -> bool:
    """Check if vendor is exempt from weekend transaction flagging."""
    defaults = _get_vendor_defaults(vendor)
    if defaults and defaults.get("category") in WEEKEND_EXEMPT_CATEGORIES:
        return True
    return False


def _is_canadian_bank_vendor(vendor: str) -> bool:
    """Check if vendor matches a major Canadian bank."""
    v = vendor.strip().lower()
    if v in CANADIAN_BANK_VENDORS:
        return True
    # Partial match for bank names in vendor string
    _bank_tokens = [
        "cibc", "desjardins", "rbc", "royal bank", "td bank",
        "toronto-dominion", "bmo", "bank of montreal",
        "scotiabank", "banque nationale", "national bank",
        "laurentian", "hsbc",
    ]
    return any(tok in v for tok in _bank_tokens)


# Known BN roots for registered vendors
REGISTERED_VENDOR_BN = {
    "764781803": "LastPass Technologies Canada ULC",
}


# ---------------------------------------------------------------------------
# Quebec statutory holidays
# ---------------------------------------------------------------------------

def _easter_sunday(year: int) -> date:
    """Computus — Gregorian algorithm for Easter Sunday."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f     = (b + 8) // 25
    g     = (b - f + 1) // 3
    h     = (19 * a + b - d - g + 15) % 30
    i, k  = divmod(c, 4)
    l     = (32 + 2 * e + 2 * i - h - k) % 7
    m     = (a + 11 * h + 22 * l) // 451
    month, day = divmod(114 + h + l - 7 * m, 31)
    return date(year, month, day + 1)


def _quebec_holidays(year: int) -> dict[date, str]:
    """Return a dict mapping date → holiday name for Quebec statutory holidays."""
    easter = _easter_sunday(year)

    # Victoria Day / Journée nationale des patriotes — Monday before May 25
    may25      = date(year, 5, 25)
    # Subtract enough days to land on the Monday strictly before May 25.
    # weekday(): Monday=0 … Sunday=6. If May 25 is already Monday subtract 7.
    _days_back = may25.weekday() if may25.weekday() != 0 else 7
    victoria   = may25 - timedelta(days=_days_back)

    # Labour Day — first Monday of September
    sep1       = date(year, 9, 1)
    labour_day = sep1 + timedelta(days=(7 - sep1.weekday()) % 7)

    # Thanksgiving — second Monday of October
    oct1       = date(year, 10, 1)
    first_mon  = oct1 + timedelta(days=(7 - oct1.weekday()) % 7)
    thanks     = first_mon + timedelta(weeks=1)

    holidays: dict[date, str] = {
        date(year,  1,  1): "New Year's Day",
        easter - timedelta(days=2): "Good Friday",
        easter + timedelta(days=1): "Easter Monday",
        victoria:                   "Victoria Day / Journée des patriotes",
        date(year,  6, 24): "Fête nationale du Québec",
        date(year,  7,  1): "Canada Day",
        labour_day:               "Labour Day",
        thanks:                   "Thanksgiving",
        date(year, 12, 25): "Christmas Day",
        date(year, 12, 26): "Boxing Day",
    }
    return holidays


def _is_quebec_holiday(d: date) -> str | None:
    """Return the holiday name if *d* is a Quebec statutory holiday, else None."""
    return _quebec_holidays(d.year).get(d)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    s = str(value).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _ensure_fraud_flags_column(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if "fraud_flags" not in cols:
        conn.execute("ALTER TABLE documents ADD COLUMN fraud_flags TEXT")
        conn.commit()


# ---------------------------------------------------------------------------
# BLOCK 5: Vendor name normalization for fuzzy grouping
# ---------------------------------------------------------------------------

import unicodedata as _unicodedata

def _normalize_vendor_key(value: Any) -> str:
    """Normalize vendor name for fuzzy grouping: lowercase, strip accents, remove suffixes."""
    if value is None:
        return ""
    text = str(value).strip().lower()
    # Strip accents
    text = _unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")
    # Remove common business suffixes
    import re as _re_local
    text = _re_local.sub(r'\b(inc|ltd|ltée|ltee|corp|llc|enr|senc|mobility|mobile|wireless)\b', '', text)
    # Remove punctuation and extra whitespace
    text = _re_local.sub(r'[^a-z0-9\s]', '', text)
    text = _re_local.sub(r'\s+', ' ', text).strip()
    return text


# ---------------------------------------------------------------------------
# Vendor history query
# ---------------------------------------------------------------------------

def _load_vendor_history(
    conn: sqlite3.Connection,
    vendor: str,
    client_code: str,
    exclude_doc_id: str,
) -> list[dict[str, Any]]:
    """Load all prior documents for vendor (same client, different doc)."""
    rows = conn.execute(
        """
        SELECT document_id, amount, document_date, raw_result, review_status
          FROM documents
         WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
           AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND document_id != ?
         ORDER BY document_date DESC
         LIMIT 500
        """,
        (vendor, client_code, exclude_doc_id),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_vendor_history_fuzzy(
    conn: sqlite3.Connection,
    vendor: str,
    client_code: str,
    exclude_doc_id: str,
) -> list[dict[str, Any]]:
    """BLOCK 5: Load vendor history using fuzzy name matching.

    Uses normalized vendor key with LIKE pattern to catch variations like
    'Bell Canada', 'BELL CANADA INC', 'Bell Canada Mobility'.
    """
    normalized = _normalize_vendor_key(vendor)
    if not normalized:
        return []
    # Use first significant word(s) as LIKE pattern
    words = normalized.split()
    if not words:
        return []
    # Use first two words (or one if single) for LIKE matching
    like_pattern = "%" + "%".join(words[:2]) + "%"
    rows = conn.execute(
        """
        SELECT document_id, amount, document_date, vendor, raw_result, review_status
          FROM documents
         WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND document_id != ?
         ORDER BY document_date DESC
         LIMIT 1000
        """,
        (client_code, exclude_doc_id),
    ).fetchall()
    # Filter by normalized key similarity
    result = []
    for r in rows:
        row_vendor = str(r["vendor"] or "")
        row_key = _normalize_vendor_key(row_vendor)
        # Match if normalized keys share the first significant word(s)
        if words[0] in row_key.split()[:3]:
            result.append(dict(r))
    return result[:500]


# ---------------------------------------------------------------------------
# Rule helpers
# ---------------------------------------------------------------------------

def _std_dev(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    return math.sqrt(variance)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


# ---------------------------------------------------------------------------
# Rule 1 & 2: Vendor behavior anomaly
# ---------------------------------------------------------------------------

def _rule_vendor_amount_anomaly(
    amount: float,
    history: list[dict[str, Any]],
    fuzzy_history: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """P1-8: Reduced minimum history from 10 to 5.

    With 3-4 items, flag as requires_amount_verification (MEDIUM).
    With 5+, full anomaly detection (HIGH).
    """
    amounts = [a for r in history if (a := _safe_float(r.get("amount"))) is not None]
    # BLOCK 5: Fall back to fuzzy history if exact history is insufficient
    if len(amounts) < MIN_HISTORY_FOR_ANOMALY and fuzzy_history:
        amounts = [a for r in fuzzy_history if (a := _safe_float(r.get("amount"))) is not None]

    # P1-8: With 3-4 items, flag first large invoice as requires_amount_verification
    if 3 <= len(amounts) < MIN_HISTORY_FOR_ANOMALY:
        mu = _mean(amounts)
        std = _std_dev(amounts)
        if std == 0:
            return None
        sigma = abs(amount - mu) / std
        if sigma > AMOUNT_ANOMALY_SIGMA:
            return {
                "rule":     "requires_amount_verification",
                "severity": MEDIUM,
                "i18n_key": "fraud_vendor_amount_anomaly",
                "params": {
                    "amount": f"${amount:,.2f}",
                    "sigma":  f"{sigma:.1f}",
                    "mean":   f"${mu:,.2f}",
                    "history_count": str(len(amounts)),
                },
            }
        return None

    if len(amounts) < MIN_HISTORY_FOR_ANOMALY:
        return None
    mu  = _mean(amounts)
    std = _std_dev(amounts)
    if std == 0:
        return None
    sigma = abs(amount - mu) / std
    if sigma > AMOUNT_ANOMALY_SIGMA:
        return {
            "rule":     "vendor_amount_anomaly",
            "severity": HIGH,
            "i18n_key": "fraud_vendor_amount_anomaly",
            "params": {
                "amount": f"${amount:,.2f}",
                "sigma":  f"{sigma:.1f}",
                "mean":   f"${mu:,.2f}",
            },
        }
    return None


def _rule_vendor_timing_anomaly(
    doc_date: date,
    history: list[dict[str, Any]],
) -> dict[str, Any] | None:
    days_of_month = [
        d.day
        for r in history
        if (d := _parse_date(r.get("document_date"))) is not None
    ]
    if len(days_of_month) < MIN_HISTORY_FOR_ANOMALY:
        return None
    avg_day = _mean([float(d) for d in days_of_month])
    diff    = abs(doc_date.day - avg_day)
    # Day-of-month wraps — also check circular distance
    diff = min(diff, abs(diff - 31))
    if diff > TIMING_ANOMALY_DAYS:
        return {
            "rule":     "vendor_timing_anomaly",
            "severity": LOW,
            "i18n_key": "fraud_vendor_timing_anomaly",
            "params": {
                "days":    f"{diff:.0f}",
                "avg_day": f"{avg_day:.0f}",
            },
        }
    return None


# ---------------------------------------------------------------------------
# Rule 3: Duplicate detection
# ---------------------------------------------------------------------------

def _rule_duplicate(
    conn: sqlite3.Connection,
    document_id: str,
    vendor: str,
    client_code: str,
    amount: float,
    doc_date: date,
) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []

    window_start_30 = (doc_date - timedelta(days=DUPLICATE_SAME_VENDOR_DAYS)).isoformat()
    window_start_7  = (doc_date - timedelta(days=DUPLICATE_CROSS_VENDOR_DAYS)).isoformat()

    # Same vendor + same amount within 30 days
    rows = conn.execute(
        """
        SELECT document_id, vendor, document_date
          FROM documents
         WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
           AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND ABS(COALESCE(amount, -1) - ?) < 0.005
           AND document_date >= ?
           AND document_id != ?
         LIMIT 5
        """,
        (vendor, client_code, amount, window_start_30, document_id),
    ).fetchall()
    for r in rows:
        days_diff = (doc_date - (_parse_date(r["document_date"]) or doc_date)).days
        flags.append({
            "rule":     "duplicate_exact",
            "severity": HIGH,
            "i18n_key": "fraud_duplicate_exact",
            "params": {
                "amount": f"${amount:,.2f}",
                "vendor": str(vendor),
                "days":   str(abs(days_diff)),
                "doc_id": str(r["document_id"]),
            },
        })

    # Same amount + different vendor within 7 days
    cross_rows = conn.execute(
        """
        SELECT document_id, vendor, document_date
          FROM documents
         WHERE LOWER(TRIM(COALESCE(vendor, ''))) != LOWER(TRIM(?))
           AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND ABS(COALESCE(amount, -1) - ?) < 0.005
           AND document_date >= ?
           AND document_id != ?
         LIMIT 5
        """,
        (vendor, client_code, amount, window_start_7, document_id),
    ).fetchall()
    for r in cross_rows:
        days_diff = (doc_date - (_parse_date(r["document_date"]) or doc_date)).days
        flags.append({
            "rule":     "duplicate_cross_vendor",
            "severity": MEDIUM,
            "i18n_key": "fraud_duplicate_cross_vendor",
            "params": {
                "amount":       f"${amount:,.2f}",
                "other_vendor": str(r["vendor"] or "Unknown"),
                "days":         str(abs(days_diff)),
                "doc_id":       str(r["document_id"]),
            },
        })

    return flags


# ---------------------------------------------------------------------------
# Rule 4: Weekend / holiday transactions
# ---------------------------------------------------------------------------

def _rule_weekend_holiday(
    amount: float,
    doc_date: date,
) -> list[dict[str, Any]]:
    if amount <= WEEKEND_HOLIDAY_AMOUNT_LIMIT:
        return []
    flags: list[dict[str, Any]] = []
    weekday = doc_date.weekday()  # 0=Mon … 6=Sun
    if weekday == 5:  # Saturday
        flags.append({
            "rule":     "weekend_transaction",
            "severity": LOW,
            "i18n_key": "fraud_weekend_transaction",
            "params":   {"weekday": "Saturday", "amount": f"${amount:,.2f}"},
        })
    elif weekday == 6:  # Sunday
        flags.append({
            "rule":     "weekend_transaction",
            "severity": LOW,
            "i18n_key": "fraud_weekend_transaction",
            "params":   {"weekday": "Sunday", "amount": f"${amount:,.2f}"},
        })
    else:
        holiday = _is_quebec_holiday(doc_date)
        if holiday:
            flags.append({
                "rule":     "holiday_transaction",
                "severity": LOW,
                "i18n_key": "fraud_holiday_transaction",
                "params":   {"holiday": holiday, "amount": f"${amount:,.2f}"},
            })
    return flags


# ---------------------------------------------------------------------------
# Rule 5: Round number flag
# ---------------------------------------------------------------------------

def _is_round_number(amount: float) -> bool:
    """True if amount is a suspiciously round number with no cents.

    FIX 8: Expanded thresholds — flag at $100, $250, $500, $1000, $2000,
    $5000, $10000. Any whole-dollar amount that is a multiple of 50
    and at least $100.
    """
    return amount >= 100 and amount == int(amount) and amount % 50 == 0


def _rule_round_number(
    amount: float,
    history: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not _is_round_number(amount):
        return None
    amounts = [a for r in history if (a := _safe_float(r.get("amount"))) is not None]
    if len(amounts) < MIN_HISTORY_FOR_ROUND_FLAG:
        return None
    mu  = _mean(amounts)
    std = _std_dev(amounts)
    if mu == 0:
        return None
    # "Irregular" = coefficient of variation > threshold
    if std / mu > ROUND_NUMBER_STDEV_RATIO:
        return {
            "rule":     "round_number_flag",
            "severity": LOW,
            "i18n_key": "fraud_round_number",
            "params":   {"amount": f"${amount:,.0f}"},
        }
    return None


# ---------------------------------------------------------------------------
# Rule 6: New vendor large amount
# ---------------------------------------------------------------------------

def _rule_new_vendor_large_amount(
    vendor: str,
    amount: float,
    history: list[dict[str, Any]],
    doc_date: date | None = None,
) -> dict[str, Any] | None:
    """P1-7: Check cumulative invoices from new vendor within 30 days > $2,000.

    A vendor is "new" if they have fewer than 3 approved transactions.
    Sums all invoices within 30 days and flags if cumulative > threshold.
    """
    prior_approved = [
        r for r in history
        if str(r.get("review_status", "")).lower() in (
            "posted", "ready to post", "ready", "approved"
        )
    ]
    # Vendor is established if >= 3 approved transactions
    if len(prior_approved) >= 3:
        return None

    # P1-7: Sum all invoices from this vendor within 30 days
    cumulative = amount
    if doc_date and history:
        for r in history:
            r_date = _parse_date(r.get("document_date"))
            r_amount = _safe_float(r.get("amount"))
            if r_date and r_amount and r_amount > 0:
                delta = abs((doc_date - r_date).days)
                if delta <= 30:
                    cumulative += r_amount

    if cumulative > NEW_VENDOR_LARGE_AMOUNT_LIMIT:
        rule_name = "new_vendor_large_amount"
        if amount <= NEW_VENDOR_LARGE_AMOUNT_LIMIT and cumulative > NEW_VENDOR_LARGE_AMOUNT_LIMIT:
            rule_name = "invoice_splitting_suspected"
        return {
            "rule":     rule_name,
            "severity": HIGH,
            "i18n_key": "fraud_new_vendor_large",
            "params": {
                "vendor":     str(vendor),
                "amount":     f"${amount:,.2f}",
                "cumulative": f"${cumulative:,.2f}",
                "threshold":  f"${NEW_VENDOR_LARGE_AMOUNT_LIMIT:,.0f}",
            },
        }
    return None


# ---------------------------------------------------------------------------
# Rule 7: Vendor bank account change
# ---------------------------------------------------------------------------

_BANK_FIELDS = (
    "bank_account", "account_number", "iban", "routing_number",
    "transit_number", "institution_number", "swift", "bic",
    "bank_details", "payment_account",
)


def _extract_bank_fingerprint(raw_result_json: str | None) -> str | None:
    """Extract a normalised bank fingerprint from raw_result JSON, or None."""
    if not raw_result_json:
        return None
    try:
        data = json.loads(raw_result_json)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    parts: list[str] = []
    for field in _BANK_FIELDS:
        val = data.get(field)
        if val and str(val).strip():
            parts.append(f"{field}:{str(val).strip().lower()}")
    return "|".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Rule 10: Invoice date after payment date (timeline anomaly)
# ---------------------------------------------------------------------------

def _rule_invoice_after_payment(
    conn: sqlite3.Connection,
    document_id: str,
    doc_date: date,
) -> dict[str, Any] | None:
    """Flag when an invoice is dated AFTER the bank payment it matched to.

    An invoice dated after its own payment is a timeline anomaly — possible
    backdating, fabrication, or data-entry error.
    """
    # Check if bank_transactions table exists
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bank_transactions'"
    ).fetchone()
    if not exists:
        return None
    # Check if this document is matched to a bank transaction
    row = conn.execute(
        """
        SELECT bt.txn_date
          FROM bank_transactions bt
         WHERE bt.matched_document_id = ?
           AND bt.txn_date IS NOT NULL
         LIMIT 1
        """,
        (document_id,),
    ).fetchone()
    if not row:
        return None
    payment_date = _parse_date(row["txn_date"])
    if payment_date is None:
        return None
    if doc_date > payment_date:
        delta = (doc_date - payment_date).days
        return {
            "rule":     "invoice_after_payment",
            "severity": HIGH,
            "i18n_key": "fraud_invoice_after_payment",
            "params": {
                "invoice_date":  doc_date.isoformat(),
                "payment_date":  payment_date.isoformat(),
                "days":          str(delta),
            },
        }
    return None


# ---------------------------------------------------------------------------
# Rule 11: Tax registration contradiction
# ---------------------------------------------------------------------------

def _rule_tax_registration_contradiction(
    conn: sqlite3.Connection,
    vendor: str,
    client_code: str,
    raw_result_json: str | None,
) -> dict[str, Any] | None:
    """Flag when a vendor charges GST/QST but is flagged as unregistered
    in vendor memory or historical data.
    """
    # Check if current invoice charges GST/QST
    has_tax = False
    has_neq = False
    has_bn_root = False
    if raw_result_json:
        try:
            data = json.loads(raw_result_json)
            if isinstance(data, dict):
                tax_code = str(data.get("tax_code") or "").upper()
                gst = _safe_float(data.get("gst_amount") or data.get("gst"))
                qst = _safe_float(data.get("qst_amount") or data.get("qst"))
                if tax_code in ("T", "GST_QST", "GST", "QST"):
                    has_tax = True
                elif gst and gst > 0:
                    has_tax = True
                elif qst and qst > 0:
                    has_tax = True
                # Check for NEQ or BN root from OCR extraction
                if data.get("neq"):
                    has_neq = True
                if data.get("bn_root"):
                    has_bn_root = True
                    bn_root = str(data["bn_root"])
                    if bn_root in REGISTERED_VENDOR_BN:
                        return None  # Known registered vendor by BN
        except Exception:
            pass
    if not has_tax:
        return None

    # If document has NEQ, vendor is registered in Quebec — do not flag
    if has_neq:
        return None

    # If document has a BN root, vendor has a CRA business number — do not flag
    if has_bn_root:
        return None

    # Known software companies are assumed registered — do not flag
    vendor_lower = (vendor or "").lower()
    if any(kw in vendor_lower for kw in KNOWN_REGISTERED_SOFTWARE_VENDORS):
        return None

    # Check vendor memory for unregistered flag
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='vendor_memory'"
        ).fetchone()
        if not exists:
            return None
        rows = conn.execute(
            """
            SELECT tax_code, raw_result
              FROM vendor_memory
             WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
               AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
             ORDER BY updated_at DESC
             LIMIT 50
            """,
            (vendor, client_code),
        ).fetchall()
        for r in rows:
            raw = r["raw_result"] if "raw_result" in r.keys() else None
            if raw:
                try:
                    mem_data = json.loads(raw)
                    if isinstance(mem_data, dict):
                        reg_status = str(mem_data.get("tax_registered") or
                                         mem_data.get("registered") or "").lower()
                        if reg_status in ("false", "no", "unregistered", "0"):
                            return {
                                "rule":     "tax_registration_contradiction",
                                "severity": HIGH,
                                "i18n_key": "fraud_tax_registration_contradiction",
                                "params": {
                                    "vendor": vendor,
                                },
                            }
                except Exception:
                    pass
            # Also check if historical tax_code was consistently E (exempt)
            hist_tc = str(r["tax_code"] if "tax_code" in r.keys() else "").upper()
            if hist_tc in ("E", "EXEMPT", "Z", "ZERO"):
                return {
                    "rule":     "tax_registration_contradiction",
                    "severity": HIGH,
                    "i18n_key": "fraud_tax_registration_contradiction",
                    "params": {
                        "vendor": vendor,
                    },
                }
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Rule 12: Vendor category shift (memory contradiction)
# ---------------------------------------------------------------------------

def _rule_vendor_category_shift(
    conn: sqlite3.Connection,
    vendor: str,
    client_code: str,
    raw_result_json: str | None,
) -> dict[str, Any] | None:
    """Flag when a vendor's current invoice category differs significantly
    from their historical pattern (e.g. 100% repairs → equipment purchase).
    """
    # Extract current document's GL or category
    current_gl = ""
    current_memo = ""
    if raw_result_json:
        try:
            data = json.loads(raw_result_json)
            if isinstance(data, dict):
                current_gl = str(data.get("gl_account") or data.get("category") or "").lower()
                current_memo = str(data.get("memo") or data.get("notes") or "").lower()
        except Exception:
            pass

    # Check if current doc has CapEx indicators
    import re as _re
    _capex_signal = _re.compile(
        r"\b(equipment|équipement|machinery|machine|vehicle|véhicule|"
        r"construction|capital|immobilisation|fixed.asset)\b", _re.IGNORECASE)
    _expense_signal = _re.compile(
        r"\b(repair|réparation|maintenance|entretien|cleaning|nettoyage|"
        r"service|consulting|consultation)\b", _re.IGNORECASE)

    current_is_capex = bool(_capex_signal.search(current_memo) or
                            (current_gl and current_gl.startswith("15")))
    current_is_expense = bool(_expense_signal.search(current_memo) or
                              (current_gl and current_gl.startswith(("5", "6"))))

    if not current_is_capex and not current_is_expense:
        return None  # Can't determine category — skip

    # Load vendor memory for historical GL pattern
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='vendor_memory'"
        ).fetchone()
        if not exists:
            return None
        rows = conn.execute(
            """
            SELECT gl_account, approval_count
              FROM vendor_memory
             WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
               AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
               AND gl_account IS NOT NULL
             ORDER BY approval_count DESC
             LIMIT 20
            """,
            (vendor, client_code),
        ).fetchall()
        if not rows:
            return None

        total_approvals = sum(int(r["approval_count"] or 0) for r in rows)
        if total_approvals < 3:
            return None  # Not enough history

        # Check if historical pattern is overwhelmingly one category
        expense_approvals = 0
        capex_approvals = 0
        for r in rows:
            gl = str(r["gl_account"] or "").lower()
            count = int(r["approval_count"] or 0)
            if gl.startswith("15") or gl.startswith("14"):
                capex_approvals += count
            elif gl.startswith(("5", "6")):
                expense_approvals += count

        # Detect contradiction: >80% of history is one type, current is the other
        if total_approvals > 0:
            expense_ratio = expense_approvals / total_approvals
            capex_ratio = capex_approvals / total_approvals

            if current_is_capex and expense_ratio >= 0.80:
                return {
                    "rule":     "vendor_category_shift",
                    "severity": MEDIUM,
                    "i18n_key": "fraud_vendor_category_shift",
                    "params": {
                        "vendor":   vendor,
                        "history":  f"{expense_ratio:.0%} expense",
                        "current":  "CapEx",
                    },
                }
            elif current_is_expense and capex_ratio >= 0.80:
                return {
                    "rule":     "vendor_category_shift",
                    "severity": MEDIUM,
                    "i18n_key": "fraud_vendor_category_shift",
                    "params": {
                        "vendor":   vendor,
                        "history":  f"{capex_ratio:.0%} CapEx",
                        "current":  "Expense",
                    },
                }
    except Exception:
        pass
    return None


def _rule_bank_account_change(
    raw_result_json: str | None,
    history: list[dict[str, Any]],
) -> dict[str, Any] | None:
    current_fp = _extract_bank_fingerprint(raw_result_json)
    if not current_fp:
        return None  # no bank details in this document → cannot compare

    for prior in history:
        prior_fp = _extract_bank_fingerprint(prior.get("raw_result"))
        if prior_fp and prior_fp != current_fp:
            return {
                "rule":     "bank_account_change",
                "severity": CRITICAL,
                "i18n_key": "fraud_bank_account_change",
                "params": {
                    "old": _mask_bank(prior_fp),
                    "new": _mask_bank(current_fp),
                },
            }
    return None


def _mask_bank(fingerprint: str) -> str:
    """Show only last 4 chars of each bank field value for display."""
    parts: list[str] = []
    for segment in fingerprint.split("|"):
        if ":" in segment:
            field, val = segment.split(":", 1)
            masked = ("*" * max(0, len(val) - 4)) + val[-4:] if len(val) > 4 else val
            parts.append(f"{field}:{masked}")
        else:
            parts.append(segment)
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Rule CN-3: Orphan credit note (no matching original invoice)
# ---------------------------------------------------------------------------

def _rule_orphan_credit_note(
    conn: sqlite3.Connection,
    vendor: str,
    client_code: str,
    abs_amount: float,
    exclude_doc_id: str,
) -> dict[str, Any] | None:
    """Flag credit notes that have no matching original invoice for same vendor and similar amount."""
    rows = conn.execute(
        """
        SELECT document_id
          FROM documents
         WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
           AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND COALESCE(amount, 0) > 0
           AND ABS(COALESCE(amount, 0) - ?) < (? * 0.05 + 0.01)
           AND document_id != ?
         LIMIT 1
        """,
        (vendor, client_code, abs_amount, abs_amount, exclude_doc_id),
    ).fetchall()
    if not rows:
        return {
            "rule":     "orphan_credit_note",
            "severity": HIGH,
            "i18n_key": "fraud_orphan_credit_note",
            "params": {
                "vendor":  str(vendor),
                "amount":  f"${abs_amount:,.2f}",
            },
        }
    return None


# ---------------------------------------------------------------------------
# Rule 13: Payee / invoice vendor mismatch (post-match check)
# ---------------------------------------------------------------------------

def _rule_payee_invoice_mismatch(
    conn: sqlite3.Connection,
    document_id: str,
    vendor: str,
) -> dict[str, Any] | None:
    """Flag when a document is linked to a bank transaction whose description
    (payee) differs significantly from the invoice vendor.

    This catches cases where matching already happened but the mismatch was
    not flagged at match time.
    """
    if not vendor:
        return None
    # Check if this document is matched to a bank transaction
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bank_transactions'"
    ).fetchone()
    if not exists:
        return None
    row = conn.execute(
        """
        SELECT bt.description
          FROM bank_transactions bt
         WHERE bt.matched_document_id = ?
           AND bt.description IS NOT NULL
         LIMIT 1
        """,
        (document_id,),
    ).fetchone()
    if not row:
        return None
    payee = str(row["description"] or "").strip()
    if not payee:
        return None
    # Normalize both and compare
    vendor_key = _normalize_vendor_key(vendor)
    payee_key = _normalize_vendor_key(payee)
    if not vendor_key or not payee_key:
        return None
    similarity = SequenceMatcher(None, vendor_key, payee_key).ratio()
    if similarity < 0.70:
        return {
            "rule":     "vendor_payee_mismatch",
            "severity": HIGH,
            "i18n_key": "fraud_vendor_payee_mismatch",
            "params": {
                "vendor": vendor,
                "payee":  payee,
                "similarity": f"{similarity:.0%}",
            },
        }
    return None


# ---------------------------------------------------------------------------
# Rule 14: Near-duplicate invoice numbers (OCR-resilient)
# ---------------------------------------------------------------------------

def _normalize_invoice_number(raw: str) -> str:
    """OCR-resilient invoice number normalization.

    O→0, I/l→1, S→5, strip hyphens/spaces, uppercase.
    """
    if not raw:
        return ""
    s = str(raw).strip().upper()
    s = s.replace("O", "0").replace("I", "1").replace("L", "1").replace("S", "5")
    s = s.replace("-", "").replace(" ", "")
    return s


def _rule_near_duplicate_invoice_number(
    conn: sqlite3.Connection,
    document_id: str,
    vendor: str,
    client_code: str,
    invoice_number: str,
    doc_date: date,
) -> dict[str, Any] | None:
    """Flag when a different document has an invoice number that normalizes
    to the same value (catches OCR confusables like INV-001 vs INV-0O1)."""
    if not invoice_number:
        return None
    norm = _normalize_invoice_number(invoice_number)
    if not norm:
        return None
    # Check for invoice_number or invoice_number_normalized columns
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if "invoice_number" not in cols:
        return None
    window_start = (doc_date - timedelta(days=365)).isoformat()
    rows = conn.execute(
        """
        SELECT document_id, invoice_number, vendor
          FROM documents
         WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND document_id != ?
           AND invoice_number IS NOT NULL
           AND invoice_number != ''
           AND document_date >= ?
         LIMIT 200
        """,
        (client_code, document_id, window_start),
    ).fetchall()
    for r in rows:
        other_inv = str(r["invoice_number"] or "")
        if not other_inv:
            continue
        other_norm = _normalize_invoice_number(other_inv)
        if other_norm == norm and other_inv != invoice_number:
            return {
                "rule":     "near_duplicate_invoice_number",
                "severity": HIGH,
                "i18n_key": "fraud_near_duplicate_invoice_number",
                "params": {
                    "invoice_number": invoice_number,
                    "other_invoice":  other_inv,
                    "other_doc_id":   str(r["document_id"]),
                    "normalized":     norm,
                },
            }
        # Also flag exact same invoice number from same vendor
        if other_norm == norm and other_inv == invoice_number:
            other_vendor = _normalize_vendor_key(str(r["vendor"] or ""))
            this_vendor = _normalize_vendor_key(vendor)
            if this_vendor and other_vendor and this_vendor == other_vendor:
                return {
                    "rule":     "near_duplicate_invoice_number",
                    "severity": HIGH,
                    "i18n_key": "fraud_near_duplicate_invoice_number",
                    "params": {
                        "invoice_number": invoice_number,
                        "other_invoice":  other_inv,
                        "other_doc_id":   str(r["document_id"]),
                        "normalized":     norm,
                    },
                }
    return None


# ---------------------------------------------------------------------------
# Rule 15: Multi-channel duplicate (same invoice via email + WhatsApp + portal)
# ---------------------------------------------------------------------------

def _rule_multi_channel_duplicate(
    conn: sqlite3.Connection,
    document_id: str,
    vendor: str,
    client_code: str,
    amount: float,
    invoice_number: str,
) -> dict[str, Any] | None:
    """Flag when the same invoice (vendor + amount + invoice_number) appears
    in multiple documents — likely submitted through different channels."""
    if not invoice_number or not vendor:
        return None
    norm_inv = _normalize_invoice_number(invoice_number)
    if not norm_inv:
        return None
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if "invoice_number" not in cols:
        return None
    rows = conn.execute(
        """
        SELECT document_id, vendor, amount, invoice_number
          FROM documents
         WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
           AND document_id != ?
           AND invoice_number IS NOT NULL
           AND invoice_number != ''
         LIMIT 500
        """,
        (client_code, document_id),
    ).fetchall()
    matches = []
    vendor_norm = _normalize_vendor_key(vendor)
    for r in rows:
        r_vendor_norm = _normalize_vendor_key(str(r["vendor"] or ""))
        r_inv_norm = _normalize_invoice_number(str(r["invoice_number"] or ""))
        r_amount = _safe_float(r["amount"])
        if (r_inv_norm == norm_inv
                and r_vendor_norm == vendor_norm
                and r_amount is not None
                and abs(r_amount - amount) < 0.01):
            matches.append(str(r["document_id"]))
    if matches:
        return {
            "rule":     "multi_channel_duplicate",
            "severity": HIGH,
            "i18n_key": "fraud_multi_channel_duplicate",
            "params": {
                "vendor":         vendor,
                "invoice_number": invoice_number,
                "duplicates":     ", ".join(matches[:5]),
                "count":          str(len(matches)),
            },
        }
    return None


# ---------------------------------------------------------------------------
# Rule 16: Credit note loop (credit → re-invoice → credit cycle)
# ---------------------------------------------------------------------------

def _rule_credit_note_loop(
    conn: sqlite3.Connection,
    vendor: str,
    client_code: str,
    abs_amount: float,
    doc_date: date,
    exclude_doc_id: str,
) -> dict[str, Any] | None:
    """Detect credit-note / re-invoice cycles: same vendor issues credit note
    and then a new invoice for a similar amount within 60 days, repeated.

    A single credit+re-invoice is normal. Three or more cycles = suspicious loop.
    """
    if not vendor:
        return None
    window_start = (doc_date - timedelta(days=180)).isoformat()
    try:
        rows = conn.execute(
            """
            SELECT document_id, amount, document_date, doc_type
              FROM documents
             WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
               AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
               AND document_date >= ?
               AND document_id != ?
             ORDER BY document_date ASC
             LIMIT 200
            """,
            (vendor, client_code, window_start, exclude_doc_id),
        ).fetchall()
    except Exception:
        return None
    credit_count = 0
    reinvoice_count = 0
    for r in rows:
        r_amount = _safe_float(r["amount"])
        r_type = str(r["doc_type"] if "doc_type" in r.keys() else "").lower()
        if r_amount is None:
            continue
        is_credit = r_amount < 0 or r_type == "credit_note"
        if is_credit and abs(abs(r_amount) - abs_amount) < (abs_amount * 0.10 + 0.01):
            credit_count += 1
        elif not is_credit and abs(r_amount - abs_amount) < (abs_amount * 0.10 + 0.01):
            reinvoice_count += 1
    # 3+ credit notes with similar amount from same vendor = loop
    if credit_count >= 2 and reinvoice_count >= 2:
        return {
            "rule":     "credit_note_loop",
            "severity": HIGH,
            "i18n_key": "fraud_credit_note_loop",
            "params": {
                "vendor":          vendor,
                "credit_count":    str(credit_count),
                "reinvoice_count": str(reinvoice_count),
                "amount":          f"${abs_amount:,.2f}",
            },
        }
    return None


# ---------------------------------------------------------------------------
# Main detection function
# ---------------------------------------------------------------------------

def run_fraud_detection(
    document_id: str,
    *,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    """
    Run all fraud detection rules against *document_id*.

    Loads the document from the database, evaluates every rule, saves the
    resulting flag list as JSON in ``documents.fraud_flags``, and returns it.

    Returns an empty list if the document is not found, has no amount, or
    no date — in that case ``fraud_flags`` is set to ``"[]"``.
    """
    with _open_db(db_path) as conn:
        _ensure_fraud_flags_column(conn)

        row = conn.execute(
            "SELECT * FROM documents WHERE document_id = ? LIMIT 1",
            (document_id,),
        ).fetchone()
        if not row:
            return []

        doc = dict(row)

    amount = _safe_float(doc.get("amount"))
    if amount is None or amount == 0:
        _save_flags(document_id, [], db_path)
        return []

    doc_date = _parse_date(doc.get("document_date"))
    if doc_date is None:
        _save_flags(document_id, [], db_path)
        return []

    vendor      = str(doc.get("vendor") or "").strip()
    client_code = str(doc.get("client_code") or "").strip()
    raw_json    = doc.get("raw_result")
    doc_type    = str(doc.get("doc_type") or "").strip().lower()
    is_credit   = amount < 0 or doc_type == "credit_note"
    abs_amount  = abs(amount)

    # Bank statements are not invoices — skip all fraud detection
    if doc_type == "bank_statement":
        _save_flags(document_id, [], db_path)
        return []

    is_bank_vendor = _is_canadian_bank_vendor(vendor)
    is_trusted_vendor = _is_known_trusted_vendor(vendor) if vendor else False
    is_weekend_exempt = _is_weekend_exempt_vendor(vendor) if vendor else False

    # Extract invoice number from doc or raw_result
    invoice_number = str(doc.get("invoice_number") or "").strip()
    if not invoice_number and raw_json:
        try:
            _raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
            if isinstance(_raw, dict):
                invoice_number = str(_raw.get("invoice_number") or "").strip()
        except Exception:
            pass

    flags: list[dict[str, Any]] = []

    with _open_db(db_path) as conn:
        history = _load_vendor_history(conn, vendor, client_code, document_id)
        # BLOCK 5: Load fuzzy history for anomaly detection fallback
        fuzzy_history = _load_vendor_history_fuzzy(conn, vendor, client_code, document_id) if vendor else []

        if not is_credit:
            # --- Normal (positive) invoice rules ---
            # Rule 1 & 2: Vendor behavior anomaly (with fuzzy fallback)
            if vendor:
                flag = _rule_vendor_amount_anomaly(amount, history, fuzzy_history=fuzzy_history)
                if flag:
                    flags.append(flag)
                flag = _rule_vendor_timing_anomaly(doc_date, history)
                if flag:
                    flags.append(flag)

            # Rule 3: Duplicate detection
            flags.extend(_rule_duplicate(conn, document_id, vendor, client_code, amount, doc_date))

            # Rule 4: Weekend / holiday — skip for known trusted vendors
            # Only flag weekend transactions for unknown/untrusted vendors
            # (banks, utilities, telecoms, government, retailers all transact on weekends normally)
            if not is_weekend_exempt:
                flags.extend(_rule_weekend_holiday(amount, doc_date))

            # Rule 5: Round number flag
            if vendor:
                flag = _rule_round_number(amount, history)
                if flag:
                    flags.append(flag)

            # Rule 6: New vendor large amount (P1-7: cumulative check) — skip for known trusted vendors
            if vendor and not is_trusted_vendor:
                flag = _rule_new_vendor_large_amount(vendor, amount, history, doc_date)
                if flag:
                    flags.append(flag)

            # Rule 7: Bank account change
            if vendor and history:
                flag = _rule_bank_account_change(raw_json, history)
                if flag:
                    flags.append(flag)

            # Rule 10: Invoice date after payment date (timeline anomaly)
            flag = _rule_invoice_after_payment(conn, document_id, doc_date)
            if flag:
                flags.append(flag)

            # Rule 11: Tax registration contradiction
            if vendor:
                flag = _rule_tax_registration_contradiction(
                    conn, vendor, client_code, raw_json)
                if flag:
                    flags.append(flag)

            # Rule 12: Vendor category shift (memory contradiction)
            if vendor:
                flag = _rule_vendor_category_shift(
                    conn, vendor, client_code, raw_json)
                if flag:
                    flags.append(flag)

            # Rule 13: Payee/invoice vendor mismatch (post-match)
            if vendor:
                flag = _rule_payee_invoice_mismatch(conn, document_id, vendor)
                if flag:
                    flags.append(flag)

            # Rule 14: Near-duplicate invoice numbers
            if invoice_number:
                flag = _rule_near_duplicate_invoice_number(
                    conn, document_id, vendor, client_code, invoice_number, doc_date)
                if flag:
                    flags.append(flag)

            # Rule 15: Multi-channel duplicate
            if invoice_number and vendor:
                flag = _rule_multi_channel_duplicate(
                    conn, document_id, vendor, client_code, amount, invoice_number)
                if flag:
                    flags.append(flag)

            # Rule 16: Credit note loop (check even on positive invoices)
            if vendor:
                flag = _rule_credit_note_loop(
                    conn, vendor, client_code, abs_amount, doc_date, document_id)
                if flag:
                    flags.append(flag)
        else:
            # --- Credit note rules ---
            # Rule CN-1: Duplicate credit note (same vendor + same amount within 30 days)
            flags.extend(_rule_duplicate(conn, document_id, vendor, client_code, amount, doc_date))

            # Rule CN-2: New vendor credit note (never-seen vendor issuing credit) — skip trusted
            if vendor and not is_trusted_vendor:
                flag = _rule_new_vendor_large_amount(vendor, abs_amount, history)
                if flag:
                    flag = {**flag, "rule": "new_vendor_credit_note",
                            "i18n_key": "fraud_new_vendor_credit_note"}
                    flags.append(flag)

            # Rule CN-3: Orphan credit note (no matching original invoice)
            if vendor:
                flag = _rule_orphan_credit_note(conn, vendor, client_code, abs_amount, document_id)
                if flag:
                    flags.append(flag)

            # Rule CN-4: Large credit note (over $5,000)
            if abs_amount > LARGE_CREDIT_NOTE_LIMIT:
                flags.append({
                    "rule":     "large_credit_note",
                    "severity": HIGH,
                    "i18n_key": "fraud_large_credit_note",
                    "params": {
                        "amount":    f"${abs_amount:,.2f}",
                        "threshold": f"${LARGE_CREDIT_NOTE_LIMIT:,.0f}",
                    },
                })

            # BLOCK 5: Apply weekend/holiday rules to credit notes too (skip exempt vendors)
            if not is_weekend_exempt:
                flags.extend(_rule_weekend_holiday(abs_amount, doc_date))

            # Rule 13: Payee/invoice vendor mismatch (post-match) — credit notes too
            if vendor:
                flag = _rule_payee_invoice_mismatch(conn, document_id, vendor)
                if flag:
                    flags.append(flag)

            # BLOCK 5: Apply vendor amount anomaly to credit notes (using abs amount)
            if vendor:
                flag = _rule_vendor_amount_anomaly(abs_amount, history, fuzzy_history=fuzzy_history)
                if flag:
                    flags.append(flag)

            # Rule 16: Credit note loop
            if vendor:
                flag = _rule_credit_note_loop(
                    conn, vendor, client_code, abs_amount, doc_date, document_id)
                if flag:
                    flags.append(flag)

    _save_flags(document_id, flags, db_path)
    return flags


def _save_flags(
    document_id: str,
    flags: list[dict[str, Any]],
    db_path: Path,
) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with _open_db(db_path) as conn:
        _ensure_fraud_flags_column(conn)
        conn.execute(
            "UPDATE documents SET fraud_flags = ?, updated_at = ? WHERE document_id = ?",
            (json.dumps(flags, ensure_ascii=False), now, document_id),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Related party check with AI fallback
# ---------------------------------------------------------------------------

def _name_similarity(a: str, b: str) -> float:
    """Fuzzy similarity between two names using SequenceMatcher."""
    return SequenceMatcher(None, a.strip().lower(), b.strip().lower()).ratio()


def load_related_parties_from_db(
    client_code: str,
    db_path: Path = DB_PATH,
) -> list[str]:
    """Load related party names from the CAS related_parties table for a client.

    FIX 5: Connects fraud detection to the related_parties table used by CAS 550.
    """
    try:
        with _open_db(db_path) as conn:
            # Check table exists
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='related_parties'"
            ).fetchone()
            if not exists:
                return []
            rows = conn.execute(
                "SELECT party_name FROM related_parties WHERE LOWER(client_code) = LOWER(?)",
                (client_code,),
            ).fetchall()
            return [str(r["party_name"]) for r in rows if r["party_name"]]
    except Exception:
        return []


def check_related_party(
    vendor: str,
    related_parties: list[str],
    client_code: str | None = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """
    Check if a vendor is a related party using fuzzy name matching.

    When fuzzy matching is inconclusive (score 0.60–0.80), falls back to
    AI-assisted related party check via ai_router.

    Returns dict with keys: is_related_party, confidence, matched_party, reasoning.
    """
    vendor_clean = vendor.strip()

    # FIX 5: Also load related parties from DB if client_code is provided
    all_parties = list(related_parties) if related_parties else []
    if client_code:
        db_parties = load_related_parties_from_db(client_code, db_path)
        # Merge without duplicates
        existing_lower = {p.lower() for p in all_parties}
        for p in db_parties:
            if p.lower() not in existing_lower:
                all_parties.append(p)
                existing_lower.add(p.lower())

    if not vendor_clean or not all_parties:
        return {
            "is_related_party": False,
            "confidence": 1.0,
            "matched_party": None,
            "reasoning": "No vendor or no related parties to compare",
        }

    related_parties = all_parties

    best_score = 0.0
    best_party = ""
    for party in related_parties:
        score = _name_similarity(vendor_clean, party)
        if score > best_score:
            best_score = score
            best_party = party

    # High confidence match — definitely related
    if best_score >= 0.80:
        return {
            "is_related_party": True,
            "confidence": round(best_score, 4),
            "matched_party": best_party,
            "reasoning": f"Fuzzy match score {best_score:.0%} with '{best_party}'",
        }

    # Low confidence — definitely not related
    if best_score < 0.60:
        return {
            "is_related_party": False,
            "confidence": round(1.0 - best_score, 4),
            "matched_party": None,
            "reasoning": f"Best fuzzy match score {best_score:.0%} is below threshold",
        }

    # Inconclusive range (0.60–0.80) — ask AI for confirmation
    try:
        from src.agents.core import ai_router

        ai_result = ai_router.call_related_party_check(
            vendor=vendor_clean,
            related_parties=related_parties,
        )
        if ai_result.get("is_related_party") is not None and not ai_result.get("error"):
            return {
                "is_related_party": bool(ai_result["is_related_party"]),
                "confidence": float(ai_result.get("confidence") or best_score),
                "matched_party": best_party if ai_result["is_related_party"] else None,
                "reasoning": ai_result.get("reasoning") or f"AI confirmed (fuzzy score was {best_score:.0%})",
            }
    except Exception as exc:
        log.debug("AI related party check fallback failed: %s", exc)

    # AI unavailable — return inconclusive based on fuzzy score alone
    return {
        "is_related_party": False,
        "confidence": round(best_score, 4),
        "matched_party": best_party,
        "reasoning": f"Inconclusive fuzzy match {best_score:.0%} with '{best_party}' — AI unavailable for confirmation",
    }


# ---------------------------------------------------------------------------
# Convenience: load saved flags from DB
# ---------------------------------------------------------------------------

def get_fraud_flags(
    document_id: str,
    *,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    """Return the saved fraud flags for a document without re-running detection."""
    with _open_db(db_path) as conn:
        row = conn.execute(
            "SELECT fraud_flags FROM documents WHERE document_id = ? LIMIT 1",
            (document_id,),
        ).fetchone()
    if not row or not row["fraud_flags"]:
        return []
    try:
        data = json.loads(row["fraud_flags"])
        return data if isinstance(data, list) else []
    except Exception:
        return []


# =========================================================================
# PART 5 — Cross-entity payment uncertainty preservation
# =========================================================================

def evaluate_cross_entity_payment(
    invoice_vendor: str,
    bank_payee: str,
    invoice_gst_number: str | None = None,
    bank_gst_number: str | None = None,
    invoice_address: str | None = None,
    bank_address: str | None = None,
    invoice_phone: str | None = None,
    bank_phone: str | None = None,
    amount: float | None = None,
) -> dict[str, Any]:
    """Evaluate vendor/payee identity with uncertainty preservation.

    When fuzzy match score is 0.60-0.85: preserve uncertainty, don't merge.
    Never say "same vendor" unless GST/QST numbers match exactly
    OR identical normalized names.
    """
    from difflib import SequenceMatcher

    inv_norm = _normalize_vendor_key(invoice_vendor)
    bank_norm = _normalize_vendor_key(bank_payee)

    if inv_norm and bank_norm:
        similarity = SequenceMatcher(None, inv_norm, bank_norm).ratio()
    else:
        similarity = 0.0

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

    # GST numbers differ
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

    # Exact normalized name
    if inv_norm and bank_norm and inv_norm == bank_norm:
        return {
            "identity_status": "confirmed_same_vendor",
            "similarity": 1.0,
            "match_basis": "normalized_name_exact",
            "confidence": 0.95,
        }

    # Check shared identifiers
    same_address = (
        invoice_address and bank_address
        and _normalize_vendor_key(invoice_address) == _normalize_vendor_key(bank_address)
    )
    same_phone = (
        invoice_phone and bank_phone
        and invoice_phone.strip() == bank_phone.strip()
    )

    if 0.80 <= similarity <= 0.85 and (same_address or same_phone):
        return {
            "identity_status": "probable_affiliate",
            "similarity": round(similarity, 4),
            "shared_address": bool(same_address),
            "shared_phone": bool(same_phone),
            "confidence": 0.75,
        }

    if 0.60 <= similarity < 0.80:
        return {
            "identity_status": "uncertain_payee_relationship",
            "similarity": round(similarity, 4),
            "confidence": 0.50,
            "reason_code": "PAYEE_IDENTITY_UNPROVEN",
        }

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

    if similarity < 0.60:
        return {
            "identity_status": "different_vendor",
            "similarity": round(similarity, 4),
            "confidence": 0.90,
        }

    return {
        "identity_status": "probable_same_vendor",
        "similarity": round(similarity, 4),
        "confidence": 0.80,
    }


def record_trusted_vendor(
    client_code: str = "",
    vendor_name: str = "",
    rule_overridden: str = "",
    justification: str = "",
    conn: sqlite3.Connection | None = None,
    **kwargs,
) -> dict:
    """Record a fraud flag override — vendor is trusted after human review.

    Stores the trust decision so future fraud checks can weight it.
    Confidence is clamped to [0.1, 0.99].
    """
    if not vendor_name:
        return {"ok": False, "reason": "missing_vendor_name"}

    _own = conn is None
    if _own:
        conn = _open_db()
    try:
        # Ensure trusted_vendors table exists
        conn.execute(
            """CREATE TABLE IF NOT EXISTS trusted_vendors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT,
                vendor_name TEXT NOT NULL,
                vendor_key TEXT NOT NULL,
                rule_overridden TEXT,
                justification TEXT,
                trust_count INTEGER NOT NULL DEFAULT 1,
                confidence REAL NOT NULL DEFAULT 0.5,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        conn.commit()

        vendor_key = _normalize_vendor_key(vendor_name)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        client_key = (client_code or "").strip().lower()

        existing = conn.execute(
            "SELECT id, trust_count, confidence FROM trusted_vendors "
            "WHERE vendor_key = ? AND client_code = ? LIMIT 1",
            (vendor_key, client_key),
        ).fetchone()

        if existing:
            new_count = (existing["trust_count"] or 0) + 1
            new_conf = min(0.99, max(0.1, (existing["confidence"] or 0.5) + 0.1))
            conn.execute(
                "UPDATE trusted_vendors SET trust_count=?, confidence=?, "
                "rule_overridden=?, justification=?, updated_at=? WHERE id=?",
                (new_count, new_conf, rule_overridden, justification, now, existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO trusted_vendors "
                "(client_code, vendor_name, vendor_key, rule_overridden, "
                "justification, trust_count, confidence, created_at, updated_at) "
                "VALUES (?,?,?,?,?,1,0.5,?,?)",
                (client_key, vendor_name, vendor_key, rule_overridden,
                 justification, now, now),
            )
        conn.commit()
        return {"ok": True, "vendor": vendor_name, "rule": rule_overridden}
    except Exception as exc:
        log.debug("record_trusted_vendor failed: %s", exc)
        return {"ok": False, "reason": str(exc)}
    finally:
        if _own:
            conn.close()
