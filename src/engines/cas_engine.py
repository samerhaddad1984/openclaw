"""
src/engines/cas_engine.py — CAS-compliant audit extension for OtoCPA.

Extends the existing audit module (audit_engine.py) with:
  - Materiality assessment (CAS 320)
  - Risk assessment matrix (CAS 315)

All monetary arithmetic uses Python Decimal.
"""
from __future__ import annotations

import json
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
    # Microsecond precision so rapid-fire save_materiality / save_* calls
    # inside a single request produce distinguishable timestamps. The
    # reassessment flow relies on ORDER BY calculated_at DESC returning
    # the newest row.
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Valid constants
# ---------------------------------------------------------------------------

VALID_MATERIALITY_BASES = {"pre_tax_income", "total_assets", "revenue"}

VALID_ASSERTIONS = {
    "completeness", "accuracy", "existence", "cutoff",
    "classification", "rights_obligations", "presentation",
}

VALID_RISK_LEVELS = {"low", "medium", "high"}


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def ensure_cas_tables(conn: sqlite3.Connection) -> None:
    """Create CAS-specific tables (idempotent)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS materiality_assessments (
            assessment_id        TEXT PRIMARY KEY,
            engagement_id        TEXT NOT NULL,
            client_code          TEXT NOT NULL,
            period               TEXT NOT NULL,
            basis                TEXT NOT NULL,
            basis_amount         REAL NOT NULL,
            planning_materiality REAL NOT NULL,
            performance_materiality REAL NOT NULL,
            clearly_trivial      REAL NOT NULL,
            calculated_at        TEXT NOT NULL,
            calculated_by        TEXT,
            notes                TEXT,
            materiality_locked   INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_mat_engagement
            ON materiality_assessments(engagement_id);

        CREATE TABLE IF NOT EXISTS risk_assessments (
            risk_id          TEXT PRIMARY KEY,
            engagement_id    TEXT NOT NULL,
            account_code     TEXT NOT NULL,
            account_name     TEXT NOT NULL,
            assertion        TEXT NOT NULL,
            inherent_risk    TEXT NOT NULL DEFAULT 'medium',
            control_risk     TEXT NOT NULL DEFAULT 'medium',
            combined_risk    TEXT NOT NULL DEFAULT 'medium',
            significant_risk INTEGER NOT NULL DEFAULT 0,
            assessed_by      TEXT,
            assessed_at      TEXT,
            notes            TEXT,
            signed_at        TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_risk_engagement
            ON risk_assessments(engagement_id);

        CREATE INDEX IF NOT EXISTS idx_risk_account
            ON risk_assessments(engagement_id, account_code);

        CREATE TABLE IF NOT EXISTS management_representation_letters (
            letter_id        TEXT PRIMARY KEY,
            engagement_id    TEXT NOT NULL,
            client_code      TEXT NOT NULL,
            period_end_date  TEXT,
            draft_text_fr    TEXT,
            draft_text_en    TEXT,
            management_name  TEXT,
            management_title TEXT,
            signed_at        TEXT,
            signed_by        TEXT,
            status           TEXT NOT NULL DEFAULT 'draft',
            created_by       TEXT,
            created_at       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_rep_letter_engagement
            ON management_representation_letters(engagement_id);

        CREATE TABLE IF NOT EXISTS control_tests (
            test_id             TEXT PRIMARY KEY,
            engagement_id       TEXT NOT NULL,
            control_name        TEXT NOT NULL,
            control_description TEXT,
            control_objective   TEXT,
            test_type           TEXT NOT NULL DEFAULT 'walkthrough',
            test_procedure      TEXT,
            sample_size         INTEGER,
            items_tested        INTEGER,
            exceptions_found    INTEGER DEFAULT 0,
            exception_details   TEXT,
            conclusion          TEXT DEFAULT 'effective',
            tested_by           TEXT,
            tested_at           TEXT,
            reviewed_by         TEXT,
            notes               TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_control_tests_engagement
            ON control_tests(engagement_id);

        CREATE TABLE IF NOT EXISTS related_parties (
            party_id          TEXT PRIMARY KEY,
            client_code       TEXT NOT NULL,
            party_name        TEXT NOT NULL,
            relationship_type TEXT NOT NULL DEFAULT 'affiliated_company',
            ownership_percentage REAL,
            notes             TEXT,
            identified_by     TEXT,
            identified_at     TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_related_parties_client
            ON related_parties(client_code);

        CREATE TABLE IF NOT EXISTS related_party_transactions (
            rpt_id                   TEXT PRIMARY KEY,
            engagement_id            TEXT NOT NULL,
            party_id                 TEXT NOT NULL,
            document_id              TEXT,
            transaction_date         TEXT,
            amount                   REAL,
            description              TEXT,
            normal_amount            REAL,
            difference               REAL,
            measurement_basis        TEXT DEFAULT 'exchange_amount',
            disclosure_required      INTEGER NOT NULL DEFAULT 1,
            audit_procedures_performed TEXT,
            conclusion               TEXT,
            reviewed_by              TEXT,
            notes                    TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_rpt_engagement
            ON related_party_transactions(engagement_id);

        CREATE INDEX IF NOT EXISTS idx_rpt_party
            ON related_party_transactions(party_id);
    """)
    conn.commit()

    # Sprint E Fix 6 — reassessment columns on materiality_assessments.
    # CAS 320 requires reassessment when the auditor becomes aware of info
    # that would have changed the initial determination. Instead of a
    # single-row-per-engagement schema we keep a row-per-assessment with a
    # `supersedes_assessment_id` link and a `reassessment_reason` audit
    # trail; get_materiality already reads ORDER BY calculated_at DESC.
    # PRAGMA result columns are indexed by integer under a default
    # sqlite3.Row factory but also expose column names. Use the 'name'
    # key when available so callers that set a row_factory still work.
    existing_cols: set[str] = set()
    for row in conn.execute("PRAGMA table_info(materiality_assessments)").fetchall():
        try:
            name = row["name"]
        except (TypeError, IndexError, KeyError):
            name = row[1]
        existing_cols.add(name)
    for col, typedef in [
        ("reassessment_reason",      "TEXT"),
        ("supersedes_assessment_id", "TEXT"),
    ]:
        if col not in existing_cols:
            conn.execute(
                f"ALTER TABLE materiality_assessments ADD COLUMN {col} {typedef}"
            )
    conn.commit()

    # The old "one per engagement" trigger blocked reassessment. Drop it —
    # the INSERT path in save_materiality now chains a new row via
    # supersedes_assessment_id whenever a prior assessment already exists.
    # The locked-immutable and cascade-lock triggers below still prevent
    # editing an assessment once the related working papers are signed.
    conn.execute("DROP TRIGGER IF EXISTS trg_materiality_one_per_engagement")

    # --- Immutability triggers (FIX 1-4) ---
    conn.executescript("""

        -- FIX 2: Materiality locked after working papers signed
        CREATE TRIGGER IF NOT EXISTS trg_materiality_locked_immutable
        BEFORE UPDATE ON materiality_assessments
        WHEN OLD.materiality_locked = 1
        BEGIN
            SELECT RAISE(ABORT, 'Materiality is locked — working papers have been signed');
        END;

        -- FIX 4: Risk assessment locked after working paper sign-off
        CREATE TRIGGER IF NOT EXISTS trg_risk_assessment_locked
        BEFORE UPDATE ON risk_assessments
        WHEN OLD.signed_at IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'Risk assessment is locked after working paper sign-off');
        END;

        -- FIX 2: Cascade lock materiality when WP is signed
        CREATE TRIGGER IF NOT EXISTS trg_cascade_lock_materiality
        AFTER UPDATE ON working_papers
        WHEN NEW.sign_off_at IS NOT NULL AND OLD.sign_off_at IS NULL
        BEGIN
            UPDATE materiality_assessments SET materiality_locked = 1
            WHERE engagement_id IN (
                SELECT engagement_id FROM engagements
                WHERE LOWER(client_code) = LOWER(NEW.client_code)
                  AND period = NEW.period
            );
        END;

        -- FIX 4: Cascade lock risk assessments when WP is signed
        CREATE TRIGGER IF NOT EXISTS trg_cascade_lock_risk_assessments
        AFTER UPDATE ON working_papers
        WHEN NEW.sign_off_at IS NOT NULL AND OLD.sign_off_at IS NULL
        BEGIN
            UPDATE risk_assessments SET signed_at = NEW.sign_off_at
            WHERE signed_at IS NULL AND engagement_id IN (
                SELECT engagement_id FROM engagements
                WHERE LOWER(client_code) = LOWER(NEW.client_code)
                  AND period = NEW.period
            );
        END;
    """)


# ---------------------------------------------------------------------------
# Materiality (CAS 320)
# ---------------------------------------------------------------------------

_MATERIALITY_RATES = {
    "pre_tax_income": Decimal("0.05"),    # 5%
    "total_assets":   Decimal("0.005"),   # 0.5%
    "revenue":        Decimal("0.02"),    # 2%
}

PERFORMANCE_RATE = Decimal("0.75")        # 75% of planning
CLEARLY_TRIVIAL_RATE = Decimal("0.05")    # 5% of planning


def calculate_materiality(
    basis_type: str,
    basis_amount: float | Decimal,
) -> dict[str, Any]:
    """Calculate materiality levels per CAS 320.

    Returns dict with planning_materiality, performance_materiality,
    clearly_trivial (all as Decimal), plus the basis info.
    """
    if basis_type not in VALID_MATERIALITY_BASES:
        raise ValueError(
            f"Invalid basis_type: {basis_type}. "
            f"Must be one of {sorted(VALID_MATERIALITY_BASES)}"
        )
    amt = _to_decimal(basis_amount)
    if amt <= _ZERO:
        raise ValueError("basis_amount must be positive")

    rate = _MATERIALITY_RATES[basis_type]
    planning = _round(amt * rate)
    performance = _round(planning * PERFORMANCE_RATE)
    trivial = _round(planning * CLEARLY_TRIVIAL_RATE)

    return {
        "basis": basis_type,
        "basis_amount": amt,
        "planning_materiality": planning,
        "performance_materiality": performance,
        "clearly_trivial": trivial,
    }


def save_materiality(
    conn: sqlite3.Connection,
    engagement_id: str,
    materiality_dict: dict[str, Any],
    username: str,
    notes: str = "",
    reassessment_reason: str = "",
) -> str:
    """Persist a materiality assessment. Returns the assessment_id.

    Sprint E Fix 6 — if a prior assessment already exists for this
    engagement, require a non-empty `reassessment_reason` and chain the
    new row via `supersedes_assessment_id`. get_materiality returns the
    most recent row, so the fresh assessment is what callers see.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")

    prior = conn.execute(
        "SELECT assessment_id, materiality_locked FROM materiality_assessments "
        "WHERE engagement_id = ? ORDER BY calculated_at DESC LIMIT 1",
        (engagement_id,),
    ).fetchone()
    supersedes_id = None
    if prior is not None:
        prior_id = prior[0] if not hasattr(prior, "keys") else prior["assessment_id"]
        prior_locked = prior[1] if not hasattr(prior, "keys") else prior["materiality_locked"]
        # Reassessment requires the CPA to state why. Loudly in the error
        # so the UI can prompt rather than surface a cryptic constraint.
        if not (reassessment_reason or "").strip():
            raise ValueError(
                "materiality_reassessment_reason_required: a prior assessment "
                "exists; provide reassessment_reason explaining why you're "
                "reassessing (CAS 320.12)."
            )
        # Once working papers are signed the prior row is locked, but
        # stacking a new assessment on top is still allowed — the locked
        # row stays untouched and the fresh one becomes the current view.
        supersedes_id = prior_id

    assessment_id = f"mat_{secrets.token_hex(8)}"
    now = _utc_now()

    conn.execute(
        """INSERT INTO materiality_assessments
           (assessment_id, engagement_id, client_code, period,
            basis, basis_amount, planning_materiality,
            performance_materiality, clearly_trivial,
            calculated_at, calculated_by, notes,
            reassessment_reason, supersedes_assessment_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            assessment_id,
            engagement_id,
            eng["client_code"],
            eng["period"],
            materiality_dict["basis"],
            float(materiality_dict["basis_amount"]),
            float(materiality_dict["planning_materiality"]),
            float(materiality_dict["performance_materiality"]),
            float(materiality_dict["clearly_trivial"]),
            now,
            username,
            notes,
            (reassessment_reason or None),
            supersedes_id,
        ),
    )
    conn.commit()
    return assessment_id


def get_materiality(
    conn: sqlite3.Connection,
    engagement_id: str,
) -> dict[str, Any] | None:
    """Return the most recent materiality assessment for an engagement."""
    ensure_cas_tables(conn)
    row = conn.execute(
        """SELECT * FROM materiality_assessments
           WHERE engagement_id = ?
           ORDER BY calculated_at DESC, rowid DESC LIMIT 1""",
        (engagement_id,),
    ).fetchone()
    return dict(row) if row else None


def get_materiality_history(
    conn: sqlite3.Connection,
    engagement_id: str,
) -> list[dict[str, Any]]:
    """Return all materiality assessments for an engagement."""
    ensure_cas_tables(conn)
    rows = conn.execute(
        """SELECT * FROM materiality_assessments
           WHERE engagement_id = ?
           ORDER BY calculated_at DESC""",
        (engagement_id,),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Risk Assessment (CAS 315)
# ---------------------------------------------------------------------------

_RISK_MATRIX: dict[tuple[str, str], str] = {
    ("low",    "low"):    "low",
    ("low",    "medium"): "low",
    ("low",    "high"):   "medium",
    ("medium", "low"):    "low",
    ("medium", "medium"): "medium",
    ("medium", "high"):   "high",
    ("high",   "low"):    "medium",
    ("high",   "medium"): "high",
    ("high",   "high"):   "high",
}


def _combine_risk(inherent: str, control: str) -> str:
    """Combine inherent and control risk into a combined risk level."""
    return _RISK_MATRIX.get((inherent, control), "medium")


def _is_significant(inherent: str, control: str) -> bool:
    """A risk is significant when both inherent and control are high,
    or inherent is high and control is medium."""
    if inherent == "high" and control in ("high", "medium"):
        return True
    return False


# ---------------------------------------------------------------------------
# Account-type risk profiles (CAS 315)
# Maps account code prefix ranges to inherent/control risk defaults,
# relevant assertions, and whether the account is always significant.
# ---------------------------------------------------------------------------

_ACCOUNT_RISK_PROFILES: dict[str, dict[str, Any]] = {
    # Cash (1000-1099)
    "cash": {
        "inherent_risk": "low",
        "control_risk": "medium",
        "assertions": ["existence", "completeness", "accuracy"],
        "always_significant": True,
        "significant_risk": False,
    },
    # Accounts receivable (1100-1199)
    "receivable": {
        "inherent_risk": "medium",
        "control_risk": "medium",
        "assertions": ["existence", "cutoff", "rights_obligations"],
        "always_significant": False,  # significant if balance > 0
        "significant_risk": False,
    },
    # Inventory (1200-1299)
    "inventory": {
        "inherent_risk": "high",
        "control_risk": "high",
        "assertions": ["existence", "completeness", "rights_obligations"],
        "always_significant": False,
        "significant_risk": True,
    },
    # Fixed assets (1500-1599)
    "fixed_assets": {
        "inherent_risk": "low",
        "control_risk": "low",
        "assertions": ["existence", "rights_obligations", "accuracy"],
        "always_significant": False,
        "significant_risk": False,
    },
    # Accounts payable (2000-2099)
    "payable": {
        "inherent_risk": "medium",
        "control_risk": "medium",
        "assertions": ["completeness", "existence"],
        "always_significant": False,  # significant if balance > 0
        "significant_risk": False,
    },
    # Long-term debt (2500-2599)
    "long_term_debt": {
        "inherent_risk": "low",
        "control_risk": "low",
        "assertions": ["completeness", "existence", "accuracy"],
        "always_significant": False,  # significant if balance > 0
        "significant_risk": False,
    },
    # Revenue (4000-4999)
    "revenue": {
        "inherent_risk": "high",
        "control_risk": "medium",
        "assertions": ["completeness", "cutoff", "accuracy"],
        "always_significant": True,
        "significant_risk": False,
    },
    # Related party
    "related_party": {
        "inherent_risk": "high",
        "control_risk": "high",
        "assertions": ["existence", "completeness", "presentation"],
        "always_significant": True,
        "significant_risk": True,
    },
    # Default fallback for other accounts
    "default": {
        "inherent_risk": "medium",
        "control_risk": "medium",
        "assertions": ["completeness", "accuracy", "existence"],
        "always_significant": False,
        "significant_risk": False,
    },
}


def _get_account_risk_profile(account_code: str, account_name: str) -> dict[str, Any]:
    """Return the risk profile for an account based on its code range and name."""
    code = account_code.strip()
    name_lower = (account_name or "").lower()

    # Related party detection (by name keywords)
    related_kw = ("apparenté", "related party", "actionnaire", "shareholder",
                  "dirigeant", "administrateur", "loan to officer")
    if any(kw in name_lower for kw in related_kw):
        return _ACCOUNT_RISK_PROFILES["related_party"]

    if code.isdigit():
        c = int(code)
        if 1000 <= c <= 1099:
            return _ACCOUNT_RISK_PROFILES["cash"]
        if 1100 <= c <= 1199:
            return _ACCOUNT_RISK_PROFILES["receivable"]
        if 1200 <= c <= 1299:
            return _ACCOUNT_RISK_PROFILES["inventory"]
        if 1500 <= c <= 1599:
            return _ACCOUNT_RISK_PROFILES["fixed_assets"]
        if 2000 <= c <= 2099:
            return _ACCOUNT_RISK_PROFILES["payable"]
        if 2500 <= c <= 2599:
            return _ACCOUNT_RISK_PROFILES["long_term_debt"]
        if 4000 <= c <= 4999:
            return _ACCOUNT_RISK_PROFILES["revenue"]

    return _ACCOUNT_RISK_PROFILES["default"]


def account_is_significant(
    account_code: str,
    account_name: str,
    engagement_id: str,
    conn: sqlite3.Connection,
) -> bool:
    """Determine if an account is significant enough to include in the risk matrix.

    An account is significant if it is:
    - Always significant by account type (cash, revenue, related party), OR
    - Has a balance > $0 for account types that require it (AR, AP, LTD), OR
    - Has a balance exceeding performance materiality for other accounts.
    """
    profile = _get_account_risk_profile(account_code, account_name)

    if profile["always_significant"] or profile["significant_risk"]:
        return True

    # Look up balance from working papers
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        return False

    wp_row = conn.execute(
        """SELECT COALESCE(balance_per_books, 0) AS net_balance FROM working_papers
           WHERE LOWER(client_code) = LOWER(?) AND period = ?
             AND account_code = ?""",
        (eng["client_code"], eng["period"], account_code),
    ).fetchone()
    wp_row = dict(wp_row) if wp_row else None
    balance = abs(float(wp_row["net_balance"])) if wp_row else 0.0

    code = account_code.strip()
    if code.isdigit():
        c = int(code)
        # AR, AP, LTD: significant if balance > 0
        if c in range(1100, 1200) or c in range(2000, 2100) or c in range(2500, 2600):
            return balance > 0.0

    # Other accounts: only if balance > performance materiality
    mat = get_materiality(conn, engagement_id)
    if mat:
        perf_mat = float(mat["performance_materiality"])
        if balance > perf_mat:
            return True

    return False


def delete_risk_matrix(
    conn: sqlite3.Connection,
    engagement_id: str,
) -> int:
    """Delete all risk assessment rows for an engagement. Returns count deleted."""
    ensure_cas_tables(conn)
    cursor = conn.execute(
        "DELETE FROM risk_assessments WHERE engagement_id = ?",
        (engagement_id,),
    )
    conn.commit()
    return cursor.rowcount


def create_risk_matrix(
    conn: sqlite3.Connection,
    engagement_id: str,
    accounts_list: list[dict[str, str]],
    assessed_by: str = "",
) -> list[dict[str, Any]]:
    """Create risk assessment rows for significant accounts with realistic defaults.

    Only creates rows for accounts that are material or inherently risky.
    Each account gets only the 2-3 most relevant assertions for its type.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")

    now = _utc_now()
    results = []
    for acct in accounts_list:
        code = acct.get("account_code", "")
        name = acct.get("account_name", code)

        # FIX 1: Only include significant accounts
        if not account_is_significant(code, name, engagement_id, conn):
            continue

        # FIX 2 & 3: Get realistic risk profile with relevant assertions
        profile = _get_account_risk_profile(code, name)
        inherent = profile["inherent_risk"]
        control = profile["control_risk"]
        combined = _combine_risk(inherent, control)
        sig = 1 if (profile["significant_risk"] or _is_significant(inherent, control)) else 0

        for assertion in profile["assertions"]:
            # Check if already exists
            existing = conn.execute(
                """SELECT risk_id FROM risk_assessments
                   WHERE engagement_id = ? AND account_code = ? AND assertion = ?""",
                (engagement_id, code, assertion),
            ).fetchone()
            if existing:
                continue  # don't overwrite existing assessments

            risk_id = f"risk_{secrets.token_hex(8)}"
            conn.execute(
                """INSERT INTO risk_assessments
                   (risk_id, engagement_id, account_code, account_name,
                    assertion, inherent_risk, control_risk, combined_risk,
                    significant_risk, assessed_by, assessed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    risk_id, engagement_id, code, name,
                    assertion, inherent, control, combined,
                    sig, assessed_by, now,
                ),
            )
            results.append({
                "risk_id": risk_id,
                "engagement_id": engagement_id,
                "account_code": code,
                "account_name": name,
                "assertion": assertion,
                "inherent_risk": inherent,
                "control_risk": control,
                "combined_risk": combined,
                "significant_risk": bool(sig),
                "assessed_by": assessed_by,
                "assessed_at": now,
            })
    conn.commit()
    return results


def assess_risk(
    conn: sqlite3.Connection,
    risk_id: str,
    *,
    inherent_risk: str | None = None,
    control_risk: str | None = None,
    notes: str | None = None,
    assessed_by: str | None = None,
) -> dict[str, Any]:
    """Update a single risk assessment row."""
    ensure_cas_tables(conn)
    row = conn.execute(
        "SELECT * FROM risk_assessments WHERE risk_id = ?", (risk_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Risk assessment not found: {risk_id}")

    r = dict(row)
    if inherent_risk is not None and inherent_risk in VALID_RISK_LEVELS:
        r["inherent_risk"] = inherent_risk
    if control_risk is not None and control_risk in VALID_RISK_LEVELS:
        r["control_risk"] = control_risk
    if notes is not None:
        r["notes"] = notes
    if assessed_by is not None:
        r["assessed_by"] = assessed_by

    # Recalculate combined risk and significant flag
    r["combined_risk"] = _combine_risk(r["inherent_risk"], r["control_risk"])
    r["significant_risk"] = 1 if _is_significant(r["inherent_risk"], r["control_risk"]) else 0

    conn.execute(
        """UPDATE risk_assessments SET
           inherent_risk = ?, control_risk = ?, combined_risk = ?,
           significant_risk = ?, notes = ?, assessed_by = ?, assessed_at = ?
           WHERE risk_id = ?""",
        (
            r["inherent_risk"], r["control_risk"], r["combined_risk"],
            r["significant_risk"], r.get("notes"), r.get("assessed_by"),
            _utc_now(), risk_id,
        ),
    )
    conn.commit()
    return dict(conn.execute(
        "SELECT * FROM risk_assessments WHERE risk_id = ?", (risk_id,)
    ).fetchone())


def create_risk_assessment(
    conn: sqlite3.Connection,
    engagement_id: str,
    client_code: str = "",
    period: str = "",
    assertion: str = "",
    account_area: str = "",
    inherent_risk: str = "medium",
    control_risk: str = "medium",
    assessed_by: str = "",
) -> dict[str, Any]:
    """Create a single risk assessment row for an engagement."""
    ensure_cas_tables(conn)
    # Validate risk levels
    if inherent_risk not in VALID_RISK_LEVELS:
        raise ValueError(f"Invalid inherent_risk: {inherent_risk!r}. Must be one of {VALID_RISK_LEVELS}")
    if control_risk not in VALID_RISK_LEVELS:
        raise ValueError(f"Invalid control_risk: {control_risk!r}. Must be one of {VALID_RISK_LEVELS}")

    combined = _combine_risk(inherent_risk, control_risk)
    sig = 1 if _is_significant(inherent_risk, control_risk) else 0
    now = _utc_now()
    risk_id = f"risk_{secrets.token_hex(8)}"

    conn.execute(
        """INSERT INTO risk_assessments
           (risk_id, engagement_id, account_code, account_name,
            assertion, inherent_risk, control_risk, combined_risk,
            significant_risk, assessed_by, assessed_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (risk_id, engagement_id, account_area, account_area,
         assertion, inherent_risk, control_risk, combined,
         sig, assessed_by, now),
    )
    conn.commit()
    return {
        "risk_id": risk_id,
        "engagement_id": engagement_id,
        "assertion": assertion,
        "inherent_risk": inherent_risk,
        "control_risk": control_risk,
        "combined_risk": combined,
        "significant_risk": bool(sig),
    }


def get_risk_assessment(
    conn: sqlite3.Connection,
    engagement_id: str,
    account_code: str | None = None,
) -> list[dict[str, Any]]:
    """Return risk assessments for an engagement, optionally filtered by account."""
    ensure_cas_tables(conn)
    if account_code:
        rows = conn.execute(
            """SELECT * FROM risk_assessments
               WHERE engagement_id = ? AND account_code = ?
               ORDER BY account_code, assertion""",
            (engagement_id, account_code),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM risk_assessments
               WHERE engagement_id = ?
               ORDER BY account_code, assertion""",
            (engagement_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_risk_summary(
    conn: sqlite3.Connection,
    engagement_id: str,
) -> dict[str, Any]:
    """Return a summary of risk assessments for an engagement."""
    ensure_cas_tables(conn)
    rows = get_risk_assessment(conn, engagement_id)
    total = len(rows)
    high = sum(1 for r in rows if r["combined_risk"] == "high")
    medium = sum(1 for r in rows if r["combined_risk"] == "medium")
    low = sum(1 for r in rows if r["combined_risk"] == "low")
    significant = sum(1 for r in rows if r["significant_risk"])

    # Group by account for account-level summary
    accounts: dict[str, dict[str, Any]] = {}
    for r in rows:
        code = r["account_code"]
        if code not in accounts:
            accounts[code] = {
                "account_code": code,
                "account_name": r["account_name"],
                "assertions": 0,
                "high_count": 0,
                "significant": False,
            }
        accounts[code]["assertions"] += 1
        if r["combined_risk"] == "high":
            accounts[code]["high_count"] += 1
        if r["significant_risk"]:
            accounts[code]["significant"] = True

    return {
        "total_assessments": total,
        "high": high,
        "medium": medium,
        "low": low,
        "significant_risks": significant,
        "accounts": list(accounts.values()),
    }


# ---------------------------------------------------------------------------
# Standard Controls Library (CAS 330)
# ---------------------------------------------------------------------------

STANDARD_CONTROLS: list[dict[str, str]] = [
    {"name": "AP authorization", "objective": "Invoices approved before payment", "description": "All invoices must be authorized by an appropriate person before payment is processed."},
    {"name": "Bank reconciliation", "objective": "Monthly reconciliation prepared and reviewed", "description": "Bank statements are reconciled to GL monthly and reviewed by a second person."},
    {"name": "Payroll authorization", "objective": "Payroll approved by owner/manager", "description": "Payroll runs require written or electronic approval from the owner or manager."},
    {"name": "Revenue completeness", "objective": "All sales recorded", "description": "All revenue transactions are recorded in the accounting system on a timely basis."},
    {"name": "Physical inventory", "objective": "Periodic count performed", "description": "Physical inventory counts are performed at least annually and reconciled to the GL."},
    {"name": "Access controls", "objective": "Accounting system access restricted", "description": "Access to the accounting system is restricted to authorized personnel with appropriate segregation of duties."},
    {"name": "Journal entry approval", "objective": "Manual JEs require approval", "description": "All manual journal entries require approval from a second authorized person before posting."},
    {"name": "Vendor master changes", "objective": "New vendors require authorization", "description": "New vendor setup and changes to vendor banking information require management authorization."},
    {"name": "Fixed asset additions", "objective": "Capital purchases require approval", "description": "Capital expenditures above a defined threshold require prior written approval."},
    {"name": "Credit card reconciliation", "objective": "Monthly statements reconciled", "description": "Credit card statements are reconciled monthly with supporting receipts reviewed."},
    {"name": "GST/QST remittance", "objective": "Filed on time", "description": "GST/QST returns are filed and remitted by the required deadlines per Revenu Québec requirements."},
    {"name": "RL-1/T4 reconciliation", "objective": "Slips agree to payroll records", "description": "Year-end RL-1 and T4 slips are reconciled to payroll records before filing."},
    {"name": "Bank signing authority", "objective": "Dual signatures for large payments", "description": "Cheques and electronic payments above a defined threshold require dual authorization."},
    {"name": "Petty cash", "objective": "Reconciled and replenished regularly", "description": "Petty cash is counted, reconciled, and replenished on a regular basis with supporting vouchers."},
    {"name": "Document retention", "objective": "Invoices retained 7 years", "description": "All financial documents are retained for a minimum of 7 years per Quebec and federal requirements."},
]

VALID_TEST_TYPES = {"walkthrough", "reperformance", "observation", "inquiry"}
VALID_CONCLUSIONS = {"effective", "ineffective", "partially_effective"}
VALID_RELATIONSHIP_TYPES = {"owner", "family_member", "affiliated_company", "key_management", "board_member"}
VALID_MEASUREMENT_BASES = {"carrying_amount", "exchange_amount", "cost"}
VALID_REP_LETTER_STATUSES = {"draft", "signed", "refused"}


# ---------------------------------------------------------------------------
# CAS 580 — Management Representation Letter
# ---------------------------------------------------------------------------

def generate_management_rep_letter(
    engagement_id: str,
    language: str,
    conn: sqlite3.Connection,
) -> str:
    """Generate a standard bilingual management representation letter per CPA Quebec standards."""
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")

    client = eng["client_code"]
    period = eng.get("period", "")
    eng_type = eng.get("engagement_type", "audit")

    if language == "fr":
        letter = (
            f"Lettre de déclaration de la direction\n\n"
            f"À l'attention de : OtoCPA CPA\n"
            f"Client : {client}\n"
            f"Période se terminant le : {period}\n"
            f"Type de mission : {eng_type}\n\n"
            f"Dans le cadre de votre {'audit' if eng_type == 'audit' else 'examen' if eng_type == 'review' else 'compilation'} "
            f"de nos états financiers pour la période se terminant le {period}, nous confirmons, "
            f"au meilleur de notre connaissance et en toute bonne foi, les déclarations suivantes :\n\n"
            f"1. Les états financiers sont présentés fidèlement conformément aux normes comptables applicables.\n"
            f"2. Toutes les opérations ont été comptabilisées et sont reflétées dans les états financiers.\n"
            f"3. Il n'existe aucune partie liée non divulguée ni aucune opération avec des parties liées "
            f"qui n'aurait pas été correctement comptabilisée ou divulguée.\n"
            f"4. Il n'y a aucun événement postérieur à la date de clôture qui nécessiterait un ajustement "
            f"ou une divulgation dans les états financiers et qui n'aurait pas été pris en compte.\n"
            f"5. Nous avons divulgué toute fraude connue ou soupçonnée affectant l'entité impliquant "
            f"la direction, les employés ayant un rôle important dans le contrôle interne, ou d'autres "
            f"personnes lorsque la fraude pourrait avoir une incidence significative sur les états financiers.\n"
            f"6. Les procès-verbaux des réunions des actionnaires et du conseil d'administration sont complets "
            f"et tous les accords significatifs nous ont été fournis.\n\n"
            f"Signature : ____________________________\n"
            f"Nom : \n"
            f"Titre : \n"
            f"Date : \n"
        )
    else:
        letter = (
            f"Management Representation Letter\n\n"
            f"To: OtoCPA CPA\n"
            f"Client: {client}\n"
            f"Period ending: {period}\n"
            f"Engagement type: {eng_type}\n\n"
            f"In connection with your {'audit' if eng_type == 'audit' else 'review' if eng_type == 'review' else 'compilation'} "
            f"of our financial statements for the period ending {period}, we confirm, "
            f"to the best of our knowledge and belief, the following representations:\n\n"
            f"1. The financial statements are fairly presented in accordance with the applicable accounting standards.\n"
            f"2. All transactions have been recorded and are reflected in the financial statements.\n"
            f"3. There are no undisclosed related parties or related party transactions "
            f"that have not been properly recorded or disclosed.\n"
            f"4. There are no subsequent events after the period end date that would require adjustment "
            f"or disclosure in the financial statements that have not been taken into account.\n"
            f"5. We have disclosed all known or suspected fraud affecting the entity involving "
            f"management, employees who have significant roles in internal control, or others "
            f"where the fraud could have a material effect on the financial statements.\n"
            f"6. The minutes of shareholders' and board of directors' meetings are complete "
            f"and all significant agreements have been made available to you.\n\n"
            f"Signature: ____________________________\n"
            f"Name: \n"
            f"Title: \n"
            f"Date: \n"
        )
    return letter


def save_rep_letter(
    engagement_id: str,
    draft_fr: str,
    draft_en: str,
    conn: sqlite3.Connection,
    created_by: str = "",
) -> str:
    """Save a management representation letter draft. Returns letter_id."""
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")

    # Check if one already exists — update it
    existing = conn.execute(
        "SELECT letter_id FROM management_representation_letters WHERE engagement_id = ? ORDER BY created_at DESC LIMIT 1",
        (engagement_id,),
    ).fetchone()
    if existing:
        letter_id = existing["letter_id"]
        conn.execute(
            """UPDATE management_representation_letters
               SET draft_text_fr = ?, draft_text_en = ?, status = 'draft'
               WHERE letter_id = ?""",
            (draft_fr, draft_en, letter_id),
        )
        conn.commit()
        return letter_id

    letter_id = f"rep_{secrets.token_hex(8)}"
    now = _utc_now()
    conn.execute(
        """INSERT INTO management_representation_letters
           (letter_id, engagement_id, client_code, period_end_date,
            draft_text_fr, draft_text_en, status, created_by, created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            letter_id, engagement_id, eng["client_code"], eng.get("period", ""),
            draft_fr, draft_en, "draft", created_by, now,
        ),
    )
    conn.commit()
    return letter_id


def mark_letter_signed(
    letter_id: str,
    management_name: str,
    management_title: str,
    conn: sqlite3.Connection,
) -> bool:
    """Mark a representation letter as signed."""
    ensure_cas_tables(conn)
    row = conn.execute(
        "SELECT * FROM management_representation_letters WHERE letter_id = ?",
        (letter_id,),
    ).fetchone()
    if not row:
        return False
    now = _utc_now()
    conn.execute(
        """UPDATE management_representation_letters
           SET status = 'signed', management_name = ?, management_title = ?,
               signed_at = ?, signed_by = ?
           WHERE letter_id = ?""",
        (management_name, management_title, now, management_name, letter_id),
    )
    conn.commit()
    return True


# ---------------------------------------------------------------------------
# CAS 580 — Rep Letter PDF (Sprint F Fix 4)
# ---------------------------------------------------------------------------

# The 10 representations required by CAS 580 Appendix 2. These are the
# statutory minimum; extra firm-specific reps can be appended by the user.
CAS_580_STANDARD_REPRESENTATIONS: list[dict[str, str]] = [
    {
        "en": "We have fulfilled our responsibilities for the preparation of the financial statements in accordance with the applicable financial reporting framework, for the period ended {period_end}.",
        "fr": "Nous avons rempli nos responsabilités à l'égard de la préparation des états financiers conformément au référentiel d'information financière applicable, pour la période close le {period_end}.",
    },
    {
        "en": "We acknowledge our responsibility for the design, implementation and maintenance of internal control to prevent and detect fraud.",
        "fr": "Nous reconnaissons notre responsabilité à l'égard de la conception, de la mise en place et du maintien du contrôle interne pour prévenir et détecter la fraude.",
    },
    {
        "en": "Significant assumptions used in making accounting estimates are reasonable.",
        "fr": "Les hypothèses importantes utilisées pour établir les estimations comptables sont raisonnables.",
    },
    {
        "en": "Related party relationships and transactions have been appropriately accounted for and disclosed.",
        "fr": "Les relations et opérations avec les parties liées ont été comptabilisées et présentées de manière appropriée.",
    },
    {
        "en": "All events subsequent to the date of the financial statements requiring adjustment or disclosure have been adjusted or disclosed.",
        "fr": "Tous les événements postérieurs à la date des états financiers nécessitant un ajustement ou une divulgation ont été ajustés ou divulgués.",
    },
    {
        "en": "The effects of uncorrected misstatements are immaterial, both individually and in aggregate, to the financial statements as a whole.",
        "fr": "Les incidences des anomalies non corrigées sont non significatives, individuellement et collectivement, pour les états financiers pris dans leur ensemble.",
    },
    {
        "en": "We have disclosed all known or suspected fraud involving management, employees who have significant roles in internal control, or others where fraud could have a material effect on the financial statements.",
        "fr": "Nous avons divulgué toute fraude connue ou soupçonnée impliquant la direction, les employés ayant un rôle important dans le contrôle interne, ou d'autres personnes lorsque la fraude pourrait avoir une incidence significative sur les états financiers.",
    },
    {
        "en": "We have disclosed all known instances of non-compliance or suspected non-compliance with laws and regulations whose effects should be considered when preparing the financial statements.",
        "fr": "Nous avons divulgué tous les cas connus de non-conformité ou les cas soupçonnés de non-conformité aux lois et règlements dont les effets devraient être pris en considération lors de la préparation des états financiers.",
    },
    {
        "en": "We have disclosed all information in relation to fraud or suspected fraud communicated to us by employees, former employees, analysts, regulators or others.",
        "fr": "Nous avons divulgué toutes les informations relatives à la fraude ou à la fraude soupçonnée qui nous ont été communiquées par des employés, d'anciens employés, des analystes, des autorités de réglementation ou d'autres personnes.",
    },
    {
        "en": "We have disclosed all known actual or possible litigation and claims whose effects should be considered when preparing the financial statements.",
        "fr": "Nous avons divulgué tous les litiges et réclamations connus, réels ou possibles, dont les effets devraient être pris en considération lors de la préparation des états financiers.",
    },
]


def _rep_letter_get_engagement_period_end(conn, engagement_id: str) -> str:
    """Derive a period_end_date from the engagement's period or raise.

    Accepts either YYYY-MM (month-only) or YYYY-MM-DD. For month-only,
    uses the last day of that month.
    """
    from src.engines.audit_engine import get_engagement
    from datetime import date
    import calendar as _cal

    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")
    period = str(eng.get("period") or "").strip()
    if not period:
        raise ValueError(
            "Engagement must have a period set before a rep letter can be "
            "issued (CAS 580 requires a dated period end).",
        )
    if len(period) == 10 and period[4] == "-" and period[7] == "-":
        return period
    if len(period) == 7 and period[4] == "-":
        y, m = int(period[:4]), int(period[5:7])
        last = _cal.monthrange(y, m)[1]
        return f"{period}-{last:02d}"
    # Unknown shape — raise rather than fabricate a date.
    raise ValueError(
        f"Engagement period '{period}' cannot be parsed as YYYY-MM or "
        "YYYY-MM-DD; set an explicit period_end_date first.",
    )


def generate_rep_letter_pdf(
    engagement_id: str,
    conn: sqlite3.Connection,
    *,
    firm_name: str = "OtoCPA CPA",
    firm_address: str = "",
    auditor_name: str = "",
    management_name: str = "",
    management_title: str = "Chief Executive Officer",
    cfo_name: str = "",
    cfo_title: str = "Chief Financial Officer",
    language: str = "en",
    extra_representations: list[str] | None = None,
    output_dir: str | Path | None = None,
    persist_evidence: bool = True,
) -> tuple[bytes, str]:
    """Generate a CAS 580 management representation letter PDF.

    Validates that the engagement has a period_end_date, embeds all 10
    standard representations, adds signature blocks for CEO and CFO, and
    (if ``persist_evidence`` is True) stores the PDF under the engagement
    evidence folder and records a row in ``audit_evidence``.

    Returns (pdf_bytes, file_path).
    """
    ensure_cas_tables(conn)
    period_end = _rep_letter_get_engagement_period_end(conn, engagement_id)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    client = eng.get("client_code") or ""
    today = _utc_now()[:10]

    try:
        pdf_bytes = _render_rep_letter_reportlab(
            firm_name=firm_name,
            firm_address=firm_address,
            auditor_name=auditor_name or firm_name,
            client_code=client,
            period_end=period_end,
            today=today,
            management_name=management_name,
            management_title=management_title,
            cfo_name=cfo_name,
            cfo_title=cfo_title,
            language=language,
            extra_representations=extra_representations or [],
        )
    except ImportError:
        pdf_bytes = _render_rep_letter_minimal(
            firm_name=firm_name,
            auditor_name=auditor_name or firm_name,
            client_code=client,
            period_end=period_end,
            today=today,
            management_name=management_name,
            management_title=management_title,
            cfo_name=cfo_name,
            cfo_title=cfo_title,
            language=language,
            extra_representations=extra_representations or [],
        )

    file_path = ""
    if persist_evidence:
        base_dir = Path(output_dir) if output_dir else ROOT_DIR / "data" / "rep_letters"
        base_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = base_dir / f"rep_letter_{engagement_id}_{stamp}.pdf"
        path.write_bytes(pdf_bytes)
        file_path = str(path)
        try:
            conn.execute(
                """INSERT INTO audit_evidence
                   (evidence_id, document_id, evidence_type, linked_document_ids,
                    match_status, notes, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    f"rep_{engagement_id}_{stamp}",
                    engagement_id,
                    "representation_letter",
                    "",
                    "complete",
                    f"CAS 580 rep letter; period_end={period_end}; file={file_path}",
                    today,
                    today,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            # Table may not exist or a row with this id already present — skip silently.
            pass

    return pdf_bytes, file_path


def _render_rep_letter_reportlab(
    *,
    firm_name: str,
    firm_address: str,
    auditor_name: str,
    client_code: str,
    period_end: str,
    today: str,
    management_name: str,
    management_title: str,
    cfo_name: str,
    cfo_title: str,
    language: str,
    extra_representations: list[str],
) -> bytes:
    from io import BytesIO
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer,
    )

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.75 * inch, bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    h_style = ParagraphStyle("h", parent=styles["Heading2"], fontSize=13, spaceAfter=10)
    normal = styles["BodyText"]
    rep_style = ParagraphStyle("rep", parent=normal, leftIndent=18, spaceAfter=8)

    story: list = []
    # Letterhead
    story.append(Paragraph(f"<b>{firm_name}</b>", h_style))
    if firm_address:
        story.append(Paragraph(firm_address, normal))
    story.append(Spacer(1, 0.2 * inch))

    # Date
    story.append(Paragraph(f"Date: {today}", normal))
    story.append(Spacer(1, 0.1 * inch))

    # Addressee
    story.append(Paragraph(f"To: {auditor_name}", normal))
    story.append(Spacer(1, 0.2 * inch))

    if language == "fr":
        intro = (
            f"La présente lettre de déclaration est fournie dans le cadre de votre mission "
            f"d'audit des états financiers de <b>{client_code}</b> pour la période close le "
            f"<b>{period_end}</b>, dans le but d'exprimer une opinion sur la fidélité "
            f"avec laquelle les états financiers présentent, à tous égards importants, "
            f"la situation financière et les résultats d'exploitation de l'entité."
        )
        rep_title = "Déclarations de la direction (CAS 580)"
    else:
        intro = (
            f"This representation letter is provided in connection with your audit of the "
            f"financial statements of <b>{client_code}</b> for the period ended "
            f"<b>{period_end}</b>, for the purpose of expressing an opinion as to whether "
            f"the financial statements present fairly, in all material respects, the "
            f"financial position and results of operations of the entity."
        )
        rep_title = "Management Representations (CAS 580)"

    story.append(Paragraph(intro, normal))
    story.append(Spacer(1, 0.2 * inch))

    # Representations
    story.append(Paragraph(f"<b>{rep_title}</b>", h_style))
    for i, rep in enumerate(CAS_580_STANDARD_REPRESENTATIONS, start=1):
        text = rep.get(language, rep["en"]).format(period_end=period_end)
        story.append(Paragraph(f"{i}. {text}", rep_style))
    # Extra user reps
    for i, extra in enumerate(extra_representations, start=len(CAS_580_STANDARD_REPRESENTATIONS) + 1):
        story.append(Paragraph(f"{i}. {extra}", rep_style))

    story.append(Spacer(1, 0.4 * inch))

    # Signature blocks
    story.append(Paragraph("_________________________________________", normal))
    story.append(Paragraph(
        f"{management_name or '(signature)'}, {management_title}", normal,
    ))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("_________________________________________", normal))
    story.append(Paragraph(
        f"{cfo_name or '(signature)'}, {cfo_title}", normal,
    ))

    doc.build(story)
    return buf.getvalue()


def _render_rep_letter_minimal(
    *,
    firm_name: str,
    auditor_name: str,
    client_code: str,
    period_end: str,
    today: str,
    management_name: str,
    management_title: str,
    cfo_name: str,
    cfo_title: str,
    language: str,
    extra_representations: list[str],
) -> bytes:
    """Lightweight hand-rolled PDF fallback when reportlab is unavailable."""
    lines = [firm_name, "", f"Date: {today}", "", f"To: {auditor_name}", ""]
    if language == "fr":
        lines.append(
            f"Lettre de declaration -- {client_code} -- Periode close le {period_end}"
        )
    else:
        lines.append(
            f"Management Representation Letter -- {client_code} -- Period ended {period_end}"
        )
    lines.append("")
    for i, rep in enumerate(CAS_580_STANDARD_REPRESENTATIONS, start=1):
        text = rep.get(language, rep["en"]).format(period_end=period_end)
        # Split long reps into ~80-char chunks for the fallback.
        wrapped = _wrap_text(f"{i}. {text}", 90)
        lines.extend(wrapped)
        lines.append("")
    for i, extra in enumerate(extra_representations, start=len(CAS_580_STANDARD_REPRESENTATIONS) + 1):
        lines.extend(_wrap_text(f"{i}. {extra}", 90))
        lines.append("")
    lines += [
        "",
        "_________________________________________",
        f"{management_name or '(signature)'}, {management_title}",
        "",
        "_________________________________________",
        f"{cfo_name or '(signature)'}, {cfo_title}",
    ]

    page_lines = [lines[i:i + 55] for i in range(0, len(lines), 55)] or [lines]
    objects: list[bytes] = []

    def _add(obj: bytes) -> int:
        objects.append(obj)
        return len(objects)

    page_ids = list(range(3, 3 + len(page_lines)))
    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    _add(b"<< /Type /Catalog /Pages 2 0 R >>")
    _add(f"<< /Type /Pages /Kids [{kids}] /Count {len(page_lines)} >>".encode())
    font_obj_id = 3 + 2 * len(page_lines)
    for idx in range(len(page_lines)):
        page_obj = (
            f"<< /Type /Page /Parent 2 0 R "
            f"/Resources << /Font << /F1 {font_obj_id} 0 R >> >> "
            f"/MediaBox [0 0 612 792] "
            f"/Contents {3 + len(page_lines) + idx} 0 R >>"
        ).encode()
        _add(page_obj)
    for chunk in page_lines:
        stream_lines = ["BT", "/F1 10 Tf", "50 752 Td"]
        for ln in chunk:
            safe = ln.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            stream_lines.append(f"({safe}) Tj")
            stream_lines.append("0 -13 Td")
        stream_lines.append("ET")
        stream = "\n".join(stream_lines).encode("latin-1", errors="replace")
        _add(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
    _add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode()
    out += (
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    ).encode()
    return bytes(out)


def _wrap_text(s: str, width: int) -> list[str]:
    words = s.split(" ")
    out: list[str] = []
    cur = ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            if cur:
                out.append(cur)
            cur = w
        else:
            cur = (cur + " " + w).strip()
    if cur:
        out.append(cur)
    return out


def get_rep_letter(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any] | None:
    """Return the most recent representation letter for an engagement."""
    ensure_cas_tables(conn)
    row = conn.execute(
        """SELECT * FROM management_representation_letters
           WHERE engagement_id = ?
           ORDER BY created_at DESC LIMIT 1""",
        (engagement_id,),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# CAS 330 — Control Testing Documentation
# ---------------------------------------------------------------------------

def create_control_test(
    engagement_id: str,
    control_name: str,
    control_objective: str,
    test_type: str,
    conn: sqlite3.Connection,
    control_description: str = "",
    tested_by: str = "",
) -> str:
    """Create a new control test record. Returns test_id."""
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")
    if test_type not in VALID_TEST_TYPES:
        test_type = "walkthrough"

    test_id = f"ctrl_{secrets.token_hex(8)}"
    now = _utc_now()
    conn.execute(
        """INSERT INTO control_tests
           (test_id, engagement_id, control_name, control_description,
            control_objective, test_type, tested_by, tested_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (test_id, engagement_id, control_name, control_description,
         control_objective, test_type, tested_by, now),
    )
    conn.commit()
    return test_id


def record_test_results(
    test_id: str,
    items_tested: int,
    exceptions_found: int,
    exception_details: str,
    conclusion: str,
    conn: sqlite3.Connection,
) -> bool:
    """Record results for a control test."""
    ensure_cas_tables(conn)
    row = conn.execute(
        "SELECT * FROM control_tests WHERE test_id = ?", (test_id,)
    ).fetchone()
    if not row:
        return False
    if conclusion not in VALID_CONCLUSIONS:
        conclusion = "effective"
    conn.execute(
        """UPDATE control_tests
           SET items_tested = ?, exceptions_found = ?, exception_details = ?,
               conclusion = ?, tested_at = ?
           WHERE test_id = ?""",
        (items_tested, exceptions_found, exception_details, conclusion, _utc_now(), test_id),
    )
    conn.commit()
    return True


def get_control_tests(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Return all control tests for an engagement."""
    ensure_cas_tables(conn)
    rows = conn.execute(
        """SELECT * FROM control_tests
           WHERE engagement_id = ?
           ORDER BY control_name""",
        (engagement_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_control_effectiveness_summary(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Return summary of control test conclusions."""
    ensure_cas_tables(conn)
    tests = get_control_tests(engagement_id, conn)
    total = len(tests)
    effective = sum(1 for t in tests if t.get("conclusion") == "effective")
    ineffective = sum(1 for t in tests if t.get("conclusion") == "ineffective")
    partial = sum(1 for t in tests if t.get("conclusion") == "partially_effective")
    return {
        "total": total,
        "effective": effective,
        "ineffective": ineffective,
        "partially_effective": partial,
    }


# ---------------------------------------------------------------------------
# CAS 550 — Related Party Procedures
# ---------------------------------------------------------------------------

def add_related_party(
    client_code: str,
    party_name: str,
    relationship_type: str,
    conn: sqlite3.Connection,
    ownership_percentage: float | None = None,
    notes: str = "",
    identified_by: str = "",
) -> str:
    """Add a related party. Returns party_id."""
    ensure_cas_tables(conn)
    # FIX 3: Check period lock
    try:
        locked = conn.execute(
            "SELECT 1 FROM period_close_locks WHERE LOWER(client_code) = LOWER(?)",
            (client_code,),
        ).fetchone()
        if locked:
            raise ValueError("Period is locked — cannot modify related parties after period close")
    except sqlite3.OperationalError:
        pass  # period_close_locks table may not exist
    if relationship_type not in VALID_RELATIONSHIP_TYPES:
        relationship_type = "affiliated_company"
    party_id = f"rp_{secrets.token_hex(8)}"
    now = _utc_now()
    conn.execute(
        """INSERT INTO related_parties
           (party_id, client_code, party_name, relationship_type,
            ownership_percentage, notes, identified_by, identified_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (party_id, client_code, party_name, relationship_type,
         ownership_percentage, notes, identified_by, now),
    )
    conn.commit()
    return party_id


def get_related_parties(
    client_code: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Return all related parties for a client."""
    ensure_cas_tables(conn)
    rows = conn.execute(
        """SELECT * FROM related_parties
           WHERE LOWER(client_code) = LOWER(?)
           ORDER BY party_name""",
        (client_code,),
    ).fetchall()
    return [dict(r) for r in rows]


def flag_related_party_transaction(
    engagement_id: str,
    document_id: str,
    party_id: str,
    measurement_basis: str,
    conn: sqlite3.Connection,
    amount: float | None = None,
    description: str = "",
    transaction_date: str = "",
) -> str:
    """Flag a document as a related party transaction. Returns rpt_id."""
    ensure_cas_tables(conn)
    # FIX 3: Check period lock
    try:
        from src.engines.audit_engine import get_engagement
        eng = get_engagement(conn, engagement_id)
        if eng:
            locked = conn.execute(
                "SELECT 1 FROM period_close_locks WHERE LOWER(client_code) = LOWER(?) AND period = ?",
                (eng["client_code"], eng["period"]),
            ).fetchone()
            if locked:
                raise ValueError("Period is locked — cannot modify related parties after period close")
    except sqlite3.OperationalError:
        pass  # period_close_locks table may not exist
    if measurement_basis not in VALID_MEASUREMENT_BASES:
        measurement_basis = "exchange_amount"
    rpt_id = f"rpt_{secrets.token_hex(8)}"
    conn.execute(
        """INSERT INTO related_party_transactions
           (rpt_id, engagement_id, party_id, document_id,
            transaction_date, amount, description, measurement_basis)
           VALUES (?,?,?,?,?,?,?,?)""",
        (rpt_id, engagement_id, party_id, document_id,
         transaction_date, amount, description, measurement_basis),
    )
    conn.commit()
    return rpt_id


def get_related_party_transactions(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Return all related party transactions for an engagement."""
    ensure_cas_tables(conn)
    rows = conn.execute(
        """SELECT rpt.*, rp.party_name, rp.relationship_type
           FROM related_party_transactions rpt
           LEFT JOIN related_parties rp ON rpt.party_id = rp.party_id
           WHERE rpt.engagement_id = ?
           ORDER BY rpt.transaction_date""",
        (engagement_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def auto_detect_related_parties(
    client_code: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Scan vendor_memory and documents for potential related parties.

    Looks for:
    - Vendors matching owner name patterns
    - Same address as client
    - Unusually round amounts to same vendor
    """
    ensure_cas_tables(conn)
    results: list[dict[str, Any]] = []

    # Check if vendor_memory table exists
    vm_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='vendor_memory'"
    ).fetchone()
    if not vm_exists:
        return results

    # Look for vendors with unusually round amounts (multiples of 1000)
    rows = conn.execute(
        """SELECT vendor, client_code, last_amount, COUNT(*) as txn_count
           FROM vendor_memory
           WHERE LOWER(client_code) = LOWER(?)
           GROUP BY vendor
           HAVING txn_count >= 2""",
        (client_code,),
    ).fetchall()

    for row in rows:
        vendor = row["vendor"] if isinstance(row, dict) else row[0]
        amount = row["last_amount"] if isinstance(row, dict) else row[2]
        count = row["txn_count"] if isinstance(row, dict) else row[3]
        evidence = []

        # Check for round amounts
        if amount and float(amount) > 0 and float(amount) % 1000 == 0:
            evidence.append("round_amounts")

        # Check for high frequency
        if count >= 5:
            evidence.append("high_frequency")

        if evidence:
            results.append({
                "vendor": vendor,
                "client_code": client_code,
                "evidence": evidence,
                "transaction_count": count,
                "last_amount": amount,
                "status": "potential_related_party",
            })

    return results


def get_related_party_summary(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Return a summary of related party activity for an engagement."""
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        return {"parties": 0, "transactions": 0, "total_amount": 0.0, "disclosure_required": 0}

    parties = get_related_parties(eng["client_code"], conn)
    transactions = get_related_party_transactions(engagement_id, conn)
    total_amount = sum(float(t.get("amount") or 0) for t in transactions)
    disclosure_count = sum(1 for t in transactions if t.get("disclosure_required"))

    return {
        "parties": len(parties),
        "transactions": len(transactions),
        "total_amount": round(total_amount, 2),
        "disclosure_required": disclosure_count,
    }


def generate_related_party_disclosure(
    engagement_id: str,
    language: str,
    conn: sqlite3.Connection,
) -> str:
    """Draft a related party disclosure note for financial statements."""
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")

    parties = get_related_parties(eng["client_code"], conn)
    transactions = get_related_party_transactions(engagement_id, conn)

    if not parties and not transactions:
        if language == "fr":
            return "Aucune partie liée ni opération entre parties liées n'a été identifiée pour la période."
        return "No related parties or related party transactions have been identified for the period."

    if language == "fr":
        lines = [
            f"Note aux états financiers — Parties liées",
            f"Client : {eng['client_code']}",
            f"Période : {eng.get('period', '')}",
            "",
            "Parties liées identifiées :",
        ]
        for p in parties:
            pct = f" ({p.get('ownership_percentage', 0) or 0}%)" if p.get("ownership_percentage") else ""
            lines.append(f"  - {p['party_name']} — {p['relationship_type']}{pct}")
        if transactions:
            lines.append("")
            lines.append("Opérations entre parties liées :")
            total = _ZERO
            for t in transactions:
                amt = _to_decimal(t.get("amount", 0))
                total += amt
                desc = t.get("description", "") or ""
                lines.append(f"  - {t.get('party_name', 'N/A')} : {float(amt):,.2f} $ — {desc}")
            lines.append(f"\nTotal des opérations entre parties liées : {float(total):,.2f} $")
    else:
        lines = [
            f"Financial Statement Note — Related Parties",
            f"Client: {eng['client_code']}",
            f"Period: {eng.get('period', '')}",
            "",
            "Identified related parties:",
        ]
        for p in parties:
            pct = f" ({p.get('ownership_percentage', 0) or 0}%)" if p.get("ownership_percentage") else ""
            lines.append(f"  - {p['party_name']} — {p['relationship_type']}{pct}")
        if transactions:
            lines.append("")
            lines.append("Related party transactions:")
            total = _ZERO
            for t in transactions:
                amt = _to_decimal(t.get("amount", 0))
                total += amt
                desc = t.get("description", "") or ""
                lines.append(f"  - {t.get('party_name', 'N/A')}: ${float(amt):,.2f} — {desc}")
            lines.append(f"\nTotal related party transactions: ${float(total):,.2f}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Engagement Completion Checklist (CAS Integration)
# ---------------------------------------------------------------------------

def get_engagement_checklist(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Return a completion checklist for the engagement based on its type.

    Each item is a dict with keys: item, status ('complete'/'incomplete'), required.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement, get_working_papers, get_engagement_progress
    eng = get_engagement(conn, engagement_id)
    if not eng:
        return []

    eng_type = eng.get("engagement_type", "audit")
    client_code = eng["client_code"]
    period = eng.get("period", "")

    checklist: list[dict[str, Any]] = []

    # Materiality calculated
    mat = get_materiality(conn, engagement_id)
    checklist.append({
        "item": "materiality_calculated",
        "status": "complete" if mat else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    # Risk matrix completed
    risks = get_risk_assessment(conn, engagement_id)
    checklist.append({
        "item": "risk_matrix_completed",
        "status": "complete" if risks else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    # Control tests documented (audit only)
    controls = get_control_tests(engagement_id, conn)
    checklist.append({
        "item": "control_tests_documented",
        "status": "complete" if controls else "incomplete",
        "required": eng_type == "audit",
    })

    # Related parties identified
    parties = get_related_parties(client_code, conn)
    # Related parties is considered complete even if empty (just means none identified)
    # but we check if the engagement has at least looked at it
    rpt_summary = get_related_party_summary(engagement_id, conn)
    checklist.append({
        "item": "related_parties_identified",
        "status": "complete" if parties or rpt_summary.get("transactions", 0) >= 0 else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    # Management rep letter signed (audit/review only)
    rep_letter = get_rep_letter(engagement_id, conn)
    checklist.append({
        "item": "rep_letter_signed",
        "status": "complete" if rep_letter and rep_letter.get("status") == "signed" else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    # Working papers signed off
    progress = get_engagement_progress(conn, engagement_id)
    all_signed = progress.get("total", 0) > 0 and progress.get("signed_off", 0) == progress.get("total", 0)
    checklist.append({
        "item": "working_papers_signed_off",
        "status": "complete" if all_signed else "incomplete",
        "required": True,
    })

    # BLOCK 2: Going concern assessed (CAS 570)
    gc_result = detect_going_concern_indicators(client_code, conn)
    gc_ok = not gc_result.get("assessment_required", False)
    # If indicators found, check that a going_concern_assessment row exists
    if not gc_ok:
        try:
            gc_row = conn.execute(
                "SELECT 1 FROM going_concern_assessments WHERE engagement_id = ?",
                (engagement_id,),
            ).fetchone()
            gc_ok = gc_row is not None
        except Exception:
            gc_ok = False
    checklist.append({
        "item": "going_concern_assessed",
        "status": "complete" if gc_ok else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    # BLOCK 4: Subsequent events reviewed (CAS 560)
    se_events = check_subsequent_events(engagement_id, conn)
    se_significant = [e for e in se_events if abs(e.get("amount", 0)) >= 5000]
    # If significant events exist, check that they are documented
    se_ok = len(se_significant) == 0
    if not se_ok:
        try:
            documented = conn.execute(
                "SELECT COUNT(*) AS cnt FROM subsequent_event_docs WHERE engagement_id = ?",
                (engagement_id,),
            ).fetchone()
            se_ok = documented is not None and documented["cnt"] >= len(se_significant)
        except Exception:
            se_ok = False  # table doesn't exist yet → not documented
    checklist.append({
        "item": "subsequent_events_clear",
        "status": "complete" if se_ok else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    # BLOCK 5: Assertion coverage for material items (CAS 500)
    assertion_ok = True
    if progress.get("total", 0) > 0:
        papers = get_working_papers(conn, client_code, eng.get("period", ""), eng_type)
        mat = get_materiality(conn, engagement_id)
        perf_mat = float(mat["performance_materiality"]) if mat else None
        for p in papers:
            bal = abs(float(p.get("balance_per_books") or 0))
            if perf_mat is not None and bal >= perf_mat:
                # Material item — check assertion coverage
                paper_id = p.get("paper_id") or str(p.get("id", ""))
                cov = get_assertion_coverage(conn, paper_id)
                if cov.get("items_with_sufficient_coverage", 0) == 0 and cov.get("total_items", 0) > 0:
                    assertion_ok = False
                    break
                if cov.get("total_items", 0) == 0:
                    assertion_ok = False
                    break
    checklist.append({
        "item": "assertion_coverage",
        "status": "complete" if assertion_ok else "incomplete",
        "required": eng_type in ("audit", "review"),
    })

    return checklist


def check_engagement_issuable(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> tuple[bool, list[str]]:
    """Check if an engagement can be issued. Returns (can_issue, list_of_blocking_items)."""
    checklist = get_engagement_checklist(engagement_id, conn)
    blocking: list[str] = []
    for item in checklist:
        if item["required"] and item["status"] != "complete":
            blocking.append(item["item"])
    return (len(blocking) == 0, blocking)


# ---------------------------------------------------------------------------
# FIX 12: CAS 570 — Going Concern Auto-Detection
# ---------------------------------------------------------------------------

def detect_going_concern_indicators(
    client_code: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Auto-detect going concern indicators per CAS 570.

    Checks financial ratios from the trial balance:
    - current_ratio < 1.0
    - Three consecutive periods with net loss
    - accounts_payable_days > 90

    Returns dict with indicators found and whether an assessment is recommended.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import ensure_audit_tables
    ensure_audit_tables(conn)

    indicators: list[dict[str, Any]] = []

    # Get latest trial balance for this client
    periods = [dict(r) for r in conn.execute(
        """SELECT DISTINCT period FROM trial_balance
           WHERE LOWER(client_code) = LOWER(?)
           ORDER BY period DESC LIMIT 3""",
        (client_code,),
    ).fetchall()]

    if not periods:
        return {"indicators": [], "assessment_required": False, "indicator_count": 0}

    latest_period = periods[0]["period"]

    # Load trial balance for latest period
    tb_rows = [dict(r) for r in conn.execute(
        """SELECT account_code, account_name, (debit_total - credit_total) AS net_balance
           FROM trial_balance
           WHERE LOWER(client_code) = LOWER(?) AND period = ?""",
        (client_code, latest_period),
    ).fetchall()]

    tb: dict[str, float] = {}
    for r in tb_rows:
        code = r["account_code"]
        bal = float(r["net_balance"])
        tb[str(code)] = bal

    # Check 1: Current ratio < 1.0
    # Current assets: 1000-1999, Current liabilities: 2000-2499
    current_assets = sum(v for k, v in tb.items() if k[:1] == "1" and int(k[:4]) < 2000)
    current_liabilities = abs(sum(v for k, v in tb.items() if k[:1] == "2" and int(k[:4]) < 2500))

    if current_liabilities > 0:
        current_ratio = current_assets / current_liabilities
        if current_ratio < 1.0:
            indicators.append({
                "indicator": "current_ratio_below_1",
                "description": f"Current ratio {current_ratio:.2f} < 1.0 — liquidity concern",
                "description_fr": f"Ratio de liquidité {current_ratio:.2f} < 1.0 — préoccupation de liquidité",
                "value": round(current_ratio, 4),
            })

    # Check 2: Net loss in latest period(s)
    # Revenue: 4000-4999, Expenses: 5000-9999
    loss_count = 0
    for p_row in periods:
        p = p_row["period"]
        p_rows = [dict(r) for r in conn.execute(
            """SELECT account_code, (debit_total - credit_total) AS net_balance FROM trial_balance
               WHERE LOWER(client_code) = LOWER(?) AND period = ?""",
            (client_code, p),
        ).fetchall()]
        revenue = sum(abs(float(r["net_balance"]))
                      for r in p_rows
                      if str(r["account_code"]).startswith("4"))
        expenses = sum(abs(float(r["net_balance"]))
                       for r in p_rows
                       if str(r["account_code"]).startswith(("5", "6", "7", "8", "9")))
        if expenses > revenue:
            loss_count += 1

    if loss_count >= 3:
        indicators.append({
            "indicator": "consecutive_net_losses",
            "description": f"{loss_count} consecutive periods with net loss",
            "description_fr": f"{loss_count} périodes consécutives avec perte nette",
            "value": loss_count,
        })

    # Check 3: AP days > 90 (approximate from AP balance vs expenses)
    ap_balance = abs(sum(v for k, v in tb.items()
                         if k.startswith("20") or k.startswith("21")))
    total_expenses = sum(abs(v) for k, v in tb.items()
                          if k[:1] in ("5", "6", "7", "8", "9"))
    if total_expenses > 0:
        ap_days = (ap_balance / total_expenses) * 365
        if ap_days > 90:
            indicators.append({
                "indicator": "ap_days_over_90",
                "description": f"Accounts payable days {ap_days:.0f} > 90 — slow payment concern",
                "description_fr": f"Délai de paiement {ap_days:.0f} jours > 90 — préoccupation de paiement",
                "value": round(ap_days, 0),
            })

    assessment_required = len(indicators) >= 2

    return {
        "indicators": indicators,
        "assessment_required": assessment_required,
        "indicator_count": len(indicators),
        "client_code": client_code,
        "period": latest_period,
    }


# ---------------------------------------------------------------------------
# FIX 13: CAS 320 — Materiality connected to working papers
# ---------------------------------------------------------------------------

def check_materiality_for_working_paper(
    conn: sqlite3.Connection,
    engagement_id: str,
    account_balance: float,
) -> dict[str, Any]:
    """Check if an account balance exceeds performance materiality.

    Returns dict with material_item flag and performance_materiality threshold.
    Used when creating working paper items to flag items requiring documented testing.
    """
    ensure_cas_tables(conn)
    mat = get_materiality(conn, engagement_id)
    if not mat:
        return {
            "material_item": False,
            "performance_materiality": None,
            "reason": "no_materiality_assessment",
        }

    perf_mat = float(mat["performance_materiality"])
    is_material = abs(account_balance) >= perf_mat

    return {
        "material_item": is_material,
        "performance_materiality": perf_mat,
        "account_balance": account_balance,
        "reason": (
            f"Balance ${abs(account_balance):,.2f} exceeds performance materiality "
            f"${perf_mat:,.2f} — documented testing required"
            if is_material
            else f"Balance ${abs(account_balance):,.2f} below performance materiality ${perf_mat:,.2f}"
        ),
    }


# ---------------------------------------------------------------------------
# FIX 14: CAS 560 — Subsequent Events Auto-Check
# ---------------------------------------------------------------------------

def check_subsequent_events(
    engagement_id: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Query documents with dates after the engagement's period_end_date.

    Flag any significant transactions as potential_subsequent_event
    requiring documentation per CAS 560.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import get_engagement, ensure_audit_tables as _eat
    _eat(conn)

    eng = get_engagement(conn, engagement_id)
    if not eng:
        return []

    period = eng.get("period", "")
    client_code = eng.get("client_code", "")
    if not period or not client_code:
        return []

    # period is typically "2025-12-31" or "YYYY-MM-DD"
    period_end = period.strip()

    # Check if documents table exists
    try:
        rows = conn.execute(
            """SELECT document_id, vendor, amount, document_date, doc_type
               FROM documents
               WHERE LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
                 AND document_date > ?
               ORDER BY document_date
               LIMIT 100""",
            (client_code, period_end),
        ).fetchall()
    except Exception:
        return []

    events: list[dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        amount = float(row.get("amount") or 0)
        # Flag significant transactions (>$5,000 or unusual types)
        if abs(amount) >= 5000:
            events.append({
                "document_id": row.get("document_id"),
                "vendor": row.get("vendor"),
                "amount": amount,
                "document_date": row.get("document_date"),
                "doc_type": row.get("doc_type"),
                "status": "potential_subsequent_event",
                "reason": (
                    f"Transaction of ${abs(amount):,.2f} on {row.get('document_date')} "
                    f"is after period end {period_end} — requires CAS 560 evaluation"
                ),
            })

    return events


# ---------------------------------------------------------------------------
# FIX 15: CAS 500 — Assertion Coverage for Working Paper Items
# ---------------------------------------------------------------------------

def add_assertion_coverage(
    conn: sqlite3.Connection,
    item_id: str,
    assertions: list[str],
) -> dict[str, Any]:
    """Record which assertions were tested for a working paper item.

    Per CAS 500, at least completeness and existence should be tested
    for each significant account.

    Stores assertions as JSON in the assertion_coverage column.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import ensure_audit_tables
    ensure_audit_tables(conn)

    # Validate assertions
    valid = [a for a in assertions if a in VALID_ASSERTIONS]

    # Ensure column exists
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(working_paper_items)").fetchall()}
        if "assertion_coverage" not in cols:
            conn.execute("ALTER TABLE working_paper_items ADD COLUMN assertion_coverage TEXT")
            conn.commit()
    except Exception:
        pass

    conn.execute(
        "UPDATE working_paper_items SET assertion_coverage = ? WHERE item_id = ?",
        (json.dumps(valid), item_id),
    )
    conn.commit()

    return {
        "item_id": item_id,
        "assertions_tested": valid,
        "has_completeness": "completeness" in valid,
        "has_existence": "existence" in valid,
        "sufficient_coverage": "completeness" in valid and "existence" in valid,
    }


def get_assertion_coverage(
    conn: sqlite3.Connection,
    paper_id: str,
) -> dict[str, Any]:
    """Get assertion coverage summary for all items in a working paper.

    Returns summary with per-item coverage and gaps.
    """
    ensure_cas_tables(conn)
    from src.engines.audit_engine import ensure_audit_tables
    ensure_audit_tables(conn)

    # Ensure column exists
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(working_paper_items)").fetchall()}
        if "assertion_coverage" not in cols:
            conn.execute("ALTER TABLE working_paper_items ADD COLUMN assertion_coverage TEXT")
            conn.commit()
    except Exception:
        pass

    rows = conn.execute(
        """SELECT item_id, document_id, assertion_coverage
           FROM working_paper_items WHERE paper_id = ?""",
        (paper_id,),
    ).fetchall()

    items: list[dict[str, Any]] = []
    gaps: list[str] = []
    for r in rows:
        row = dict(r)
        coverage_raw = row.get("assertion_coverage")
        try:
            coverage = json.loads(coverage_raw) if coverage_raw else []
        except Exception:
            coverage = []

        has_comp = "completeness" in coverage
        has_exist = "existence" in coverage
        sufficient = has_comp and has_exist

        items.append({
            "item_id": row["item_id"],
            "document_id": row.get("document_id"),
            "assertions_tested": coverage,
            "sufficient_coverage": sufficient,
        })
        if not sufficient:
            missing = []
            if not has_comp:
                missing.append("completeness")
            if not has_exist:
                missing.append("existence")
            gaps.append(f"Item {row['item_id']}: missing {', '.join(missing)}")

    return {
        "paper_id": paper_id,
        "items": items,
        "total_items": len(items),
        "items_with_sufficient_coverage": sum(1 for i in items if i["sufficient_coverage"]),
        "gaps": gaps,
    }


# ---------------------------------------------------------------------------
# CAS 265 — Control Deficiency Severity + Management Letter (Sprint F Fix 6)
# ---------------------------------------------------------------------------

# CAS 265.6 definitions:
#   * Deficiency — a control is missing or not operating effectively.
#   * Significant deficiency — deficiency that merits attention of those
#     charged with governance.
#   * Material weakness — a deficiency, or combination of deficiencies, that
#     could result in a material misstatement.

CONTROL_SEVERITY_OBSERVATION = "observation"
CONTROL_SEVERITY_SIGNIFICANT = "significant"
CONTROL_SEVERITY_MATERIAL = "material_weakness"

VALID_SEVERITIES = {
    CONTROL_SEVERITY_OBSERVATION,
    CONTROL_SEVERITY_SIGNIFICANT,
    CONTROL_SEVERITY_MATERIAL,
}


def classify_control_deficiency(severity_factors: dict[str, Any]) -> str:
    """Classify a control deficiency per CAS 265.6.

    severity_factors keys:
        likelihood_of_misstatement       low / moderate / high
        magnitude_potential              below_materiality / at_materiality / above_materiality
        compensating_controls_exist      bool
        management_override_possible     bool
        multiple_deficiencies_aggregate  bool (optional)
    """
    likelihood = (severity_factors.get("likelihood_of_misstatement") or "").lower()
    magnitude = (severity_factors.get("magnitude_potential") or "").lower()
    override_possible = bool(severity_factors.get("management_override_possible"))
    compensating = bool(severity_factors.get("compensating_controls_exist"))
    aggregate_material = bool(severity_factors.get("multiple_deficiencies_aggregate"))

    # Material weakness if the deficiency could allow a material misstatement
    # that compensating controls would not detect.
    if (magnitude == "above_materiality" and not compensating) or override_possible:
        return CONTROL_SEVERITY_MATERIAL
    if aggregate_material:
        return CONTROL_SEVERITY_MATERIAL

    # Significant if reasonably likely to result in more-than-trivial
    # misstatement OR if likelihood is moderate/high at materiality.
    if magnitude == "above_materiality":
        # Above materiality but compensating controls soften to significant.
        return CONTROL_SEVERITY_SIGNIFICANT
    if likelihood in ("moderate", "high") and magnitude == "at_materiality":
        return CONTROL_SEVERITY_SIGNIFICANT
    if likelihood == "high" and magnitude == "below_materiality" and not compensating:
        return CONTROL_SEVERITY_SIGNIFICANT

    return CONTROL_SEVERITY_OBSERVATION


CONTROL_DEFICIENCIES_DDL = """
CREATE TABLE IF NOT EXISTS control_deficiencies (
    deficiency_id TEXT PRIMARY KEY,
    engagement_id TEXT NOT NULL,
    control_test_id TEXT,
    title TEXT NOT NULL,
    description TEXT,
    likelihood_of_misstatement TEXT,
    magnitude_potential TEXT,
    compensating_controls_exist INTEGER DEFAULT 0,
    management_override_possible INTEGER DEFAULT 0,
    multiple_deficiencies_aggregate INTEGER DEFAULT 0,
    severity TEXT NOT NULL,
    recommendation TEXT,
    management_response TEXT,
    response_due_date TEXT,
    communicated_at TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_control_deficiencies_engagement
    ON control_deficiencies(engagement_id, severity);
"""


def ensure_control_deficiency_table(conn: sqlite3.Connection) -> None:
    for stmt in CONTROL_DEFICIENCIES_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


def record_control_deficiency(
    conn: sqlite3.Connection,
    *,
    engagement_id: str,
    title: str,
    description: str,
    likelihood_of_misstatement: str,
    magnitude_potential: str,
    compensating_controls_exist: bool = False,
    management_override_possible: bool = False,
    multiple_deficiencies_aggregate: bool = False,
    control_test_id: str = "",
    recommendation: str = "",
    created_by: str = "",
) -> tuple[str, str]:
    """Insert a deficiency row and auto-classify its severity. Returns
    (deficiency_id, severity).
    """
    ensure_control_deficiency_table(conn)
    factors = {
        "likelihood_of_misstatement": likelihood_of_misstatement,
        "magnitude_potential": magnitude_potential,
        "compensating_controls_exist": compensating_controls_exist,
        "management_override_possible": management_override_possible,
        "multiple_deficiencies_aggregate": multiple_deficiencies_aggregate,
    }
    severity = classify_control_deficiency(factors)
    deficiency_id = f"def_{secrets.token_hex(8)}"
    conn.execute(
        """INSERT INTO control_deficiencies
           (deficiency_id, engagement_id, control_test_id, title, description,
            likelihood_of_misstatement, magnitude_potential,
            compensating_controls_exist, management_override_possible,
            multiple_deficiencies_aggregate, severity, recommendation, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            deficiency_id, engagement_id, control_test_id, title, description,
            likelihood_of_misstatement, magnitude_potential,
            1 if compensating_controls_exist else 0,
            1 if management_override_possible else 0,
            1 if multiple_deficiencies_aggregate else 0,
            severity, recommendation, created_by,
        ),
    )
    conn.commit()
    return deficiency_id, severity


def get_control_deficiencies(
    conn: sqlite3.Connection,
    engagement_id: str,
) -> list[dict]:
    ensure_control_deficiency_table(conn)
    rows = conn.execute(
        "SELECT * FROM control_deficiencies WHERE engagement_id=? ORDER BY severity DESC, created_at",
        (engagement_id,),
    ).fetchall()
    return [dict(r) for r in rows] if rows else []


def update_management_response(
    conn: sqlite3.Connection,
    deficiency_id: str,
    response: str,
    response_due_date: str = "",
) -> bool:
    ensure_control_deficiency_table(conn)
    cur = conn.execute(
        "UPDATE control_deficiencies SET management_response=?, response_due_date=? WHERE deficiency_id=?",
        (response, response_due_date, deficiency_id),
    )
    conn.commit()
    return cur.rowcount > 0


def generate_management_letter_pdf(
    engagement_id: str,
    conn: sqlite3.Connection,
    *,
    firm_name: str = "OtoCPA CPA",
    firm_address: str = "",
    language: str = "en",
    output_dir: str | Path | None = None,
    persist: bool = True,
) -> tuple[bytes, str, int]:
    """Generate a CAS 265 management letter PDF summarising significant and
    material deficiencies. Returns (pdf_bytes, file_path, count_reported).

    CAS 265.9 — observation-only deficiencies are NOT required in the letter;
    we exclude them here so the letter only communicates matters that merit
    governance attention.
    """
    ensure_control_deficiency_table(conn)
    deficiencies = get_control_deficiencies(conn, engagement_id)
    reportable = [
        d for d in deficiencies
        if d["severity"] in (CONTROL_SEVERITY_SIGNIFICANT, CONTROL_SEVERITY_MATERIAL)
    ]
    from src.engines.audit_engine import get_engagement
    eng = get_engagement(conn, engagement_id)
    if not eng:
        raise ValueError(f"Engagement not found: {engagement_id}")
    client_code = eng.get("client_code") or ""
    period = eng.get("period") or ""

    try:
        pdf_bytes = _render_management_letter_reportlab(
            firm_name=firm_name,
            firm_address=firm_address,
            client_code=client_code,
            period=period,
            reportable=reportable,
            language=language,
        )
    except ImportError:
        pdf_bytes = _render_management_letter_minimal(
            firm_name=firm_name,
            client_code=client_code,
            period=period,
            reportable=reportable,
            language=language,
        )

    file_path = ""
    if persist:
        base_dir = Path(output_dir) if output_dir else ROOT_DIR / "data" / "management_letters"
        base_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = base_dir / f"mgmt_letter_{engagement_id}_{stamp}.pdf"
        path.write_bytes(pdf_bytes)
        file_path = str(path)
        # Mark the reported deficiencies as communicated.
        for d in reportable:
            conn.execute(
                "UPDATE control_deficiencies SET communicated_at=? WHERE deficiency_id=?",
                (datetime.now(timezone.utc).isoformat(), d["deficiency_id"]),
            )
        conn.commit()
    return pdf_bytes, file_path, len(reportable)


def _render_management_letter_reportlab(
    *, firm_name: str, firm_address: str, client_code: str, period: str,
    reportable: list[dict], language: str,
) -> bytes:
    from io import BytesIO
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.75 * inch, bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    h = ParagraphStyle("h", parent=styles["Heading2"], fontSize=13, spaceAfter=10)
    normal = styles["BodyText"]
    story: list = []
    story.append(Paragraph(f"<b>{firm_name}</b>", styles["Heading1"]))
    if firm_address:
        story.append(Paragraph(firm_address, normal))
    story.append(Spacer(1, 0.2 * inch))

    if language == "fr":
        title = f"Lettre à la direction — {client_code} — Période {period}"
        intro = (
            f"Dans le cadre de notre mission d'audit des états financiers de {client_code} "
            f"pour la période {period}, nous avons relevé les déficiences importantes "
            f"suivantes dans le contrôle interne, conformément à la norme CAS 265."
        )
        if not reportable:
            intro = (
                f"Dans le cadre de notre mission d'audit des états financiers de {client_code} "
                f"pour la période {period}, aucune déficience importante dans le contrôle interne "
                f"n'a été relevée exigeant une communication formelle selon la norme CAS 265."
            )
    else:
        title = f"Management Letter — {client_code} — Period {period}"
        intro = (
            f"In connection with our audit of the financial statements of {client_code} "
            f"for the period {period}, we identified the following significant and material "
            f"control deficiencies requiring communication to those charged with governance, "
            f"in accordance with CAS 265."
        )
        if not reportable:
            intro = (
                f"In connection with our audit of the financial statements of {client_code} "
                f"for the period {period}, no significant deficiencies or material weaknesses "
                f"in internal control were identified that require formal communication "
                f"under CAS 265."
            )

    story.append(Paragraph(title, h))
    story.append(Paragraph(intro, normal))
    story.append(Spacer(1, 0.2 * inch))

    for i, d in enumerate(reportable, start=1):
        sev_label = (
            "Material Weakness" if d["severity"] == CONTROL_SEVERITY_MATERIAL
            else "Significant Deficiency"
        )
        story.append(Paragraph(
            f"<b>{i}. {d.get('title', '(untitled)')} — {sev_label}</b>",
            h,
        ))
        if d.get("description"):
            story.append(Paragraph(f"<b>Finding:</b> {d['description']}", normal))
        if d.get("recommendation"):
            story.append(Paragraph(f"<b>Recommendation:</b> {d['recommendation']}", normal))
        if d.get("management_response"):
            story.append(Paragraph(
                f"<b>Management response:</b> {d['management_response']}",
                normal,
            ))
        story.append(Spacer(1, 0.15 * inch))

    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("_________________________________________", normal))
    story.append(Paragraph(f"{firm_name}", normal))
    doc.build(story)
    return buf.getvalue()


def _render_management_letter_minimal(
    *, firm_name: str, client_code: str, period: str,
    reportable: list[dict], language: str,
) -> bytes:
    lines = [firm_name, "", f"Management Letter - {client_code} - {period}", ""]
    if not reportable:
        lines.append("No significant deficiencies or material weaknesses identified.")
    else:
        for i, d in enumerate(reportable, start=1):
            sev = (
                "Material Weakness" if d["severity"] == CONTROL_SEVERITY_MATERIAL
                else "Significant Deficiency"
            )
            lines.append(f"{i}. {d.get('title', '(untitled)')} [{sev}]")
            if d.get("description"):
                lines.append(f"   Finding: {d['description']}")
            if d.get("recommendation"):
                lines.append(f"   Recommendation: {d['recommendation']}")
            if d.get("management_response"):
                lines.append(f"   Management response: {d['management_response']}")
            lines.append("")

    # Reuse the rep-letter minimal renderer structure.
    page_lines = [lines[i:i + 55] for i in range(0, len(lines), 55)] or [lines]
    objects: list[bytes] = []

    def _add(obj: bytes) -> int:
        objects.append(obj)
        return len(objects)

    page_ids = list(range(3, 3 + len(page_lines)))
    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    _add(b"<< /Type /Catalog /Pages 2 0 R >>")
    _add(f"<< /Type /Pages /Kids [{kids}] /Count {len(page_lines)} >>".encode())
    font_obj_id = 3 + 2 * len(page_lines)
    for idx in range(len(page_lines)):
        page_obj = (
            f"<< /Type /Page /Parent 2 0 R "
            f"/Resources << /Font << /F1 {font_obj_id} 0 R >> >> "
            f"/MediaBox [0 0 612 792] "
            f"/Contents {3 + len(page_lines) + idx} 0 R >>"
        ).encode()
        _add(page_obj)
    for chunk in page_lines:
        stream_lines = ["BT", "/F1 10 Tf", "50 752 Td"]
        for ln in chunk:
            safe = ln.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            stream_lines.append(f"({safe}) Tj")
            stream_lines.append("0 -13 Td")
        stream_lines.append("ET")
        stream = "\n".join(stream_lines).encode("latin-1", errors="replace")
        _add(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
    _add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode()
    out += (
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    ).encode()
    return bytes(out)
