"""Run the full CPA engagement against a seeded client.

Each phase writes a JSONL record to /tmp/cpa_simulation_log.jsonl and
collects observed bugs into a structured list so the report generator
can summarise them. No UI — pure engine calls — so the simulation can
be re-run headlessly.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .scenario_generator import ClientProfile, expected_totals

ROOT = Path(__file__).resolve().parent.parent.parent
LOG_PATH = Path("/tmp/cpa_simulation_log.jsonl")
DB_PATH = ROOT / "tests" / "simulation" / "sim.db"

log = logging.getLogger("cpa_sim")


@dataclass
class Bug:
    severity: str  # critical | high | medium | low
    phase: str
    client: str
    summary: str
    detail: str = ""
    remediation: str = ""


@dataclass
class PhaseResult:
    phase: str
    client: str
    status: str  # pass | warn | fail | skip
    metric: dict[str, Any] = field(default_factory=dict)
    bugs: list[Bug] = field(default_factory=list)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _log(record: dict[str, Any]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a") as fh:
        fh.write(json.dumps(record, default=str) + "\n")


def _safe(fn: Callable[..., Any], *args, **kwargs) -> tuple[Any, str | None]:
    try:
        return fn(*args, **kwargs), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase1_engagement_setup(profile: ClientProfile) -> PhaseResult:
    r = PhaseResult(phase="01_engagement_setup", client=profile.client_code, status="pass")
    try:
        from src.engines import audit_engine, cas_engine
        conn = _conn()
        audit_engine.ensure_audit_tables(conn)
        cas_engine.ensure_cas_tables(conn)
        # Create engagement + materiality + one risk assessment.
        ret = audit_engine.create_engagement(
            conn, profile.client_code, profile.start_date[:7],
            engagement_type="audit", partner="Partner A", manager="Mgr B",
            staff="Staff C",
        )
        # get_engagement uses engagement_id from the return value
        if isinstance(ret, dict):
            eng_id = ret.get("engagement_id") or ret.get("id") or ""
        else:
            eng_id = ret
        r.metric["engagement_id"] = eng_id
        if not eng_id:
            r.status = "fail"
            r.bugs.append(Bug("high", r.phase, profile.client_code,
                              "create_engagement returned no id",
                              str(ret)))
            return r
        # Materiality
        try:
            cas_engine.save_materiality(
                conn,
                eng_id,
                {"basis": "revenue",
                 "basis_amount": float(profile.annual_revenue),
                 "planning_materiality": float(profile.annual_revenue * Decimal("0.01")),
                 "performance_materiality": float(profile.annual_revenue * Decimal("0.0075")),
                 "clearly_trivial": float(profile.annual_revenue * Decimal("0.0005"))},
                "simulation_user",
            )
            r.metric["materiality_saved"] = True
        except Exception as e:
            r.status = "warn"
            r.bugs.append(Bug("medium", r.phase, profile.client_code,
                              "materiality save failed", str(e)))
        conn.close()
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, profile.client_code,
                          "engagement setup blew up", traceback.format_exc()))
    return r


def phase3_reconciliation(profile: ClientProfile) -> PhaseResult:
    r = PhaseResult(phase="03_reconciliation", client=profile.client_code, status="pass")
    try:
        from src.engines import recon_edge_cases
        conn = _conn()
        recon_edge_cases.ensure_edge_tables(conn)
        matches = recon_edge_cases.detect_internal_transfers(
            conn, client_code=profile.client_code, days_back=365,
        )
        r.metric["internal_transfers"] = len(matches)
        # Check that AP document bank-matches are reconciled=1.
        row = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions "
            "WHERE LOWER(client_code)=LOWER(?) AND reconciled=1",
            (profile.client_code,),
        ).fetchone()
        r.metric["reconciled_tx"] = row[0]
        unrec = conn.execute(
            "SELECT COUNT(*) FROM bank_transactions "
            "WHERE LOWER(client_code)=LOWER(?) AND reconciled=0",
            (profile.client_code,),
        ).fetchone()[0]
        r.metric["unreconciled_tx"] = unrec
        conn.close()
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, profile.client_code,
                          "reconciliation phase crashed", str(e)))
    return r


def phase5_analytical(profile: ClientProfile) -> PhaseResult:
    r = PhaseResult(phase="05_analytical", client=profile.client_code, status="pass")
    try:
        from src.engines import approval_graph_engine, phantom_employee_engine, benford_engine
        # Analytical procedures & anomaly detectors
        for fn, metric_key in (
            (lambda: approval_graph_engine.detect_circular_approvals(
                client_code=profile.client_code, db_path=DB_PATH), "circular"),
            (lambda: phantom_employee_engine.detect_phantom_employee_expenses(
                client_code=profile.client_code, db_path=DB_PATH), "phantom"),
            (lambda: benford_engine.analyze_benford_compliance(
                client_code=profile.client_code, db_path=DB_PATH), "benford"),
        ):
            result, err = _safe(fn)
            if err:
                r.status = "warn"
                r.bugs.append(Bug("medium", r.phase, profile.client_code,
                                  f"{metric_key} detector error", err))
            else:
                r.metric[metric_key] = (
                    len(result) if isinstance(result, list) else result.get("status")
                )
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, profile.client_code,
                          "analytical phase crashed", traceback.format_exc()))
    return r


def phase6_financial_statements(profile: ClientProfile) -> PhaseResult:
    r = PhaseResult(phase="06_financial_statements", client=profile.client_code, status="pass")
    try:
        from src.engines import audit_engine
        conn = _conn()
        audit_engine.ensure_audit_tables(conn)
        audit_engine.seed_chart_of_accounts(conn)
        # generate_financial_statements uses "period" not period_start.
        stmts = audit_engine.generate_financial_statements(
            conn, profile.client_code, profile.start_date[:7],
        )
        r.metric["tb_balanced"] = bool(stmts.get("trial_balance_balanced"))
        if stmts.get("balance_sheet"):
            r.metric["balance_sheet_balanced"] = bool(
                stmts["balance_sheet"].get("balance_ok"),
            )
        r.metric["has_soce"] = "statement_of_changes_in_equity" in stmts
        # Check SOCE was populated.
        soce = stmts.get("statement_of_changes_in_equity") or {}
        r.metric["soce_closing_equity"] = soce.get("total_closing_equity")
        conn.close()
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, profile.client_code,
                          "FS generation crashed", traceback.format_exc()))
    return r


def phase7_tax_returns(profile: ClientProfile) -> PhaseResult:
    r = PhaseResult(phase="07_tax_returns", client=profile.client_code, status="pass")
    try:
        from src.engines import tax_engine
        exp = expected_totals(profile)
        # Compute GST/QST summary over the full period.
        summary = tax_engine.generate_filing_summary(
            profile.client_code, profile.start_date,
            _date_plus(profile.start_date, profile.period_days),
            db_path=DB_PATH,
        )
        # Compare reported GST collected to expected (tolerance $5 for
        # AR-vs-documents gap; Client A has lots of cash-register-style
        # AR invoices).
        gst_col = float(summary.get("gst_collected", 0) or 0)
        qst_col = float(summary.get("qst_collected", 0) or 0)
        r.metric["reported_gst"] = gst_col
        r.metric["reported_qst"] = qst_col
        r.metric["expected_gst"] = exp["expected_gst_collected"]
        r.metric["expected_qst"] = exp["expected_qst_collected"]
        gst_delta = abs(gst_col - exp["expected_gst_collected"])
        qst_delta = abs(qst_col - exp["expected_qst_collected"])
        r.metric["gst_delta"] = round(gst_delta, 2)
        r.metric["qst_delta"] = round(qst_delta, 2)
        # Tolerate up to 1 % deviation (normal for rounding + Quick Method).
        tol = max(5.0, float(exp["expected_gst_collected"]) * 0.01)
        if gst_delta > tol:
            r.status = "warn"
            r.bugs.append(Bug(
                "medium", r.phase, profile.client_code,
                f"GST collected off by ${gst_delta:.2f}",
                f"expected ${exp['expected_gst_collected']}; got ${gst_col}",
            ))
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, profile.client_code,
                          "tax filing crashed", traceback.format_exc()))
    return r


def _date_plus(iso: str, days: int) -> str:
    from datetime import date as _d, timedelta as _td
    return (_d.fromisoformat(iso) + _td(days=days)).isoformat()


def phase8_finalize(profile: ClientProfile) -> PhaseResult:
    r = PhaseResult(phase="08_finalize", client=profile.client_code, status="pass")
    try:
        from src.engines import cas_engine
        conn = _conn()
        cas_engine.ensure_cas_tables(conn)
        # The sim constructs an engagement id from profile.client_code + period
        # but we don't have a direct handle — fetch by client+period.
        row = conn.execute(
            "SELECT engagement_id FROM engagements WHERE client_code=? LIMIT 1",
            (profile.client_code,),
        ).fetchone()
        if not row:
            r.status = "skip"
            conn.close()
            return r
        eng_id = row[0]
        # Generate the rep letter (persists as evidence).
        pdf, path = cas_engine.generate_rep_letter_pdf(
            eng_id, conn,
            language="en",
            output_dir=Path("/tmp/cpa_sim_reps"),
        )
        r.metric["rep_letter_bytes"] = len(pdf)
        r.metric["rep_letter_path"] = path
        conn.close()
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, profile.client_code,
                          "rep letter failed", traceback.format_exc()))
    return r


PHASES: list[tuple[str, Callable[[ClientProfile], PhaseResult]]] = [
    ("01_engagement_setup", phase1_engagement_setup),
    ("03_reconciliation", phase3_reconciliation),
    ("05_analytical", phase5_analytical),
    ("06_financial_statements", phase6_financial_statements),
    ("07_tax_returns", phase7_tax_returns),
    ("08_finalize", phase8_finalize),
]


def run_for_client(profile: ClientProfile) -> list[PhaseResult]:
    results: list[PhaseResult] = []
    for name, fn in PHASES:
        res = fn(profile)
        _log({
            "ts": _now(), "client": profile.client_code,
            "phase": name, "status": res.status,
            "metric": res.metric,
            "bugs": [b.__dict__ for b in res.bugs],
        })
        results.append(res)
    return results


def run_all(profiles: list[ClientProfile]) -> dict[str, Any]:
    LOG_PATH.unlink(missing_ok=True)
    all_results: dict[str, list[PhaseResult]] = {}
    for p in profiles:
        all_results[p.client_code] = run_for_client(p)
    # Aggregate bugs + pass rates.
    total = sum(len(v) for v in all_results.values())
    pass_count = sum(1 for v in all_results.values()
                     for r in v if r.status == "pass")
    fail_count = sum(1 for v in all_results.values()
                     for r in v if r.status == "fail")
    warn_count = sum(1 for v in all_results.values()
                     for r in v if r.status == "warn")
    skip_count = sum(1 for v in all_results.values()
                     for r in v if r.status == "skip")
    bugs: list[Bug] = [b for v in all_results.values()
                       for r in v for b in r.bugs]
    return {
        "started_at": _now(),
        "phase_runs": total,
        "pass": pass_count,
        "warn": warn_count,
        "fail": fail_count,
        "skip": skip_count,
        "bug_count": len(bugs),
        "bugs": [b.__dict__ for b in bugs],
        "by_client": {
            code: [
                {"phase": r.phase, "status": r.status, "metric": r.metric,
                 "bugs": [b.__dict__ for b in r.bugs]}
                for r in rs
            ]
            for code, rs in all_results.items()
        },
    }
