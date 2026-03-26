"""
src/engines/payroll_engine.py — Quebec payroll compliance engine.

Deterministic validation of Quebec-specific payroll rules:
  - HSF (Health Services Fund) rate tiers
  - QPP vs CPP province check
  - QPIP vs EI rate validation
  - RL-1 / T4 reconciliation
  - CNESST premium rate by industry unit

All monetary arithmetic uses Python Decimal.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

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


# ---------------------------------------------------------------------------
# HSF (Health Services Fund / Fonds des services de santé)
# ---------------------------------------------------------------------------
# 2024-2025 rate tiers based on total payroll
HSF_TIERS: list[tuple[Decimal, Decimal, Decimal]] = [
    # (max_payroll, rate_below_threshold, rate_above_threshold)
    (Decimal("1000000"),  Decimal("0.0125"),  Decimal("0.0125")),   # ≤$1M: 1.25%
    (Decimal("2000000"),  Decimal("0.0125"),  Decimal("0.0165")),   # $1M-$2M: progressive
    (Decimal("3000000"),  Decimal("0.0165"),  Decimal("0.0200")),   # $2M-$3M: progressive
    (Decimal("5000000"),  Decimal("0.0200"),  Decimal("0.0250")),   # $3M-$5M: progressive
    (Decimal("7000000"),  Decimal("0.0250"),  Decimal("0.0370")),   # $5M-$7M: progressive
]
HSF_MAX_RATE = Decimal("0.0426")  # >$7M: 4.26%


def _expected_hsf_rate(total_payroll: Decimal) -> Decimal:
    """Return the correct HSF rate for a given total payroll."""
    if total_payroll <= _ZERO:
        return _ZERO
    for max_pay, rate_low, rate_high in HSF_TIERS:
        if total_payroll <= max_pay:
            return rate_low
    return HSF_MAX_RATE


def validate_hsf_rate(total_payroll: float | Decimal, rate_used: float | Decimal) -> dict:
    """
    Validate the Health Services Fund (HSF/FSS) rate against total payroll.

    Returns dict with: valid, expected_rate, rate_used, total_payroll,
    error_type (hsf_rate_error or None), description_en, description_fr.
    """
    payroll = _to_decimal(total_payroll)
    used = _to_decimal(rate_used)
    expected = _expected_hsf_rate(payroll)

    valid = abs(used - expected) < Decimal("0.0005")
    result: dict[str, Any] = {
        "valid": valid,
        "expected_rate": str(expected),
        "rate_used": str(used),
        "total_payroll": str(payroll),
        "error_type": None if valid else "hsf_rate_error",
    }
    if not valid:
        result["description_en"] = (
            f"HSF rate {used} applied to payroll of ${payroll:,} is incorrect. "
            f"Expected rate: {expected} based on total payroll tier."
        )
        result["description_fr"] = (
            f"Le taux du FSS de {used} appliqué à une masse salariale de "
            f"{payroll:,}$ est incorrect. Taux attendu: {expected} selon "
            f"le palier de la masse salariale totale."
        )
    return result


# ---------------------------------------------------------------------------
# QPP vs CPP
# ---------------------------------------------------------------------------
# 2024-2025 QPP rates
QPP_RATE_EMPLOYEE = Decimal("0.064")      # 6.40% (base)
QPP2_RATE_EMPLOYEE = Decimal("0.04")      # 4.00% (QPP2 — second ceiling)
CPP_RATE_EMPLOYEE = Decimal("0.0595")     # 5.95% (base CPP)
CPP2_RATE_EMPLOYEE = Decimal("0.04")      # 4.00% (CPP2)


def validate_qpp_cpp(
    province: str,
    pension_plan_used: str,
) -> dict:
    """
    Validate that a Quebec employee contributes to QPP (not CPP) and vice versa.

    Parameters
    ----------
    province : str          Two-letter province code (QC, ON, etc.)
    pension_plan_used : str  "QPP" or "CPP"

    Returns dict with: valid, expected_plan, plan_used, error_type, descriptions.
    """
    prov = province.strip().upper()
    plan = pension_plan_used.strip().upper()

    expected = "QPP" if prov == "QC" else "CPP"
    valid = plan == expected

    result: dict[str, Any] = {
        "valid": valid,
        "province": prov,
        "expected_plan": expected,
        "plan_used": plan,
        "error_type": None if valid else "qpp_cpp_error",
    }
    if not valid:
        if prov == "QC" and plan == "CPP":
            result["description_en"] = (
                "Quebec employee is deducting CPP instead of QPP. "
                "Quebec employees must contribute to the Quebec Pension Plan (QPP), "
                "not the Canada Pension Plan (CPP)."
            )
            result["description_fr"] = (
                "L'employé du Québec cotise au RPC au lieu du RRQ. "
                "Les employés du Québec doivent cotiser au Régime de rentes "
                "du Québec (RRQ), et non au Régime de pensions du Canada (RPC)."
            )
        else:
            result["description_en"] = (
                f"Employee in {prov} is deducting QPP instead of CPP. "
                f"Only Quebec employees contribute to QPP."
            )
            result["description_fr"] = (
                f"L'employé en {prov} cotise au RRQ au lieu du RPC. "
                f"Seuls les employés du Québec cotisent au RRQ."
            )
    return result


# ---------------------------------------------------------------------------
# QPIP vs EI
# ---------------------------------------------------------------------------
# 2024-2025 rates
EI_RATE_REGULAR = Decimal("0.0166")       # 1.66% — non-Quebec
EI_RATE_QUEBEC = Decimal("0.01320")       # 1.32% — reduced for Quebec (QPIP)
QPIP_RATE_EMPLOYEE = Decimal("0.00494")   # 0.494%
QPIP_RATE_EMPLOYER = Decimal("0.00692")   # 0.692%


def validate_qpip_ei(
    province: str,
    ei_rate_used: float | Decimal,
) -> dict:
    """
    Validate EI rate considering QPIP.

    Quebec employees pay a reduced EI rate (1.32% vs 1.66%) because they
    pay QPIP separately. Flag if a Quebec employee has the full EI rate
    or a non-Quebec employee has the reduced rate.
    """
    prov = province.strip().upper()
    used = _to_decimal(ei_rate_used)

    if prov == "QC":
        expected = EI_RATE_QUEBEC
    else:
        expected = EI_RATE_REGULAR

    valid = abs(used - expected) < Decimal("0.0005")

    result: dict[str, Any] = {
        "valid": valid,
        "province": prov,
        "expected_ei_rate": str(expected),
        "ei_rate_used": str(used),
        "error_type": None if valid else "qpip_ei_error",
    }
    if not valid:
        if prov == "QC" and used > expected:
            result["description_en"] = (
                f"Quebec employee paying full EI rate ({used}) instead of reduced "
                f"rate ({expected}). Quebec employees pay QPIP separately, so "
                f"their EI premium rate is reduced."
            )
            result["description_fr"] = (
                f"Employé du Québec payant le taux complet d'AE ({used}) au lieu "
                f"du taux réduit ({expected}). Les employés du Québec paient le "
                f"RQAP séparément, donc leur taux de cotisation à l'AE est réduit."
            )
        elif prov != "QC" and used < expected:
            result["description_en"] = (
                f"Non-Quebec employee ({prov}) paying reduced Quebec EI rate "
                f"({used}) instead of standard rate ({expected})."
            )
            result["description_fr"] = (
                f"Employé hors Québec ({prov}) payant le taux réduit québécois "
                f"d'AE ({used}) au lieu du taux standard ({expected})."
            )
    return result


# ---------------------------------------------------------------------------
# RL-1 / T4 reconciliation
# ---------------------------------------------------------------------------
# Key box mappings: RL-1 box → T4 box → description
RL1_T4_BOX_MAP: dict[str, dict[str, str]] = {
    "A": {"t4_box": "14", "label_en": "Employment income", "label_fr": "Revenus d'emploi"},
    "B": {"t4_box": "26", "label_en": "QPP pensionable earnings", "label_fr": "Gains admissibles au RRQ"},
    "C": {"t4_box": "16", "label_en": "QPP employee contribution", "label_fr": "Cotisation de l'employé au RRQ"},
    "D": {"t4_box": "22", "label_en": "Income tax deducted", "label_fr": "Impôt sur le revenu retenu"},
    "E": {"t4_box": "24", "label_en": "EI insurable earnings", "label_fr": "Gains assurables d'AE"},
    "F": {"t4_box": "18", "label_en": "EI premium", "label_fr": "Cotisation à l'AE"},
    "G": {"t4_box": "44", "label_en": "Union dues", "label_fr": "Cotisations syndicales"},
    "H": {"t4_box": "55", "label_en": "QPIP premium (employee)", "label_fr": "Cotisation au RQAP (employé)"},
}


def reconcile_rl1_t4(
    rl1_data: dict[str, float | Decimal],
    t4_data: dict[str, float | Decimal],
) -> dict:
    """
    Reconcile RL-1 and T4 box amounts.

    Parameters
    ----------
    rl1_data : dict mapping RL-1 box letters (A, B, C, …) to amounts.
    t4_data  : dict mapping T4 box numbers (14, 16, 18, …) to amounts.

    Returns dict with: valid, mismatches (list), matched_count, total_boxes.
    """
    mismatches: list[dict[str, Any]] = []
    matched = 0

    for rl1_box, mapping in RL1_T4_BOX_MAP.items():
        t4_box = mapping["t4_box"]
        rl1_val = _to_decimal(rl1_data.get(rl1_box, 0))
        t4_val = _to_decimal(t4_data.get(t4_box, 0))

        if rl1_val == _ZERO and t4_val == _ZERO:
            continue  # Both empty, skip

        if abs(rl1_val - t4_val) > Decimal("0.01"):
            mismatches.append({
                "rl1_box": rl1_box,
                "t4_box": t4_box,
                "label_en": mapping["label_en"],
                "label_fr": mapping["label_fr"],
                "rl1_amount": str(rl1_val),
                "t4_amount": str(t4_val),
                "difference": str(_round(rl1_val - t4_val)),
                "error_type": "rl1_t4_mismatch",
            })
        else:
            matched += 1

    return {
        "valid": len(mismatches) == 0,
        "mismatches": mismatches,
        "matched_count": matched,
        "total_boxes": len(RL1_T4_BOX_MAP),
        "error_type": "rl1_t4_mismatch" if mismatches else None,
    }


# ---------------------------------------------------------------------------
# CNESST premium rate by industry unit
# ---------------------------------------------------------------------------
# Sample industry classification units and their 2024-2025 rates
CNESST_INDUSTRY_RATES: dict[str, dict[str, Any]] = {
    "54010": {"description_en": "Office / administrative", "description_fr": "Bureau / administration", "rate": Decimal("0.0054")},
    "54020": {"description_en": "Professional services", "description_fr": "Services professionnels", "rate": Decimal("0.0054")},
    "52010": {"description_en": "Retail trade", "description_fr": "Commerce de détail", "rate": Decimal("0.0175")},
    "32010": {"description_en": "Food manufacturing", "description_fr": "Fabrication alimentaire", "rate": Decimal("0.0282")},
    "23010": {"description_en": "General construction", "description_fr": "Construction générale", "rate": Decimal("0.0585")},
    "23020": {"description_en": "Residential construction", "description_fr": "Construction résidentielle", "rate": Decimal("0.0475")},
    "23030": {"description_en": "Electrical contracting", "description_fr": "Entrepreneur électricien", "rate": Decimal("0.0365")},
    "23040": {"description_en": "Plumbing/HVAC", "description_fr": "Plomberie/CVC", "rate": Decimal("0.0310")},
    "23050": {"description_en": "Roofing", "description_fr": "Couverture/toiture", "rate": Decimal("0.0892")},
    "48010": {"description_en": "Road transport", "description_fr": "Transport routier", "rate": Decimal("0.0425")},
    "62010": {"description_en": "Health care services", "description_fr": "Services de soins de santé", "rate": Decimal("0.0156")},
    "72010": {"description_en": "Restaurants/food services", "description_fr": "Restauration/services alimentaires", "rate": Decimal("0.0198")},
    "56010": {"description_en": "Cleaning / janitorial", "description_fr": "Nettoyage / conciergerie", "rate": Decimal("0.0345")},
    "11010": {"description_en": "Agriculture", "description_fr": "Agriculture", "rate": Decimal("0.0412")},
    "31010": {"description_en": "Metal manufacturing", "description_fr": "Fabrication métallique", "rate": Decimal("0.0356")},
    "21010": {"description_en": "Mining/quarrying", "description_fr": "Exploitation minière/carrière", "rate": Decimal("0.0678")},
    "71010": {"description_en": "Daycare services", "description_fr": "Services de garde", "rate": Decimal("0.0128")},
    "81010": {"description_en": "Auto repair", "description_fr": "Réparation automobile", "rate": Decimal("0.0245")},
    "33010": {"description_en": "Machinery manufacturing", "description_fr": "Fabrication de machines", "rate": Decimal("0.0198")},
    "41010": {"description_en": "Wholesale trade", "description_fr": "Commerce de gros", "rate": Decimal("0.0156")},
}


def validate_cnesst_rate(
    industry_code: str,
    rate_used: float | Decimal,
) -> dict:
    """
    Validate that the correct CNESST premium rate is used for the industry.

    Parameters
    ----------
    industry_code : str   CNESST unit classification code (e.g. "23010")
    rate_used     : float/Decimal  The rate applied

    Returns dict with: valid, expected_rate, rate_used, industry, error_type.
    """
    code = str(industry_code).strip()
    used = _to_decimal(rate_used)

    industry_info = CNESST_INDUSTRY_RATES.get(code)
    if not industry_info:
        return {
            "valid": False,
            "industry_code": code,
            "rate_used": str(used),
            "error_type": "cnesst_unknown_industry",
            "description_en": f"Unknown CNESST industry code: {code}",
            "description_fr": f"Code d'unité CNESST inconnu: {code}",
        }

    expected = industry_info["rate"]
    valid = abs(used - expected) < Decimal("0.0005")

    result: dict[str, Any] = {
        "valid": valid,
        "industry_code": code,
        "industry_en": industry_info["description_en"],
        "industry_fr": industry_info["description_fr"],
        "expected_rate": str(expected),
        "rate_used": str(used),
        "error_type": None if valid else "cnesst_rate_error",
    }
    if not valid:
        result["description_en"] = (
            f"CNESST rate {used} for industry {code} "
            f"({industry_info['description_en']}) is incorrect. "
            f"Expected rate: {expected}."
        )
        result["description_fr"] = (
            f"Le taux CNESST de {used} pour l'unité {code} "
            f"({industry_info['description_fr']}) est incorrect. "
            f"Taux attendu: {expected}."
        )
    return result
