#!/usr/bin/env python3
"""
scripts/generate_test_data.py

Generates 50,000 realistic fake Quebec transactions and inserts them into the
documents table across 25 fake clients.  After insertion it runs
fraud_engine.run_fraud_detection() on every document and updates fraud_flags.

Scenario distribution per client (2,000 docs total):
  normal         1100  55 % — correct GL/tax, confidence 0.85-0.99
  duplicate       200  10 % — same vendor+amount, date within 30 days
  weekend         100   5 % — Saturday or Sunday, amount > $500
  new_vendor      100   5 % — unknown vendor, amount > $2,000
  round_number    100   5 % — amount in {$500, $1 000, $2 000, $5 000}
  meal            100   5 % — tax M, restaurant vendor, $30-$300
  insurance       100   5 % — tax I, insurance vendor
  low_confidence  100   5 % — confidence 0.40-0.65, NeedsReview
  math_mismatch   100   5 % — gst+qst deliberately wrong in raw_result

Usage:
    python scripts/generate_test_data.py
"""
from __future__ import annotations

import json
import math
import random
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.engines.fraud_engine import (
    DUPLICATE_SAME_VENDOR_DAYS,
    NEW_VENDOR_LARGE_AMOUNT_LIMIT,
    WEEKEND_HOLIDAY_AMOUNT_LIMIT,
    _ensure_fraud_flags_column,
    _is_quebec_holiday,
    _parse_date,
    _rule_weekend_holiday,
    _safe_float,
)

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SEED = 2024
random.seed(SEED)

# ── Tax constants ─────────────────────────────────────────────────────────────

_CENT = Decimal("0.01")
_GST  = Decimal("0.05")
_QST  = Decimal("0.09975")
_INS  = Decimal("0.09")        # Quebec insurance charge (non-recoverable)
_T_DIV = Decimal("1.14975")    # 1 + GST + QST
_I_DIV = Decimal("1.09")       # 1 + 9 % insurance charge


def _d(v: Any) -> Decimal:
    return Decimal(str(v))


def _r2(v: Decimal) -> Decimal:
    return v.quantize(_CENT, rounding=ROUND_HALF_UP)


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ── Client definitions ────────────────────────────────────────────────────────

CLIENTS: dict[str, str] = {
    "MARCEL":    "Restaurant Chez Marcel Inc",
    "BOLDUC":    "Construction Bolduc Inc",
    "DENTAIRE":  "Cabinet Dentaire St-Laurent",
    "BOUTIQUE":  "Boutique Mode Québec Inc",
    "TECHLAVAL": "Services TI Laval Inc",
    "PLOMBERIE": "Services de plomberie Gagnon Inc",
    "AVOCAT":    "Cabinet juridique Beauchamp",
    "IMMO":      "Gestion immobilière Tremblay",
    "TRANSPORT": "Transport Lapointe Inc",
    "CLINIQUE":  "Clinique médicale du Parc",
    "EPICERIE":    "Dépanneur Chez Paulo",
    "MANUFACTURE": "Fabrication Dubois Inc",
    "NETTOYAGE":   "Services d'entretien Ménard",
    "AGENCE":      "Agence de communication Pixel",
    "GARDERIE":    "Garderie Les Petits Lapins",
    "ELECTRICIEN": "Électricité Fortier Inc",
    "TRAITEUR":    "Traiteur Saveurs du Québec",
    "PHARMACIE":   "Pharmacie Santé Plus",
    "TOITURE":     "Toiture Expertpro",
    "CONSULT":     "Conseil en gestion RH Beaulieu",
    "PAYSAGE":     "Paysagement Verdure Québec Inc",
    "VETERINAIRE": "Clinique Vétérinaire Beaulac",
    "DEMENAGEMENT":"Déménagement Express Montréal",
    "IMPRIMERIE":  "Imprimerie Rapide Québec Inc",
    "SECURITE":    "Services de Sécurité Vigilance",
}

# ── Scenario counts (must total 2000) ─────────────────────────────────────────

SCENARIO_COUNTS: dict[str, int] = {
    "normal":         1100,
    "duplicate":       200,
    "weekend":         100,
    "new_vendor":      100,
    "round_number":    100,
    "meal":            100,
    "insurance":       100,
    "low_confidence":  100,
    "math_mismatch":   100,
}
assert sum(SCENARIO_COUNTS.values()) == 2000

# ── Vendor pools per client ───────────────────────────────────────────────────
# Each entry: (display_name, gl_account, tax_code, total_min, total_max)
# total_min / total_max are the *tax-inclusive* invoice total in CAD.

VENDOR_POOLS: dict[str, list[tuple[str, str, str, int, int]]] = {
    "MARCEL": [
        ("Sysco Québec",                 "Achats et matières premières",  "T", 500,   5000),
        ("Distribution Métro Inc",       "Achats et matières premières",  "T", 300,   3000),
        ("Provigo Commerce",             "Achats et matières premières",  "T", 200,   2500),
        ("Distribution Alim Aux (DAA)", "Achats et matières premières",   "T", 400,   4000),
        ("Fromages du Roy Ltée",         "Achats et matières premières",  "T", 150,    800),
        ("Les Brasseurs du Nord",        "Achats boissons",               "T", 300,   2000),
        ("Boulangerie Première Moisson", "Achats et matières premières",  "T", 100,    600),
        ("Fruits Légumes Clément",       "Achats et matières premières",  "T", 150,    800),
        ("Papiers Cascades Canada",      "Fournitures de restaurant",     "T", 200,   1200),
        ("Cuisine Commerciale Resto-Pro","Fournitures de restaurant",     "T", 300,   2500),
        ("Hydro-Québec",                 "Électricité et gaz",            "E", 300,   1500),
        ("Énergir Distribution",         "Électricité et gaz",            "E", 200,    800),
        ("Bell Communications",          "Télécommunications",            "T", 150,    500),
        ("Vidéotron Affaires",           "Télécommunications",            "T", 120,    400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",  20,    150),
        ("Entretien Nettoy-Pro Enr",     "Entretien et nettoyage",        "T", 200,    800),
        ("Blanchisserie Express Mtl",    "Entretien et nettoyage",        "T", 150,    600),
        ("Buanderie Sanitaire Québec",   "Entretien et nettoyage",        "T", 100,    500),
        ("BMO Frais de Service",         "Frais bancaires",               "E",  15,    120),
        ("Aliments Ultima Inc",          "Achats et matières premières",  "T", 250,   1800),
    ],
    "BOLDUC": [
        ("Rona Pro Laval",               "Matériaux et fournitures",      "T", 300,   8000),
        ("Home Depot Pro",               "Matériaux et fournitures",      "T", 400,  10000),
        ("Canac Matériaux",              "Matériaux et fournitures",      "T", 250,   6000),
        ("Patrick Morin Inc",            "Matériaux et fournitures",      "T", 300,   5000),
        ("BMR Construction",             "Matériaux et fournitures",      "T", 200,   4000),
        ("Réno-Dépôt Pro",               "Matériaux et fournitures",      "T", 350,   7000),
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T", 100,    800),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T", 120,    700),
        ("Esso Commercial Québec",       "Carburant et huile",            "T", 100,    600),
        ("Westburne Électrique",         "Matériaux et fournitures",      "T", 400,   5000),
        ("Rexel Canada Électrique",      "Matériaux et fournitures",      "T", 350,   4500),
        ("Fastenal Canada Inc",          "Matériaux et fournitures",      "T", 150,   2000),
        ("Groupe Deschênes Inc",         "Matériaux et fournitures",      "T", 300,   3500),
        ("Hydro-Québec",                 "Électricité et gaz",            "E", 200,    900),
        ("Bell Affaires Québec",         "Télécommunications",            "T", 150,    500),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",  25,    200),
        ("RBC Frais de Service",         "Frais bancaires",               "E",  20,    150),
        ("Sunbelt Location Équip",       "Location d'équipement",         "T", 500,   5000),
        ("RSC Équipement Location",      "Location d'équipement",         "T", 400,   4000),
        ("Équipement Bisson Ltée",       "Location d'équipement",         "T", 300,   3000),
    ],
    "DENTAIRE": [
        ("Dentsply Canada",              "Fournitures dentaires",         "T", 200,   3000),
        ("Patterson Dental Canada",      "Fournitures dentaires",         "T", 300,   4000),
        ("Kerr Canada Dental",           "Fournitures dentaires",         "T", 150,   2000),
        ("3M Canada Dentaire",           "Fournitures dentaires",         "T", 100,   1500),
        ("Sirona Dental Systems",        "Équipement médical",            "T", 500,   8000),
        ("Planmeca Finland (CA)",        "Équipement médical",            "T", 800,  12000),
        ("Lyreco Canada Inc",            "Fournitures de bureau",         "T",  50,    500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",  60,    400),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",  25,    200),
        ("Logiciel Dentitek Inc",        "Logiciels et abonnements",      "T", 100,    800),
        ("Bell Communications",          "Télécommunications",            "T", 120,    400),
        ("Hydro-Québec",                 "Électricité et gaz",            "E", 150,    600),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",  15,    100),
        ("TD Frais de Service",          "Frais bancaires",               "E",  20,    120),
        ("Service Stérilisation Pro",    "Fournitures dentaires",         "T",  80,    500),
        ("Pharmascience Inc",            "Fournitures médicales",         "T", 100,    800),
        ("Zoom Video Communications",    "Logiciels et abonnements",      "T",  15,    150),
        ("Intact Assurance Dentaire",    "Assurances",                    "I", 800,   3000),
        ("SSQ Assurance Groupe",         "Assurances",                    "I", 600,   2500),
        ("Entretien Médical Express",    "Entretien et nettoyage",        "T", 150,    600),
    ],
    "BOUTIQUE": [
        ("Mode Atlantique Distribution", "Achats marchandises",           "T", 1000, 15000),
        ("Importation Styl-Mode Inc",    "Achats marchandises",           "T",  800, 10000),
        ("Collection Mode QC Enr",       "Achats marchandises",           "T",  600,  8000),
        ("Grossiste Textile Montréal",   "Achats marchandises",           "T",  500,  7000),
        ("Fournisseur Mode Plus Ltée",   "Achats marchandises",           "T",  400,  5000),
        ("Amazon Business Canada",       "Fournitures de bureau",         "T",   50,   500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Shopify Plus Canada",          "Logiciels et abonnements",      "T",   79,   300),
        ("Facebook Ads Canada",          "Publicité et marketing",        "T",  200,  3000),
        ("Google Ads Canada",            "Publicité et marketing",        "T",  200,  3000),
        ("Bell Communications",          "Télécommunications",            "T",  120,   400),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  110,   350),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   700),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("Purolator Canada Ltée",        "Transport et déplacements",     "T",   30,   300),
        ("FedEx Canada Inc",             "Transport et déplacements",     "T",   25,   250),
        ("Intact Assurance Commerce",    "Assurances",                    "I",  500,  2000),
        ("La Personnelle Assurance",     "Assurances",                    "I",  400,  1800),
        ("Adobe Creative Cloud",         "Logiciels et abonnements",      "T",   54,   300),
        ("Mailchimp Canada",             "Logiciels et abonnements",      "T",   20,   200),
    ],
    "TECHLAVAL": [
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",  100,  1500),
        ("Adobe Creative Cloud",         "Logiciels et abonnements",      "T",   54,   800),
        ("Google Workspace",             "Logiciels et abonnements",      "T",   60,   600),
        ("AWS Canada (Amazon)",          "Infrastructure infonuagique",   "T",  200,  5000),
        ("Slack Technologies Inc",       "Logiciels et abonnements",      "T",   30,   400),
        ("GitHub Enterprise",            "Logiciels et abonnements",      "T",   21,   300),
        ("Zoom Video Communications",    "Logiciels et abonnements",      "T",   15,   200),
        ("Atlassian Jira/Confluence",    "Logiciels et abonnements",      "T",   40,   500),
        ("Salesforce Canada Inc",        "Logiciels et abonnements",      "T",  150,  2000),
        ("Bell Affaires Québec",         "Télécommunications",            "T",  200,   800),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  150,   600),
        ("Rogers Affaires Canada",       "Télécommunications",            "T",  180,   700),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   800),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("Banque Nationale Frais",       "Frais bancaires",               "E",   15,   100),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   50,   400),
        ("LinkedIn Ads Canada",          "Publicité et marketing",        "T",  300,  3000),
        ("Intact Assurance TI",          "Assurances",                    "I",  600,  2500),
        ("La Capitale Assurance",        "Assurances",                    "I",  500,  2000),
        ("OVH Cloud Canada",             "Infrastructure infonuagique",   "T",  100,  2000),
    ],
    "PLOMBERIE": [
        ("Wolseley Canada Plomberie",    "Matériaux et fournitures",      "T",  200,  5000),
        ("Masters Plomberie Québec",     "Matériaux et fournitures",      "T",  150,  3000),
        ("Groupe Deschênes Plomberie",   "Matériaux et fournitures",      "T",  300,  6000),
        ("Noble Canada Plomberie",       "Matériaux et fournitures",      "T",  100,  2500),
        ("Emco Corporation QC",          "Matériaux et fournitures",      "T",  200,  4000),
        ("Rona Pro Plomberie",           "Matériaux et fournitures",      "T",  150,  3500),
        ("Home Depot Pro Plomberie",     "Matériaux et fournitures",      "T",  200,  5000),
        ("Canac Plomberie",              "Matériaux et fournitures",      "T",  100,  2000),
        ("Ultramar Carburant",           "Carburant et huile",            "T",  100,   700),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   600),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   800),
        ("Bell Communications",          "Télécommunications",            "T",  120,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("Sunbelt Location Équip",       "Location d'équipement",         "T",  300,  4000),
        ("Équipement Bisson Ltée",       "Location d'équipement",         "T",  200,  3000),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  100,   800),
        ("Mark's Work Wearhouse",        "Équipements de protection individuelle", "T", 100, 500),
        ("Intact Assurance",             "Assurances",                    "I",  600,  2500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("BMO Frais de Service",         "Frais bancaires",               "E",   15,   120),
    ],
    "AVOCAT": [
        ("Thomson Reuters Canada",       "Logiciels et abonnements",      "T",  200,  3000),
        ("LexisNexis Canada",            "Logiciels et abonnements",      "T",  150,  2500),
        ("Westlaw Canada",               "Logiciels et abonnements",      "T",  100,  2000),
        ("SOQUIJ Abonnement",            "Logiciels et abonnements",      "T",   80,   600),
        ("Barreau du Québec Cotisation", "Honoraires professionnels",     "T",  500,  3000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   400),
        ("Lyreco Canada Inc",            "Fournitures de bureau",         "T",   50,   350),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
        ("Adobe Creative Cloud",         "Logiciels et abonnements",      "T",   54,   300),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  110,   400),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   900),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("TD Frais de Service",          "Frais bancaires",               "E",   20,   120),
        ("Purolator Canada Ltée",        "Transport et déplacements",     "T",   30,   200),
        ("FedEx Canada Inc",             "Transport et déplacements",     "T",   25,   250),
        ("Intact Assurance Juridique",   "Assurances",                    "I",  800,  4000),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  600,  3000),
        ("Imprimerie Solisco",           "Impression et papeterie",       "T",  100,   800),
        ("Minuteman Press Québec",       "Impression et papeterie",       "T",   80,   500),
    ],
    "IMMO": [
        ("Hydro-Québec Immeubles",       "Électricité et gaz",            "E",  500,  5000),
        ("Énergir Chauffage",            "Électricité et gaz",            "E",  300,  3000),
        ("Bell Communications",          "Télécommunications",            "T",  200,   800),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  150,   600),
        ("GDI Services aux immeubles",   "Entretien et nettoyage",        "T",  500,  5000),
        ("ServiceMaster Canada",         "Entretien et nettoyage",        "T",  400,  4000),
        ("Jan-Pro Québec",               "Entretien et nettoyage",        "T",  300,  3000),
        ("Rona Pro Entretien",           "Matériaux et fournitures",      "T",  200,  3000),
        ("Home Depot Pro Immo",          "Matériaux et fournitures",      "T",  300,  5000),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   30,   250),
        ("RBC Frais de Service",         "Frais bancaires",               "E",   25,   200),
        ("Intact Assurance Immeuble",    "Assurances",                    "I", 1000,  8000),
        ("La Capitale Assurance Immo",   "Assurances",                    "I",  800,  6000),
        ("Waste Management Québec",      "Gestion des déchets",           "T",  200,  1500),
        ("ADT Sécurité Québec",          "Sécurité et surveillance",      "T",  100,   500),
        ("Garda World Sécurité",         "Sécurité et surveillance",      "T",  200,  1500),
        ("Groupe Vertdure Paysage",      "Aménagement paysager",          "T",  300,  3000),
        ("Déneigement Pro Québec",       "Entretien et nettoyage",        "T",  500,  5000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
    ],
    "TRANSPORT": [
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  500, 10000),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  400,  8000),
        ("Esso Commercial Québec",       "Carburant et huile",            "T",  300,  6000),
        ("Shell Commercial Canada",      "Carburant et huile",            "T",  400,  7000),
        ("Canadian Tire Auto",           "Entretien véhicules",           "T",  200,  3000),
        ("Kal Tire Québec",              "Entretien véhicules",           "T",  300,  5000),
        ("NAPA Autopro Service",         "Entretien véhicules",           "T",  150,  2000),
        ("Midas Québec Camions",         "Entretien véhicules",           "T",  200,  3000),
        ("Pneus Touchette Fleet",        "Entretien véhicules",           "T",  250,  4000),
        ("SAAQ Immatriculation",         "Permis et immatriculations",    "T",  200,  2000),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   800),
        ("Bell Affaires Québec",         "Télécommunications",            "T",  150,   600),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   25,   200),
        ("Banque Nationale Frais",       "Frais bancaires",               "E",   20,   150),
        ("Intact Assurance Transport",   "Assurances",                    "I", 1000,  8000),
        ("SSQ Assurance Flotte",         "Assurances",                    "I",  800,  6000),
        ("Sunbelt Location Équip",       "Location d'équipement",         "T",  300,  4000),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  200,  2000),
        ("Mark's Work Wearhouse",        "Équipements de protection individuelle", "T", 100, 500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "CLINIQUE": [
        ("McKesson Canada Pharma",       "Fournitures médicales",         "T",  300,  5000),
        ("Cardinal Health Canada",       "Fournitures médicales",         "T",  200,  4000),
        ("Medline Canada",               "Fournitures médicales",         "T",  150,  3000),
        ("Becton Dickinson Canada",      "Fournitures médicales",         "T",  100,  2000),
        ("Sirona Dental Systems",        "Équipement médical",            "T",  500,  8000),
        ("Stryker Canada Médical",       "Équipement médical",            "T",  800, 15000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Lyreco Canada Inc",            "Fournitures de bureau",         "T",   50,   400),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
        ("TELUS Santé Logiciel",         "Logiciels et abonnements",      "T",  100,  1000),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   900),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("TD Frais de Service",          "Frais bancaires",               "E",   20,   120),
        ("Entretien Médical Express",    "Entretien et nettoyage",        "T",  200,  1000),
        ("Molly Maid Clinique",          "Entretien et nettoyage",        "T",  150,   800),
        ("Intact Assurance Médicale",    "Assurances",                    "I",  800,  4000),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  600,  3000),
        ("Pharmascience Inc",            "Fournitures médicales",         "T",  100,  1500),
        ("Laboratoire Médical Biron",    "Frais de santé",                "E",  100,   800),
    ],
    "EPICERIE": [
        ("Distribution Alim-Plus",       "Achats et matières premières",  "T",  400,  5000),
        ("Saputo Produits Laitiers",     "Achats et matières premières",  "T",  300,  4000),
        ("Laiterie Natrel Inc",          "Achats et matières premières",  "T",  200,  3000),
        ("Coca-Cola Canada Bottling",    "Achats boissons",               "T",  150,  2500),
        ("PepsiCo Canada Breuvages",     "Achats boissons",               "T",  150,  2500),
        ("Boulangerie St-Méthode",       "Achats et matières premières",  "T",  100,   800),
        ("Provigo Commerce",             "Achats et matières premières",  "T",  200,  2000),
        ("Distribution Métro Inc",       "Achats et matières premières",  "T",  300,  3500),
        ("Agropur Coopérative",          "Achats et matières premières",  "T",  250,  3000),
        ("Olymel S.E.C.",                "Achats et matières premières",  "T",  200,  2500),
        ("Exceldor Coopérative",         "Achats et matières premières",  "T",  150,  2000),
        ("Frito-Lay Canada",             "Achats et matières premières",  "T",  100,  1500),
        ("Dare Foods Limitée",           "Achats et matières premières",  "T",   80,  1000),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,  1000),
        ("Énergir Distribution",         "Électricité et gaz",            "E",  150,   700),
        ("Bell Communications",          "Télécommunications",            "T",  100,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("BMO Frais de Service",         "Frais bancaires",               "E",   15,   120),
        ("Intact Assurance Commerce",    "Assurances",                    "I",  500,  2500),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  100,   350),
    ],
    "MANUFACTURE": [
        ("Acier Richelieu Inc",          "Matières premières",            "T",  500,  8000),
        ("Aluminium Saguenay Ltée",      "Matières premières",            "T",  400,  7000),
        ("Plastiques Moore Inc",         "Matières premières",            "T",  300,  5000),
        ("Produits Chimiques Magnus",    "Matières premières",            "T",  200,  4000),
        ("Bois Franc Québec Inc",        "Matières premières",            "T",  350,  6000),
        ("Emballages Cascades",          "Fournitures d'emballage",       "T",  200,  3000),
        ("Groupe Soucy Industriel",      "Équipement industriel",         "T",  500, 10000),
        ("Grainger Canada",              "Fournitures industrielles",     "T",  150,  3000),
        ("Fastenal Canada Inc",          "Fournitures industrielles",     "T",  100,  2000),
        ("Wajax Équipement",             "Équipement industriel",         "T",  400,  8000),
        ("Hydro-Québec Industriel",      "Électricité et gaz",            "E",  500,  5000),
        ("Énergir Industriel",           "Électricité et gaz",            "E",  300,  3000),
        ("Bell Affaires Québec",         "Télécommunications",            "T",  150,   600),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   25,   200),
        ("RBC Frais de Service",         "Frais bancaires",               "E",   20,   150),
        ("Intact Assurance Industrielle","Assurances",                    "I", 1000,  6000),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  800,  4000),
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  200,  1500),
        ("Mark's Work Wearhouse",        "Équipements de protection individuelle", "T", 100, 600),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "NETTOYAGE": [
        ("Produits Sanitaires Lépine",   "Fournitures de nettoyage",      "T",  200,  3000),
        ("Swish Maintenance Ltée",       "Fournitures de nettoyage",      "T",  150,  2500),
        ("Dustbane Products Inc",        "Fournitures de nettoyage",      "T",  100,  2000),
        ("Deb Group Canada",             "Fournitures de nettoyage",      "T",  100,  1500),
        ("Bunzl Canada Distribution",    "Fournitures de nettoyage",      "T",  200,  3500),
        ("Kärcher Canada Inc",           "Équipement de nettoyage",       "T",  300,  5000),
        ("Tennant Company Canada",       "Équipement de nettoyage",       "T",  400,  6000),
        ("Ultramar Carburant",           "Carburant et huile",            "T",  100,   700),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   600),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  150,  1000),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   600),
        ("Bell Communications",          "Télécommunications",            "T",  100,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("BMO Frais de Service",         "Frais bancaires",               "E",   15,   120),
        ("Intact Assurance",             "Assurances",                    "I",  500,  2500),
        ("La Personnelle Assurance",     "Assurances",                    "I",  400,  2000),
        ("Mark's Work Wearhouse",        "Uniformes et vêtements",        "T",  100,   500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  100,   350),
        ("CSST Québec Cotisations",      "Cotisations et permis",         "E",  200,  1500),
    ],
    "AGENCE": [
        ("Adobe Creative Cloud",         "Logiciels et abonnements",      "T",  100,  1200),
        ("Google Workspace",             "Logiciels et abonnements",      "T",   60,   600),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   50,   500),
        ("Slack Technologies Inc",       "Logiciels et abonnements",      "T",   30,   400),
        ("Figma Design Inc",             "Logiciels et abonnements",      "T",   25,   300),
        ("Canva Pro Canada",             "Logiciels et abonnements",      "T",   20,   200),
        ("Mailchimp Canada",             "Logiciels et abonnements",      "T",   20,   300),
        ("HubSpot Marketing",            "Logiciels et abonnements",      "T",  200,  3000),
        ("Facebook Ads Canada",          "Publicité et marketing",        "T",  300,  5000),
        ("Google Ads Canada",            "Publicité et marketing",        "T",  300,  5000),
        ("LinkedIn Ads Canada",          "Publicité et marketing",        "T",  200,  3000),
        ("Shutterstock Canada",          "Logiciels et abonnements",      "T",   30,   400),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  110,   400),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   700),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Intact Assurance Affaires",    "Assurances",                    "I",  500,  2500),
        ("AWS Canada (Amazon)",          "Infrastructure infonuagique",   "T",  100,  3000),
        ("Zoom Video Communications",    "Logiciels et abonnements",      "T",   15,   200),
    ],
    "GARDERIE": [
        ("Éducation Brault & Bouthillier","Fournitures éducatives",       "T",  100,  2000),
        ("Spectrum Nasco Éducatif",       "Fournitures éducatives",        "T",   80,  1500),
        ("Scholars Choice Canada",        "Fournitures éducatives",        "T",   60,  1200),
        ("Distribution Alim-Plus",        "Achats alimentaires",           "Z",  200,  2000),
        ("Sysco Québec Petits Formats",   "Achats alimentaires",           "Z",  150,  1500),
        ("Provigo Commerce Alimentaire",  "Achats alimentaires",           "Z",  100,  1000),
        ("Laiterie Natrel Inc",           "Achats alimentaires",           "Z",   80,   600),
        ("Hydro-Québec",                  "Électricité et gaz",            "E",  200,  1000),
        ("Énergir Distribution",          "Électricité et gaz",            "E",  150,   800),
        ("Bell Communications",           "Télécommunications",            "T",  100,   400),
        ("Vidéotron Affaires",            "Télécommunications",            "T",  100,   350),
        ("Bureau en Gros",                "Fournitures de bureau",         "T",   40,   300),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E",   20,   150),
        ("TD Frais de Service",           "Frais bancaires",               "E",   15,   100),
        ("Intact Assurance Garderie",     "Assurances",                    "I",  600,  3000),
        ("SSQ Assurance Groupe",          "Assurances",                    "I",  500,  2500),
        ("Produits Sanitaires Lépine",    "Fournitures de nettoyage",      "T",  100,   800),
        ("CSST Québec Cotisations",       "Cotisations et permis",         "E",  200,  1500),
        ("Jouets Éducatifs Québec",       "Fournitures éducatives",        "T",   50,   500),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T",   25,   200),
    ],
    "ELECTRICIEN": [
        ("Westburne Électrique",         "Matériaux et fournitures",      "T",  300,  6000),
        ("Rexel Canada Électrique",      "Matériaux et fournitures",      "T",  250,  5000),
        ("Guillevin International",      "Matériaux et fournitures",      "T",  200,  4000),
        ("Nedco Division Rexel",         "Matériaux et fournitures",      "T",  150,  3500),
        ("Eaton Électrique Canada",      "Matériaux et fournitures",      "T",  200,  5000),
        ("Schneider Electric Canada",    "Matériaux et fournitures",      "T",  300,  7000),
        ("Leviton Canada",               "Matériaux et fournitures",      "T",  100,  2000),
        ("Milwaukee Tool Canada",        "Outillage",                     "T",  100,  1500),
        ("DeWalt Outils Professionnels", "Outillage",                     "T",  150,  2000),
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  100,   800),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   700),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   900),
        ("Bell Communications",          "Télécommunications",            "T",  120,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   25,   200),
        ("RBC Frais de Service",         "Frais bancaires",               "E",   20,   150),
        ("Sunbelt Location Équip",       "Location d'équipement",         "T",  300,  4000),
        ("Mark's Work Wearhouse",        "Équipements de protection individuelle", "T", 100, 500),
        ("Intact Assurance Entrepreneur","Assurances",                    "I",  600,  3000),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  150,  1000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "TRAITEUR": [
        ("Sysco Québec",                 "Achats et matières premières",  "T",  400,  6000),
        ("Distribution Métro Inc",       "Achats et matières premières",  "T",  300,  4000),
        ("Distribution Alim Aux (DAA)",  "Achats et matières premières",  "T",  300,  5000),
        ("Fruits Légumes Clément",       "Achats et matières premières",  "T",  150,  1000),
        ("Fromages du Roy Ltée",         "Achats et matières premières",  "T",  100,   800),
        ("Les Brasseurs du Nord",        "Achats boissons",               "T",  200,  2000),
        ("Boulangerie Première Moisson", "Achats et matières premières",  "T",  100,   700),
        ("Aliments Ultima Inc",          "Achats et matières premières",  "T",  200,  2000),
        ("Papiers Cascades Canada",      "Fournitures de service",        "T",  150,  1000),
        ("Cuisine Commerciale Resto-Pro","Équipement de cuisine",         "T",  300,  5000),
        ("Ultramar Carburant",           "Carburant et huile",            "T",  100,   700),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   600),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  250,  1200),
        ("Énergir Distribution",         "Électricité et gaz",            "E",  150,   700),
        ("Bell Communications",          "Télécommunications",            "T",  120,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("BMO Frais de Service",         "Frais bancaires",               "E",   15,   120),
        ("Intact Assurance Restauration","Assurances",                    "I",  600,  3000),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  150,  1000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "PHARMACIE": [
        ("McKesson Canada Pharma",       "Fournitures pharmaceutiques",   "T",  500,  8000),
        ("Cardinal Health Canada",       "Fournitures pharmaceutiques",   "T",  400,  6000),
        ("Pharmascience Inc",            "Fournitures pharmaceutiques",   "T",  300,  5000),
        ("Apotex Canada Inc",            "Fournitures pharmaceutiques",   "T",  200,  4000),
        ("Teva Canada Limitée",          "Fournitures pharmaceutiques",   "T",  250,  4500),
        ("Bausch Health Canada",         "Fournitures pharmaceutiques",   "T",  150,  3000),
        ("Medline Canada",               "Fournitures médicales",         "T",  100,  2000),
        ("Becton Dickinson Canada",      "Fournitures médicales",         "T",  100,  1500),
        ("Kroll Logiciel Pharmacie",     "Logiciels et abonnements",      "T",  100,  1000),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   900),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("TD Frais de Service",          "Frais bancaires",               "E",   20,   120),
        ("Intact Assurance Pharmacie",   "Assurances",                    "I",  800,  4000),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  600,  3000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Purolator Canada Ltée",        "Transport et déplacements",     "T",   30,   250),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  100,   400),
        ("Entretien Médical Express",    "Entretien et nettoyage",        "T",  150,   600),
    ],
    "TOITURE": [
        ("BP Canada Matériaux Toiture",  "Matériaux de toiture",          "T",  400,  8000),
        ("IKO Industries Toiture",       "Matériaux de toiture",          "T",  350,  7000),
        ("CertainTeed Canada Toiture",   "Matériaux de toiture",          "T",  300,  6000),
        ("Soprema Canada Inc",           "Matériaux de toiture",          "T",  500, 10000),
        ("Rona Pro Matériaux",           "Matériaux et fournitures",      "T",  200,  5000),
        ("Home Depot Pro Toiture",       "Matériaux et fournitures",      "T",  300,  6000),
        ("BMR Construction Toiture",     "Matériaux et fournitures",      "T",  200,  4000),
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  100,   800),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   700),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   800),
        ("Bell Communications",          "Télécommunications",            "T",  120,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   25,   200),
        ("RBC Frais de Service",         "Frais bancaires",               "E",   20,   150),
        ("Sunbelt Location Équip",       "Location d'équipement",         "T",  400,  5000),
        ("Équipement Bisson Ltée",       "Location d'équipement",         "T",  300,  4000),
        ("Mark's Work Wearhouse",        "Équipements de protection individuelle", "T", 100, 600),
        ("Intact Assurance Construction","Assurances",                    "I",  800,  4000),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  600,  3000),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  150,  1000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "CONSULT": [
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   50,   500),
        ("Adobe Creative Cloud",         "Logiciels et abonnements",      "T",   54,   400),
        ("Google Workspace",             "Logiciels et abonnements",      "T",   60,   600),
        ("Slack Technologies Inc",       "Logiciels et abonnements",      "T",   30,   400),
        ("Zoom Video Communications",    "Logiciels et abonnements",      "T",   15,   200),
        ("HubSpot CRM Canada",           "Logiciels et abonnements",      "T",  100,  1500),
        ("LinkedIn Ads Canada",          "Publicité et marketing",        "T",  200,  2000),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   400),
        ("Lyreco Canada Inc",            "Fournitures de bureau",         "T",   50,   350),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  110,   400),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   700),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("TD Frais de Service",          "Frais bancaires",               "E",   20,   120),
        ("Air Canada Affaires",          "Transport et déplacements",     "T",  200,  3000),
        ("Via Rail Canada",              "Transport et déplacements",     "T",   50,   500),
        ("Intact Assurance Professionnelle","Assurances",                 "I",  600,  3000),
        ("La Capitale Assurance",        "Assurances",                    "I",  500,  2500),
        ("Purolator Canada Ltée",        "Transport et déplacements",     "T",   30,   200),
        ("FedEx Canada Inc",             "Transport et déplacements",     "T",   25,   250),
    ],
    "PAYSAGE": [
        ("Pépinière Villeneuve Inc",     "Plants et végétaux",            "T",  200,  4000),
        ("Centre Jardin Hamel",          "Plants et végétaux",            "T",  150,  3000),
        ("Botanix Québec Grossiste",     "Plants et végétaux",            "T",  100,  2500),
        ("Terre Brune Québec Inc",       "Terre et matériaux",            "T",  200,  3000),
        ("Pierre Décorative Québec",     "Terre et matériaux",            "T",  150,  2500),
        ("Rona Pro Paysage",             "Matériaux et fournitures",      "T",  200,  4000),
        ("Home Depot Pro Jardin",        "Matériaux et fournitures",      "T",  150,  3500),
        ("John Deere Québec",            "Équipement paysager",           "T",  500,  8000),
        ("Husqvarna Canada Pro",         "Équipement paysager",           "T",  300,  5000),
        ("STIHL Canada Limitée",         "Équipement paysager",           "T",  200,  3000),
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  100,   800),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   700),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   600),
        ("Bell Communications",          "Télécommunications",            "T",  100,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  150,  1000),
        ("Intact Assurance Paysagiste",  "Assurances",                    "I",  500,  2500),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  400,  2000),
        ("Mark's Work Wearhouse",        "Uniformes et vêtements",        "T",  100,   500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "VETERINAIRE": [
        ("CDMV Inc (Centre Distribution)","Fournitures vétérinaires",     "T",  300,  5000),
        ("Covetrus Canada",              "Fournitures vétérinaires",      "T",  200,  4000),
        ("Zoetis Canada Inc",            "Produits pharmaceutiques vét.", "T",  250,  4500),
        ("Elanco Canada Limitée",        "Produits pharmaceutiques vét.", "T",  200,  3500),
        ("Boehringer Ingelheim Vét.",    "Produits pharmaceutiques vét.", "T",  150,  3000),
        ("Idexx Laboratoires Canada",    "Équipement de laboratoire",     "T",  400,  6000),
        ("Heska Canada Inc",             "Équipement de laboratoire",     "T",  300,  5000),
        ("Logiciel AVImark Vétérinaire", "Logiciels et abonnements",      "T",  100,   800),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  200,   900),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("TD Frais de Service",          "Frais bancaires",               "E",   20,   120),
        ("Entretien Médical Express",    "Entretien et nettoyage",        "T",  150,   600),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Lyreco Canada Inc",            "Fournitures de bureau",         "T",   50,   400),
        ("Intact Assurance Vétérinaire", "Assurances",                    "I",  700,  3500),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  500,  2500),
        ("Purolator Canada Ltée",        "Transport et déplacements",     "T",   30,   250),
        ("Pharmascience Inc Vét.",       "Fournitures vétérinaires",      "T",  100,  1500),
    ],
    "DEMENAGEMENT": [
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  300,  5000),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  250,  4000),
        ("Esso Commercial Québec",       "Carburant et huile",            "T",  200,  3500),
        ("Shell Commercial Canada",      "Carburant et huile",            "T",  250,  4000),
        ("Canadian Tire Auto",           "Entretien véhicules",           "T",  200,  3000),
        ("Kal Tire Québec",              "Entretien véhicules",           "T",  250,  4000),
        ("NAPA Autopro Service",         "Entretien véhicules",           "T",  150,  2000),
        ("Pneus Touchette Fleet",        "Entretien véhicules",           "T",  200,  3000),
        ("Emballages Cartopak Inc",      "Fournitures d'emballage",       "T",  100,  1500),
        ("Papiers Cascades Canada",      "Fournitures d'emballage",       "T",  150,  2000),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   700),
        ("Bell Communications",          "Télécommunications",            "T",  100,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   25,   200),
        ("Banque Nationale Frais",       "Frais bancaires",               "E",   20,   150),
        ("Intact Assurance Transport",   "Assurances",                    "I",  800,  5000),
        ("SSQ Assurance Flotte",         "Assurances",                    "I",  600,  4000),
        ("SAAQ Immatriculation",         "Permis et immatriculations",    "T",  200,  2000),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  200,  2000),
        ("Mark's Work Wearhouse",        "Uniformes et vêtements",        "T",  100,   500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
    ],
    "IMPRIMERIE": [
        ("Résolu Produits Forestiers",   "Papier et matières premières",  "T",  500,  8000),
        ("Domtar Canada Papier",         "Papier et matières premières",  "T",  400,  7000),
        ("Kruger Produits Ltée",         "Papier et matières premières",  "T",  300,  5000),
        ("Encres Supérieure Inc",        "Encres et consommables",        "T",  200,  4000),
        ("Sun Chemical Canada",          "Encres et consommables",        "T",  250,  4500),
        ("Flint Group Canada",           "Encres et consommables",        "T",  150,  3000),
        ("Heidelberg Canada Équip.",     "Équipement d'impression",       "T",  500, 10000),
        ("Konica Minolta Canada",        "Équipement d'impression",       "T",  300,  6000),
        ("Xerox Canada Ltée",            "Équipement d'impression",       "T",  200,  5000),
        ("Hydro-Québec Industriel",      "Électricité et gaz",            "E",  300,  2000),
        ("Énergir Distribution",         "Électricité et gaz",            "E",  200,  1200),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   25,   200),
        ("RBC Frais de Service",         "Frais bancaires",               "E",   20,   150),
        ("Intact Assurance Industrielle","Assurances",                    "I",  700,  3500),
        ("SSQ Assurance Groupe",         "Assurances",                    "I",  500,  2500),
        ("Purolator Canada Ltée",        "Transport et déplacements",     "T",   30,   300),
        ("FedEx Canada Inc",             "Transport et déplacements",     "T",   25,   250),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
    ],
    "SECURITE": [
        ("Honeywell Sécurité Canada",   "Équipement de sécurité",        "T",  300,  6000),
        ("Bosch Sécurité Canada",        "Équipement de sécurité",        "T",  250,  5000),
        ("Hikvision Canada Inc",         "Équipement de surveillance",    "T",  200,  4000),
        ("Axis Communications Canada",   "Équipement de surveillance",    "T",  300,  5000),
        ("Tyco Sécurité Intégrée",       "Équipement de sécurité",        "T",  400,  7000),
        ("ADT Sécurité Québec",          "Services de surveillance",      "T",  100,  1000),
        ("Garda World Sécurité",         "Services de surveillance",      "T",  200,  2000),
        ("Ultramar Carburant Fleet",     "Carburant et huile",            "T",  100,   800),
        ("Petro-Canada Fleet",           "Carburant et huile",            "T",  100,   700),
        ("Enterprise Location Véhicule", "Location de véhicules",         "T",  150,  1000),
        ("Hydro-Québec",                 "Électricité et gaz",            "E",  150,   700),
        ("Bell Communications",          "Télécommunications",            "T",  120,   500),
        ("Vidéotron Affaires",           "Télécommunications",            "T",  100,   400),
        ("Desjardins Frais Bancaires",   "Frais bancaires",               "E",   20,   150),
        ("BMO Frais de Service",         "Frais bancaires",               "E",   15,   120),
        ("Mark's Work Wearhouse",        "Uniformes et vêtements",        "T",  100,   600),
        ("Intact Assurance Sécurité",    "Assurances",                    "I",  700,  3500),
        ("La Personnelle Assurance",     "Assurances",                    "I",  500,  2500),
        ("Bureau en Gros",               "Fournitures de bureau",         "T",   40,   300),
        ("Microsoft 365 Business",       "Logiciels et abonnements",      "T",   25,   200),
    ],
}

# ── Meal vendors (scenario: meal) ─────────────────────────────────────────────

MEAL_VENDORS: list[tuple[str, str]] = [
    ("Restaurant Le Vieux-Port",     "Repas d'affaires"),
    ("Brasserie Mc Tavish Québec",   "Repas d'affaires"),
    ("Café du Vieux-Québec",         "Repas d'affaires"),
    ("Restaurant Chez Cora",         "Repas d'affaires"),
    ("St-Hubert Rôtisserie",         "Repas d'affaires"),
    ("La Belle Province Resto",      "Repas d'affaires"),
    ("Bâton Rouge Brasserie",        "Repas d'affaires"),
    ("Cage aux Sports Laval",        "Repas d'affaires"),
    ("Scores Rôtisserie Laval",      "Repas d'affaires"),
    ("Pacini Restaurant Québec",     "Repas d'affaires"),
    ("Tim Hortons Affaires",         "Repas d'affaires"),
    ("Starbucks Québec",             "Repas d'affaires"),
    ("Second Cup Montréal",          "Repas d'affaires"),
    ("Van Houtte Café",              "Repas d'affaires"),
    ("Le Commensal Restaurant",      "Repas d'affaires"),
]

# ── Insurance vendors (scenario: insurance) ───────────────────────────────────

INSURANCE_VENDORS: list[tuple[str, str, int, int]] = [
    ("Intact Assurance Corporative",   "Assurances",  800, 4000),
    ("SSQ Assurance Groupe",           "Assurances",  600, 3500),
    ("La Capitale Assurance",          "Assurances",  700, 3000),
    ("Industrielle Alliance",          "Assurances",  900, 4500),
    ("Promutuel Assurance Québec",     "Assurances",  500, 2500),
    ("Belairdirect Affaires",          "Assurances",  600, 3000),
    ("La Personnelle Assurance",       "Assurances",  450, 2000),
    ("Desjardins Assurances Générales","Assurances",  800, 4000),
    ("Aviva Canada Assurance",         "Assurances",  700, 3500),
    ("Co-operators Assurance",         "Assurances",  550, 2800),
]

# ── New-vendor name templates (never seen before) ─────────────────────────────

_NEW_VENDOR_TEMPLATES: list[str] = [
    "Fournisseur Inconnu {n} Enr",
    "Services Conseil Nouveau {n} Inc",
    "Entreprise Mystère {n} Inc",
    "Prestataire Québec {n} Ltée",
    "Consultant Nouveau {n} SENC",
    "Tech Startup {n} Canada Inc",
    "Ressources Spécialisées {n}",
    "Groupe Expertise {n} Inc",
    "Services Professionnels {n} Inc",
    "Distribution Exclusive {n} Ltée",
    "Innovation Québec {n} Inc",
    "Solutions Nouvelles {n} Inc",
    "Partenaire Récent {n} SENC",
    "Agence Spécialisée {n} Inc",
    "Groupe Nouveau {n} Canada",
    "Fournisseur Premium {n} Inc",
    "Services Créatifs {n} SENC",
    "Consultants Associés {n} Inc",
    "Expertise Nouvelle {n} Inc",
    "Fournisseur Unique {n} Ltée",
]


def _new_vendor_name(n: int) -> str:
    tpl = _NEW_VENDOR_TEMPLATES[n % len(_NEW_VENDOR_TEMPLATES)]
    return tpl.format(n=n + 1000)


# ── Handwriting scenario vendors ─────────────────────────────────────────────

HANDWRITING_VENDORS: list[tuple[str, str, str, int, int]] = [
    # Contractor cash receipts
    ("Jean-Pierre Rénovation",        "Travaux d'entretien",          "T",  50,  2000),
    ("Plomberie Martin Côté",         "Réparations plomberie",        "T", 100,  3000),
    ("Électricien Luc Tremblay",      "Travaux électriques",          "T", 150,  5000),
    ("Peinture Gagnon & Fils",        "Travaux de peinture",          "T",  80,  1500),
    ("Menuiserie Artisan Québec",     "Travaux de menuiserie",        "T", 200,  4000),
    ("Déneigement Lapointe Enr",      "Déneigement",                  "T", 100,  1000),
    ("Réparations Générales Dubois",  "Réparations diverses",         "T",  50,  2500),
    # Restaurant manual receipts
    ("Cantine du Coin",               "Repas d'affaires",             "M",  10,   80),
    ("Resto Le P'tit Québécois",      "Repas d'affaires",             "M",  15,  120),
    ("Brasserie du Village",          "Repas d'affaires",             "M",  20,  150),
    ("Café Chez Madeleine",           "Repas d'affaires",             "M",   8,   50),
    ("Casse-Croûte L'Express",        "Repas d'affaires",             "M",   5,   40),
    # Market vendor receipts
    ("Marché Jean-Talon Stand 14",    "Achats et matières premières", "T",  20,  300),
    ("Ferme Biologique St-Laurent",   "Achats et matières premières", "T",  30,  500),
    ("Boulangerie Artisanale QC",     "Achats et matières premières", "T",  15,  200),
    ("Fromager du Marché Atwater",    "Achats et matières premières", "T",  25,  400),
    ("Producteur de Miel Lanaudière", "Achats et matières premières", "T",  10,  150),
    # Taxi receipts
    ("Taxi Diamond Montréal",         "Transport et déplacements",    "T",  10,   80),
    ("Taxi Coop Québec",              "Transport et déplacements",    "T",   8,   60),
    ("Taxi Hochelaga",                "Transport et déplacements",    "T",  12,  100),
]

HANDWRITING_PAYMENT_METHODS: list[str] = [
    "cash", "debit", "credit", "cheque", "cash", "cash", "debit",
]

_HANDWRITING_NOTES_TEMPLATES: list[str] = [
    "Paiement comptant — pas de reçu imprimé",
    "Reçu manuscrit — encre pâle",
    "Écriture difficile à lire",
    "Document froissé, certains champs illisibles",
    "Reçu de taxi — écriture rapide",
    "Reçu marché — papier de mauvaise qualité",
    "Facture manuscrite entrepreneur",
    "Note de frais manuscrite",
]


# ── Tax helpers ───────────────────────────────────────────────────────────────

def _breakdown(total: Decimal, tax_code: str) -> dict[str, float]:
    """
    Given a tax-inclusive total and tax_code, return breakdown:
    subtotal, gst, qst, tax_total.
    """
    total = _r2(_d(total))
    if tax_code in ("T", "M"):
        subtotal = _r2(total / _T_DIV)
        gst      = _r2(subtotal * _GST)
        qst      = _r2(subtotal * _QST)
        tax_total = gst + qst
    elif tax_code == "I":
        # Amount stored is the total (subtotal + 9 % provincial charge).
        subtotal  = _r2(total / _I_DIV)
        gst       = Decimal("0.00")
        qst       = _r2(subtotal * _INS)
        tax_total = qst
    else:   # E, Z
        subtotal  = total
        gst       = Decimal("0.00")
        qst       = Decimal("0.00")
        tax_total = Decimal("0.00")
    return {
        "subtotal":   float(subtotal),
        "gst":        float(gst),
        "qst":        float(qst),
        "tax_total":  float(tax_total),
    }


def _random_amount(lo: int, hi: int) -> float:
    """Random amount in [lo, hi] rounded to 2 decimal places."""
    cents_lo = lo * 100
    cents_hi = hi * 100
    return random.randint(cents_lo, cents_hi) / 100


def _random_date(start: date = date(2024, 1, 15),
                 end: date = date(2025, 3, 15)) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _next_weekend_date(after: date = date(2024, 1, 1)) -> date:
    """Return a random Saturday or Sunday in 2024."""
    # Collect all Saturdays and Sundays in 2024-2025
    weekends = [
        date(2024, 1, 1) + timedelta(days=d)
        for d in range(500)
        if (date(2024, 1, 1) + timedelta(days=d)).weekday() in (5, 6)
    ]
    return random.choice(weekends)


def _build_raw_result(
    vendor: str,
    amount: float,
    tax_code: str,
    invoice_n: int,
    *,
    mismatch: bool = False,
) -> str:
    bd = _breakdown(_d(str(amount)), tax_code)
    total = amount
    gst   = bd["gst"]
    qst   = bd["qst"]
    sub   = bd["subtotal"]

    if mismatch:
        # Deliberately corrupt: inflate GST by a random amount (> $0.02 tolerance)
        error = round(random.uniform(2.5, 15.0), 2)
        gst = round(gst + error, 2)

    return json.dumps({
        "vendor":          vendor,
        "invoice_number":  f"INV-{invoice_n:06d}",
        "subtotal":        round(sub, 2),
        "gst_amount":      round(gst, 2),
        "qst_amount":      round(qst, 2),
        "total":           round(total, 2),
        "currency":        "CAD",
        "province":        "QC",
    }, ensure_ascii=False)


# ── Document builder ──────────────────────────────────────────────────────────

def _build_doc(
    doc_id: str,
    client_code: str,
    vendor: str,
    gl_account: str,
    tax_code: str,
    amount: float,
    doc_date: date,
    scenario: str,
    *,
    confidence: float | None = None,
    review_status: str | None = None,
    mismatch: bool = False,
    invoice_n: int = 0,
) -> dict[str, Any]:
    now = _utcnow()
    bd  = _breakdown(_d(str(amount)), tax_code)

    conf = confidence if confidence is not None else round(random.uniform(0.85, 0.99), 4)
    status = review_status if review_status is not None else "ReadyToPost"

    return {
        "document_id":      doc_id,
        "file_name":        f"{doc_id}.pdf",
        "file_path":        f"/test_data/{client_code}/{doc_id}.pdf",
        "client_code":      client_code,
        "vendor":           vendor,
        "doc_type":         "invoice",
        "amount":           round(amount, 2),
        "document_date":    doc_date.isoformat(),
        "gl_account":       gl_account,
        "tax_code":         tax_code,
        "category":         "expense",
        "review_status":    status,
        "confidence":       conf,
        "raw_result":       _build_raw_result(vendor, amount, tax_code, invoice_n,
                                              mismatch=mismatch),
        "created_at":       now,
        "updated_at":       now,
        "currency":         "CAD",
        "subtotal":         bd["subtotal"],
        "tax_total":        bd["tax_total"],
        "extraction_method":"generated",
        "ingest_source":    f"test:{scenario}",
        "fraud_flags":      "[]",
        "handwriting_low_confidence": 0,
        "handwriting_sample": 0,
    }


# ── Per-client document generator ────────────────────────────────────────────

def generate_client_docs(
    client_code: str,
    first_doc_n: int,
) -> list[dict[str, Any]]:
    """Generate all 2,000 documents for one client.  Returns list of doc dicts."""
    pool  = VENDOR_POOLS[client_code]
    docs: list[dict[str, Any]] = []
    n     = first_doc_n          # rolling doc ID counter

    # ── 1. Normal (220) ────────────────────────────────────────────────────
    normal_docs: list[dict[str, Any]] = []
    for _ in range(SCENARIO_COUNTS["normal"]):
        vendor, gl, tax, amin, amax = random.choice(pool)
        amt  = _random_amount(amin, amax)
        ddate = _random_date()
        status = random.choice(["ReadyToPost", "Posted", "Posted"])
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, tax, amt, ddate,
            "normal", review_status=status, invoice_n=n,
        )
        normal_docs.append(doc)
        docs.append(doc)
        n += 1

    # ── 2. Duplicates (40) ─────────────────────────────────────────────────
    for _ in range(SCENARIO_COUNTS["duplicate"]):
        src = random.choice(normal_docs)
        days_offset = random.randint(1, 29)
        src_date = date.fromisoformat(src["document_date"])
        dup_date = src_date + timedelta(days=days_offset)
        doc = _build_doc(
            f"doc_{n:05d}", client_code,
            src["vendor"], src["gl_account"], src["tax_code"],
            src["amount"], dup_date,
            "duplicate", review_status="ReadyToPost", invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 3. Weekend transactions (20) ───────────────────────────────────────
    for _ in range(SCENARIO_COUNTS["weekend"]):
        vendor, gl, tax, amin, amax = random.choice(pool)
        # Force amount > $500
        amt = _random_amount(max(amin, 501), max(amax, 600))
        ddate = _next_weekend_date()
        assert ddate.weekday() in (5, 6), f"Expected weekend, got {ddate}"
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, tax, amt, ddate,
            "weekend", review_status="ReadyToPost", invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 4. New vendor large amount (20) ───────────────────────────────────
    for i in range(SCENARIO_COUNTS["new_vendor"]):
        vendor = _new_vendor_name(i + first_doc_n)
        gl     = "Charges d'exploitation"
        tax    = "T"
        amt    = _random_amount(2100, 8000)
        ddate  = _random_date()
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, tax, amt, ddate,
            "new_vendor", review_status="ReadyToPost", invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 5. Round number (20) ───────────────────────────────────────────────
    round_amounts = [500.0, 1000.0, 2000.0, 5000.0]
    for _ in range(SCENARIO_COUNTS["round_number"]):
        vendor, gl, tax, amin, amax = random.choice(pool)
        amt   = random.choice(round_amounts)
        ddate = _random_date()
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, tax, amt, ddate,
            "round_number", review_status="ReadyToPost", invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 6. Meal receipts (20) ─────────────────────────────────────────────
    for _ in range(SCENARIO_COUNTS["meal"]):
        vendor, gl = random.choice(MEAL_VENDORS)
        amt   = _random_amount(30, 300)
        ddate = _random_date()
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, "M", amt, ddate,
            "meal", review_status="ReadyToPost", invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 7. Insurance invoices (20) ────────────────────────────────────────
    for _ in range(SCENARIO_COUNTS["insurance"]):
        vendor, gl, amin, amax = random.choice(INSURANCE_VENDORS)
        amt   = _random_amount(amin, amax)
        ddate = _random_date()
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, "I", amt, ddate,
            "insurance", review_status="ReadyToPost", invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 8. Low confidence (20) ────────────────────────────────────────────
    for _ in range(SCENARIO_COUNTS["low_confidence"]):
        vendor, gl, tax, amin, amax = random.choice(pool)
        amt   = _random_amount(amin, amax)
        ddate = _random_date()
        conf  = round(random.uniform(0.40, 0.65), 4)
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, tax, amt, ddate,
            "low_confidence",
            confidence=conf,
            review_status="NeedsReview",
            invoice_n=n,
        )
        docs.append(doc)
        n += 1

    # ── 9. Math mismatch (20) ─────────────────────────────────────────────
    for _ in range(SCENARIO_COUNTS["math_mismatch"]):
        vendor, gl, tax, amin, amax = random.choice(pool)
        # Use T code so GST/QST values in raw_result are meaningful
        tax   = "T"
        lo    = min(max(amin, 200), amax)
        amt   = _random_amount(lo, amax)
        ddate = _random_date()
        doc = _build_doc(
            f"doc_{n:05d}", client_code, vendor, gl, tax, amt, ddate,
            "math_mismatch",
            review_status="NeedsReview",
            mismatch=True,
            invoice_n=n,
        )
        docs.append(doc)
        n += 1

    assert len(docs) == 2000, f"Expected 2000 docs, got {len(docs)}"
    return docs


# ── DB helpers ────────────────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT OR IGNORE INTO documents (
    document_id, file_name, file_path, client_code,
    vendor, doc_type, amount, document_date,
    gl_account, tax_code, category, review_status,
    confidence, raw_result, created_at, updated_at,
    currency, subtotal, tax_total, extraction_method, ingest_source, fraud_flags,
    handwriting_low_confidence, handwriting_sample
) VALUES (
    :document_id, :file_name, :file_path, :client_code,
    :vendor, :doc_type, :amount, :document_date,
    :gl_account, :tax_code, :category, :review_status,
    :confidence, :raw_result, :created_at, :updated_at,
    :currency, :subtotal, :tax_total, :extraction_method, :ingest_source, :fraud_flags,
    :handwriting_low_confidence, :handwriting_sample
)
"""


def _get_next_doc_n(conn: sqlite3.Connection) -> int:
    """Return the next safe sequential doc number (above existing max)."""
    row = conn.execute(
        """
        SELECT MAX(CAST(SUBSTR(document_id, 5) AS INTEGER))
          FROM documents
         WHERE document_id LIKE 'doc_%'
           AND SUBSTR(document_id, 5) GLOB '[0-9]*'
        """
    ).fetchone()
    return (row[0] or 0) + 1


def _build_handwriting_raw_result(
    vendor: str,
    amount: float,
    tax_code: str,
    invoice_n: int,
    *,
    illegible_fields: list[str] | None = None,
    payment_method: str = "cash",
) -> str:
    """Build a raw_result JSON for handwriting test data."""
    bd = _breakdown(_d(str(amount)), tax_code)
    illegible = set(illegible_fields or [])

    return json.dumps({
        "vendor_name":    "illegible" if "vendor_name" in illegible else vendor,
        "amount":         None if "amount" in illegible else round(bd["subtotal"], 2),
        "date":           "illegible" if "date" in illegible else None,
        "gst_amount":     None if "gst_amount" in illegible else round(bd["gst"], 2),
        "qst_amount":     None if "qst_amount" in illegible else round(bd["qst"], 2),
        "total":          None if "total" in illegible else round(amount, 2),
        "payment_method": "illegible" if "payment_method" in illegible else payment_method,
        "notes":          random.choice(_HANDWRITING_NOTES_TEMPLATES),
        "field_confidence": {
            "vendor_name":    round(random.uniform(0.2, 0.5), 2) if "vendor_name" in illegible else round(random.uniform(0.7, 0.95), 2),
            "amount":         round(random.uniform(0.2, 0.5), 2) if "amount" in illegible else round(random.uniform(0.7, 0.95), 2),
            "date":           round(random.uniform(0.2, 0.5), 2) if "date" in illegible else round(random.uniform(0.7, 0.95), 2),
            "gst_amount":     round(random.uniform(0.2, 0.5), 2) if "gst_amount" in illegible else round(random.uniform(0.7, 0.95), 2),
            "qst_amount":     round(random.uniform(0.2, 0.5), 2) if "qst_amount" in illegible else round(random.uniform(0.7, 0.95), 2),
            "total":          round(random.uniform(0.2, 0.5), 2) if "total" in illegible else round(random.uniform(0.7, 0.95), 2),
            "payment_method": round(random.uniform(0.2, 0.5), 2) if "payment_method" in illegible else round(random.uniform(0.7, 0.95), 2),
            "notes":          round(random.uniform(0.6, 0.9), 2),
        },
        "invoice_number": f"HW-{invoice_n:06d}",
        "subtotal":       round(bd["subtotal"], 2),
        "tax_total":      round(bd["tax_total"], 2),
        "currency":       "CAD",
        "province":       "QC",
    }, ensure_ascii=False)


def _build_handwriting_doc(
    doc_id: str,
    client_code: str,
    vendor: str,
    gl_account: str,
    tax_code: str,
    amount: float,
    doc_date: date,
    scenario: str,
    *,
    confidence: float,
    illegible_fields: list[str] | None = None,
    payment_method: str = "cash",
    invoice_n: int = 0,
    handwriting_low_confidence: bool = False,
) -> dict[str, Any]:
    now = _utcnow()
    bd  = _breakdown(_d(str(amount)), tax_code)
    review_status = "NeedsReview" if handwriting_low_confidence else "ReadyToPost"

    return {
        "document_id":      doc_id,
        "file_name":        f"{doc_id}_handwritten.jpg",
        "file_path":        f"/test_data/{client_code}/{doc_id}_handwritten.jpg",
        "client_code":      client_code,
        "vendor":           vendor,
        "doc_type":         "receipt",
        "amount":           round(amount, 2),
        "document_date":    doc_date.isoformat(),
        "gl_account":       gl_account,
        "tax_code":         tax_code,
        "category":         "expense",
        "review_status":    review_status,
        "confidence":       confidence,
        "raw_result":       _build_handwriting_raw_result(
            vendor, amount, tax_code, invoice_n,
            illegible_fields=illegible_fields,
            payment_method=payment_method,
        ),
        "created_at":       now,
        "updated_at":       now,
        "currency":         "CAD",
        "subtotal":         bd["subtotal"],
        "tax_total":        bd["tax_total"],
        "extraction_method": "vision_handwriting",
        "ingest_source":    f"test:handwriting_{scenario}",
        "fraud_flags":      "[]",
        "handwriting_low_confidence": 1 if handwriting_low_confidence else 0,
        "handwriting_sample": 1,
    }


def generate_handwriting_samples(first_n: int) -> list[dict[str, Any]]:
    """
    Generate 500 handwritten receipt test scenarios.

    Distribution:
      200 fully legible       (confidence 0.75-0.95)
      150 partially illegible (1-2 illegible fields, confidence 0.55-0.74)
      100 mostly illegible    (3+ illegible fields, confidence 0.35-0.54)
       50 mixed print+handwriting (confidence 0.65-0.85)
    """
    docs: list[dict[str, Any]] = []
    n = first_n
    client_codes = list(CLIENTS.keys())
    _all_fields = ["vendor_name", "amount", "date", "gst_amount", "qst_amount", "total", "payment_method"]

    # 1. Fully legible (200)
    for _ in range(200):
        client = random.choice(client_codes)
        vendor, gl, tax, amin, amax = random.choice(HANDWRITING_VENDORS)
        amt = _random_amount(amin, amax)
        ddate = _random_date()
        conf = round(random.uniform(0.75, 0.95), 4)
        pmt = random.choice(HANDWRITING_PAYMENT_METHODS)
        doc = _build_handwriting_doc(
            f"doc_{n:05d}", client, vendor, gl, tax, amt, ddate,
            "legible", confidence=conf, payment_method=pmt,
            invoice_n=n, handwriting_low_confidence=False,
        )
        docs.append(doc)
        n += 1

    # 2. Partially illegible — 1-2 fields (150)
    for _ in range(150):
        client = random.choice(client_codes)
        vendor, gl, tax, amin, amax = random.choice(HANDWRITING_VENDORS)
        amt = _random_amount(amin, amax)
        ddate = _random_date()
        conf = round(random.uniform(0.55, 0.74), 4)
        num_illegible = random.choice([1, 2])
        illegible = random.sample(_all_fields, num_illegible)
        pmt = random.choice(HANDWRITING_PAYMENT_METHODS)
        doc = _build_handwriting_doc(
            f"doc_{n:05d}", client, vendor, gl, tax, amt, ddate,
            "partial_illegible", confidence=conf,
            illegible_fields=illegible, payment_method=pmt,
            invoice_n=n, handwriting_low_confidence=True,
        )
        docs.append(doc)
        n += 1

    # 3. Mostly illegible — 3+ fields (100)
    for _ in range(100):
        client = random.choice(client_codes)
        vendor, gl, tax, amin, amax = random.choice(HANDWRITING_VENDORS)
        amt = _random_amount(amin, amax)
        ddate = _random_date()
        conf = round(random.uniform(0.35, 0.54), 4)
        num_illegible = random.randint(3, len(_all_fields))
        illegible = random.sample(_all_fields, num_illegible)
        pmt = random.choice(HANDWRITING_PAYMENT_METHODS)
        doc = _build_handwriting_doc(
            f"doc_{n:05d}", client, vendor, gl, tax, amt, ddate,
            "mostly_illegible", confidence=conf,
            illegible_fields=illegible, payment_method=pmt,
            invoice_n=n, handwriting_low_confidence=True,
        )
        docs.append(doc)
        n += 1

    # 4. Mixed print and handwriting (50)
    for _ in range(50):
        client = random.choice(client_codes)
        vendor, gl, tax, amin, amax = random.choice(HANDWRITING_VENDORS)
        amt = _random_amount(amin, amax)
        ddate = _random_date()
        conf = round(random.uniform(0.65, 0.85), 4)
        pmt = random.choice(HANDWRITING_PAYMENT_METHODS)
        doc = _build_handwriting_doc(
            f"doc_{n:05d}", client, vendor, gl, tax, amt, ddate,
            "mixed_print_handwriting", confidence=conf,
            payment_method=pmt, invoice_n=n,
            handwriting_low_confidence=conf < 0.7,
        )
        docs.append(doc)
        n += 1

    assert len(docs) == 500, f"Expected 500 handwriting docs, got {len(docs)}"
    return docs


def _insert_docs(conn: sqlite3.Connection, docs: list[dict[str, Any]]) -> int:
    inserted = 0
    for i, doc in enumerate(docs, 1):
        cur = conn.execute(_INSERT_SQL, doc)
        inserted += cur.rowcount
        if i % 1000 == 0:
            conn.commit()
            print(f"  [{i:5d}/{len(docs)}] inserted so far: {inserted}")
    conn.commit()
    return inserted


# ── Bulk fraud detection (fast deterministic checks only) ─────────────────────

def _bulk_fraud_detection(all_docs: list[dict[str, Any]]) -> int:
    """
    Run fast deterministic fraud checks on all documents in batches of 500
    using a single DB connection.

    Checks run (all fast & deterministic):
      - duplicate_exact       — in-memory lookup by (vendor, client, amount)
      - weekend_transaction   — pure date check
      - new_vendor_large_amount — in-memory vendor set tracking
      - round_number_flag     — pure amount check (simplified: no vendor history needed)

    Skips slower vendor_amount_anomaly and vendor_timing_anomaly which
    require per-vendor historical queries.
    """
    from collections import defaultdict, Counter

    BATCH = 500
    total = len(all_docs)
    flagged = 0
    print(f"\nRunning bulk fraud detection on {total} documents (fast checks)...")

    # ── Build in-memory indexes for duplicate & new-vendor checks ──────
    # Key: (vendor_lower, client_lower, amount_rounded) → list of (doc_id, doc_date)
    dup_index: dict[tuple[str, str, float], list[tuple[str, date]]] = defaultdict(list)
    # Count how many approved/posted docs exist per (vendor, client)
    approved_vendor_counts: Counter[tuple[str, str]] = Counter()
    # Track vendor amount history for round_number check: (vendor, client) → [amounts]
    vendor_amounts: dict[tuple[str, str], list[float]] = defaultdict(list)

    _APPROVED_STATUSES = {"posted", "ready to post", "ready", "approved", "readytopost"}

    # Pre-parse all docs into a fast structure
    parsed: list[tuple[str, float | None, date | None, str, str]] = []
    for doc in all_docs:
        amount   = _safe_float(doc.get("amount"))
        doc_date = _parse_date(doc.get("document_date"))
        vendor   = str(doc.get("vendor") or "").strip().lower()
        client   = str(doc.get("client_code") or "").strip().lower()
        parsed.append((doc["document_id"], amount, doc_date, vendor, client))

    # First pass: build the duplicate index, vendor approval counts, and amount history
    for i, doc in enumerate(all_docs):
        doc_id, amount, doc_date, vendor, client = parsed[i]
        if amount and amount > 0 and doc_date:
            # Round to 2 decimals for matching (same as ABS(a-b) < 0.005)
            amt_key = round(amount, 2)
            dup_index[(vendor, client, amt_key)].append((doc_id, doc_date))
        if amount and amount > 0 and vendor:
            vendor_amounts[(vendor, client)].append(amount)
        status = str(doc.get("review_status") or "").lower()
        if vendor and status in _APPROVED_STATUSES:
            approved_vendor_counts[(vendor, client)] += 1

    # ── Process in batches, write fraud_flags to DB ────────────────────
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    _ensure_fraud_flags_column(conn)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for batch_start in range(0, total, BATCH):
        batch_end = min(batch_start + BATCH, total)
        updates: list[tuple[str, str, str]] = []

        for i in range(batch_start, batch_end):
            doc_id, amount, doc_date, vendor, client = parsed[i]

            if amount is None or amount <= 0 or doc_date is None:
                updates.append(("[]", now, doc_id))
                continue

            flags: list[dict[str, Any]] = []

            # ── duplicate_exact: same vendor+client+amount within 30 days ──
            amt_key = round(amount, 2)
            for other_id, other_date in dup_index.get((vendor, client, amt_key), []):
                if other_id == doc_id:
                    continue
                days_diff = abs((doc_date - other_date).days)
                if days_diff <= DUPLICATE_SAME_VENDOR_DAYS:
                    flags.append({
                        "rule":     "duplicate_exact",
                        "severity": "high",
                        "i18n_key": "fraud_duplicate_exact",
                        "params": {
                            "amount": f"${amount:,.2f}",
                            "vendor": vendor,
                            "days":   str(days_diff),
                            "doc_id": other_id,
                        },
                    })
                    break  # one duplicate flag is enough

            # ── weekend_transaction / holiday_transaction ──
            flags.extend(_rule_weekend_holiday(amount, doc_date))

            # ── new_vendor_large_amount ──
            if vendor and amount > NEW_VENDOR_LARGE_AMOUNT_LIMIT:
                # Exclude the current doc from approved count (mirrors
                # fraud_engine's exclude_doc_id in vendor history query)
                own_status = str(all_docs[i].get("review_status") or "").lower()
                prior_approved = approved_vendor_counts.get((vendor, client), 0)
                if own_status in _APPROVED_STATUSES:
                    prior_approved -= 1
                if prior_approved <= 0:
                    flags.append({
                        "rule":     "new_vendor_large_amount",
                        "severity": "high",
                        "i18n_key": "fraud_new_vendor_large",
                        "params": {
                            "vendor":    vendor,
                            "amount":    f"${amount:,.2f}",
                            "threshold": f"${NEW_VENDOR_LARGE_AMOUNT_LIMIT:,.0f}",
                        },
                    })

            # ── round_number_flag ──
            if vendor and amount > 0 and amount % 500 == 0 and amount == int(amount):
                hist = vendor_amounts.get((vendor, client), [])
                # Need at least 5 other amounts (exclude self)
                other_amounts = [a for a in hist if abs(a - amount) > 0.005]
                if len(other_amounts) >= 5:
                    mu = sum(other_amounts) / len(other_amounts)
                    if mu > 0:
                        var = sum((v - mu) ** 2 for v in other_amounts) / (len(other_amounts) - 1)
                        std = math.sqrt(var)
                        if std / mu > 0.10:
                            flags.append({
                                "rule":     "round_number_flag",
                                "severity": "low",
                                "i18n_key": "fraud_round_number",
                                "params":   {"amount": f"${amount:,.0f}"},
                            })

            updates.append((json.dumps(flags, ensure_ascii=False), now, doc_id))
            if flags:
                flagged += 1

        conn.executemany(
            "UPDATE documents SET fraud_flags = ?, updated_at = ? WHERE document_id = ?",
            updates,
        )
        conn.commit()

        processed = batch_end
        if processed % 1000 == 0 or processed == total:
            print(f"  [{processed:5d}/{total}]  flagged so far: {flagged}")

    conn.close()
    return flagged


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Ensure required columns exist (fraud_engine and handwriting also check, but do it here too)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
    for col, typedef in [
        ("fraud_flags", "TEXT"),
        ("handwriting_low_confidence", "INTEGER NOT NULL DEFAULT 0"),
        ("handwriting_sample", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE documents ADD COLUMN {col} {typedef}")
    conn.commit()

    # Remove any previously generated test data so re-runs are idempotent.
    existing = conn.execute(
        "SELECT COUNT(*) FROM documents WHERE ingest_source LIKE 'test:%'"
    ).fetchone()[0]
    if existing:
        conn.execute("DELETE FROM documents WHERE ingest_source LIKE 'test:%'")
        conn.commit()
        print(f"Removed {existing} existing test documents (re-generating)")

    first_n = _get_next_doc_n(conn)
    print(f"Starting doc IDs at doc_{first_n:05d}")

    # ── Generate all docs ─────────────────────────────────────────────────
    all_docs: list[dict[str, Any]] = []
    n = first_n
    for client_code in CLIENTS:
        client_docs = generate_client_docs(client_code, n)
        all_docs.extend(client_docs)
        n += len(client_docs)
        print(f"  Generated {len(client_docs)} docs for {client_code}")

    # ── Generate handwriting samples ─────────────────────────────────────
    hw_docs = generate_handwriting_samples(n)
    all_docs.extend(hw_docs)
    n += len(hw_docs)
    print(f"  Generated {len(hw_docs)} handwriting samples")

    print(f"\nTotal generated: {len(all_docs)}")

    # ── Insert into DB ────────────────────────────────────────────────────
    total_inserted = _insert_docs(conn, all_docs)
    conn.close()
    print(f"Inserted: {total_inserted} rows (skipped existing: {len(all_docs) - total_inserted})")

    # ── Run bulk fraud detection (fast deterministic checks only) ────────
    flagged = _bulk_fraud_detection(all_docs)
    print(f"\nFraud detection complete - {flagged}/{len(all_docs)} documents flagged")

    # ── Report by scenario ────────────────────────────────────────────────
    print("\n-- Final counts by scenario (all clients) -------------------------")
    by_scenario: dict[str, int] = {}
    for d in all_docs:
        s = d["ingest_source"].replace("test:", "")
        by_scenario[s] = by_scenario.get(s, 0) + 1

    total = 0
    for scenario, count in sorted(by_scenario.items()):
        expected_per_client = SCENARIO_COUNTS.get(scenario, "?")
        expected_total = (
            expected_per_client * len(CLIENTS)
            if isinstance(expected_per_client, int) else "?"
        )
        print(f"  {scenario:<20s}  {count:4d}  (expected {expected_total})")
        if isinstance(expected_total, int):
            total += count

    print(f"  {'TOTAL':<20s}  {sum(by_scenario.values()):4d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
