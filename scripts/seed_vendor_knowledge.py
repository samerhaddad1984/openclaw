#!/usr/bin/env python3
"""
seed_vendor_knowledge.py

Pre-loads 1,000 common Quebec vendors into:
  1. learning_memory_patterns  – one row per vendor (GL account + tax code)
  2. ai_response_cache         – one pre-built cache entry per vendor so the
                                 first classify_document call is served from
                                 cache (zero HTTP cost).

Run:
    python scripts/seed_vendor_knowledge.py
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── Seed constants ────────────────────────────────────────────────────────────

SEED_EVENT_TYPE    = "posted_successfully"
SEED_CLIENT_CODE   = "__global__"
SEED_OUTCOME_COUNT = 10
SEED_SUCCESS_COUNT = 9        # 9/10 = 90 % ≥ 85 % threshold → memory short-circuit fires
SEED_CONFIDENCE    = 0.90
SEED_TASK_TYPE     = "classify_document"
SEED_CACHE_TTL_DAYS = 30      # matches _CACHE_TTL_DAYS["classify_document"] in ai_router.py

# Standard prompt used when building deterministic cache keys for vendor classification.
# Any caller that issues  ai_router.call("classify_document", _SEED_PROMPT, {"vendor": name})
# will receive a cache hit for every seeded vendor.
_SEED_PROMPT = (
    "Classify this accounting document and suggest "
    "the appropriate GL account and tax code."
)

# ── Vendor catalogue: (display_name, gl_account, tax_code) ───────────────────
# 1,000 common Quebec vendors across 50+ GL categories.

VENDORS: list[tuple[str, str, str]] = [

    # ── Matériaux et fournitures ─ T (20) ──────────────────────────────────
    ("Rona",                          "Matériaux et fournitures", "T"),
    ("Home Depot",                    "Matériaux et fournitures", "T"),
    ("Canac",                         "Matériaux et fournitures", "T"),
    ("Patrick Morin",                 "Matériaux et fournitures", "T"),
    ("BMR",                           "Matériaux et fournitures", "T"),
    ("Réno-Dépôt",                    "Matériaux et fournitures", "T"),
    ("Canadian Tire",                 "Matériaux et fournitures", "T"),
    ("Kent Building Supplies",        "Matériaux et fournitures", "T"),
    ("Home Hardware",                 "Matériaux et fournitures", "T"),
    ("Lowe's",                        "Matériaux et fournitures", "T"),
    ("Quincaillerie Richelieu",       "Matériaux et fournitures", "T"),
    ("Timber Mart",                   "Matériaux et fournitures", "T"),
    ("Richelieu Hardware",            "Matériaux et fournitures", "T"),
    ("Westburne",                     "Matériaux et fournitures", "T"),
    ("Rexel Canada",                  "Matériaux et fournitures", "T"),
    ("Fastenal Canada",               "Matériaux et fournitures", "T"),
    ("Groupe Deschênes",              "Matériaux et fournitures", "T"),
    ("Entrepôt du peintre",           "Matériaux et fournitures", "T"),
    ("Dulux Peintures",               "Matériaux et fournitures", "T"),
    ("Ameublement Tanguay",           "Matériaux et fournitures", "T"),

    # ── Carburant et huile ─ T (10) ────────────────────────────────────────
    ("Ultramar",                      "Carburant et huile", "T"),
    ("Esso",                          "Carburant et huile", "T"),
    ("Petro-Canada",                  "Carburant et huile", "T"),
    ("Shell",                         "Carburant et huile", "T"),
    ("Irving Oil",                    "Carburant et huile", "T"),
    ("Pioneer Pétrole",               "Carburant et huile", "T"),
    ("Couche-Tard Carburant",         "Carburant et huile", "T"),
    ("Fas Gas",                       "Carburant et huile", "T"),
    ("Sunoco",                        "Carburant et huile", "T"),
    ("Sonic Fuel",                    "Carburant et huile", "T"),

    # ── Télécommunications ─ T (10) ────────────────────────────────────────
    ("Bell",                          "Télécommunications", "T"),
    ("Vidéotron",                     "Télécommunications", "T"),
    ("Rogers",                        "Télécommunications", "T"),
    ("Telus",                         "Télécommunications", "T"),
    ("Fizz",                          "Télécommunications", "T"),
    ("Koodo",                         "Télécommunications", "T"),
    ("Freedom Mobile",                "Télécommunications", "T"),
    ("Eastlink",                      "Télécommunications", "T"),
    ("Tbaytel",                       "Télécommunications", "T"),
    ("Distributel",                   "Télécommunications", "T"),

    # ── Électricité et gaz ─ E (5) ─────────────────────────────────────────
    ("Hydro-Québec",                  "Électricité et gaz", "E"),
    ("Énergir",                       "Électricité et gaz", "E"),
    ("Gazifère",                      "Électricité et gaz", "E"),
    ("Hydro One",                     "Électricité et gaz", "E"),
    ("Enbridge Gas",                  "Électricité et gaz", "E"),

    # ── Frais bancaires ─ E (10) ───────────────────────────────────────────
    ("Desjardins frais",              "Frais bancaires", "E"),
    ("BMO frais",                     "Frais bancaires", "E"),
    ("TD frais",                      "Frais bancaires", "E"),
    ("RBC frais",                     "Frais bancaires", "E"),
    ("Banque Nationale frais",        "Frais bancaires", "E"),
    ("CIBC frais",                    "Frais bancaires", "E"),
    ("Scotiabank frais",              "Frais bancaires", "E"),
    ("Banque Laurentienne frais",     "Frais bancaires", "E"),
    ("HSBC frais",                    "Frais bancaires", "E"),
    ("Caisse Populaire frais",        "Frais bancaires", "E"),

    # ── Assurances ─ I (10) ────────────────────────────────────────────────
    ("Intact Assurance",              "Assurances", "I"),
    ("SSQ",                           "Assurances", "I"),
    ("La Capitale",                   "Assurances", "I"),
    ("Industrielle Alliance",         "Assurances", "I"),
    ("Desjardins Assurances",         "Assurances", "I"),
    ("Promutuel",                     "Assurances", "I"),
    ("Belairdirect",                  "Assurances", "I"),
    ("Aviva Canada",                  "Assurances", "I"),
    ("Economical Insurance",          "Assurances", "I"),
    ("La Personnelle",                "Assurances", "I"),

    # ── Fournitures de bureau ─ T (10) ─────────────────────────────────────
    ("Bureau en Gros",                "Fournitures de bureau", "T"),
    ("Costco Business",               "Fournitures de bureau", "T"),
    ("Amazon Business",               "Fournitures de bureau", "T"),
    ("Staples Canada",                "Fournitures de bureau", "T"),
    ("Bureau Plus",                   "Fournitures de bureau", "T"),
    ("Lyreco",                        "Fournitures de bureau", "T"),
    ("Office Depot Canada",           "Fournitures de bureau", "T"),
    ("Monk Office",                   "Fournitures de bureau", "T"),
    ("Grand & Toy",                   "Fournitures de bureau", "T"),
    ("Clément",                       "Fournitures de bureau", "T"),

    # ── Logiciels et abonnements ─ T (15) ──────────────────────────────────
    ("Adobe",                         "Logiciels et abonnements", "T"),
    ("Microsoft 365",                 "Logiciels et abonnements", "T"),
    ("Google Workspace",              "Logiciels et abonnements", "T"),
    ("Shopify",                       "Logiciels et abonnements", "T"),
    ("Sage 50",                       "Logiciels et abonnements", "T"),
    ("QuickBooks Online",             "Logiciels et abonnements", "T"),
    ("Slack",                         "Logiciels et abonnements", "T"),
    ("Zoom",                          "Logiciels et abonnements", "T"),
    ("Dropbox",                       "Logiciels et abonnements", "T"),
    ("Salesforce",                    "Logiciels et abonnements", "T"),
    ("HubSpot",                       "Logiciels et abonnements", "T"),
    ("DocuSign",                      "Logiciels et abonnements", "T"),
    ("Mailchimp",                     "Logiciels et abonnements", "T"),
    ("Asana",                         "Logiciels et abonnements", "T"),
    ("Notion",                        "Logiciels et abonnements", "T"),

    # ── Transport et déplacements ─ T (20) ─────────────────────────────────
    ("VIA Rail",                      "Transport et déplacements", "T"),
    ("Air Canada",                    "Transport et déplacements", "T"),
    ("STM",                           "Transport et déplacements", "T"),
    ("Stationnement",                 "Transport et déplacements", "T"),
    ("Uber",                          "Transport et déplacements", "T"),
    ("Porter Airlines",               "Transport et déplacements", "T"),
    ("Air Transat",                   "Transport et déplacements", "T"),
    ("Avis Location",                 "Transport et déplacements", "T"),
    ("Budget Location",               "Transport et déplacements", "T"),
    ("Hertz Canada",                  "Transport et déplacements", "T"),
    ("STLaval",                       "Transport et déplacements", "T"),
    ("Réseau de transport de la Capitale", "Transport et déplacements", "T"),
    ("exo Transport",                 "Transport et déplacements", "T"),
    ("Greyhound Canada",              "Transport et déplacements", "T"),
    ("National Car Rental",           "Transport et déplacements", "T"),
    ("FedEx",                         "Transport et déplacements", "T"),
    ("UPS Canada",                    "Transport et déplacements", "T"),
    ("Purolator",                     "Transport et déplacements", "T"),
    ("Postes Canada",                 "Transport et déplacements", "T"),
    ("Intelcom",                      "Transport et déplacements", "T"),

    # ── Repas d'affaires ─ M (30) ──────────────────────────────────────────
    ("Restaurant",                    "Repas d'affaires", "M"),
    ("Café",                          "Repas d'affaires", "M"),
    ("Brasserie",                     "Repas d'affaires", "M"),
    ("Tim Hortons",                   "Repas d'affaires", "M"),
    ("McDonald's",                    "Repas d'affaires", "M"),
    ("Subway",                        "Repas d'affaires", "M"),
    ("A&W",                           "Repas d'affaires", "M"),
    ("Harvey's",                      "Repas d'affaires", "M"),
    ("St-Hubert",                     "Repas d'affaires", "M"),
    ("Cage aux Sports",               "Repas d'affaires", "M"),
    ("Bâton Rouge",                   "Repas d'affaires", "M"),
    ("Scores Rôtisserie",             "Repas d'affaires", "M"),
    ("Mikes",                         "Repas d'affaires", "M"),
    ("Valentines",                    "Repas d'affaires", "M"),
    ("La Belle Province",             "Repas d'affaires", "M"),
    ("Pizza Hut",                     "Repas d'affaires", "M"),
    ("Dominos Pizza",                 "Repas d'affaires", "M"),
    ("Second Cup",                    "Repas d'affaires", "M"),
    ("Van Houtte",                    "Repas d'affaires", "M"),
    ("Starbucks",                     "Repas d'affaires", "M"),
    ("Montana's",                     "Repas d'affaires", "M"),
    ("The Keg",                       "Repas d'affaires", "M"),
    ("Benny & Co",                    "Repas d'affaires", "M"),
    ("Boston Pizza",                  "Repas d'affaires", "M"),
    ("Burger King",                   "Repas d'affaires", "M"),
    ("KFC",                           "Repas d'affaires", "M"),
    ("Poulet Rouge",                  "Repas d'affaires", "M"),
    ("Dunkin",                        "Repas d'affaires", "M"),
    ("Pacini",                        "Repas d'affaires", "M"),
    ("La Cremière",                   "Repas d'affaires", "M"),

    # ── Frais médicaux ─ E (10) ────────────────────────────────────────────
    ("Médecin",                       "Frais médicaux", "E"),
    ("Dentiste",                      "Frais médicaux", "E"),
    ("Pharmacie",                     "Frais médicaux", "E"),
    ("Pharmaprix",                    "Frais médicaux", "E"),
    ("Jean Coutu",                    "Frais médicaux", "E"),
    ("Brunet Pharmacie",              "Frais médicaux", "E"),
    ("Uniprix",                       "Frais médicaux", "E"),
    ("Proxim Pharmacie",              "Frais médicaux", "E"),
    ("Clinique médicale",             "Frais médicaux", "E"),
    ("Optométriste",                  "Frais médicaux", "E"),

    # ── Publicité et marketing ─ T (10) ───────────────────────────────────
    ("Facebook Ads",                  "Publicité et marketing", "T"),
    ("Google Ads",                    "Publicité et marketing", "T"),
    ("LinkedIn Ads",                  "Publicité et marketing", "T"),
    ("Twitter Ads",                   "Publicité et marketing", "T"),
    ("TikTok Ads",                    "Publicité et marketing", "T"),
    ("Kijiji Annonces",               "Publicité et marketing", "T"),
    ("LesPAC",                        "Publicité et marketing", "T"),
    ("Publisac",                      "Publicité et marketing", "T"),
    ("Transcontinental Pub",          "Publicité et marketing", "T"),
    ("Cogeco Pub",                    "Publicité et marketing", "T"),

    # ── Honoraires professionnels ─ T (30) ────────────────────────────────
    ("Avocat",                        "Honoraires professionnels", "T"),
    ("Notaire",                       "Honoraires professionnels", "T"),
    ("Consultant",                    "Honoraires professionnels", "T"),
    ("Comptable CPA",                 "Honoraires professionnels", "T"),
    ("Architecte",                    "Honoraires professionnels", "T"),
    ("Ingénieur",                     "Honoraires professionnels", "T"),
    ("Arpenteur-géomètre",            "Honoraires professionnels", "T"),
    ("Expert en sinistres",           "Honoraires professionnels", "T"),
    ("Psychologue",                   "Honoraires professionnels", "T"),
    ("Physiothérapeute",              "Honoraires professionnels", "T"),
    ("Courtier immobilier",           "Honoraires professionnels", "T"),
    ("Courtier hypothécaire",         "Honoraires professionnels", "T"),
    ("Planificateur financier",       "Honoraires professionnels", "T"),
    ("Fiscaliste",                    "Honoraires professionnels", "T"),
    ("Traducteur",                    "Honoraires professionnels", "T"),
    ("Graphiste",                     "Honoraires professionnels", "T"),
    ("Développeur web",               "Honoraires professionnels", "T"),
    ("Agence de marketing",           "Honoraires professionnels", "T"),
    ("Agence RH",                     "Honoraires professionnels", "T"),
    ("Chasseur de têtes",             "Honoraires professionnels", "T"),
    ("Formateur",                     "Honoraires professionnels", "T"),
    ("Coach d'affaires",              "Honoraires professionnels", "T"),
    ("Expert-comptable",              "Honoraires professionnels", "T"),
    ("Vérificateur",                  "Honoraires professionnels", "T"),
    ("Conseiller juridique",          "Honoraires professionnels", "T"),
    ("Huissier",                      "Honoraires professionnels", "T"),
    ("Technicien juridique",          "Honoraires professionnels", "T"),
    ("Médiateur",                     "Honoraires professionnels", "T"),
    ("Arbitre",                       "Honoraires professionnels", "T"),
    ("Arpenteur",                     "Honoraires professionnels", "T"),

    # ── Location d'équipement ─ T (10) ────────────────────────────────────
    ("Sunbelt Rentals",               "Location d'équipement", "T"),
    ("RSC Equipment Rental",          "Location d'équipement", "T"),
    ("Équipement Bisson",             "Location d'équipement", "T"),
    ("Totem Équipement",              "Location d'équipement", "T"),
    ("Location Brisson",              "Location d'équipement", "T"),
    ("Maxim Crane",                   "Location d'équipement", "T"),
    ("Battlefield Equipment",         "Location d'équipement", "T"),
    ("Hewden",                        "Location d'équipement", "T"),
    ("Briggs Equipment",              "Location d'équipement", "T"),
    ("Finning International",         "Location d'équipement", "T"),

    # ── Épiceries et alimentation ─ Z/T (25) ─────────────────────────────
    ("IGA",                           "Épicerie et fournitures alimentaires", "Z"),
    ("Metro Épicerie",                "Épicerie et fournitures alimentaires", "Z"),
    ("Maxi",                          "Épicerie et fournitures alimentaires", "Z"),
    ("Super C",                       "Épicerie et fournitures alimentaires", "Z"),
    ("Provigo",                       "Épicerie et fournitures alimentaires", "Z"),
    ("Costco",                        "Épicerie et fournitures alimentaires", "T"),
    ("Marché Atwater",                "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Jean-Talon",             "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Central Montréal",       "Épicerie et fournitures alimentaires", "Z"),
    ("Avril Supermarché Santé",       "Épicerie et fournitures alimentaires", "Z"),
    ("Rachelle-Béry Bio",             "Épicerie et fournitures alimentaires", "Z"),
    ("Intermarché Québec",            "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Adonis",                 "Épicerie et fournitures alimentaires", "Z"),
    ("PA Supermarché",                "Épicerie et fournitures alimentaires", "Z"),
    ("Maxi & Cie Alimentation",       "Épicerie et fournitures alimentaires", "Z"),
    ("Metro Plus Épicerie",           "Épicerie et fournitures alimentaires", "Z"),
    ("IGA Extra",                     "Épicerie et fournitures alimentaires", "Z"),
    ("Loblaw Companies",              "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Tau",                    "Épicerie et fournitures alimentaires", "Z"),
    ("Club Entrepôt",                 "Épicerie et fournitures alimentaires", "T"),
    ("Walmart Épicerie",              "Épicerie et fournitures alimentaires", "Z"),
    ("Dollarama Alimentaire",         "Épicerie et fournitures alimentaires", "Z"),
    ("Distribution Aubut",            "Épicerie et fournitures alimentaires", "Z"),
    ("Mayrand Food Depot",            "Épicerie et fournitures alimentaires", "Z"),
    ("Marché 440 Laval",              "Épicerie et fournitures alimentaires", "Z"),

    # ── Quincaillerie et équipement ─ T (20) ─────────────────────────────
    ("Canadian Tire Équipement",      "Équipement et outillage", "T"),
    ("Princess Auto",                 "Équipement et outillage", "T"),
    ("Fastenal Outillage",            "Équipement et outillage", "T"),
    ("Guilbault Équipement",          "Équipement et outillage", "T"),
    ("Strongco Équipement",           "Équipement et outillage", "T"),
    ("Hewitt Équipement",             "Équipement et outillage", "T"),
    ("NAPA Auto Parts",               "Équipement et outillage", "T"),
    ("UAP Inc",                       "Équipement et outillage", "T"),
    ("Acklands-Grainger",             "Équipement et outillage", "T"),
    ("Quincaillerie Beaubien",        "Équipement et outillage", "T"),
    ("Quincaillerie Notre-Dame",      "Équipement et outillage", "T"),
    ("Grainger Canada",               "Équipement et outillage", "T"),
    ("KMS Tools Québec",              "Équipement et outillage", "T"),
    ("Hilti Canada",                  "Équipement et outillage", "T"),
    ("Stanley Black & Decker CA",     "Équipement et outillage", "T"),
    ("DeWalt Canada",                 "Équipement et outillage", "T"),
    ("Makita Canada",                 "Équipement et outillage", "T"),
    ("Milwaukee Tool Canada",         "Équipement et outillage", "T"),
    ("Snap-on Tools Canada",          "Équipement et outillage", "T"),
    ("Bosch Outillage Canada",        "Équipement et outillage", "T"),

    # ── Vêtements et EPI ─ T (20) ────────────────────────────────────────
    ("Mark's Work Wearhouse",         "Équipements de protection individuelle", "T"),
    ("Wolseley Canada",               "Équipements de protection individuelle", "T"),
    ("Cintas Canada",                 "Équipements de protection individuelle", "T"),
    ("Aramark Canada",                "Équipements de protection individuelle", "T"),
    ("Safetyline",                    "Équipements de protection individuelle", "T"),
    ("Sylprotec",                     "Équipements de protection individuelle", "T"),
    ("Magasins Latulippe",            "Équipements de protection individuelle", "T"),
    ("SPI Santé Sécurité",            "Équipements de protection individuelle", "T"),
    ("Levitt-Safety Canada",          "Équipements de protection individuelle", "T"),
    ("3M Sécurité Canada",            "Équipements de protection individuelle", "T"),
    ("Honeywell Safety Canada",       "Équipements de protection individuelle", "T"),
    ("Uline Canada EPI",              "Équipements de protection individuelle", "T"),
    ("Ansell Canada",                 "Équipements de protection individuelle", "T"),
    ("MSA Safety Canada",             "Équipements de protection individuelle", "T"),
    ("Dupont Safety Canada",          "Équipements de protection individuelle", "T"),
    ("Groupe Sécurité Plus",          "Équipements de protection individuelle", "T"),
    ("Bunzl Canada EPI",              "Équipements de protection individuelle", "T"),
    ("Vêtements Supérieur Québec",    "Équipements de protection individuelle", "T"),
    ("Uniformes Bolduc Ltée",         "Équipements de protection individuelle", "T"),
    ("Vêtements de travail JP",       "Équipements de protection individuelle", "T"),

    # ── Hôtels et hébergement ─ T (20) ───────────────────────────────────
    ("Marriott Montréal",             "Frais de voyage et hébergement", "T"),
    ("Hilton Québec",                 "Frais de voyage et hébergement", "T"),
    ("Delta Hotels Montréal",         "Frais de voyage et hébergement", "T"),
    ("Fairmont Le Château Frontenac", "Frais de voyage et hébergement", "T"),
    ("Best Western Québec",           "Frais de voyage et hébergement", "T"),
    ("Comfort Inn Québec",            "Frais de voyage et hébergement", "T"),
    ("Holiday Inn Montréal",          "Frais de voyage et hébergement", "T"),
    ("Hôtel Le Germain",              "Frais de voyage et hébergement", "T"),
    ("Hôtel Bonaventure Montréal",    "Frais de voyage et hébergement", "T"),
    ("Auberge Saint-Gabriel",         "Frais de voyage et hébergement", "T"),
    ("InterContinental Montréal",     "Frais de voyage et hébergement", "T"),
    ("Novotel Montréal",              "Frais de voyage et hébergement", "T"),
    ("Hyatt Regency Montréal",        "Frais de voyage et hébergement", "T"),
    ("Hôtel Le Crystal Montréal",     "Frais de voyage et hébergement", "T"),
    ("Sheraton Laval",                "Frais de voyage et hébergement", "T"),
    ("Hôtel Palace Royal Québec",     "Frais de voyage et hébergement", "T"),
    ("Château Laurier Québec",        "Frais de voyage et hébergement", "T"),
    ("Alt Hôtel Montréal",            "Frais de voyage et hébergement", "T"),
    ("Sandman Hôtel Montréal",        "Frais de voyage et hébergement", "T"),
    ("Travelodge Québec",             "Frais de voyage et hébergement", "T"),

    # ── Location de véhicules ─ T (20) ───────────────────────────────────
    ("Enterprise Rent-A-Car",         "Location de véhicules", "T"),
    ("Hertz Location",                "Location de véhicules", "T"),
    ("Avis Location Auto",            "Location de véhicules", "T"),
    ("Budget Location Auto",          "Location de véhicules", "T"),
    ("Discount Location Auto",        "Location de véhicules", "T"),
    ("Via Route Location",            "Location de véhicules", "T"),
    ("National Location Auto",        "Location de véhicules", "T"),
    ("Alamo Rent A Car Québec",       "Location de véhicules", "T"),
    ("Europcar Montréal",             "Location de véhicules", "T"),
    ("Turo Québec",                   "Location de véhicules", "T"),
    ("Location Laval Auto",           "Location de véhicules", "T"),
    ("Location Jean Légaré",          "Location de véhicules", "T"),
    ("Communauto",                    "Location de véhicules", "T"),
    ("Car2go Montréal",               "Location de véhicules", "T"),
    ("Sixt Location Québec",          "Location de véhicules", "T"),
    ("Dollar Rent A Car QC",          "Location de véhicules", "T"),
    ("Thrifty Location Auto",         "Location de véhicules", "T"),
    ("Location Pelletier",            "Location de véhicules", "T"),
    ("Location Sauvageau",            "Location de véhicules", "T"),
    ("FlexCar Québec",                "Location de véhicules", "T"),

    # ── Matériel informatique ─ T (20) ───────────────────────────────────
    ("Best Buy Affaires",             "Matériel informatique", "T"),
    ("Bureau en Gros Tech",           "Matériel informatique", "T"),
    ("Dell Canada",                   "Matériel informatique", "T"),
    ("Lenovo Canada",                 "Matériel informatique", "T"),
    ("Apple Store Montréal",          "Matériel informatique", "T"),
    ("HP Canada",                     "Matériel informatique", "T"),
    ("ASUS Canada",                   "Matériel informatique", "T"),
    ("Microsoft Store Canada",        "Matériel informatique", "T"),
    ("CDW Canada",                    "Matériel informatique", "T"),
    ("Softchoice Canada",             "Matériel informatique", "T"),
    ("Insight Canada",                "Matériel informatique", "T"),
    ("Compugen Inc",                  "Matériel informatique", "T"),
    ("Broccolini TI",                 "Matériel informatique", "T"),
    ("SYNNEX Canada",                 "Matériel informatique", "T"),
    ("Ingram Micro Canada",           "Matériel informatique", "T"),
    ("D&H Canada",                    "Matériel informatique", "T"),
    ("Memory Express Québec",         "Matériel informatique", "T"),
    ("Canada Computers Montréal",     "Matériel informatique", "T"),
    ("Micro Bytes Informatique",      "Matériel informatique", "T"),
    ("Info-Tech Québec",              "Matériel informatique", "T"),

    # ── Papeterie et imprimerie ─ T (15) ─────────────────────────────────
    ("Impression Drummond",           "Impression et papeterie", "T"),
    ("Minuteman Press Québec",        "Impression et papeterie", "T"),
    ("FedEx Office Montréal",         "Impression et papeterie", "T"),
    ("Imprimerie Solisco",            "Impression et papeterie", "T"),
    ("Imprimerie Transcontinental",   "Impression et papeterie", "T"),
    ("Copies Express Montréal",       "Impression et papeterie", "T"),
    ("Imprimerie Marquis",            "Impression et papeterie", "T"),
    ("PrintFleet Canada",             "Impression et papeterie", "T"),
    ("Staples Copy Centre",           "Impression et papeterie", "T"),
    ("Imprimerie Gaspésie",           "Impression et papeterie", "T"),
    ("Papeterie Saint-Laurent",       "Impression et papeterie", "T"),
    ("Reliure Plus Montréal",         "Impression et papeterie", "T"),
    ("Cartouches Certifiées QC",      "Impression et papeterie", "T"),
    ("Impression Direct Laval",       "Impression et papeterie", "T"),
    ("Bureau et Papeterie Express",   "Impression et papeterie", "T"),

    # ── Services de nettoyage ─ T (15) ───────────────────────────────────
    ("Molly Maid Québec",             "Entretien et nettoyage", "T"),
    ("ServiceMaster Canada",          "Entretien et nettoyage", "T"),
    ("Jan-Pro Québec",                "Entretien et nettoyage", "T"),
    ("Entretien Ménager GDL",         "Entretien et nettoyage", "T"),
    ("Services Ménagers Roy",         "Entretien et nettoyage", "T"),
    ("Nettoyage Impérial Montréal",   "Entretien et nettoyage", "T"),
    ("GDI Services aux immeubles",    "Entretien et nettoyage", "T"),
    ("Bee-Clean Building Maintenance","Entretien et nettoyage", "T"),
    ("C&W Services Canada",           "Entretien et nettoyage", "T"),
    ("Sodexo Entretien Canada",       "Entretien et nettoyage", "T"),
    ("Pro-Net Nettoyage Inc",         "Entretien et nettoyage", "T"),
    ("Multi-Nettoyage Québec",        "Entretien et nettoyage", "T"),
    ("Entretien Deschênes Inc",       "Entretien et nettoyage", "T"),
    ("Groupe Distinction Entretien",  "Entretien et nettoyage", "T"),
    ("Nettoyage Précision Ltée",      "Entretien et nettoyage", "T"),

    # ── Livraison et courrier ─ T (20) ───────────────────────────────────
    ("Purolator Courrier",            "Frais de livraison et courrier", "T"),
    ("FedEx Livraison Canada",        "Frais de livraison et courrier", "T"),
    ("UPS Livraison Canada",          "Frais de livraison et courrier", "T"),
    ("Postes Canada Courrier",        "Frais de livraison et courrier", "T"),
    ("DHL Express Canada",            "Frais de livraison et courrier", "T"),
    ("Canpar Express",                "Frais de livraison et courrier", "T"),
    ("Dicom Transport",               "Frais de livraison et courrier", "T"),
    ("Day & Ross Freight",            "Frais de livraison et courrier", "T"),
    ("Loomis Express",                "Frais de livraison et courrier", "T"),
    ("Intelcom Livraison",            "Frais de livraison et courrier", "T"),
    ("ICS Courier Canada",            "Frais de livraison et courrier", "T"),
    ("Nationex Courrier",             "Frais de livraison et courrier", "T"),
    ("Sameday Worldwide",             "Frais de livraison et courrier", "T"),
    ("TransForce TFI",                "Frais de livraison et courrier", "T"),
    ("Vitran Express",                "Frais de livraison et courrier", "T"),
    ("Midland Transport QC",          "Frais de livraison et courrier", "T"),
    ("Robert Transport",              "Frais de livraison et courrier", "T"),
    ("Groupe Guilbault Transport",    "Frais de livraison et courrier", "T"),
    ("Transport Morneau",             "Frais de livraison et courrier", "T"),
    ("Messageries Dynamiques",        "Frais de livraison et courrier", "T"),

    # ── Services financiers ─ E (20) ─────────────────────────────────────
    ("PayPal frais",                  "Frais de traitement des paiements", "E"),
    ("Stripe frais",                  "Frais de traitement des paiements", "E"),
    ("Square frais",                  "Frais de traitement des paiements", "E"),
    ("Moneris frais",                 "Frais de traitement des paiements", "E"),
    ("Desjardins Paiement",           "Frais de traitement des paiements", "E"),
    ("Global Payments Canada",        "Frais de traitement des paiements", "E"),
    ("TD Merchant Services",          "Frais de traitement des paiements", "E"),
    ("Chase Paymentech Canada",       "Frais de traitement des paiements", "E"),
    ("Paysafe Canada",                "Frais de traitement des paiements", "E"),
    ("Helcim Inc",                    "Frais de traitement des paiements", "E"),
    ("Lightspeed Payments",           "Frais de traitement des paiements", "E"),
    ("Clover Canada Frais",           "Frais de traitement des paiements", "E"),
    ("Shopify Payments Frais",        "Frais de traitement des paiements", "E"),
    ("Interac Frais",                 "Frais de traitement des paiements", "E"),
    ("Bambora Frais Canada",          "Frais de traitement des paiements", "E"),
    ("WePay Canada Frais",            "Frais de traitement des paiements", "E"),
    ("Braintree Payments Frais",      "Frais de traitement des paiements", "E"),
    ("Adyen Canada Frais",            "Frais de traitement des paiements", "E"),
    ("Worldpay Canada",               "Frais de traitement des paiements", "E"),
    ("Nuvei Corporation Frais",       "Frais de traitement des paiements", "E"),

    # ── Formation et éducation ─ T (20) ──────────────────────────────────
    ("Udemy Formation",               "Formation et développement professionnel", "T"),
    ("Coursera Formation",            "Formation et développement professionnel", "T"),
    ("LinkedIn Learning",             "Formation et développement professionnel", "T"),
    ("Formations CPA Québec",         "Formation et développement professionnel", "T"),
    ("Ordre des CPA du Québec",       "Formation et développement professionnel", "T"),
    ("HEC Montréal Formation",        "Formation et développement professionnel", "T"),
    ("Université Laval Formation",    "Formation et développement professionnel", "T"),
    ("UQAM Formation Continue",       "Formation et développement professionnel", "T"),
    ("Technologia Formation",         "Formation et développement professionnel", "T"),
    ("AFI Expertise Formation",       "Formation et développement professionnel", "T"),
    ("Edx Formation en Ligne",        "Formation et développement professionnel", "T"),
    ("Pluralsight Formation",         "Formation et développement professionnel", "T"),
    ("Skillshare Formation",          "Formation et développement professionnel", "T"),
    ("MasterClass Formation",         "Formation et développement professionnel", "T"),
    ("Cégep Formation Continue",      "Formation et développement professionnel", "T"),
    ("Infopresse Formation",          "Formation et développement professionnel", "T"),
    ("Barreau du Québec Formation",   "Formation et développement professionnel", "T"),
    ("PMI Montréal Formation",        "Formation et développement professionnel", "T"),
    ("IIBA Québec Formation",         "Formation et développement professionnel", "T"),
    ("Conférences Infopresse",        "Formation et développement professionnel", "T"),

    # ── Santé et bien-être ─ E (20) ──────────────────────────────────────
    ("Physiothérapeute Clinique",     "Frais de santé", "E"),
    ("Chiropraticien Centre",         "Frais de santé", "E"),
    ("Optométriste Clinique",         "Frais de santé", "E"),
    ("Clinique Physiothérapie PCN",   "Frais de santé", "E"),
    ("Centre Chiropratique Québec",   "Frais de santé", "E"),
    ("Visique Optométriste",          "Frais de santé", "E"),
    ("IRIS Optométriste",             "Frais de santé", "E"),
    ("Clinique Ostéopathie Mtl",      "Frais de santé", "E"),
    ("Massothérapeute Agréé QC",      "Frais de santé", "E"),
    ("Centre Podiatrique Québec",     "Frais de santé", "E"),
    ("Clinique Audiologie Mtl",       "Frais de santé", "E"),
    ("Services Ergothérapie QC",      "Frais de santé", "E"),
    ("Centre Acupuncture Québec",     "Frais de santé", "E"),
    ("Kinésiologue Certifié QC",      "Frais de santé", "E"),
    ("Clinique Nutrition Québec",     "Frais de santé", "E"),
    ("Psychologue Clinique Mtl",      "Frais de santé", "E"),
    ("Orthophoniste Québec",          "Frais de santé", "E"),
    ("Centre Orthopédique Mtl",       "Frais de santé", "E"),
    ("Laboratoire Médical Biron",     "Frais de santé", "E"),
    ("Clinique Santé Voyage QC",      "Frais de santé", "E"),

    # ── Médias et abonnements ─ T (20) ───────────────────────────────────
    ("La Presse+",                    "Abonnements et médias", "T"),
    ("Le Devoir",                     "Abonnements et médias", "T"),
    ("Journal de Montréal",           "Abonnements et médias", "T"),
    ("LinkedIn Premium",              "Abonnements et médias", "T"),
    ("Le Soleil Québec",              "Abonnements et médias", "T"),
    ("Radio-Canada Premium",          "Abonnements et médias", "T"),
    ("TVA Nouvelles Premium",         "Abonnements et médias", "T"),
    ("Les Affaires Abonnement",       "Abonnements et médias", "T"),
    ("L'actualité Magazine",          "Abonnements et médias", "T"),
    ("Protégez-Vous Abonnement",      "Abonnements et médias", "T"),
    ("Globe and Mail Business",       "Abonnements et médias", "T"),
    ("Financial Times Canada",        "Abonnements et médias", "T"),
    ("Wall Street Journal CA",        "Abonnements et médias", "T"),
    ("Bloomberg Terminal Canada",     "Abonnements et médias", "T"),
    ("Reuters Canada",                "Abonnements et médias", "T"),
    ("Spotify Business",              "Abonnements et médias", "T"),
    ("YouTube Premium Business",      "Abonnements et médias", "T"),
    ("Netflix Affaires",              "Abonnements et médias", "T"),
    ("Canva Pro Abonnement",          "Abonnements et médias", "T"),
    ("Figma Abonnement Pro",          "Abonnements et médias", "T"),

    # ── Événements et conférences ─ T (15) ───────────────────────────────
    ("Palais des congrès Montréal",   "Frais de représentation", "T"),
    ("Centre Bell Événements",        "Frais de représentation", "T"),
    ("Montréal en Lumière",           "Frais de représentation", "T"),
    ("Place des Arts Événements",     "Frais de représentation", "T"),
    ("Centre des congrès Québec",     "Frais de représentation", "T"),
    ("Expo Entrepreneurs Québec",     "Frais de représentation", "T"),
    ("Startupfest Montréal",          "Frais de représentation", "T"),
    ("C2 Montréal Conférence",        "Frais de représentation", "T"),
    ("Salon de l'emploi Montréal",    "Frais de représentation", "T"),
    ("Festival TransAmériques",       "Frais de représentation", "T"),
    ("Salon International Auto QC",   "Frais de représentation", "T"),
    ("Congrès AQTR Transport",        "Frais de représentation", "T"),
    ("Salon Construction Québec",     "Frais de représentation", "T"),
    ("Sommet du numérique Mtl",       "Frais de représentation", "T"),
    ("Gala Excellence Affaires QC",   "Frais de représentation", "T"),

    # ── Entretien véhicules ─ T (20) ─────────────────────────────────────
    ("Canadian Tire Auto",            "Entretien véhicules", "T"),
    ("Kal Tire Québec",               "Entretien véhicules", "T"),
    ("NAPA Autopro",                  "Entretien véhicules", "T"),
    ("Mr. Lube Québec",               "Entretien véhicules", "T"),
    ("Midas Québec",                  "Entretien véhicules", "T"),
    ("Monsieur Muffler Montréal",     "Entretien véhicules", "T"),
    ("Pneus À Rabais Québec",         "Entretien véhicules", "T"),
    ("Point S Pneus Québec",          "Entretien véhicules", "T"),
    ("Pneus Touchette",               "Entretien véhicules", "T"),
    ("Garage Côté & Fils",            "Entretien véhicules", "T"),
    ("Centre du Camion Montréal",     "Entretien véhicules", "T"),
    ("Concessionnaire Ford QC",       "Entretien véhicules", "T"),
    ("Concessionnaire GM Québec",     "Entretien véhicules", "T"),
    ("Concessionnaire Toyota QC",     "Entretien véhicules", "T"),
    ("AutoPlace Parts Québec",        "Entretien véhicules", "T"),
    ("Lordco Auto Parts QC",          "Entretien véhicules", "T"),
    ("Carquest Auto Parts QC",        "Entretien véhicules", "T"),
    ("Bumper to Bumper Québec",       "Entretien véhicules", "T"),
    ("Jiffy Lube Montréal",           "Entretien véhicules", "T"),
    ("Garage Mécanique Pro QC",       "Entretien véhicules", "T"),

    # ── Sécurité et surveillance ─ T (10) ────────────────────────────────
    ("Garda World Sécurité",          "Sécurité et surveillance", "T"),
    ("Securitas Canada",              "Sécurité et surveillance", "T"),
    ("ADT Sécurité Québec",           "Sécurité et surveillance", "T"),
    ("Chubb Sécurité Canada",         "Sécurité et surveillance", "T"),
    ("Tyco Sécurité Intégrée",        "Sécurité et surveillance", "T"),
    ("Protection Vidéotron",          "Sécurité et surveillance", "T"),
    ("Alarm Force Québec",            "Sécurité et surveillance", "T"),
    ("G4S Sécurité Canada",           "Sécurité et surveillance", "T"),
    ("Paladin Sécurité Québec",       "Sécurité et surveillance", "T"),
    ("Commissionnaires Québec",       "Sécurité et surveillance", "T"),

    # ══════════════════════════════════════════════════════════════════════
    # ══  500 ADDITIONAL VENDORS (bringing total to 1,000) ═══════════════
    # ══════════════════════════════════════════════════════════════════════

    # ── SAQ (Société des alcools) ─ T (10) ──────────────────────────────
    ("SAQ Centre-ville Montréal",    "Représentation et alcool", "T"),
    ("SAQ Signature Québec",         "Représentation et alcool", "T"),
    ("SAQ Express Laval",            "Représentation et alcool", "T"),
    ("SAQ Dépôt Montréal",           "Représentation et alcool", "T"),
    ("SAQ Sélection Sherbrooke",     "Représentation et alcool", "T"),
    ("SAQ Classique Gatineau",       "Représentation et alcool", "T"),
    ("SAQ Express Trois-Rivières",   "Représentation et alcool", "T"),
    ("SAQ Signature Montréal",       "Représentation et alcool", "T"),
    ("SAQ Inspire Longueuil",        "Représentation et alcool", "T"),
    ("SAQ Express Rimouski",         "Représentation et alcool", "T"),

    # ── SAAQ (permis et immatriculation) ─ E (10) ──────────────────────
    ("SAAQ Bureau Montréal",         "Immatriculation et permis", "E"),
    ("SAAQ Bureau Québec",           "Immatriculation et permis", "E"),
    ("SAAQ Bureau Laval",            "Immatriculation et permis", "E"),
    ("SAAQ Bureau Sherbrooke",       "Immatriculation et permis", "E"),
    ("SAAQ Bureau Gatineau",         "Immatriculation et permis", "E"),
    ("SAAQ Bureau Trois-Rivières",   "Immatriculation et permis", "E"),
    ("SAAQ Bureau Saguenay",         "Immatriculation et permis", "E"),
    ("SAAQ Bureau Longueuil",        "Immatriculation et permis", "E"),
    ("SAAQ Services en ligne",       "Immatriculation et permis", "E"),
    ("SAAQ Bureau Drummondville",    "Immatriculation et permis", "E"),

    # ── Notaires et huissiers ─ T (15) ─────────────────────────────────
    ("Notaire Me Tremblay",          "Honoraires juridiques", "T"),
    ("Notaire Me Gagnon",            "Honoraires juridiques", "T"),
    ("Notaire Me Côté",              "Honoraires juridiques", "T"),
    ("Notaire Me Bouchard",          "Honoraires juridiques", "T"),
    ("Notaire Me Gauthier",          "Honoraires juridiques", "T"),
    ("Étude Notariale St-Laurent",   "Honoraires juridiques", "T"),
    ("Notaire Me Pelletier",         "Honoraires juridiques", "T"),
    ("Étude Notariale Montréal",     "Honoraires juridiques", "T"),
    ("Huissier Québec Métro",        "Honoraires juridiques", "T"),
    ("Huissier Justice Montréal",    "Honoraires juridiques", "T"),
    ("Huissier Rive-Sud Service",    "Honoraires juridiques", "T"),
    ("Chambre des Notaires QC",      "Honoraires juridiques", "T"),
    ("Huissier Royal Québec",        "Honoraires juridiques", "T"),
    ("Étude Notariale Laval",        "Honoraires juridiques", "T"),
    ("Huissier Express Montréal",    "Honoraires juridiques", "T"),

    # ── Arpenteurs-géomètres ─ T (10) ──────────────────────────────────
    ("Arpenteur-géomètre Laval",     "Honoraires professionnels", "T"),
    ("Géomètre Québec Métro",        "Honoraires professionnels", "T"),
    ("Arpentage Montréal Inc",       "Honoraires professionnels", "T"),
    ("Groupe Arpentage Rive-Sud",    "Honoraires professionnels", "T"),
    ("Géomètre Expert Sherbrooke",   "Honoraires professionnels", "T"),
    ("Arpentage Pro Gatineau",       "Honoraires professionnels", "T"),
    ("Cabinet Arpentage Saguenay",   "Honoraires professionnels", "T"),
    ("Géomètre Trois-Rivières",      "Honoraires professionnels", "T"),
    ("Arpentage Laurentides Inc",    "Honoraires professionnels", "T"),
    ("Arpenteur Conseil Québec",     "Honoraires professionnels", "T"),

    # ── Cabanes à sucre ─ M (10) ───────────────────────────────────────
    ("Cabane à sucre Chez Dallaire", "Repas d'affaires", "M"),
    ("Érablière du Cap",             "Repas d'affaires", "M"),
    ("Sucrerie de la Montagne",      "Repas d'affaires", "M"),
    ("Cabane à sucre Constantin",    "Repas d'affaires", "M"),
    ("Érablière Charbonneau",        "Repas d'affaires", "M"),
    ("Cabane à sucre Handfield",     "Repas d'affaires", "M"),
    ("Érablière au Sous-Bois",       "Repas d'affaires", "M"),
    ("Cabane chez Ti-Mousse",        "Repas d'affaires", "M"),
    ("Sucrerie du Terroir Québec",   "Repas d'affaires", "M"),
    ("Érablière Famille Bolduc",     "Repas d'affaires", "M"),

    # ── Garderies et CPE ─ E (15) ──────────────────────────────────────
    ("Garderie Les Petits Lapins",   "Frais de garde", "E"),
    ("CPE Soleil Levant",            "Frais de garde", "E"),
    ("CPE Les Joyeux Lutins",        "Frais de garde", "E"),
    ("Garderie Arc-en-Ciel Mtl",     "Frais de garde", "E"),
    ("CPE La Petite École",          "Frais de garde", "E"),
    ("Garderie Les Coccinelles",     "Frais de garde", "E"),
    ("CPE Le Jardin Enchanté",       "Frais de garde", "E"),
    ("Garderie Pomme d'Api Laval",   "Frais de garde", "E"),
    ("CPE Les Petits Trésors",       "Frais de garde", "E"),
    ("Garderie Petit Monde Québec",  "Frais de garde", "E"),
    ("CPE Chez Grand-Maman",         "Frais de garde", "E"),
    ("Garderie Étoile Filante",      "Frais de garde", "E"),
    ("CPE Les Petits Pas",           "Frais de garde", "E"),
    ("Garderie Tournesol Montréal",  "Frais de garde", "E"),
    ("CPE La Maison des Enfants",    "Frais de garde", "E"),

    # ── Infrastructure infonuagique ─ T (15) ───────────────────────────
    ("AWS Canada (Amazon Cloud)",    "Infrastructure infonuagique", "T"),
    ("Azure Microsoft Canada",       "Infrastructure infonuagique", "T"),
    ("Google Cloud Platform CA",     "Infrastructure infonuagique", "T"),
    ("DigitalOcean Canada",          "Infrastructure infonuagique", "T"),
    ("Cloudflare Enterprise CA",     "Infrastructure infonuagique", "T"),
    ("OVH Cloud Québec",             "Infrastructure infonuagique", "T"),
    ("Linode Akamai Canada",         "Infrastructure infonuagique", "T"),
    ("Vultr Cloud Canada",           "Infrastructure infonuagique", "T"),
    ("IBM Cloud Canada",             "Infrastructure infonuagique", "T"),
    ("Oracle Cloud Canada",          "Infrastructure infonuagique", "T"),
    ("Rackspace Canada",             "Infrastructure infonuagique", "T"),
    ("Heroku Salesforce Cloud",      "Infrastructure infonuagique", "T"),
    ("Netlify Enterprise CA",        "Infrastructure infonuagique", "T"),
    ("Vercel Enterprise CA",         "Infrastructure infonuagique", "T"),
    ("Fastly CDN Canada",            "Infrastructure infonuagique", "T"),

    # ── Gestion des déchets ─ T (10) ───────────────────────────────────
    ("Waste Management Québec",      "Gestion des déchets", "T"),
    ("GFL Environmental Québec",     "Gestion des déchets", "T"),
    ("Enviro Connexions Mtl",        "Gestion des déchets", "T"),
    ("Derichebourg Environnement",   "Gestion des déchets", "T"),
    ("EBI Environnement Inc",        "Gestion des déchets", "T"),
    ("Récupération Frontenac",       "Gestion des déchets", "T"),
    ("Sani-Éco Environnement",      "Gestion des déchets", "T"),
    ("Services Matrec Québec",       "Gestion des déchets", "T"),
    ("Groupe Tiru Canada",           "Gestion des déchets", "T"),
    ("Conteneurs Laurentides",       "Gestion des déchets", "T"),

    # ── Aménagement paysager ─ T (10) ──────────────────────────────────
    ("Groupe Vertdure Paysage",      "Aménagement paysager", "T"),
    ("Nutri-Lawn Québec",            "Aménagement paysager", "T"),
    ("Weed Man Québec",              "Aménagement paysager", "T"),
    ("Paysagement Lavoie Inc",       "Aménagement paysager", "T"),
    ("Pépinière Jasmin Montréal",    "Aménagement paysager", "T"),
    ("Paysagiste Écovert Québec",    "Aménagement paysager", "T"),
    ("Arbre Expert Québec",          "Aménagement paysager", "T"),
    ("Gazon Savard Ltée",            "Aménagement paysager", "T"),
    ("Jardin Hamel Québec",          "Aménagement paysager", "T"),
    ("Botanix Centre Jardin",        "Aménagement paysager", "T"),

    # ── Permis et immatriculations ─ E (10) ────────────────────────────
    ("RBQ Licence Entrepreneur",     "Permis et immatriculations", "E"),
    ("Ville de Montréal Permis",     "Permis et immatriculations", "E"),
    ("Ville de Québec Permis",       "Permis et immatriculations", "E"),
    ("Ville de Laval Permis",        "Permis et immatriculations", "E"),
    ("Ville de Gatineau Permis",     "Permis et immatriculations", "E"),
    ("Ville de Sherbrooke Permis",   "Permis et immatriculations", "E"),
    ("Ville de Longueuil Permis",    "Permis et immatriculations", "E"),
    ("CCQ Carte Compétence",         "Permis et immatriculations", "E"),
    ("CNESST Permis Travail",        "Permis et immatriculations", "E"),
    ("Régie du Bâtiment QC",         "Permis et immatriculations", "E"),

    # ── More Matériaux et fournitures ─ T (15) ─────────────────────────
    ("Matériaux Pont-Masson",        "Matériaux et fournitures", "T"),
    ("Gypse et Plâtre Montréal",     "Matériaux et fournitures", "T"),
    ("Béton Provincial QC",          "Matériaux et fournitures", "T"),
    ("Ciment St-Laurent",            "Matériaux et fournitures", "T"),
    ("Boiseries Raymond Ltée",       "Matériaux et fournitures", "T"),
    ("Sable et Gravier Québec",      "Matériaux et fournitures", "T"),
    ("Maçonnerie Montréal Inc",      "Matériaux et fournitures", "T"),
    ("Acier Leroux Inc",             "Matériaux et fournitures", "T"),
    ("Aluminium Saguenay",           "Matériaux et fournitures", "T"),
    ("Isolation Majeau Inc",         "Matériaux et fournitures", "T"),
    ("Toiture BP Canada",            "Matériaux et fournitures", "T"),
    ("Briques Montréal Ltée",        "Matériaux et fournitures", "T"),
    ("Fenêtres Magistral",           "Matériaux et fournitures", "T"),
    ("Portes et Fenêtres Novatech",  "Matériaux et fournitures", "T"),
    ("Bois Hamel Québec",            "Matériaux et fournitures", "T"),

    # ── More Carburant et huile ─ T (10) ───────────────────────────────
    ("Harnois Énergies",             "Carburant et huile", "T"),
    ("MacEwen Pétrole Québec",       "Carburant et huile", "T"),
    ("Parkland Fuel Québec",         "Carburant et huile", "T"),
    ("Crevier Lubrifiants",          "Carburant et huile", "T"),
    ("Pétrole Global QC",            "Carburant et huile", "T"),
    ("Station Mobil Québec",         "Carburant et huile", "T"),
    ("Pétroles Therrien",            "Carburant et huile", "T"),
    ("Énergies Sonic Québec",        "Carburant et huile", "T"),
    ("Canadian Fuel Distributors",   "Carburant et huile", "T"),
    ("Pétroles Bélanger Inc",        "Carburant et huile", "T"),

    # ── More Télécommunications ─ T (10) ───────────────────────────────
    ("SaskTel Affaires Québec",      "Télécommunications", "T"),
    ("Ebox Internet Québec",         "Télécommunications", "T"),
    ("TekSavvy Solutions Québec",    "Télécommunications", "T"),
    ("Cogeco Connexion Affaires",    "Télécommunications", "T"),
    ("Virgin Plus Affaires",         "Télécommunications", "T"),
    ("Fido Affaires Québec",         "Télécommunications", "T"),
    ("Public Mobile Affaires",       "Télécommunications", "T"),
    ("Oxio Internet Québec",         "Télécommunications", "T"),
    ("Bravo Telecom Montréal",       "Télécommunications", "T"),
    ("Oricom Internet Québec",       "Télécommunications", "T"),

    # ── More Électricité et gaz ─ E (10) ──────────────────────────────
    ("Hydro-Sherbrooke",             "Électricité et gaz", "E"),
    ("Énergir Affaires Montréal",    "Électricité et gaz", "E"),
    ("Gaz Métro Québec",             "Électricité et gaz", "E"),
    ("Bullfrog Power Canada",        "Électricité et gaz", "E"),
    ("Hydro-Québec Affaires",        "Électricité et gaz", "E"),
    ("Énergir Affaires Québec",      "Électricité et gaz", "E"),
    ("Énergie Valero Québec",        "Électricité et gaz", "E"),
    ("Direct Énergie Québec",        "Électricité et gaz", "E"),
    ("Gaz Naturel Québec Inc",       "Électricité et gaz", "E"),
    ("Énergie Brookfield QC",        "Électricité et gaz", "E"),

    # ── More Frais bancaires ─ E (10) ─────────────────────────────────
    ("Banque Royale frais affaires", "Frais bancaires", "E"),
    ("Desjardins frais entreprise",  "Frais bancaires", "E"),
    ("BMO frais commerciaux",        "Frais bancaires", "E"),
    ("TD frais entreprise",          "Frais bancaires", "E"),
    ("CIBC frais affaires",          "Frais bancaires", "E"),
    ("Banque Nationale commerce",    "Frais bancaires", "E"),
    ("Scotiabank frais affaires",    "Frais bancaires", "E"),
    ("Banque Manuvie frais",         "Frais bancaires", "E"),
    ("Caisse Desjardins affaires",   "Frais bancaires", "E"),
    ("Banque Équitable frais",       "Frais bancaires", "E"),

    # ── More Assurances ─ I (15) ──────────────────────────────────────
    ("Wawanesa Assurance QC",        "Assurances", "I"),
    ("Zurich Assurance Canada",      "Assurances", "I"),
    ("Chubb Assurance Canada",       "Assurances", "I"),
    ("RSA Canada Assurance",         "Assurances", "I"),
    ("Northbridge Assurance",        "Assurances", "I"),
    ("Travelers Canada Assurance",   "Assurances", "I"),
    ("Markel Canada Assurance",      "Assurances", "I"),
    ("Unica Assurance Québec",       "Assurances", "I"),
    ("L'Union Canadienne Assurance", "Assurances", "I"),
    ("Berkley Canada Assurance",     "Assurances", "I"),
    ("Gore Mutual Assurance",        "Assurances", "I"),
    ("Pembridge Assurance Canada",   "Assurances", "I"),
    ("Pafco Assurance Canada",       "Assurances", "I"),
    ("Garantie Optimale Québec",     "Assurances", "I"),
    ("Groupe Cloutier Assurance",    "Assurances", "I"),

    # ── More Fournitures de bureau ─ T (10) ───────────────────────────
    ("Papeterie Montréal Plus",      "Fournitures de bureau", "T"),
    ("Fournitures SelectBureau",     "Fournitures de bureau", "T"),
    ("Hamster Fournitures Bureau",   "Fournitures de bureau", "T"),
    ("Basics Office Products QC",    "Fournitures de bureau", "T"),
    ("Novexco Fournitures Québec",   "Fournitures de bureau", "T"),
    ("Denis Office Supplies",        "Fournitures de bureau", "T"),
    ("Offix Direct Québec",          "Fournitures de bureau", "T"),
    ("S.P. Richards Québec",         "Fournitures de bureau", "T"),
    ("Papeterie Laval Express",      "Fournitures de bureau", "T"),
    ("MégaBureau Québec Inc",        "Fournitures de bureau", "T"),

    # ── More Logiciels et abonnements ─ T (15) ────────────────────────
    ("Monday.com Affaires",          "Logiciels et abonnements", "T"),
    ("Trello Enterprise Canada",     "Logiciels et abonnements", "T"),
    ("Freshbooks Canada",            "Logiciels et abonnements", "T"),
    ("Xero Canada",                  "Logiciels et abonnements", "T"),
    ("Acomba Logiciel",              "Logiciels et abonnements", "T"),
    ("Caseware Canada",              "Logiciels et abonnements", "T"),
    ("Wrike Enterprise CA",          "Logiciels et abonnements", "T"),
    ("Zendesk Canada",               "Logiciels et abonnements", "T"),
    ("Intercom Canada",              "Logiciels et abonnements", "T"),
    ("Calendly Business CA",         "Logiciels et abonnements", "T"),
    ("Miro Enterprise Canada",       "Logiciels et abonnements", "T"),
    ("Loom Business Canada",         "Logiciels et abonnements", "T"),
    ("Grammarly Business CA",        "Logiciels et abonnements", "T"),
    ("Webflow Enterprise CA",        "Logiciels et abonnements", "T"),
    ("Canva Enterprise Canada",      "Logiciels et abonnements", "T"),

    # ── More Transport et déplacements ─ T (10) ───────────────────────
    ("Flair Airlines",               "Transport et déplacements", "T"),
    ("Sunwing Airlines Canada",      "Transport et déplacements", "T"),
    ("WestJet Canada",               "Transport et déplacements", "T"),
    ("Orléans Express Autobus",      "Transport et déplacements", "T"),
    ("Limocar Autobus Québec",       "Transport et déplacements", "T"),
    ("Eva Air Canada",               "Transport et déplacements", "T"),
    ("Intercar Autobus QC",          "Transport et déplacements", "T"),
    ("Taxi Champlain Montréal",      "Transport et déplacements", "T"),
    ("Lyft Canada",                  "Transport et déplacements", "T"),
    ("Taxi Coop Laval",              "Transport et déplacements", "T"),

    # ── More Repas d'affaires ─ M (20) ────────────────────────────────
    ("Chez Ashton Québec",           "Repas d'affaires", "M"),
    ("Restaurant Normandin",         "Repas d'affaires", "M"),
    ("Rotisserie Benny Québec",      "Repas d'affaires", "M"),
    ("Thaï Express",                 "Repas d'affaires", "M"),
    ("Sushi Shop",                   "Repas d'affaires", "M"),
    ("Cuisine Szechuan Montréal",    "Repas d'affaires", "M"),
    ("Nickels Restaurant",           "Repas d'affaires", "M"),
    ("Cora Déjeuner",                "Repas d'affaires", "M"),
    ("Mandy's Salads Montréal",      "Repas d'affaires", "M"),
    ("Olive et Gourmando",           "Repas d'affaires", "M"),
    ("Restaurant Schwartz Mtl",      "Repas d'affaires", "M"),
    ("Le Pied de Cochon",            "Repas d'affaires", "M"),
    ("Joe Beef Montréal",            "Repas d'affaires", "M"),
    ("Restaurant Toqué",             "Repas d'affaires", "M"),
    ("Chez Victoire Montréal",       "Repas d'affaires", "M"),
    ("Au Pied de Cochon Cabane",     "Repas d'affaires", "M"),
    ("Wilensky's Light Lunch",       "Repas d'affaires", "M"),
    ("Gibby's Restaurant Mtl",       "Repas d'affaires", "M"),
    ("Jardin Nelson Vieux-Mtl",      "Repas d'affaires", "M"),
    ("Café Olimpico Montréal",       "Repas d'affaires", "M"),

    # ── More Publicité et marketing ─ T (10) ──────────────────────────
    ("Cossette Communication",       "Publicité et marketing", "T"),
    ("Sid Lee Agence Montréal",      "Publicité et marketing", "T"),
    ("LG2 Publicité Québec",         "Publicité et marketing", "T"),
    ("Bleublancrouge Agence",        "Publicité et marketing", "T"),
    ("Tam-Tam\\TBWA Montréal",      "Publicité et marketing", "T"),
    ("Havas Montréal",               "Publicité et marketing", "T"),
    ("Dentsu Canada Québec",         "Publicité et marketing", "T"),
    ("Pages Jaunes Canada",          "Publicité et marketing", "T"),
    ("Yelp Publicité Canada",        "Publicité et marketing", "T"),
    ("Instagram Ads Affaires",       "Publicité et marketing", "T"),

    # ── More Honoraires professionnels ─ T (10) ───────────────────────
    ("Évaluateur agréé Québec",      "Honoraires professionnels", "T"),
    ("Actuaire Conseil Montréal",    "Honoraires professionnels", "T"),
    ("Expert en bâtiment Québec",    "Honoraires professionnels", "T"),
    ("Urbaniste Conseil Québec",     "Honoraires professionnels", "T"),
    ("Designer intérieur Québec",    "Honoraires professionnels", "T"),
    ("Agent immobilier Québec",      "Honoraires professionnels", "T"),
    ("Conseiller financier Mtl",     "Honoraires professionnels", "T"),
    ("Inspecteur en bâtiment QC",    "Honoraires professionnels", "T"),
    ("Technicien comptable Laval",   "Honoraires professionnels", "T"),
    ("Conseiller RH Montréal",       "Honoraires professionnels", "T"),

    # ── More Location d'équipement ─ T (10) ───────────────────────────
    ("United Rentals Canada",        "Location d'équipement", "T"),
    ("Strongco Location Québec",     "Location d'équipement", "T"),
    ("Wajax Location Québec",        "Location d'équipement", "T"),
    ("Location Pro Montréal",        "Location d'équipement", "T"),
    ("Location Simplex Québec",      "Location d'équipement", "T"),
    ("Hewitt Location Équipement",   "Location d'équipement", "T"),
    ("SMS Location Québec",          "Location d'équipement", "T"),
    ("Cooper Location Équip QC",     "Location d'équipement", "T"),
    ("All-Star Equipment QC",        "Location d'équipement", "T"),
    ("National Location Équip QC",   "Location d'équipement", "T"),

    # ── More Épicerie et fournitures alimentaires ─ Z (10) ────────────
    ("Marché Bonanza Montréal",      "Épicerie et fournitures alimentaires", "Z"),
    ("Fruiterie 440 Laval",          "Épicerie et fournitures alimentaires", "Z"),
    ("Épicerie Coréenne Montréal",   "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Haïtien Montréal",      "Épicerie et fournitures alimentaires", "Z"),
    ("Épicerie Libanaise Mtl",       "Épicerie et fournitures alimentaires", "Z"),
    ("Supermarché Kim Phat",         "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Oriental Montréal",     "Épicerie et fournitures alimentaires", "Z"),
    ("Épicerie Européenne Québec",   "Épicerie et fournitures alimentaires", "Z"),
    ("Marché Akhavan Montréal",      "Épicerie et fournitures alimentaires", "Z"),
    ("Épicerie Segal Montréal",      "Épicerie et fournitures alimentaires", "Z"),

    # ── More Équipement et outillage ─ T (10) ─────────────────────────
    ("Metabo Outillage Canada",      "Équipement et outillage", "T"),
    ("Festool Canada Québec",        "Équipement et outillage", "T"),
    ("Ridgid Outillage Québec",      "Équipement et outillage", "T"),
    ("Dremel Outillage Canada",      "Équipement et outillage", "T"),
    ("Husqvarna Canada Québec",      "Équipement et outillage", "T"),
    ("Stihl Canada Québec",          "Équipement et outillage", "T"),
    ("Echo Outillage Canada",        "Équipement et outillage", "T"),
    ("John Deere Québec",            "Équipement et outillage", "T"),
    ("Kubota Canada Québec",         "Équipement et outillage", "T"),
    ("Caterpillar Québec",           "Équipement et outillage", "T"),

    # ── More Entretien véhicules ─ T (10) ─────────────────────────────
    ("Speedy Auto Service Québec",   "Entretien véhicules", "T"),
    ("Fix Auto Québec",              "Entretien véhicules", "T"),
    ("ProColor Collision QC",        "Entretien véhicules", "T"),
    ("Maaco Québec",                 "Entretien véhicules", "T"),
    ("Pneus Ratté Québec",           "Entretien véhicules", "T"),
    ("Active Green Ross Québec",     "Entretien véhicules", "T"),
    ("Mécanique Générale Dubois",    "Entretien véhicules", "T"),
    ("Garage Beaulieu et Fils",      "Entretien véhicules", "T"),
    ("Centre Auto Laval Inc",        "Entretien véhicules", "T"),
    ("Pièces d'Auto Québec Inc",     "Entretien véhicules", "T"),

    # ── More Abonnements et médias ─ T (10) ───────────────────────────
    ("Le Journal de Québec",         "Abonnements et médias", "T"),
    ("Métro Média Montréal",         "Abonnements et médias", "T"),
    ("Le Droit Ottawa-Gatineau",     "Abonnements et médias", "T"),
    ("La Tribune Sherbrooke",        "Abonnements et médias", "T"),
    ("Le Quotidien Saguenay",        "Abonnements et médias", "T"),
    ("Le Nouvelliste Trois-Rivières","Abonnements et médias", "T"),
    ("Voir Montréal Hebdo",          "Abonnements et médias", "T"),
    ("Info-Dimanche Rivière-du-Loup","Abonnements et médias", "T"),
    ("Le Courrier Laval",            "Abonnements et médias", "T"),
    ("Revue Commerce Québec",        "Abonnements et médias", "T"),

    # ── More Formation et développement professionnel ─ T (10) ────────
    ("Collège LaSalle Formation",    "Formation et développement professionnel", "T"),
    ("ETS Formation Continue",       "Formation et développement professionnel", "T"),
    ("Polytechnique Formation",      "Formation et développement professionnel", "T"),
    ("Concordia Formation Continue", "Formation et développement professionnel", "T"),
    ("McGill Formation Executive",   "Formation et développement professionnel", "T"),
    ("ITHQ Formation Cuisine",       "Formation et développement professionnel", "T"),
    ("Cégep Limoilou Formation",     "Formation et développement professionnel", "T"),
    ("Formation Secourisme QC",      "Formation et développement professionnel", "T"),
    ("Formation SST Québec",         "Formation et développement professionnel", "T"),
    ("Conseil RH Formation QC",      "Formation et développement professionnel", "T"),

    # ── More Frais de représentation ─ T (10) ─────────────────────────
    ("Groupe CH Canadiens Mtl",      "Frais de représentation", "T"),
    ("Festival Juste pour Rire",     "Frais de représentation", "T"),
    ("Grand Prix F1 Montréal",       "Frais de représentation", "T"),
    ("Orchestre Symphonique Mtl",    "Frais de représentation", "T"),
    ("Théâtre du Nouveau Monde",     "Frais de représentation", "T"),
    ("Cirque du Soleil Billets",     "Frais de représentation", "T"),
    ("Impact CF Montréal",           "Frais de représentation", "T"),
    ("Alouettes de Montréal",        "Frais de représentation", "T"),
    ("Musée des Beaux-Arts Mtl",     "Frais de représentation", "T"),
    ("Casino Montréal Événement",    "Frais de représentation", "T"),

    # ── More Frais de santé ─ E (10) ──────────────────────────────────
    ("Clinique Dentaire Express",    "Frais de santé", "E"),
    ("Centre Dermatologie Québec",   "Frais de santé", "E"),
    ("Clinique Allergologie Mtl",    "Frais de santé", "E"),
    ("Centre Cardiologie Québec",    "Frais de santé", "E"),
    ("Clinique Urologie Montréal",   "Frais de santé", "E"),
    ("Centre Pneumologie Québec",    "Frais de santé", "E"),
    ("Clinique Rhumatologie Mtl",    "Frais de santé", "E"),
    ("Centre Gastro-Entérologie",    "Frais de santé", "E"),
    ("Clinique Endocrinologie QC",   "Frais de santé", "E"),
    ("Centre Neurologie Montréal",   "Frais de santé", "E"),

    # ── More Frais de livraison et courrier ─ T (10) ──────────────────
    ("Jet Worldwide Express QC",     "Frais de livraison et courrier", "T"),
    ("ATS Healthcare Logistics",     "Frais de livraison et courrier", "T"),
    ("Spyder Express Montréal",      "Frais de livraison et courrier", "T"),
    ("Flash Messenger Québec",       "Frais de livraison et courrier", "T"),
    ("Priority Express Canada",      "Frais de livraison et courrier", "T"),
    ("Dynamex Canada Express",       "Frais de livraison et courrier", "T"),
    ("GO! Express Québec",           "Frais de livraison et courrier", "T"),
    ("Courrier Plus Montréal",       "Frais de livraison et courrier", "T"),
    ("Livraison Rapide Québec",      "Frais de livraison et courrier", "T"),
    ("Express Montréal Ltée",        "Frais de livraison et courrier", "T"),

    # ── More Entretien et nettoyage ─ T (10) ──────────────────────────
    ("Nettoyage Signature Québec",   "Entretien et nettoyage", "T"),
    ("Propreté Totale Montréal",     "Entretien et nettoyage", "T"),
    ("Service Ménager Laval",        "Entretien et nettoyage", "T"),
    ("Entretien National QC",        "Entretien et nettoyage", "T"),
    ("Nettoyage Commercial Pro",     "Entretien et nettoyage", "T"),
    ("Service Complet Entretien",    "Entretien et nettoyage", "T"),
    ("Ménage Parfait Montréal",      "Entretien et nettoyage", "T"),
    ("Nettoyage Industriel Québec",  "Entretien et nettoyage", "T"),
    ("Entretien Général Laval",      "Entretien et nettoyage", "T"),
    ("Pro-Clean Services Québec",    "Entretien et nettoyage", "T"),

    # ── More Sécurité et surveillance ─ T (10) ────────────────────────
    ("Sonitrol Sécurité Québec",     "Sécurité et surveillance", "T"),
    ("ICT Protection Québec",        "Sécurité et surveillance", "T"),
    ("SECOM Sécurité Canada",        "Sécurité et surveillance", "T"),
    ("Protection Incendie Québec",   "Sécurité et surveillance", "T"),
    ("Honeywell Sécurité Canada",    "Sécurité et surveillance", "T"),
    ("Johnson Controls Sécurité",    "Sécurité et surveillance", "T"),
    ("Bosch Sécurité Canada",        "Sécurité et surveillance", "T"),
    ("Hikvision Canada Québec",      "Sécurité et surveillance", "T"),
    ("Dahua Sécurité Canada",        "Sécurité et surveillance", "T"),
    ("Axis Communications QC",       "Sécurité et surveillance", "T"),

    # ── More Frais médicaux ─ E (10) ──────────────────────────────────
    ("Familiprix Pharmacie",         "Frais médicaux", "E"),
    ("Costco Pharmacie Québec",      "Frais médicaux", "E"),
    ("Walmart Pharmacie QC",         "Frais médicaux", "E"),
    ("Laboratoire CDL Québec",       "Frais médicaux", "E"),
    ("Centre Prélèvement Biron",     "Frais médicaux", "E"),
    ("Clinique Sans Rendez-vous",    "Frais médicaux", "E"),
    ("Urgence Santé Montréal",       "Frais médicaux", "E"),
    ("Clinique Voyage Santé QC",     "Frais médicaux", "E"),
    ("Centre de Santé Laval",        "Frais médicaux", "E"),
    ("Pharmacie Québécoise Inc",     "Frais médicaux", "E"),

    # ── Fournitures vétérinaires ─ T (10) ─────────────────────────────
    ("CDMV Fournitures Vétérinaire", "Fournitures vétérinaires", "T"),
    ("Dispomed Vétérinaire QC",      "Fournitures vétérinaires", "T"),
    ("Mondou Fournitures Animal",    "Fournitures vétérinaires", "T"),
    ("Animalerie Montréal Inc",      "Fournitures vétérinaires", "T"),
    ("Global Pet Foods Québec",      "Fournitures vétérinaires", "T"),
    ("Idexx Laboratories Canada",    "Fournitures vétérinaires", "T"),
    ("Zoetis Canada Vétérinaire",    "Fournitures vétérinaires", "T"),
    ("Merial Canada Animal",         "Fournitures vétérinaires", "T"),
    ("Elanco Canada Vétérinaire",    "Fournitures vétérinaires", "T"),
    ("Royal Canin Canada Pro",       "Fournitures vétérinaires", "T"),

    # ── Déménagement et entreposage ─ T (10) ──────────────────────────
    ("Déménagement Économique QC",   "Déménagement et entreposage", "T"),
    ("TransPro Déménagement Mtl",    "Déménagement et entreposage", "T"),
    ("Déménagio Montréal",           "Déménagement et entreposage", "T"),
    ("StorageMart Québec",           "Déménagement et entreposage", "T"),
    ("Access Storage Québec",        "Déménagement et entreposage", "T"),
    ("Public Storage Montréal",      "Déménagement et entreposage", "T"),
    ("Mini-Entrepôt Québec",         "Déménagement et entreposage", "T"),
    ("U-Haul Québec Location",       "Déménagement et entreposage", "T"),
    ("Kubik Entreposage Mtl",        "Déménagement et entreposage", "T"),
    ("Sentinel Entreposage QC",      "Déménagement et entreposage", "T"),

    # ── Fournitures pharmaceutiques ─ T (10) ──────────────────────────
    ("McKesson Pharma Québec",       "Fournitures pharmaceutiques", "T"),
    ("Kohl & Frisch Pharma QC",      "Fournitures pharmaceutiques", "T"),
    ("AmerisourceBergen Canada",     "Fournitures pharmaceutiques", "T"),
    ("Teva Canada Pharmaceutique",   "Fournitures pharmaceutiques", "T"),
    ("Apotex Canada Québec",         "Fournitures pharmaceutiques", "T"),
    ("Sandoz Canada Pharmaceut",     "Fournitures pharmaceutiques", "T"),
    ("Bausch Health Canada",         "Fournitures pharmaceutiques", "T"),
    ("Pfizer Canada Québec",         "Fournitures pharmaceutiques", "T"),
    ("Sanofi Canada Québec",         "Fournitures pharmaceutiques", "T"),
    ("Merck Canada Québec",          "Fournitures pharmaceutiques", "T"),

    # ── Fournitures éducatives ─ T (10) ───────────────────────────────
    ("Brault & Bouthillier Éduc",    "Fournitures éducatives", "T"),
    ("Editions CEC Québec",          "Fournitures éducatives", "T"),
    ("Chenelière Éducation",         "Fournitures éducatives", "T"),
    ("Scholastic Canada Éducation",  "Fournitures éducatives", "T"),
    ("ERPI Éducation Québec",        "Fournitures éducatives", "T"),
    ("Spectrum Éducatif Québec",     "Fournitures éducatives", "T"),
    ("Didacto Jeux Éducatifs",       "Fournitures éducatives", "T"),
    ("Randolph Jeux Québec",         "Fournitures éducatives", "T"),
    ("Matériel Éducatif Québec",     "Fournitures éducatives", "T"),
    ("Ludik Québec Éducation",       "Fournitures éducatives", "T"),

    # ── Fournitures de restaurant ─ T (10) ─────────────────────────────
    ("Équipement CRS Cuisine",       "Fournitures de restaurant", "T"),
    ("Russell Hendrix Resto Équip",  "Fournitures de restaurant", "T"),
    ("Faema Canada Espresso",        "Fournitures de restaurant", "T"),
    ("Rational Canada Four",         "Fournitures de restaurant", "T"),
    ("Hobart Canada Équipement",     "Fournitures de restaurant", "T"),
    ("Vollrath Canada Cuisine",      "Fournitures de restaurant", "T"),
    ("True Manufacturing Canada",    "Fournitures de restaurant", "T"),
    ("Cambro Canada Restauration",   "Fournitures de restaurant", "T"),
    ("Waring Commercial Canada",     "Fournitures de restaurant", "T"),
    ("Globe Food Equipment QC",      "Fournitures de restaurant", "T"),

    # ── Travaux d'entretien ─ T (10) ───────────────────────────────────
    ("Rénovation Tremblay Inc",      "Travaux d'entretien", "T"),
    ("Peinture Montréal Pro",        "Travaux d'entretien", "T"),
    ("Plâtrier Express Québec",      "Travaux d'entretien", "T"),
    ("Menuiserie Laval Inc",         "Travaux d'entretien", "T"),
    ("Réparation Générale Côté",     "Travaux d'entretien", "T"),
    ("Entrepreneur Rénovation QC",   "Travaux d'entretien", "T"),
    ("Travaux Martin et Fils",       "Travaux d'entretien", "T"),
    ("Entretien Bâtiment Pro QC",    "Travaux d'entretien", "T"),
    ("Services Rénovation Plus",     "Travaux d'entretien", "T"),
    ("Réparations Express Mtl",      "Travaux d'entretien", "T"),

    # ── Travaux électriques ─ T (10) ───────────────────────────────────
    ("Électricien Laval Express",    "Travaux électriques", "T"),
    ("Fortier Électrique Inc",       "Travaux électriques", "T"),
    ("Électricité Pro Montréal",     "Travaux électriques", "T"),
    ("Courant Plus Québec Inc",      "Travaux électriques", "T"),
    ("Services Électriques Gagné",   "Travaux électriques", "T"),
    ("Installation Électrique Pro",  "Travaux électriques", "T"),
    ("Hydro-Électrique Laval",       "Travaux électriques", "T"),
    ("Voltec Électrique Québec",     "Travaux électriques", "T"),
    ("Ampère Services Électr QC",    "Travaux électriques", "T"),
    ("Watt Électrique Montréal",     "Travaux électriques", "T"),

    # ── Toiture et couverture ─ T (10) ─────────────────────────────────
    ("Toiture Expert Montréal",      "Toiture et couverture", "T"),
    ("Couvreur Pro Québec",          "Toiture et couverture", "T"),
    ("Toitures Laval Inc",           "Toiture et couverture", "T"),
    ("Couverture Rive-Sud Pro",      "Toiture et couverture", "T"),
    ("IKO Toiture Canada",           "Toiture et couverture", "T"),
    ("BP Canada Bardeaux",           "Toiture et couverture", "T"),
    ("CertainTeed Canada Toiture",   "Toiture et couverture", "T"),
    ("GAF Toiture Canada",           "Toiture et couverture", "T"),
    ("Soprema Canada Québec",        "Toiture et couverture", "T"),
    ("Toiture Duvernay Ltée",        "Toiture et couverture", "T"),

    # ── Location de véhicules (more) ─ T (5) ──────────────────────────
    ("Zipcar Montréal",              "Location de véhicules", "T"),
    ("Modo Coopérative Auto QC",     "Location de véhicules", "T"),
    ("Penske Location Camion QC",    "Location de véhicules", "T"),
    ("Ryder Location Camion QC",     "Location de véhicules", "T"),
    ("Location Sauvé Camion QC",     "Location de véhicules", "T"),

    # ── Frais de traitement des paiements (more) ─ E (8) ──────────────
    ("Pivotal Payments Canada",      "Frais de traitement des paiements", "E"),
    ("Fattmerchant Canada",          "Frais de traitement des paiements", "E"),
    ("Tilt Payments Québec",         "Frais de traitement des paiements", "E"),
    ("Rotessa Payments Canada",      "Frais de traitement des paiements", "E"),
    ("Payfirma Canada Frais",        "Frais de traitement des paiements", "E"),
    ("iATS Payments Canada",         "Frais de traitement des paiements", "E"),
    ("Plooto Payments Canada",       "Frais de traitement des paiements", "E"),
    ("Plastiq Payments Canada",      "Frais de traitement des paiements", "E"),

    # ── Achats marchandises ─ T (10) ───────────────────────────────────
    ("Grossiste Mode Québec",        "Achats marchandises", "T"),
    ("Distribution Textile Canada",  "Achats marchandises", "T"),
    ("Import Export Mode Mtl",       "Achats marchandises", "T"),
    ("Fournisseur Bijoux Québec",    "Achats marchandises", "T"),
    ("Grossiste Chaussures QC",      "Achats marchandises", "T"),
    ("Accessoires Plus Montréal",    "Achats marchandises", "T"),
    ("Produits Beauté Gros QC",      "Achats marchandises", "T"),
    ("Cosmétiques Montréal Gros",    "Achats marchandises", "T"),
    ("Artisanat Québécois Gros",     "Achats marchandises", "T"),
    ("Cadeaux Souvenirs Gros QC",    "Achats marchandises", "T"),

    # ── Équipement médical (more) ─ T (10) ────────────────────────────
    ("Olympus Canada Médical",       "Équipement médical", "T"),
    ("Medtronic Canada Québec",      "Équipement médical", "T"),
    ("Hologic Canada Médical",       "Équipement médical", "T"),
    ("Zimmer Biomet Canada",         "Équipement médical", "T"),
    ("Smith & Nephew Canada",        "Équipement médical", "T"),
    ("Edwards Lifesciences CA",      "Équipement médical", "T"),
    ("Intuitive Surgical CA",        "Équipement médical", "T"),
    ("Dräger Canada Médical",        "Équipement médical", "T"),
    ("Hill-Rom Canada Médical",      "Équipement médical", "T"),
    ("Getinge Canada Médical",       "Équipement médical", "T"),

    # ── Fournitures dentaires (more) ─ T (5) ──────────────────────────
    ("Dentsply Sirona Canada",       "Fournitures dentaires", "T"),
    ("Brasseler Canada Dental",      "Fournitures dentaires", "T"),
    ("Septodont Canada Dental",      "Fournitures dentaires", "T"),
    ("Ultradent Products Canada",    "Fournitures dentaires", "T"),
    ("GC America Canada Dental",     "Fournitures dentaires", "T"),

    # ── Fournitures médicales (more) ─ T (5) ──────────────────────────
    ("Mölnlycke Health Canada",      "Fournitures médicales", "T"),
    ("Coloplast Canada Médical",     "Fournitures médicales", "T"),
    ("ConvaTec Canada Médical",      "Fournitures médicales", "T"),
    ("Halyard Health Canada",        "Fournitures médicales", "T"),
    ("Ansell Médical Canada",        "Fournitures médicales", "T"),

]

# ── Helpers (mirror logic from learning_memory_store.py / ai_router.py) ───────

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_key(value: str) -> str:
    return value.strip().casefold()


def _build_memory_key(
    event_type: str,
    vendor: str,
    client_code: str,
    doc_type: str,
    category: str,
    gl_account: str,
    tax_code: str,
) -> str:
    """Mirrors LearningMemoryStore._build_memory_key exactly."""
    parts = [event_type, vendor, client_code, doc_type, category, gl_account, tax_code]
    return "|".join(_normalize_key(p) for p in parts)


def _build_cache_key(task_type: str, sanitized_prompt: str) -> str:
    """Mirrors ai_router._cache_key exactly: SHA-256(task_type + NUL + prompt)."""
    raw = f"{task_type}\x00{sanitized_prompt}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_seed_prompt(vendor_name: str) -> str:
    """
    Construct the full prompt the ai_router would assemble for this vendor.
    Matches:  f"{prompt}\\n\\nContext:\\n{json.dumps(context, ...)}"
    """
    context = {"vendor": vendor_name}
    return (
        f"{_SEED_PROMPT}\n\nContext:\n"
        f"{json.dumps(context, ensure_ascii=False, indent=2)}"
    )


# ── DDL (minimal, matches existing schema) ────────────────────────────────────

_PATTERNS_DDL = """
CREATE TABLE IF NOT EXISTS learning_memory_patterns (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_key        TEXT NOT NULL,
    event_type        TEXT NOT NULL DEFAULT '',
    vendor            TEXT,
    vendor_key        TEXT NOT NULL DEFAULT '',
    client_code       TEXT,
    client_code_key   TEXT NOT NULL DEFAULT '',
    doc_type          TEXT,
    category          TEXT,
    gl_account        TEXT,
    tax_code          TEXT,
    outcome_count     INTEGER NOT NULL DEFAULT 0,
    success_count     INTEGER NOT NULL DEFAULT 0,
    review_count      INTEGER NOT NULL DEFAULT 0,
    posted_count      INTEGER NOT NULL DEFAULT 0,
    avg_confidence    REAL NOT NULL DEFAULT 0.0,
    avg_amount        REAL,
    last_document_id  TEXT,
    last_payload_json TEXT,
    created_at        TEXT NOT NULL DEFAULT '',
    updated_at        TEXT NOT NULL DEFAULT ''
)
"""

_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS ai_response_cache (
    cache_key     TEXT PRIMARY KEY,
    task_type     TEXT NOT NULL,
    provider      TEXT NOT NULL,
    response_json TEXT NOT NULL,
    hit_count     INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    last_used_at  TEXT NOT NULL,
    expires_at    TEXT NOT NULL
)
"""

_PATTERNS_UNIQUE_IDX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_memory_patterns_memory_key
ON learning_memory_patterns(memory_key)
"""


# ── Seed logic ────────────────────────────────────────────────────────────────

def seed(db_path: Path = DB_PATH) -> dict[str, int]:
    """
    Seed both tables.  Returns {"vendors_seeded": N, "cache_entries_created": M}.
    Uses INSERT OR IGNORE so re-runs are idempotent.
    """
    now = _utc_now_iso()
    expires_at = (
        datetime.now(timezone.utc).replace(microsecond=0)
        + timedelta(days=SEED_CACHE_TTL_DAYS)
    ).isoformat()

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(_PATTERNS_DDL)
        conn.execute(_CACHE_DDL)
        conn.execute(_PATTERNS_UNIQUE_IDX)
        conn.commit()

        vendors_seeded = 0
        cache_entries_created = 0

        for vendor_name, gl_account, tax_code in VENDORS:
            vendor_key = _normalize_key(vendor_name)
            client_code_key = _normalize_key(SEED_CLIENT_CODE)

            memory_key = _build_memory_key(
                event_type=SEED_EVENT_TYPE,
                vendor=vendor_name,
                client_code=SEED_CLIENT_CODE,
                doc_type="",
                category="",
                gl_account=gl_account,
                tax_code=tax_code,
            )

            last_payload = json.dumps(
                {
                    "vendor": vendor_name,
                    "gl_account": gl_account,
                    "tax_code": tax_code,
                    "client_code": SEED_CLIENT_CODE,
                    "event_type": SEED_EVENT_TYPE,
                    "source": "seed_vendor_knowledge",
                },
                ensure_ascii=False,
            )

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO learning_memory_patterns (
                    memory_key, event_type, vendor, vendor_key,
                    client_code, client_code_key,
                    doc_type, category, gl_account, tax_code,
                    outcome_count, success_count, review_count, posted_count,
                    avg_confidence, avg_amount,
                    last_document_id, last_payload_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    memory_key,
                    SEED_EVENT_TYPE,
                    vendor_name,
                    vendor_key,
                    SEED_CLIENT_CODE,
                    client_code_key,
                    "",         # doc_type
                    "",         # category
                    gl_account,
                    tax_code,
                    SEED_OUTCOME_COUNT,
                    SEED_SUCCESS_COUNT,
                    0,          # review_count
                    SEED_OUTCOME_COUNT,  # posted_count (all successfully posted)
                    SEED_CONFIDENCE,
                    None,       # avg_amount
                    None,       # last_document_id
                    last_payload,
                    now,
                    now,
                ),
            )
            vendors_seeded += cur.rowcount

            # ── Cache entry ───────────────────────────────────────────────
            seed_prompt = _build_seed_prompt(vendor_name)
            cache_key = _build_cache_key(SEED_TASK_TYPE, seed_prompt)

            response_payload = json.dumps(
                {
                    "gl_account": gl_account,
                    "tax_code": tax_code,
                    "category": "",
                    "doc_type": "invoice",
                    "confidence": SEED_CONFIDENCE,
                    "source": "seeded",
                    "vendor": vendor_name,
                },
                ensure_ascii=False,
            )

            cur2 = conn.execute(
                """
                INSERT OR IGNORE INTO ai_response_cache
                    (cache_key, task_type, provider, response_json,
                     hit_count, created_at, last_used_at, expires_at)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    cache_key,
                    SEED_TASK_TYPE,
                    "seeded",
                    response_payload,
                    now,
                    now,
                    expires_at,
                ),
            )
            cache_entries_created += cur2.rowcount

        conn.commit()
    finally:
        conn.close()

    return {
        "vendors_seeded": vendors_seeded,
        "cache_entries_created": cache_entries_created,
    }


# ── CRA T2 Schedule 1 expense categories ─────────────────────────────────────

CRA_T2_CATEGORIES: list[tuple[str, str, str, str]] = [
    # (cra_line, gl_account_fr_en, tax_code, description)
    ("8520", "Publicité / Advertising",                           "T", "Advertising and promotion"),
    ("8590", "Mauvaises créances / Bad debts",                    "E", "Bad debts expense"),
    ("8760", "Taxes, permis et cotisations / Business tax fees licences", "E", "Business taxes, licences, dues"),
    ("8870", "Livraison et transport / Delivery freight express", "T", "Delivery, freight, express"),
    ("9224", "Carburant / Fuel costs",                            "T", "Fuel costs"),
    ("8690", "Assurances / Insurance",                            "I", "Insurance premiums"),
    ("8710", "Intérêts et frais bancaires / Interest bank charges", "E", "Interest and bank charges"),
    ("9281", "Entretien et réparations / Maintenance repairs",    "T", "Maintenance and repairs"),
    ("9270", "Honoraires de gestion / Management admin fees",     "T", "Management and admin fees"),
    ("9200", "Repas et représentation 50% / Meals entertainment", "M", "Meals and entertainment (50%)"),
    ("9281b", "Frais de véhicule / Motor vehicle expenses",       "T", "Motor vehicle expenses"),
    ("8810", "Fournitures de bureau / Office expenses",           "T", "Office expenses and supplies"),
    ("8860", "Honoraires professionnels / Professional fees",     "T", "Professional fees"),
    ("9270b", "Loyer / Rent",                                     "T", "Rent"),
    ("9060", "Salaires et avantages / Salaries benefits",         "E", "Salaries, wages, benefits"),
    ("9220", "Téléphone et services publics / Telephone utilities", "T", "Telephone and utilities"),
    ("9200b", "Voyages / Travel",                                 "T", "Travel expenses"),
    ("9270c", "Autres dépenses / Other expenses",                 "T", "Other expenses"),
]


def seed_cra_categories(db_path: Path = DB_PATH) -> int:
    """Seed CRA T2 Schedule 1 expense categories into learning_memory_patterns.

    Each category is stored as a global pattern so the system can suggest
    GL accounts based on CRA line classification.
    """
    now = _utc_now_iso()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(_PATTERNS_DDL)
        conn.execute(_PATTERNS_UNIQUE_IDX)
        conn.commit()

        count = 0
        for cra_line, gl_label, tax_code, _desc in CRA_T2_CATEGORIES:
            vendor_name = f"CRA-T2-{cra_line}"
            memory_key = _build_memory_key(
                event_type=SEED_EVENT_TYPE,
                vendor=vendor_name,
                client_code=SEED_CLIENT_CODE,
                doc_type="",
                category="cra_t2_schedule1",
                gl_account=gl_label,
                tax_code=tax_code,
            )
            last_payload = json.dumps(
                {
                    "cra_t2_line": cra_line,
                    "gl_account": gl_label,
                    "tax_code": tax_code,
                    "category": "cra_t2_schedule1",
                    "source": "seed_cra_categories",
                },
                ensure_ascii=False,
            )
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO learning_memory_patterns (
                    memory_key, event_type, vendor, vendor_key,
                    client_code, client_code_key,
                    doc_type, category, gl_account, tax_code,
                    outcome_count, success_count, review_count, posted_count,
                    avg_confidence, avg_amount,
                    last_document_id, last_payload_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    memory_key,
                    SEED_EVENT_TYPE,
                    vendor_name,
                    _normalize_key(vendor_name),
                    SEED_CLIENT_CODE,
                    _normalize_key(SEED_CLIENT_CODE),
                    "",
                    "cra_t2_schedule1",
                    gl_label,
                    tax_code,
                    SEED_OUTCOME_COUNT,
                    SEED_SUCCESS_COUNT,
                    0,
                    SEED_OUTCOME_COUNT,
                    SEED_CONFIDENCE,
                    None,
                    None,
                    last_payload,
                    now,
                    now,
                ),
            )
            count += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return count


# ── Chart of accounts & CO-17 seed wrappers ─────────────────────────────────

def seed_chart_and_co17(db_path: Path = DB_PATH) -> dict[str, int]:
    """Seed the expanded 200-account Quebec chart and CO-17 mappings."""
    from src.engines.audit_engine import (
        seed_chart_of_accounts_quebec,
        seed_co17_mappings,
    )
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        coa_count = seed_chart_of_accounts_quebec(conn)
        co17_count = seed_co17_mappings(conn)
    finally:
        conn.close()
    return {"chart_of_accounts": coa_count, "co17_mappings": co17_count}


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    print(f"Seeding vendor knowledge -> {DB_PATH}")
    result = seed()
    print(f"  Vendors seeded (learning_memory_patterns) : {result['vendors_seeded']}")
    print(f"  Cache entries created (ai_response_cache) : {result['cache_entries_created']}")

    cra_count = seed_cra_categories()
    print(f"  CRA T2 categories seeded                  : {cra_count}")

    chart_result = seed_chart_and_co17()
    print(f"  Chart of accounts entries created          : {chart_result['chart_of_accounts']}")
    print(f"  CO-17 mappings created                     : {chart_result['co17_mappings']}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
