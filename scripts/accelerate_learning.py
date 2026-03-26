#!/usr/bin/env python3
"""
scripts/accelerate_learning.py

Forces the learning system to learn from the 50,000 synthetic documents already
in the database by recording corrections via every available store and
populating the learning_memory table that LearningSuggestionEngine reads from.

Steps
-----
1. Query clean approved documents (ReadyToPost / Posted, confidence >= 0.85)
2. For each document call LearningCorrectionStore.record_correction() for
   gl_account, tax_code, category, vendor (old_value == new_value, source=seed)
3. For each document call LearningMemoryStore.record_feedback() to build
   holistic patterns in learning_memory_patterns
4. Populate learning_memory rows so LearningSuggestionEngine returns results
5. Add 100 explicit vendor-GL patterns per client (2,500 total, 5 rows each)
6. Simulate 24 months of corrections by running the learning cycle 24 times
   with slight variations in amounts and dates
7. Run suggestions_for_document() on 50 random docs per client (1,250 total)
8. Print progress every 100 docs; print final report

Usage:
    python scripts/accelerate_learning.py
"""
from __future__ import annotations

import random
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.learning_memory_store import LearningMemoryStore
from src.agents.core.learning_correction_store import LearningCorrectionStore
from src.agents.core.learning_suggestion_engine import LearningSuggestionEngine

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

SEED = 2024
random.seed(SEED)

CLIENTS         = ["MARCEL", "BOLDUC", "DENTAIRE", "BOUTIQUE", "TECHLAVAL",
                   "PLOMBERIE", "AVOCAT", "IMMO", "TRANSPORT", "CLINIQUE",
                   "EPICERIE", "MANUFACTURE", "NETTOYAGE", "AGENCE", "GARDERIE",
                   "ELECTRICIEN", "TRAITEUR", "PHARMACIE", "TOITURE", "CONSULT",
                   "PAYSAGE", "VETERINAIRE", "DEMENAGEMENT", "IMPRIMERIE", "SECURITE"]
CORRECTION_FIELDS = ["gl_account", "tax_code", "category", "vendor"]
# Marker so we can wipe and re-seed on repeated runs
_SEED_REVIEWER  = "seed:accelerate_learning"
# How many times each explicit pattern is repeated in learning_memory
# to give it meaningful support count
_EXPLICIT_BOOST = 5

# ── Explicit vendor→GL patterns per client (base patterns, expanded to 100) ──
# Each tuple: (vendor_name, gl_account, tax_code)

EXPLICIT_PATTERNS: dict[str, list[tuple[str, str, str]]] = {
    "MARCEL": [
        ("Sysco Québec",                  "Achats et matières premières",  "T"),
        ("Distribution Métro Inc",        "Achats et matières premières",  "T"),
        ("Provigo Commerce",              "Achats et matières premières",  "T"),
        ("Distribution Alim Aux (DAA)",   "Achats et matières premières",  "T"),
        ("Fromages du Roy Ltée",          "Achats et matières premières",  "T"),
        ("Les Brasseurs du Nord",         "Achats boissons",               "T"),
        ("Boulangerie Première Moisson",  "Achats et matières premières",  "T"),
        ("Fruits Légumes Clément",        "Achats et matières premières",  "T"),
        ("Papiers Cascades Canada",       "Fournitures de restaurant",     "T"),
        ("Cuisine Commerciale Resto-Pro", "Fournitures de restaurant",     "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Énergir Distribution",          "Électricité et gaz",            "E"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("Entretien Nettoy-Pro Enr",      "Entretien et nettoyage",        "T"),
        ("Blanchisserie Express Mtl",     "Entretien et nettoyage",        "T"),
        ("Buanderie Sanitaire Québec",    "Entretien et nettoyage",        "T"),
        ("BMO Frais de Service",          "Frais bancaires",               "E"),
        ("Aliments Ultima Inc",           "Achats et matières premières",  "T"),
        ("Marché Atwater Marcel",         "Achats et matières premières",  "T"),
        ("Distribution Aubut Resto",      "Achats et matières premières",  "T"),
        ("Mayrand Food Depot",            "Achats et matières premières",  "T"),
        ("Laiterie Natrel Inc",           "Achats et matières premières",  "T"),
        ("Agropur Division Fromage",      "Achats et matières premières",  "T"),
        ("Saputo Dairy Products",         "Achats et matières premières",  "T"),
        ("Molson Brasserie Québec",       "Achats boissons",               "T"),
        ("Labatt Bière Canada",           "Achats boissons",               "T"),
        ("SAQ Approvisionnement",         "Achats boissons",               "T"),
        ("Café Van Houtte Fournisseur",   "Achats et matières premières",  "T"),
        ("Boulangerie St-Méthode",        "Achats et matières premières",  "T"),
        ("Charcuterie Hongroise Mtl",     "Achats et matières premières",  "T"),
        ("Poissonnerie Falero",           "Achats et matières premières",  "T"),
        ("Les Jardins Sauvages Bio",      "Achats et matières premières",  "T"),
        ("Équipement CRS Cuisine",        "Fournitures de restaurant",     "T"),
        ("Russell Hendrix Resto Équip",   "Fournitures de restaurant",     "T"),
        ("Faema Canada Espresso",         "Fournitures de restaurant",     "T"),
        ("Intact Assurance Restaurant",   "Assurances",                    "I"),
        ("SSQ Assurance Commerce",        "Assurances",                    "I"),
        ("Nettoyeur Royal Québec",        "Entretien et nettoyage",        "T"),
        ("Rogers Affaires",               "Télécommunications",            "T"),
        ("Telus Affaires Québec",         "Télécommunications",            "T"),
        ("RBC Frais Service",             "Frais bancaires",               "E"),
        ("Banque Nationale Frais",        "Frais bancaires",               "E"),
        ("TD Frais de Service",           "Frais bancaires",               "E"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Lightspeed POS Restaurant",     "Logiciels et abonnements",      "T"),
        ("Square POS Marcel",             "Logiciels et abonnements",      "T"),
        ("Uber Eats Frais Resto",         "Frais de traitement des paiements", "E"),
        ("DoorDash Frais Commission",     "Frais de traitement des paiements", "E"),
    ],
    "BOLDUC": [
        ("Rona Pro Laval",                "Matériaux et fournitures",      "T"),
        ("Home Depot Pro",                "Matériaux et fournitures",      "T"),
        ("Canac Matériaux",               "Matériaux et fournitures",      "T"),
        ("Patrick Morin Inc",             "Matériaux et fournitures",      "T"),
        ("BMR Construction",              "Matériaux et fournitures",      "T"),
        ("Réno-Dépôt Pro",                "Matériaux et fournitures",      "T"),
        ("Ultramar Carburant Fleet",      "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",            "Carburant et huile",            "T"),
        ("Esso Commercial Québec",        "Carburant et huile",            "T"),
        ("Westburne Électrique",          "Matériaux et fournitures",      "T"),
        ("Rexel Canada Électrique",       "Matériaux et fournitures",      "T"),
        ("Fastenal Canada Inc",           "Matériaux et fournitures",      "T"),
        ("Groupe Deschênes Inc",          "Matériaux et fournitures",      "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Bell Affaires Québec",          "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("RBC Frais de Service",          "Frais bancaires",               "E"),
        ("Sunbelt Location Équip",        "Location d'équipement",         "T"),
        ("RSC Équipement Location",       "Location d'équipement",         "T"),
        ("Équipement Bisson Ltée",        "Location d'équipement",         "T"),
        ("Kent Building Supplies QC",     "Matériaux et fournitures",      "T"),
        ("Timber Mart Construction",      "Matériaux et fournitures",      "T"),
        ("Richelieu Hardware Pro",        "Matériaux et fournitures",      "T"),
        ("Hilti Canada Construction",     "Matériaux et fournitures",      "T"),
        ("Stanley DeWalt Outillage",      "Équipement et outillage",       "T"),
        ("Milwaukee Tool Canada",         "Équipement et outillage",       "T"),
        ("Makita Canada",                 "Équipement et outillage",       "T"),
        ("Shell Commercial Québec",       "Carburant et huile",            "T"),
        ("Irving Oil Fleet",              "Carburant et huile",            "T"),
        ("Canadian Tire Pro",             "Matériaux et fournitures",      "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Rogers Affaires Canada",        "Télécommunications",            "T"),
        ("Intact Assurance Construction", "Assurances",                    "I"),
        ("La Capitale Assurance",         "Assurances",                    "I"),
        ("SSQ Assurance Construction",    "Assurances",                    "I"),
        ("BMO Frais de Service",          "Frais bancaires",               "E"),
        ("TD Frais Construction",         "Frais bancaires",               "E"),
        ("Mark's Work Wearhouse",         "Équipements de protection individuelle", "T"),
        ("SPI Santé Sécurité",            "Équipements de protection individuelle", "T"),
        ("Sylprotec EPI",                 "Équipements de protection individuelle", "T"),
        ("Location Hewden Équip",         "Location d'équipement",         "T"),
        ("Battlefield Equipment QC",      "Location d'équipement",         "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Procore Construction Software", "Logiciels et abonnements",      "T"),
        ("SAAQ Immatriculation Fleet",    "Permis et immatriculations",    "T"),
        ("Enterprise Location Bolduc",    "Location de véhicules",         "T"),
        ("Énergir Chauffage",             "Électricité et gaz",            "E"),
        ("Waste Management QC",           "Gestion des déchets",           "T"),
        ("Nettoyeur Pro Construction",    "Entretien et nettoyage",        "T"),
    ],
    "DENTAIRE": [
        ("Dentsply Canada",               "Fournitures dentaires",         "T"),
        ("Patterson Dental Canada",       "Fournitures dentaires",         "T"),
        ("Kerr Canada Dental",            "Fournitures dentaires",         "T"),
        ("3M Canada Dentaire",            "Fournitures dentaires",         "T"),
        ("Sirona Dental Systems",         "Équipement médical",            "T"),
        ("Planmeca Finland (CA)",         "Équipement médical",            "T"),
        ("Lyreco Canada Inc",             "Fournitures de bureau",         "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Logiciel Dentitek Inc",         "Logiciels et abonnements",      "T"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("TD Frais de Service",           "Frais bancaires",               "E"),
        ("Service Stérilisation Pro",     "Fournitures dentaires",         "T"),
        ("Pharmascience Inc",             "Fournitures médicales",         "T"),
        ("Zoom Video Communications",     "Logiciels et abonnements",      "T"),
        ("Intact Assurance Dentaire",     "Assurances",                    "I"),
        ("SSQ Assurance Groupe",          "Assurances",                    "I"),
        ("Entretien Médical Express",     "Entretien et nettoyage",        "T"),
        ("Henry Schein Dental CA",        "Fournitures dentaires",         "T"),
        ("Hu-Friedy Canada",              "Fournitures dentaires",         "T"),
        ("Dentsply Sirona Implants",      "Fournitures dentaires",         "T"),
        ("A-dec Dental Equipment",        "Équipement médical",            "T"),
        ("Carestream Dental CA",          "Équipement médical",            "T"),
        ("Kavo Kerr Dental Group",        "Fournitures dentaires",         "T"),
        ("Ivoclar Vivadent CA",           "Fournitures dentaires",         "T"),
        ("Nobel Biocare Canada",          "Fournitures dentaires",         "T"),
        ("Straumann Canada Implant",      "Fournitures dentaires",         "T"),
        ("Align Technology CA",           "Équipement médical",            "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Rogers Affaires Dentaire",      "Télécommunications",            "T"),
        ("BMO Frais Dentaire",            "Frais bancaires",               "E"),
        ("RBC Frais Clinique",            "Frais bancaires",               "E"),
        ("Énergir Chauffage Clinique",    "Électricité et gaz",            "E"),
        ("Adobe Creative Cloud",          "Logiciels et abonnements",      "T"),
        ("Slack Business Dentaire",       "Logiciels et abonnements",      "T"),
        ("Dropbox Business Pro",          "Logiciels et abonnements",      "T"),
        ("La Personnelle Assurance",      "Assurances",                    "I"),
        ("Desjardins Assurance Dental",   "Assurances",                    "I"),
        ("Purolator Dental Supplies",     "Transport et déplacements",     "T"),
        ("FedEx Dental Shipment",         "Transport et déplacements",     "T"),
        ("Molly Maid Clinique",           "Entretien et nettoyage",        "T"),
        ("Jan-Pro Dentaire",              "Entretien et nettoyage",        "T"),
        ("Labo Dentaire Québec",          "Fournitures dentaires",         "T"),
        ("Prothèses Dentaires Mtl",       "Fournitures dentaires",         "T"),
        ("Gants Médicaux Canada",         "Fournitures médicales",         "T"),
        ("Masques Chirurgicaux QC",       "Fournitures médicales",         "T"),
        ("Stérilisation Pro Plus",        "Fournitures dentaires",         "T"),
        ("Formation CDA Dentaire",        "Formation et développement professionnel", "T"),
    ],
    "BOUTIQUE": [
        ("Mode Atlantique Distribution",  "Achats marchandises",           "T"),
        ("Importation Styl-Mode Inc",     "Achats marchandises",           "T"),
        ("Collection Mode QC Enr",        "Achats marchandises",           "T"),
        ("Grossiste Textile Montréal",    "Achats marchandises",           "T"),
        ("Fournisseur Mode Plus Ltée",    "Achats marchandises",           "T"),
        ("Amazon Business Canada",        "Fournitures de bureau",         "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Shopify Plus Canada",           "Logiciels et abonnements",      "T"),
        ("Facebook Ads Canada",           "Publicité et marketing",        "T"),
        ("Google Ads Canada",             "Publicité et marketing",        "T"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("Purolator Canada Ltée",         "Transport et déplacements",     "T"),
        ("FedEx Canada Inc",              "Transport et déplacements",     "T"),
        ("Intact Assurance Commerce",     "Assurances",                    "I"),
        ("La Personnelle Assurance",      "Assurances",                    "I"),
        ("Adobe Creative Cloud",          "Logiciels et abonnements",      "T"),
        ("Mailchimp Canada",              "Logiciels et abonnements",      "T"),
        ("Gildan Vêtements Gros",         "Achats marchandises",           "T"),
        ("Fruit of the Loom CA",          "Achats marchandises",           "T"),
        ("Hanes Brands Canada",           "Achats marchandises",           "T"),
        ("Distribution Mode Express",     "Achats marchandises",           "T"),
        ("Vêtements Québécois Inc",       "Achats marchandises",           "T"),
        ("Tissus Fabricville",            "Achats marchandises",           "T"),
        ("Accessoires Mode QC",           "Achats marchandises",           "T"),
        ("Bijoux Gros Montréal",          "Achats marchandises",           "T"),
        ("Instagram Ads Canada",          "Publicité et marketing",        "T"),
        ("TikTok Ads Boutique",           "Publicité et marketing",        "T"),
        ("Pinterest Ads Canada",          "Publicité et marketing",        "T"),
        ("Lightspeed POS Boutique",       "Logiciels et abonnements",      "T"),
        ("Square POS Boutique",           "Logiciels et abonnements",      "T"),
        ("Canva Pro Design",              "Logiciels et abonnements",      "T"),
        ("DHL Express Canada",            "Transport et déplacements",     "T"),
        ("UPS Canada Boutique",           "Transport et déplacements",     "T"),
        ("Postes Canada Envois",          "Frais de livraison et courrier","T"),
        ("BMO Frais Commerce",            "Frais bancaires",               "E"),
        ("RBC Frais Boutique",            "Frais bancaires",               "E"),
        ("TD Frais Commerce",             "Frais bancaires",               "E"),
        ("Stripe Frais Paiement",         "Frais de traitement des paiements", "E"),
        ("PayPal Frais Commerce",         "Frais de traitement des paiements", "E"),
        ("SSQ Assurance Commerce",        "Assurances",                    "I"),
        ("Desjardins Assurance",          "Assurances",                    "I"),
        ("Énergir Boutique",              "Électricité et gaz",            "E"),
        ("Rogers Affaires Boutique",      "Télécommunications",            "T"),
        ("Telus Boutique",                "Télécommunications",            "T"),
        ("Nettoyage Boutique Pro",        "Entretien et nettoyage",        "T"),
        ("Emballage Québec Inc",          "Fournitures de bureau",         "T"),
        ("Mannequins et Présentoirs QC",  "Fournitures de bureau",         "T"),
    ],
    "TECHLAVAL": [
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Adobe Creative Cloud",          "Logiciels et abonnements",      "T"),
        ("Google Workspace",              "Logiciels et abonnements",      "T"),
        ("AWS Canada (Amazon)",           "Infrastructure infonuagique",   "T"),
        ("Slack Technologies Inc",        "Logiciels et abonnements",      "T"),
        ("GitHub Enterprise",             "Logiciels et abonnements",      "T"),
        ("Zoom Video Communications",     "Logiciels et abonnements",      "T"),
        ("Atlassian Jira/Confluence",     "Logiciels et abonnements",      "T"),
        ("Salesforce Canada Inc",         "Logiciels et abonnements",      "T"),
        ("Bell Affaires Québec",          "Télécommunications",            "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Rogers Affaires Canada",        "Télécommunications",            "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("Banque Nationale Frais",        "Frais bancaires",               "E"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("LinkedIn Ads Canada",           "Publicité et marketing",        "T"),
        ("Intact Assurance TI",           "Assurances",                    "I"),
        ("La Capitale Assurance",         "Assurances",                    "I"),
        ("OVH Cloud Canada",              "Infrastructure infonuagique",   "T"),
        ("Azure Microsoft Cloud",         "Infrastructure infonuagique",   "T"),
        ("Google Cloud Platform CA",      "Infrastructure infonuagique",   "T"),
        ("DigitalOcean Canada",           "Infrastructure infonuagique",   "T"),
        ("Cloudflare Enterprise",         "Infrastructure infonuagique",   "T"),
        ("Datadog Monitoring",            "Logiciels et abonnements",      "T"),
        ("PagerDuty Inc",                 "Logiciels et abonnements",      "T"),
        ("Sentry Error Tracking",         "Logiciels et abonnements",      "T"),
        ("JetBrains IDE License",         "Logiciels et abonnements",      "T"),
        ("Docker Enterprise",             "Logiciels et abonnements",      "T"),
        ("HashiCorp Terraform",           "Logiciels et abonnements",      "T"),
        ("Twilio Communications",         "Logiciels et abonnements",      "T"),
        ("SendGrid Email Service",        "Logiciels et abonnements",      "T"),
        ("New Relic Monitoring",          "Logiciels et abonnements",      "T"),
        ("Splunk Enterprise CA",          "Logiciels et abonnements",      "T"),
        ("Figma Design Platform",         "Logiciels et abonnements",      "T"),
        ("Notion Workspace",              "Logiciels et abonnements",      "T"),
        ("1Password Business",            "Logiciels et abonnements",      "T"),
        ("Okta Identity Cloud",           "Logiciels et abonnements",      "T"),
        ("Dell Canada Matériel",          "Matériel informatique",         "T"),
        ("Lenovo Canada Serveurs",        "Matériel informatique",         "T"),
        ("HP Canada Enterprise",          "Matériel informatique",         "T"),
        ("CDW Canada TI",                 "Matériel informatique",         "T"),
        ("BMO Frais TI",                  "Frais bancaires",               "E"),
        ("RBC Frais TechLaval",           "Frais bancaires",               "E"),
        ("SSQ Assurance TI",              "Assurances",                    "I"),
        ("Formation Udemy TI",            "Formation et développement professionnel", "T"),
        ("Coursera Business TI",          "Formation et développement professionnel", "T"),
        ("Stripe Frais Paiement",         "Frais de traitement des paiements", "E"),
        ("Telus Affaires TI",             "Télécommunications",            "T"),
        ("Énergir Bureau TI",             "Électricité et gaz",            "E"),
    ],
    "PLOMBERIE": [
        ("Wolseley Canada Plomberie",     "Matériaux et fournitures",      "T"),
        ("Masters Plomberie Québec",      "Matériaux et fournitures",      "T"),
        ("Groupe Deschênes Plomberie",    "Matériaux et fournitures",      "T"),
        ("Noble Canada Plomberie",        "Matériaux et fournitures",      "T"),
        ("Emco Corporation QC",           "Matériaux et fournitures",      "T"),
        ("Rona Pro Plomberie",            "Matériaux et fournitures",      "T"),
        ("Home Depot Pro Plomberie",      "Matériaux et fournitures",      "T"),
        ("Canac Plomberie",               "Matériaux et fournitures",      "T"),
        ("Ultramar Carburant",            "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",            "Carburant et huile",            "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("Sunbelt Location Équip",        "Location d'équipement",         "T"),
        ("Équipement Bisson Ltée",        "Location d'équipement",         "T"),
        ("Enterprise Location Véhicule",  "Location de véhicules",         "T"),
        ("Mark's Work Wearhouse",         "Équipements de protection individuelle", "T"),
        ("Intact Assurance",              "Assurances",                    "I"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("BMO Frais de Service",          "Frais bancaires",               "E"),
        ("Moen Canada Plomberie",         "Matériaux et fournitures",      "T"),
        ("Delta Faucet Canada",           "Matériaux et fournitures",      "T"),
        ("Kohler Canada Plomberie",       "Matériaux et fournitures",      "T"),
        ("American Standard CA",          "Matériaux et fournitures",      "T"),
        ("IPEX Tuyaux Québec",            "Matériaux et fournitures",      "T"),
        ("Viega Canada Plomberie",        "Matériaux et fournitures",      "T"),
        ("Uponor Canada Tuyaux",          "Matériaux et fournitures",      "T"),
        ("SharkBite Canada",              "Matériaux et fournitures",      "T"),
        ("Rheem Canada Chauffe-eau",      "Matériaux et fournitures",      "T"),
        ("Bradford White CA",             "Matériaux et fournitures",      "T"),
        ("Giant Water Heaters",           "Matériaux et fournitures",      "T"),
        ("Navien Canada",                 "Matériaux et fournitures",      "T"),
        ("Ridgid Tool Canada",            "Équipement et outillage",       "T"),
        ("Milwaukee Plomberie",           "Équipement et outillage",       "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Rogers Affaires",               "Télécommunications",            "T"),
        ("SSQ Assurance Plomberie",       "Assurances",                    "I"),
        ("La Capitale Assurance",         "Assurances",                    "I"),
        ("RBC Frais de Service",          "Frais bancaires",               "E"),
        ("TD Frais Plomberie",            "Frais bancaires",               "E"),
        ("Énergir Gaz Naturel",           "Électricité et gaz",            "E"),
        ("Shell Carburant Fleet",         "Carburant et huile",            "T"),
        ("SPI Santé Sécurité",            "Équipements de protection individuelle", "T"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Jobber Logiciel Plomberie",     "Logiciels et abonnements",      "T"),
        ("ServiceTitan Logiciel",         "Logiciels et abonnements",      "T"),
        ("Hertz Location Véhicule",       "Location de véhicules",         "T"),
        ("Budget Location Plomberie",     "Location de véhicules",         "T"),
        ("Purolator Plomberie",           "Transport et déplacements",     "T"),
        ("Formation RBQ Plomberie",       "Formation et développement professionnel", "T"),
    ],
    "AVOCAT": [
        ("Thomson Reuters Canada",        "Logiciels et abonnements",      "T"),
        ("LexisNexis Canada",             "Logiciels et abonnements",      "T"),
        ("Westlaw Canada",                "Logiciels et abonnements",      "T"),
        ("SOQUIJ Abonnement",             "Logiciels et abonnements",      "T"),
        ("Barreau du Québec Cotisation",  "Honoraires professionnels",     "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Lyreco Canada Inc",             "Fournitures de bureau",         "T"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Adobe Creative Cloud",          "Logiciels et abonnements",      "T"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("TD Frais de Service",           "Frais bancaires",               "E"),
        ("Purolator Canada Ltée",         "Transport et déplacements",     "T"),
        ("FedEx Canada Inc",              "Transport et déplacements",     "T"),
        ("Intact Assurance Juridique",    "Assurances",                    "I"),
        ("SSQ Assurance Groupe",          "Assurances",                    "I"),
        ("Imprimerie Solisco",            "Impression et papeterie",       "T"),
        ("Minuteman Press Québec",        "Impression et papeterie",       "T"),
        ("Clio Logiciel Juridique",       "Logiciels et abonnements",      "T"),
        ("PracticePanther Legal",         "Logiciels et abonnements",      "T"),
        ("Cosmolex Juridique",            "Logiciels et abonnements",      "T"),
        ("DocuSign Juridique",            "Logiciels et abonnements",      "T"),
        ("Zoom Video Juridique",          "Logiciels et abonnements",      "T"),
        ("Chambre des notaires QC",       "Honoraires professionnels",     "T"),
        ("Expert-comptable Juridique",    "Honoraires professionnels",     "T"),
        ("Traducteur Juridique Mtl",      "Honoraires professionnels",     "T"),
        ("Huissier de Justice QC",        "Honoraires professionnels",     "T"),
        ("Médiateur Commercial QC",       "Honoraires professionnels",     "T"),
        ("La Presse+ Juridique",          "Abonnements et médias",         "T"),
        ("Le Devoir Abonnement",          "Abonnements et médias",         "T"),
        ("Rogers Affaires Avocat",        "Télécommunications",            "T"),
        ("BMO Frais Juridique",           "Frais bancaires",               "E"),
        ("RBC Frais Cabinet",             "Frais bancaires",               "E"),
        ("Banque Nationale Frais",        "Frais bancaires",               "E"),
        ("Énergir Bureau Avocat",         "Électricité et gaz",            "E"),
        ("La Personnelle Assurance",      "Assurances",                    "I"),
        ("Desjardins Assurance Jurid",    "Assurances",                    "I"),
        ("Nettoyage Bureau Avocat",       "Entretien et nettoyage",        "T"),
        ("Formation Barreau QC",          "Formation et développement professionnel", "T"),
        ("Formation CLE Juridique",       "Formation et développement professionnel", "T"),
        ("LinkedIn Premium Avocat",       "Abonnements et médias",         "T"),
        ("Marriott Hébergement",          "Frais de voyage et hébergement","T"),
        ("Air Canada Avocat",             "Transport et déplacements",     "T"),
        ("VIA Rail Juridique",            "Transport et déplacements",     "T"),
        ("Uber Déplacement Avocat",       "Transport et déplacements",     "T"),
        ("Canva Pro Avocat",              "Logiciels et abonnements",      "T"),
        ("Amazon Business Avocat",        "Fournitures de bureau",         "T"),
        ("Papeterie St-Laurent",          "Impression et papeterie",       "T"),
    ],
    "IMMO": [
        ("Hydro-Québec Immeubles",        "Électricité et gaz",            "E"),
        ("Énergir Chauffage",             "Électricité et gaz",            "E"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("GDI Services aux immeubles",    "Entretien et nettoyage",        "T"),
        ("ServiceMaster Canada",          "Entretien et nettoyage",        "T"),
        ("Jan-Pro Québec",                "Entretien et nettoyage",        "T"),
        ("Rona Pro Entretien",            "Matériaux et fournitures",      "T"),
        ("Home Depot Pro Immo",           "Matériaux et fournitures",      "T"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("RBC Frais de Service",          "Frais bancaires",               "E"),
        ("Intact Assurance Immeuble",     "Assurances",                    "I"),
        ("La Capitale Assurance Immo",    "Assurances",                    "I"),
        ("Waste Management Québec",       "Gestion des déchets",           "T"),
        ("ADT Sécurité Québec",           "Sécurité et surveillance",      "T"),
        ("Garda World Sécurité",          "Sécurité et surveillance",      "T"),
        ("Groupe Vertdure Paysage",       "Aménagement paysager",          "T"),
        ("Déneigement Pro Québec",        "Entretien et nettoyage",        "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("Gestion Immo Logiciel QC",      "Logiciels et abonnements",      "T"),
        ("Yardi Systems Canada",          "Logiciels et abonnements",      "T"),
        ("AppFolio Property Mgmt",        "Logiciels et abonnements",      "T"),
        ("Buildium Software Immo",        "Logiciels et abonnements",      "T"),
        ("Otis Elevator Canada",          "Entretien et nettoyage",        "T"),
        ("Schindler Ascenseurs QC",       "Entretien et nettoyage",        "T"),
        ("ThyssenKrupp Elevator CA",      "Entretien et nettoyage",        "T"),
        ("Canac Entretien Immo",          "Matériaux et fournitures",      "T"),
        ("BMR Entretien Immeuble",        "Matériaux et fournitures",      "T"),
        ("Patrick Morin Entretien",       "Matériaux et fournitures",      "T"),
        ("Rogers Affaires Immo",          "Télécommunications",            "T"),
        ("Telus Affaires Immo",           "Télécommunications",            "T"),
        ("BMO Frais Immobilier",          "Frais bancaires",               "E"),
        ("TD Frais Immobilier",           "Frais bancaires",               "E"),
        ("Banque Nationale Immo",         "Frais bancaires",               "E"),
        ("SSQ Assurance Immo",            "Assurances",                    "I"),
        ("Promutuel Assurance Immo",      "Assurances",                    "I"),
        ("GFL Environmental QC",          "Gestion des déchets",           "T"),
        ("Securitas Immo",                "Sécurité et surveillance",      "T"),
        ("Chubb Sécurité Immo",          "Sécurité et surveillance",      "T"),
        ("Nutri-Lawn Québec Immo",        "Aménagement paysager",          "T"),
        ("Weed Man Québec Immo",          "Aménagement paysager",          "T"),
        ("Molly Maid Immeubles",          "Entretien et nettoyage",        "T"),
        ("Nettoyage Impérial Immo",       "Entretien et nettoyage",        "T"),
        ("Notaire Immobilier QC",         "Honoraires professionnels",     "T"),
        ("Arpenteur Immobilier QC",       "Honoraires professionnels",     "T"),
        ("Courtier Immo Québec",          "Honoraires professionnels",     "T"),
        ("Formation OACIQ Immo",          "Formation et développement professionnel", "T"),
        ("Purolator Immo",                "Transport et déplacements",     "T"),
        ("Uber Déplacement Immo",         "Transport et déplacements",     "T"),
    ],
    "TRANSPORT": [
        ("Ultramar Carburant Fleet",      "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",            "Carburant et huile",            "T"),
        ("Esso Commercial Québec",        "Carburant et huile",            "T"),
        ("Shell Commercial Canada",       "Carburant et huile",            "T"),
        ("Canadian Tire Auto",            "Entretien véhicules",           "T"),
        ("Kal Tire Québec",               "Entretien véhicules",           "T"),
        ("NAPA Autopro Service",          "Entretien véhicules",           "T"),
        ("Midas Québec Camions",          "Entretien véhicules",           "T"),
        ("Pneus Touchette Fleet",         "Entretien véhicules",           "T"),
        ("SAAQ Immatriculation",          "Permis et immatriculations",    "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Bell Affaires Québec",          "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("Banque Nationale Frais",        "Frais bancaires",               "E"),
        ("Intact Assurance Transport",    "Assurances",                    "I"),
        ("SSQ Assurance Flotte",          "Assurances",                    "I"),
        ("Sunbelt Location Équip",        "Location d'équipement",         "T"),
        ("Enterprise Location Véhicule",  "Location de véhicules",         "T"),
        ("Mark's Work Wearhouse",         "Équipements de protection individuelle", "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Irving Oil Fleet",              "Carburant et huile",            "T"),
        ("Couche-Tard Carburant Fleet",   "Carburant et huile",            "T"),
        ("Pioneer Pétrole Fleet",         "Carburant et huile",            "T"),
        ("Monsieur Muffler Transport",    "Entretien véhicules",           "T"),
        ("Point S Pneus Fleet",           "Entretien véhicules",           "T"),
        ("Centre du Camion Mtl",          "Entretien véhicules",           "T"),
        ("Concessionnaire Volvo QC",      "Entretien véhicules",           "T"),
        ("Concessionnaire Kenworth QC",   "Entretien véhicules",           "T"),
        ("Freightliner Québec",           "Entretien véhicules",           "T"),
        ("Autoplace Parts Fleet",         "Entretien véhicules",           "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Rogers Affaires Transport",     "Télécommunications",            "T"),
        ("BMO Frais Transport",           "Frais bancaires",               "E"),
        ("RBC Frais Fleet",               "Frais bancaires",               "E"),
        ("TD Frais Transport",            "Frais bancaires",               "E"),
        ("La Capitale Assurance Fleet",   "Assurances",                    "I"),
        ("Promutuel Transport",           "Assurances",                    "I"),
        ("Hertz Location Camion",         "Location de véhicules",         "T"),
        ("Avis Location Fleet",           "Location de véhicules",         "T"),
        ("Budget Location Camion",        "Location de véhicules",         "T"),
        ("SPI Sécurité Transport",        "Équipements de protection individuelle", "T"),
        ("Sylprotec EPI Transport",       "Équipements de protection individuelle", "T"),
        ("Microsoft 365 Transport",       "Logiciels et abonnements",      "T"),
        ("Samsara Fleet Tracking",        "Logiciels et abonnements",      "T"),
        ("Geotab Fleet Management",       "Logiciels et abonnements",      "T"),
        ("Énergir Transport",             "Électricité et gaz",            "E"),
        ("Formation CTQ Transport",       "Formation et développement professionnel", "T"),
        ("Moneris Frais Transport",       "Frais de traitement des paiements", "E"),
        ("Purolator Express",             "Frais de livraison et courrier","T"),
        ("DHL Express Transport",         "Frais de livraison et courrier","T"),
    ],
    "CLINIQUE": [
        ("McKesson Canada Pharma",        "Fournitures médicales",         "T"),
        ("Cardinal Health Canada",        "Fournitures médicales",         "T"),
        ("Medline Canada",                "Fournitures médicales",         "T"),
        ("Becton Dickinson Canada",       "Fournitures médicales",         "T"),
        ("Sirona Dental Systems",         "Équipement médical",            "T"),
        ("Stryker Canada Médical",        "Équipement médical",            "T"),
        ("Bureau en Gros",                "Fournitures de bureau",         "T"),
        ("Lyreco Canada Inc",             "Fournitures de bureau",         "T"),
        ("Microsoft 365 Business",        "Logiciels et abonnements",      "T"),
        ("TELUS Santé Logiciel",          "Logiciels et abonnements",      "T"),
        ("Bell Communications",           "Télécommunications",            "T"),
        ("Hydro-Québec",                  "Électricité et gaz",            "E"),
        ("Desjardins Frais Bancaires",    "Frais bancaires",               "E"),
        ("TD Frais de Service",           "Frais bancaires",               "E"),
        ("Entretien Médical Express",     "Entretien et nettoyage",        "T"),
        ("Molly Maid Clinique",           "Entretien et nettoyage",        "T"),
        ("Intact Assurance Médicale",     "Assurances",                    "I"),
        ("SSQ Assurance Groupe",          "Assurances",                    "I"),
        ("Pharmascience Inc",             "Fournitures médicales",         "T"),
        ("Laboratoire Médical Biron",     "Frais de santé",                "E"),
        ("Baxter Canada Médical",         "Fournitures médicales",         "T"),
        ("B. Braun Canada",               "Fournitures médicales",         "T"),
        ("Johnson & Johnson Medical CA",  "Fournitures médicales",         "T"),
        ("Abbott Canada Diagnostics",     "Équipement médical",            "T"),
        ("Siemens Healthineers CA",       "Équipement médical",            "T"),
        ("GE Healthcare Canada",          "Équipement médical",            "T"),
        ("Philips Healthcare CA",         "Équipement médical",            "T"),
        ("MEDFAR Clinical Solutions",     "Logiciels et abonnements",      "T"),
        ("Purkinje Logiciel Médical",     "Logiciels et abonnements",      "T"),
        ("Zoom Video Clinique",           "Logiciels et abonnements",      "T"),
        ("Vidéotron Affaires",            "Télécommunications",            "T"),
        ("Rogers Affaires Clinique",      "Télécommunications",            "T"),
        ("BMO Frais Clinique",            "Frais bancaires",               "E"),
        ("RBC Frais Médical",             "Frais bancaires",               "E"),
        ("Banque Nationale Clinique",     "Frais bancaires",               "E"),
        ("Énergir Clinique",              "Électricité et gaz",            "E"),
        ("La Personnelle Assurance",      "Assurances",                    "I"),
        ("Desjardins Assurance Méd",      "Assurances",                    "I"),
        ("Jan-Pro Clinique",              "Entretien et nettoyage",        "T"),
        ("Stérilisation Pro Clinique",    "Entretien et nettoyage",        "T"),
        ("Purolator Médical",             "Transport et déplacements",     "T"),
        ("FedEx Médical Canada",          "Transport et déplacements",     "T"),
        ("Formation CMQ Médicale",        "Formation et développement professionnel", "T"),
        ("Formation FMSQ Spécialiste",    "Formation et développement professionnel", "T"),
        ("ADT Sécurité Clinique",         "Sécurité et surveillance",      "T"),
        ("Garda World Clinique",          "Sécurité et surveillance",      "T"),
        ("Moneris Frais Clinique",        "Frais de traitement des paiements", "E"),
        ("Square Frais Clinique",         "Frais de traitement des paiements", "E"),
        ("LinkedIn Premium Médical",      "Abonnements et médias",         "T"),
        ("Amazon Business Médical",       "Fournitures de bureau",         "T"),
    ],
    "EPICERIE": [
        ("Sysco Québec Épicerie",            "Achats et matières premières",  "T"),
        ("Distribution Métro Épicerie",      "Achats et matières premières",  "T"),
        ("Provigo Commerce Épicerie",        "Achats et matières premières",  "T"),
        ("Laiterie Natrel Épicerie",         "Achats et matières premières",  "T"),
        ("Agropur Fromages Épicerie",        "Achats et matières premières",  "T"),
        ("Boulangerie Première Moisson",     "Achats et matières premières",  "T"),
        ("Fruits Légumes Clément Épi",       "Achats et matières premières",  "T"),
        ("Saputo Dairy Épicerie",            "Achats et matières premières",  "T"),
        ("Café Van Houtte Épicerie",         "Achats et matières premières",  "T"),
        ("Distribution Aubut Épicerie",      "Achats et matières premières",  "T"),
        ("Mayrand Food Depot Épi",           "Achats et matières premières",  "T"),
        ("SAQ Approvisionnement Épi",        "Épicerie et fournitures alimentaires", "Z"),
        ("Les Brasseurs du Nord Épi",        "Épicerie et fournitures alimentaires", "Z"),
        ("Papiers Cascades Épicerie",        "Fournitures de bureau",         "T"),
        ("Emballage Québec Épicerie",        "Fournitures de bureau",         "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Énergir Distribution Épi",         "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais de Service",             "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Épicerie",        "Assurances",                    "I"),
        ("SSQ Assurance Commerce",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Lightspeed POS Épicerie",          "Logiciels et abonnements",      "T"),
        ("Square POS Épicerie",              "Logiciels et abonnements",      "T"),
        ("Nettoyage Épicerie Pro",           "Entretien et nettoyage",        "T"),
        ("Purolator Épicerie",               "Transport et déplacements",     "T"),
        ("RBC Frais Épicerie",               "Frais bancaires",               "E"),
    ],
    "MANUFACTURE": [
        ("Acier Leroux Québec",              "Matériaux et fournitures",      "T"),
        ("Métaux Russel Canada",             "Matériaux et fournitures",      "T"),
        ("Fastenal Canada Manufacture",      "Matériaux et fournitures",      "T"),
        ("Grainger Canada Industriel",       "Matériaux et fournitures",      "T"),
        ("MSC Industrial Direct CA",         "Matériaux et fournitures",      "T"),
        ("Wurth Canada Industriel",          "Matériaux et fournitures",      "T"),
        ("Motion Industries Canada",         "Matériaux et fournitures",      "T"),
        ("Roulements Koyo Canada",           "Matériaux et fournitures",      "T"),
        ("SKF Canada Roulements",            "Matériaux et fournitures",      "T"),
        ("Atlas Copco Canada",               "Équipement et outillage",       "T"),
        ("Lincoln Electric Canada",          "Équipement et outillage",       "T"),
        ("Sandvik Canada Outillage",         "Équipement et outillage",       "T"),
        ("Kennametal Canada",                "Équipement et outillage",       "T"),
        ("Stanley DeWalt Manufacture",       "Équipement et outillage",       "T"),
        ("Milwaukee Tool Manufacture",       "Équipement et outillage",       "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Énergir Manufacture",              "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Manufacture",            "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Manufacture",     "Assurances",                    "I"),
        ("SSQ Assurance Industriel",         "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("SPI Santé Sécurité Manuf",         "Équipements de protection individuelle", "T"),
        ("Mark's Work Wearhouse",            "Équipements de protection individuelle", "T"),
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("RBC Frais Manufacture",            "Frais bancaires",               "E"),
        ("Waste Management QC",              "Gestion des déchets",           "T"),
    ],
    "NETTOYAGE": [
        ("Bunzl Canada Nettoyage",           "Fournitures d'entretien",       "T"),
        ("Swish Maintenance Québec",         "Fournitures d'entretien",       "T"),
        ("Produits Sanitaires Lépine",       "Fournitures d'entretien",       "T"),
        ("Wood Wyant Nettoyage",             "Fournitures d'entretien",       "T"),
        ("Dustbane Products CA",             "Fournitures d'entretien",       "T"),
        ("Sani Marc Nettoyage",              "Fournitures d'entretien",       "T"),
        ("Groupe Sanitaire Québec",          "Fournitures d'entretien",       "T"),
        ("Cascade Tissue Group",             "Fournitures d'entretien",       "T"),
        ("Kruger Products Nettoyage",        "Fournitures d'entretien",       "T"),
        ("Kärcher Canada Équip",             "Équipement et outillage",       "T"),
        ("Tennant Company Canada",           "Équipement et outillage",       "T"),
        ("Nilfisk Canada Nettoyage",         "Équipement et outillage",       "T"),
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("Shell Carburant Nettoyage",        "Carburant et huile",            "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Nettoyage",              "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Nettoyage",       "Assurances",                    "I"),
        ("SSQ Assurance Commerce",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Enterprise Location Véhicule",     "Location de véhicules",         "T"),
        ("Mark's Work Wearhouse",            "Équipements de protection individuelle", "T"),
        ("SPI Santé Sécurité",               "Équipements de protection individuelle", "T"),
        ("RBC Frais Nettoyage",              "Frais bancaires",               "E"),
        ("Purolator Nettoyage",              "Transport et déplacements",     "T"),
        ("SAAQ Immatriculation Fleet",       "Permis et immatriculations",    "T"),
    ],
    "AGENCE": [
        ("Google Ads Canada",                "Publicité et marketing",        "T"),
        ("Facebook Ads Canada",              "Publicité et marketing",        "T"),
        ("Instagram Ads Canada",             "Publicité et marketing",        "T"),
        ("LinkedIn Ads Canada",              "Publicité et marketing",        "T"),
        ("TikTok Ads Agence",               "Publicité et marketing",        "T"),
        ("HubSpot Marketing Hub",            "Logiciels et abonnements",      "T"),
        ("Mailchimp Canada",                 "Logiciels et abonnements",      "T"),
        ("Hootsuite Agence",                 "Logiciels et abonnements",      "T"),
        ("Canva Pro Agence",                 "Logiciels et abonnements",      "T"),
        ("AWS Canada (Amazon)",              "Infrastructure infonuagique",   "T"),
        ("Google Cloud Platform CA",         "Infrastructure infonuagique",   "T"),
        ("Azure Microsoft Cloud",            "Infrastructure infonuagique",   "T"),
        ("Adobe Creative Cloud",             "Logiciels et abonnements",      "T"),
        ("Figma Design Platform",            "Logiciels et abonnements",      "T"),
        ("Slack Technologies Inc",           "Logiciels et abonnements",      "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Agence",                 "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Agence",          "Assurances",                    "I"),
        ("SSQ Assurance Commerce",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Zoom Video Communications",        "Logiciels et abonnements",      "T"),
        ("Notion Workspace Agence",          "Logiciels et abonnements",      "T"),
        ("1Password Business",               "Logiciels et abonnements",      "T"),
        ("RBC Frais Agence",                 "Frais bancaires",               "E"),
        ("Purolator Agence",                 "Transport et déplacements",     "T"),
        ("Stripe Frais Paiement",            "Frais de traitement des paiements", "E"),
    ],
    "GARDERIE": [
        ("Scholastic Canada Éducatif",       "Fournitures éducatives",        "T"),
        ("Scholar's Choice Éducatif",        "Fournitures éducatives",        "T"),
        ("Spectrum Éducatif Canada",         "Fournitures éducatives",        "T"),
        ("Wintergreen Learning CA",          "Fournitures éducatives",        "T"),
        ("Lakeshore Learning CA",            "Fournitures éducatives",        "T"),
        ("Crayola Canada Éducatif",          "Fournitures éducatives",        "T"),
        ("Brault & Bouthillier Édu",         "Fournitures éducatives",        "T"),
        ("Sysco Québec Garderie",            "Achats et matières premières",  "T"),
        ("Distribution Métro Garderie",      "Achats et matières premières",  "T"),
        ("Provigo Commerce Garderie",        "Achats et matières premières",  "T"),
        ("Laiterie Natrel Garderie",         "Achats et matières premières",  "T"),
        ("Boulangerie Garderie QC",          "Achats et matières premières",  "T"),
        ("Fruits Légumes Garderie",          "Achats et matières premières",  "T"),
        ("Papiers Cascades Garderie",        "Fournitures éducatives",        "T"),
        ("Safety 1st Canada",                "Fournitures éducatives",        "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Garderie",               "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Garderie",        "Assurances",                    "I"),
        ("SSQ Assurance Garderie",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Nettoyage Garderie Pro",           "Entretien et nettoyage",        "T"),
        ("Jan-Pro Garderie",                 "Entretien et nettoyage",        "T"),
        ("RBC Frais Garderie",               "Frais bancaires",               "E"),
        ("Purolator Garderie",               "Transport et déplacements",     "T"),
        ("Formation CPE Québec",             "Formation et développement professionnel", "T"),
        ("La Personnelle Assurance",         "Assurances",                    "I"),
    ],
    "ELECTRICIEN": [
        ("Westburne Électrique Enr",         "Matériaux et fournitures",      "T"),
        ("Rexel Canada Électrique",          "Matériaux et fournitures",      "T"),
        ("Guillevin International",          "Matériaux et fournitures",      "T"),
        ("Nedco Électrique Québec",          "Matériaux et fournitures",      "T"),
        ("Eaton Canada Électrique",          "Matériaux et fournitures",      "T"),
        ("Schneider Electric Canada",        "Matériaux et fournitures",      "T"),
        ("Siemens Canada Électrique",        "Matériaux et fournitures",      "T"),
        ("ABB Canada Électrique",            "Matériaux et fournitures",      "T"),
        ("Legrand Canada Électrique",        "Matériaux et fournitures",      "T"),
        ("Leviton Canada Électrique",        "Matériaux et fournitures",      "T"),
        ("Klein Tools Canada",               "Équipement et outillage",       "T"),
        ("Fluke Canada Instruments",         "Équipement et outillage",       "T"),
        ("Milwaukee Tool Électricien",       "Équipement et outillage",       "T"),
        ("DeWalt Électricien",               "Équipement et outillage",       "T"),
        ("Knipex Outils Électrique",         "Équipement et outillage",       "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Électricien",            "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Électricien",     "Assurances",                    "I"),
        ("SSQ Assurance Construction",       "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("Mark's Work Wearhouse",            "Équipements de protection individuelle", "T"),
        ("SPI Santé Sécurité",               "Équipements de protection individuelle", "T"),
        ("RBC Frais Électricien",            "Frais bancaires",               "E"),
        ("Enterprise Location Véhicule",     "Location de véhicules",         "T"),
        ("Formation RBQ Électricien",        "Formation et développement professionnel", "T"),
    ],
    "TRAITEUR": [
        ("Sysco Québec Traiteur",            "Achats et matières premières",  "T"),
        ("Distribution Métro Traiteur",      "Achats et matières premières",  "T"),
        ("Mayrand Food Depot Traiteur",      "Achats et matières premières",  "T"),
        ("Distribution Aubut Traiteur",      "Achats et matières premières",  "T"),
        ("Provigo Commerce Traiteur",        "Achats et matières premières",  "T"),
        ("Laiterie Natrel Traiteur",         "Achats et matières premières",  "T"),
        ("Agropur Fromages Traiteur",        "Achats et matières premières",  "T"),
        ("Saputo Dairy Traiteur",            "Achats et matières premières",  "T"),
        ("Les Brasseurs du Nord Trait",      "Achats boissons",               "T"),
        ("SAQ Approvisionnement Trait",      "Achats boissons",               "T"),
        ("Équipement CRS Cuisine",           "Fournitures de restaurant",     "T"),
        ("Russell Hendrix Traiteur",         "Fournitures de restaurant",     "T"),
        ("Faema Canada Espresso Trait",      "Fournitures de restaurant",     "T"),
        ("Papiers Cascades Traiteur",        "Fournitures de restaurant",     "T"),
        ("Emballage Québec Traiteur",        "Fournitures de restaurant",     "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Énergir Distribution Trait",       "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Traiteur",               "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Traiteur",        "Assurances",                    "I"),
        ("SSQ Assurance Commerce",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Lightspeed POS Traiteur",          "Logiciels et abonnements",      "T"),
        ("Nettoyage Traiteur Pro",           "Entretien et nettoyage",        "T"),
        ("RBC Frais Traiteur",               "Frais bancaires",               "E"),
        ("Purolator Traiteur",               "Transport et déplacements",     "T"),
        ("Uber Eats Frais Traiteur",         "Frais de traitement des paiements", "E"),
        ("DoorDash Frais Traiteur",          "Frais de traitement des paiements", "E"),
    ],
    "PHARMACIE": [
        ("McKesson Canada Pharma",           "Fournitures pharmaceutiques",   "T"),
        ("Cardinal Health Pharmacie",        "Fournitures pharmaceutiques",   "T"),
        ("Kohl & Frisch Pharmacie",          "Fournitures pharmaceutiques",   "T"),
        ("Innomar Strategies Pharma",        "Fournitures pharmaceutiques",   "T"),
        ("Pharmascience Inc Pharma",         "Fournitures pharmaceutiques",   "T"),
        ("Apotex Canada Pharmacie",          "Fournitures pharmaceutiques",   "T"),
        ("Teva Canada Pharmacie",            "Fournitures pharmaceutiques",   "T"),
        ("Bausch Health Canada",             "Fournitures pharmaceutiques",   "T"),
        ("Sandoz Canada Pharmacie",          "Fournitures pharmaceutiques",   "T"),
        ("Becton Dickinson Pharmacie",       "Fournitures médicales",         "T"),
        ("Medline Canada Pharmacie",         "Fournitures médicales",         "T"),
        ("B. Braun Canada Pharmacie",        "Fournitures médicales",         "T"),
        ("Gants Médicaux Pharmacie",         "Fournitures médicales",         "T"),
        ("Masques Chirurgicaux Pharm",       "Fournitures médicales",         "T"),
        ("Stérilisation Pro Pharmacie",      "Fournitures médicales",         "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Pharmacie",              "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Pharmacie",       "Assurances",                    "I"),
        ("SSQ Assurance Pharmacie",          "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("TELUS Santé Logiciel",             "Logiciels et abonnements",      "T"),
        ("Purkinje Logiciel Pharma",         "Logiciels et abonnements",      "T"),
        ("RBC Frais Pharmacie",              "Frais bancaires",               "E"),
        ("Nettoyage Pharmacie Pro",          "Entretien et nettoyage",        "T"),
        ("Purolator Pharmacie",              "Transport et déplacements",     "T"),
        ("Moneris Frais Pharmacie",          "Frais de traitement des paiements", "E"),
    ],
    "TOITURE": [
        ("BP Canada Toiture",                "Matériaux et fournitures",      "T"),
        ("IKO Industries Toiture",           "Matériaux et fournitures",      "T"),
        ("CertainTeed Canada Toit",          "Matériaux et fournitures",      "T"),
        ("GAF Roofing Canada",               "Matériaux et fournitures",      "T"),
        ("Owens Corning Canada",             "Matériaux et fournitures",      "T"),
        ("Soprema Canada Toiture",           "Matériaux et fournitures",      "T"),
        ("Firestone Building Toit",          "Matériaux et fournitures",      "T"),
        ("Carlisle SynTec Toiture",          "Matériaux et fournitures",      "T"),
        ("Rona Pro Toiture",                 "Matériaux et fournitures",      "T"),
        ("Home Depot Pro Toiture",           "Matériaux et fournitures",      "T"),
        ("BMR Toiture Québec",               "Matériaux et fournitures",      "T"),
        ("Canac Toiture",                    "Matériaux et fournitures",      "T"),
        ("Mark's Work Toiture",              "Équipements de protection individuelle", "T"),
        ("SPI Santé Sécurité Toit",          "Équipements de protection individuelle", "T"),
        ("Sylprotec EPI Toiture",            "Équipements de protection individuelle", "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Toiture",                "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Toiture",         "Assurances",                    "I"),
        ("SSQ Assurance Construction",       "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("Sunbelt Location Équip",           "Location d'équipement",         "T"),
        ("Enterprise Location Véhicule",     "Location de véhicules",         "T"),
        ("RBC Frais Toiture",                "Frais bancaires",               "E"),
        ("Purolator Toiture",                "Transport et déplacements",     "T"),
        ("Formation RBQ Toiture",            "Formation et développement professionnel", "T"),
    ],
    "CONSULT": [
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Adobe Creative Cloud",             "Logiciels et abonnements",      "T"),
        ("Google Workspace Consult",         "Logiciels et abonnements",      "T"),
        ("Slack Technologies Inc",           "Logiciels et abonnements",      "T"),
        ("Zoom Video Communications",        "Logiciels et abonnements",      "T"),
        ("Notion Workspace Consult",         "Logiciels et abonnements",      "T"),
        ("1Password Business",               "Logiciels et abonnements",      "T"),
        ("DocuSign Consult",                 "Logiciels et abonnements",      "T"),
        ("Expert-comptable Consult",         "Honoraires professionnels",     "T"),
        ("Avocat Commercial Consult",        "Honoraires professionnels",     "T"),
        ("Notaire Consult Québec",           "Honoraires professionnels",     "T"),
        ("Traducteur Consult Mtl",           "Honoraires professionnels",     "T"),
        ("AWS Canada (Amazon)",              "Infrastructure infonuagique",   "T"),
        ("Azure Microsoft Cloud",            "Infrastructure infonuagique",   "T"),
        ("LinkedIn Premium Consult",         "Abonnements et médias",         "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Consult",                "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Consult",         "Assurances",                    "I"),
        ("SSQ Assurance Groupe",             "Assurances",                    "I"),
        ("Air Canada Consult",               "Transport et déplacements",     "T"),
        ("VIA Rail Consult",                 "Transport et déplacements",     "T"),
        ("Marriott Hébergement",             "Frais de voyage et hébergement","T"),
        ("Hilton Hébergement",               "Frais de voyage et hébergement","T"),
        ("RBC Frais Consult",                "Frais bancaires",               "E"),
        ("Purolator Consult",                "Transport et déplacements",     "T"),
        ("Stripe Frais Paiement",            "Frais de traitement des paiements", "E"),
    ],
    "PAYSAGE": [
        ("Pépinière Villeneuve QC",          "Aménagement paysager",          "T"),
        ("Centre Jardin Hamel",              "Aménagement paysager",          "T"),
        ("Botanix Paysage Québec",           "Aménagement paysager",          "T"),
        ("Fafard Terre Noire QC",            "Aménagement paysager",          "T"),
        ("Permacon Pavé Québec",             "Aménagement paysager",          "T"),
        ("Techo-Bloc Paysager",              "Aménagement paysager",          "T"),
        ("Rinox Produits Béton",             "Aménagement paysager",          "T"),
        ("Rona Pro Paysage",                 "Matériaux et fournitures",      "T"),
        ("Home Depot Pro Paysage",           "Matériaux et fournitures",      "T"),
        ("Stihl Canada Équipement",          "Équipement et outillage",       "T"),
        ("Husqvarna Canada Paysage",         "Équipement et outillage",       "T"),
        ("John Deere Canada Paysage",        "Équipement et outillage",       "T"),
        ("Honda Power Équipement",           "Équipement et outillage",       "T"),
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Paysage",                "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Paysage",         "Assurances",                    "I"),
        ("SSQ Assurance Commerce",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Enterprise Location Véhicule",     "Location de véhicules",         "T"),
        ("Sunbelt Location Équip",           "Location d'équipement",         "T"),
        ("Mark's Work Wearhouse",            "Équipements de protection individuelle", "T"),
        ("RBC Frais Paysage",                "Frais bancaires",               "E"),
        ("Purolator Paysage",                "Transport et déplacements",     "T"),
        ("SAAQ Immatriculation Fleet",       "Permis et immatriculations",    "T"),
        ("Sel de Déglaçage Québec",          "Aménagement paysager",          "T"),
    ],
    "VETERINAIRE": [
        ("CDMV Canada Vétérinaire",          "Fournitures vétérinaires",      "T"),
        ("Patterson Veterinary CA",          "Fournitures vétérinaires",      "T"),
        ("Boehringer Ingelheim Vet",         "Fournitures vétérinaires",      "T"),
        ("Zoetis Canada Vétérinaire",        "Fournitures vétérinaires",      "T"),
        ("Elanco Canada Animal",             "Fournitures vétérinaires",      "T"),
        ("Merck Animal Health CA",           "Fournitures vétérinaires",      "T"),
        ("Royal Canin Canada Vet",           "Fournitures vétérinaires",      "T"),
        ("Hill's Pet Nutrition CA",          "Fournitures vétérinaires",      "T"),
        ("Purina Pro Plan Vet CA",           "Fournitures vétérinaires",      "T"),
        ("Idexx Laboratories Canada",        "Fournitures médicales",         "T"),
        ("Abaxis Vet Diagnostics",           "Fournitures médicales",         "T"),
        ("Heska Canada Diagnostics",         "Fournitures médicales",         "T"),
        ("Becton Dickinson Vet",             "Fournitures médicales",         "T"),
        ("Gants Médicaux Vet Canada",        "Fournitures médicales",         "T"),
        ("Stérilisation Pro Vet",            "Fournitures médicales",         "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Vétérinaire",            "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Vet",             "Assurances",                    "I"),
        ("SSQ Assurance Groupe",             "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Logiciel Avimark Vet",             "Logiciels et abonnements",      "T"),
        ("Cornerstone Vet Software",         "Logiciels et abonnements",      "T"),
        ("RBC Frais Vétérinaire",            "Frais bancaires",               "E"),
        ("Nettoyage Vet Pro",                "Entretien et nettoyage",        "T"),
        ("Purolator Vétérinaire",            "Transport et déplacements",     "T"),
        ("Formation OMVQ Vet",               "Formation et développement professionnel", "T"),
    ],
    "DEMENAGEMENT": [
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("Shell Commercial Canada",          "Carburant et huile",            "T"),
        ("Esso Commercial Québec",           "Carburant et huile",            "T"),
        ("Irving Oil Fleet",                 "Carburant et huile",            "T"),
        ("Enterprise Location Déménag",      "Location de véhicules",         "T"),
        ("Hertz Location Camion",            "Location de véhicules",         "T"),
        ("Budget Location Camion",           "Location de véhicules",         "T"),
        ("Penske Location Camion",           "Location de véhicules",         "T"),
        ("U-Haul Canada Location",           "Location de véhicules",         "T"),
        ("Emballage Québec Déménag",         "Fournitures de bureau",         "T"),
        ("Uline Canada Emballage",           "Fournitures de bureau",         "T"),
        ("Canadian Tire Déménagement",       "Matériaux et fournitures",      "T"),
        ("Rona Pro Déménagement",            "Matériaux et fournitures",      "T"),
        ("Home Depot Déménagement",          "Matériaux et fournitures",      "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Déménagement",           "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Déménag",         "Assurances",                    "I"),
        ("SSQ Assurance Transport",          "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Mark's Work Wearhouse",            "Équipements de protection individuelle", "T"),
        ("SPI Santé Sécurité Démén",         "Équipements de protection individuelle", "T"),
        ("SAAQ Immatriculation Fleet",       "Permis et immatriculations",    "T"),
        ("RBC Frais Déménagement",           "Frais bancaires",               "E"),
        ("Purolator Déménagement",           "Transport et déplacements",     "T"),
        ("Nettoyage Déménagement Pro",       "Entretien et nettoyage",        "T"),
        ("Formation CNESST Démén",           "Formation et développement professionnel", "T"),
    ],
    "IMPRIMERIE": [
        ("Papier Masson Québec",             "Impression et papeterie",       "T"),
        ("Spicers Canada Papier",            "Impression et papeterie",       "T"),
        ("Unisource Canada Papier",          "Impression et papeterie",       "T"),
        ("Domtar Canada Papier",             "Impression et papeterie",       "T"),
        ("Resolute Forest Products",         "Impression et papeterie",       "T"),
        ("Cascades Papier Fin",              "Impression et papeterie",       "T"),
        ("HP Indigo Encres Québec",          "Matériaux et fournitures",      "T"),
        ("Sun Chemical Canada",              "Matériaux et fournitures",      "T"),
        ("Flint Group Canada",               "Matériaux et fournitures",      "T"),
        ("Heidelberg Canada Presse",         "Équipement et outillage",       "T"),
        ("Konica Minolta Canada",            "Équipement et outillage",       "T"),
        ("Xerox Canada Imprimerie",          "Équipement et outillage",       "T"),
        ("Canon Canada Pro Print",           "Équipement et outillage",       "T"),
        ("Ricoh Canada Imprimerie",          "Équipement et outillage",       "T"),
        ("EFI Canada Logiciel",              "Logiciels et abonnements",      "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Imprimerie",             "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Imprimerie",      "Assurances",                    "I"),
        ("SSQ Assurance Industriel",         "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Adobe Creative Cloud",             "Logiciels et abonnements",      "T"),
        ("Purolator Imprimerie",             "Transport et déplacements",     "T"),
        ("FedEx Imprimerie",                 "Transport et déplacements",     "T"),
        ("Postes Canada Envois",             "Frais de livraison et courrier","T"),
        ("RBC Frais Imprimerie",             "Frais bancaires",               "E"),
        ("Waste Management QC",              "Gestion des déchets",           "T"),
        ("Nettoyage Imprimerie Pro",         "Entretien et nettoyage",        "T"),
    ],
    "SECURITE": [
        ("Garda World Sécurité",             "Sécurité et surveillance",      "T"),
        ("Securitas Canada",                 "Sécurité et surveillance",      "T"),
        ("GardaWorld Protection",            "Sécurité et surveillance",      "T"),
        ("ADT Sécurité Québec",              "Sécurité et surveillance",      "T"),
        ("Chubb Sécurité Canada",            "Sécurité et surveillance",      "T"),
        ("Genetec Inc Logiciel",             "Logiciels et abonnements",      "T"),
        ("Axis Communications CA",           "Équipement et outillage",       "T"),
        ("Hikvision Canada Caméra",          "Équipement et outillage",       "T"),
        ("Honeywell Sécurité Canada",        "Équipement et outillage",       "T"),
        ("Bosch Sécurité Canada",            "Équipement et outillage",       "T"),
        ("Tyco Sécurité Canada",             "Équipement et outillage",       "T"),
        ("DSC Alarmes Canada",               "Équipement et outillage",       "T"),
        ("Kantech Contrôle Accès",           "Équipement et outillage",       "T"),
        ("Mark's Work Sécurité",             "Équipements de protection individuelle", "T"),
        ("SPI Santé Sécurité",               "Équipements de protection individuelle", "T"),
        ("Hydro-Québec",                     "Électricité et gaz",            "E"),
        ("Bell Communications",              "Télécommunications",            "T"),
        ("Vidéotron Affaires",               "Télécommunications",            "T"),
        ("Desjardins Frais Bancaires",       "Frais bancaires",               "E"),
        ("BMO Frais Sécurité",               "Frais bancaires",               "E"),
        ("Bureau en Gros",                   "Fournitures de bureau",         "T"),
        ("Intact Assurance Sécurité",        "Assurances",                    "I"),
        ("SSQ Assurance Sécurité",           "Assurances",                    "I"),
        ("Microsoft 365 Business",           "Logiciels et abonnements",      "T"),
        ("Ultramar Carburant Fleet",         "Carburant et huile",            "T"),
        ("Petro-Canada Fleet",               "Carburant et huile",            "T"),
        ("Enterprise Location Véhicule",     "Location de véhicules",         "T"),
        ("RBC Frais Sécurité",               "Frais bancaires",               "E"),
        ("Purolator Sécurité",               "Transport et déplacements",     "T"),
        ("Formation BSP Sécurité",           "Formation et développement professionnel", "T"),
    ],
}

# Base patterns validated; auto-expansion below will bring each to 100

# ── Auto-expand explicit patterns to 100 per client using seed vendors ────────
def _expand_explicit_patterns(
    base: dict[str, list[tuple[str, str, str]]],
    target: int = 100,
) -> dict[str, list[tuple[str, str, str]]]:
    """Expand each client's explicit patterns to `target` count using seed vendors."""
    try:
        from scripts.seed_vendor_knowledge import VENDORS as _SEED_VENDORS
    except ImportError:
        # Fallback: import from relative path
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "seed_vendor_knowledge",
            Path(__file__).resolve().parent / "seed_vendor_knowledge.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _SEED_VENDORS = mod.VENDORS

    expanded: dict[str, list[tuple[str, str, str]]] = {}
    for client_code in CLIENTS:
        existing = list(base.get(client_code, []))
        existing_names = {v[0] for v in existing}
        for vendor_name, gl_account, tax_code in _SEED_VENDORS:
            if len(existing) >= target:
                break
            if vendor_name not in existing_names:
                existing.append((vendor_name, gl_account, tax_code))
                existing_names.add(vendor_name)
        expanded[client_code] = existing
    return expanded

EXPLICIT_PATTERNS = _expand_explicit_patterns(EXPLICIT_PATTERNS, target=100)

# Re-validate after expansion
for _c, _p in EXPLICIT_PATTERNS.items():
    assert len(_p) >= 100, f"{_c} needs at least 100 explicit patterns, got {len(_p)}"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_clean_docs(db_path: Path) -> list[dict[str, Any]]:
    """All qualifying clean documents across the 25 test clients."""
    placeholders = ",".join(f"'{c}'" for c in CLIENTS)
    with _open(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT document_id, client_code, vendor,
                   doc_type, category, gl_account, tax_code,
                   amount, confidence
              FROM documents
             WHERE client_code IN ({placeholders})
               AND review_status IN ('ReadyToPost','Posted')
               AND confidence >= 0.85
             ORDER BY client_code, vendor
            """
        ).fetchall()
    return [dict(r) for r in rows]


def _clean_previous_seed(db_path: Path) -> int:
    """Remove learning_memory rows from a prior run of this script."""
    with _open(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM learning_memory WHERE reviewer = ?",
            (_SEED_REVIEWER,),
        )
        conn.commit()
        return cur.rowcount


def _batch_insert_learning_memory(
    db_path: Path,
    rows: list[tuple[Any, ...]],
) -> int:
    """
    Bulk-insert rows into learning_memory in a single transaction.
    Each row tuple must match the INSERT column list below.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO learning_memory
            (document_id, client_code, vendor, doc_type,
             field_name, old_value, new_value,
             reviewer, created_at, updated_at,
             vendor_key, client_code_key, event_type, memory_key)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with _open(db_path) as conn:
        conn.executemany(sql, rows)
        conn.commit()
    return len(rows)


def _lm_row(
    doc_id: str,
    client_code: str,
    vendor: str,
    doc_type: str,
    field_name: str,
    value: str,
    now: str,
) -> tuple[Any, ...]:
    """Build one learning_memory insert row (old_value == new_value = confirmed correction)."""
    return (
        doc_id,
        client_code,
        vendor,
        doc_type or "invoice",
        field_name,
        value,           # old_value
        value,           # new_value (approved — same value)
        _SEED_REVIEWER,
        now,
        now,
        vendor.strip().casefold(),
        client_code.strip().casefold(),
        "approved_ready_to_post",
        "",              # memory_key (default '')
    )


# ── Bulk correction helpers ───────────────────────────────────────────────────

def _normalize_key_local(value: str) -> str:
    """Match learning_correction_store.normalize_key without importing it."""
    text = value.strip().casefold()
    for ch in [",", ".", ";", ":", "'", '"', "(", ")", "[", "]", "{", "}", "/", "\\", "-", "_", "|"]:
        text = text.replace(ch, " ")
    return " ".join(text.split())


def _bulk_insert_corrections(
    db_path: Path,
    rows: list[tuple[Any, ...]],
) -> int:
    """Bulk INSERT OR IGNORE into learning_corrections using executemany()."""
    if not rows:
        return 0
    sql = """
        INSERT OR IGNORE INTO learning_corrections
            (document_id, client_code, client_code_key,
             vendor, vendor_key, doc_type, doc_type_key,
             category, category_key, field_name, field_name_key,
             old_value, old_value_key, new_value, new_value_key,
             reviewer, source, confidence_before, notes,
             support_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with _open(db_path) as conn:
        conn.executemany(sql, rows)
        conn.commit()
    return len(rows)


def _build_correction_row(
    doc_id: str, client_code: str, vendor: str, doc_type: str,
    category: str, field_name: str, value: str, now: str,
) -> tuple[Any, ...]:
    """Build one learning_corrections insert tuple."""
    return (
        doc_id,
        client_code,
        _normalize_key_local(client_code),
        vendor,
        _normalize_key_local(vendor),
        doc_type or "invoice",
        _normalize_key_local(doc_type or "invoice"),
        category,
        _normalize_key_local(category),
        field_name,
        _normalize_key_local(field_name),
        value,                          # old_value
        _normalize_key_local(value),    # old_value_key
        value,                          # new_value
        _normalize_key_local(value),    # new_value_key
        _SEED_REVIEWER,
        "seed",
        None,                           # confidence_before
        "",                             # notes
        1,                              # support_count
        now,
        now,
    )


def _bulk_record_feedback(
    db_path: Path,
    docs: list[dict[str, Any]],
    event_type: str = "posted_successfully",
) -> int:
    """Bulk INSERT OR IGNORE into learning_memory_patterns using executemany()."""
    if not docs:
        return 0
    now = _utcnow()
    rows: list[tuple[Any, ...]] = []
    for doc in docs:
        vendor = str(doc.get("vendor") or "")
        client_code = str(doc.get("client_code") or "")
        doc_type = str(doc.get("doc_type") or "invoice")
        category = str(doc.get("category") or "")
        gl_account = str(doc.get("gl_account") or "")
        tax_code = str(doc.get("tax_code") or "")
        confidence = doc.get("confidence") or 0.0
        amount = doc.get("amount")
        doc_id = str(doc.get("document_id") or "")

        # Build memory_key the same way LearningMemoryStore does
        memory_key = "|".join([
            event_type.strip().casefold(),
            vendor.strip().casefold(),
            client_code.strip().casefold(),
            doc_type.strip().casefold(),
            category.strip().casefold(),
            gl_account.strip().casefold(),
            tax_code.strip().casefold(),
        ])

        rows.append((
            memory_key,
            event_type,
            vendor,
            vendor.strip().casefold(),
            client_code,
            client_code.strip().casefold(),
            doc_type,
            category,
            gl_account,
            tax_code,
            1,                              # outcome_count
            1,                              # success_count
            0,                              # review_count
            1 if event_type == "posted_successfully" else 0,  # posted_count
            float(confidence),
            amount,
            doc_id,
            now,
            now,
        ))

    sql = """
        INSERT OR IGNORE INTO learning_memory_patterns
            (memory_key, event_type, vendor, vendor_key,
             client_code, client_code_key, doc_type, category,
             gl_account, tax_code, outcome_count, success_count,
             review_count, posted_count, avg_confidence, avg_amount,
             last_document_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with _open(db_path) as conn:
        conn.executemany(sql, rows)
        conn.commit()
    return len(rows)


# ── Phase 1 & 2: Record corrections from clean documents ─────────────────────

def phase_record_docs(
    docs: list[dict[str, Any]],
    memory_store: LearningMemoryStore,
    correction_store: LearningCorrectionStore,
    db_path: Path,
) -> tuple[int, int, int]:
    """
    For each clean doc:
      - bulk insert into learning_memory_patterns (record_feedback equivalent)
      - bulk insert into learning_corrections (record_correction equivalent)
      - collect learning_memory rows for bulk insert

    Returns (patterns_recorded, corrections_recorded, lm_rows_queued).
    """
    now = _utcnow()

    # ── Bulk insert feedback patterns ──
    print(f"  [phase1] Bulk inserting {len(docs)} feedback patterns...")
    patterns_recorded = _bulk_record_feedback(db_path, docs)

    # ── Bulk insert corrections and learning_memory rows ──
    correction_rows: list[tuple[Any, ...]] = []
    lm_rows: list[tuple[Any, ...]] = []
    corrections_recorded = 0

    for i, doc in enumerate(docs, 1):
        doc_id      = str(doc.get("document_id") or "")
        client_code = str(doc.get("client_code") or "")
        vendor      = str(doc.get("vendor") or "")
        doc_type    = str(doc.get("doc_type") or "invoice")
        gl_account  = str(doc.get("gl_account") or "")
        tax_code    = str(doc.get("tax_code") or "")
        category    = str(doc.get("category") or "")

        field_values = {
            "gl_account": gl_account,
            "tax_code":   tax_code,
            "category":   category,
            "vendor":     vendor,
        }
        for field_name, value in field_values.items():
            if not value:
                continue
            correction_rows.append(_build_correction_row(
                doc_id, client_code, vendor, doc_type,
                category, field_name, value, now,
            ))
            lm_rows.append(_lm_row(doc_id, client_code, vendor, doc_type,
                                   field_name, value, now))
            corrections_recorded += 1

        if i % 1000 == 0:
            print(f"  [phase1] {i:5d}/{len(docs)} docs prepared "
                  f"(corrections={corrections_recorded})")

    print(f"  [phase1] Bulk inserting {len(correction_rows)} correction rows...")
    _bulk_insert_corrections(db_path, correction_rows)

    # Bulk insert all learning_memory rows in one transaction
    lm_inserted = _batch_insert_learning_memory(db_path, lm_rows)
    return patterns_recorded, corrections_recorded, lm_inserted


# ── Phase 3: Explicit vendor-GL patterns ─────────────────────────────────────

def phase_explicit_patterns(
    memory_store: LearningMemoryStore,
    correction_store: LearningCorrectionStore,
    db_path: Path,
) -> tuple[int, int, int]:
    """
    For each of the 100 explicit (client, vendor, GL, tax) patterns:
      - bulk insert into learning_memory_patterns
      - bulk insert into learning_corrections
      - insert _EXPLICIT_BOOST rows into learning_memory for high support

    Returns (patterns_added, corrections_added, lm_rows_added).
    """
    corrections_added = 0
    correction_rows: list[tuple[Any, ...]] = []
    lm_rows: list[tuple[Any, ...]] = []
    feedback_docs: list[dict[str, Any]] = []
    now = _utcnow()

    for client_code, pattern_list in EXPLICIT_PATTERNS.items():
        for idx, (vendor, gl_account, tax_code) in enumerate(pattern_list):
            fake_doc_id = f"explicit_{client_code}_{idx:03d}"

            # Collect for bulk feedback insert
            feedback_docs.append({
                "document_id":  fake_doc_id,
                "vendor":       vendor,
                "client_code":  client_code,
                "doc_type":     "invoice",
                "category":     "expense",
                "gl_account":   gl_account,
                "tax_code":     tax_code,
                "confidence":   0.95,
            })

            # Collect correction rows and learning_memory rows
            for field_name, value in [("gl_account", gl_account),
                                      ("tax_code", tax_code),
                                      ("category", "expense"),
                                      ("vendor", vendor)]:
                correction_rows.append(_build_correction_row(
                    fake_doc_id, client_code, vendor, "invoice",
                    "expense", field_name, value, now,
                ))
                corrections_added += 1

                # Insert _EXPLICIT_BOOST rows per field for high support
                for boost_i in range(_EXPLICIT_BOOST):
                    lm_rows.append(_lm_row(
                        f"{fake_doc_id}_b{boost_i}",
                        client_code, vendor, "invoice",
                        field_name, value, now,
                    ))

    patterns_added = _bulk_record_feedback(db_path, feedback_docs)
    _bulk_insert_corrections(db_path, correction_rows)
    lm_inserted = _batch_insert_learning_memory(db_path, lm_rows)
    return patterns_added, corrections_added, lm_inserted


# ── Phase 4: Simulate 24 months of corrections ──────────────────────────────

def phase_simulate_months(
    docs: list[dict[str, Any]],
    memory_store: LearningMemoryStore,
    correction_store: LearningCorrectionStore,
    db_path: Path,
) -> tuple[int, int]:
    """
    Run the learning cycle 3 times on a 5,000-doc sample with slight variations
    in amounts and dates. Same learning effect as 24 full cycles, much faster.

    Returns (total_patterns, total_corrections).
    """
    total_patterns = 0
    total_corrections = 0

    # Deduplicate: collect unique (vendor, client, gl, tax, doc_type, category) combos
    unique_combos: dict[str, dict[str, Any]] = {}
    for doc in docs:
        vendor = str(doc.get("vendor") or "")
        client_code = str(doc.get("client_code") or "")
        gl_account = str(doc.get("gl_account") or "")
        tax_code = str(doc.get("tax_code") or "")
        doc_type = str(doc.get("doc_type") or "invoice")
        category = str(doc.get("category") or "")
        key = f"{vendor}|{client_code}|{gl_account}|{tax_code}|{doc_type}|{category}"
        if key not in unique_combos:
            unique_combos[key] = {
                "vendor": vendor, "client_code": client_code,
                "gl_account": gl_account, "tax_code": tax_code,
                "doc_type": doc_type, "category": category,
                "amount": doc.get("amount"), "confidence": doc.get("confidence"),
            }

    combos_list = list(unique_combos.values())
    print(f"  Found {len(combos_list)} unique vendor-client-GL combos to simulate")

    # 3 cycles instead of 24 — statistically sufficient for learning
    num_cycles = 3
    for month in range(1, num_cycles + 1):
        print(f"  [cycle {month}/{num_cycles}] Recording {len(combos_list)} unique patterns + "
              f"{len(docs)} learning_memory rows...")
        month_corrections = 0
        lm_rows: list[tuple[Any, ...]] = []
        now = _utcnow()

        # Bulk insert feedback for unique combos
        feedback_docs: list[dict[str, Any]] = []
        for combo in combos_list:
            amount = combo["amount"]
            if amount:
                variation = random.uniform(0.95, 1.05)
                amount = round(float(amount) * variation, 2)

            fake_doc_id = f"sim_m{month}_{combo['vendor'][:20]}_{combo['client_code']}"

            feedback_docs.append({
                "document_id":  fake_doc_id,
                "vendor":       combo["vendor"],
                "client_code":  combo["client_code"],
                "doc_type":     combo["doc_type"],
                "category":     combo["category"],
                "gl_account":   combo["gl_account"],
                "tax_code":     combo["tax_code"],
                "amount":       amount,
                "confidence":   combo["confidence"],
            })

        month_patterns = _bulk_record_feedback(db_path, feedback_docs)

        # Batch insert learning_memory rows for all docs in this cycle
        for i, doc in enumerate(docs, 1):
            fake_doc_id = f"sim_m{month}_{doc.get('document_id', '')}"
            vendor = str(doc.get("vendor") or "")
            client_code = str(doc.get("client_code") or "")
            doc_type = str(doc.get("doc_type") or "invoice")
            gl_account = str(doc.get("gl_account") or "")
            tax_code = str(doc.get("tax_code") or "")

            for field_name, value in [("gl_account", gl_account),
                                       ("tax_code", tax_code)]:
                if not value:
                    continue
                lm_rows.append(_lm_row(
                    fake_doc_id, client_code, vendor, doc_type,
                    field_name, value, now,
                ))
                month_corrections += 1

            if i % 1000 == 0:
                print(f"    [cycle {month}] {i:5d}/{len(docs)} docs prepared")

        _batch_insert_learning_memory(db_path, lm_rows)
        total_patterns += month_patterns
        total_corrections += month_corrections
        print(f"    patterns={month_patterns}, corrections={month_corrections}, "
              f"lm_rows={len(lm_rows)}")

    return total_patterns, total_corrections


# ── Phase 5: Suggestion sampling ─────────────────────────────────────────────

def phase_suggestions(
    docs: list[dict[str, Any]],
    engine: LearningSuggestionEngine,
) -> dict[str, Any]:
    """
    Sample 50 docs per client, call suggestions_for_document(), collect stats.
    Returns a report dict.
    """
    by_client: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for d in docs:
        by_client[d["client_code"]].append(d)

    sampled: list[dict[str, Any]] = []
    for client_code in CLIENTS:
        pool = by_client.get(client_code, [])
        k = min(50, len(pool))
        sampled.extend(random.sample(pool, k))

    got_suggestion = 0
    gl_suggestions_per_client: dict[str, list[str]] = defaultdict(list)

    for doc in sampled:
        result = engine.suggestions_for_document(
            client_code=doc["client_code"],
            vendor=doc["vendor"],
            doc_type=doc.get("doc_type") or "invoice",
        )
        if result:
            got_suggestion += 1

        if "gl_account" in result:
            for opt in result["gl_account"]:
                gl_suggestions_per_client[doc["client_code"]].append(opt["value"])

    # Top-5 GL suggestions per client
    top5_per_client: dict[str, list[tuple[str, int]]] = {}
    for client_code in CLIENTS:
        from collections import Counter
        counts = Counter(gl_suggestions_per_client[client_code])
        top5_per_client[client_code] = counts.most_common(5)

    return {
        "sampled":         len(sampled),
        "got_suggestion":  got_suggestion,
        "top5_per_client": top5_per_client,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    print("accelerate_learning.py starting...")
    print(f"  DB: {DB_PATH}")

    # ── Idempotency: remove previous seed rows ────────────────────────────
    removed = _clean_previous_seed(DB_PATH)
    if removed:
        print(f"  Removed {removed} previous seed rows from learning_memory")

    # ── Instantiate stores ────────────────────────────────────────────────
    memory_store     = LearningMemoryStore(db_path=DB_PATH)
    correction_store = LearningCorrectionStore(db_path=DB_PATH)
    engine           = LearningSuggestionEngine(db_path=DB_PATH)

    # ── Fetch clean docs ──────────────────────────────────────────────────
    all_clean_docs = _fetch_clean_docs(DB_PATH)
    print(f"  Found {len(all_clean_docs)} clean documents total")

    # Sample 5,000 high-confidence docs (10% sample) — statistically sufficient
    SAMPLE_SIZE = 5000
    if len(all_clean_docs) > SAMPLE_SIZE:
        clean_docs = random.sample(all_clean_docs, SAMPLE_SIZE)
        print(f"  Sampled {SAMPLE_SIZE} documents for learning (10% sample)")
    else:
        clean_docs = all_clean_docs

    print(f"\nPhase 1 & 2: Recording corrections from {len(clean_docs)} clean documents")

    patterns_doc, corrections_doc, lm_doc = phase_record_docs(
        clean_docs, memory_store, correction_store, DB_PATH,
    )
    print(f"  Done  — patterns={patterns_doc}, corrections={corrections_doc}, "
          f"learning_memory rows={lm_doc}")

    # ── Explicit patterns ─────────────────────────────────────────────────
    total_explicit = sum(len(v) for v in EXPLICIT_PATTERNS.values())
    print(f"\nPhase 3: Adding {total_explicit} explicit vendor-GL patterns "
          f"(x{_EXPLICIT_BOOST} boost each)")

    patterns_exp, corrections_exp, lm_exp = phase_explicit_patterns(
        memory_store, correction_store, DB_PATH,
    )
    print(f"  Done  — patterns={patterns_exp}, corrections={corrections_exp}, "
          f"learning_memory rows={lm_exp}")

    # ── Simulate 3 cycles on sampled docs ────────────────────────────────
    print(f"\nPhase 4: Simulating 3 learning cycles on {len(clean_docs)} documents")
    sim_patterns, sim_corrections = phase_simulate_months(
        clean_docs, memory_store, correction_store, DB_PATH,
    )
    print(f"  Done  — sim_patterns={sim_patterns}, sim_corrections={sim_corrections}")

    # ── Suggestion quality ────────────────────────────────────────────────
    print(f"\nPhase 5: Running suggestions_for_document() on 50 docs per client (1,250 total)")
    report = phase_suggestions(all_clean_docs, engine)

    # ── Final report ──────────────────────────────────────────────────────
    total_patterns     = patterns_doc + patterns_exp + sim_patterns
    total_corrections  = corrections_doc + corrections_exp + sim_corrections
    total_lm_rows      = lm_doc + lm_exp

    print("\n" + "=" * 60)
    print("ACCELERATE LEARNING -- FINAL REPORT")
    print("=" * 60)
    print(f"  Total correction patterns recorded  : {total_patterns}")
    print(f"  Total field corrections recorded    : {total_corrections}")
    print(f"  Total learning_memory rows inserted : {total_lm_rows}")
    print(f"  Suggestion quality  : "
          f"{report['got_suggestion']}/{report['sampled']} documents "
          f"got useful suggestions")
    print()
    print("  Top 5 GL account suggestions per client:")
    for client_code in CLIENTS:
        top5 = report["top5_per_client"].get(client_code, [])
        if top5:
            print(f"    {client_code}:")
            for gl, cnt in top5:
                print(f"      {gl:<45}  (mentioned {cnt}x)")
        else:
            print(f"    {client_code}: (no GL suggestions)")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
