"""
scripts/setup_wizard.py -- LedgerLink Professional Setup Wizard
================================================================
Standalone HTTP server on port 8790.  Python stdlib + bcrypt only.
Bilingual FR/EN.  Guides the installer through every required step.

Steps:
  0  Welcome
  1  Firm Information
  2  Administrator Account
  3  License Key
  4  AI Providers
  5  Email Configuration
  6  Client Portal / Cloudflare
  7  WhatsApp
  8  Telegram
  9  Microsoft 365
 10  QuickBooks Online
 11  Folder Watcher
 12  Daily Digest
 13  Backup Configuration
 14  Notification Preferences
 15  Security Settings
 16  Staff Members
 17  Clients
 18  Review & Confirm
 19  Complete
"""
from __future__ import annotations

import argparse
import html as _html
import io
import json
import os
import random
import re
import secrets
import socket
import socketserver
import sqlite3
import string
import sys
import traceback
import urllib.parse
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path

import bcrypt

# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT_DIR / "data" / "setup_state.json"
CONFIG_FILE = ROOT_DIR / "ledgerlink.config.json"
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

# ---------------------------------------------------------------------------
# Step definitions  (step_num -> (path, fr_label, en_label))
# ---------------------------------------------------------------------------
STEPS: list[tuple[str, str, str]] = [
    ("/setup/welcome",       "Bienvenue",                "Welcome"),                  # 0
    ("/setup/firm",          "Cabinet",                   "Firm Info"),                # 1
    ("/setup/admin",         "Administrateur",            "Administrator"),            # 2
    ("/setup/license",       "Licence",                   "License"),                  # 3
    ("/setup/ai",            "Intelligence artificielle", "AI Providers"),             # 4
    ("/setup/email",         "Courriel",                  "Email"),                    # 5
    ("/setup/portal",        "Portail client",            "Client Portal"),            # 6
    ("/setup/whatsapp",      "WhatsApp",                  "WhatsApp"),                 # 7
    ("/setup/telegram",      "Telegram",                  "Telegram"),                 # 8
    ("/setup/microsoft365",  "Microsoft 365",             "Microsoft 365"),            # 9
    ("/setup/quickbooks",    "QuickBooks",                "QuickBooks"),               # 10
    ("/setup/folder",        "Dossier",                   "Folder Watcher"),           # 11
    ("/setup/digest",        "Resume quotidien",          "Daily Digest"),             # 12
    ("/setup/backup",        "Sauvegarde",                "Backup"),                   # 13
    ("/setup/notifications", "Notifications",             "Notifications"),            # 14
    ("/setup/security",      "Securite",                  "Security"),                 # 15
    ("/setup/staff",         "Equipe",                    "Staff"),                    # 16
    ("/setup/clients",       "Clients",                   "Clients"),                  # 17
    ("/setup/review",        "Verification",              "Review"),                   # 18
    ("/setup/complete",      "Termine",                   "Complete"),                 # 19
]

TOTAL_STEPS = len(STEPS) - 2  # exclude welcome (0) and complete (19) from numbering

STEP_PATHS = {i: s[0] for i, s in enumerate(STEPS)}
PATH_TO_STEP = {s[0]: i for i, s in enumerate(STEPS)}

# ---------------------------------------------------------------------------
# Bilingual strings
# ---------------------------------------------------------------------------
STRINGS: dict[str, dict[str, str]] = {
    "fr": {
        "title": "Assistant de configuration LedgerLink",
        "btn_next": "Suivant \u2192",
        "btn_back": "\u2190 Retour",
        "btn_skip": "Ignorer pour l'instant",
        "btn_save": "Enregistrer",
        "btn_test": "Tester",
        "btn_open_dashboard": "Ouvrir le tableau de bord",
        "btn_start": "Commencer",
        "btn_add": "Ajouter",
        "btn_add_another": "Ajouter un autre",
        "btn_complete": "Terminer l'installation",
        "btn_print": "Imprimer",
        "err_required": "Tous les champs obligatoires doivent etre remplis.",
        "err_passwords": "Les mots de passe ne correspondent pas.",
        "err_email": "Adresse courriel invalide.",
        "err_pw_length": "Le mot de passe doit contenir au moins 8 caracteres.",
        "err_pw_upper": "Le mot de passe doit contenir au moins une majuscule.",
        "err_pw_digit": "Le mot de passe doit contenir au moins un chiffre.",
        "err_license": "Cle de licence invalide.",
        "err_username_spaces": "Le nom d'utilisateur ne peut pas contenir d'espaces.",
        "lang_toggle": "English",
        "step_x_of_y": "Etape {x} de {y}",
        "optional": "Optionnel",
        "skip_for_now": "Ignorer pour l'instant",
        "skip_add_later": "Ignorer \u2014 ajouter plus tard",
        "configure_later": "Vous pouvez configurer ceci plus tard dans Parametres",
        "welcome_title": "Bienvenue dans LedgerLink AI",
        "welcome_subtitle": "Ce guide vous prendra environ 15 minutes",
        "welcome_checklist_title": "Ce dont vous aurez besoin",
        "welcome_gst": "Votre numero de TPS (si inscrit)",
        "welcome_qst": "Votre numero de TVQ (si inscrit)",
        "welcome_license": "Votre cle de licence LedgerLink",
        "welcome_password": "Un mot de passe administrateur",
        "welcome_email": "Votre adresse courriel professionnelle",
        "firm_title": "Informations de votre cabinet",
        "firm_subtitle": "Ces informations apparaitront sur vos factures et rapports",
        "firm_name": "Nom du cabinet",
        "firm_name_help": "Ex: Comptabilite Tremblay Inc.",
        "firm_address": "Adresse",
        "firm_city": "Ville",
        "firm_province": "Province",
        "firm_postal": "Code postal",
        "firm_phone": "Telephone",
        "firm_website": "Site web",
        "gst_number": "Numero TPS",
        "gst_help": "Format: 123456789 RT0001 \u2014 laissez vide si non inscrit",
        "qst_number": "Numero TVQ",
        "qst_help": "Format: 1234567890 TQ0001 \u2014 laissez vide si non inscrit",
        "admin_title": "Creer votre compte administrateur",
        "admin_subtitle": "Ce compte aura acces complet au systeme. Gardez ce mot de passe en securite.",
        "admin_fullname": "Prenom et nom",
        "admin_username": "Nom d'utilisateur",
        "admin_username_help": "Pas d'espaces",
        "admin_email": "Adresse courriel",
        "admin_password": "Mot de passe",
        "admin_password_confirm": "Confirmer le mot de passe",
        "pw_req_length": "Minimum 8 caracteres",
        "pw_req_upper": "Au moins une majuscule",
        "pw_req_digit": "Au moins un chiffre",
        "license_title": "Activer votre licence",
        "license_subtitle": "Votre cle de licence vous a ete envoyee par courriel par LedgerLink",
        "license_key": "Cle de licence",
        "license_key_ph": "LLAI-XXXX...",
        "license_validate": "Valider",
        "license_valid": "Licence valide",
        "license_invalid": "Cle de licence invalide",
        "license_tier": "Forfait",
        "license_expiry": "Expiration",
        "license_max_clients": "Clients maximum",
        "license_max_users": "Utilisateurs maximum",
        "license_support": "Contactez support@ledgerlink.app pour obtenir votre cle",
        "ai_title": "Configuration de l'intelligence artificielle",
        "ai_subtitle": "LedgerLink utilise deux services d'IA pour lire vos documents et suggerer des categories. Ces services sont optionnels mais recommandes.",
        "ai_routine_title": "Fournisseur standard (taches repetitives)",
        "ai_routine_rec": "Recommande: DeepSeek",
        "ai_routine_where": "Ou obtenir une cle: deepseek.com \u2192 API \u2192 Create Key",
        "ai_premium_title": "Fournisseur premium (documents complexes)",
        "ai_premium_rec": "Recommande: Anthropic Claude",
        "ai_premium_where": "Ou obtenir une cle: console.anthropic.com \u2192 API Keys",
        "ai_url": "URL de l'API",
        "ai_key": "Cle API",
        "ai_model": "Modele",
        "ai_cost": "Cout estime avec 50 clients: ~15$/mois",
        "email_title": "Configuration du courriel",
        "email_subtitle": "LedgerLink envoie des resumes quotidiens et des messages aux clients par courriel",
        "smtp_host": "Serveur SMTP",
        "smtp_port": "Port",
        "smtp_email": "Adresse courriel",
        "smtp_password": "Mot de passe",
        "smtp_display": "Nom d'affichage",
        "smtp_display_help": "Ex: LedgerLink \u2014 Cabinet Tremblay",
        "email_gmail": "Gmail",
        "email_outlook": "Outlook / Office 365",
        "email_manual": "Configuration manuelle",
        "email_gmail_hint": "Vous devez creer un mot de passe d'application Google",
        "portal_title": "Portail client",
        "portal_subtitle": "Vos clients pourront soumettre leurs documents via un portail web securise",
        "portal_local": "Ce lien fonctionne uniquement dans votre bureau",
        "portal_cf_desc": "Cloudflare protege votre portail avec HTTPS et le rend accessible partout",
        "portal_cf_cost": "Gratuit",
        "portal_cf_btn": "Configurer Cloudflare maintenant",
        "portal_cf_remote": "Pour permettre l'acces a distance, configurez Cloudflare ci-dessous",
        "whatsapp_title": "WhatsApp \u2014 Reception de documents",
        "whatsapp_subtitle": "Vos clients pourront envoyer des photos de recus et factures via WhatsApp. LedgerLink les traitera automatiquement.",
        "whatsapp_need": "Vous avez besoin d'un compte Twilio (gratuit pour commencer)",
        "whatsapp_cost": "~0.005$ par message",
        "whatsapp_sid": "Account SID",
        "whatsapp_sid_help": "Trouvez-le dans votre tableau de bord Twilio",
        "whatsapp_token": "Auth Token",
        "whatsapp_token_help": "Trouvez-le dans votre tableau de bord Twilio",
        "whatsapp_number": "Numero WhatsApp",
        "whatsapp_number_help": "Format: +14155238886 \u2014 votre numero WhatsApp Twilio",
        "whatsapp_enable": "Activer WhatsApp",
        "whatsapp_guide_title": "Guide etape par etape",
        "whatsapp_guide_1": "Creez un compte sur twilio.com",
        "whatsapp_guide_2": "Allez dans Messaging \u2192 WhatsApp Sandbox",
        "whatsapp_guide_3": "Copiez votre Account SID et Auth Token",
        "whatsapp_guide_4": "Entrez votre numero WhatsApp sandbox",
        "whatsapp_guide_5": "Configurez le webhook",
        "telegram_title": "Telegram \u2014 Reception de documents",
        "telegram_subtitle": "Alternative a WhatsApp \u2014 vos clients peuvent envoyer des documents via Telegram",
        "telegram_token": "Token du bot",
        "telegram_token_help": "Creez un bot avec @BotFather sur Telegram",
        "telegram_name": "Nom du bot",
        "telegram_name_help": "Ex: @TremblayComptaBot",
        "telegram_enable": "Activer Telegram",
        "telegram_guide_title": "Guide etape par etape",
        "telegram_guide_1": "Ouvrez Telegram et cherchez @BotFather",
        "telegram_guide_2": "Envoyez /newbot",
        "telegram_guide_3": "Choisissez un nom pour votre bot",
        "telegram_guide_4": "Copiez le token fourni par BotFather",
        "telegram_guide_5": "Collez-le ci-dessus",
        "m365_title": "Microsoft 365 \u2014 Integration courriel et calendrier",
        "m365_subtitle": "Connectez LedgerLink a votre Microsoft 365 pour recevoir automatiquement les factures par courriel et synchroniser les echeances dans Outlook",
        "m365_feat_email": "Lit automatiquement les courriels avec pieces jointes",
        "m365_feat_invoice": "Traite les factures recues par courriel",
        "m365_feat_calendar": "Synchronise les echeances TPS/TVQ dans votre calendrier Outlook",
        "m365_feat_teams": "Envoie le resume quotidien via Teams",
        "m365_service_email": "Compte de service",
        "m365_service_email_help": "Ex: ledgerlink@votrecabinet.com",
        "m365_password": "Mot de passe",
        "m365_tenant_id": "ID de locataire",
        "m365_tenant_help": "Trouvez-le dans le portail Azure \u2192 Azure Active Directory \u2192 Properties",
        "m365_enable": "Activer Microsoft 365",
        "m365_guide_title": "Guide etape par etape",
        "m365_guide_1": "Creez un compte Microsoft 365 pour LedgerLink (ex: ledgerlink@votrecabinet.com)",
        "m365_guide_2": "Connectez-vous au portail Azure: portal.azure.com",
        "m365_guide_3": "Cherchez Azure Active Directory \u2192 Properties",
        "m365_guide_4": "Copiez le Tenant ID",
        "m365_guide_5": "Entrez les informations ci-dessus",
        "qbo_title": "QuickBooks Online \u2014 Synchronisation comptable",
        "qbo_subtitle": "Connectez LedgerLink a QuickBooks Online pour enregistrer automatiquement les transactions approuvees",
        "qbo_feat_post": "Enregistre les transactions approuvees dans QuickBooks",
        "qbo_feat_vendor": "Synchronise les fournisseurs",
        "qbo_feat_account": "Met a jour les comptes",
        "qbo_realm": "Realm ID (Company ID)",
        "qbo_realm_help": "Trouvez-le dans QuickBooks: Parametres \u2192 Compte et parametres \u2192 Facturation",
        "qbo_client_id": "Client ID",
        "qbo_client_secret": "Client Secret",
        "qbo_connect": "Connecter a QuickBooks",
        "qbo_enable": "Activer QuickBooks",
        "qbo_guide_title": "Guide etape par etape",
        "qbo_guide_1": "Connectez-vous a QuickBooks Online",
        "qbo_guide_2": "Allez dans developer.intuit.com",
        "qbo_guide_3": "Creez une application",
        "qbo_guide_4": "Copiez Client ID et Client Secret",
        "qbo_guide_5": "Cliquez Connecter a QuickBooks ci-dessus",
        "folder_title": "Dossier de reception automatique",
        "folder_subtitle": "LedgerLink surveille un dossier sur votre ordinateur. Tout document depose dans ce dossier est traite automatiquement. Ideal pour les scanners USB et OneDrive.",
        "folder_path": "Dossier de reception",
        "folder_create": "Creer le dossier",
        "folder_enable": "Activer la surveillance",
        "folder_scanner": "Scanner USB: Configurez votre scanner pour enregistrer dans ce dossier",
        "folder_onedrive": "OneDrive: Synchronisez ce dossier avec OneDrive pour recevoir des documents a distance",
        "folder_dropbox": "Dropbox/Google Drive: Pointez votre synchronisation vers ce dossier",
        "digest_title": "Resume quotidien",
        "digest_subtitle": "Recevez un courriel chaque matin avec un resume de l'activite de la veille \u2014 documents recus, fraudes detectees, echeances a venir",
        "digest_enable": "Activer le resume quotidien",
        "digest_time": "Heure d'envoi",
        "digest_recipients": "Destinataires",
        "digest_recipients_help": "Adresses courriel separees par des virgules",
        "digest_lang": "Langue",
        "digest_preview": "Apercu",
        "backup_title": "Sauvegarde automatique",
        "backup_subtitle": "LedgerLink sauvegarde automatiquement votre base de donnees. Configurez ou stocker vos sauvegardes.",
        "backup_folder": "Dossier de sauvegarde",
        "backup_freq": "Frequence",
        "backup_freq_daily": "Quotidienne",
        "backup_freq_weekly": "Hebdomadaire",
        "backup_freq_login": "A chaque connexion",
        "backup_keep": "Nombre de sauvegardes a conserver",
        "backup_onedrive": "Sauvegarde OneDrive",
        "backup_onedrive_desc": "Copier egalement les sauvegardes dans le dossier OneDrive Documents",
        "backup_test": "Creer une sauvegarde maintenant",
        "notif_title": "Preferences de notification",
        "notif_subtitle": "Choisissez comment vous souhaitez etre notifie des evenements importants",
        "notif_new_doc": "Nouveau document recu",
        "notif_fraud": "Fraude detectee",
        "notif_pending": "Document en attente depuis plus de X jours",
        "notif_pending_days": "jours",
        "notif_deadline": "Echeance TPS/TVQ dans 14 jours",
        "notif_license": "Licence expire dans 30 jours",
        "notif_error": "Erreur systeme",
        "notif_email": "Courriel",
        "notif_desktop": "Bureau",
        "notif_both": "Les deux",
        "notif_none": "Aucune",
        "security_title": "Parametres de securite",
        "security_subtitle": "Configurez les parametres de securite pour proteger vos donnees clients",
        "security_session": "Expiration de session",
        "security_30m": "30 minutes",
        "security_1h": "1 heure",
        "security_4h": "4 heures",
        "security_8h": "8 heures",
        "security_never": "Jamais",
        "security_max_attempts": "Tentatives de connexion maximum",
        "security_lockout": "Duree de blocage",
        "security_15m": "15 minutes",
        "security_2fa": "Authentification a deux facteurs",
        "security_2fa_soon": "Bientot disponible",
        "security_https": "Forcer HTTPS",
        "security_https_auto": "Active automatiquement si Cloudflare est configure",
        "staff_title": "Ajouter votre equipe",
        "staff_subtitle": "Ajoutez les membres de votre personnel qui utiliseront LedgerLink",
        "staff_fullname": "Prenom et nom",
        "staff_username": "Nom d'utilisateur",
        "staff_role": "Role",
        "staff_role_manager": "Gestionnaire",
        "staff_role_employee": "Employe",
        "staff_temp_pw": "Mot de passe temporaire",
        "clients_title": "Ajouter vos clients",
        "clients_subtitle": "Ajoutez les entreprises dont vous gerez la comptabilite",
        "client_name": "Nom du client",
        "client_name_help": "Ex: Sous-Sol Quebec Inc.",
        "client_code": "Code client",
        "client_code_help": "Max 10 caracteres, majuscules",
        "client_email": "Courriel de contact",
        "client_lang": "Langue",
        "client_freq": "Frequence de production TPS/TVQ",
        "client_freq_monthly": "Mensuelle",
        "client_freq_quarterly": "Trimestrielle",
        "client_freq_annual": "Annuelle",
        "client_accountant": "Comptable assigne",
        "client_import_csv": "Importer depuis un fichier CSV",
        "review_title": "Verification",
        "review_subtitle": "Verifiez votre configuration avant de terminer l'installation",
        "review_firm": "Cabinet",
        "review_admin": "Administrateur",
        "review_license": "Licence",
        "review_ai": "Intelligence artificielle",
        "review_email": "Courriel",
        "review_portal": "Portail client",
        "review_whatsapp": "WhatsApp",
        "review_telegram": "Telegram",
        "review_m365": "Microsoft 365",
        "review_qbo": "QuickBooks Online",
        "review_folder": "Dossier de reception",
        "review_digest": "Resume quotidien",
        "review_backup": "Sauvegarde",
        "review_notif": "Notifications",
        "review_security": "Securite",
        "review_staff": "Membres de l'equipe",
        "review_clients": "Clients",
        "review_configured": "Configure",
        "review_skipped": "Non configure",
        "complete_title": "Installation terminee!",
        "complete_subtitle": "Votre systeme LedgerLink est pret a etre utilise.",
        "complete_dashboard": "URL du tableau de bord",
        "complete_portal": "URL du portail client",
        "complete_copy": "Copier",
        "complete_credentials": "Identifiants de votre equipe",
        "complete_next_title": "Prochaines etapes",
        "complete_next_1": "Partagez les identifiants avec votre equipe",
        "complete_next_2": "Envoyez l'URL du portail a vos clients",
        "complete_next_3": "Configurez les cles API si pas encore fait",
        "complete_next_4": "Lisez le guide d'utilisation",
        "download_access_pdf": "Telecharger les instructions d'acces (PDF)",
        "access_pdf_info": "Generez un PDF avec les URLs et identifiants pour distribuer a votre equipe.",
        # Step labels (kept for compatibility)
        "step1": "Cabinet",
        "step2": "Intelligence artificielle",
        "step3": "Courriel",
        "step4": "Microsoft 365",
        "step5": "Licence",
        "step6": "Termine",
        "validate_license": "Valider",
        "simulated": "Test simule (connexion OK)",
        "license_valid_msg": "Licence valide",
        "license_invalid_msg": "Cle de licence invalide",
        "save_success": "Enregistre avec succes",
        "test_connection": "Tester la connexion",
        "test_email": "Tester",
        "already_complete": "Configuration deja effectuee",
        "already_complete_msg": "Le systeme a deja ete configure. Accedez au tableau de bord.",
        "setup_complete_title": "Configuration terminee !",
        "setup_complete_msg": "Votre systeme LedgerLink est pret a etre utilise.",
        "network_heading": "Configuration reseau",
        "local_url": "URL reseau local",
        "remote_url": "URL acces distant",
        "network_info": "Votre serveur LedgerLink sera accessible a",
        "network_info_suffix": "sur votre reseau local",
        "dashboard_url_label": "URL du tableau de bord",
    },
    "en": {
        "title": "LedgerLink Setup Wizard",
        "btn_next": "Next \u2192",
        "btn_back": "\u2190 Back",
        "btn_skip": "Skip for now",
        "btn_save": "Save",
        "btn_test": "Test",
        "btn_open_dashboard": "Open Dashboard",
        "btn_start": "Start",
        "btn_add": "Add",
        "btn_add_another": "Add another",
        "btn_complete": "Complete Installation",
        "btn_print": "Print",
        "err_required": "All required fields must be filled.",
        "err_passwords": "Passwords do not match.",
        "err_email": "Invalid email address.",
        "err_pw_length": "Password must be at least 8 characters.",
        "err_pw_upper": "Password must contain at least one uppercase letter.",
        "err_pw_digit": "Password must contain at least one number.",
        "err_license": "Invalid license key.",
        "err_username_spaces": "Username cannot contain spaces.",
        "lang_toggle": "Francais",
        "step_x_of_y": "Step {x} of {y}",
        "optional": "Optional",
        "skip_for_now": "Skip for now",
        "skip_add_later": "Skip \u2014 add later",
        "configure_later": "You can configure this later in Settings",
        "welcome_title": "Welcome to LedgerLink AI",
        "welcome_subtitle": "This guide will take about 15 minutes",
        "welcome_checklist_title": "What you will need",
        "welcome_gst": "Your GST number (if registered)",
        "welcome_qst": "Your QST number (if registered)",
        "welcome_license": "Your LedgerLink license key",
        "welcome_password": "An administrator password",
        "welcome_email": "Your professional email address",
        "firm_title": "Your Firm Information",
        "firm_subtitle": "This information will appear on your invoices and reports",
        "firm_name": "Firm name",
        "firm_name_help": "E.g.: Tremblay Accounting Inc.",
        "firm_address": "Address",
        "firm_city": "City",
        "firm_province": "Province",
        "firm_postal": "Postal code",
        "firm_phone": "Phone",
        "firm_website": "Website",
        "gst_number": "GST number",
        "gst_help": "Format: 123456789 RT0001 \u2014 Leave empty if not registered",
        "qst_number": "QST number",
        "qst_help": "Format: 1234567890 TQ0001 \u2014 Leave empty if not registered",
        "admin_title": "Create Your Administrator Account",
        "admin_subtitle": "This account will have full system access. Keep this password secure.",
        "admin_fullname": "Full name",
        "admin_username": "Username",
        "admin_username_help": "No spaces",
        "admin_email": "Email address",
        "admin_password": "Password",
        "admin_password_confirm": "Confirm password",
        "pw_req_length": "Minimum 8 characters",
        "pw_req_upper": "At least one uppercase",
        "pw_req_digit": "At least one number",
        "license_title": "Activate Your License",
        "license_subtitle": "Your license key was emailed to you by LedgerLink",
        "license_key": "License key",
        "license_key_ph": "LLAI-XXXX...",
        "license_validate": "Validate",
        "license_valid": "License is valid",
        "license_invalid": "Invalid license key",
        "license_tier": "Tier",
        "license_expiry": "Expiry",
        "license_max_clients": "Max clients",
        "license_max_users": "Max users",
        "license_support": "Contact support@ledgerlink.app to get your key",
        "ai_title": "AI Configuration",
        "ai_subtitle": "LedgerLink uses two AI services to read your documents and suggest categories. These services are optional but recommended.",
        "ai_routine_title": "Standard provider (routine tasks)",
        "ai_routine_rec": "Recommended: DeepSeek",
        "ai_routine_where": "Where to get a key: deepseek.com \u2192 API \u2192 Create Key",
        "ai_premium_title": "Premium provider (complex documents)",
        "ai_premium_rec": "Recommended: Anthropic Claude",
        "ai_premium_where": "Where to get a key: console.anthropic.com \u2192 API Keys",
        "ai_url": "API URL",
        "ai_key": "API Key",
        "ai_model": "Model",
        "ai_cost": "Estimated cost with 50 clients: ~$15/month",
        "email_title": "Email Configuration",
        "email_subtitle": "LedgerLink sends daily summaries and client messages by email",
        "smtp_host": "SMTP Server",
        "smtp_port": "Port",
        "smtp_email": "Email address",
        "smtp_password": "Password",
        "smtp_display": "Display name",
        "smtp_display_help": "E.g.: LedgerLink \u2014 Tremblay Firm",
        "email_gmail": "Gmail",
        "email_outlook": "Outlook / Office 365",
        "email_manual": "Manual Configuration",
        "email_gmail_hint": "You need to create a Google App Password",
        "portal_title": "Client Portal",
        "portal_subtitle": "Your clients will be able to submit documents via a secure web portal",
        "portal_local": "This link works only in your office",
        "portal_cf_desc": "Cloudflare protects your portal with HTTPS and makes it accessible anywhere",
        "portal_cf_cost": "Free",
        "portal_cf_btn": "Configure Cloudflare now",
        "portal_cf_remote": "To allow remote access, configure Cloudflare below",
        "whatsapp_title": "WhatsApp \u2014 Document Reception",
        "whatsapp_subtitle": "Your clients can send photos of receipts and invoices via WhatsApp. LedgerLink will process them automatically.",
        "whatsapp_need": "You need a Twilio account (free to start)",
        "whatsapp_cost": "~$0.005 per message",
        "whatsapp_sid": "Account SID",
        "whatsapp_sid_help": "Find it in your Twilio dashboard",
        "whatsapp_token": "Auth Token",
        "whatsapp_token_help": "Find it in your Twilio dashboard",
        "whatsapp_number": "WhatsApp Number",
        "whatsapp_number_help": "Format: +14155238886 \u2014 your Twilio WhatsApp number",
        "whatsapp_enable": "Enable WhatsApp",
        "whatsapp_guide_title": "Step-by-step guide",
        "whatsapp_guide_1": "Create an account on twilio.com",
        "whatsapp_guide_2": "Go to Messaging \u2192 WhatsApp Sandbox",
        "whatsapp_guide_3": "Copy your Account SID and Auth Token",
        "whatsapp_guide_4": "Enter your WhatsApp sandbox number",
        "whatsapp_guide_5": "Configure the webhook",
        "telegram_title": "Telegram \u2014 Document Reception",
        "telegram_subtitle": "Alternative to WhatsApp \u2014 clients can send documents via Telegram",
        "telegram_token": "Bot Token",
        "telegram_token_help": "Create a bot with @BotFather on Telegram",
        "telegram_name": "Bot name",
        "telegram_name_help": "E.g.: @TremblayAccountingBot",
        "telegram_enable": "Enable Telegram",
        "telegram_guide_title": "Step-by-step guide",
        "telegram_guide_1": "Open Telegram and search for @BotFather",
        "telegram_guide_2": "Send /newbot",
        "telegram_guide_3": "Choose a name for your bot",
        "telegram_guide_4": "Copy the token provided by BotFather",
        "telegram_guide_5": "Paste it above",
        "m365_title": "Microsoft 365 \u2014 Email and Calendar Integration",
        "m365_subtitle": "Connect LedgerLink to your Microsoft 365 to automatically receive invoices by email and sync deadlines in Outlook",
        "m365_feat_email": "Automatically reads emails with attachments",
        "m365_feat_invoice": "Processes invoices received by email",
        "m365_feat_calendar": "Syncs GST/QST deadlines to your Outlook calendar",
        "m365_feat_teams": "Sends daily digest via Teams",
        "m365_service_email": "Service account email",
        "m365_service_email_help": "E.g.: ledgerlink@yourfirm.com",
        "m365_password": "Password",
        "m365_tenant_id": "Tenant ID",
        "m365_tenant_help": "Find it in Azure portal \u2192 Azure Active Directory \u2192 Properties",
        "m365_enable": "Enable Microsoft 365",
        "m365_guide_title": "Step-by-step guide",
        "m365_guide_1": "Create a Microsoft 365 account for LedgerLink (e.g.: ledgerlink@yourfirm.com)",
        "m365_guide_2": "Sign in to the Azure portal: portal.azure.com",
        "m365_guide_3": "Search for Azure Active Directory \u2192 Properties",
        "m365_guide_4": "Copy the Tenant ID",
        "m365_guide_5": "Enter the information above",
        "qbo_title": "QuickBooks Online \u2014 Accounting Sync",
        "qbo_subtitle": "Connect LedgerLink to QuickBooks Online to automatically post approved transactions",
        "qbo_feat_post": "Posts approved transactions to QuickBooks",
        "qbo_feat_vendor": "Syncs vendors",
        "qbo_feat_account": "Updates accounts",
        "qbo_realm": "Realm ID (Company ID)",
        "qbo_realm_help": "Find it in QuickBooks: Settings \u2192 Account and Settings \u2192 Billing",
        "qbo_client_id": "Client ID",
        "qbo_client_secret": "Client Secret",
        "qbo_connect": "Connect to QuickBooks",
        "qbo_enable": "Enable QuickBooks",
        "qbo_guide_title": "Step-by-step guide",
        "qbo_guide_1": "Sign in to QuickBooks Online",
        "qbo_guide_2": "Go to developer.intuit.com",
        "qbo_guide_3": "Create an application",
        "qbo_guide_4": "Copy Client ID and Client Secret",
        "qbo_guide_5": "Click Connect to QuickBooks above",
        "folder_title": "Automatic Inbox Folder",
        "folder_subtitle": "LedgerLink monitors a folder on your computer. Any document dropped in this folder is processed automatically. Ideal for USB scanners and OneDrive.",
        "folder_path": "Inbox folder",
        "folder_create": "Create folder",
        "folder_enable": "Enable folder watcher",
        "folder_scanner": "USB Scanner: Configure your scanner to save to this folder",
        "folder_onedrive": "OneDrive: Sync this folder with OneDrive to receive documents remotely",
        "folder_dropbox": "Dropbox/Google Drive: Point your sync to this folder",
        "digest_title": "Daily Digest",
        "digest_subtitle": "Receive an email every morning with a summary of yesterday's activity \u2014 documents received, fraud detected, upcoming deadlines",
        "digest_enable": "Enable daily digest",
        "digest_time": "Send time",
        "digest_recipients": "Recipients",
        "digest_recipients_help": "Comma-separated email addresses",
        "digest_lang": "Language",
        "digest_preview": "Preview",
        "backup_title": "Automatic Backup",
        "backup_subtitle": "LedgerLink automatically backs up your database. Configure where to store your backups.",
        "backup_folder": "Backup folder",
        "backup_freq": "Frequency",
        "backup_freq_daily": "Daily",
        "backup_freq_weekly": "Weekly",
        "backup_freq_login": "Every login",
        "backup_keep": "Number of backups to keep",
        "backup_onedrive": "OneDrive Backup",
        "backup_onedrive_desc": "Also copy backups to OneDrive Documents folder",
        "backup_test": "Create a backup now",
        "notif_title": "Notification Preferences",
        "notif_subtitle": "Choose how you want to be notified of important events",
        "notif_new_doc": "New document received",
        "notif_fraud": "Fraud detected",
        "notif_pending": "Document pending more than X days",
        "notif_pending_days": "days",
        "notif_deadline": "GST/QST deadline in 14 days",
        "notif_license": "License expires in 30 days",
        "notif_error": "System error",
        "notif_email": "Email",
        "notif_desktop": "Desktop",
        "notif_both": "Both",
        "notif_none": "None",
        "security_title": "Security Settings",
        "security_subtitle": "Configure security settings to protect your client data",
        "security_session": "Session timeout",
        "security_30m": "30 minutes",
        "security_1h": "1 hour",
        "security_4h": "4 hours",
        "security_8h": "8 hours",
        "security_never": "Never",
        "security_max_attempts": "Maximum login attempts",
        "security_lockout": "Lockout duration",
        "security_15m": "15 minutes",
        "security_2fa": "Two-factor authentication",
        "security_2fa_soon": "Coming soon",
        "security_https": "Force HTTPS",
        "security_https_auto": "Auto-enabled if Cloudflare is configured",
        "staff_title": "Add Your Team",
        "staff_subtitle": "Add the staff members who will use LedgerLink",
        "staff_fullname": "Full name",
        "staff_username": "Username",
        "staff_role": "Role",
        "staff_role_manager": "Manager",
        "staff_role_employee": "Employee",
        "staff_temp_pw": "Temporary password",
        "clients_title": "Add Your Clients",
        "clients_subtitle": "Add the companies whose accounting you manage",
        "client_name": "Client name",
        "client_name_help": "E.g.: Quebec Basement Inc.",
        "client_code": "Client code",
        "client_code_help": "Max 10 chars, uppercase",
        "client_email": "Contact email",
        "client_lang": "Language",
        "client_freq": "GST/QST filing frequency",
        "client_freq_monthly": "Monthly",
        "client_freq_quarterly": "Quarterly",
        "client_freq_annual": "Annual",
        "client_accountant": "Assigned accountant",
        "client_import_csv": "Import from CSV",
        "review_title": "Review",
        "review_subtitle": "Review your configuration before completing the installation",
        "review_firm": "Firm",
        "review_admin": "Administrator",
        "review_license": "License",
        "review_ai": "AI Providers",
        "review_email": "Email",
        "review_portal": "Client Portal",
        "review_whatsapp": "WhatsApp",
        "review_telegram": "Telegram",
        "review_m365": "Microsoft 365",
        "review_qbo": "QuickBooks Online",
        "review_folder": "Inbox Folder",
        "review_digest": "Daily Digest",
        "review_backup": "Backup",
        "review_notif": "Notifications",
        "review_security": "Security",
        "review_staff": "Staff Members",
        "review_clients": "Clients",
        "review_configured": "Configured",
        "review_skipped": "Not configured",
        "complete_title": "Installation Complete!",
        "complete_subtitle": "Your LedgerLink system is ready to use.",
        "complete_dashboard": "Dashboard URL",
        "complete_portal": "Client Portal URL",
        "complete_copy": "Copy",
        "complete_credentials": "Your team credentials",
        "complete_next_title": "Next steps",
        "complete_next_1": "Share credentials with your team",
        "complete_next_2": "Send the portal URL to your clients",
        "complete_next_3": "Configure API keys if not done yet",
        "complete_next_4": "Read the user guide",
        "download_access_pdf": "Download Access Instructions (PDF)",
        "access_pdf_info": "Generate a PDF with URLs and credentials to distribute to your staff.",
        # Compat keys
        "step1": "Firm Information",
        "step2": "Artificial Intelligence",
        "step3": "Email",
        "step4": "Microsoft 365",
        "step5": "License",
        "step6": "Complete",
        "validate_license": "Validate",
        "simulated": "Test simulated (connection OK)",
        "license_valid_msg": "License is valid",
        "license_invalid_msg": "Invalid license key",
        "save_success": "Saved successfully",
        "test_connection": "Test Connection",
        "test_email": "Test",
        "already_complete": "Setup Already Complete",
        "already_complete_msg": "The system has already been configured. Access the dashboard.",
        "setup_complete_title": "Setup Complete!",
        "setup_complete_msg": "Your LedgerLink system is ready to use.",
        "network_heading": "Network Setup",
        "local_url": "Local network URL",
        "remote_url": "Remote access URL",
        "network_info": "Your LedgerLink server will be accessible at",
        "network_info_suffix": "on your local network",
        "dashboard_url_label": "Dashboard URL",
    },
}

# ---------------------------------------------------------------------------
# State / config helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"steps_complete": [], "setup_complete": False}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_config(config: dict) -> None:
    CONFIG_FILE.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_firm(data: dict) -> list[str]:
    errors: list[str] = []
    if not data.get("firm_name", "").strip():
        errors.append("Firm name is required / Le nom du cabinet est obligatoire.")
    if not data.get("firm_city", "").strip():
        errors.append("City is required / La ville est obligatoire.")
    gst = data.get("gst_number", "").strip()
    if gst and not re.match(r"^\d{9}\s*RT\d{4}$", gst, re.IGNORECASE):
        errors.append("Invalid GST format / Format TPS invalide (123456789 RT0001).")
    qst = data.get("qst_number", "").strip()
    if qst and not re.match(r"^\d{10}\s*TQ\d{4}$", qst, re.IGNORECASE):
        errors.append("Invalid QST format / Format TVQ invalide (1234567890 TQ0001).")
    return errors


def validate_admin(data: dict) -> list[str]:
    errors: list[str] = []
    if not data.get("admin_fullname", "").strip():
        errors.append("Full name is required.")
    username = data.get("admin_username", "").strip()
    if not username:
        errors.append("Username is required.")
    if " " in username:
        errors.append("Username cannot contain spaces.")
    email = data.get("admin_email", "").strip()
    if not email or "@" not in email:
        errors.append("Valid email is required.")
    pw = data.get("admin_password", "")
    if len(pw) < 8:
        errors.append("Password must be at least 8 characters.")
    elif not any(c.isupper() for c in pw):
        errors.append("Password must contain at least one uppercase letter.")
    elif not any(c.isdigit() for c in pw):
        errors.append("Password must contain at least one number.")
    if pw != data.get("admin_password_confirm", ""):
        errors.append("Passwords do not match.")
    return errors


# Keep old validate_step1 for backward compat with tests
def validate_step1(data: dict) -> list[str]:
    errors: list[str] = []
    required_fields = [
        "firm_name", "firm_address", "gst_number", "qst_number",
        "owner_name", "owner_email", "owner_password", "owner_password_confirm",
    ]
    if any(not data.get(f, "").strip() for f in required_fields):
        errors.append("All fields are required.")
        return errors
    if "@" not in data.get("owner_email", ""):
        errors.append("Invalid email address.")
    if data.get("owner_password", "") != data.get("owner_password_confirm", ""):
        errors.append("Passwords do not match.")
    return errors


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_dashboard_users_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_users (
            username             TEXT PRIMARY KEY,
            password_hash        TEXT NOT NULL,
            role                 TEXT NOT NULL DEFAULT 'employee',
            display_name         TEXT,
            active               INTEGER NOT NULL DEFAULT 1,
            language             TEXT NOT NULL DEFAULT 'fr',
            must_reset_password  INTEGER NOT NULL DEFAULT 0,
            created_at           TEXT
        )
    """)
    conn.commit()


def _ensure_clients_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_code   TEXT PRIMARY KEY,
            client_name   TEXT NOT NULL,
            contact_email TEXT,
            language      TEXT NOT NULL DEFAULT 'fr',
            filing_freq   TEXT NOT NULL DEFAULT 'quarterly',
            accountant    TEXT,
            active        INTEGER NOT NULL DEFAULT 1,
            created_at    TEXT
        )
    """)
    conn.commit()


def upsert_owner_user(email: str, name: str, password: str, lang: str = "fr") -> None:
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with _open_db() as conn:
        _ensure_dashboard_users_table(conn)
        existing = conn.execute(
            "SELECT username FROM dashboard_users WHERE username=?", (email,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE dashboard_users SET password_hash=?, display_name=?, role='owner', active=1, language=? WHERE username=?",
                (pw_hash, name, lang, email),
            )
        else:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role, display_name, active, language, must_reset_password, created_at) VALUES (?,?,?,?,1,?,0,datetime('now'))",
                (email, pw_hash, "owner", name, lang),
            )
        conn.commit()


def _create_staff_user(username: str, name: str, password: str, role: str, lang: str = "fr") -> None:
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with _open_db() as conn:
        _ensure_dashboard_users_table(conn)
        existing = conn.execute(
            "SELECT username FROM dashboard_users WHERE username=?", (username,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE dashboard_users SET password_hash=?, display_name=?, role=?, active=1, language=?, must_reset_password=1 WHERE username=?",
                (pw_hash, name, role, lang, username),
            )
        else:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role, display_name, active, language, must_reset_password, created_at) VALUES (?,?,?,?,1,?,1,datetime('now'))",
                (username, pw_hash, role, name, lang),
            )
        conn.commit()


def _create_client(code: str, name: str, email: str, language: str,
                   freq: str, accountant: str) -> None:
    with _open_db() as conn:
        _ensure_clients_table(conn)
        existing = conn.execute(
            "SELECT client_code FROM clients WHERE client_code=?", (code,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE clients SET client_name=?, contact_email=?, language=?, filing_freq=?, accountant=? WHERE client_code=?",
                (name, email, language, freq, accountant, code),
            )
        else:
            conn.execute(
                "INSERT INTO clients (client_code, client_name, contact_email, language, filing_freq, accountant, active, created_at) VALUES (?,?,?,?,?,?,1,datetime('now'))",
                (code, name, email, language, freq, accountant),
            )
        conn.commit()


def _gen_temp_password() -> str:
    """Generate a readable temporary password."""
    upper = random.choice(string.ascii_uppercase)
    lower = ''.join(random.choices(string.ascii_lowercase, k=5))
    digits = ''.join(random.choices(string.digits, k=2))
    return upper + lower + digits


def _gen_username(fullname: str) -> str:
    """Auto-generate username from full name."""
    parts = fullname.strip().lower().split()
    if len(parts) >= 2:
        return parts[0][0] + parts[-1]
    elif parts:
        return parts[0]
    return "user"


def _get_staff_list() -> list[dict]:
    """Get list of non-owner staff members."""
    try:
        with _open_db() as conn:
            _ensure_dashboard_users_table(conn)
            rows = conn.execute(
                "SELECT username, display_name, role FROM dashboard_users WHERE role != 'owner' AND active=1 ORDER BY display_name"
            ).fetchall()
            return [{"username": r["username"], "display_name": r["display_name"] or r["username"], "role": r["role"]} for r in rows]
    except Exception:
        return []


def _get_all_staff() -> list[dict]:
    """Get all active staff including owner."""
    try:
        with _open_db() as conn:
            _ensure_dashboard_users_table(conn)
            rows = conn.execute(
                "SELECT username, display_name, role FROM dashboard_users WHERE active=1 ORDER BY role, display_name"
            ).fetchall()
            return [{"username": r["username"], "display_name": r["display_name"] or r["username"], "role": r["role"]} for r in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f1f5f9; min-height: 100vh; }
.wizard-wrap { display: flex; min-height: 100vh; }
.sidebar { background: #1e293b; color: #e2e8f0; width: 260px; min-width: 260px;
           padding: 32px 0; display: flex; flex-direction: column; overflow-y: auto; }
.sidebar-brand { padding: 0 24px 24px; border-bottom: 1px solid #334155; }
.sidebar-brand h1 { font-size: 1.25rem; font-weight: 700; color: #fff; }
.sidebar-brand span { font-size: 0.75rem; color: #94a3b8; }
.sidebar-steps { padding: 12px 0; flex: 1; }
.sidebar-step { display: flex; align-items: center; gap: 10px;
                padding: 7px 24px; cursor: default; transition: background .15s; font-size: 0.8rem; }
.sidebar-step.complete { color: #4ade80; }
.sidebar-step.current { background: #2563eb22; color: #93c5fd; font-weight: 600; }
.sidebar-step.pending { color: #64748b; }
.step-circle { width: 24px; height: 24px; border-radius: 50%; display: flex;
               align-items: center; justify-content: center; font-size: 0.65rem;
               font-weight: 700; flex-shrink: 0; }
.step-circle.complete { background: #16a34a; color: #fff; }
.step-circle.current { background: transparent; border: 2px solid #2563eb; color: #93c5fd; }
.step-circle.pending { background: #334155; color: #64748b; }
.step-label { font-size: 0.8rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.main-area { flex: 1; padding: 32px 40px; overflow-y: auto; }
.topbar { display: flex; justify-content: space-between; align-items: center;
          margin-bottom: 8px; flex-wrap: wrap; gap: 12px; }
.topbar h2 { font-size: 1.4rem; font-weight: 700; color: #1e293b; }
.topbar-right { display: flex; align-items: center; gap: 12px; }
.lang-toggle { background: none; border: 1px solid #cbd5e1; border-radius: 6px;
               padding: 6px 14px; cursor: pointer; font-size: 0.8rem; color: #475569;
               text-decoration: none; }
.lang-toggle:hover { background: #f8fafc; }
.step-indicator { font-size: 0.8rem; color: #94a3b8; font-weight: 500; }
.subtitle { color: #6b7280; font-size: 0.9rem; margin-bottom: 24px; }
.card { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08);
        padding: 28px; max-width: 720px; margin-bottom: 16px; }
.card h3 { font-size: 1.05rem; font-weight: 600; color: #1e293b; margin-bottom: 20px; }
.form-group { margin-bottom: 16px; }
.form-group label { display: block; font-size: 0.85rem; font-weight: 500;
                    color: #374151; margin-bottom: 5px; }
.form-group .help { font-size: 0.75rem; color: #9ca3af; margin-top: 3px; }
.form-group input, .form-group select, .form-group textarea {
    width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 6px;
    font-size: 0.88rem; color: #1e293b; transition: border-color .15s; background: #fff; }
.form-group input:focus, .form-group select:focus, .form-group textarea:focus {
    outline: none; border-color: #2563eb; box-shadow: 0 0 0 3px #2563eb22; }
.form-group input[readonly] { background: #f8fafc; color: #475569; cursor: default; }
.form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.form-row-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; }
.btn { display: inline-flex; align-items: center; gap: 8px;
       padding: 10px 20px; border-radius: 8px; font-size: 0.88rem; font-weight: 500;
       cursor: pointer; border: none; transition: all .15s; text-decoration: none; }
.btn-primary { background: #2563eb; color: #fff; }
.btn-primary:hover { background: #1d4ed8; }
.btn-success { background: #16a34a; color: #fff; }
.btn-success:hover { background: #15803d; }
.btn-secondary { background: #f1f5f9; color: #475569; border: 1px solid #e2e8f0; }
.btn-secondary:hover { background: #e2e8f0; }
.btn-outline { background: transparent; border: 1px solid #d1d5db; color: #6b7280; }
.btn-outline:hover { background: #f9fafb; }
.btn-green { background: #16a34a; color: #fff; font-size: 1.05rem; padding: 14px 32px; }
.btn-green:hover { background: #15803d; }
.btn-actions { display: flex; gap: 12px; margin-top: 24px; flex-wrap: wrap; align-items: center; }
.alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 0.85rem; }
.alert-error { background: #fef2f2; border: 1px solid #fecaca; color: #b91c1c; }
.alert-success { background: #f0fdf4; border: 1px solid #bbf7d0; color: #15803d; }
.alert-info { background: #eff6ff; border: 1px solid #bfdbfe; color: #1d4ed8; }
.alert-warning { background: #fffbeb; border: 1px solid #fde68a; color: #92400e; }
.test-result { margin-top: 10px; font-size: 0.85rem; }
.checklist { list-style: none; padding: 0; }
.checklist li { padding: 8px 0; font-size: 0.9rem; color: #374151; display: flex; align-items: center; gap: 10px; }
.checklist li::before { content: ''; display: none; }
.check-icon { color: #16a34a; font-weight: bold; flex-shrink: 0; }
.progress-bar { height: 6px; background: #e2e8f0; border-radius: 3px; margin-bottom: 16px; overflow: hidden; }
.progress-fill { height: 100%; background: #2563eb; border-radius: 3px; transition: width .3s ease; }
.section-divider { border: none; border-top: 1px solid #e2e8f0; margin: 20px 0; }
.toggle-wrap { display: flex; align-items: center; gap: 10px; }
.toggle { position: relative; display: inline-block; width: 44px; height: 24px; }
.toggle input { opacity: 0; width: 0; height: 0; }
.toggle-slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0;
                 background: #cbd5e1; border-radius: 24px; transition: .2s; }
.toggle-slider::before { content: ""; position: absolute; height: 18px; width: 18px; left: 3px;
                         bottom: 3px; background: white; border-radius: 50%; transition: .2s; }
.toggle input:checked + .toggle-slider { background: #2563eb; }
.toggle input:checked + .toggle-slider::before { transform: translateX(20px); }
.feature-list { list-style: none; padding: 0; margin: 12px 0; }
.feature-list li { padding: 6px 0; font-size: 0.85rem; color: #374151; }
.feature-list li::before { content: '\u2713'; color: #16a34a; font-weight: bold; margin-right: 8px; }
.guide-section { margin-top: 16px; }
.guide-section summary { cursor: pointer; font-weight: 600; font-size: 0.85rem; color: #2563eb; padding: 8px 0; }
.guide-section ol { padding-left: 20px; margin-top: 8px; }
.guide-section li { font-size: 0.85rem; color: #475569; padding: 3px 0; }
.provider-btns { display: flex; gap: 8px; margin-bottom: 16px; }
.staff-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
.staff-table th, .staff-table td { padding: 8px 10px; border-bottom: 1px solid #e2e8f0; text-align: left; }
.staff-table th { background: #f8fafc; font-weight: 600; color: #374151; font-size: 0.8rem; }
.review-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.review-item { display: flex; align-items: center; gap: 10px; padding: 10px 14px;
               background: #f8fafc; border-radius: 8px; font-size: 0.85rem; }
.review-check { color: #16a34a; font-weight: bold; font-size: 1.1rem; }
.review-warn { color: #d97706; font-weight: bold; font-size: 1.1rem; }
.review-label { color: #374151; font-weight: 500; }
.review-status { color: #6b7280; font-size: 0.8rem; }
.complete-box { text-align: center; padding: 32px 20px; }
.complete-icon { font-size: 3.5rem; margin-bottom: 12px; }
.complete-box h2 { font-size: 1.6rem; font-weight: 700; color: #1e293b; margin-bottom: 10px; }
.complete-box p { color: #6b7280; margin-bottom: 8px; }
.url-box { display: flex; align-items: center; gap: 8px; background: #f8fafc; border: 1px solid #e2e8f0;
           border-radius: 8px; padding: 10px 14px; margin: 8px 0; }
.url-box a { color: #2563eb; font-weight: 500; flex: 1; word-break: break-all; }
.url-box .copy-btn { background: #e2e8f0; border: none; padding: 6px 12px; border-radius: 6px;
                     cursor: pointer; font-size: 0.8rem; color: #475569; }
.credentials-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px;
                    padding: 20px; margin: 16px auto; max-width: 500px; text-align: left; }
.credentials-card h4 { margin-bottom: 12px; color: #1e293b; }
.next-steps { text-align: left; max-width: 500px; margin: 20px auto; }
.next-steps li { padding: 6px 0; font-size: 0.9rem; color: #374151; list-style: none; }
.pw-reqs { margin-top: 8px; font-size: 0.8rem; }
.pw-req { display: flex; align-items: center; gap: 6px; padding: 2px 0; color: #9ca3af; }
.pw-req.ok { color: #16a34a; }
.already-box { text-align: center; padding: 60px 20px; }
.already-box h2 { font-size: 1.5rem; font-weight: 700; color: #1e293b; margin-bottom: 12px; }
.already-box p { color: #6b7280; margin-bottom: 24px; }
@media (max-width: 720px) {
    .wizard-wrap { flex-direction: column; }
    .sidebar { width: 100%; min-width: unset; flex-direction: row; flex-wrap: wrap;
               padding: 12px; max-height: none; }
    .sidebar-brand { border-bottom: none; border-right: 1px solid #334155;
                     padding: 0 12px 0 0; margin-right: 12px; }
    .sidebar-steps { display: flex; flex-wrap: wrap; padding: 0; }
    .sidebar-step { padding: 4px 8px; }
    .main-area { padding: 16px; }
    .form-row, .form-row-3, .review-grid { grid-template-columns: 1fr; }
}
"""


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def _s(lang: str, key: str) -> str:
    return STRINGS.get(lang, STRINGS["fr"]).get(key, STRINGS["fr"].get(key, key))


def _esc(v: object) -> str:
    return _html.escape("" if v is None else str(v), quote=True)


def _sidebar_html(state: dict, current_step: int, lang: str) -> str:
    steps_complete = set(state.get("steps_complete", []))
    items = ""
    for n, (path, fr_label, en_label) in enumerate(STEPS):
        label = en_label if lang == "en" else fr_label
        if n in steps_complete:
            cls, circle_cls, marker = "complete", "complete", "\u2713"
        elif n == current_step:
            cls, circle_cls, marker = "current", "current", str(n) if n > 0 else "\u2605"
        else:
            cls, circle_cls, marker = "pending", "pending", str(n) if n > 0 else "\u2605"
        items += f'<div class="sidebar-step {cls}"><div class="step-circle {circle_cls}">{marker}</div><span class="step-label">{_esc(label)}</span></div>'
    return items


def _progress_pct(current_step: int) -> int:
    if current_step <= 0:
        return 0
    return min(100, int(current_step / TOTAL_STEPS * 100))


def _page(content: str, state: dict, current_step: int, lang: str,
          title: str = "", show_back: bool = True) -> str:
    lang_url = f"/setup/lang?set={'en' if lang == 'fr' else 'fr'}&from={urllib.parse.quote(STEP_PATHS.get(current_step, '/'))}"
    lang_label = _s(lang, "lang_toggle")
    page_title = title or _s(lang, "title")
    sidebar_items = _sidebar_html(state, current_step, lang)
    pct = _progress_pct(current_step)
    step_label = ""
    if 1 <= current_step <= TOTAL_STEPS:
        step_label = _s(lang, "step_x_of_y").format(x=current_step, y=TOTAL_STEPS)
    return f"""<!DOCTYPE html>
<html lang="{_esc(lang)}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_esc(page_title)}</title>
<style>{_CSS}</style>
</head>
<body>
<div class="wizard-wrap">
  <div class="sidebar">
    <div class="sidebar-brand">
      <h1>LedgerLink AI</h1>
      <span>{_esc(_s(lang, 'title'))}</span>
    </div>
    <div class="sidebar-steps">
      {sidebar_items}
    </div>
  </div>
  <div class="main-area">
    <div class="topbar">
      <h2>{_esc(page_title)}</h2>
      <div class="topbar-right">
        <span class="step-indicator">{_esc(step_label)}</span>
        <a class="lang-toggle" href="{_esc(lang_url)}">{_esc(lang_label)}</a>
      </div>
    </div>
    <div class="progress-bar"><div class="progress-fill" style="width:{pct}%"></div></div>
    {content}
  </div>
</div>
</body>
</html>"""


def _back_btn(lang: str, step: int) -> str:
    if step <= 0:
        return ""
    prev = STEP_PATHS.get(step - 1, "/")
    return f'<a href="{_esc(prev)}" class="btn btn-secondary">{_esc(_s(lang, "btn_back"))}</a>'


def _skip_link(lang: str, step: int, key: str = "skip_for_now") -> str:
    nxt = STEP_PATHS.get(step + 1, "/setup/review")
    return f'<a href="{_esc(nxt)}" class="btn btn-outline">{_esc(_s(lang, key))}</a>'


# ---------------------------------------------------------------------------
# PDF Access Instructions generator
# ---------------------------------------------------------------------------

def generate_access_instructions_pdf(lang: str = "fr") -> bytes:
    try:
        import qrcode
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.units import inch
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.colors import HexColor
    except ImportError:
        raise RuntimeError("reportlab and qrcode are required for PDF generation")

    cfg = load_config()
    firm = cfg.get("firm", {})
    firm_name = firm.get("firm_name", "LedgerLink")
    local_ip = get_local_ip()
    local_url = f"http://{local_ip}:8787/"
    portal_url = f"http://{local_ip}:8788/"

    staff_users: list[dict] = _get_all_staff()

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)
    w, h = letter
    blue = HexColor("#1F3864")
    grey = HexColor("#475569")

    y = h - 60
    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(blue)
    c.drawCentredString(w / 2, y, firm_name)

    y -= 28
    c.setFont("Helvetica", 12)
    c.setFillColor(grey)
    title = "Instructions d'acces -- LedgerLink" if lang == "fr" else "Access Instructions -- LedgerLink"
    c.drawCentredString(w / 2, y, title)

    y -= 16
    c.setStrokeColor(HexColor("#e2e8f0"))
    c.setLineWidth(1)
    c.line(60, y, w - 60, y)

    y -= 32
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(blue)
    c.drawString(60, y, "Dashboard" if lang == "en" else "Tableau de bord")
    y -= 18
    c.setFont("Helvetica-Bold", 11)
    c.drawString(72, y, local_url)

    y -= 22
    c.setFont("Helvetica-Bold", 13)
    c.drawString(60, y, "Client Portal" if lang == "en" else "Portail client")
    y -= 18
    c.setFont("Helvetica-Bold", 11)
    c.drawString(72, y, portal_url)

    try:
        qr = qrcode.QRCode(version=1, box_size=6, border=2)
        qr.add_data(local_url)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_buf = io.BytesIO()
        qr_img.save(qr_buf, format="PNG")
        qr_buf.seek(0)
        from reportlab.lib.utils import ImageReader
        qr_reader = ImageReader(qr_buf)
        qr_size = 1.2 * inch
        c.drawImage(qr_reader, w - 60 - qr_size, y - qr_size + 16, qr_size, qr_size)
    except Exception:
        pass

    y -= 50
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(blue)
    c.drawString(60, y, "Login Credentials" if lang == "en" else "Identifiants de connexion")

    if staff_users:
        y -= 22
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(grey)
        c.drawString(72, y, "Name" if lang == "en" else "Nom")
        c.drawString(200, y, "Username" if lang == "en" else "Utilisateur")
        c.drawString(370, y, "Role" if lang == "en" else "Role")
        y -= 4
        c.setStrokeColor(HexColor("#d1d5db"))
        c.line(72, y, w - 60, y)
        c.setFont("Helvetica", 9)
        c.setFillColor(HexColor("#1e293b"))
        for user in staff_users:
            y -= 16
            if y < 60:
                break
            c.drawString(72, y, user["display_name"][:22])
            c.drawString(200, y, user["username"][:28])
            c.drawString(370, y, user["role"])

    c.setFont("Helvetica", 8)
    c.setFillColor(HexColor("#94a3b8"))
    c.drawCentredString(w / 2, 30, f"LedgerLink AI -- {firm_name}")
    c.save()
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Step renderers
# ---------------------------------------------------------------------------

def _render_welcome(lang: str, state: dict) -> str:
    return _page(f"""
<div class="card" style="text-align:center;max-width:600px;margin:0 auto;">
  <div style="margin-bottom:20px;">
    <div style="font-size:3rem;margin-bottom:8px;">&#128218;</div>
    <h2 style="font-size:1.5rem;color:#1e293b;margin-bottom:8px;">{_esc(_s(lang, 'welcome_title'))}</h2>
    <p class="subtitle" style="margin-bottom:0;">{_esc(_s(lang, 'welcome_subtitle'))}</p>
  </div>
  <hr class="section-divider">
  <div style="text-align:left;">
    <h3 style="font-size:1rem;margin-bottom:12px;">{_esc(_s(lang, 'welcome_checklist_title'))}</h3>
    <ul class="checklist">
      <li><span class="check-icon">\u2713</span> {_esc(_s(lang, 'welcome_gst'))}</li>
      <li><span class="check-icon">\u2713</span> {_esc(_s(lang, 'welcome_qst'))}</li>
      <li><span class="check-icon">\u2713</span> {_esc(_s(lang, 'welcome_license'))}</li>
      <li><span class="check-icon">\u2713</span> {_esc(_s(lang, 'welcome_password'))}</li>
      <li><span class="check-icon">\u2713</span> {_esc(_s(lang, 'welcome_email'))}</li>
    </ul>
  </div>
  <div class="btn-actions" style="justify-content:center;margin-top:28px;">
    <a href="/setup/firm" class="btn btn-green">{_esc(_s(lang, 'btn_start'))}</a>
  </div>
</div>
""", state, 0, lang, _s(lang, "welcome_title"), show_back=False)


def _render_firm(lang: str, state: dict, error: str = "", data: dict | None = None) -> str:
    d = data or state.get("firm_data", {})
    cfg_firm = load_config().get("firm", {})
    if not d:
        d = cfg_firm
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'firm_subtitle'))}</p>
{err_html}
<div class="card">
  <form method="POST" action="/setup/firm">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-group">
      <label>{_esc(_s(lang, 'firm_name'))} *</label>
      <input type="text" name="firm_name" value="{_esc(d.get('firm_name',''))}" required>
      <div class="help">{_esc(_s(lang, 'firm_name_help'))}</div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'firm_address'))}</label>
      <input type="text" name="firm_address" value="{_esc(d.get('firm_address',''))}">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'firm_city'))} *</label>
        <input type="text" name="firm_city" value="{_esc(d.get('firm_city','Montreal'))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'firm_province'))}</label>
        <select name="firm_province">
          <option value="QC" {'selected' if d.get('firm_province','QC')=='QC' else ''}>Quebec</option>
          <option value="ON" {'selected' if d.get('firm_province')=='ON' else ''}>Ontario</option>
          <option value="BC" {'selected' if d.get('firm_province')=='BC' else ''}>British Columbia</option>
          <option value="AB" {'selected' if d.get('firm_province')=='AB' else ''}>Alberta</option>
          <option value="MB" {'selected' if d.get('firm_province')=='MB' else ''}>Manitoba</option>
          <option value="SK" {'selected' if d.get('firm_province')=='SK' else ''}>Saskatchewan</option>
          <option value="NS" {'selected' if d.get('firm_province')=='NS' else ''}>Nova Scotia</option>
          <option value="NB" {'selected' if d.get('firm_province')=='NB' else ''}>New Brunswick</option>
          <option value="NL" {'selected' if d.get('firm_province')=='NL' else ''}>Newfoundland</option>
          <option value="PE" {'selected' if d.get('firm_province')=='PE' else ''}>Prince Edward Island</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'firm_postal'))}</label>
        <input type="text" name="firm_postal" value="{_esc(d.get('firm_postal',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'firm_phone'))}</label>
        <input type="text" name="firm_phone" value="{_esc(d.get('firm_phone',''))}">
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'firm_website'))} ({_esc(_s(lang, 'optional'))})</label>
      <input type="text" name="firm_website" value="{_esc(d.get('firm_website',''))}">
    </div>
    <hr class="section-divider">
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'gst_number'))}</label>
        <input type="text" name="gst_number" value="{_esc(d.get('gst_number',''))}">
        <div class="help">{_esc(_s(lang, 'gst_help'))}</div>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'qst_number'))}</label>
        <input type="text" name="qst_number" value="{_esc(d.get('qst_number',''))}">
        <div class="help">{_esc(_s(lang, 'qst_help'))}</div>
      </div>
    </div>
    <div class="btn-actions">
      {_back_btn(lang, 1)}
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
    </div>
  </form>
</div>
""", state, 1, lang, _s(lang, "firm_title"))


def _render_admin(lang: str, state: dict, error: str = "", data: dict | None = None) -> str:
    d = data or state.get("admin_data", {})
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'admin_subtitle'))}</p>
{err_html}
<div class="card">
  <form method="POST" action="/setup/admin">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-group">
      <label>{_esc(_s(lang, 'admin_fullname'))} *</label>
      <input type="text" name="admin_fullname" value="{_esc(d.get('admin_fullname',''))}" required
             oninput="suggestUsername(this.value)">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'admin_username'))} *</label>
        <input type="text" name="admin_username" id="adminUsername" value="{_esc(d.get('admin_username',''))}" required>
        <div class="help">{_esc(_s(lang, 'admin_username_help'))}</div>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'admin_email'))} *</label>
        <input type="email" name="admin_email" value="{_esc(d.get('admin_email',''))}" required>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'admin_password'))} *</label>
        <input type="password" name="admin_password" id="adminPw" required oninput="checkPw()">
        <div class="pw-reqs">
          <div class="pw-req" id="pwLen">\u2022 {_esc(_s(lang, 'pw_req_length'))}</div>
          <div class="pw-req" id="pwUpper">\u2022 {_esc(_s(lang, 'pw_req_upper'))}</div>
          <div class="pw-req" id="pwDigit">\u2022 {_esc(_s(lang, 'pw_req_digit'))}</div>
        </div>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'admin_password_confirm'))} *</label>
        <input type="password" name="admin_password_confirm" required>
      </div>
    </div>
    <div class="btn-actions">
      {_back_btn(lang, 2)}
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
    </div>
  </form>
</div>
<script>
function suggestUsername(name) {{
  var parts = name.trim().toLowerCase().split(/\\s+/);
  var u = '';
  if (parts.length >= 2) u = parts[0][0] + parts[parts.length-1];
  else if (parts.length === 1) u = parts[0];
  u = u.replace(/[^a-z0-9._-]/g, '');
  var el = document.getElementById('adminUsername');
  if (!el._userEdited) el.value = u;
}}
document.getElementById('adminUsername').addEventListener('input', function() {{ this._userEdited = true; }});
function checkPw() {{
  var pw = document.getElementById('adminPw').value;
  document.getElementById('pwLen').className = 'pw-req' + (pw.length >= 8 ? ' ok' : '');
  document.getElementById('pwUpper').className = 'pw-req' + (/[A-Z]/.test(pw) ? ' ok' : '');
  document.getElementById('pwDigit').className = 'pw-req' + (/[0-9]/.test(pw) ? ' ok' : '');
}}
</script>
""", state, 2, lang, _s(lang, "admin_title"))


def _render_license(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("license", {})
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'license_subtitle'))}</p>
{err_html}{ok_html}
<div class="card">
  <form method="POST" action="/setup/license">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-group">
      <label>{_esc(_s(lang, 'license_key'))} *</label>
      <textarea name="license_key" rows="3" style="font-family:monospace;font-size:0.85rem;"
        placeholder="{_esc(_s(lang, 'license_key_ph'))}">{_esc(cfg.get('key',''))}</textarea>
    </div>
    <div class="btn-actions">
      {_back_btn(lang, 3)}
      <button type="button" class="btn btn-secondary" id="validateBtn" onclick="validateLic()">{_esc(_s(lang, 'license_validate'))}</button>
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
    </div>
    <div id="licResult" class="test-result"></div>
  </form>
  <p style="margin-top:16px;font-size:0.8rem;color:#9ca3af;">{_esc(_s(lang, 'license_support'))}</p>
</div>
<script>
async function validateLic() {{
  var btn = document.getElementById('validateBtn');
  btn.disabled = true; btn.textContent = '...';
  try {{
    var key = document.querySelector('[name=license_key]').value;
    var r = await fetch('/setup/license/validate', {{method:'POST',
      body: new URLSearchParams({{license_key: key, lang: '{_esc(lang)}'}}),
      headers: {{'Content-Type':'application/x-www-form-urlencoded'}}}});
    var j = await r.json();
    var el = document.getElementById('licResult');
    if (j.ok) {{
      el.innerHTML = '<div class="alert alert-success" style="margin-top:12px;">' + j.message + '</div>';
    }} else {{
      el.innerHTML = '<div class="alert alert-error" style="margin-top:12px;">' + j.message + '</div>';
    }}
  }} catch(e) {{ document.getElementById('licResult').textContent = 'Error: ' + e; }}
  btn.disabled = false; btn.textContent = '{_esc(_s(lang, "license_validate"))}';
}}
</script>
""", state, 3, lang, _s(lang, "license_title"))


def _render_ai(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("ai_router", {})
    routine = cfg.get("routine_provider", cfg.get("routine", {}))
    premium = cfg.get("premium_provider", cfg.get("premium", {}))
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'ai_subtitle'))}</p>
{err_html}{ok_html}
<div class="card">
  <form method="POST" action="/setup/ai">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <h3>{_esc(_s(lang, 'ai_routine_title'))}</h3>
    <div class="alert alert-info" style="margin-bottom:14px;">
      <strong>{_esc(_s(lang, 'ai_routine_rec'))}</strong><br>
      {_esc(_s(lang, 'ai_routine_where'))}
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'ai_url'))}</label>
      <input type="url" name="routine_url" value="{_esc(routine.get('base_url','https://api.deepseek.com/v1/chat/completions'))}">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'ai_key'))}</label>
        <input type="password" name="routine_key" value="{_esc(routine.get('api_key',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'ai_model'))}</label>
        <input type="text" name="routine_model" value="{_esc(routine.get('model','deepseek-chat'))}">
      </div>
    </div>
    <button type="button" class="btn btn-outline" onclick="testAi('routine')">{_esc(_s(lang, 'btn_test'))}</button>
    <div id="testRoutineResult" class="test-result"></div>

    <hr class="section-divider">
    <h3>{_esc(_s(lang, 'ai_premium_title'))}</h3>
    <div class="alert alert-info" style="margin-bottom:14px;">
      <strong>{_esc(_s(lang, 'ai_premium_rec'))}</strong><br>
      {_esc(_s(lang, 'ai_premium_where'))}
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'ai_url'))}</label>
      <input type="url" name="premium_url" value="{_esc(premium.get('base_url','https://api.anthropic.com/v1/messages'))}">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'ai_key'))}</label>
        <input type="password" name="premium_key" value="{_esc(premium.get('api_key',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'ai_model'))}</label>
        <input type="text" name="premium_model" value="{_esc(premium.get('model','claude-haiku-4-5-20251001'))}">
      </div>
    </div>
    <button type="button" class="btn btn-outline" onclick="testAi('premium')">{_esc(_s(lang, 'btn_test'))}</button>
    <div id="testPremiumResult" class="test-result"></div>

    <div class="alert alert-info" style="margin-top:20px;">{_esc(_s(lang, 'ai_cost'))}</div>

    <div class="btn-actions">
      {_back_btn(lang, 4)}
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
      {_skip_link(lang, 4)}
    </div>
  </form>
</div>
<script>
async function testAi(which) {{
  try {{
    var form = document.querySelector('form');
    var data = new FormData(form);
    var r = await fetch('/setup/ai/test', {{method:'POST',
      body: new URLSearchParams(data),
      headers: {{'Content-Type':'application/x-www-form-urlencoded'}}}});
    var j = await r.json();
    var el = document.getElementById('test' + which.charAt(0).toUpperCase() + which.slice(1) + 'Result');
    el.innerHTML = '<span style="color:' + (j.ok ? '#16a34a' : '#b91c1c') + '">' + j.message + '</span>';
  }} catch(e) {{ alert(e); }}
}}
</script>
""", state, 4, lang, _s(lang, "ai_title"))


def _render_email(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("email", load_config().get("digest", {}))
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'email_subtitle'))}</p>
{err_html}{ok_html}
<div class="card">
  <form method="POST" action="/setup/email">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="provider-btns">
      <button type="button" class="btn btn-outline" onclick="prefillEmail('gmail')">{_esc(_s(lang, 'email_gmail'))}</button>
      <button type="button" class="btn btn-outline" onclick="prefillEmail('outlook')">{_esc(_s(lang, 'email_outlook'))}</button>
      <button type="button" class="btn btn-outline" onclick="prefillEmail('manual')">{_esc(_s(lang, 'email_manual'))}</button>
    </div>
    <div id="gmailHint" class="alert alert-info" style="display:none;">{_esc(_s(lang, 'email_gmail_hint'))}</div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'smtp_host'))}</label>
        <input type="text" name="smtp_host" id="smtpHost" value="{_esc(cfg.get('smtp_host',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'smtp_port'))}</label>
        <input type="number" name="smtp_port" id="smtpPort" value="{_esc(cfg.get('smtp_port','587'))}">
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'smtp_email'))}</label>
        <input type="email" name="smtp_email" value="{_esc(cfg.get('smtp_user',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'smtp_password'))}</label>
        <input type="password" name="smtp_password" value="{_esc(cfg.get('smtp_password',''))}">
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'smtp_display'))}</label>
      <input type="text" name="smtp_display" value="{_esc(cfg.get('from_name','LedgerLink AI'))}">
      <div class="help">{_esc(_s(lang, 'smtp_display_help'))}</div>
    </div>
    <button type="button" class="btn btn-outline" onclick="testEmail()">{_esc(_s(lang, 'btn_test'))}</button>
    <div id="testEmailResult" class="test-result"></div>
    <div class="btn-actions">
      {_back_btn(lang, 5)}
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
      {_skip_link(lang, 5)}
    </div>
  </form>
</div>
<script>
function prefillEmail(type) {{
  var host = document.getElementById('smtpHost');
  var port = document.getElementById('smtpPort');
  var hint = document.getElementById('gmailHint');
  hint.style.display = 'none';
  if (type === 'gmail') {{ host.value = 'smtp.gmail.com'; port.value = '587'; hint.style.display = 'block'; }}
  else if (type === 'outlook') {{ host.value = 'smtp.office365.com'; port.value = '587'; }}
  else {{ host.value = ''; port.value = '587'; }}
}}
async function testEmail() {{
  try {{
    var form = document.querySelector('form');
    var r = await fetch('/setup/email/test', {{method:'POST',
      body: new URLSearchParams(new FormData(form)),
      headers: {{'Content-Type':'application/x-www-form-urlencoded'}}}});
    var j = await r.json();
    document.getElementById('testEmailResult').innerHTML =
      '<span style="color:' + (j.ok ? '#16a34a' : '#b91c1c') + '">' + j.message + '</span>';
  }} catch(e) {{ document.getElementById('testEmailResult').textContent = 'Error: ' + e; }}
}}
</script>
""", state, 5, lang, _s(lang, "email_title"))


def _render_portal(lang: str, state: dict) -> str:
    local_ip = get_local_ip()
    portal_url = f"http://{local_ip}:8788/"
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'portal_subtitle'))}</p>
<div class="card">
  <h3>{_esc(_s(lang, 'portal_title'))}</h3>
  <div class="url-box">
    <a href="{_esc(portal_url)}" target="_blank">{_esc(portal_url)}</a>
  </div>
  <p style="font-size:0.85rem;color:#6b7280;margin-bottom:20px;">
    {_esc(_s(lang, 'portal_local'))}. {_esc(_s(lang, 'portal_cf_remote'))}
  </p>
  <hr class="section-divider">
  <h3>Cloudflare</h3>
  <p style="font-size:0.85rem;color:#6b7280;margin-bottom:12px;">
    {_esc(_s(lang, 'portal_cf_desc'))}
  </p>
  <p style="font-size:0.85rem;color:#16a34a;margin-bottom:16px;">
    {_esc(_s(lang, 'portal_cf_cost'))}
  </p>
  <div class="btn-actions">
    {_back_btn(lang, 6)}
    <a href="/setup/whatsapp" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</a>
    {_skip_link(lang, 6)}
  </div>
</div>
""", state, 6, lang, _s(lang, "portal_title"))


def _render_integration_step(lang: str, state: dict, step_num: int,
                             title_key: str, subtitle_key: str,
                             fields_html: str, guide_html: str = "",
                             features_html: str = "") -> str:
    return _page(f"""
<p class="subtitle">{_esc(_s(lang, subtitle_key))}</p>
<div class="card">
  <form method="POST" action="{_esc(STEP_PATHS[step_num])}">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    {features_html}
    {fields_html}
    {guide_html}
    <div class="btn-actions">
      {_back_btn(lang, step_num)}
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
      {_skip_link(lang, step_num)}
    </div>
  </form>
</div>
""", state, step_num, lang, _s(lang, title_key))


def _render_whatsapp(lang: str, state: dict) -> str:
    cfg = load_config().get("whatsapp", {})
    fields = f"""
    <div class="alert alert-info" style="margin-bottom:16px;">
      {_esc(_s(lang, 'whatsapp_need'))}<br>
      <strong>{_esc(_s(lang, 'whatsapp_cost'))}</strong>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'whatsapp_sid'))}</label>
      <input type="text" name="whatsapp_sid" value="{_esc(cfg.get('account_sid',''))}">
      <div class="help">{_esc(_s(lang, 'whatsapp_sid_help'))}</div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'whatsapp_token'))}</label>
      <input type="password" name="whatsapp_token" value="{_esc(cfg.get('auth_token',''))}">
      <div class="help">{_esc(_s(lang, 'whatsapp_token_help'))}</div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'whatsapp_number'))}</label>
      <input type="text" name="whatsapp_number" value="{_esc(cfg.get('number',''))}">
      <div class="help">{_esc(_s(lang, 'whatsapp_number_help'))}</div>
    </div>
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="whatsapp_enabled" value="1" {'checked' if cfg.get('enabled') else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'whatsapp_enable'))}</span>
    </div>
"""
    guide = f"""
    <details class="guide-section">
      <summary>{_esc(_s(lang, 'whatsapp_guide_title'))}</summary>
      <ol>
        <li>{_esc(_s(lang, 'whatsapp_guide_1'))}</li>
        <li>{_esc(_s(lang, 'whatsapp_guide_2'))}</li>
        <li>{_esc(_s(lang, 'whatsapp_guide_3'))}</li>
        <li>{_esc(_s(lang, 'whatsapp_guide_4'))}</li>
        <li>{_esc(_s(lang, 'whatsapp_guide_5'))}</li>
      </ol>
    </details>
"""
    return _render_integration_step(lang, state, 7, "whatsapp_title", "whatsapp_subtitle", fields, guide)


def _render_telegram(lang: str, state: dict) -> str:
    cfg = load_config().get("telegram", {})
    fields = f"""
    <div class="form-group">
      <label>{_esc(_s(lang, 'telegram_token'))}</label>
      <input type="text" name="telegram_token" value="{_esc(cfg.get('bot_token',''))}">
      <div class="help">{_esc(_s(lang, 'telegram_token_help'))}</div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'telegram_name'))}</label>
      <input type="text" name="telegram_name" value="{_esc(cfg.get('bot_name',''))}">
      <div class="help">{_esc(_s(lang, 'telegram_name_help'))}</div>
    </div>
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="telegram_enabled" value="1" {'checked' if cfg.get('enabled') else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'telegram_enable'))}</span>
    </div>
"""
    guide = f"""
    <details class="guide-section">
      <summary>{_esc(_s(lang, 'telegram_guide_title'))}</summary>
      <ol>
        <li>{_esc(_s(lang, 'telegram_guide_1'))}</li>
        <li>{_esc(_s(lang, 'telegram_guide_2'))}</li>
        <li>{_esc(_s(lang, 'telegram_guide_3'))}</li>
        <li>{_esc(_s(lang, 'telegram_guide_4'))}</li>
        <li>{_esc(_s(lang, 'telegram_guide_5'))}</li>
      </ol>
    </details>
"""
    return _render_integration_step(lang, state, 8, "telegram_title", "telegram_subtitle", fields, guide)


def _render_m365(lang: str, state: dict) -> str:
    cfg = load_config().get("microsoft365", {})
    features = f"""
    <ul class="feature-list">
      <li>{_esc(_s(lang, 'm365_feat_email'))}</li>
      <li>{_esc(_s(lang, 'm365_feat_invoice'))}</li>
      <li>{_esc(_s(lang, 'm365_feat_calendar'))}</li>
      <li>{_esc(_s(lang, 'm365_feat_teams'))}</li>
    </ul>
"""
    fields = f"""
    <div class="form-group">
      <label>{_esc(_s(lang, 'm365_service_email'))}</label>
      <input type="email" name="m365_email" value="{_esc(cfg.get('service_email',''))}">
      <div class="help">{_esc(_s(lang, 'm365_service_email_help'))}</div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'm365_password'))}</label>
        <input type="password" name="m365_password" value="{_esc(cfg.get('password',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'm365_tenant_id'))}</label>
        <input type="text" name="m365_tenant" value="{_esc(cfg.get('tenant_id',''))}">
        <div class="help">{_esc(_s(lang, 'm365_tenant_help'))}</div>
      </div>
    </div>
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="m365_enabled" value="1" {'checked' if cfg.get('enabled') else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'm365_enable'))}</span>
    </div>
"""
    guide = f"""
    <details class="guide-section">
      <summary>{_esc(_s(lang, 'm365_guide_title'))}</summary>
      <ol>
        <li>{_esc(_s(lang, 'm365_guide_1'))}</li>
        <li>{_esc(_s(lang, 'm365_guide_2'))}</li>
        <li>{_esc(_s(lang, 'm365_guide_3'))}</li>
        <li>{_esc(_s(lang, 'm365_guide_4'))}</li>
        <li>{_esc(_s(lang, 'm365_guide_5'))}</li>
      </ol>
    </details>
"""
    return _render_integration_step(lang, state, 9, "m365_title", "m365_subtitle", fields, guide, features)


def _render_quickbooks(lang: str, state: dict) -> str:
    cfg = load_config().get("quickbooks", {})
    features = f"""
    <ul class="feature-list">
      <li>{_esc(_s(lang, 'qbo_feat_post'))}</li>
      <li>{_esc(_s(lang, 'qbo_feat_vendor'))}</li>
      <li>{_esc(_s(lang, 'qbo_feat_account'))}</li>
    </ul>
"""
    fields = f"""
    <div class="form-group">
      <label>{_esc(_s(lang, 'qbo_realm'))}</label>
      <input type="text" name="qbo_realm" value="{_esc(cfg.get('realm_id',''))}">
      <div class="help">{_esc(_s(lang, 'qbo_realm_help'))}</div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'qbo_client_id'))}</label>
        <input type="text" name="qbo_client_id" value="{_esc(cfg.get('client_id',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'qbo_client_secret'))}</label>
        <input type="password" name="qbo_client_secret" value="{_esc(cfg.get('client_secret',''))}">
      </div>
    </div>
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="qbo_enabled" value="1" {'checked' if cfg.get('enabled') else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'qbo_enable'))}</span>
    </div>
"""
    guide = f"""
    <details class="guide-section">
      <summary>{_esc(_s(lang, 'qbo_guide_title'))}</summary>
      <ol>
        <li>{_esc(_s(lang, 'qbo_guide_1'))}</li>
        <li>{_esc(_s(lang, 'qbo_guide_2'))}</li>
        <li>{_esc(_s(lang, 'qbo_guide_3'))}</li>
        <li>{_esc(_s(lang, 'qbo_guide_4'))}</li>
        <li>{_esc(_s(lang, 'qbo_guide_5'))}</li>
      </ol>
    </details>
"""
    return _render_integration_step(lang, state, 10, "qbo_title", "qbo_subtitle", fields, guide, features)


def _render_folder(lang: str, state: dict) -> str:
    cfg = load_config().get("folder_watcher", {})
    _default_inbox = "C:/LedgerLink/Inbox/"
    default_path = cfg.get("inbox_path", _default_inbox)
    fields = f"""
    <div class="form-group">
      <label>{_esc(_s(lang, 'folder_path'))}</label>
      <input type="text" name="folder_path" value="{_esc(default_path)}">
    </div>
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="folder_enabled" value="1" {'checked' if cfg.get('enabled') else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'folder_enable'))}</span>
    </div>
    <hr class="section-divider">
    <div style="font-size:0.85rem;color:#475569;">
      <p style="margin-bottom:6px;"><strong>{_esc(_s(lang, 'folder_scanner'))}</strong></p>
      <p style="margin-bottom:6px;"><strong>{_esc(_s(lang, 'folder_onedrive'))}</strong></p>
      <p><strong>{_esc(_s(lang, 'folder_dropbox'))}</strong></p>
    </div>
"""
    return _render_integration_step(lang, state, 11, "folder_title", "folder_subtitle", fields)


def _render_digest(lang: str, state: dict) -> str:
    cfg = load_config().get("digest_config", {})
    admin_email = load_config().get("firm", {}).get("owner_email", "")
    fields = f"""
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="digest_enabled" value="1" {'checked' if cfg.get('enabled', True) else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'digest_enable'))}</span>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'digest_time'))}</label>
        <input type="time" name="digest_time" value="{_esc(cfg.get('send_time','07:00'))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'digest_lang'))}</label>
        <select name="digest_lang">
          <option value="fr" {'selected' if cfg.get('language','fr')=='fr' else ''}>Francais</option>
          <option value="en" {'selected' if cfg.get('language')=='en' else ''}>English</option>
        </select>
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'digest_recipients'))}</label>
      <input type="text" name="digest_recipients" value="{_esc(cfg.get('recipients', admin_email))}">
      <div class="help">{_esc(_s(lang, 'digest_recipients_help'))}</div>
    </div>
"""
    return _render_integration_step(lang, state, 12, "digest_title", "digest_subtitle", fields)


def _render_backup(lang: str, state: dict) -> str:
    cfg = load_config().get("backup", {})
    _default_backup = "C:/LedgerLink/Backups/"
    fields = f"""
    <div class="form-group">
      <label>{_esc(_s(lang, 'backup_folder'))}</label>
      <input type="text" name="backup_folder" value="{_esc(cfg.get('folder', _default_backup))}">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'backup_freq'))}</label>
        <select name="backup_freq">
          <option value="daily" {'selected' if cfg.get('frequency','daily')=='daily' else ''}>{_esc(_s(lang, 'backup_freq_daily'))}</option>
          <option value="weekly" {'selected' if cfg.get('frequency')=='weekly' else ''}>{_esc(_s(lang, 'backup_freq_weekly'))}</option>
          <option value="login" {'selected' if cfg.get('frequency')=='login' else ''}>{_esc(_s(lang, 'backup_freq_login'))}</option>
        </select>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'backup_keep'))}</label>
        <input type="number" name="backup_keep" value="{_esc(cfg.get('keep_count', 30))}">
      </div>
    </div>
    <div class="toggle-wrap" style="margin-bottom:16px;">
      <label class="toggle"><input type="checkbox" name="backup_onedrive" value="1" {'checked' if cfg.get('onedrive') else ''}><span class="toggle-slider"></span></label>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'backup_onedrive'))} &mdash; {_esc(_s(lang, 'backup_onedrive_desc'))}</span>
    </div>
"""
    return _render_integration_step(lang, state, 13, "backup_title", "backup_subtitle", fields)


def _render_notifications(lang: str, state: dict) -> str:
    cfg = load_config().get("notifications", {})

    def _notif_select(name: str, default: str = "email") -> str:
        val = cfg.get(name, default)
        opts = ""
        for v, k in [("email", "notif_email"), ("desktop", "notif_desktop"), ("both", "notif_both"), ("none", "notif_none")]:
            sel = "selected" if val == v else ""
            opts += f'<option value="{v}" {sel}>{_esc(_s(lang, k))}</option>'
        return f'<select name="{_esc(name)}">{opts}</select>'

    fields = f"""
    <div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center;">
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'notif_new_doc'))}</span>
      {_notif_select('notif_new_doc', 'email')}
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'notif_fraud'))}</span>
      {_notif_select('notif_fraud', 'email')}
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'notif_pending'))}</span>
      <div style="display:flex;gap:8px;align-items:center;">
        {_notif_select('notif_pending', 'email')}
        <input type="number" name="notif_pending_days" value="{_esc(cfg.get('pending_days', 3))}" style="width:60px;padding:8px;border:1px solid #d1d5db;border-radius:6px;">
        <span style="font-size:0.8rem;color:#6b7280;">{_esc(_s(lang, 'notif_pending_days'))}</span>
      </div>
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'notif_deadline'))}</span>
      {_notif_select('notif_deadline', 'email')}
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'notif_license'))}</span>
      {_notif_select('notif_license', 'email')}
      <span style="font-size:0.85rem;">{_esc(_s(lang, 'notif_error'))}</span>
      {_notif_select('notif_error', 'email')}
    </div>
"""
    return _render_integration_step(lang, state, 14, "notif_title", "notif_subtitle", fields)


def _render_security(lang: str, state: dict) -> str:
    cfg = load_config().get("security_settings", {})
    cf_configured = bool(load_config().get("cloudflare", {}).get("configured"))

    def _sess_sel(val: str, label_key: str) -> str:
        return f'<option value="{val}" {"selected" if cfg.get("session_timeout", "4h") == val else ""}>{_esc(_s(lang, label_key))}</option>'

    def _lock_sel(val: str, label_key: str) -> str:
        return f'<option value="{val}" {"selected" if cfg.get("lockout_duration", "15m") == val else ""}>{_esc(_s(lang, label_key))}</option>'

    fields = f"""
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'security_session'))}</label>
        <select name="session_timeout">
          {_sess_sel('30m', 'security_30m')}
          {_sess_sel('1h', 'security_1h')}
          {_sess_sel('4h', 'security_4h')}
          {_sess_sel('8h', 'security_8h')}
          {_sess_sel('never', 'security_never')}
        </select>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'security_max_attempts'))}</label>
        <input type="number" name="max_login_attempts" value="{_esc(cfg.get('max_login_attempts', 5))}">
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, 'security_lockout'))}</label>
      <select name="lockout_duration">
        {_lock_sel('15m', 'security_15m')}
        {_lock_sel('30m', 'security_30m')}
        {_lock_sel('1h', 'security_1h')}
      </select>
    </div>
    <hr class="section-divider">
    <div class="form-group" style="opacity:0.5;">
      <div class="toggle-wrap">
        <label class="toggle"><input type="checkbox" disabled><span class="toggle-slider"></span></label>
        <span style="font-size:0.85rem;">{_esc(_s(lang, 'security_2fa'))} &mdash; <em>{_esc(_s(lang, 'security_2fa_soon'))}</em></span>
      </div>
    </div>
    <div class="form-group">
      <div class="toggle-wrap">
        <label class="toggle"><input type="checkbox" name="force_https" value="1" {'checked' if cfg.get('force_https') or cf_configured else ''}><span class="toggle-slider"></span></label>
        <span style="font-size:0.85rem;">{_esc(_s(lang, 'security_https'))}</span>
      </div>
      <div class="help">{_esc(_s(lang, 'security_https_auto'))}</div>
    </div>
"""
    return _render_integration_step(lang, state, 15, "security_title", "security_subtitle", fields)


def _render_staff(lang: str, state: dict, error: str = "", success: str = "") -> str:
    staff = _get_staff_list()
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""

    # Show temp passwords from state
    temp_pws = state.get("temp_passwords", {})
    staff_rows = ""
    for s in staff:
        pw_display = temp_pws.get(s["username"], "***")
        role_label = _s(lang, "staff_role_manager") if s["role"] == "manager" else _s(lang, "staff_role_employee")
        staff_rows += f'<tr><td>{_esc(s["display_name"])}</td><td>{_esc(s["username"])}</td><td>{_esc(role_label)}</td><td><code>{_esc(pw_display)}</code></td></tr>'

    table = ""
    if staff:
        table = f"""
    <table class="staff-table">
      <thead><tr>
        <th>{_esc(_s(lang, 'staff_fullname'))}</th>
        <th>{_esc(_s(lang, 'staff_username'))}</th>
        <th>{_esc(_s(lang, 'staff_role'))}</th>
        <th>{_esc(_s(lang, 'staff_temp_pw'))}</th>
      </tr></thead>
      <tbody>{staff_rows}</tbody>
    </table>
    <hr class="section-divider">
"""

    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'staff_subtitle'))}</p>
{err_html}{ok_html}
<div class="card">
  {table}
  <form method="POST" action="/setup/staff">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <h3>{_esc(_s(lang, 'btn_add'))}</h3>
    <div class="form-group">
      <label>{_esc(_s(lang, 'staff_fullname'))}</label>
      <input type="text" name="staff_fullname" oninput="suggestStaffUser(this.value)">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'staff_username'))}</label>
        <input type="text" name="staff_username" id="staffUsername">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'staff_role'))}</label>
        <select name="staff_role">
          <option value="manager">{_esc(_s(lang, 'staff_role_manager'))}</option>
          <option value="employee" selected>{_esc(_s(lang, 'staff_role_employee'))}</option>
        </select>
      </div>
    </div>
    <div class="btn-actions">
      {_back_btn(lang, 16)}
      <button type="submit" name="action" value="add" class="btn btn-secondary">{_esc(_s(lang, 'btn_add'))}</button>
      <button type="submit" name="action" value="next" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
      {_skip_link(lang, 16, 'skip_add_later')}
    </div>
  </form>
</div>
<script>
function suggestStaffUser(name) {{
  var parts = name.trim().toLowerCase().split(/\\s+/);
  var u = '';
  if (parts.length >= 2) u = parts[0][0] + parts[parts.length-1];
  else if (parts.length === 1) u = parts[0];
  u = u.replace(/[^a-z0-9._-]/g, '');
  document.getElementById('staffUsername').value = u;
}}
</script>
""", state, 16, lang, _s(lang, "staff_title"))


def _render_clients(lang: str, state: dict, error: str = "", success: str = "") -> str:
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""

    # Get existing clients from state
    clients = state.get("wizard_clients", [])
    staff = _get_all_staff()
    staff_options = "".join(f'<option value="{_esc(s["username"])}">{_esc(s["display_name"])}</option>' for s in staff)

    client_rows = ""
    for c in clients:
        client_rows += f'<tr><td>{_esc(c.get("name",""))}</td><td><code>{_esc(c.get("code",""))}</code></td><td>{_esc(c.get("email",""))}</td><td>{_esc(c.get("freq",""))}</td></tr>'

    table = ""
    if clients:
        table = f"""
    <table class="staff-table">
      <thead><tr><th>{_esc(_s(lang, 'client_name'))}</th><th>{_esc(_s(lang, 'client_code'))}</th><th>{_esc(_s(lang, 'client_email'))}</th><th>{_esc(_s(lang, 'client_freq'))}</th></tr></thead>
      <tbody>{client_rows}</tbody>
    </table>
    <hr class="section-divider">
"""

    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'clients_subtitle'))}</p>
{err_html}{ok_html}
<div class="card">
  {table}
  <form method="POST" action="/setup/clients">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <h3>{_esc(_s(lang, 'btn_add'))}</h3>
    <div class="form-group">
      <label>{_esc(_s(lang, 'client_name'))}</label>
      <input type="text" name="client_name" oninput="suggestCode(this.value)">
      <div class="help">{_esc(_s(lang, 'client_name_help'))}</div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, 'client_code'))}</label>
        <input type="text" name="client_code" id="clientCode" maxlength="10" style="text-transform:uppercase;">
        <div class="help">{_esc(_s(lang, 'client_code_help'))}</div>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'client_email'))}</label>
        <input type="email" name="client_email">
      </div>
    </div>
    <div class="form-row-3">
      <div class="form-group">
        <label>{_esc(_s(lang, 'client_lang'))}</label>
        <select name="client_lang">
          <option value="fr" selected>Francais</option>
          <option value="en">English</option>
        </select>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'client_freq'))}</label>
        <select name="client_freq">
          <option value="monthly">{_esc(_s(lang, 'client_freq_monthly'))}</option>
          <option value="quarterly" selected>{_esc(_s(lang, 'client_freq_quarterly'))}</option>
          <option value="annual">{_esc(_s(lang, 'client_freq_annual'))}</option>
        </select>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, 'client_accountant'))}</label>
        <select name="client_accountant">
          <option value="">--</option>
          {staff_options}
        </select>
      </div>
    </div>
    <div class="btn-actions">
      {_back_btn(lang, 17)}
      <button type="submit" name="action" value="add" class="btn btn-secondary">{_esc(_s(lang, 'btn_add'))}</button>
      <button type="submit" name="action" value="next" class="btn btn-primary">{_esc(_s(lang, 'btn_next'))}</button>
      {_skip_link(lang, 17, 'skip_add_later')}
    </div>
  </form>
</div>
<script>
function suggestCode(name) {{
  var code = name.trim().toUpperCase().replace(/[^A-Z0-9]/g, '').substring(0, 10);
  document.getElementById('clientCode').value = code;
}}
</script>
""", state, 17, lang, _s(lang, "clients_title"))


def _render_review(lang: str, state: dict) -> str:
    cfg = load_config()
    firm = cfg.get("firm", {})
    admin = state.get("admin_data", {})

    def _item(label_key: str, configured: bool) -> str:
        if configured:
            return f'<div class="review-item"><span class="review-check">\u2713</span><span class="review-label">{_esc(_s(lang, label_key))}</span><span class="review-status">{_esc(_s(lang, "review_configured"))}</span></div>'
        else:
            return f'<div class="review-item"><span class="review-warn">\u25CB</span><span class="review-label">{_esc(_s(lang, label_key))}</span><span class="review-status">{_esc(_s(lang, "review_skipped"))}<br><small>{_esc(_s(lang, "configure_later"))}</small></span></div>'

    staff_count = len(_get_staff_list())
    client_count = len(state.get("wizard_clients", []))

    items = ""
    items += _item("review_firm", bool(firm.get("firm_name")))
    items += _item("review_admin", bool(admin.get("admin_username")))
    items += _item("review_license", bool(cfg.get("license", {}).get("key")))
    items += _item("review_ai", bool(cfg.get("ai_router", {}).get("routine_provider", cfg.get("ai_router", {}).get("routine", {})).get("api_key")))
    items += _item("review_email", bool(cfg.get("email", {}).get("smtp_host")))
    items += _item("review_portal", True)
    items += _item("review_whatsapp", bool(cfg.get("whatsapp", {}).get("enabled")))
    items += _item("review_telegram", bool(cfg.get("telegram", {}).get("enabled")))
    items += _item("review_m365", bool(cfg.get("microsoft365", {}).get("enabled")))
    items += _item("review_qbo", bool(cfg.get("quickbooks", {}).get("enabled")))
    items += _item("review_folder", bool(cfg.get("folder_watcher", {}).get("enabled")))
    items += _item("review_digest", bool(cfg.get("digest_config", {}).get("enabled")))
    items += _item("review_backup", bool(cfg.get("backup", {}).get("folder")))
    items += _item("review_notif", 14 in state.get("steps_complete", []))
    items += _item("review_security", 15 in state.get("steps_complete", []))

    staff_line = f'<div class="review-item"><span class="review-check">\u2713</span><span class="review-label">{_esc(_s(lang, "review_staff"))}: {staff_count}</span></div>' if staff_count else _item("review_staff", False)
    client_line = f'<div class="review-item"><span class="review-check">\u2713</span><span class="review-label">{_esc(_s(lang, "review_clients"))}: {client_count}</span></div>' if client_count else _item("review_clients", False)
    items += staff_line + client_line

    return _page(f"""
<p class="subtitle">{_esc(_s(lang, 'review_subtitle'))}</p>
<div class="card">
  <div class="review-grid">
    {items}
  </div>
  <div class="btn-actions" style="margin-top:28px;">
    {_back_btn(lang, 18)}
    <a href="/setup/complete" class="btn btn-green">{_esc(_s(lang, 'btn_complete'))}</a>
  </div>
</div>
""", state, 18, lang, _s(lang, "review_title"))


def _render_complete(lang: str, state: dict) -> str:
    cfg = load_config()
    local_ip = get_local_ip()
    dash_url = f"http://{local_ip}:8787/"
    portal_url = f"http://{local_ip}:8788/"

    # Credential cards for staff
    temp_pws = state.get("temp_passwords", {})
    staff = _get_staff_list()
    cred_rows = ""
    for s in staff:
        pw = temp_pws.get(s["username"], "***")
        cred_rows += f'<tr><td>{_esc(s["display_name"])}</td><td><code>{_esc(s["username"])}</code></td><td><code>{_esc(pw)}</code></td></tr>'

    cred_card = ""
    if cred_rows:
        cred_card = f"""
    <div class="credentials-card" id="credCard">
      <h4>{_esc(_s(lang, 'complete_credentials'))}</h4>
      <table class="staff-table">
        <thead><tr><th>Name</th><th>Username</th><th>Password</th></tr></thead>
        <tbody>{cred_rows}</tbody>
      </table>
      <div style="margin-top:12px;text-align:center;">
        <button onclick="window.print()" class="btn btn-outline">{_esc(_s(lang, 'btn_print'))}</button>
      </div>
    </div>
"""

    return _page(f"""
<div class="card">
  <div class="complete-box">
    <div class="complete-icon">&#127881;</div>
    <h2>{_esc(_s(lang, 'complete_title'))}</h2>
    <p>{_esc(_s(lang, 'complete_subtitle'))}</p>

    <div style="text-align:left;max-width:520px;margin:20px auto;">
      <p style="font-weight:600;margin-bottom:6px;">{_esc(_s(lang, 'complete_dashboard'))}</p>
      <div class="url-box">
        <a href="{_esc(dash_url)}" target="_blank">{_esc(dash_url)}</a>
        <button class="copy-btn" onclick="navigator.clipboard.writeText('{_esc(dash_url)}')">{_esc(_s(lang, 'complete_copy'))}</button>
      </div>
      <p style="font-weight:600;margin-bottom:6px;margin-top:14px;">{_esc(_s(lang, 'complete_portal'))}</p>
      <div class="url-box">
        <a href="{_esc(portal_url)}" target="_blank">{_esc(portal_url)}</a>
        <button class="copy-btn" onclick="navigator.clipboard.writeText('{_esc(portal_url)}')">{_esc(_s(lang, 'complete_copy'))}</button>
      </div>
    </div>

    {cred_card}

    <div class="next-steps">
      <h3 style="margin-bottom:12px;">{_esc(_s(lang, 'complete_next_title'))}</h3>
      <ul style="padding-left:0;">
        <li>\u25A1 {_esc(_s(lang, 'complete_next_1'))}</li>
        <li>\u25A1 {_esc(_s(lang, 'complete_next_2'))}</li>
        <li>\u25A1 {_esc(_s(lang, 'complete_next_3'))}</li>
        <li>\u25A1 {_esc(_s(lang, 'complete_next_4'))}</li>
      </ul>
    </div>

    <div style="margin-top:24px;display:flex;flex-wrap:wrap;gap:12px;justify-content:center;">
      <a href="{_esc(dash_url)}" class="btn btn-green" target="_blank">{_esc(_s(lang, 'btn_open_dashboard'))}</a>
      <a href="/setup/access-pdf?lang={_esc(lang)}" class="btn btn-secondary">{_esc(_s(lang, 'download_access_pdf'))}</a>
    </div>
  </div>
</div>
""", state, 19, lang, _s(lang, "complete_title"))


def _render_already_complete(lang: str, state: dict) -> str:
    cfg = load_config()
    port = cfg.get("port", 8787)
    dash_url = f"http://127.0.0.1:{port}"
    return _page(f"""
<div class="card">
  <div class="already-box">
    <h2>{_esc(_s(lang, 'already_complete'))}</h2>
    <p>{_esc(_s(lang, 'already_complete_msg'))}</p>
    <a href="{_esc(dash_url)}" class="btn btn-primary">{_esc(_s(lang, 'btn_open_dashboard'))}</a>
  </div>
</div>
""", state, 19, lang, _s(lang, "already_complete"))


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class SetupWizardHandler(BaseHTTPRequestHandler):
    server_version = "LedgerLinkSetupWizard/2.0"

    def log_message(self, fmt: str, *args: object) -> None:  # type: ignore[override]
        pass

    def _get_lang(self) -> str:
        cookie_str = self.headers.get("Cookie", "")
        for part in cookie_str.split(";"):
            part = part.strip()
            if part.startswith("wizard_lang="):
                val = part[len("wizard_lang="):]
                if val in ("fr", "en"):
                    return val
        return "fr"

    def _send_html(self, body: str, status: int = 200) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json(self, data: dict, status: int = 200) -> None:
        encoded = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _redirect(self, location: str, extra_headers: list[tuple[str, str]] | None = None) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        if extra_headers:
            for k, v in extra_headers:
                self.send_header(k, v)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _parse_body(self) -> dict[str, str]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        parsed = urllib.parse.parse_qs(raw.decode("utf-8"), keep_blank_values=True)
        return {k: v[0] if v else "" for k, v in parsed.items()}

    def _mark_step(self, state: dict, step: int) -> None:
        steps_complete = set(state.get("steps_complete", []))
        steps_complete.add(step)
        state["steps_complete"] = list(steps_complete)
        save_state(state)

    def _first_incomplete_step(self, state: dict) -> int:
        steps_complete = set(state.get("steps_complete", []))
        for n in range(0, len(STEPS)):
            if n not in steps_complete:
                return n
        return len(STEPS) - 1

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------

    def do_GET(self) -> None:
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            lang = self._get_lang()
            state = load_state()

            # Language toggle
            if path == "/setup/lang":
                new_lang = qs.get("set", ["fr"])[0]
                if new_lang not in ("fr", "en"):
                    new_lang = "fr"
                from_path = qs.get("from", ["/"])[0]
                self._redirect(from_path, extra_headers=[
                    ("Set-Cookie", f"wizard_lang={new_lang}; Path=/; SameSite=Lax"),
                ])
                return

            # Root
            if path in ("/", ""):
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._redirect("/setup/welcome")
                return

            # Route each step
            renderers = {
                "/setup/welcome":       lambda: _render_welcome(lang, state),
                "/setup/firm":          lambda: _render_firm(lang, state),
                "/setup/admin":         lambda: _render_admin(lang, state),
                "/setup/license":       lambda: _render_license(lang, state),
                "/setup/ai":            lambda: _render_ai(lang, state),
                "/setup/email":         lambda: _render_email(lang, state),
                "/setup/portal":        lambda: _render_portal(lang, state),
                "/setup/whatsapp":      lambda: _render_whatsapp(lang, state),
                "/setup/telegram":      lambda: _render_telegram(lang, state),
                "/setup/microsoft365":  lambda: _render_m365(lang, state),
                "/setup/quickbooks":    lambda: _render_quickbooks(lang, state),
                "/setup/folder":        lambda: _render_folder(lang, state),
                "/setup/digest":        lambda: _render_digest(lang, state),
                "/setup/backup":        lambda: _render_backup(lang, state),
                "/setup/notifications": lambda: _render_notifications(lang, state),
                "/setup/security":      lambda: _render_security(lang, state),
                "/setup/staff":         lambda: _render_staff(lang, state),
                "/setup/clients":       lambda: _render_clients(lang, state),
                "/setup/review":        lambda: _render_review(lang, state),
            }

            if path in renderers:
                if state.get("setup_complete") and path != "/setup/welcome":
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._send_html(renderers[path]())
                return

            if path == "/setup/complete":
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(19)
                state["steps_complete"] = list(steps_complete)
                state["setup_complete"] = True
                save_state(state)
                self._send_html(_render_complete(lang, state))
                return

            if path == "/setup/access-pdf":
                pdf_lang = qs.get("lang", [lang])[0]
                if pdf_lang not in ("fr", "en"):
                    pdf_lang = "fr"
                try:
                    pdf_bytes = generate_access_instructions_pdf(pdf_lang)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/pdf")
                    self.send_header("Content-Disposition", 'attachment; filename="LedgerLink_Access_Instructions.pdf"')
                    self.send_header("Content-Length", str(len(pdf_bytes)))
                    self.end_headers()
                    self.wfile.write(pdf_bytes)
                except Exception as exc:
                    self._send_html(f"<h2>PDF Error</h2><pre>{_esc(str(exc))}</pre>", 500)
                return

            self._send_html("<h2>404 Not Found</h2><p><a href='/'>Home</a></p>", 404)

        except Exception:
            self._send_html(f"<h2>Server Error</h2><pre>{traceback.format_exc()}</pre>", 500)

    # ------------------------------------------------------------------
    # POST
    # ------------------------------------------------------------------

    def do_POST(self) -> None:
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            form = self._parse_body()
            lang = form.get("lang", self._get_lang())
            if lang not in ("fr", "en"):
                lang = "fr"
            state = load_state()

            # Block all POST submissions after setup is complete
            if state.get("setup_complete") and path not in ("/setup/lang",):
                self._send_html(_render_already_complete(lang, state))
                return

            # -- Step 1: Firm --
            if path == "/setup/firm":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                errors = validate_firm(form)
                if errors:
                    self._send_html(_render_firm(lang, state, error=" ".join(errors), data=form))
                    return
                cfg = load_config()
                cfg["firm"] = {
                    "firm_name": form.get("firm_name", "").strip(),
                    "firm_address": form.get("firm_address", "").strip(),
                    "firm_city": form.get("firm_city", "").strip(),
                    "firm_province": form.get("firm_province", "QC"),
                    "firm_postal": form.get("firm_postal", "").strip(),
                    "firm_phone": form.get("firm_phone", "").strip(),
                    "firm_website": form.get("firm_website", "").strip(),
                    "gst_number": form.get("gst_number", "").strip(),
                    "qst_number": form.get("qst_number", "").strip(),
                }
                local_ip = get_local_ip()
                cfg["network"] = {"local_ip": local_ip, "local_port": 8787, "bind_all_interfaces": True}
                cfg["host"] = "0.0.0.0"
                save_config(cfg)
                self._mark_step(state, 1)
                self._redirect("/setup/admin")
                return

            # -- Step 2: Admin --
            if path == "/setup/admin":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                errors = validate_admin(form)
                if errors:
                    self._send_html(_render_admin(lang, state, error=" ".join(errors), data=form))
                    return
                try:
                    upsert_owner_user(
                        email=form["admin_username"].strip(),
                        name=form["admin_fullname"].strip(),
                        password=form["admin_password"],
                        lang=lang,
                    )
                except Exception as exc:
                    self._send_html(_render_admin(lang, state, error=str(exc), data=form))
                    return
                state["admin_data"] = {
                    "admin_fullname": form.get("admin_fullname", "").strip(),
                    "admin_username": form.get("admin_username", "").strip(),
                    "admin_email": form.get("admin_email", "").strip(),
                }
                # Save admin email in firm config too
                cfg = load_config()
                cfg.setdefault("firm", {})["owner_email"] = form.get("admin_email", "").strip()
                save_config(cfg)
                self._mark_step(state, 2)
                self._redirect("/setup/license")
                return

            # -- Step 3: License validate (AJAX) --
            if path == "/setup/license/validate":
                if state.get("setup_complete"):
                    self._send_json({"ok": False, "message": "Setup already complete"})
                    return
                key = form.get("license_key", "").strip()
                if not key:
                    self._send_json({"ok": False, "message": "License key is required"})
                    return
                try:
                    from src.engines.license_engine import load_license, get_signing_secret, TIER_DEFAULTS
                    secret = get_signing_secret()
                    payload = load_license(key, secret)
                    tier = payload.get("tier", "--")
                    expiry = payload.get("expiry_date", "--")
                    max_c = payload.get("max_clients", TIER_DEFAULTS.get(tier, {}).get("max_clients", 0))
                    max_u = payload.get("max_users", TIER_DEFAULTS.get(tier, {}).get("max_users", 0))
                    msg = f"\u2713 {_s(lang, 'license_tier')}: {tier} | {_s(lang, 'license_expiry')}: {expiry} | {_s(lang, 'license_max_clients')}: {max_c} | {_s(lang, 'license_max_users')}: {max_u}"
                    self._send_json({"ok": True, "message": msg})
                except Exception as exc:
                    self._send_json({"ok": False, "message": str(exc)})
                return

            # -- Step 3: License save --
            if path == "/setup/license":
                key = form.get("license_key", "").strip()
                if key:
                    try:
                        from src.engines.license_engine import save_license_to_config, get_signing_secret
                        secret = get_signing_secret()
                        save_license_to_config(key, secret)
                    except Exception as exc:
                        self._send_html(_render_license(lang, state, error=str(exc)))
                        return
                self._mark_step(state, 3)
                self._redirect("/setup/ai")
                return

            # -- Step 4: AI --
            if path == "/setup/ai":
                cfg = load_config()
                cfg["ai_router"] = {
                    "routine_provider": {
                        "base_url": form.get("routine_url", "").strip(),
                        "api_key": form.get("routine_key", "").strip(),
                        "model": form.get("routine_model", "").strip(),
                    },
                    "premium_provider": {
                        "base_url": form.get("premium_url", "").strip(),
                        "api_key": form.get("premium_key", "").strip(),
                        "model": form.get("premium_model", "").strip(),
                    },
                }
                save_config(cfg)
                self._mark_step(state, 4)
                self._redirect("/setup/email")
                return

            # -- Step 4: AI test (AJAX) --
            if path == "/setup/ai/test":
                url = form.get("routine_url", "").strip()
                key = form.get("routine_key", "").strip()
                if not url:
                    self._send_json({"ok": False, "message": "API URL is required"})
                    return
                try:
                    import urllib.request
                    req = urllib.request.Request(
                        url.rstrip("/").replace("/chat/completions", "") + "/models",
                        headers={"Authorization": f"Bearer {key}"},
                    )
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        self._send_json({"ok": True, "message": f"Connected ({resp.status})"})
                except Exception as exc:
                    self._send_json({"ok": False, "message": f"Connection failed: {exc}"})
                return

            # -- Step 5: Email --
            if path == "/setup/email":
                cfg = load_config()
                try:
                    smtp_port = int(form.get("smtp_port", "587"))
                except ValueError:
                    smtp_port = 587
                cfg["email"] = {
                    "smtp_host": form.get("smtp_host", "").strip(),
                    "smtp_port": smtp_port,
                    "smtp_user": form.get("smtp_email", "").strip(),
                    "smtp_password": form.get("smtp_password", ""),
                    "from_address": form.get("smtp_email", "").strip(),
                    "from_name": form.get("smtp_display", "LedgerLink AI").strip(),
                    "enabled": True,
                }
                save_config(cfg)
                self._mark_step(state, 5)
                self._redirect("/setup/portal")
                return

            # -- Step 5: Email test --
            if path == "/setup/email/test":
                host = form.get("smtp_host", "").strip()
                if not host:
                    self._send_json({"ok": False, "message": "SMTP host is required"})
                    return
                self._send_json({"ok": True, "message": "Test email sent (simulated)"})
                return

            # -- Step 7: WhatsApp --
            if path == "/setup/whatsapp":
                cfg = load_config()
                cfg["whatsapp"] = {
                    "account_sid": form.get("whatsapp_sid", "").strip(),
                    "auth_token": form.get("whatsapp_token", "").strip(),
                    "number": form.get("whatsapp_number", "").strip(),
                    "enabled": form.get("whatsapp_enabled") == "1",
                }
                save_config(cfg)
                self._mark_step(state, 7)
                self._redirect("/setup/telegram")
                return

            # -- Step 8: Telegram --
            if path == "/setup/telegram":
                cfg = load_config()
                cfg["telegram"] = {
                    "bot_token": form.get("telegram_token", "").strip(),
                    "bot_name": form.get("telegram_name", "").strip(),
                    "enabled": form.get("telegram_enabled") == "1",
                }
                save_config(cfg)
                self._mark_step(state, 8)
                self._redirect("/setup/microsoft365")
                return

            # -- Step 9: Microsoft 365 --
            if path == "/setup/microsoft365":
                cfg = load_config()
                cfg["microsoft365"] = {
                    "service_email": form.get("m365_email", "").strip(),
                    "password": form.get("m365_password", ""),
                    "tenant_id": form.get("m365_tenant", "").strip(),
                    "enabled": form.get("m365_enabled") == "1",
                }
                save_config(cfg)
                self._mark_step(state, 9)
                self._redirect("/setup/quickbooks")
                return

            # -- Step 10: QuickBooks --
            if path == "/setup/quickbooks":
                cfg = load_config()
                cfg["quickbooks"] = {
                    "realm_id": form.get("qbo_realm", "").strip(),
                    "client_id": form.get("qbo_client_id", "").strip(),
                    "client_secret": form.get("qbo_client_secret", ""),
                    "enabled": form.get("qbo_enabled") == "1",
                }
                save_config(cfg)
                self._mark_step(state, 10)
                self._redirect("/setup/folder")
                return

            # -- Step 11: Folder watcher --
            if path == "/setup/folder":
                cfg = load_config()
                folder_path = form.get("folder_path", "").strip()
                cfg["folder_watcher"] = {
                    "inbox_path": folder_path,
                    "enabled": form.get("folder_enabled") == "1",
                }
                save_config(cfg)
                # Create the folder if it doesn't exist
                if folder_path:
                    try:
                        Path(folder_path).mkdir(parents=True, exist_ok=True)
                    except Exception:
                        pass
                self._mark_step(state, 11)
                self._redirect("/setup/digest")
                return

            # -- Step 12: Digest --
            if path == "/setup/digest":
                cfg = load_config()
                cfg["digest_config"] = {
                    "enabled": form.get("digest_enabled") == "1",
                    "send_time": form.get("digest_time", "07:00"),
                    "recipients": form.get("digest_recipients", "").strip(),
                    "language": form.get("digest_lang", "fr"),
                }
                save_config(cfg)
                self._mark_step(state, 12)
                self._redirect("/setup/backup")
                return

            # -- Step 13: Backup --
            if path == "/setup/backup":
                cfg = load_config()
                try:
                    keep = int(form.get("backup_keep", "30"))
                except ValueError:
                    keep = 30
                cfg["backup"] = {
                    "folder": form.get("backup_folder", "").strip(),
                    "frequency": form.get("backup_freq", "daily"),
                    "keep_count": keep,
                    "onedrive": form.get("backup_onedrive") == "1",
                }
                save_config(cfg)
                # Create backup folder
                bk_path = form.get("backup_folder", "").strip()
                if bk_path:
                    try:
                        Path(bk_path).mkdir(parents=True, exist_ok=True)
                    except Exception:
                        pass
                self._mark_step(state, 13)
                self._redirect("/setup/notifications")
                return

            # -- Step 14: Notifications --
            if path == "/setup/notifications":
                cfg = load_config()
                try:
                    pending_days = int(form.get("notif_pending_days", "3"))
                except ValueError:
                    pending_days = 3
                cfg["notifications"] = {
                    "notif_new_doc": form.get("notif_new_doc", "email"),
                    "notif_fraud": form.get("notif_fraud", "email"),
                    "notif_pending": form.get("notif_pending", "email"),
                    "pending_days": pending_days,
                    "notif_deadline": form.get("notif_deadline", "email"),
                    "notif_license": form.get("notif_license", "email"),
                    "notif_error": form.get("notif_error", "email"),
                }
                save_config(cfg)
                self._mark_step(state, 14)
                self._redirect("/setup/security")
                return

            # -- Step 15: Security --
            if path == "/setup/security":
                cfg = load_config()
                try:
                    max_attempts = int(form.get("max_login_attempts", "5"))
                except ValueError:
                    max_attempts = 5
                cfg["security_settings"] = {
                    "session_timeout": form.get("session_timeout", "4h"),
                    "max_login_attempts": max_attempts,
                    "lockout_duration": form.get("lockout_duration", "15m"),
                    "force_https": form.get("force_https") == "1",
                }
                save_config(cfg)
                self._mark_step(state, 15)
                self._redirect("/setup/staff")
                return

            # -- Step 16: Staff --
            if path == "/setup/staff":
                action = form.get("action", "next")
                if action == "add":
                    fullname = form.get("staff_fullname", "").strip()
                    username = form.get("staff_username", "").strip()
                    role = form.get("staff_role", "employee")
                    if not fullname or not username:
                        self._send_html(_render_staff(lang, state, error="Name and username are required."))
                        return
                    temp_pw = _gen_temp_password()
                    try:
                        _create_staff_user(username, fullname, temp_pw, role, lang)
                    except Exception as exc:
                        self._send_html(_render_staff(lang, state, error=str(exc)))
                        return
                    temp_pws = state.get("temp_passwords", {})
                    temp_pws[username] = temp_pw
                    state["temp_passwords"] = temp_pws
                    save_state(state)
                    self._send_html(_render_staff(lang, state, success=f"Added {fullname} ({username}) - Password: {temp_pw}"))
                    return
                # action == next
                self._mark_step(state, 16)
                self._redirect("/setup/clients")
                return

            # -- Step 17: Clients --
            if path == "/setup/clients":
                action = form.get("action", "next")
                if action == "add":
                    name = form.get("client_name", "").strip()
                    code = form.get("client_code", "").strip().upper()[:10]
                    email = form.get("client_email", "").strip()
                    client_lang = form.get("client_lang", "fr")
                    freq = form.get("client_freq", "quarterly")
                    accountant = form.get("client_accountant", "")
                    if not name:
                        self._send_html(_render_clients(lang, state, error="Client name is required."))
                        return
                    if not code:
                        code = name.upper().replace(" ", "")[:10]
                    try:
                        _create_client(code, name, email, client_lang, freq, accountant)
                    except Exception as exc:
                        self._send_html(_render_clients(lang, state, error=str(exc)))
                        return
                    clients = state.get("wizard_clients", [])
                    clients.append({"name": name, "code": code, "email": email, "freq": freq})
                    state["wizard_clients"] = clients
                    save_state(state)
                    self._send_html(_render_clients(lang, state, success=f"Added {name} ({code})"))
                    return
                # action == next
                self._mark_step(state, 17)
                self._redirect("/setup/review")
                return

            self._send_html("<h2>404</h2><p><a href='/'>Home</a></p>", 404)

        except Exception:
            self._send_html(f"<h2>Error</h2><pre>{traceback.format_exc()}</pre>", 500)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LedgerLink Setup Wizard")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    with socketserver.TCPServer((args.host, args.port), SetupWizardHandler) as httpd:
        httpd.allow_reuse_address = True
        print(f"Setup wizard running at http://{args.host}:{args.port}")
        print("Open your browser and navigate to the URL above.")
        httpd.serve_forever()
