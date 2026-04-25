"""CPA admin UI label translations for OtoCPA.

The JSON dictionaries (``fr.json`` / ``en.json``) hold translations used
inside templates via ``t("key", lang)``. This module covers the smaller
set of **inline button / link labels** that historically were hardcoded
English in ``scripts/review_dashboard.py``.

Keeping these separate from the JSON dicts:
* lets us import Python helpers (``ui_t``) without parsing JSON,
* keeps the JSON files scoped to template-level keys,
* makes a grep for "button label" converge on one file.

Usage at a call site::

    from src.i18n.ui_labels import ui_t
    f'<button>{ui_t("add_client", lang)}</button>'

Quebec-specific accounting terminology (TPS/TVQ, bilan, grand livre,
écritures de journal) is anchored here so the vocabulary stays
consistent across the product.
"""
from __future__ import annotations

from typing import Dict


# Canonical FR values are Québec French (not Parisian French). Keep
# them aligned with Ordre des CPA du Québec practice.
LABELS: Dict[str, Dict[str, str]] = {
    # Core verbs
    "add":              {"fr": "Ajouter",          "en": "Add"},
    "edit":             {"fr": "Modifier",         "en": "Edit"},
    "delete":           {"fr": "Supprimer",        "en": "Delete"},
    "save":             {"fr": "Enregistrer",      "en": "Save"},
    "save_changes":     {"fr": "Enregistrer",      "en": "Save changes"},
    "cancel":           {"fr": "Annuler",          "en": "Cancel"},
    "submit":           {"fr": "Soumettre",        "en": "Submit"},
    "approve":          {"fr": "Approuver",        "en": "Approve"},
    "reject":           {"fr": "Rejeter",          "en": "Reject"},
    "assign":           {"fr": "Attribuer",        "en": "Assign"},
    "review":           {"fr": "Examiner",         "en": "Review"},
    "remove":           {"fr": "Retirer",          "en": "Remove"},
    "filter":           {"fr": "Filtrer",          "en": "Filter"},
    "clear":            {"fr": "Effacer",          "en": "Clear"},
    "search":           {"fr": "Rechercher",       "en": "Search"},
    "view":             {"fr": "Voir",             "en": "View"},
    "print":            {"fr": "Imprimer",         "en": "Print"},
    "export":           {"fr": "Exporter",         "en": "Export"},
    "import":           {"fr": "Importer",         "en": "Import"},
    "sync":             {"fr": "Synchroniser",     "en": "Sync"},
    "refresh":          {"fr": "Actualiser",       "en": "Refresh"},
    "reset":            {"fr": "Réinitialiser",    "en": "Reset"},
    "reverse":          {"fr": "Contrepasser",     "en": "Reverse"},
    "download":         {"fr": "Télécharger",      "en": "Download"},
    "upload":           {"fr": "Téléverser",       "en": "Upload"},
    "connect":          {"fr": "Connecter",        "en": "Connect"},
    "disconnect":       {"fr": "Déconnecter",      "en": "Disconnect"},
    "back":             {"fr": "Retour",           "en": "Back"},
    "next":             {"fr": "Suivant",          "en": "Next"},
    "prev":             {"fr": "Précédent",        "en": "Prev"},
    "load":             {"fr": "Charger",          "en": "Load"},
    "open":             {"fr": "Ouvrir",           "en": "Open"},
    "close":            {"fr": "Fermer",           "en": "Close"},
    "create":           {"fr": "Créer",            "en": "Create"},
    "post":             {"fr": "Comptabiliser",    "en": "Post"},
    "match":            {"fr": "Apparier",         "en": "Match"},
    "allocate":         {"fr": "Allouer",          "en": "Allocate"},
    "estimate":         {"fr": "Estimer",          "en": "Estimate"},
    "rotate_token":     {"fr": "Renouveler le jeton", "en": "Rotate token"},
    "copy_link":        {"fr": "Copier le lien",   "en": "Copy link"},
    "send_by_email":    {"fr": "Envoyer par courriel", "en": "Send by email"},
    "send_message":     {"fr": "Envoyer le message", "en": "Send message"},
    "open_chat":        {"fr": "Ouvrir la conversation", "en": "Open chat"},

    # Client management
    "add_client":       {"fr": "Ajouter un client", "en": "Add client"},
    "new_client":       {"fr": "Nouveau client",    "en": "New client"},

    # Team management
    "invite_team_member": {"fr": "Inviter un collègue", "en": "Invite team member"},
    "add_partner":      {"fr": "Ajouter un associé", "en": "Add partner"},
    "remove_partner":   {"fr": "Retirer l'associé",  "en": "Remove partner"},

    # Auth
    "enable_2fa":       {"fr": "Activer la 2FA",   "en": "Enable 2FA"},
    "disable_2fa":      {"fr": "Désactiver la 2FA", "en": "Disable 2FA"},
    "logout":           {"fr": "Déconnexion",      "en": "Logout"},
    "sign_in":          {"fr": "Connexion",        "en": "Sign in"},

    # Navigation / sections
    "settings":         {"fr": "Paramètres",       "en": "Settings"},
    "users":            {"fr": "Utilisateurs",     "en": "Users"},
    "reports":          {"fr": "Rapports",         "en": "Reports"},
    "clients":          {"fr": "Clients",          "en": "Clients"},
    "dashboard":        {"fr": "Tableau de bord",  "en": "Dashboard"},
    "profile":          {"fr": "Profil",           "en": "Profile"},
    "billing":          {"fr": "Facturation",      "en": "Billing"},
    "diagnostics":      {"fr": "Diagnostics",      "en": "Diagnostics"},
    "back_to_diagnostics": {"fr": "Retour aux diagnostics", "en": "Back to Diagnostics"},

    # Close / period
    "close_period":     {"fr": "Fermer la période", "en": "Close period"},
    "fiscal_year":      {"fr": "Exercice financier", "en": "Fiscal year"},

    # Banking
    "connect_bank":     {"fr": "Connecter le compte bancaire", "en": "Connect Bank Account"},
    "connect_quickbooks": {"fr": "Connecter QuickBooks", "en": "Connect QuickBooks"},
    "sync_now":         {"fr": "Synchroniser maintenant", "en": "Sync now"},
    "bank_statement":   {"fr": "Relevé bancaire",  "en": "Bank statement"},
    "reconciliation":   {"fr": "Rapprochement",    "en": "Reconciliation"},

    # Accounting terminology (Quebec French)
    "gst":              {"fr": "TPS",               "en": "GST"},
    "qst":              {"fr": "TVQ",               "en": "QST"},
    "hst":              {"fr": "TVH",               "en": "HST"},
    "corp_tax":         {"fr": "Impôt corporatif",  "en": "Corp tax"},
    "period_close":     {"fr": "Fermeture de période", "en": "Period close"},
    "general_ledger":   {"fr": "Grand livre",       "en": "General ledger"},
    "chart_of_accounts": {"fr": "Plan comptable",   "en": "Chart of accounts"},
    "journal_entry":    {"fr": "Écriture de journal", "en": "Journal entry"},
    "post_journal_entry": {"fr": "Enregistrer l'écriture", "en": "Post journal entry"},
    "manual_je":        {"fr": "Écriture manuelle", "en": "Manual journal entry"},

    # Financial statements (Quebec French terminology)
    "financial_statements": {"fr": "États financiers", "en": "Financial statements"},
    "trial_balance":    {"fr": "Balance de vérification", "en": "Trial balance"},
    "income_statement": {"fr": "État des résultats", "en": "Income statement"},
    "balance_sheet":    {"fr": "Bilan",              "en": "Balance sheet"},
    "cash_flow":        {"fr": "Flux de trésorerie", "en": "Cash flow"},
    "soce":             {"fr": "État des variations des capitaux propres",
                         "en": "Statement of changes in equity"},
    "tb_col_debit":     {"fr": "Débit",              "en": "Debit"},
    "tb_col_credit":    {"fr": "Crédit",             "en": "Credit"},
    "tb_col_account":   {"fr": "Compte",             "en": "Account"},
    "tb_total_debits":  {"fr": "Total des débits",   "en": "Total debits"},
    "tb_total_credits": {"fr": "Total des crédits",  "en": "Total credits"},
    "pl_revenue":       {"fr": "Produits",           "en": "Revenue"},
    "pl_expenses":      {"fr": "Charges",            "en": "Expenses"},
    "pl_gross_profit":  {"fr": "Bénéfice brut",      "en": "Gross profit"},
    "pl_operating_income": {"fr": "Bénéfice d'exploitation", "en": "Operating income"},
    "pl_net_income":    {"fr": "Bénéfice net",       "en": "Net income"},
    "pl_period_ending": {"fr": "Pour la période terminée le",
                         "en": "For the period ending"},
    "bs_assets":        {"fr": "Actif",              "en": "Assets"},
    "bs_current_assets":{"fr": "Actif à court terme", "en": "Current assets"},
    "bs_fixed_assets":  {"fr": "Immobilisations",    "en": "Fixed assets"},
    "bs_liabilities":   {"fr": "Passif",             "en": "Liabilities"},
    "bs_current_liabilities": {"fr": "Passif à court terme", "en": "Current liabilities"},
    "bs_long_term_liabilities": {"fr": "Passif à long terme",
                                  "en": "Long-term liabilities"},
    "bs_equity":        {"fr": "Capitaux propres",   "en": "Equity"},
    "bs_retained_earnings": {"fr": "Bénéfices non répartis", "en": "Retained earnings"},
    "cf_operating":     {"fr": "Activités d'exploitation", "en": "Operating activities"},
    "cf_investing":     {"fr": "Activités d'investissement", "en": "Investing activities"},
    "cf_financing":     {"fr": "Activités de financement", "en": "Financing activities"},
    "cf_net_change":    {"fr": "Augmentation/Diminution nette de la trésorerie",
                         "en": "Net increase/decrease in cash"},
    "soce_opening":     {"fr": "Solde d'ouverture",  "en": "Opening balance"},
    "soce_closing":     {"fr": "Solde de clôture",   "en": "Closing balance"},
    "soce_dividends":   {"fr": "Dividendes déclarés", "en": "Dividends declared"},
    "soce_net_income":  {"fr": "Bénéfice net de la période", "en": "Net income for period"},

    # Statement PDFs
    "download_t661":    {"fr": "Télécharger le PDF T661", "en": "Download T661 PDF"},
    "soce_pdf":         {"fr": "PDF – État des variations", "en": "SOCE PDF"},
    "t5013_pdf":        {"fr": "PDF T5013",           "en": "T5013 PDF"},
    "management_letter": {"fr": "Lettre à la direction (NCA 265)",
                          "en": "Management letter (CAS 265)"},
    "pdf_cas_580":      {"fr": "PDF (NCA 580)",       "en": "PDF (CAS 580)"},

    # Detectors / audit
    "run_all_detectors": {"fr": "Exécuter tous les détecteurs", "en": "Run all detectors"},
    "calculate_sample": {"fr": "Calculer et échantillonner", "en": "Calculate & sample"},
    "save_narrative":   {"fr": "Enregistrer la narration", "en": "Save narrative"},
    "clear_cache":      {"fr": "Vider le cache",       "en": "Clear cache"},
    "more_filters":     {"fr": "Plus de filtres",      "en": "More filters"},

    # Client portal — single-language strings (rendered using the client's
    # configured language, not bilingual).
    "portal_upload_heading":       {"fr": "Envoyer un document",         "en": "Upload a document"},
    "portal_drop_files":           {"fr": "Glissez vos fichiers",        "en": "Drop files here"},
    "portal_note_optional":        {"fr": "Note (facultatif)",           "en": "Note (optional)"},
    "portal_note_placeholder":     {"fr": "Ex : facture d'épicerie",     "en": "Ex: grocery invoice"},
    "portal_send_upload":          {"fr": "Envoyer",                     "en": "Upload"},
    "portal_uploading":            {"fr": "Téléversement…",              "en": "Uploading…"},
    "portal_files_selected":       {"fr": "fichier(s) sélectionné(s)",   "en": "file(s) selected"},
    "portal_format_hint":          {"fr": "Formats : PDF, JPG, PNG. 20 Mo maximum par fichier.",
                                    "en": "Formats: PDF, JPG, PNG. 20 MB max per file."},
    "portal_processing_status":    {"fr": "Traitement en cours…",        "en": "Processing…"},
    "portal_done_word":            {"fr": "terminé",                     "en": "done"},
    "portal_processing_word":      {"fr": "en traitement",               "en": "processing"},
    "portal_queued_word":          {"fr": "en file",                     "en": "queued"},
    "portal_documents_uploaded":   {"fr": "document(s) téléversé(s)",    "en": "document(s) uploaded"},
    "portal_failed_word":          {"fr": "échec(s)",                    "en": "failed"},
    "portal_upload_failed":        {"fr": "Échec du téléversement",      "en": "Upload failed"},
    "portal_nothing_queued":       {"fr": "Rien à traiter",              "en": "Nothing queued"},
    "portal_queued_for_processing":{"fr": "document(s) en file. Traitement…",
                                    "en": "document(s) queued. Processing…"},

    "portal_my_documents":         {"fr": "Mes documents",               "en": "My documents"},
    "portal_no_documents_yet":     {"fr": "Aucun document encore soumis.",
                                    "en": "No documents submitted yet."},
    "portal_col_file":             {"fr": "Fichier",                     "en": "File"},
    "portal_col_date":             {"fr": "Date",                        "en": "Date"},
    "portal_col_vendor":           {"fr": "Fournisseur",                 "en": "Vendor"},
    "portal_col_amount":           {"fr": "Montant",                     "en": "Amount"},
    "portal_col_status":           {"fr": "Statut",                      "en": "Status"},

    "portal_status_new":           {"fr": "Reçu",                        "en": "New"},
    "portal_status_in_review":     {"fr": "En révision",                 "en": "In review"},
    "portal_status_posted":        {"fr": "Validé",                      "en": "Posted"},
    "portal_status_ready":         {"fr": "Prêt",                        "en": "Ready"},

    "portal_bank_account":         {"fr": "Compte bancaire",             "en": "Bank account"},
    "portal_bank_connected_note":  {"fr": "Connecté via Plaid. Votre CPA voit les transactions, mais pas vos identifiants bancaires.",
                                    "en": "Connected via Plaid. Your CPA sees transactions but never your banking credentials."},
    "portal_connect_bank_heading": {"fr": "Connectez votre banque",      "en": "Connect your bank"},
    "portal_connect_bank_body":    {"fr": "Connectez votre compte bancaire de façon sécurisée via Plaid. Votre CPA peut lire les transactions sans voir vos mots de passe.",
                                    "en": "Securely link your bank account through Plaid. Your CPA can read transactions without ever seeing your passwords."},
    "portal_connect_bank_button":  {"fr": "Connecter la banque",         "en": "Connect bank"},
    "portal_last_sync":            {"fr": "Dernière synchro",            "en": "Last sync"},
    "portal_never":                {"fr": "jamais",                      "en": "never"},
    "portal_loading":              {"fr": "Chargement…",                 "en": "Loading…"},
    "portal_plaid_not_configured": {"fr": "Plaid n'est pas configuré",   "en": "Plaid not configured"},
    "portal_connection_failed":    {"fr": "Échec de la connexion",       "en": "Connection failed"},

    "portal_no_messages":          {"fr": "Aucun message. Envoyez un message à votre CPA ci-dessous.",
                                    "en": "No messages yet. Send one to your CPA below."},
    "portal_message_placeholder":  {"fr": "Écrivez à votre CPA…",        "en": "Write to your CPA…"},
    "portal_send_message":         {"fr": "Envoyer",                     "en": "Send"},
    "portal_empty_message_error":  {"fr": "Message vide",                "en": "Empty message"},
    "portal_no_file_uploaded_error":{"fr": "Aucun fichier téléversé",    "en": "No file uploaded"},
    "portal_no_file_selected_error":{"fr": "Aucun fichier sélectionné",  "en": "No file selected"},
    "portal_upload_flash":         {"fr": "document(s) en file de traitement",
                                    "en": "document(s) queued for processing"},
    "portal_upload_flash_failed":  {"fr": "échec(s) lors de l'ajout en file",
                                    "en": "failed to queue"},

    "portal_link_rotated_title":   {"fr": "Lien renouvelé",              "en": "Link rotated"},
    "portal_link_rotated_body":    {"fr": "Votre ancien lien cesse immédiatement de fonctionner.",
                                    "en": "Your old link stops working immediately."},
    "portal_continue":             {"fr": "Continuer",                   "en": "Continue"},

    "portal_offline_title":        {"fr": "Hors ligne",                  "en": "Offline"},
    "portal_offline_body":         {"fr": "Vous semblez être hors ligne. Reconnectez-vous et réessayez.",
                                    "en": "You appear to be offline. Reconnect and try again."},
    "portal_retry":                {"fr": "Réessayer",                   "en": "Retry"},
    "portal_text_label":           {"fr": "Texter",                      "en": "Text"},

    # Document detail (CPA-side, but we want the "uploaded by" caption to
    # render in the active dashboard lang only).
    "doc_uploaded_by":             {"fr": "Téléversé par",               "en": "Uploaded by"},
    "doc_gl_tax_heading":          {"fr": "GL / Taxe",                   "en": "GL / Tax"},
    "doc_gl_tax_line_items_note":  {"fr": "Définis par les postes",      "en": "Defined by line items"},
    "doc_gl_tax_edit_note":        {"fr": "GL et taxe définis par les postes",
                                    "en": "GL and tax defined by line items"},
    "doc_lines_count_summary":     {"fr": "Ce document contient {n} poste(s) répartis sur {m} compte(s) GL.",
                                    "en": "This document has {n} line(s) across {m} GL account(s)."},
    "doc_multiple_categories":     {"fr": "Plusieurs",                   "en": "Multiple"},
}


def ui_t(key: str, lang: str = "fr") -> str:
    """Return the translated label for *key* in *lang*.

    Falls back to EN when the FR value is missing, then to the key
    itself. Same fallback semantics as :func:`src.i18n.t`.
    """
    entry = LABELS.get(key)
    if not entry:
        return key
    if lang == "fr":
        return entry.get("fr") or entry.get("en") or key
    return entry.get("en") or entry.get("fr") or key


def bilingual(key: str) -> str:
    """Return ``"FR / EN"`` for places that display both languages.

    Used on shared surfaces (client portal landing pages, toggle
    switches) where a bilingual label is friendlier than picking one.
    """
    entry = LABELS.get(key)
    if not entry:
        return key
    fr = entry.get("fr") or key
    en = entry.get("en") or key
    if fr == en:
        return fr
    return f"{fr} / {en}"
