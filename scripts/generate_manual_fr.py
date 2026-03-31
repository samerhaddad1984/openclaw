#!/usr/bin/env python3
"""Générer le Manuel d'utilisation LedgerLink (Français) en PDF avec ReportLab."""

import os
import sys
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.units import inch, cm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable,
)

# ── Palette de couleurs ──────────────────────────────────────
BLUE = HexColor("#1F3864")
LIGHT_BLUE = HexColor("#D6E4F0")
AMBER = HexColor("#FFF3CD")
AMBER_BORDER = HexColor("#856404")
TEAL = HexColor("#D1ECF1")
TEAL_BORDER = HexColor("#0C5460")
GREY = HexColor("#F2F2F2")
DARK_GREY = HexColor("#333333")
WHITE = white

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "docs"
OUT_PATH = OUT_DIR / "LedgerLink_Manuel_Utilisateur_FR.pdf"


# ── Styles personnalisés ─────────────────────────────────────
def get_styles():
    """Retourne un dictionnaire de ParagraphStyles personnalisés."""
    ss = getSampleStyleSheet()
    styles = {}
    styles["Title"] = ParagraphStyle(
        "ManualTitle", parent=ss["Title"],
        fontSize=28, leading=34, textColor=BLUE,
        spaceAfter=6, alignment=TA_CENTER,
    )
    styles["Subtitle"] = ParagraphStyle(
        "ManualSubtitle", parent=ss["Normal"],
        fontSize=14, leading=18, textColor=DARK_GREY,
        spaceAfter=24, alignment=TA_CENTER,
    )
    styles["H1"] = ParagraphStyle(
        "ManualH1", parent=ss["Heading1"],
        fontSize=20, leading=26, textColor=BLUE,
        spaceBefore=24, spaceAfter=10,
    )
    styles["H2"] = ParagraphStyle(
        "ManualH2", parent=ss["Heading2"],
        fontSize=15, leading=20, textColor=BLUE,
        spaceBefore=16, spaceAfter=8,
    )
    styles["H3"] = ParagraphStyle(
        "ManualH3", parent=ss["Heading3"],
        fontSize=12, leading=16, textColor=BLUE,
        spaceBefore=10, spaceAfter=6,
    )
    styles["Body"] = ParagraphStyle(
        "ManualBody", parent=ss["Normal"],
        fontSize=10, leading=14, textColor=DARK_GREY,
        spaceAfter=6, alignment=TA_JUSTIFY,
    )
    styles["BodyBold"] = ParagraphStyle(
        "ManualBodyBold", parent=styles["Body"],
        fontName="Helvetica-Bold",
    )
    styles["Bullet"] = ParagraphStyle(
        "ManualBullet", parent=styles["Body"],
        leftIndent=24, bulletIndent=12,
        spaceBefore=2, spaceAfter=2,
    )
    styles["Code"] = ParagraphStyle(
        "ManualCode", parent=ss["Code"],
        fontSize=9, leading=12, backColor=GREY,
        leftIndent=12, rightIndent=12,
        spaceBefore=4, spaceAfter=4,
    )
    styles["Footer"] = ParagraphStyle(
        "ManualFooter", parent=ss["Normal"],
        fontSize=8, textColor=DARK_GREY, alignment=TA_CENTER,
    )
    return styles


# ── Utilitaires ──────────────────────────────────────────────
def warning_box(text, styles):
    """Boîte d'avertissement ambre."""
    content = Paragraph(f"<b>Avertissement :</b> {text}", styles["Body"])
    t = Table([[content]], colWidths=[6.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), AMBER),
        ("BOX", (0, 0), (-1, -1), 1, AMBER_BORDER),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
    ]))
    return t


def tip_box(text, styles):
    """Boîte de conseil sarcelle."""
    content = Paragraph(f"<b>Conseil :</b> {text}", styles["Body"])
    t = Table([[content]], colWidths=[6.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), TEAL),
        ("BOX", (0, 0), (-1, -1), 1, TEAL_BORDER),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
    ]))
    return t


def bullet_list(items, styles):
    """Retourne une liste de paragraphes à puces."""
    return [Paragraph(f"\u2022 {item}", styles["Bullet"]) for item in items]


def make_table(headers, rows, col_widths=None):
    """Tableau de données stylisé avec en-tête bleu."""
    data = [headers] + rows
    if col_widths is None:
        col_widths = [6.5 * inch / len(headers)] * len(headers)
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("BACKGROUND", (0, 1), (-1, -1), WHITE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY]),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#CCCCCC")),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def numbered_list(items, styles):
    """Retourne une liste numérotée de paragraphes."""
    return [Paragraph(f"<b>{i}.</b> {item}", styles["Bullet"])
            for i, item in enumerate(items, 1)]


def sp():
    return Spacer(1, 12)


# ═══════════════════════════════════════════════════════════════
#  CONSTRUCTEURS DE SECTIONS
# ═══════════════════════════════════════════════════════════════

def build_cover_page(story, styles):
    """Page couverture avec titre, sous-titre et version."""
    story.append(Spacer(1, 2.5 * inch))
    story.append(Paragraph("LedgerLink AI", styles["Title"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Manuel d'utilisation", ParagraphStyle(
        "CoverManual", parent=styles["Title"], fontSize=22, leading=28,
    )))
    story.append(Spacer(1, 0.5 * inch))
    story.append(HRFlowable(width="60%", thickness=2, color=BLUE))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph(
        "Automatisation comptable intelligente pour les cabinets CPA canadiens",
        styles["Subtitle"],
    ))
    story.append(Paragraph("Version 1.0 &mdash; 2026", ParagraphStyle(
        "CoverVer", parent=styles["Subtitle"], fontSize=11,
    )))
    story.append(Spacer(1, 1.5 * inch))
    story.append(Paragraph(
        "Conforme au Quebec &bull; TPS/TVQ &bull; Mission de verification CPA &bull; "
        "Bilingue FR/EN &bull; Propulse par l'IA",
        ParagraphStyle("CoverTags", parent=styles["Body"],
                       alignment=TA_CENTER, fontSize=10, textColor=BLUE),
    ))
    story.append(PageBreak())


def build_toc(story, styles):
    """Page de la table des matieres."""
    story.append(Paragraph("Table des matieres", styles["H1"]))
    story.append(sp())
    toc_entries = [
        ("1", "Introduction", "3"),
        ("2", "Installation", "7"),
        ("3", "Gestion des utilisateurs", "14"),
        ("4", "Flux de travail quotidien", "18"),
        ("5", "Fiscalite quebecoise", "28"),
        ("6", "Portail client", "38"),
        ("7", "Fermeture de periode et facturation", "42"),
        ("8", "Module de mission CPA", "46"),
        ("9", "Administration", "55"),
        ("10", "Depannage", "59"),
        ("11", "Glossaire", "63"),
    ]
    toc_style = ParagraphStyle(
        "TOCLine", parent=styles["Body"],
        fontSize=11, leading=22, leftIndent=10,
    )
    for num, title, page in toc_entries:
        dots = " " + "." * (60 - len(title) - len(num))
        story.append(Paragraph(
            f"<b>{num}.</b>&nbsp;&nbsp;{title}"
            f'<font color="#999999">{dots}</font> {page}',
            toc_style,
        ))
    story.append(PageBreak())


# ── Section 1 : Introduction ─────────────────────────────────
def build_section_1_introduction(story, styles):
    """Section 1 — Presentation de LedgerLink, architecture, configuration."""
    story.append(Paragraph("1. Introduction", styles["H1"]))
    story.append(sp())

    story.append(Paragraph("1.1 Qu'est-ce que LedgerLink AI?", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI est une plateforme d'automatisation comptable intelligente "
        "concue pour les cabinets CPA canadiens, avec une specialisation "
        "approfondie en fiscalite quebecoise (TPS/TVQ), un fonctionnement "
        "bilingue francais/anglais et un soutien complet aux missions de "
        "verification selon les NCA. La plateforme automatise la reception des "
        "documents, l'extraction par IA, la detection de fraude, le "
        "rapprochement bancaire, la production fiscale et les flux de travail "
        "de mission CPA &mdash; reduisant l'effort de tenue de livres manuel "
        "de pres de 80 %.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("1.2 Capacites principales", styles["H2"]))
    story.extend(bullet_list([
        "Reception de documents multicanal : WhatsApp, Telegram, courriel, "
        "surveillance de dossier, portail client, telechargement manuel",
        "Extraction OCR alimentee par l'IA avec prise en charge de l'ecriture manuscrite",
        "Moteur deterministe de detection de fraude a 13 regles",
        "Classification de substance economique (immobilisations, charges payees "
        "d'avance, emprunts, depenses personnelles, remises fiscales)",
        "Moteur fiscal complet TPS/TVQ/TVH avec suivi des CTI/RTI",
        "Importation de releves bancaires avec appariement intelligent par similarite",
        "Integration QuickBooks en ligne pour l'inscription des ecritures",
        "Module de mission CPA couvrant les NCA 315, 320, 330, 500, 530, 550, "
        "560, 570, 580, 700 et NCCQ 1",
        "Preremplissage FPZ-500 de Revenu Quebec et methode rapide",
        "Validation complete de la paie : RRQ, RQAP, AE, FSS, CNESST, RL-1/T4",
        "Interface bilingue (francais et anglais) avec bascule en un clic",
        "Portail client avec tunnel Cloudflare pour l'acces a distance securise",
        "Controle d'acces par roles : Proprietaire, Gestionnaire, Employe, Client",
    ], styles))
    story.append(sp())

    _build_section_1_architecture(story, styles)
    _build_section_1_requirements(story, styles)
    _build_section_1_ai_providers(story, styles)
    story.append(PageBreak())


def _build_section_1_architecture(story, styles):
    """Sous-section : architecture a 3 couches."""
    story.append(Paragraph("1.3 Architecture a trois couches", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink utilise un pipeline de traitement a trois couches pour "
        "maximiser la precision tout en minimisant les couts d'IA :",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Couche 1\nDeterministe", "Moteur fiscal, moteur de fraude,\n"
         "moteur de substance, validateur\nde paie, analyseur bancaire",
         "Aucun cout IA, instantane,\n100 % reproductible"],
        ["Couche 2\nIA courante", "Classification des documents,\nextraction "
         "du fournisseur,\nsuggestion de compte du grand livre",
         "Fournisseur IA economique\n(ex. DeepSeek)"],
        ["Couche 3\nIA premium", "Explication des anomalies complexes,\n"
         "narratif de conformite,\ndossiers de travail",
         "Fournisseur IA premium\n(ex. Claude)"],
    ]
    story.append(make_table(
        ["Couche", "Fonctions", "Profil de cout"],
        rows,
        col_widths=[1.3 * inch, 2.8 * inch, 2.4 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "La couche 1 traite plus de 70 % des operations sans aucun appel IA. "
        "Cela maintient les couts d'exploitation bas tout en assurant une "
        "precision deterministe pour les calculs fiscaux et la detection de fraude.",
        styles,
    ))
    story.append(sp())


def _build_section_1_requirements(story, styles):
    """Sous-section : configuration requise."""
    story.append(Paragraph("1.4 Configuration requise", styles["H2"]))
    rows = [
        ["Systeme d'exploitation", "Windows 10/11 (64 bits) ou macOS 12+"],
        ["Python", "3.11 ou superieur"],
        ["Memoire vive", "4 Go minimum, 8 Go recommande"],
        ["Espace disque", "500 Mo pour l'application + croissance de la base de donnees"],
        ["Navigateur", "Chrome, Edge, Firefox ou Safari (version courante)"],
        ["Reseau", "Internet pour les fournisseurs IA et le tunnel Cloudflare"],
        ["Courriel (optionnel)", "Compte SMTP (Gmail, Outlook ou personnalise)"],
    ]
    story.append(make_table(
        ["Composant", "Exigence"],
        rows,
        col_widths=[2.0 * inch, 4.5 * inch],
    ))
    story.append(sp())


def _build_section_1_ai_providers(story, styles):
    """Sous-section : fournisseurs IA."""
    story.append(Paragraph("1.5 Fournisseurs IA", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink utilise deux niveaux de fournisseurs IA configures lors "
        "de l'installation :",
        styles["Body"],
    ))
    rows = [
        ["Standard (Courant)", "DeepSeek", "Classification des documents,\n"
         "extraction du fournisseur,\nsuggestion de compte du grand livre"],
        ["Premium (Complexe)", "Claude via OpenRouter", "Explication des anomalies,\n"
         "narratif de conformite,\ngeneration des dossiers de travail,\n"
         "classification de substance"],
    ]
    story.append(make_table(
        ["Niveau", "Fournisseur recommande", "Taches"],
        rows,
        col_widths=[1.5 * inch, 1.8 * inch, 3.2 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Les deux fournisseurs utilisent le format d'API compatible OpenAI. "
        "Vous pouvez substituer tout fournisseur prenant en charge ce format, "
        "y compris des modeles heberges localement. Les cles API sont "
        "configurees lors de l'assistant de configuration (etape 4) et "
        "stockees chiffrees dans le fichier de configuration.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(warning_box(
        "L'extraction par IA n'est pas precise a 100 %. Tous les documents "
        "traites par l'IA passent par la file de revision ou un reviseur "
        "humain peut verifier et corriger toute erreur avant l'inscription "
        "dans QuickBooks.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("1.6 Formats de documents pris en charge", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink traite les documents dans plusieurs formats via son moteur "
        "OCR. La detection du format utilise les octets magiques (signature "
        "du fichier), et non l'extension, pour une identification fiable.",
        styles["Body"],
    ))
    story.append(sp())
    fmt_rows = [
        ["PDF", "Extraction de texte natif via pdfplumber.\n"
         "Bascule vers l'IA visuelle si texte &lt; 20 mots.", "Format principal"],
        ["JPEG / JPG", "Extraction par IA visuelle.\nEcriture manuscrite auto-detectee.",
         "Courant pour les photos"],
        ["PNG", "Extraction par IA visuelle.\nPrise en charge de la transparence.",
         "Captures d'ecran, numerisations"],
        ["TIFF", "Extraction par IA visuelle.\nMulti-pages pris en charge.",
         "Numeriseurs professionnels"],
        ["WebP", "Extraction par IA visuelle.\nFormat Web compact.",
         "Telechargements Web"],
        ["HEIC", "Converti automatiquement en JPEG (qualite 92)\n"
         "puis traite par IA visuelle.", "Photos iPhone"],
    ]
    story.append(make_table(
        ["Format", "Methode de traitement", "Source typique"],
        fmt_rows,
        col_widths=[1.2 * inch, 3.3 * inch, 2.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("1.7 Prise en charge de l'ecriture manuscrite", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink detecte automatiquement les documents manuscrits a l'aide "
        "de plusieurs heuristiques et les achemine vers un modele IA specialise "
        "pour une precision amelioree :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Extraction de texte &lt; 10 mots par pdfplumber ajoute +0,4 au "
        "score de probabilite d'ecriture manuscrite",
        "Variance des pixels de l'image &gt; 2000 ajoute +0,2 (patron de variation d'encre)",
        "Longueur moyenne des mots &lt; 4,0 ajoute +0,1 (abreviations courantes "
        "en ecriture manuscrite)",
        "Score &gt; 0,5 declenche le modele de recu manuscrit",
        "Seuil de confiance pour les documents manuscrits : 0,70 minimum",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Pour de meilleurs resultats avec les recus manuscrits, photographiez "
        "le document sous un bon eclairage avec le texte occupant la majeure "
        "partie du cadre. Evitez les ombres et les angles obliques.",
        styles,
    ))

    story.append(sp())
    story.append(Paragraph("1.8 Architecture de securite", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink met en oeuvre plusieurs couches de securite pour proteger "
        "les donnees financieres sensibles :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Authentification :</b> Basee sur les sessions avec hachage bcrypt "
        "des mots de passe (12 tours). Les sessions expirent apres 12 heures "
        "(configurable).",
        "<b>Limitation du debit :</b> 5 tentatives de connexion echouees par "
        "15 minutes par adresse IP declenchent un blocage HTTP 429.",
        "<b>Acces par roles :</b> Quatre roles (Proprietaire/Gestionnaire/"
        "Employe/Client) avec des permissions hierarchiques appliquees sur "
        "chaque route.",
        "<b>Journalisation d'audit :</b> Toutes les derogations de fraude, "
        "approbations d'inscription et actions administratives sont "
        "journalisees de facon permanente.",
        "<b>Chiffrement des cles API :</b> Les cles API des fournisseurs sont "
        "stockees chiffrees dans le fichier de configuration.",
        "<b>Temoins securises :</b> Le drapeau HttpOnly est active sur les "
        "jetons de session. Le drapeau Secure est active automatiquement "
        "lorsque HTTPS est detecte.",
        "<b>Integrite de la base de donnees :</b> Des declencheurs SQLite "
        "assurent l'immutabilite des dossiers de travail approuves et des "
        "rapprochements finalises.",
        "<b>Verrouillage optimiste :</b> La verification de version empeche "
        "les approbations obsoletes (piege 6).",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "LedgerLink stocke toutes les donnees localement sur votre serveur. "
        "Aucune donnee client n'est envoyee aux serveurs de LedgerLink. Les "
        "fournisseurs IA ne recoivent que le contenu du document necessaire "
        "a l'extraction &mdash; jamais les noms des clients, numeros de compte "
        "ou autres informations d'identification au-dela de ce qui apparait "
        "sur le document lui-meme.",
        styles["Body"],
    ))


# ── Section 2 : Installation ─────────────────────────────────
def build_section_2_installation(story, styles):
    """Section 2 — Installation et assistant de configuration."""
    story.append(Paragraph("2. Installation", styles["H1"]))
    story.append(sp())

    _build_section_2_install_bat(story, styles)
    _build_section_2_wizard(story, styles)
    _build_section_2_first_login(story, styles)
    _build_section_2_config(story, styles)
    story.append(PageBreak())


def _build_section_2_install_bat(story, styles):
    """Guide INSTALL.bat."""
    story.append(Paragraph("2.1 Installation Windows (INSTALL.bat)", styles["H2"]))
    story.append(Paragraph(
        "Le script INSTALL.bat automatise l'ensemble du processus d'installation. "
        "Double-cliquez sur le fichier et accordez l'acces administrateur "
        "lorsque demande.",
        styles["Body"],
    ))
    story.append(sp())
    story.extend(numbered_list([
        "<b>Elevation administrateur</b> &mdash; Demande les privileges "
        "d'administrateur pour installer le service Windows.",
        "<b>Verification Python</b> &mdash; Verifie que Python 3.11+ est "
        "installe. Si absent, telecharge et installe automatiquement "
        "Python 3.11.9.",
        "<b>Installation des dependances</b> &mdash; Execute "
        "<font face='Courier'>pip install -r requirements.txt</font> pour "
        "installer tous les paquets.",
        "<b>Migration de la base de donnees</b> &mdash; Cree la base de "
        "donnees SQLite et toutes les tables requises via "
        "<font face='Courier'>migrate_db.py</font>.",
        "<b>Installation du service Windows</b> &mdash; Enregistre LedgerLink "
        "comme service d'arriere-plan persistant.",
        "<b>Demarrage du service</b> &mdash; Lance le service immediatement.",
        "<b>Creation des raccourcis de bureau</b> &mdash; Cree les raccourcis "
        "\"LedgerLink AI\" (tableau de bord) et \"LedgerLink Setup\" (assistant).",
        "<b>Lancement de l'assistant</b> &mdash; Ouvre l'assistant de "
        "configuration en 20 etapes dans votre navigateur a l'adresse "
        "<font face='Courier'>http://127.0.0.1:8790/</font>.",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "L'installation complete prend habituellement de 3 a 5 minutes. Un "
        "fichier journal est enregistre dans C:\\LedgerLink\\install.log pour "
        "le depannage.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("2.1.1 Installation macOS", styles["H3"]))
    story.append(Paragraph(
        "Sur macOS, ouvrez le Terminal et executez : <font face='Courier'>"
        "cd ~/Desktop/LedgerLink &amp;&amp; bash INSTALL_MAC.sh</font>. "
        "Le script installe les dependances, cree un service launchd et "
        "ouvre l'assistant.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("2.1.2 Installation sur un second poste", styles["H3"]))
    story.append(Paragraph(
        "Pour installer sur des postes de travail supplementaires, copiez "
        "votre fichier <font face='Courier'>ledgerlink.config.json</font> "
        "sur une cle USB et executez :",
        styles["Body"],
    ))
    story.append(Paragraph(
        "python scripts/install_second_machine.py --config \"E:\\ledgerlink.config.json\"",
        styles["Code"],
    ))
    story.append(Paragraph(
        "La configuration multi-postes recommandee est l'option B : executez "
        "LedgerLink sur un seul serveur avec <font face='Courier'>"
        "host: 0.0.0.0</font>, puis accedez-y depuis les autres postes "
        "via navigateur a l'adresse IP du serveur.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_2_wizard(story, styles):
    """Guide de l'assistant de configuration en 20 etapes."""
    story.append(Paragraph("2.2 Assistant de configuration (20 etapes)", styles["H2"]))
    story.append(Paragraph(
        "L'assistant de configuration fonctionne sur le port 8790 et vous "
        "guide a travers chaque option de configuration. Une barre laterale "
        "affiche la progression. Toutes les etapes sont bilingues "
        "(francais/anglais) avec une bascule de langue.",
        styles["Body"],
    ))
    story.append(sp())

    wizard_steps = [
        ["0", "Bienvenue", "Liste de controle des informations requises : "
         "no TPS, no TVQ, cle de licence, mot de passe admin, courriel professionnel"],
        ["1", "Information du cabinet", "Nom de l'entreprise, adresse, province, "
         "telephone, site Web, numeros d'inscription TPS/TVQ"],
        ["2", "Administrateur", "Creer le compte proprietaire avec nom complet, "
         "identifiant, courriel et mot de passe (8+ car., majuscule, chiffre)"],
        ["3", "Cle de licence", "Saisir et valider votre cle LLAI- ; "
         "affiche le niveau, max clients/utilisateurs, expiration"],
        ["4", "Fournisseurs IA", "Configurer les fournisseurs IA standard "
         "(DeepSeek) et premium (Claude/OpenRouter) avec URL et cles API"],
        ["5", "Courriel (SMTP)", "Configuration SMTP avec modeles Gmail/Outlook ; "
         "bouton de test pour verifier la livraison"],
        ["6", "Portail client", "Afficher l'URL du portail local (port 8788) ; "
         "tunnel Cloudflare optionnel pour l'acces a distance"],
        ["7", "WhatsApp", "Integration Twilio : SID du compte, jeton d'auth, "
         "numero WhatsApp (~0,005 $/message)"],
        ["8", "Telegram", "Creation du bot via @BotFather ; saisir le jeton "
         "et le nom du bot pour activer la reception Telegram"],
        ["9", "Microsoft 365", "Compte de service pour la lecture automatique "
         "des courriels, synchronisation Outlook, resume Teams"],
        ["10", "QuickBooks en ligne", "ID du domaine, ID client, secret client "
         "pour l'inscription des transactions et la synchronisation des fournisseurs"],
        ["11", "Surveillance de dossier", "Definir le chemin du dossier de "
         "reception (defaut : C:/LedgerLink/Inbox/) pour numeriseur USB ou "
         "synchronisation infonuagique"],
        ["12", "Resume quotidien", "Configurer les resumes par courriel : "
         "heure d'envoi, destinataires, langue (FR/EN)"],
        ["13", "Sauvegarde", "Dossier de sauvegarde, frequence "
         "(quotidienne/hebdomadaire/a la connexion), nombre de copies, "
         "bascule OneDrive"],
        ["14", "Notifications", "Alertes par evenement : nouveau doc, fraude, "
         "en attente > X jours, echeance, licence, erreurs ; selection du canal"],
        ["15", "Securite", "Delai d'expiration de session, tentatives max de "
         "connexion, duree de verrouillage, forcer HTTPS"],
        ["16", "Membres du personnel", "Ajouter des gestionnaires et employes "
         "avec mots de passe temporaires ; affiche le tableau du personnel"],
        ["17", "Clients", "Ajouter des clients comptables avec code, courriel, "
         "langue, frequence de production, comptable assigne ; importation CSV"],
        ["18", "Revue et confirmation", "Resume de tous les elements configures "
         "avec indicateurs d'etat (Configure/Non configure)"],
        ["19", "Termine", "URL du tableau de bord, URL du portail, tableau des "
         "identifiants du personnel, telechargement PDF des instructions "
         "d'acces avec codes QR"],
    ]
    story.append(make_table(
        ["Etape", "Nom", "Description"],
        wizard_steps,
        col_widths=[0.5 * inch, 1.2 * inch, 4.8 * inch],
    ))
    story.append(sp())
    story.append(warning_box(
        "Conservez les identifiants du personnel de l'etape 19. Les mots de "
        "passe temporaires ne sont affiches qu'une seule fois. Telechargez "
        "le PDF pour les conserver.",
        styles,
    ))
    story.append(sp())


def _build_section_2_first_login(story, styles):
    """Instructions de premiere connexion."""
    story.append(Paragraph("2.3 Premiere connexion", styles["H2"]))
    story.extend(numbered_list([
        "Ouvrez votre navigateur a l'adresse <font face='Courier'>"
        "http://127.0.0.1:8787/</font> (ou cliquez sur le raccourci de bureau).",
        "Saisissez le nom d'utilisateur et le mot de passe administrateur "
        "crees a l'etape 2.",
        "Le systeme detecte l'absence de personnel ou de clients et redirige "
        "vers l'integration.",
        "Suivez l'integration en 3 etapes : ajouter du personnel, ajouter "
        "des clients, reviser.",
        "Une fois termine, vous arrivez au tableau de bord principal de revision.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Le tableau de bord est limite a 5 tentatives de connexion echouees "
        "par 15 minutes par adresse IP. Au-dela de cette limite, vous recevez "
        "une reponse HTTP 429 et devez attendre avant de reessayer.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_2_config(story, styles):
    """Reference du fichier de configuration."""
    story.append(Paragraph("2.4 Reference du fichier de configuration", styles["H2"]))
    story.append(Paragraph(
        "Tous les parametres sont stockes dans <font face='Courier'>"
        "ledgerlink.config.json</font> a la racine du projet. Sections "
        "principales :",
        styles["Body"],
    ))
    config_rows = [
        ["host / port", "Liaison reseau (defaut 0.0.0.0:8787)"],
        ["session_hours", "Duree de session avant reconnexion (defaut 12)"],
        ["ai_router.routine_provider", "URL, modele et cle du fournisseur IA standard"],
        ["ai_router.premium_provider", "URL, modele et cle du fournisseur IA premium"],
        ["email_digest", "Parametres SMTP, destinataires, horaire"],
        ["security.bcrypt_rounds", "Force du hachage des mots de passe (defaut 12)"],
        ["client_portal.port", "Port du portail (defaut 8788)"],
        ["client_portal.max_upload_mb", "Limite de taille du telechargement (defaut 20 Mo)"],
        ["ingest.port", "Port du service de reception (defaut 8789)"],
        ["database_path", "Emplacement de la base de donnees SQLite"],
    ]
    story.append(make_table(
        ["Parametre", "Description"],
        config_rows,
        col_widths=[2.5 * inch, 4.0 * inch],
    ))
    story.append(sp())
    story.append(warning_box(
        "Ne partagez jamais votre fichier de configuration publiquement "
        "&mdash; il contient des cles API chiffrees et des identifiants SMTP. "
        "Utilisez le script de provisionnement pour creer des copies propres "
        "a distribuer.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("2.5 Options de deploiement multi-postes", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink prend en charge trois modeles de deploiement pour les "
        "cabinets disposant de plusieurs postes de travail :",
        styles["Body"],
    ))
    story.append(sp())
    deploy_rows = [
        ["Option A\nLecteur reseau\npartage", "Placer le fichier .db sur un\n"
         "partage reseau. Configurer\ndatabase_path vers\n\\\\SERVEUR\\partage\\ledgerlink.db",
         "Installation simple", "SQLite ne prend en charge\n"
         "qu'un seul ecrivain a la fois"],
        ["Option B\nServeur + Navigateur\n(Recommande)", "Executer LedgerLink sur un\n"
         "serveur avec host: 0.0.0.0.\nLes autres postes accedent\nvia le navigateur.",
         "Base de donnees unique,\naucun probleme de\nsynchronisation, zero\n"
         "installation client", "Le serveur doit rester\nen ligne"],
        ["Option C\nBD separees\navec synchronisation", "Chaque poste a sa propre\n"
         "base de donnees. Synchroniser\nvia Parametres > Sauvegarde\n> Exporter/Importer.",
         "Entierement independant", "Synchronisation manuelle\nrequise ; risque\nde conflits"],
    ]
    story.append(make_table(
        ["Option", "Configuration", "Avantages", "Limites"],
        deploy_rows,
        col_widths=[1.2 * inch, 2.1 * inch, 1.6 * inch, 1.6 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "L'option B est recommandee pour la plupart des cabinets. Elle ne "
        "necessite aucune installation sur les postes clients &mdash; tout "
        "appareil disposant d'un navigateur Web peut acceder au tableau de "
        "bord a l'adresse IP du serveur.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("2.6 Transfert de licence entre postes", styles["H2"]))
    story.extend(numbered_list([
        "Sur l'ancien poste : allez dans Parametres &gt; Licence &gt; Desactiver.",
        "Copiez <font face='Courier'>ledgerlink.config.json</font> sur le "
        "nouveau poste via cle USB ou partage reseau.",
        "Sur le nouveau poste : la licence s'active automatiquement au "
        "premier demarrage.",
        "En cas de probleme persistant, contactez support@ledgerlink.ca pour "
        "une reinitialisation cote serveur de l'activation de la machine.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("2.7 Provisionnement de nouveaux clients", styles["H2"]))
    story.append(Paragraph(
        "Les administrateurs du cabinet peuvent provisionner de nouveaux "
        "clients avec une seule commande qui genere une cle de licence, "
        "construit un ZIP d'installation et envoie optionnellement le "
        "paquet par courriel au client :",
        styles["Body"],
    ))
    story.append(Paragraph(
        'python scripts/provision_client.py --firm "Cabinet XYZ" '
        '--tier professionnel --months 12 --email client@exemple.com '
        '--contact "Jean Dupont"',
        styles["Code"],
    ))
    story.append(Paragraph(
        "Le script consigne tous les clients provisionnes dans "
        "<font face='Courier'>clients.csv</font> avec la cle de licence, "
        "le niveau, la date d'expiration et la date de provisionnement.",
        styles["Body"],
    ))


# ── Section 3 : Gestion des utilisateurs ─────────────────────
def build_section_3_user_management(story, styles):
    """Section 3 — Roles, comptes, portefeuilles, mots de passe."""
    story.append(Paragraph("3. Gestion des utilisateurs", styles["H1"]))
    story.append(sp())

    _build_section_3_roles(story, styles)
    _build_section_3_accounts(story, styles)
    _build_section_3_portfolios(story, styles)
    _build_section_3_passwords(story, styles)
    story.append(PageBreak())


def _build_section_3_roles(story, styles):
    """Tableau des roles."""
    story.append(Paragraph("3.1 Roles et permissions", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink utilise quatre roles avec des permissions hierarchiques. "
        "Chaque utilisateur se voit attribuer exactement un role lors de "
        "sa creation.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Proprietaire", "Acces complet au systeme. Peut gerer les\n"
         "utilisateurs, voir tous les clients, configurer\n"
         "la licence, executer les outils admin,\n"
         "acceder au module de mission, generer\ndes factures.",
         "Associe ou\ndirigeant du cabinet"],
        ["Gestionnaire", "Voir tous les clients, assigner le travail,\n"
         "approuver les inscriptions, gerer les\n"
         "portefeuilles, acceder a l'importation\n"
         "bancaire, au rapprochement, a la\nfermeture de periode, aux communications.",
         "Comptable senior\nou chef d'equipe"],
        ["Employe", "Voir uniquement les clients assignes. Peut\n"
         "reclamer et reviser des documents, mettre\n"
         "a jour les champs, placer des retenues.\n"
         "Ne peut pas approuver les inscriptions\nni gerer l'equipe.",
         "Teneur de livres\nou junior"],
        ["Client", "Acces au portail client uniquement. Peut\n"
         "televerser des documents, consulter\n"
         "l'historique des soumissions. Aucun acces\n"
         "au tableau de bord. Ne peut pas voir\nles donnees des autres clients.",
         "Client externe\ndu cabinet"],
    ]
    story.append(make_table(
        ["Role", "Permissions", "Utilisateur typique"],
        rows,
        col_widths=[1.0 * inch, 3.5 * inch, 2.0 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Les fonctionnalites reservees au Proprietaire incluent : gestion "
        "des utilisateurs, activation de licence, tableau de bord analytique, "
        "module de mission, depannage systeme, gestion du cache, "
        "reinitialisation de la memoire fournisseur et administration a distance.",
        styles,
    ))
    story.append(sp())


def _build_section_3_accounts(story, styles):
    """Creation de comptes utilisateurs."""
    story.append(Paragraph("3.2 Creation de comptes utilisateurs", styles["H2"]))
    story.append(Paragraph(
        "Seul le role Proprietaire peut creer de nouveaux utilisateurs du "
        "tableau de bord.",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Naviguez vers <b>Utilisateurs</b> dans la barre laterale d'administration.",
        "Cliquez sur <b>Ajouter un utilisateur</b>.",
        "Saisissez : nom d'affichage, identifiant (unique), role "
        "(Gestionnaire ou Employe).",
        "Definissez un mot de passe temporaire (ou laissez l'assistant en "
        "generer un).",
        "Le nouvel utilisateur apparait dans le tableau. Partagez les "
        "identifiants de facon securisee &mdash; le mot de passe temporaire "
        "n'est affiche qu'une seule fois.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Les comptes clients sont crees separement via la configuration du "
        "portail client (voir Section 6). Les utilisateurs clients "
        "n'apparaissent pas dans la liste des utilisateurs du tableau de bord.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_3_portfolios(story, styles):
    """Gestion des portefeuilles."""
    story.append(Paragraph("3.3 Gestion des portefeuilles", styles["H2"]))
    story.append(Paragraph(
        "Les portefeuilles controlent quels clients chaque membre du "
        "personnel peut voir. Les Gestionnaires et Proprietaires accedent "
        "a l'ecran des portefeuilles depuis la barre laterale.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Assigner :</b> Lier un code client a un comptable. Le comptable "
        "voit les documents de ce client dans sa file.",
        "<b>Retirer :</b> Supprimer le lien entre un client et un comptable. "
        "Les documents demeurent mais ne sont plus visibles pour ce membre.",
        "<b>Transferer :</b> Deplacer un client d'un comptable a un autre en "
        "une seule action. Utile lors des transitions de personnel.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Les Proprietaires et Gestionnaires voient toujours tous les clients, "
        "independamment des affectations de portefeuille. Les Employes ne "
        "voient que leurs clients assignes.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_3_passwords(story, styles):
    """Gestion des mots de passe."""
    story.append(Paragraph("3.4 Gestion des mots de passe", styles["H2"]))
    story.extend(bullet_list([
        "Les mots de passe sont haches avec bcrypt (12 tours par defaut).",
        "Les mots de passe historiques SHA-256 sont mis a niveau automatiquement "
        "a la prochaine connexion.",
        "Les Proprietaires peuvent reinitialiser le mot de passe de tout "
        "utilisateur depuis la page Utilisateurs.",
        "Les utilisateurs peuvent modifier leur propre mot de passe depuis "
        "la page Changer le mot de passe.",
        "Exigences minimales : 8 caracteres, au moins une majuscule, "
        "au moins un chiffre.",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Il n'existe aucun mecanisme de recuperation de mot de passe. Si le "
        "Proprietaire oublie son mot de passe, la base de donnees doit etre "
        "modifiee manuellement ou l'assistant de configuration relance.",
        styles,
    ))


# ── Section 4 : Flux de travail quotidien ────────────────────
def build_section_4_daily_workflow(story, styles):
    """Section 4 — Reception, revision, fraude, approbations, banque."""
    story.append(Paragraph("4. Flux de travail quotidien", styles["H1"]))
    story.append(sp())

    _build_section_4_intake(story, styles)
    _build_section_4_review_queue(story, styles)
    _build_section_4_fraud(story, styles)
    _build_section_4_substance(story, styles)
    _build_section_4_uncertainty(story, styles)
    _build_section_4_approvals(story, styles)
    _build_section_4_bank(story, styles)
    _build_section_4_journal(story, styles)
    story.append(PageBreak())


def _build_section_4_intake(story, styles):
    """Methodes de reception des documents."""
    story.append(Paragraph("4.1 Methodes de reception des documents", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink accepte les documents provenant de six canaux. Tous les "
        "canaux alimentent le meme pipeline de traitement : extraction OCR, "
        "detection de fraude, classification de substance et placement dans "
        "la file de revision.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Portail client", "Telechargement via navigateur au port 8788.\n"
         "Formats : PDF, JPEG, PNG, TIFF, WebP, HEIC.\n"
         "Maximum 20 Mo par fichier."],
        ["WhatsApp", "Envoyez une photo/PDF au numero WhatsApp\n"
         "Twilio du cabinet. Acheminement automatique\n"
         "par numero de telephone de l'expediteur\nvers le code client."],
        ["Telegram", "Envoyez un document au bot Telegram\n"
         "du cabinet. Messages traites via le pont\n"
         "OpenClaw a /ingest/openclaw."],
        ["Courriel", "Transferez les factures au courriel de\n"
         "reception. Les pieces jointes MIME sont\n"
         "extraites automatiquement. Le service\nfonctionne sur le port 8789."],
        ["Surveillance de dossier", "Deposez les fichiers dans le dossier de\n"
         "reception surveille (defaut : C:/LedgerLink/\n"
         "Inbox/). Ideal pour les numeriseurs USB\net les dossiers synchronises."],
        ["Telechargement\nmanuel", "Telechargez directement depuis la file\n"
         "de documents du tableau de bord.\nGlisser-deposer pris en charge."],
    ]
    story.append(make_table(
        ["Canal", "Fonctionnement"],
        rows,
        col_widths=[1.5 * inch, 5.0 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Pour les numeriseurs de documents USB, pointez le dossier de sortie "
        "du numeriseur vers le dossier de reception LedgerLink. Les documents "
        "sont pris en charge automatiquement en quelques secondes.",
        styles,
    ))
    story.append(sp())


def _build_section_4_review_queue(story, styles):
    """File de revision et etats des documents."""
    story.append(Paragraph("4.2 File de revision", styles["H2"]))
    story.append(Paragraph(
        "Tous les documents recus apparaissent dans la file de revision sur "
        "la page d'accueil du tableau de bord. Les documents sont "
        "automatiquement classifies et notes pour la confiance.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.2.1 Etats des documents", styles["H3"]))
    rows = [
        ["EnRevision", "Confiance faible, indicateur de fraude, indicateur\n"
         "de substance ou montant eleve (&ge; 25 000 $).\n"
         "Necessite une revision humaine."],
        ["Pret", "Confiance elevee (&ge; 0,85), tous les champs\n"
         "remplis, aucun indicateur bloquant.\nPeut etre inscrit."],
        ["Exception", "Champ critique manquant (fournisseur ou code\n"
         "client). Doit etre resolu avant le traitement."],
        ["EnRetenue", "Place manuellement en retenue par le reviseur\n"
         "avec une note de justification. Reste jusqu'a\nremise a l'etat Pret."],
        ["Inscrit", "Inscrit avec succes dans QuickBooks en ligne.\n"
         "Immuable &mdash; les corrections necessitent\nde nouvelles ecritures."],
        ["Ignore", "Exclu du traitement. Peut etre restaure."],
    ]
    story.append(make_table(
        ["Etat", "Description"],
        rows,
        col_widths=[1.3 * inch, 5.2 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("4.2.2 Filtres de la file", styles["H3"]))
    story.extend(bullet_list([
        "<b>Filtre d'etat :</b> EnRevision, Pret, Exception, Inscrit, "
        "EnRetenue, Ignore",
        "<b>Recherche :</b> Recherche par mot-cle dans le fournisseur, le "
        "montant, les notes",
        "<b>Code client :</b> Filtrer pour un seul client",
        "<b>Mode de file :</b> Tous visibles, Ma file (assignes a moi), "
        "Non assignes",
        "<b>Inclure les ignores :</b> Bascule pour afficher ou masquer "
        "les documents ignores",
    ], styles))
    story.append(sp())

    story.append(Paragraph("4.2.3 Vue de detail du document", styles["H3"]))
    story.append(Paragraph(
        "Cliquer sur un document ouvre la vue de detail affichant : nom du "
        "fournisseur, code client, type de document, montant, date, compte "
        "du grand livre, code de taxe, categorie, score de confiance, "
        "resultat OCR brut, historique d'apprentissage, suggestions "
        "d'apprentissage, etat de preparation a l'inscription et "
        "correspondances de memoire fournisseur. La visionneuse PDF est "
        "integree pour une comparaison cote a cote.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.2.4 Logique d'approbation automatique", styles["H3"]))
    story.append(Paragraph(
        "Les documents avec une confiance &ge; 0,85, tous les champs requis "
        "et aucun indicateur bloquant sont automatiquement marques comme "
        "Pret. Les conditions suivantes bloquent l'approbation automatique :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Indicateurs de fraude de severite elevee ou critique (confiance "
        "plafonnee a 0,60)",
        "Indicateurs de substance necessitant une revision humaine",
        "Montants eleves &ge; 25 000 $ (confiance plafonnee a 0,75)",
        "Credits importants &lt; -5 000 $ (confiance plafonnee a 0,65)",
        "Format de date invalide ou montant total nul",
        "Champs obligatoires manquants : fournisseur, total, date",
        "Factures a taxe mixte (confiance plafonnee a 0,50)",
    ], styles))
    story.append(sp())


def _build_section_4_fraud(story, styles):
    """Regles de detection de fraude."""
    story.append(Paragraph("4.3 Detection de fraude", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink execute 13 regles deterministes de fraude sur chaque "
        "document. Aucune IA n'est utilisee pour la detection &mdash; l'IA "
        "n'explique que les elements signales. Chaque regle produit un "
        "indicateur avec un niveau de severite.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["1", "Anomalie de montant\nfournisseur",
         "Montant &gt; 2 ecarts-types de la\nmoyenne du fournisseur", "ELEVE"],
        ["2", "Anomalie de calendrier\nfournisseur",
         "Jour de facture &gt; 14 jours de la\nnorme", "MOYEN"],
        ["3", "Doublon (meme\nfournisseur)",
         "Meme fournisseur + meme montant\nen 30 jours", "ELEVE"],
        ["4", "Doublon (fournisseurs\ndifferents)",
         "Fournisseur different + meme montant\nen 7 jours", "MOYEN"],
        ["5", "Transaction de\nfin de semaine",
         "Samedi/dimanche avec montant\n&gt; 200 $", "FAIBLE"],
        ["6", "Transaction de\njour ferie",
         "Jour ferie statutaire du Quebec,\nmontant &gt; 200 $", "FAIBLE"],
        ["7", "Montant rond", "Montant exact rond d'un fournisseur\nirregulier", "FAIBLE"],
        ["8", "Nouveau fournisseur\nmontant eleve",
         "Premiere facture d'un fournisseur\nau-dessus de 2 000 $", "MOYEN"],
        ["9", "Changement de\ncompte bancaire",
         "Coordonnees bancaires du fournisseur\nmodifiees entre les factures", "CRITIQUE"],
        ["10", "Facture apres\npaiement",
         "Date de facture posterieure a la date\nde paiement correspondant", "ELEVE"],
        ["11", "Probleme\nd'inscription fiscale",
         "Facture TPS/TVQ d'un fournisseur\nhistoriquement exempte", "ELEVE"],
        ["12", "Changement de\ncategorie",
         "Categorie du fournisseur contredit\n80 %+ de l'historique", "MOYEN"],
        ["13", "Divergence de\nbeneficiaire",
         "Beneficiaire bancaire different du\nfournisseur de la facture", "ELEVE"],
    ]
    story.append(make_table(
        ["No", "Regle", "Condition de declenchement", "Severite"],
        rows,
        col_widths=[0.3 * inch, 1.6 * inch, 3.1 * inch, 1.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("4.3.1 Flux de derogation de fraude", styles["H3"]))
    story.append(Paragraph(
        "Les indicateurs de fraude de severite Critique et Elevee necessitent "
        "une derogation du Gestionnaire ou du Proprietaire avant l'inscription. "
        "Le flux de derogation exige :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Case de reconnaissance explicite",
        "Justification detaillee (minimum 10 caracteres)",
        "Toutes les derogations sont journalisees avec horodatage, nom "
        "d'utilisateur, ID du document, indicateurs de fraude et motif "
        "de derogation",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Les derogations de fraude creent une piste d'audit permanente. "
        "Assurez-vous que les justifications sont rigoureuses et exactes, "
        "car elles pourraient etre examinees lors de missions de "
        "verification externes.",
        styles,
    ))
    story.append(sp())


def _build_section_4_substance(story, styles):
    """Indicateurs de substance."""
    story.append(Paragraph("4.4 Classification de substance", styles["H2"]))
    story.append(Paragraph(
        "Le moteur de substance identifie les elements hors exploitation "
        "necessitant un traitement special au grand livre. La detection "
        "utilise la correspondance de mots-cles bilingues avec bascule IA "
        "lorsque la confiance est faible.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Immobilisation", "Equipement, vehicules,\nordinateurs, renovations, CVAC",
         "1500\n(Immobilisations)", "0,70"],
        ["Charge payee\nd'avance", "Assurance, loyer d'avance,\nabonnements annuels",
         "1300\n(Charges payees\nd'avance)", "0,70"],
        ["Emprunt", "Prets, hypotheques, marges\nde credit, contrats de\nlocation-financement",
         "2500\n(Long terme)", "0,70"],
        ["Remise fiscale", "TPS/TVQ, retenues a la source,\nCNESST, FSS",
         "2200-2215\n(Passifs fiscaux)", "0,70"],
        ["Depense\npersonnelle", "Epicerie, vetements,\nNetflix, vacances, gym",
         "5400\n(Personnel)", "0,70"],
        ["Actionnaire", "Retraits, transactions entre\nparties liees, prets",
         "2600\n(Actionnaire)", "0,70"],
    ]
    story.append(make_table(
        ["Categorie", "Exemples", "Suggestion GL", "Conf. max"],
        rows,
        col_widths=[1.2 * inch, 2.0 * inch, 1.3 * inch, 0.9 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Lorsqu'un indicateur de substance est leve, la suggestion de compte "
        "du grand livre change automatiquement. Verifiez le compte suggere "
        "sur la page de detail du document avant d'approuver.",
        styles,
    ))
    story.append(sp())


def _build_section_4_uncertainty(story, styles):
    """Raisons d'incertitude."""
    story.append(Paragraph("4.5 Suivi de l'incertitude", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink suit plus de 21 modes de defaillance avec des raisons "
        "d'incertitude structurees. Chaque raison comprend des descriptions "
        "bilingues et des exigences de preuve. Les decisions d'inscription "
        "sont basees sur la confiance :",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["&ge; 0,80 (tous champs)", "INSCRIPTION_SURE",
         "Approbation automatique\nsi aucune raison non resolue"],
        ["0,60 &ndash; 0,79", "INSCRIPTION_PARTIELLE_AVEC_INDICATEURS",
         "Permis avec revision\nmanuelle et indicateurs"],
        ["&lt; 0,60", "BLOCAGE_EN_ATTENTE_DE_REVISION",
         "Inscription bloquee\njusqu'a resolution"],
    ]
    story.append(make_table(
        ["Confiance", "Recommandation", "Action"],
        rows,
        col_widths=[1.5 * inch, 2.5 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_section_4_approvals(story, styles):
    """Flux d'approbation et d'inscription."""
    story.append(Paragraph("4.6 Approbation et inscription", styles["H2"]))
    story.append(Paragraph(
        "Le flux d'inscription fait passer les documents par quatre etats :",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "<b>Brouillon</b> &mdash; Travail d'inscription initial cree a "
        "partir du document revise. Codes de taxe valides par rapport au "
        "compte du grand livre et a la province.",
        "<b>Pret a inscrire</b> &mdash; Toutes les validations reussies. "
        "La verification mathematique confirme que sous-total + taxes = total "
        "(garde contre les hallucinations).",
        "<b>Approuve pour inscription</b> &mdash; Le Gestionnaire ou le "
        "Proprietaire approuve le travail d'inscription. Verification des "
        "indicateurs de fraude appliquee.",
        "<b>Inscrit</b> &mdash; Transaction soumise a QuickBooks en ligne. "
        "Retourne posting_id et external_id. Le document devient immuable.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Les inscriptions echouees peuvent etre retentees depuis la file. "
        "La tentative de nouveau verifie egalement les indicateurs de fraude "
        "pour empecher le contournement.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_4_bank(story, styles):
    """Importation bancaire et rapprochement."""
    story.append(Paragraph("4.7 Importation bancaire et appariement", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink importe les releves bancaires en formats CSV et PDF des "
        "principales banques quebecoises : Desjardins, Banque Nationale, "
        "BMO, TD et RBC. La detection de la banque est automatique.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.7.1 Appariement intelligent", styles["H3"]))
    story.append(Paragraph(
        "Chaque transaction bancaire est appariee aux factures existantes "
        "selon trois criteres :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Similarite du fournisseur :</b> Correspondance floue &ge; 80 % "
        "(avec elimination des accents et gestion des suffixes d'entreprise "
        "quebecois : inc, ltee, enr, senc)",
        "<b>Tolerance de montant :</b> Dans les 2 % du montant de la facture",
        "<b>Fenetre de date :</b> Dans les 7 jours de la date de paiement prevue",
    ], styles))
    story.append(sp())

    story.append(Paragraph("4.7.2 Paiements fractionnes", styles["H3"]))
    story.append(Paragraph(
        "Lorsqu'une seule transaction bancaire couvre plusieurs factures, "
        "utilisez la fonctionnalite de paiement fractionne. Le systeme "
        "detecte les fractionnements potentiels et permet l'allocation "
        "manuelle entre les factures.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.7.3 Detection des contre-passations", styles["H3"]))
    story.append(Paragraph(
        "Le moteur d'appariement bancaire detecte les contre-passations en "
        "utilisant : similarite du fournisseur &ge; 80 %, signes opposes ou "
        "mots-cles de contre-passation (annulation, reversal, correction), "
        "montant dans les 1 % et proximite de date dans les 5 jours ouvrables.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.7.4 Appariement multidevise", styles["H3"]))
    story.append(Paragraph(
        "Pour les transactions en USD appariees a des factures en CAD, le "
        "moteur d'appariement bancaire applique les taux de change de la "
        "Banque du Canada avec une tolerance de 2 %.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_4_journal(story, styles):
    """Ecritures de journal manuelles."""
    story.append(Paragraph("4.8 Ecritures de journal manuelles", styles["H2"]))
    story.append(Paragraph(
        "Les ecritures de journal manuelles (EJM) peuvent etre creees par "
        "les Gestionnaires et les Proprietaires pour les ajustements non "
        "captures par le traitement des documents.",
        styles["Body"],
    ))
    story.append(sp())
    story.extend(bullet_list([
        "<b>Creer :</b> Code client, periode, date, comptes debit/credit, "
        "montant, description.",
        "<b>Detection de conflit :</b> Detecte automatiquement lorsqu'une "
        "EJM entre en conflit avec une correction d'inscription automatisee "
        "(piege 7). Les ecritures conflictuelles sont mises en quarantaine "
        "pour revision.",
        "<b>Detection de taxe fantome :</b> Signale les demandes de CTI sans "
        "inscription a la TPS/TVQ. Etat defini a blocage_taxe_fantome.",
        "<b>Inscrire :</b> Passer de l'etat brouillon a inscrit.",
        "<b>Contre-passer :</b> Creer une ecriture de contre-passation pour "
        "une EJM inscrite ou en brouillon.",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Les ecritures de journal manuelles sont soumises au verrouillage "
        "optimiste (piege 6). Si un autre utilisateur modifie le meme "
        "document entre votre lecture et votre approbation, le systeme "
        "rejette la version obsolete et vous oblige a rafraichir.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("4.9 Apprentissage et memoire fournisseur", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink apprend continuellement des corrections des reviseurs "
        "pour ameliorer la precision future. Le systeme d'apprentissage "
        "comporte trois composantes :",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.9.1 Historique d'apprentissage", styles["H3"]))
    story.append(Paragraph(
        "Chaque correction effectuee lors de la revision d'un document est "
        "enregistree dans l'historique d'apprentissage. Cela comprend les "
        "modifications au : nom du fournisseur, compte du grand livre, "
        "code de taxe, categorie, montant et date. L'historique est visible "
        "sur la page de detail du document dans la section "
        "\"Historique d'apprentissage\".",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.9.2 Suggestions d'apprentissage", styles["H3"]))
    story.append(Paragraph(
        "En fonction des corrections passees, le systeme suggere des valeurs "
        "pour les nouveaux documents. Les suggestions apparaissent sur la "
        "page de detail du document et incluent des scores de confiance. "
        "Une confiance plus elevee (approbations passees plus coherentes) "
        "signifie des suggestions plus fiables.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.9.3 Memoire fournisseur", styles["H3"]))
    story.append(Paragraph(
        "La memoire fournisseur stocke les patrons appris par combinaison "
        "fournisseur et client :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Compte du grand livre prefere pour chaque fournisseur",
        "Code de taxe typiquement attribue",
        "Fourchette de montant attendue (pour la detection d'anomalies)",
        "Calendrier habituel de facturation (jour du mois)",
        "Preferences de categorie",
        "La confiance augmente avec chaque approbation coherente",
        "Reinitialisation disponible depuis Admin &gt; Memoire fournisseur au besoin",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Les 5 premieres transactions de tout fournisseur necessitent une "
        "revision manuelle complete. Apres 5 approbations coherentes, le "
        "systeme developpe suffisamment de confiance pour des scores "
        "d'approbation automatique plus eleves et des references de "
        "detection de fraude plus fiables.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("4.10 Reference des champs de document", styles["H2"]))
    story.append(Paragraph(
        "Chaque document dans la file de revision contient les champs "
        "suivants, extraits par l'IA et modifiables par les reviseurs :",
        styles["Body"],
    ))
    story.append(sp())
    field_rows = [
        ["Fournisseur", "Nom de l'entreprise ou de la personne sur la facture", "Obligatoire"],
        ["Code client", "Client comptable auquel le document appartient", "Obligatoire"],
        ["Type de document", "facture, recu, note_de_credit, facture_services\n"
         "publics, transaction_bancaire, releve_carte_credit", "Auto-detecte"],
        ["Montant", "Montant total de la facture (negatif pour les credits)", "Obligatoire"],
        ["Date du document", "Date de la facture ou du recu", "Obligatoire"],
        ["Compte GL", "Code du compte du grand livre", "Suggere par l'IA"],
        ["Code de taxe", "T, Z, E, M, I, TVH, TVH_ATL, TPS_SEULE,\nTVA, AUCUN",
         "Suggere par l'IA"],
        ["Categorie", "Categorie de depense pour les rapports", "Suggeree par l'IA"],
        ["Etat de revision", "EnRevision, Pret, Exception, EnRetenue,\nInscrit, Ignore",
         "Auto-attribue"],
        ["Confiance", "Score de confiance effectif de 0,00 a 1,00", "Calcule"],
        ["Indicateurs de\nfraude", "Tableau JSON des regles de fraude declenchees", "Auto-detecte"],
        ["Indicateurs de\nsubstance", "Immobilisation, charge payee d'avance,\n"
         "emprunt, taxe, personnel, etc.", "Auto-detecte"],
        ["Resultat brut", "JSON complet de l'extraction OCR", "Lecture seule"],
    ]
    story.append(make_table(
        ["Champ", "Description", "Source"],
        field_rows,
        col_widths=[1.3 * inch, 3.2 * inch, 1.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("4.11 Communications", styles["H2"]))
    story.append(Paragraph(
        "Les Gestionnaires et Proprietaires peuvent rediger et envoyer des "
        "messages aux clients directement depuis le tableau de bord. "
        "Naviguez vers <b>Communications</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Brouillon :</b> L'IA genere un message contextuel en utilisant "
        "le fournisseur, le montant et le code client. Prise en charge du "
        "francais et de l'anglais.",
        "<b>Modifier :</b> Reviser et modifier le brouillon avant l'envoi.",
        "<b>Envoyer :</b> Livrer le message par SMTP au courriel du client.",
        "<b>Historique :</b> Consulter tous les messages envoyes et "
        "en brouillon avec horodatage.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("4.12 Tableau de bord analytique", styles["H2"]))
    story.append(Paragraph(
        "La page d'analytique (Proprietaire uniquement) fournit des "
        "renseignements operationnels :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Productivite du personnel :</b> Documents traites, temps moyen "
        "de revision, taux de retenue, taux d'approbation par membre d'equipe",
        "<b>Complexite des clients :</b> Nombre de documents, taux "
        "d'approbation, taux d'exception par client",
        "<b>Tendances mensuelles :</b> Graphiques de volume, distribution "
        "des etats dans le temps",
        "<b>Resume de fraude :</b> Nombre d'incidents par type de regle",
        "<b>Echeances a risque :</b> Dates de production a venir susceptibles "
        "d'etre manquees",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Utilisez ces indicateurs pour identifier les goulots d'etranglement, "
        "reallouer le personnel et surveiller quels clients generent le "
        "plus d'exceptions.",
        styles["Body"],
    ))


# ── Section 5 : Fiscalite quebecoise ─────────────────────────
def build_section_5_quebec_tax(story, styles):
    """Section 5 — Conformite fiscale complete du Quebec."""
    story.append(Paragraph("5. Fiscalite quebecoise", styles["H1"]))
    story.append(sp())

    _build_section_5_rates(story, styles)
    _build_section_5_tax_codes(story, styles)
    _build_section_5_itc_itr(story, styles)
    _build_section_5_filing(story, styles)
    _build_section_5_revenu_quebec(story, styles)
    _build_section_5_quick_method(story, styles)
    _build_section_5_deadlines(story, styles)
    _build_section_5_payroll(story, styles)
    _build_section_5_customs(story, styles)
    _build_section_5_mixed(story, styles)
    story.append(PageBreak())


def _build_section_5_rates(story, styles):
    """Taux TPS/TVQ/TVH par province."""
    story.append(Paragraph("5.1 Taux de taxe par province", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink calcule les taxes en utilisant l'arithmetique Decimal "
        "exacte (ROUND_HALF_UP a 0,01 $). Tous les taux sont deterministes "
        "sans aucune intervention de l'IA.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Quebec (QC)", "5,000 %", "9,975 %", "&mdash;", "14,975 %"],
        ["Ontario (ON)", "&mdash;", "&mdash;", "13,000 %", "13,000 %"],
        ["Nouveau-Brunswick (NB)", "&mdash;", "&mdash;", "15,000 %", "15,000 %"],
        ["Nouvelle-Ecosse (NE)", "&mdash;", "&mdash;", "15,000 %", "15,000 %"],
        ["Terre-Neuve (TNL)", "&mdash;", "&mdash;", "15,000 %", "15,000 %"],
        ["Ile-du-Prince-Edouard (IPE)", "&mdash;", "&mdash;", "15,000 %", "15,000 %"],
        ["Colombie-Britannique (CB)", "5,000 %", "&mdash;", "&mdash;", "5 % + 7 % TVP"],
        ["Manitoba (MB)", "5,000 %", "&mdash;", "&mdash;", "5 % + 7 % TVP"],
        ["Saskatchewan (SK)", "5,000 %", "&mdash;", "&mdash;", "5 % + 6 % TVP"],
        ["Alberta (AB)", "5,000 %", "&mdash;", "&mdash;", "5,000 %"],
        ["Territoires (TNO/NU/YT)", "5,000 %", "&mdash;", "&mdash;", "5,000 %"],
    ]
    story.append(make_table(
        ["Province", "TPS", "TVQ", "TVH", "Taux effectif"],
        rows,
        col_widths=[1.8 * inch, 0.8 * inch, 0.8 * inch, 0.8 * inch, 1.3 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Au Quebec, la TPS et la TVQ sont calculees en parallele sur le "
        "montant avant taxes (non en cascade). Cela differe de l'ancien "
        "systeme ou la TVQ etait calculee sur le montant incluant la TPS.",
        styles,
    ))
    story.append(sp())


def _build_section_5_tax_codes(story, styles):
    """Codes de taxe avec exemples."""
    story.append(Paragraph("5.2 Codes de taxe", styles["H2"]))
    story.append(Paragraph(
        "Chaque document se voit attribuer un code de taxe qui determine "
        "le traitement fiscal et l'admissibilite aux CTI/RTI :",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["T", "Taxable", "TPS 5 % + TVQ 9,975 %", "100 % / 100 %",
         "Fournitures de bureau\nd'un fournisseur quebecois"],
        ["Z", "Detaxe", "0 % de taxe", "100 % CTI sur intrants",
         "Produits alimentaires de\nbase, medicaments, exports"],
        ["E", "Exempte", "0 % de taxe", "Aucun CTI",
         "Loyer residentiel,\nservices financiers, sante"],
        ["M", "Repas", "TPS + TVQ (50 % admissible)", "50 % / 50 %",
         "Repas d'affaires,\ndivertissement"],
        ["I", "Assurance", "QC 9 % non recuperable", "Aucun CTI",
         "Primes d'assurance\nde biens/dommages"],
        ["TVH", "TVH Ontario", "13 %", "100 %",
         "Achat d'un fournisseur\nontarien"],
        ["TVH_ATL", "TVH Atlantique", "15 %", "100 %",
         "Achat du NB, NE,\nTNL, IPE"],
        ["TPS_SEULE", "TPS seulement", "5 %", "100 %",
         "Achat de l'AB,\nTNO, NU, YT"],
        ["TVA", "TVA etrangere", "Variable", "Non recuperable",
         "Achat international\navec taxe etrangere"],
        ["AUCUN", "Aucune taxe", "0 %", "S.O.",
         "Frais gouvernementaux,\nfrais bancaires"],
    ]
    story.append(make_table(
        ["Code", "Nom", "Taux de taxe", "CTI/RTI", "Exemple"],
        rows,
        col_widths=[0.6 * inch, 1.0 * inch, 1.4 * inch, 1.2 * inch, 1.6 * inch],
    ))
    story.append(sp())
    story.append(warning_box(
        "Le code de taxe I (Assurance) est specifique au Quebec. Les primes "
        "d'assurance au Quebec portent une taxe provinciale de 9 % qui n'est "
        "PAS recuperable comme RTI, contrairement a la TVQ ordinaire.",
        styles,
    ))
    story.append(sp())


def _build_section_5_itc_itr(story, styles):
    """Explication des CTI/RTI."""
    story.append(Paragraph(
        "5.3 Credits de taxe sur les intrants (CTI) et remboursements de "
        "la taxe sur les intrants (RTI)", styles["H2"]))
    story.append(Paragraph(
        "Les entreprises inscrites a la TPS/TVQ peuvent recuperer les taxes "
        "payees sur les depenses d'entreprise au moyen de credits de taxe "
        "sur les intrants (CTI pour la TPS) et de remboursements de la taxe "
        "sur les intrants (RTI pour la TVQ).",
        styles["Body"],
    ))
    story.append(sp())
    story.extend(bullet_list([
        "<b>Recuperation totale (T, TVH, TPS_SEULE) :</b> 100 % de la "
        "TPS/TVQ/TVH payee est admissible",
        "<b>Recuperation partielle (M - Repas) :</b> Seulement 50 % de la "
        "TPS et de la TVQ est admissible pour les repas et le divertissement",
        "<b>Detaxe (Z) :</b> Aucune taxe facturee, mais le CTI sur les "
        "intrants est tout de meme admissible (ex. exportateur achetant "
        "des fournitures)",
        "<b>Exempte (E) :</b> Aucune taxe facturee ET aucune recuperation "
        "de CTI sur les intrants",
        "<b>Assurance (I) :</b> La taxe de 9 % sur les primes au Quebec "
        "n'est jamais recuperable",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "LedgerLink suit les montants de CTI/RTI par document et les "
        "totalise dans le sommaire de production pour les declarations "
        "de TPS/TVQ.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_5_filing(story, styles):
    """Sommaire de production."""
    story.append(Paragraph("5.4 Sommaire de production", styles["H2"]))
    story.append(Paragraph(
        "Le sommaire de production totalise tous les documents inscrits pour "
        "une periode et calcule la TPS et la TVQ nettes payables ou "
        "remboursables :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "TPS totale percue sur les ventes",
        "Total des CTI (TPS payee sur les achats)",
        "TPS nette = Percue - CTI",
        "TVQ totale percue sur les ventes",
        "Total des RTI (TVQ payee sur les achats)",
        "TVQ nette = Percue - RTI",
        "Ajustements pour repas (restriction de 50 % appliquee)",
    ], styles))
    story.append(sp())


def _build_section_5_revenu_quebec(story, styles):
    """Preremplissage Revenu Quebec."""
    story.append(Paragraph("5.5 Preremplissage FPZ-500 de Revenu Quebec", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink genere des donnees de preremplissage pour le FPZ-500 "
        "(declaration de TPS/TVQ du Quebec) a partir des documents inscrits "
        "pour la periode. Naviguez vers <b>Revenu Quebec</b> dans la barre "
        "laterale (Gestionnaire/Proprietaire).",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Selectionnez le client et la periode",
        "Revisez les montants calcules (ventes, CTI, RTI, ajustements)",
        "Telechargez le resume PDF comme reference lors de la production en ligne",
        "Configurez le client entre la methode standard et la methode rapide",
    ], styles))
    story.append(sp())


def _build_section_5_quick_method(story, styles):
    """Methode rapide."""
    story.append(Paragraph("5.6 Methode rapide de comptabilite", styles["H2"]))
    story.append(Paragraph(
        "Les petites entreprises peuvent opter pour la methode rapide, qui "
        "simplifie la remise de TPS/TVQ a un pourcentage des ventes taxables "
        "(taxes incluses). LedgerLink prend en charge la configuration de "
        "la methode rapide par client.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Configurer via <b>Revenu Quebec &gt; Definir la configuration</b>",
        "Selectionner le type d'entite : commerce de detail ou services",
        "Le systeme applique le taux de la methode rapide au lieu du "
        "calcul detaille de CTI/RTI",
        "Les clients en methode rapide ne reclament pas de CTI individuels",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "La methode rapide est accessible aux entreprises dont les ventes "
        "taxables annuelles (taxes incluses) sont de 400 000 $ ou moins. "
        "Elle reduit considerablement la complexite de la tenue de livres "
        "pour les clients admissibles.",
        styles,
    ))
    story.append(sp())


def _build_section_5_deadlines(story, styles):
    """Calendrier et echeances de production."""
    story.append(Paragraph("5.7 Calendrier de production", styles["H2"]))
    story.append(Paragraph(
        "Le calendrier de production suit les echeances de declaration "
        "TPS/TVQ par client. Naviguez vers <b>Calendrier</b> dans la "
        "barre laterale.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Mensuelle", "Dernier jour du mois suivant\nla periode de declaration",
         "Grandes entreprises\n(ventes annuelles &gt; 6 M$)"],
        ["Trimestrielle", "Dernier jour du mois suivant\nla fin du trimestre",
         "Entreprises moyennes\n(1,5 M$ &ndash; 6 M$ annuel)"],
        ["Annuelle", "3 mois apres la fin de\nl'exercice financier",
         "Petites entreprises\n(&lt; 1,5 M$ annuel)"],
    ]
    story.append(make_table(
        ["Frequence", "Echeance", "Declarant typique"],
        rows,
        col_widths=[1.3 * inch, 2.5 * inch, 2.2 * inch],
    ))
    story.append(sp())
    story.extend(bullet_list([
        "Configurer la frequence et la fin d'exercice par client",
        "Generation automatique des echeances selon la configuration",
        "Marquer comme produit avec le nom de l'utilisateur et l'horodatage",
        "Alertes de notification 14 jours avant l'echeance (configurable)",
    ], styles))
    story.append(sp())


def _build_section_5_payroll(story, styles):
    """Conformite de la paie."""
    story.append(Paragraph("5.8 Conformite de la paie (Quebec)", styles["H2"]))
    story.append(Paragraph(
        "Le moteur de paie valide les retenues a la source specifiques au "
        "Quebec en utilisant des regles deterministes. Toute l'arithmetique "
        "utilise Python Decimal.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.1 Taux de retenue a la source (2024-2025)", styles["H3"]))
    rows = [
        ["RRQ (Regime de rentes\ndu Quebec)", "Employe : 6,40 %\nEmploye2 : 4,00 %",
         "Residents du Quebec\nuniquement (remplace le RPC)"],
        ["RPC (Regime de pensions\ndu Canada)", "Employe : 5,95 %\nEmploye2 : 4,00 %",
         "Toutes les autres\nprovinces"],
        ["AE (Assurance-emploi)", "Quebec : 1,32 %\nAutre : 1,66 %",
         "Taux reduit pour le Quebec\n(compensation RQAP)"],
        ["RQAP (Regime quebecois\nd'assurance parentale)", "Employe : 0,494 %\n"
         "Employeur : 0,692 %", "Quebec uniquement"],
    ]
    story.append(make_table(
        ["Retenue", "Taux", "Notes"],
        rows,
        col_widths=[2.0 * inch, 1.8 * inch, 2.2 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.2 Paliers du FSS (Fonds des services de sante)",
                           styles["H3"]))
    hsf_rows = [
        ["&le; 1 000 000 $", "1,25 %"],
        ["1 M$ &ndash; 2 M$", "1,25 % &ndash; 1,65 % (progressif)"],
        ["2 M$ &ndash; 3 M$", "1,65 % &ndash; 2,00 %"],
        ["3 M$ &ndash; 5 M$", "2,00 % &ndash; 2,50 %"],
        ["5 M$ &ndash; 7 M$", "2,50 % &ndash; 3,70 %"],
        ["&gt; 7 000 000 $", "4,26 %"],
    ]
    story.append(make_table(
        ["Masse salariale totale", "Taux FSS"],
        hsf_rows,
        col_widths=[2.5 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.3 Cotisations CNESST", styles["H3"]))
    story.append(Paragraph(
        "Les cotisations CNESST (sante et securite au travail) varient selon "
        "le code de classification industrielle. Exemples de taux :",
        styles["Body"],
    ))
    cnesst_rows = [
        ["54010", "Bureau / Professionnel", "0,54 %"],
        ["23010", "Construction", "5,85 %"],
        ["52010", "Commerce de detail", "1,22 %"],
        ["61010", "Transport", "3,44 %"],
        ["62010", "Restauration / Alimentation", "2,10 %"],
    ]
    story.append(make_table(
        ["Code", "Industrie", "Taux"],
        cnesst_rows,
        col_widths=[1.0 * inch, 3.0 * inch, 1.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.4 Rapprochement RL-1 / T4", styles["H3"]))
    story.append(Paragraph(
        "Les employeurs quebecois doivent produire a la fois le RL-1 "
        "(provincial) et le T4 (federal). LedgerLink valide la "
        "correspondance entre les cases :",
        styles["Body"],
    ))
    rl1_rows = [
        ["Case A &mdash; Revenu d'emploi", "Case 14 &mdash; Revenu d'emploi"],
        ["Case C &mdash; Cotisation RRQ employe", "Case 16 &mdash; Cotisation RPC employe"],
        ["Case F &mdash; Prime d'AE", "Case 18 &mdash; Prime d'AE"],
        ["Case H &mdash; Cotisation RQAP employe", "Case 55 &mdash; Gains assurables RPAP"],
    ]
    story.append(make_table(
        ["RL-1 (Quebec)", "T4 (Federal)"],
        rl1_rows,
        col_widths=[3.0 * inch, 3.0 * inch],
    ))
    story.append(sp())


def _build_section_5_customs(story, styles):
    """Douanes et importations."""
    story.append(Paragraph("5.9 Douanes et importations", styles["H2"]))
    story.append(Paragraph(
        "Le moteur douanier traite les calculs de taxe a l'importation de "
        "l'ASFC (Agence des services frontaliers du Canada) selon l'article "
        "45 de la Loi sur les douanes.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.9.1 Determination de la valeur en douane", styles["H3"]))
    story.extend(bullet_list([
        "Escompte affiche sur la facture commerciale + inconditionnel + "
        "pas apres l'importation : utiliser le prix escompte",
        "Escompte conditionnel (volume, fidelite) : utiliser le prix "
        "sans escompte",
        "Escompte post-importation : utiliser le prix sans escompte",
        "Aucun escompte : le montant de la facture est la valeur en douane",
    ], styles))
    story.append(sp())

    story.append(Paragraph("5.9.2 Calcul de la taxe a l'importation", styles["H3"]))
    story.append(Paragraph(
        "TPS sur les importations : (valeur en douane + droits + accise) x "
        "5 %. Cette TPS est recuperable comme CTI. Pour les importateurs "
        "quebecois, la TVQ est calculee sur : (valeur en douane + droits + "
        "TPS) x 9,975 %.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.9.3 Validation du taux de change", styles["H3"]))
    story.append(Paragraph(
        "LedgerLink valide les taux de change par rapport aux taux quotidiens "
        "de la Banque du Canada. Les taux manuels s'ecartant de plus de 1 % "
        "du taux de la BdC sont signales pour revision.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.9.4 Prevention de la double imposition", styles["H3"]))
    story.append(Paragraph(
        "Le moteur douanier detecte les scenarios ou la TPS/TVQ a "
        "l'importation et la TPS/TVQ domestique pourraient etre facturees, "
        "empechant la double imposition.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_5_mixed(story, styles):
    """Factures mixtes."""
    story.append(Paragraph("5.10 Factures a taxe mixte", styles["H2"]))
    story.append(Paragraph(
        "Certaines factures contiennent a la fois des articles taxables et "
        "exemptes (ex. fournitures medicales avec alimentation). Le resolveur "
        "de code de taxe detecte les factures mixtes en utilisant la "
        "correspondance de mots-cles bilingues avec bascule IA.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Correspondance forte de mots-cles : confiance 0,85 "
        "(ex. \"partiellement exempte\")",
        "Detection secondaire : indicateurs exemptes et taxables presents "
        "simultanement (confiance 0,65)",
        "Bascule IA lorsque la confiance par mots-cles &lt; 0,70",
        "Les factures mixtes bloquent l'approbation automatique et "
        "necessitent une allocation fiscale manuelle",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Les factures a taxe mixte necessitent une attribution du code de "
        "taxe ligne par ligne. Utilisez la vue par poste pour attribuer "
        "les codes T, E ou Z aux lignes individuelles avant l'inscription.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("5.11 Regime fiscal par poste", styles["H2"]))
    story.append(Paragraph(
        "Le moteur de postes extrait les lignes individuelles des factures "
        "et attribue des regimes fiscaux par ligne selon les regles du lieu "
        "de fourniture de la Loi sur la taxe d'accise (LTA, annexe IX).",
        styles["Body"],
    ))
    story.append(sp())
    supply_rows = [
        ["Biens corporels", "Destination de livraison\n(province de l'acheteur)",
         "L'adresse de livraison\ndetermine la taxe"],
        ["Services", "Ou principalement\nexecutes",
         "Lieu du service ou\nprovince de l'acheteur"],
        ["Biens immeubles", "Ou le bien est\nsitue",
         "Toujours la province\ndu bien"],
        ["Biens incorporels", "Province du\ndestinataire (acheteur)",
         "Logiciels, licences,\nabonnements"],
        ["Expedition", "Suit la fourniture\nprincipale ou la destination",
         "Partie d'un contrat\nplus large"],
    ]
    story.append(make_table(
        ["Type de fourniture", "Lieu de fourniture", "Notes"],
        supply_rows,
        col_widths=[1.3 * inch, 2.0 * inch, 2.2 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Le moteur detecte le type de fourniture a partir de mots-cles : "
        "\"expedition\", \"fret\" = expedition ; \"service\", \"main-d'oeuvre\", "
        "\"installation\" = service ; defaut = biens corporels.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.12 Rapprochement de facture", styles["H2"]))
    story.append(Paragraph(
        "Le validateur de rapprochement verifie que les totaux des lignes "
        "calcules correspondent au total de la facture. Seuils de tolerance :",
        styles["Body"],
    ))
    recon_rows = [
        ["&le; 0,02 $", "Correspondance exacte", "Acceptable &mdash; proceder\na l'inscription"],
        ["&le; 1,00 $\n(devise)", "Arrondi de change", "Acceptable pour les factures\nen devise etrangere"],
        ["&le; 1,00 $\n(taxe)", "Ambiguite fiscale", "Signaler pour revision ;\ndetection taxes incluses"],
        ["&le; 5,00 $", "Lignes manquantes", "Verifier l'expedition, la\nmanutention ou les ecofrais"],
        ["&le; 50,00 $", "Majoration fournisseur", "Verifier aupres du fournisseur ;\nfrais admin possibles"],
        ["&gt; 50,00 $", "Irresoluble", "BLOQUER L'INSCRIPTION &mdash;\nrevision manuelle requise"],
    ]
    story.append(make_table(
        ["Ecart", "Classification", "Action"],
        recon_rows,
        col_widths=[1.2 * inch, 1.5 * inch, 2.8 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.13 Exemples de calcul de taxe", styles["H2"]))
    story.append(Paragraph(
        "Les exemples suivants illustrent comment LedgerLink calcule les "
        "taxes pour les scenarios courants :",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Exemple 1 : Achat standard au Quebec (code de taxe T)",
                           styles["H3"]))
    story.append(Paragraph(
        "Fournitures de bureau d'un fournisseur quebecois, montant avant "
        "taxes 100,00 $ :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "TPS = 100,00 $ x 5,000 % = 5,00 $",
        "TVQ = 100,00 $ x 9,975 % = 9,98 $ (en parallele, pas sur le montant de TPS)",
        "Total = 100,00 $ + 5,00 $ + 9,98 $ = 114,98 $",
        "CTI admissible : 5,00 $ (TPS)",
        "RTI admissible : 9,98 $ (TVQ)",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Exemple 2 : Repas d'affaires (code de taxe M)",
                           styles["H3"]))
    story.append(Paragraph(
        "Repas au restaurant pour reunion d'affaires, montant avant taxes "
        "80,00 $ :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "TPS = 80,00 $ x 5,000 % = 4,00 $",
        "TVQ = 80,00 $ x 9,975 % = 7,98 $",
        "Total = 80,00 $ + 4,00 $ + 7,98 $ = 91,98 $",
        "CTI admissible : 4,00 $ x 50 % = 2,00 $ (restriction de 50 %)",
        "RTI admissible : 7,98 $ x 50 % = 3,99 $ (restriction de 50 %)",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Exemple 3 : Achat en Ontario (code de taxe TVH)",
                           styles["H3"]))
    story.append(Paragraph(
        "Equipement informatique d'un fournisseur ontarien, montant avant "
        "taxes 500,00 $ :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "TVH = 500,00 $ x 13,000 % = 65,00 $",
        "Total = 500,00 $ + 65,00 $ = 565,00 $",
        "CTI admissible : 65,00 $ (recuperation totale de la TVH)",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Exemple 4 : Importation des Etats-Unis", styles["H3"]))
    story.append(Paragraph(
        "Machinerie importee des Etats-Unis, valeur en douane 10 000 $ CAD, "
        "droits 500 $, aucune accise :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Base TPS = 10 000 $ + 500 $ = 10 500 $",
        "TPS = 10 500 $ x 5,000 % = 525,00 $ (recuperable comme CTI)",
        "Base TVQ = 10 000 $ + 500 $ + 525 $ = 11 025 $",
        "TVQ = 11 025 $ x 9,975 % = 1 099,74 $ (recuperable comme RTI)",
        "Cout total debarque = 10 000 $ + 500 $ + 525 $ + 1 099,74 $ = 12 124,74 $",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Notez que la TVQ sur les importations est calculee sur la valeur "
        "en douane PLUS les droits PLUS la TPS. Cela differe des achats "
        "domestiques ou la TPS et la TVQ sont calculees en parallele sur "
        "le montant avant taxes.",
        styles,
    ))


# ── Section 6 : Portail client ───────────────────────────────
def build_section_6_client_portal(story, styles):
    """Section 6 — Portail, Cloudflare, QR, WhatsApp/Telegram."""
    story.append(Paragraph("6. Portail client", styles["H1"]))
    story.append(sp())

    _build_section_6_overview(story, styles)
    _build_section_6_credentials(story, styles)
    _build_section_6_submission(story, styles)
    _build_section_6_cloudflare(story, styles)
    _build_section_6_qr(story, styles)
    _build_section_6_messaging(story, styles)
    story.append(PageBreak())


def _build_section_6_overview(story, styles):
    """Apercu du portail."""
    story.append(Paragraph("6.1 Apercu du portail", styles["H2"]))
    story.append(Paragraph(
        "Le portail client fonctionne sur le port 8788 et offre une interface "
        "de telechargement simple pour que les clients soumettent leurs "
        "documents. Les clients n'ont pas besoin d'installer LedgerLink "
        "&mdash; ils accedent au portail via leur navigateur.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Formats pris en charge : PDF, JPEG, PNG, TIFF, WebP, HEIC",
        "Taille maximale de telechargement : 20 Mo (configurable)",
        "Les documents sont automatiquement achemines vers la file de revision du client",
        "Les clients peuvent consulter leur historique de soumission",
    ], styles))
    story.append(sp())


def _build_section_6_credentials(story, styles):
    """Creation des identifiants du portail."""
    story.append(Paragraph("6.2 Comptes clients", styles["H2"]))
    story.append(Paragraph(
        "Les comptes clients sont crees lors de l'assistant de configuration "
        "(etape 17) ou via la page Clients du tableau de bord. Chaque "
        "client recoit :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Code client unique (jusqu'a 10 caracteres majuscules)",
        "Adresse courriel de contact",
        "Langue preferee (francais ou anglais)",
        "Frequence de production (mensuelle, trimestrielle ou annuelle)",
        "Comptable assigne pour la gestion du portefeuille",
    ], styles))
    story.append(sp())


def _build_section_6_submission(story, styles):
    """Methodes de soumission de documents."""
    story.append(Paragraph("6.3 Methodes de soumission de documents", styles["H2"]))
    rows = [
        ["Telechargement\nnavigateur", "Naviguez vers l'URL du portail,\n"
         "connectez-vous, glissez-deposez les fichiers",
         "Le plus courant ; aucune\nconfiguration requise"],
        ["WhatsApp", "Envoyez photos/PDF au numero\nTwilio du cabinet",
         "Pratique pour mobile ;\nnecessite la configuration Twilio"],
        ["Telegram", "Envoyez des documents au bot\nTelegram du cabinet",
         "Messagerie gratuite ;\nnecessite la creation du bot"],
        ["Courriel", "Transferez les factures a l'adresse\ncourriel de reception",
         "Fonctionne avec tout client\nde messagerie ; port 8789"],
        ["Depot de fichier", "Deposez les fichiers dans le dossier\n"
         "OneDrive/Dropbox partage",
         "Ideal pour la numerisation\nen lot ; prise en charge auto"],
    ]
    story.append(make_table(
        ["Methode", "Comment faire", "Notes"],
        rows,
        col_widths=[1.2 * inch, 3.0 * inch, 2.0 * inch],
    ))
    story.append(sp())


def _build_section_6_cloudflare(story, styles):
    """Configuration du tunnel Cloudflare."""
    story.append(Paragraph("6.4 Tunnel Cloudflare (acces a distance)", styles["H2"]))
    story.append(Paragraph(
        "Pour rendre le portail client accessible sur Internet (pas "
        "seulement le reseau local), configurez un tunnel Cloudflare lors "
        "de l'assistant de configuration (etape 6).",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Creez un compte Cloudflare gratuit sur cloudflare.com",
        "Ajoutez votre domaine au DNS Cloudflare",
        "Installez cloudflared sur le serveur LedgerLink",
        "Creez un tunnel : <font face='Courier'>cloudflared tunnel create "
        "ledgerlink</font>",
        "Configurez le tunnel pour pointer vers localhost:8788",
        "Demarrez le tunnel : <font face='Courier'>cloudflared tunnel run "
        "ledgerlink</font>",
        "Mettez a jour le DNS pour pointer votre sous-domaine vers le tunnel",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Les tunnels Cloudflare sont gratuits et fournissent le chiffrement "
        "HTTPS, la protection DDoS et le CDN mondial. Aucune redirection "
        "de port ni adresse IP statique n'est requise. Lorsque HTTPS est "
        "actif, LedgerLink active automatiquement les temoins securises.",
        styles,
    ))
    story.append(sp())


def _build_section_6_qr(story, styles):
    """Codes QR."""
    story.append(Paragraph("6.5 Codes QR", styles["H2"]))
    story.append(Paragraph(
        "Generez des codes QR pour chaque client menant directement a leur "
        "page de telechargement. Naviguez vers <b>QR</b> dans la barre "
        "laterale.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Telechargez les codes QR individuels des clients en images PNG",
        "Telechargez tous les codes QR des clients en PDF multi-pages",
        "Imprimez et remettez aux clients pour un acces facile au portail",
        "Les codes QR encodent l'URL du portail avec le code client prerempli",
    ], styles))
    story.append(sp())


def _build_section_6_messaging(story, styles):
    """WhatsApp/Telegram via OpenClaw."""
    story.append(Paragraph("6.6 WhatsApp et Telegram via OpenClaw", styles["H2"]))
    story.append(Paragraph(
        "Les messages WhatsApp et Telegram sont traites via le pont OpenClaw. "
        "Le pont accepte les charges utiles JSON a "
        "<font face='Courier'>/ingest/openclaw</font> (aucune authentification "
        "requise pour le point de terminaison de reception).",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "WhatsApp utilise Twilio comme fournisseur de messagerie (~0,005 $/message)",
        "Telegram utilise un bot personnalise cree via @BotFather (gratuit)",
        "Les messages sont achemines vers les codes clients par numero de "
        "telephone de l'expediteur ou ID utilisateur Telegram",
        "Les expediteurs inconnus recoivent HTTP 404 (expediteur_inconnu)",
        "La reception reussie retourne document_id et etat",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Configurez WhatsApp a l'etape 7 de l'assistant et Telegram a l'etape 8.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("6.7 Integration Microsoft 365", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink s'integre a Microsoft 365 pour l'automatisation du "
        "courriel et du calendrier. Configurez a l'etape 9 de l'assistant.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Fonctionnalites :", styles["BodyBold"]))
    story.extend(bullet_list([
        "<b>Lecture automatique des courriels :</b> Le compte de service "
        "surveille une boite aux lettres partagee pour les factures entrantes "
        "et ingere automatiquement les pieces jointes.",
        "<b>Traitement des factures :</b> Les pieces jointes PDF et images "
        "sont extraites et traitees via le pipeline OCR standard.",
        "<b>Synchronisation du calendrier Outlook :</b> Les echeances de "
        "production et les dates de fermeture de periode sont synchronisees "
        "avec un calendrier Outlook partage.",
        "<b>Resume Teams :</b> Resume quotidien publie dans un canal Teams "
        "avec l'etat de la file, les alertes de fraude et les echeances "
        "a venir.",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Configuration Azure AD :", styles["BodyBold"]))
    story.extend(numbered_list([
        "Allez dans Azure Active Directory &gt; Inscriptions d'applications",
        "Creez une nouvelle inscription pour LedgerLink",
        "Accordez les permissions Mail.Read, Mail.ReadWrite et Calendars.ReadWrite",
        "Creez un secret client (note : les secrets expirent ; definissez un rappel)",
        "Saisissez l'ID du locataire, l'ID client et le secret client a l'etape 9",
        "Testez la connectivite depuis l'assistant de configuration",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Le compte de service Microsoft 365 devrait etre une boite aux "
        "lettres dediee (ex. factures@votrecabinet.com), et non un compte "
        "personnel. Cela assure un fonctionnement continu et evite les "
        "problemes d'authentification multifacteur.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("6.8 Integration QuickBooks en ligne", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink inscrit les transactions approuvees dans QuickBooks en "
        "ligne. Configurez a l'etape 10 de l'assistant.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Capacites :", styles["BodyBold"]))
    story.extend(bullet_list([
        "<b>Inscrire les transactions :</b> Les documents approuves creent "
        "des factures a payer, des depenses ou des ecritures de journal dans QBO.",
        "<b>Synchroniser les fournisseurs :</b> Les donnees maitresses des "
        "fournisseurs sont synchronisees entre LedgerLink et QBO.",
        "<b>Mettre a jour les comptes :</b> Le plan comptable est synchronise "
        "depuis QBO pour un appariement precis au grand livre.",
        "<b>Verifier les inscriptions :</b> L'outil de verification QBO "
        "confirme que les transactions inscrites apparaissent correctement.",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Configuration :", styles["BodyBold"]))
    story.extend(numbered_list([
        "Allez sur developer.intuit.com et creez une application",
        "Definissez l'URI de redirection vers votre URL LedgerLink",
        "Notez l'ID de domaine (ID d'entreprise) depuis QBO",
        "Saisissez l'ID de domaine, l'ID client et le secret client a l'etape 10",
        "Autorisez la connexion via le flux OAuth",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Les jetons QBO expirent periodiquement. LedgerLink rafraichit "
        "automatiquement les jetons, mais si l'inscription echoue avec une "
        "erreur d'authentification, reauthorisez depuis l'assistant.",
        styles,
    ))


# ── Section 7 : Fermeture de periode et facturation ──────────
def build_section_7_monthend(story, styles):
    """Section 7 — Fermeture de periode, suivi du temps, facturation."""
    story.append(Paragraph("7. Fermeture de periode et facturation", styles["H1"]))
    story.append(sp())

    _build_section_7_checklist(story, styles)
    _build_section_7_period_lock(story, styles)
    _build_section_7_time_tracking(story, styles)
    _build_section_7_invoicing(story, styles)
    _build_section_7_reconciliation(story, styles)
    story.append(PageBreak())


def _build_section_7_checklist(story, styles):
    """Liste de controle de fermeture de periode."""
    story.append(Paragraph("7.1 Liste de controle de fermeture de periode", styles["H2"]))
    story.append(Paragraph(
        "Naviguez vers <b>Fermeture de periode</b> dans la barre laterale "
        "(Gestionnaire/Proprietaire). La liste de controle suit toutes les "
        "taches de fin de mois :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Reviser tous les documents EnRevision pour la periode",
        "Completer le rapprochement bancaire",
        "Verifier les montants de production TPS/TVQ",
        "Verifier les elements de paie en suspens",
        "Reviser et inscrire les ecritures de journal manuelles",
        "Verifier que toutes les affectations de documents sont completees",
        "Generer le rapport PDF de fermeture de periode",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Chaque element de la liste de controle a une affectation "
        "d'utilisateur responsable et une date d'echeance. Marquez les "
        "elements comme ouverts ou fermes au fur et a mesure que vous "
        "progressez dans la fin de mois.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_7_period_lock(story, styles):
    """Verrouillage de periode."""
    story.append(Paragraph("7.2 Verrouillage des periodes", styles["H2"]))
    story.append(Paragraph(
        "Une fois la fin de mois completee, verrouillez la periode pour "
        "empecher toute modification supplementaire des documents. Les "
        "periodes verrouillees :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Empechent les modifications de documents pour la plage de dates verrouillees",
        "Bloquent les nouvelles inscriptions pour la periode verrouillees",
        "Declenchent un instantane de production (moteur de modifications) "
        "pour preserver l'etat au moment de la production",
        "Toute correction aux periodes verrouillees leve des indicateurs "
        "de modification (piege 1) plutot que de modifier la production originale",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Le verrouillage de periode est irreversible via l'interface. Une "
        "fois verrouillees, les corrections doivent passer par le flux de "
        "modification. Cela assure la preservation de la piste d'audit.",
        styles,
    ))
    story.append(sp())


def _build_section_7_time_tracking(story, styles):
    """Suivi du temps."""
    story.append(Paragraph("7.3 Suivi du temps", styles["H2"]))
    story.append(Paragraph(
        "Suivez les heures facturables par document ou par client via la "
        "fonctionnalite de suivi du temps (Gestionnaire/Proprietaire).",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Demarrer le chronometre :</b> POST /time/start avec document_id "
        "ou client_code (optionnel)",
        "<b>Arreter le chronometre :</b> POST /time/stop avec entry_id. "
        "Retourne la duree en minutes.",
        "Les entrees de temps sont liees aux documents et codes clients",
        "Utilisees pour la generation de factures en fin de mois",
    ], styles))
    story.append(sp())


def _build_section_7_invoicing(story, styles):
    """Generation de factures."""
    story.append(Paragraph("7.4 Generation de factures", styles["H2"]))
    story.append(Paragraph(
        "Generez des factures professionnelles a partir des entrees de temps. "
        "Naviguez vers <b>Facture</b> dans la barre laterale.",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Selectionnez le code client et la periode de facturation (dates de debut/fin)",
        "Saisissez le taux horaire, le nom du cabinet, le nom du client",
        "Saisissez les numeros de TPS et de TVQ pour le calcul de taxe",
        "Le systeme calcule : heures facturables, sous-total, TPS (5 %), "
        "TVQ (9,975 %), total",
        "Generez et telechargez la facture PDF",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Convention de nommage des factures : <font face='Courier'>"
        "facture_{client}_{debut}_{fin}_{numero}.pdf</font>",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_7_reconciliation(story, styles):
    """Rapprochement bancaire."""
    story.append(Paragraph("7.5 Rapprochement bancaire", styles["H2"]))
    story.append(Paragraph(
        "Le module de rapprochement bancaire offre un rapprochement a "
        "deux cotes. Naviguez vers <b>Rapprochement</b> dans la barre "
        "laterale.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("7.5.1 Creation d'un rapprochement", styles["H3"]))
    story.extend(numbered_list([
        "Cliquez sur <b>Nouveau rapprochement</b>",
        "Saisissez : code client, nom du compte, numero de compte",
        "Saisissez : date de fin de periode, solde du releve, solde du grand livre",
        "Le systeme prerempli automatiquement les elements en suspens a "
        "partir des documents non apparies",
    ], styles))
    story.append(sp())

    story.append(Paragraph("7.5.2 Elements de rapprochement", styles["H3"]))
    rows = [
        ["Depots en transit", "Montants au grand livre mais pas\n"
         "encore sur le releve bancaire", "Ajoutes au solde\ndu releve"],
        ["Cheques en circulation", "Montants sur le releve mais pas\n"
         "encore au grand livre", "Soustraits du solde\ndu releve"],
        ["Erreurs bancaires", "Erreurs sur le releve bancaire", "Ajuster le solde\ndu releve"],
        ["Erreurs comptables", "Erreurs dans les registres du\ngrand livre",
         "Ajuster le solde\ndu grand livre"],
    ]
    story.append(make_table(
        ["Type d'element", "Description", "Effet"],
        rows,
        col_widths=[1.5 * inch, 2.5 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("7.5.3 Formule de rapprochement", styles["H3"]))
    story.append(Paragraph(
        "Solde bancaire ajuste = Solde du releve + Depots en transit "
        "- Cheques en circulation +/- Erreurs bancaires",
        styles["Code"],
    ))
    story.append(Paragraph(
        "Solde comptable ajuste = Solde du grand livre +/- Erreurs comptables",
        styles["Code"],
    ))
    story.append(Paragraph(
        "Les deux soldes ajustes doivent correspondre a 0,01 $ pres pour "
        "finaliser. Les rapprochements finalises sont immuables et proteges "
        "par des declencheurs de base de donnees.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Telechargez le rapport de rapprochement en PDF bilingue depuis la "
        "page de detail.",
        styles["Body"],
    ))


# ── Section 8 : Module de mission CPA ────────────────────────
def build_section_8_audit(story, styles):
    """Section 8 — Module complet de mission CPA selon les NCA."""
    story.append(Paragraph("8. Module de mission CPA", styles["H1"]))
    story.append(sp())
    story.append(Paragraph(
        "Le module de mission est disponible pour les detenteurs de licence "
        "de niveau Entreprise et fournit un soutien complet aux missions CPA "
        "selon les normes canadiennes d'audit (NCA). Toutes les "
        "fonctionnalites sont accessibles aux roles Proprietaire et "
        "Gestionnaire.",
        styles["Body"],
    ))
    story.append(sp())

    _build_section_8_engagements(story, styles)
    _build_section_8_working_papers(story, styles)
    _build_section_8_materiality(story, styles)
    _build_section_8_risk(story, styles)
    _build_section_8_controls(story, styles)
    _build_section_8_sampling(story, styles)
    _build_section_8_going_concern(story, styles)
    _build_section_8_subsequent(story, styles)
    _build_section_8_rep_letter(story, styles)
    _build_section_8_related_parties(story, styles)
    _build_section_8_opinion(story, styles)
    _build_section_8_assertions(story, styles)
    _build_section_8_quality(story, styles)
    story.append(PageBreak())


def _build_section_8_engagements(story, styles):
    """Types de missions et gestion."""
    story.append(Paragraph("8.1 Types de missions", styles["H2"]))
    rows = [
        ["Mission de\nverification", "Mission d'audit complete selon les NCA.\n"
         "Fournit une assurance raisonnable.", "NCA 200-810"],
        ["Mission d'examen", "Mission d'assurance limitee.\n"
         "Demandes d'informations et procedures\nanalytiques.", "NCME 2400"],
        ["Mission de\ncompilation", "Aucune assurance. Preparation des\n"
         "etats financiers uniquement.", "NCSC 4200"],
    ]
    story.append(make_table(
        ["Type", "Description", "Normes"],
        rows,
        col_widths=[1.2 * inch, 3.3 * inch, 1.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Creez des missions depuis <b>Missions</b> dans la barre laterale. "
        "Chaque mission suit : code client, periode, type, associe, "
        "gestionnaire, personnel, heures prevues, budget et honoraires. "
        "L'evaluation de la continuite d'exploitation s'execute "
        "automatiquement a la creation.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_working_papers(story, styles):
    """Dossiers de travail."""
    story.append(Paragraph("8.2 Dossiers de travail (documentation NCA)", styles["H2"]))
    story.append(Paragraph(
        "Les dossiers de travail documentent les elements probants recueillis "
        "et les conclusions tirees lors de la mission. Naviguez vers "
        "<b>Dossiers de travail</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Creation automatique des dossiers de travail a partir du plan "
        "comptable (200+ comptes standard du Quebec)",
        "Chaque dossier suit : solde selon les livres, solde confirme, "
        "prepare par, revise par, date d'approbation",
        "Ajouter des elements avec des pointages : teste, confirme, "
        "exception, non_applicable",
        "Lier des documents comme elements probants aux postes du dossier de travail",
        "Telecharger des feuilles sommaires PDF avec en-tetes bilingues "
        "et mise en evidence des exceptions",
        "Verification automatique de l'importance relative sur les comptes significatifs",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Les dossiers de travail approuves sont immuables (exigence P0-2). "
        "Une fois approuves, aucune modification n'est permise. Cela est "
        "applique par des declencheurs de base de donnees SQLite. Les "
        "approbations doivent etre effectuees dans les 24 heures suivant "
        "la derniere modification.",
        styles,
    ))
    story.append(sp())


def _build_section_8_materiality(story, styles):
    """Importance relative (NCA 320)."""
    story.append(Paragraph("8.3 Importance relative (NCA 320)", styles["H2"]))
    story.append(Paragraph(
        "Calculez et documentez l'importance relative pour la mission.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Benefice avant impots", "5 %", "Base la plus courante pour\n"
         "les entites a but lucratif"],
        ["Total de l'actif", "0,5 %", "Utile pour les entites\n"
         "a forte intensite d'actifs\n(immobilier)"],
        ["Produits", "2 %", "Utilise pour les organismes\nsans but lucratif ou\n"
         "les entites en demarrage"],
    ]
    story.append(make_table(
        ["Base", "Taux", "Quand utiliser"],
        rows,
        col_widths=[1.5 * inch, 0.8 * inch, 3.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "<b>Importance relative pour les travaux</b> = 75 % de l'importance "
        "relative de planification. Ce seuil inferieur est utilise pour les "
        "tests de comptes individuels.",
        styles["Body"],
    ))
    story.append(Paragraph(
        "<b>Seuil de signification clairement negligeable</b> = 5 % de "
        "l'importance relative de planification. Les anomalies inferieures "
        "a ce montant sont accumulees mais ne font pas l'objet d'une "
        "investigation individuelle.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_risk(story, styles):
    """Evaluation des risques (NCA 315)."""
    story.append(Paragraph("8.4 Evaluation des risques (NCA 315)", styles["H2"]))
    story.append(Paragraph(
        "La matrice de risques evalue le risque inherent et le risque lie "
        "aux controles au niveau des assertions pour chaque compte significatif.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("Assertions evaluees :", styles["BodyBold"]))
    story.extend(bullet_list([
        "<b>Exhaustivite</b> &mdash; Toutes les transactions enregistrees",
        "<b>Exactitude</b> &mdash; Montants correctement presentes",
        "<b>Existence</b> &mdash; Les actifs/passifs existent en fin de periode",
        "<b>Coupure</b> &mdash; Les transactions dans la bonne periode",
        "<b>Classification</b> &mdash; Compte du grand livre correct",
        "<b>Droits et obligations</b> &mdash; L'entite a des droits sur les actifs",
        "<b>Presentation</b> &mdash; Divulgation et classification appropriees",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "<b>Risque significatif :</b> Signale lorsque le risque inherent est "
        "Eleve ET que le risque lie aux controles est Moyen ou Eleve. Les "
        "risques significatifs necessitent des procedures d'audit speciales.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_controls(story, styles):
    """Tests des controles (NCA 330)."""
    story.append(Paragraph("8.5 Tests des controles (NCA 330)", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink comprend une bibliotheque de 15 controles standard pour "
        "les cabinets CPA du Quebec :",
        styles["Body"],
    ))
    story.append(sp())
    control_rows = [
        ["1", "Autorisation des CP", "Approbation des factures avant paiement"],
        ["2", "Rapprochement bancaire", "Appariement mensuel du releve bancaire"],
        ["3", "Autorisation de la paie", "Embauches et modifications de taux"],
        ["4", "Exhaustivite des produits", "Toutes les ventes enregistrees"],
        ["5", "Inventaire physique", "Denombrement annuel et rapprochement"],
        ["6", "Controles d'acces", "Revue des connexions et permissions systeme"],
        ["7", "Approbation des EJ", "Revue et autorisation des EJM"],
        ["8", "Modifications fournisseurs", "Approbation des nouveaux fournisseurs et modifications"],
        ["9", "Ajouts d'immobilisations", "Approbation des investissements et capitalisation"],
        ["10", "Rapprochement cartes\nde credit", "Appariement mensuel des releves"],
        ["11", "Remise TPS/TVQ", "Verification de la production et du paiement"],
        ["12", "Rapprochement RL-1/T4", "Exactitude des feuillets de paie"],
        ["13", "Autorisation bancaire", "Revue des signataires autorises"],
        ["14", "Petite caisse", "Denombrement et rapprochement"],
        ["15", "Conservation des\ndocuments", "Conformite a la retention de 7 ans"],
    ]
    story.append(make_table(
        ["No", "Controle", "Objectif"],
        control_rows,
        col_widths=[0.4 * inch, 2.2 * inch, 3.4 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Types de tests : cheminement, re-execution, observation, demande "
        "d'informations. Conclusions : efficace, inefficace, "
        "partiellement_efficace. Suivi de la taille de l'echantillon et des "
        "exceptions trouvees.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_sampling(story, styles):
    """Sondage statistique (NCA 530)."""
    story.append(Paragraph("8.6 Sondage statistique (NCA 530)", styles["H2"]))
    story.append(Paragraph(
        "L'outil de sondage selectionne les elements a tester en utilisant "
        "un echantillonnage aleatoire reproductible (semence basee sur l'ID "
        "du dossier de travail). Naviguez vers <b>Sondage</b> dans la barre "
        "laterale de mission.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Specifiez le client, la periode, le code de compte et la taille "
        "de l'echantillon",
        "Les elements sont selectionnes aleatoirement dans la population",
        "Marquez chaque element : teste, exception, non_applicable",
        "Les resultats du sondage sont lies aux dossiers de travail",
        "Resultats reproductibles (meme semence = meme echantillon)",
    ], styles))
    story.append(sp())


def _build_section_8_going_concern(story, styles):
    """Continuite d'exploitation (NCA 570)."""
    story.append(Paragraph("8.7 Continuite d'exploitation (NCA 570)", styles["H2"]))
    story.append(Paragraph(
        "L'evaluation de la continuite d'exploitation s'execute "
        "automatiquement lorsqu'une mission est creee ou mise a jour. Le "
        "systeme detecte des indicateurs tels que :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Pertes d'exploitation recurrentes",
        "Fonds de roulement negatif",
        "Incapacite de payer les creanciers a temps",
        "Perte de clients ou de fournisseurs cles",
        "Evenements juridiques ou reglementaires menacant la viabilite",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Les resultats sont stockes dans la table going_concern_assessments "
        "et lies a la mission. Les preoccupations signalees declenchent des "
        "exigences de divulgation supplementaires.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_subsequent(story, styles):
    """Evenements posterieurs (NCA 560)."""
    story.append(Paragraph("8.8 Evenements posterieurs (NCA 560)", styles["H2"]))
    story.append(Paragraph(
        "Le moteur de modifications suit les evenements entre la fin de la "
        "periode et la date du rapport. Lorsqu'une correction est apportee "
        "a une periode produite :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "L'instantane de production original est preserve (jamais reecrit)",
        "Un indicateur de modification est leve pour la periode produite",
        "Les corrections sont dirigees vers la periode de correction",
        "La chronologie complete des modifications est disponible pour la "
        "revue de mission",
        "Les requetes \"ce qui etait cru au moment T\" soutiennent "
        "l'analyse des evenements",
    ], styles))
    story.append(sp())


def _build_section_8_rep_letter(story, styles):
    """Lettre de declaration de la direction (NCA 580)."""
    story.append(Paragraph("8.9 Lettre de declaration de la direction (NCA 580)",
                           styles["H2"]))
    story.append(Paragraph(
        "Generez des lettres de declaration de la direction bilingues "
        "(FR/EN). Fonctionnalite reservee au Proprietaire, accessible "
        "depuis la barre laterale de mission.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Les declarations standard comprennent :",
                           styles["BodyBold"]))
    story.extend(numbered_list([
        "Les etats financiers sont presentes fidelement selon les normes applicables",
        "Toutes les transactions ont ete enregistrees",
        "Les informations sur les parties liees sont completes",
        "Les evenements posterieurs a la date du bilan ont ete divulgues",
        "Toute fraude impliquant la direction ou les employes-cles a ete divulguee",
        "Les proces-verbaux et les accords significatifs sont complets",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "L'etat de la lettre progresse : brouillon &rarr; signee &rarr; "
        "refusee. Suivi du nom, du titre et de la date de signature de "
        "la direction.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_related_parties(story, styles):
    """Parties liees (NCA 550)."""
    story.append(Paragraph("8.10 Parties liees (NCA 550)", styles["H2"]))
    story.append(Paragraph(
        "Identifiez et suivez les parties liees et leurs transactions.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Proprietaire", "Actionnaire individuel ou proprietaire unique"],
        ["Membre de la famille", "Conjoint, enfants ou proches parents du proprietaire"],
        ["Societe affiliee", "Entite sous controle ou propriete commune"],
        ["Direction cle", "PDG, chef des finances ou autres cadres superieurs"],
        ["Membre du CA", "Membre du conseil d'administration"],
    ]
    story.append(make_table(
        ["Type de relation", "Description"],
        rows,
        col_widths=[2.0 * inch, 4.0 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Pour chaque transaction entre parties liees, consignez : le montant, "
        "la base d'evaluation (valeur_comptable, valeur_d_echange, cout) et "
        "si la divulgation est requise. Generez le texte de divulgation pour "
        "les notes aux etats financiers.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_opinion(story, styles):
    """Rapport du verificateur (NCA 700)."""
    story.append(Paragraph("8.11 Rapport du verificateur (NCA 700)", styles["H2"]))
    story.append(Paragraph(
        "Emettez le rapport de mission depuis la page de detail de la "
        "mission. Le systeme effectue des verifications pre-emission :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Tous les elements bloquants de la liste de controle doivent etre resolus",
        "Les dossiers de travail doivent etre approuves",
        "L'importance relative doit etre documentee",
        "La matrice de risques doit etre complete",
        "La lettre de declaration de la direction doit etre signee",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Le rapport PDF est genere avec les details de la mission, la "
        "periode, les affectations d'equipe et les constatations. L'etat "
        "passe a complete.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_assertions(story, styles):
    """Couverture des assertions (NCA 500)."""
    story.append(Paragraph("8.12 Couverture des assertions (NCA 500)", styles["H2"]))
    story.append(Paragraph(
        "Les dossiers de travail suivent la couverture des assertions au "
        "niveau des comptes. Les sept assertions (exhaustivite, exactitude, "
        "existence, coupure, classification, droits et obligations, "
        "presentation) sont liees aux postes du dossier de travail via "
        "l'action Enregistrer les assertions.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph(
        "La vue de couverture des assertions montre quelles assertions ont "
        "ete testees pour chaque compte significatif, aidant a assurer une "
        "couverture d'audit complete.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_quality(story, styles):
    """Controle qualite (NCCQ 1)."""
    story.append(Paragraph("8.13 Controle qualite (NCCQ 1)", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink soutient le controle qualite a travers plusieurs "
        "mecanismes alignes sur les exigences de la NCCQ 1 :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Equipe de mission :</b> Affectations d'associe, de gestionnaire "
        "et de personnel avec separation des taches",
        "<b>Revue des dossiers de travail :</b> Des champs separes "
        "prepare_par et revise_par imposent une revue par une seconde personne",
        "<b>Immutabilite :</b> Les dossiers approuves ne peuvent etre "
        "modifies, preservant la piste d'audit",
        "<b>Liaison des elements probants :</b> L'appariement a trois voies "
        "(BC, facture, paiement) assure des chaines de preuves completes",
        "<b>Heures prevues vs reelles :</b> Suivi du budget au niveau de "
        "la mission",
        "<b>Retention de 7 ans :</b> Controle de conservation des documents "
        "dans la bibliotheque de controles standards",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Ces fonctionnalites soutiennent les politiques de controle qualite "
        "du cabinet telles qu'exigees par la NCCQ 1 et les normes CPA "
        "provinciales.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("8.14 Etats financiers", styles["H2"]))
    story.append(Paragraph(
        "Generez des etats financiers a partir des documents inscrits. "
        "Naviguez vers <b>Etats financiers</b> dans la barre laterale "
        "de mission.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Bilan :</b> Actif (1000-1999), Passif (2000-2999), "
        "Capitaux propres (3000-3999) selon le plan comptable du Quebec",
        "<b>Etat des resultats :</b> Produits (4000-4999), Charges (5000-5999)",
        "<b>Balance de verification :</b> Tous les comptes GL avec totaux "
        "debit/credit",
        "Les etats sont generes a partir de la balance de verification, qui "
        "totalise les documents inscrits par compte GL",
        "Telecharger en PDF avec en-tetes bilingues",
    ], styles))
    story.append(sp())

    story.append(Paragraph("8.15 Plan comptable", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink comprend un plan comptable precharge de 200+ comptes "
        "suivant le plan comptable standard du Quebec. Plages de comptes "
        "principales :",
        styles["Body"],
    ))
    story.append(sp())
    coa_rows = [
        ["1000-1099", "Encaisse et equivalents", "Comptes bancaires, petite caisse"],
        ["1100-1199", "Comptes clients", "Debiteurs, provisions"],
        ["1200-1299", "Stocks", "Marchandises, travaux en cours"],
        ["1300-1399", "Charges payees d'avance", "Assurance, loyer, abonnements"],
        ["1400-1499", "Depots", "Depots de garantie, depots de services publics"],
        ["1500-1999", "Immobilisations", "Equipement, vehicules, batiments, ameliorations locatives"],
        ["2000-2099", "Comptes fournisseurs", "Crediteurs commerciaux"],
        ["2100-2199", "Charges a payer", "Salaires, interets, services publics"],
        ["2200-2299", "Passifs fiscaux", "TPS, TVQ, retenues a la source"],
        ["2300-2499", "Emprunts courants", "Marge de credit, portion courante"],
        ["2500-2999", "Passifs a long terme", "Hypotheques, prets a terme, contrats de location"],
        ["3000-3999", "Capitaux propres", "Capital, benefices non repartis, retraits"],
        ["4000-4999", "Produits", "Ventes, revenus de services, autres produits"],
        ["5000-5999", "Charges", "Charges d'exploitation par categorie"],
    ]
    story.append(make_table(
        ["Plage", "Categorie", "Exemples"],
        coa_rows,
        col_widths=[1.0 * inch, 2.0 * inch, 3.0 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Le plan comptable comprend egalement les correspondances de lignes "
        "T2 de l'ARC et les correspondances de lignes de depenses CO-17 de "
        "Revenu Quebec pour la preparation des declarations fiscales.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("8.16 Appariement a trois voies (elements probants)",
                           styles["H2"]))
    story.append(Paragraph(
        "Le systeme d'elements probants de mission prend en charge "
        "l'appariement a trois voies pour verifier l'exhaustivite des "
        "transactions :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Bon de commande (BC) :</b> Autorisation d'achat",
        "<b>Facture :</b> Facture du fournisseur pour les biens ou services",
        "<b>Paiement :</b> Transaction bancaire ou cheque compensant la facture",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Le systeme suit l'etat de l'appariement : manquant (aucun lien), "
        "partiel (1-2 documents lies), complet (les trois apparies). Une "
        "tolerance de montant est appliquee &mdash; le montant du BC doit "
        "correspondre approximativement au montant de la facture, qui doit "
        "correspondre approximativement au montant du paiement.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("8.17 Chaine de corrections et suivi des modifications",
                           styles["H2"]))
    story.append(Paragraph(
        "LedgerLink maintient un graphe de chaine de corrections qui suit "
        "toutes les corrections de documents, les decompositions de notes "
        "de credit et les regroupements de doublons. Cela garantit qu'un "
        "evenement economique produit exactement une correction.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Protections cles contre les pieges :", styles["BodyBold"]))
    trap_rows = [
        ["Piege 1", "Modification de\nperiode produite",
         "Les corrections aux periodes produites\nlevent des indicateurs de modification.\n"
         "La production originale est preservee\n(jamais reecrite)."],
        ["Piege 2", "Decomposition de\nnote de credit",
         "Les notes de credit sont decomposees\navec niveaux de confiance : explicite\n"
         "(0,95), lie (0,80), non lie (0,45)."],
        ["Piege 3", "Anomalie de\nchevauchement",
         "Detecte quand un nouveau fournisseur\nchevauche le champ de travail du\n"
         "fournisseur original. Signale pour revision."],
        ["Piege 5", "Regroupement de\ndoublons",
         "Regroupe 3+ variantes du meme\ndocument en un seul regroupement.\n"
         "Empeche les corrections a n voies."],
        ["Piege 6", "Detection de\nversion obsolete",
         "Le verrouillage optimiste rejette les\napprobations sur des versions obsoletes\n"
         "de documents. Force le rafraichissement."],
        ["Piege 7", "Collision d'ecriture\nde journal manuelle",
         "Detecte quand une EJM entre en conflit\navec une correction automatisee.\n"
         "Met l'ecriture en quarantaine."],
        ["Piege 8", "Protection contre\nla restauration",
         "Restauration explicite avec audit et\nblocage de reimportation jusqu'a ce\n"
         "qu'il soit securitaire de retraiter."],
    ]
    story.append(make_table(
        ["Piege", "Nom", "Protection"],
        trap_rows,
        col_widths=[0.7 * inch, 1.3 * inch, 3.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "La chaine de corrections complete est parcourable du document "
        "racine a la feuille, fournissant une lignee d'audit complete "
        "pour toute transaction.",
        styles["Body"],
    ))


# ── Section 9 : Administration ───────────────────────────────
def build_section_9_administration(story, styles):
    """Section 9 — Licence, outils d'administration, sauvegardes."""
    story.append(Paragraph("9. Administration", styles["H1"]))
    story.append(sp())

    _build_section_9_license(story, styles)
    _build_section_9_troubleshoot(story, styles)
    _build_section_9_autofix(story, styles)
    _build_section_9_backups(story, styles)
    _build_section_9_vendor_memory(story, styles)
    _build_section_9_cache(story, styles)
    _build_section_9_updates(story, styles)
    _build_section_9_remote(story, styles)
    story.append(PageBreak())


def _build_section_9_license(story, styles):
    """Niveaux de licence."""
    story.append(Paragraph("9.1 Niveaux de licence", styles["H2"]))
    story.append(Paragraph(
        "Les licences LedgerLink sont signees avec HMAC-SHA256 et encodees "
        "au format de cle LLAI-. Quatre niveaux sont disponibles :",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Essentiel", "10", "3",
         "Revision de base, inscription de base"],
        ["Professionnel", "30", "5",
         "Routeur IA, analyseur bancaire,\ndetection de fraude, Revenu Quebec,\n"
         "suivi du temps, fin de mois"],
        ["Cabinet", "75", "15",
         "Analytique, Microsoft 365,\ncalendrier de production,\n"
         "communications clients"],
        ["Entreprise", "Illimite", "Illimite",
         "Module de mission, etats financiers,\nsondage, acces API"],
    ]
    story.append(make_table(
        ["Niveau", "Max clients", "Max utilisateurs", "Fonctionnalites"],
        rows,
        col_widths=[1.2 * inch, 0.9 * inch, 0.9 * inch, 3.0 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Consultez l'etat de votre licence, la date d'expiration et les "
        "activations de machines depuis <b>Licence</b> dans la barre "
        "laterale d'administration (Proprietaire uniquement).",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_9_troubleshoot(story, styles):
    """Page de diagnostic."""
    story.append(Paragraph("9.2 Diagnostics systeme", styles["H2"]))
    story.append(Paragraph(
        "La page de depannage (<b>Depannage</b> dans la barre laterale "
        "d'administration) affiche l'etat du systeme en temps reel :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Etat de la surveillance de dossier (en cours/arrete)",
        "Etat du pont OpenClaw (connecte/deconnecte)",
        "Etat du tunnel Cloudflare (actif/inactif)",
        "Chemin et taille du fichier de la base de donnees",
        "Temps de fonctionnement du service et liaisons de port",
        "Verification de connectivite des fournisseurs IA",
    ], styles))
    story.append(sp())


def _build_section_9_autofix(story, styles):
    """Script de reparation automatique."""
    story.append(Paragraph("9.3 Script de reparation automatique", styles["H2"]))
    story.append(Paragraph(
        "Le script de reparation automatique detecte et repare "
        "automatiquement les problemes courants de base de donnees. "
        "Executez-le depuis la page de depannage ou en ligne de commande : "
        "<font face='Courier'>python scripts/autofix.py</font>",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Repare les colonnes de base de donnees manquantes (migrations additives)",
        "Corrige les jetons de session corrompus",
        "Recree les index de base de donnees manquants",
        "Valide l'integrite des cles etrangeres",
        "Rapporte les problemes trouves et les actions prises",
    ], styles))
    story.append(sp())


def _build_section_9_backups(story, styles):
    """Gestion des sauvegardes."""
    story.append(Paragraph("9.4 Sauvegardes", styles["H2"]))
    story.append(Paragraph(
        "Configurez les sauvegardes lors de l'installation (etape 13) ou "
        "depuis la page de depannage. Options :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Dossier de sauvegarde :</b> Defaut C:/LedgerLink/Backups/",
        "<b>Frequence :</b> Quotidienne, Hebdomadaire ou A chaque connexion",
        "<b>Retention :</b> Nombre de copies de sauvegarde a conserver",
        "<b>Synchronisation OneDrive :</b> Sauvegarde infonuagique optionnelle",
        "<b>Sauvegarde manuelle :</b> Telechargez la base de donnees depuis "
        "la page de depannage",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Le fichier de base de donnees SQLite contient tous les documents, "
        "utilisateurs, donnees de mission et la configuration. Un seul "
        "fichier de sauvegarde preserve tout. Testez les restaurations "
        "periodiquement.",
        styles,
    ))
    story.append(sp())


def _build_section_9_vendor_memory(story, styles):
    """Gestion de la memoire fournisseur."""
    story.append(Paragraph("9.5 Memoire fournisseur et alias", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink apprend des corrections pour constituer la memoire "
        "fournisseur &mdash; une base de donnees de patrons qui ameliore "
        "la precision au fil du temps.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Memoire fournisseur :</b> Suit les comptes GL approuves, les "
        "codes de taxe et les categories par combinaison fournisseur/client",
        "<b>Scores de confiance :</b> La confiance de la memoire augmente "
        "avec des approbations coherentes",
        "<b>Reinitialisation :</b> Effacer la memoire fournisseur pour un "
        "fournisseur/client specifique depuis Admin &gt; Memoire fournisseur",
        "<b>Alias de fournisseur :</b> Associer des noms alternatifs de "
        "fournisseur a des noms canoniques (ex. \"Desjardins\" et "
        "\"Mouvement Desjardins\")",
    ], styles))
    story.append(sp())


def _build_section_9_cache(story, styles):
    """Cache IA."""
    story.append(Paragraph("9.6 Cache IA", styles["H2"]))
    story.append(Paragraph(
        "Le routeur IA met en cache les reponses pour reduire les couts "
        "d'API. Consultez les statistiques du cache et videz-le depuis "
        "<b>Admin &gt; Cache</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Consulter le taux de succes du cache et la taille de stockage",
        "Vider le cache pour forcer de nouvelles reponses IA",
        "Utile apres un changement de fournisseur ou de modele IA",
    ], styles))
    story.append(sp())


def _build_section_9_updates(story, styles):
    """Mises a jour logicielles."""
    story.append(Paragraph("9.7 Mises a jour logicielles", styles["H2"]))
    story.append(Paragraph(
        "Verifiez et installez les mises a jour depuis "
        "<b>Admin &gt; Mises a jour</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Verifier :</b> Interroge le serveur de versions distant",
        "<b>Installer :</b> Telecharge et applique la mise a jour en "
        "arriere-plan. Le service redemarre automatiquement.",
    ], styles))
    story.append(sp())


def _build_section_9_remote(story, styles):
    """Gestion a distance."""
    story.append(Paragraph("9.8 Gestion a distance", styles["H2"]))
    story.append(Paragraph(
        "Pour les deploiements multi-postes, utilisez la page de gestion "
        "a distance (Proprietaire uniquement) pour gerer les instances "
        "LedgerLink distantes :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Redemarrer :</b> Redemarrer le service LedgerLink distant",
        "<b>Sauvegarder :</b> Declencher une sauvegarde de base de donnees distante",
        "<b>Mettre a jour :</b> Pousser une mise a jour logicielle vers l'instance distante",
        "<b>Reparation auto :</b> Executer le script de reparation automatique a distance",
    ], styles))
    story.append(sp())

    story.append(Paragraph("9.9 Configuration du resume quotidien", styles["H2"]))
    story.append(Paragraph(
        "Configurez les resumes automatiques par courriel depuis l'etape "
        "12 de l'assistant ou le fichier de configuration. Le resume "
        "quotidien comprend :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Nombre de nouveaux documents recus depuis le dernier resume",
        "Documents en attente de revision (nombre EnRevision)",
        "Documents en retenue avec motifs",
        "Alertes de fraude necessitant une attention",
        "Echeances de production a venir dans les 14 jours",
        "Resume de productivite du personnel (si Proprietaire)",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Configurez l'heure d'envoi, la liste de destinataires (courriels "
        "separes par des virgules) et la langue (FR ou EN). Necessite la "
        "configuration SMTP (etape 5).",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("9.10 Configuration des notifications", styles["H2"]))
    story.append(Paragraph(
        "Ajustez finement quels evenements declenchent des notifications "
        "et par quel canal. Configurez a l'etape 14 de l'assistant.",
        styles["Body"],
    ))
    story.append(sp())
    notif_rows = [
        ["Nouveau document recu", "Courriel, Bureau, Les deux, Aucun"],
        ["Fraude detectee", "Courriel, Bureau, Les deux, Aucun"],
        ["Document en attente > X jours", "Courriel, Bureau, Les deux, Aucun"],
        ["Echeance TPS/TVQ (14 jours)", "Courriel, Bureau, Les deux, Aucun"],
        ["Licence expire (30 jours)", "Courriel, Bureau, Les deux, Aucun"],
        ["Erreurs systeme", "Courriel, Bureau, Les deux, Aucun"],
    ]
    story.append(make_table(
        ["Evenement", "Options de canal"],
        notif_rows,
        col_widths=[3.0 * inch, 3.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("9.11 Pratiques exemplaires de securite", styles["H2"]))
    story.append(Paragraph(
        "Configuration de securite recommandee pour les environnements "
        "de production :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Delai d'expiration de session :</b> Definir a 1 heure ou "
        "4 heures pour les environnements actifs. Ne jamais desactiver "
        "le delai sur les postes partages.",
        "<b>Tentatives de connexion maximales :</b> Garder a 5 (defaut). "
        "N'augmenter que si les utilisateurs tapent frequemment leur mot "
        "de passe incorrectement.",
        "<b>Duree de verrouillage :</b> 15 minutes (defaut) equilibre "
        "securite et convivialite.",
        "<b>Forcer HTTPS :</b> Toujours activer lors de l'utilisation du "
        "tunnel Cloudflare. Assure que les temoins portent le drapeau Secure.",
        "<b>Mots de passe robustes :</b> Appliquer les exigences minimales "
        "de 8 caracteres, majuscule et chiffre pour tous les utilisateurs.",
        "<b>Rotation des cles API :</b> Effectuer la rotation des cles API "
        "des fournisseurs IA trimestriellement. Mettre a jour dans le fichier "
        "de configuration et redemarrer le service.",
        "<b>Sauvegardes de la base de donnees :</b> Frequence quotidienne "
        "avec au moins 7 copies conservees. Activer la synchronisation "
        "OneDrive pour la protection hors site.",
        "<b>Revue du journal d'audit :</b> Revoir periodiquement les "
        "journaux de derogation de fraude et les approbations d'inscription "
        "pour detecter les anomalies.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("9.12 Maintenance de la base de donnees", styles["H2"]))
    story.append(Paragraph(
        "La base de donnees SQLite grossit au fil du temps avec "
        "l'accumulation des documents. Conseils de maintenance :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Surveiller la taille de la base de donnees depuis la page de depannage",
        "Archiver les periodes completees en exportant les donnees et en creant "
        "une nouvelle base de donnees pour le nouvel exercice",
        "Executer VACUUM periodiquement : <font face='Courier'>sqlite3 "
        "data/ledgerlink_agent.db \"VACUUM;\"</font>",
        "Assurer un espace disque adequat (minimum 500 Mo libres recommande)",
        "SQLite prend en charge des bases de donnees jusqu'a 281 To ; en "
        "pratique, les performances peuvent se degrader au-dela de 1-2 Go "
        "sans optimisation",
    ], styles))


# ── Section 10 : Depannage ───────────────────────────────────
def build_section_10_troubleshooting(story, styles):
    """Section 10 — Problemes courants et solutions."""
    story.append(Paragraph("10. Depannage", styles["H1"]))
    story.append(sp())

    issues = [
        ("10.1 La connexion echoue", [
            ("Symptome", "Impossible de se connecter, ou HTTP 429 Trop de requetes."),
            ("Cause", "Limitation du debit : 5 tentatives echouees par 15 minutes par IP."),
            ("Correctif", "Attendez 15 minutes et reessayez. Verifiez le nom d'utilisateur "
             "et le mot de passe. Le Proprietaire peut reinitialiser les mots de passe "
             "depuis la page Utilisateurs. Verifiez que le service fonctionne sur le port 8787."),
        ]),
        ("10.2 La visionneuse PDF n'affiche pas", [
            ("Symptome", "Le PDF du document est vide ou ne se charge pas."),
            ("Cause", "Incompatibilite de la visionneuse PDF du navigateur ou fichier corrompu."),
            ("Correctif", "Essayez un autre navigateur (Chrome recommande). Telechargez le PDF "
             "et ouvrez-le dans un lecteur PDF autonome. Verifiez que le fichier existe dans "
             "le repertoire de donnees."),
        ]),
        ("10.3 Erreurs QuickBooks en ligne", [
            ("Symptome", "L'inscription echoue avec une erreur d'authentification QBO."),
            ("Cause", "Identifiants QBO expires ou invalides."),
            ("Correctif", "Reauthorisez QBO depuis l'etape 10 de l'assistant. Verifiez l'ID "
             "de domaine, l'ID client et le secret client. Verifiez que l'application QBO "
             "est en mode production (pas en bac a sable)."),
        ]),
        ("10.4 Extraction IA incorrecte", [
            ("Symptome", "Nom du fournisseur, montant ou date extraits incorrectement."),
            ("Cause", "Faible confiance OCR sur des numerisations de mauvaise qualite ou "
             "de l'ecriture manuscrite."),
            ("Correctif", "Corrigez les champs dans la vue de detail du document. La correction "
             "alimente la memoire fournisseur pour une precision future. Pour des problemes "
             "persistants avec un fournisseur, verifiez les alias de fournisseur. Envisagez "
             "une renumerisation a plus haute resolution."),
        ]),
        ("10.5 La reception par courriel ne fonctionne pas", [
            ("Symptome", "Les courriels transferes n'apparaissent pas dans la file."),
            ("Cause", "Service de reception non demarre ou SMTP mal configure."),
            ("Correctif", "Verifiez que le service de reception fonctionne sur le port 8789. "
             "Verifiez les parametres SMTP dans la configuration. Testez la livraison de "
             "courriel depuis l'assistant. Revisez les regles de pare-feu pour le port 8789."),
        ]),
        ("10.6 Tunnel Cloudflare en panne", [
            ("Symptome", "Le portail client est inaccessible depuis l'exterieur du reseau."),
            ("Cause", "Service cloudflared arrete ou mauvaise configuration DNS."),
            ("Correctif", "Redemarrez cloudflared : <font face='Courier'>cloudflared "
             "tunnel run ledgerlink</font>. Verifiez que le DNS pointe vers le tunnel. "
             "Verifiez le tableau de bord Cloudflare pour l'etat du tunnel."),
        ]),
        ("10.7 Indicateurs de substance incorrects", [
            ("Symptome", "Document signale comme immobilisation alors qu'il s'agit d'une reparation."),
            ("Cause", "Detection par mots-cles declenchee sur une correspondance partielle."),
            ("Correctif", "Remplacez la classification de substance sur la page de detail du "
             "document. Le compte du grand livre se mettra a jour en consequence. Le systeme "
             "utilise des mots-cles negatifs (ex. \"maintenance\" annule la detection "
             "d'immobilisation) mais des cas limites existent."),
        ]),
        ("10.8 Faux positifs de fraude", [
            ("Symptome", "Transaction legitime signalee comme fraude."),
            ("Cause", "Les regles statistiques se declenchent sur des patrons inhabituels "
             "mais valides."),
            ("Correctif", "Utilisez le flux de derogation de fraude (Gestionnaire/Proprietaire). "
             "Fournissez une justification detaillee. La derogation est journalisee. Considerez "
             "que les nouveaux fournisseurs avec de grosses premieres factures declencheront "
             "toujours la regle 8 jusqu'a ce que 5+ transactions constituent un historique."),
        ]),
        ("10.9 Reinitialisation de la memoire fournisseur", [
            ("Symptome", "Des suggestions GL incorrectes persistent apres les corrections."),
            ("Cause", "Memoire fournisseur obsolete provenant d'approbations incorrectes anterieures."),
            ("Correctif", "Naviguez vers Admin &gt; Memoire fournisseur. Selectionnez le "
             "fournisseur et le code client. Cliquez sur Reinitialiser. Cela efface les "
             "patrons appris et force le systeme a reapprendre des approbations futures."),
        ]),
        ("10.10 Problemes de performance", [
            ("Symptome", "Le tableau de bord se charge lentement ou expire."),
            ("Cause", "Base de donnees volumineuse, memoire vive insuffisante ou problemes reseau."),
            ("Correctif", "Videz le cache IA (Admin &gt; Cache). Archivez les anciens documents "
             "des periodes completees. Assurez-vous d'avoir au moins 4 Go de memoire vive. "
             "Verifiez qu'aucun autre service ne monopolise le port 8787. Envisagez le "
             "deploiement serveur uniquement (option B) pour le multi-postes."),
        ]),
    ]

    for heading, items in issues:
        story.append(Paragraph(heading, styles["H2"]))
        for label, text in items:
            story.append(Paragraph(f"<b>{label} :</b> {text}", styles["Body"]))
        story.append(sp())

    story.append(Paragraph("10.11 Reference des codes d'erreur", styles["H2"]))
    story.append(Paragraph(
        "Codes d'erreur HTTP courants retournes par LedgerLink et leur "
        "signification :",
        styles["Body"],
    ))
    story.append(sp())
    err_rows = [
        ["200", "OK", "Requete reussie"],
        ["302", "Redirection", "Apres la soumission d'un formulaire,\n"
         "redirection vers la page suivante"],
        ["400", "Requete invalide", "Entree invalide : champ obligatoire\n"
         "manquant, JSON invalide ou donnees mal formees"],
        ["401", "Non autorise", "Session expiree ou non connecte.\n"
         "Redirection vers la page de connexion."],
        ["403", "Interdit", "Permissions de role insuffisantes.\n"
         "Fonctionnalite Proprietaire/Gestionnaire\naccedee par un Employe."],
        ["404", "Non trouve", "L'ID du document n'existe pas ou\n"
         "expediteur inconnu lors de la reception OpenClaw."],
        ["429", "Trop de requetes", "Limite de debit depassee. Attendre\n"
         "15 minutes avant de reessayer la connexion."],
        ["500", "Erreur serveur", "Erreur inattendue. Verifier install.log\n"
         "et redemarrer le service."],
    ]
    story.append(make_table(
        ["Code", "Etat", "Description"],
        err_rows,
        col_widths=[0.6 * inch, 1.4 * inch, 4.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("10.12 Recuperation de la base de donnees", styles["H2"]))
    story.append(Paragraph(
        "Si la base de donnees SQLite est corrompue, suivez ces etapes :",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Arretez le service LedgerLink.",
        "Localisez la derniere sauvegarde dans C:/LedgerLink/Backups/ (ou "
        "votre dossier de sauvegarde configure).",
        "Copiez le fichier de sauvegarde vers <font face='Courier'>"
        "data/ledgerlink_agent.db</font>, en remplacant le fichier corrompu.",
        "Executez <font face='Courier'>python scripts/migrate_db.py</font> "
        "pour appliquer toute migration en attente.",
        "Executez <font face='Courier'>python scripts/autofix.py</font> "
        "pour verifier l'integrite.",
        "Redemarrez le service.",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Les donnees saisies depuis la derniere sauvegarde seront perdues. "
        "Augmentez la frequence de sauvegarde si la perte de donnees est "
        "une preoccupation. Envisagez des sauvegardes quotidiennes avec "
        "synchronisation OneDrive pour une protection maximale.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("10.13 Conflits de port", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink utilise trois ports par defaut. Si un port est deja "
        "utilise par une autre application :",
        styles["Body"],
    ))
    port_rows = [
        ["8787", "Tableau de bord", "Modifier 'port' dans la configuration"],
        ["8788", "Portail client", "Modifier 'client_portal.port' dans la configuration"],
        ["8789", "Service de reception", "Modifier 'ingest.port' dans la configuration"],
        ["8790", "Assistant de configuration", "Ne fonctionne que lors de l'installation initiale"],
    ]
    story.append(make_table(
        ["Port", "Service", "Comment modifier"],
        port_rows,
        col_widths=[0.8 * inch, 1.5 * inch, 3.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Sous Windows, verifiez l'utilisation des ports avec : "
        "<font face='Courier'>netstat -ano | findstr :8787</font>. "
        "Terminez le processus en conflit ou modifiez le port LedgerLink "
        "dans le fichier de configuration.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("10.14 Mise a niveau de Python", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink necessite Python 3.11 ou superieur. Pour mettre a niveau :",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Telechargez le dernier Python 3.11+ depuis python.org.",
        "Installez avec \"Add Python to PATH\" coche.",
        "Arretez le service LedgerLink.",
        "Executez : <font face='Courier'>pip install -r requirements.txt</font>",
        "Redemarrez le service.",
        "Verifiez : <font face='Courier'>python --version</font>",
    ], styles))
    story.append(sp())

    story.append(PageBreak())


# ── Section 11 : Glossaire ───────────────────────────────────
def build_section_11_glossary(story, styles):
    """Section 11 — 55+ termes FR/EN cote a cote."""
    story.append(Paragraph("11. Glossaire", styles["H1"]))
    story.append(sp())
    story.append(Paragraph(
        "LedgerLink est entierement bilingue. Ce glossaire fournit les "
        "equivalents francais et anglais des termes cles utilises dans "
        "l'ensemble de l'application.",
        styles["Body"],
    ))
    story.append(sp())

    _build_glossary_general(story, styles)
    _build_glossary_tax(story, styles)
    _build_glossary_audit(story, styles)
    _build_glossary_payroll(story, styles)


def _build_glossary_general(story, styles):
    """Termes comptables generaux."""
    story.append(Paragraph("11.1 Termes comptables generaux", styles["H2"]))
    rows = [
        ["Comptes fournisseurs", "Accounts Payable",
         "Montants dus aux fournisseurs"],
        ["Comptes clients", "Accounts Receivable",
         "Montants dus par les clients"],
        ["Bilan", "Balance Sheet",
         "Etat de la situation financiere"],
        ["Rapprochement bancaire", "Bank Reconciliation",
         "Appariement des registres\nbancaires et du grand livre"],
        ["Plan comptable", "Chart of Accounts",
         "Liste des comptes du grand livre"],
        ["Note de credit", "Credit Note",
         "Contre-passation ou reduction\nd'une facture"],
        ["Fin d'exercice", "Fiscal Year-End",
         "Fin de la periode comptable"],
        ["Grand livre", "General Ledger",
         "Registre comptable principal"],
        ["Etat des resultats", "Income Statement",
         "Compte de resultat"],
        ["Facture", "Invoice",
         "Demande de paiement pour\nbiens ou services"],
        ["Ecriture de journal", "Journal Entry",
         "Ecriture comptable manuelle"],
        ["Inscription", "Posting",
         "Enregistrement au grand livre/QBO"],
        ["Recu", "Receipt",
         "Preuve de paiement"],
        ["Balance de verification", "Trial Balance",
         "BV pour tous les comptes"],
        ["Fournisseur", "Vendor",
         "Prestataire de biens/services"],
    ]
    story.append(make_table(
        ["Francais", "English", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_glossary_tax(story, styles):
    """Termes fiscaux et de conformite."""
    story.append(Paragraph("11.2 Termes fiscaux et de conformite", styles["H2"]))
    rows = [
        ["TPS (Taxe sur les produits\net services)", "GST (Goods and\nServices Tax)",
         "Taxe federale de 5 % sur\nles biens et services"],
        ["TVQ (Taxe de vente\ndu Quebec)", "QST (Quebec\nSales Tax)",
         "Taxe provinciale de\n9,975 % du Quebec"],
        ["TVH (Taxe de vente\nharmonisee)", "HST (Harmonized\nSales Tax)",
         "Taxe de vente harmonisee\n(ON, Atlantique)"],
        ["CTI (Credit de taxe\nsur les intrants)", "ITC (Input\nTax Credit)",
         "Recuperation de TPS/TVH\nsur les achats d'entreprise"],
        ["RTI (Remboursement de la\ntaxe sur les intrants)", "ITR (Input\nTax Refund)",
         "Recuperation de TVQ\nsur les achats d'entreprise"],
        ["Methode rapide", "Quick Method",
         "Calcul simplifie de TPS/TVQ\npour les petites entreprises"],
        ["FPZ-500", "FPZ-500",
         "Formulaire de declaration\nTPS/TVQ du Quebec"],
        ["Fourniture exoneree", "Exempt Supply",
         "Fourniture sans taxe\net sans CTI"],
        ["Fourniture detaxee", "Zero-rated Supply",
         "Fourniture a 0 % de taxe\nmais CTI admissible"],
        ["Fourniture mixte", "Mixed Supply",
         "Facture avec articles\ntaxables et exemptes"],
        ["Lieu de fourniture", "Place of Supply",
         "Province determinant\nle regime fiscal"],
        ["Valeur en douane", "Customs Value",
         "Base d'evaluation de\nl'ASFC a l'importation"],
    ]
    story.append(make_table(
        ["Francais", "English", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_glossary_audit(story, styles):
    """Termes de mission et NCA."""
    story.append(Paragraph("11.3 Termes de mission et NCA", styles["H2"]))
    rows = [
        ["NCA (Normes canadiennes\nd'audit)", "CAS (Canadian\nAuditing Standards)",
         "Normes canadiennes\nd'audit"],
        ["Importance relative", "Materiality",
         "Seuil NCA 320 pour les\nanomalies significatives"],
        ["Mission", "Engagement",
         "Mission de verification/examen/\ncompilation"],
        ["Dossier de travail", "Working Paper",
         "Documentation de mission\npour chaque compte"],
        ["Feuille sommaire", "Lead Sheet",
         "Dossier de travail resume\npour un groupe de comptes"],
        ["Pointage", "Tick Mark",
         "Symbole indiquant le\ntest effectue"],
        ["Assertion", "Assertion",
         "Affirmation de la direction\nsur les etats financiers"],
        ["Evaluation des risques", "Risk Assessment",
         "Identification des risques\nselon NCA 315"],
        ["Test de controle", "Control Test",
         "Test NCA 330 des\ncontroles internes"],
        ["Sondage", "Sampling",
         "Selection d'echantillon\nstatistique NCA 530"],
        ["Continuite d'exploitation", "Going Concern",
         "Evaluation de la viabilite\nselon NCA 570"],
        ["Lettre de declaration", "Rep Letter",
         "Lettre de declaration de\nla direction NCA 580"],
        ["Partie liee", "Related Party",
         "Entite ou individu lie\nselon NCA 550"],
        ["Rapport du verificateur", "Audit Opinion",
         "Conclusion du verificateur\nselon NCA 700"],
        ["NCCQ 1", "CSQC 1",
         "Norme de controle qualite\npour les cabinets CPA"],
        ["Mission de verification", "Audit",
         "Mission d'audit avec\nassurance raisonnable"],
        ["Mission d'examen", "Review",
         "Mission d'assurance\nlimitee"],
        ["Mission de compilation", "Compilation",
         "Preparation des etats\nfinanciers sans assurance"],
    ]
    story.append(make_table(
        ["Francais", "English", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_glossary_payroll(story, styles):
    """Termes de paie et specifiques au Quebec."""
    story.append(Paragraph("11.4 Termes de paie et specifiques au Quebec", styles["H2"]))
    rows = [
        ["RRQ (Regime de rentes\ndu Quebec)", "QPP (Quebec\nPension Plan)",
         "Regime de rentes du Quebec"],
        ["RPC (Regime de pensions\ndu Canada)", "CPP (Canada\nPension Plan)",
         "Regime de pensions du\nCanada (hors Quebec)"],
        ["AE (Assurance-emploi)", "EI (Employment\nInsurance)",
         "Assurance-emploi"],
        ["RQAP (Regime quebecois\nd'assurance parentale)", "QPIP (Quebec\nParental Insurance)",
         "Regime quebecois\nd'assurance parentale"],
        ["FSS (Fonds des services\nde sante)", "HSF (Health\nServices Fund)",
         "Fonds des services de sante\n(contribution employeur)"],
        ["CNESST", "CNESST",
         "Commission des normes,\nde l'equite, de la sante\net de la securite du travail"],
        ["Releve 1 (RL-1)", "RL-1",
         "Releve de revenus\nd'emploi du Quebec"],
        ["T4", "T4",
         "Releve de revenus\nd'emploi federal"],
        ["DAS (Deductions a\nla source)", "Source Deductions",
         "Retenues salariales\n(remise de paie)"],
        ["Revenu Quebec", "Revenu Quebec",
         "Agence du revenu\nprovinciale du Quebec"],
        ["ARC (Agence du revenu\ndu Canada)", "CRA (Canada\nRevenue Agency)",
         "Agence du revenu du Canada"],
    ]
    story.append(make_table(
        ["Francais", "English", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(sp())

    story.append(Paragraph("11.5 Termes systeme et techniques", styles["H2"]))
    rows = [
        ["ROC (Reconnaissance optique\nde caracteres)", "OCR (Optical Character\nRecognition)",
         "Reconnaissance de caracteres\na partir d'images"],
        ["API (Interface de\nprogrammation)", "API (Application\nProgramming Interface)",
         "Interface de programmation\nd'applications"],
        ["SMTP", "SMTP",
         "Protocole d'envoi\nde courriel"],
        ["Tunnel Cloudflare", "Cloudflare Tunnel",
         "Acces a distance securise\nsans redirection de port"],
        ["SQLite", "SQLite",
         "Moteur de base de\ndonnees integre"],
        ["OpenClaw", "OpenClaw",
         "Pont de messagerie\nWhatsApp/Telegram"],
        ["Surveillance de dossier", "Folder Watcher",
         "Detection automatique des\nnouveaux fichiers dans le dossier"],
        ["Routeur IA", "AI Router",
         "Achemine les taches vers\nle fournisseur IA economique\nou premium"],
        ["Memoire fournisseur", "Vendor Memory",
         "Patrons appris pour\nchaque fournisseur"],
        ["Approbation automatique", "Auto-Approval",
         "Documents approuves\nsans revision humaine"],
        ["Score de confiance", "Confidence Score",
         "Mesure de fiabilite\nde l'extraction 0,00-1,00"],
        ["Verrouillage optimiste", "Optimistic Locking",
         "Verification de version\nempechant les ecritures obsoletes"],
        ["Indicateur de modification", "Amendment Flag",
         "Marque la periode produite\nnecessitant une correction"],
        ["Instantane de production", "Filing Snapshot",
         "Etat fige au moment\nde la production fiscale"],
    ]
    story.append(make_table(
        ["Francais", "English", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph(
        "Ce glossaire couvre les principaux termes utilises dans LedgerLink. "
        "L'interface de l'application affiche tous les termes dans les deux "
        "langues et peut etre basculee en un clic depuis n'importe quelle page.",
        styles["Body"],
    ))
    story.append(sp())

    # ── Annexe : Carte de reference rapide ──
    story.append(PageBreak())
    story.append(Paragraph("Annexe A : Carte de reference rapide", styles["H1"]))
    story.append(sp())
    story.append(Paragraph(
        "Cette carte de reference rapide resume les operations les plus "
        "courantes dans LedgerLink pour une utilisation quotidienne.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("A.1 URLs du tableau de bord", styles["H2"]))
    url_rows = [
        ["Tableau de bord principal", "http://127.0.0.1:8787/"],
        ["Portail client", "http://127.0.0.1:8788/"],
        ["Assistant de configuration", "http://127.0.0.1:8790/"],
        ["Page de connexion", "http://127.0.0.1:8787/login"],
        ["Analytique", "http://127.0.0.1:8787/analytics"],
        ["Rapprochement", "http://127.0.0.1:8787/reconciliation"],
        ["Calendrier de production", "http://127.0.0.1:8787/calendar"],
        ["Dossiers de travail", "http://127.0.0.1:8787/working_papers"],
        ["Missions", "http://127.0.0.1:8787/engagements"],
        ["Depannage", "http://127.0.0.1:8787/troubleshoot"],
    ]
    story.append(make_table(
        ["Fonctionnalite", "URL"],
        url_rows,
        col_widths=[2.0 * inch, 4.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.2 Guide rapide du flux de documents", styles["H2"]))
    story.extend(numbered_list([
        "Le document arrive par n'importe quel canal de reception (portail, "
        "courriel, WhatsApp, Telegram, surveillance de dossier ou "
        "telechargement manuel).",
        "Le moteur OCR extrait les donnees : fournisseur, montant, date, "
        "code de taxe, compte du grand livre.",
        "Le moteur de fraude execute 13 regles deterministes sur le document.",
        "Le moteur de substance classifie les elements hors exploitation "
        "(immobilisations, charges payees d'avance, etc.).",
        "La politique de revision calcule le score de confiance effectif.",
        "Si confiance &ge; 0,85 et aucun blocage : etat = Pret (approbation automatique).",
        "Si confiance &lt; 0,85 ou blocages presents : etat = EnRevision.",
        "Le reviseur verifie et corrige les champs sur la page de detail.",
        "Le reviseur change l'etat a Pret (ou EnRetenue avec motif).",
        "Le Gestionnaire/Proprietaire cree un travail d'inscription a "
        "partir des documents Pret.",
        "La verification mathematique confirme que sous-total + taxes = total.",
        "La verification des indicateurs de fraude s'execute a nouveau lors "
        "de l'approbation.",
        "Le Gestionnaire/Proprietaire approuve le travail d'inscription.",
        "La transaction est inscrite dans QuickBooks en ligne. Etat = Inscrit.",
        "Les corrections alimentent la memoire fournisseur pour ameliorer "
        "la precision future.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("A.3 Reference rapide des codes de taxe", styles["H2"]))
    tax_quick = [
        ["T", "Standard Quebec", "TPS 5 % + TVQ 9,975 %", "CTI + RTI complets"],
        ["Z", "Detaxe", "0 %", "CTI sur intrants"],
        ["E", "Exempte", "0 %", "Aucun CTI"],
        ["M", "Repas (50 %)", "TPS + TVQ", "50 % CTI + RTI"],
        ["I", "Assurance (QC)", "9 % non recuperable", "Aucun CTI"],
        ["TVH", "Ontario", "13 %", "CTI complet"],
        ["TVH_ATL", "Atlantique", "15 %", "CTI complet"],
        ["TPS_SEULE", "AB/Territoires", "5 %", "CTI complet"],
        ["TVA", "Etrangere", "Variable", "Aucune recuperation"],
        ["AUCUN", "Aucune taxe", "0 %", "S.O."],
    ]
    story.append(make_table(
        ["Code", "Usage", "Taux", "Recuperation"],
        tax_quick,
        col_widths=[0.8 * inch, 1.5 * inch, 1.8 * inch, 1.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.4 Reference rapide des regles de fraude", styles["H2"]))
    fraud_quick = [
        ["1-2", "Anomalies fournisseur", "Ecart de montant/calendrier par rapport a l'historique",
         "Necessite 5+ transactions\nprealables"],
        ["3-4", "Doublons", "Doublons meme/entre fournisseurs",
         "Fenetres de 30 jours / 7 jours"],
        ["5-6", "Fin de semaine/ferie", "Transactions les jours non ouvrables",
         "Seuil de montant &gt; 200 $"],
        ["7", "Montant rond", "Montant parfaitement rond",
         "De fournisseur irregulier"],
        ["8", "Nouveau fournisseur\nmontant eleve", "Premiere facture &gt; 2 000 $",
         "Efface apres 5 transactions"],
        ["9", "Changement bancaire", "Coordonnees de paiement changees",
         "Severite CRITIQUE"],
        ["10", "Apres paiement", "Facture datee apres le paiement",
         "Severite ELEVEE"],
        ["11", "Contradiction fiscale", "TPS/TVQ de fournisseur exempte",
         "Severite ELEVEE"],
        ["12-13", "Categorie/Beneficiaire", "Divergence avec le patron historique",
         "Severite MOYEN/ELEVE"],
    ]
    story.append(make_table(
        ["Regles", "Nom", "Declencheur", "Notes"],
        fraud_quick,
        col_widths=[0.6 * inch, 1.3 * inch, 2.4 * inch, 2.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.5 Reference rapide des NCA", styles["H2"]))
    cas_quick = [
        ["NCA 315", "Evaluation des risques", "Risque inherent + controles par assertion"],
        ["NCA 320", "Importance relative", "Planification, travaux, clairement negligeable"],
        ["NCA 330", "Tests des controles", "15 controles standards, 4 types de tests"],
        ["NCA 500", "Couverture des assertions", "7 assertions par compte"],
        ["NCA 530", "Sondage", "Sondage statistique avec semence"],
        ["NCA 550", "Parties liees", "5 types de relations, suivi des TPL"],
        ["NCA 560", "Evenements posterieurs", "Suivi de la chronologie des modifications"],
        ["NCA 570", "Continuite d'exploitation", "Auto-detection a la creation de mission"],
        ["NCA 580", "Lettre de declaration", "Modele bilingue, 6 declarations"],
        ["NCA 700", "Rapport du verificateur", "Verification de la liste de controle pre-emission"],
        ["NCCQ 1", "Controle qualite", "Affectation d'equipe, revision, immutabilite"],
    ]
    story.append(make_table(
        ["Norme", "Sujet", "Implementation LedgerLink"],
        cas_quick,
        col_widths=[0.9 * inch, 1.5 * inch, 3.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.6 Comparaison des niveaux de licence", styles["H2"]))
    tier_rows = [
        ["Fonctionnalite", "Essentiel", "Professionnel", "Cabinet", "Entreprise"],
        ["Max clients", "10", "30", "75", "Illimite"],
        ["Max utilisateurs", "3", "5", "15", "Illimite"],
        ["Revision de base", "Oui", "Oui", "Oui", "Oui"],
        ["Routeur IA", "Non", "Oui", "Oui", "Oui"],
        ["Detection de fraude", "Non", "Oui", "Oui", "Oui"],
        ["Analyseur bancaire", "Non", "Oui", "Oui", "Oui"],
        ["Revenu Quebec", "Non", "Oui", "Oui", "Oui"],
        ["Suivi du temps", "Non", "Oui", "Oui", "Oui"],
        ["Analytique", "Non", "Non", "Oui", "Oui"],
        ["Microsoft 365", "Non", "Non", "Oui", "Oui"],
        ["Calendrier de production", "Non", "Non", "Oui", "Oui"],
        ["Communications", "Non", "Non", "Oui", "Oui"],
        ["Module de mission", "Non", "Non", "Non", "Oui"],
        ["Etats financiers", "Non", "Non", "Non", "Oui"],
        ["Sondage", "Non", "Non", "Non", "Oui"],
        ["Acces API", "Non", "Non", "Non", "Oui"],
    ]
    data = [tier_rows[0]] + tier_rows[1:]
    t = Table(data, colWidths=[1.5 * inch, 0.9 * inch, 1.1 * inch, 0.9 * inch, 1.0 * inch],
              repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BACKGROUND", (0, 1), (-1, -1), WHITE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY]),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#CCCCCC")),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
    ]))
    story.append(t)
    story.append(sp())

    story.append(Paragraph("A.7 Reference rapide des indicateurs de substance",
                           styles["H2"]))
    sub_quick = [
        ["Immobilisation", "Equipement, vehicules,\nordinateurs, renovations",
         "GL 1500", "\"Maintenance\" ou \"reparation\"\nannule la detection"],
        ["Charge payee\nd'avance", "Assurance, loyer d'avance,\nabonnements annuels",
         "GL 1300", "\"Assurance qualite\" n'est\nPAS de l'assurance (faux positif)"],
        ["Emprunt", "Hypotheques, marges de\ncredit, locations-financement",
         "GL 2500", "\"Pret-a-porter\" n'est PAS\nun emprunt (faux positif)"],
        ["Remise fiscale", "TPS/TVQ, retenues a\nla source, CNESST",
         "GL 2200-2215", "Retenues a la source et\nremises gouvernementales"],
        ["Personnel", "Epicerie, vetements,\nNetflix, gym, vacances",
         "GL 5400", "\"Personnel RH\" designe\nles RH, pas le personnel"],
        ["Actionnaire", "Retraits, prets entre\nparties liees, dividendes",
         "GL 2600", "Les transactions d'actionnaire\nnecessitent une revue NCA 550"],
    ]
    story.append(make_table(
        ["Categorie", "Exemples", "GL", "Notes de faux positifs"],
        sub_quick,
        col_widths=[1.0 * inch, 1.6 * inch, 0.8 * inch, 2.4 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.8 Liste de controle de rapprochement", styles["H2"]))
    story.append(Paragraph(
        "Utilisez cette liste de controle avant de finaliser un "
        "rapprochement bancaire :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Le solde du releve correspond exactement au PDF du releve bancaire",
        "Le solde du grand livre correspond a la balance de verification pour le compte",
        "Tous les depots en transit ont ete verifies contre les bordereaux de depot",
        "Tous les cheques en circulation ont ete confirmes comme non encore compenses",
        "Les erreurs bancaires ont une documentation a l'appui",
        "Les erreurs comptables ont ete corrigees par des ecritures de journal",
        "Solde bancaire ajuste = Solde comptable ajuste (a 0,01 $ pres)",
        "Le rapprochement a ete telecharge en PDF pour le dossier de mission",
    ], styles))
    story.append(sp())

    story.append(Paragraph("A.9 Soutien et coordonnees", styles["H2"]))
    story.append(Paragraph(
        "Si vous rencontrez des problemes non couverts dans ce manuel, les "
        "ressources suivantes sont disponibles :",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Soutien par courriel :</b> support@ledgerlink.ca",
        "<b>Guide d'installation :</b> docs/README_INSTALL.txt (inclus dans "
        "le paquet d'installation)",
        "<b>Guide du second poste :</b> docs/SECOND_MACHINE_INSTALL.md",
        "<b>Outil de reparation automatique :</b> Executez "
        "<font face='Courier'>python scripts/autofix.py</font> pour un "
        "diagnostic et une reparation automatiques",
        "<b>Page de depannage :</b> Accedez depuis la barre laterale "
        "d'administration pour l'etat du systeme en temps reel",
        "<b>Problemes de licence :</b> Contactez support@ledgerlink.ca pour "
        "les transferts de licence et les reinitialisations d'activation",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Lorsque vous contactez le soutien, veuillez inclure : votre niveau "
        "de licence, le message d'erreur ou la capture d'ecran, les etapes "
        "pour reproduire le probleme et le contenu de "
        "C:\\LedgerLink\\install.log le cas echeant.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph(
        "&mdash; Fin du Manuel d'utilisation LedgerLink AI &mdash;",
        ParagraphStyle("EndMark", parent=styles["Body"],
                       alignment=TA_CENTER, textColor=BLUE,
                       fontSize=11, spaceBefore=24),
    ))


# ═══════════════════════════════════════════════════════════════
#  MODELE DE PAGE ET MAIN
# ═══════════════════════════════════════════════════════════════

def _on_page(canvas, doc):
    """Dessiner la ligne d'en-tete et le numero de page."""
    canvas.saveState()
    # Ligne d'en-tete
    canvas.setStrokeColor(BLUE)
    canvas.setLineWidth(1)
    canvas.line(
        0.75 * inch, letter[1] - 0.6 * inch,
        letter[0] - 0.75 * inch, letter[1] - 0.6 * inch,
    )
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(BLUE)
    canvas.drawString(0.75 * inch, letter[1] - 0.55 * inch,
                      "LedgerLink AI - Manuel d'utilisation")
    # Numero de page
    canvas.setFillColor(DARK_GREY)
    canvas.drawCentredString(
        letter[0] / 2, 0.5 * inch,
        f"Page {doc.page}",
    )
    canvas.restoreState()


def _on_first_page(canvas, doc):
    """Page couverture — aucun en-tete/pied de page."""
    pass


def main():
    """Construire le manuel PDF complet."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(OUT_PATH),
        pagesize=letter,
        topMargin=0.85 * inch,
        bottomMargin=0.85 * inch,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        title="LedgerLink AI - Manuel d'utilisation",
        author="LedgerLink AI",
    )

    story = []
    styles = get_styles()

    build_cover_page(story, styles)
    build_toc(story, styles)
    build_section_1_introduction(story, styles)
    build_section_2_installation(story, styles)
    build_section_3_user_management(story, styles)
    build_section_4_daily_workflow(story, styles)
    build_section_5_quebec_tax(story, styles)
    build_section_6_client_portal(story, styles)
    build_section_7_monthend(story, styles)
    build_section_8_audit(story, styles)
    build_section_9_administration(story, styles)
    build_section_10_troubleshooting(story, styles)
    build_section_11_glossary(story, styles)

    doc.build(story, onFirstPage=_on_first_page, onLaterPages=_on_page)
    print(f"PDF genere : {OUT_PATH}")
    print(f"Taille du fichier : {OUT_PATH.stat().st_size / 1024:.0f} Ko")

    # Compter les pages
    try:
        from reportlab.lib.utils import open_for_read
        import re
        with open(str(OUT_PATH), "rb") as f:
            data = f.read()
        pages = len(re.findall(rb"/Type\s*/Page[^s]", data))
        print(f"Nombre de pages : {pages}")
    except Exception:
        print("Nombre de pages : verifier dans le visionneur PDF")


if __name__ == "__main__":
    main()
